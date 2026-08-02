# Docket detached dispatch design — paper-dogfood review

**Evaluated:** `docs/architecture/docket-detached-dispatch-design.md` as read on `suttonmay5/dckt-40` (spike code consulted as runtime reference only; the doc is the contract under test).
**Method:** paper dogfood. No implementation exists, so every "measurement" below is a concrete code sketch written strictly from what the doc promises, with every guess forced by the doc flagged. Three personas: an embedded Elixir host, a standalone-Elixir customer, and a Python executor fleet behind a bridge.
**Authority:** same as the DCKT-40 friction report this design claims to close.

**Overall.** This design closes eight of the prior report's eleven findings, most of them structurally rather than by patching — the stub module, the identity re-derivation, the forgeable completion, and the duplicate-work-by-construction poller are all *unrepresentable* now, which is the strongest kind of fix. The embedded-Elixir integration drops from "rebuild an index, deploy a stub node, hand-assemble a private struct" to roughly one hundred lines of ordinary GenServer. The remaining friction concentrates in exactly two places: the **completion fence's answer vocabulary** (the doc promises the executor an answer it cannot actually deliver, and three sections disagree about the fence order) and the **read/operator surface over the claim index** (the doc's own recoverability story depends on a read it never provides). Both are spec-level defects, fixable with words and one SELECT, not new machinery.

---

## Part I — paper-dogfood sketches

### I.a Embedded Elixir host

The complete integration a host team writes, using only what the doc specifies.

**Graph declaration** (~22 lines). The doc's own declaration example never shows how a detached node's results enter graph state — its example node writes nothing, which is the one thing every real detached node does. I had to infer from "output writes validate identically to module nodes" that I declare fields and the completion's `outputs` map writes them:

```elixir
graph =
  Docket.Graph.new!(id: "support_pipeline")
  |> Docket.Graph.put_input!("ticket", schema: :map, required: true)
  |> Docket.Graph.put_node!("classify",
       implementation: MyApp.Nodes.Classify,
       fields: %{"classification" => :string})
  |> Docket.Graph.put_node!("summarize",
       implementation: :detached,
       config: %{"endpoint" => "summarize", "style" => "terse"},
       fields: %{"summary" => :string},            # GUESS: is this how a detached node declares its writes?
       policies: %{
         "retry" => %{"max_attempts" => 3, "backoff_ms" => 5_000},
         "detach" => %{
           "start_to_close_ms" => 600_000,
           "schedule_to_start_ms" => 3_600_000,
           "on_deadline" => "reschedule"
         }
       })
  |> Docket.Graph.put_node!("finalize", implementation: MyApp.Nodes.Finalize)
  |> Docket.Graph.put_edge!("e1", source: "classify", target: "summarize")
  |> Docket.Graph.put_edge!("e2", source: "summarize", target: "finalize")
  |> Docket.Graph.put_output!("summary", channel: "state:summary")
```

**The dispatcher** — the subscribe → drain-claim → dispatch → complete GenServer the doc implies but never sketches (~80 lines):

```elixir
defmodule MyApp.DetachedDispatcher do
  use GenServer
  require Logger

  @runtime MyApp.Docket
  @poll_ms 30_000   # GUESS: needed even in-VM? Doc says tolerate missed wakes, but scopes
                    # the poll-fallback advice to "multi-VM Postgres deployments".

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    :ok = Docket.subscribe_detached(@runtime, [])   # GUESS: return value unspecified
    schedule_poll()
    {:ok, %{}, {:continue, :drain}}                 # drain on start: a wake fired while we
  end                                               # were down is lost (implied, not stated)

  @impl true
  def handle_continue(:drain, state), do: {:noreply, drain(state)}

  @impl true
  def handle_info({:docket_detached, _instance, _node_id}, state) do
    # GUESS: the wake-hint message shape is documented nowhere. "The hint carries no
    # payload beyond instance and node id" describes contents, not the term I match on.
    {:noreply, drain(state)}
  end

  def handle_info(:poll, state) do
    schedule_poll()
    {:noreply, drain(state)}
  end

  def handle_info(_task_result, state), do: {:noreply, state}

  defp drain(state) do
    case Docket.claim_detached(@runtime, node_ids: ["summarize"]) do
      {:ok, task} ->
        Task.Supervisor.start_child(MyApp.DispatchTasks, fn -> execute(task) end)
        drain(state)
      :empty ->
        state
      {:error, err} ->
        Logger.error("claim failed: #{inspect(err)}")
        state
    end
  end

  defp execute(task) do
    payload = %{
      config: task.config, snapshot: task.snapshot,
      attempt: task.attempt, max_attempts: task.max_attempts,
      prior_failures: task.prior_failures,
      deadline_at: task.deadline_at, idempotency_key: task.idempotency_key
    }

    case MyApp.ExecutorClient.post(task.config["endpoint"], payload,
           recv_timeout: budget_ms(task.deadline_at)) do
      {:ok, %{status: 200, body: outputs}} ->
        finish(task, {:ok, outputs}, [])
      {:ok, %{status: s, body: body}} when s in 400..499 ->
        finish(task, {:error, "executor rejected (#{s}): #{inspect(body)}"}, permanent: true)
      {:ok, %{status: s}} ->
        finish(task, {:error, "executor error #{s}"}, [])
      {:error, reason} ->
        # TRAP (finding 3): the transport failed, no work happened, but my only moves are
        # (a) complete {:error, ...} and burn one of 3 retry-budget attempts on a
        # connection refusal, or (b) do nothing and strand the task claimed for the
        # full 10-minute start_to_close. There is no release.
        finish(task, {:error, "dispatch failed: #{inspect(reason)}"}, [])
    end
  end

  defp finish(task, result, opts) do
    case Docket.complete_detached(@runtime, task, result, opts) do
      {:applied, _run} -> :ok
      {:unchanged, :superseded, _run} -> Logger.info("stale/duplicate completion dropped")
      {:unchanged, :terminal, _run} -> Logger.info("run terminal; result discarded")
      {:error, %Docket.Error{} = err} -> Logger.error("completion rejected: #{inspect(err)}")
    end
  end

  defp budget_ms(deadline_at),
    do: max(DateTime.diff(deadline_at, DateTime.utc_now(), :millisecond) - 2_000, 1_000)

  defp schedule_poll, do: Process.send_after(self(), :poll, @poll_ms)
end
```

**Count:** ~22 (graph) + ~80 (dispatcher) + 2 (supervision) ≈ **105 lines** for a working integration with typed failure/permanent handling. Under the spike contract the same persona needed a checkpoint-observer index rebuild, an Elixir stub node, and hand-assembled identity structs before line one of dispatch logic. This is a categorical improvement.

**Every point the doc left me guessing** (each becomes a finding below):

1. The wake-hint **message shape** — I cannot write `handle_info/2` from this doc (finding 7).
2. `subscribe_detached` return value, accepted opts, unsubscribe, and process-death semantics (finding 7).
3. Whether an in-VM claimant still needs the poll fallback (the doc's best-effort language says yes; its "documented pattern for multi-VM" scoping says maybe not) (finding 7).
4. What `outputs` is — shape, validation target, and how the node declares writable fields (finding 6).
5. What to do when dispatch fails after claim — no release path exists and the seam is not in the out-of-scope table (finding 3).
6. Whether `on_deadline` may be omitted and what its default is (finding 8).
7. Under `tenant_mode: :required`, what I pass to `complete_detached` after a cross-scope claim (finding 4).

### I.b Standalone Elixir service (customer with no Elixir codebase)

What they must build to run Docket as its own service, separated honestly:

**Normal Elixir app boilerplate** (not Docket's problem): mix project + OTP release + Dockerfile; an Ecto repo and database config; a supervision tree; health checks; deployment. Any Elixir service pays this.

**Docket-specific but deliberate** (recorded in the doc's out-of-scope table or repo doctrine — deferrals, not gaps):

- **The entire HTTP/API layer.** Graph CRUD (`Graph.from_map!` → `save_graph`), `start_run`, `inspect_run`/`list_runs`/`list_events`, `cancel_run`, `resolve_interrupt`, and the dispatch seam itself — either executor-facing claim/complete endpoints (pull) or a push dispatcher like sketch I.a. Roughly 8–10 endpoints plus auth, JSON mapping, and a long-poll or webhook bridge for the wake hint (which is an Elixir message, so exposing "wake" over HTTP means the service implements long-polling itself). Realistic size: a small Phoenix/Plug app, ~800–1500 lines. This is the doc's explicit "hosted control plane" boundary; every standalone customer writes the same app, and that is the stated business wedge.
- **Migrations by hand-pinned snippet.** `mix docket.gen.migration` is fresh-install-only; every upgrade is a copy-pasted `Migration.up(version: n)` from release notes. Doctrine, documented in the doc's Migration section.
- **Output extraction** for the "get result" endpoint — hand-rolled channel reading; the out-of-scope table defers it explicitly (prior finding 11).
- **Multi-instance wake** — poll fallback until `pg_notify` ships; the recommended shape is documented in the doc, which is exactly what a deferral should look like.

**Docket-specific and *not* deliberate** (the actual gaps for this persona): no non-destructive read over the claim index for their ops dashboard (finding 5), the tenancy hole if they run `tenant_mode: :required` (finding 4), and the unspecified subscribe contract their long-poll bridge sits on (finding 7).

**One genuine unlock worth naming:** because a detached node has **no module** and `config_schema` is inline, a graph consisting of detached nodes is authorable entirely from a JSON document — `Graph.from_map!` with an empty implementations registry. Declared detached nodes make a generic, module-free graph service *possible* for the first time; under the spike contract this persona was structurally impossible without shipping custom BEAM code per node type. The doc never points this out, and it is its best standalone selling point.

### I.c Python executor fleet

The seam is Elixir; the team builds a thin bridge. What crosses the wire:

**Elixir → Python (work order), one JSON POST** — every field verbatim from `%Docket.DetachedTask{}`:

```json
{
  "task": {
    "node_id": "summarize", "attempt": 2, "max_attempts": 3,
    "prior_failures": [{"attempt": 1, "reason": "model timeout"}],
    "config": {"endpoint": "summarize", "style": "terse"},
    "snapshot": {"ticket": {"subject": "…"}, "classification": "billing"},
    "idempotency_key": "opaque-string",
    "scheduled_at": "2026-08-01T12:00:00Z",
    "claimed_at":   "2026-08-01T12:00:41Z",
    "deadline_at":  "2026-08-01T12:10:41Z"
  },
  "completion": {
    "url": "https://host.internal/docket/complete",
    "ref": {"run_id": "…", "task_id": "…", "attempt": 2, "token": "base64url-32-bytes"}
  }
}
```

**What the Python side consumes:** `config` (what to do), `snapshot` (the data — its size is governed by the graph's edges per the doc's egress caution), `attempt`/`max_attempts` (`attempt == max_attempts` → last try, choose the cheap/safe strategy — reading, not deriving), `prior_failures` (why it is being retried), `deadline_at` (its own timeout budget: `deadline - now - margin`), `idempotency_key` (the ledger key for external effects), and `completion.ref` as an **opaque dict**.

**Python → Elixir (completion):**

```python
def complete(ref, ok=None, error=None, permanent=False):
    body = {"ref": ref}                    # echoed verbatim, all four fields
    body["result"] = {"ok": ok} if error is None \
        else {"error": error, "permanent": permanent}
    r = requests.post(COMPLETE_URL, json=body, timeout=10)
    out = r.json()
    if out.get("applied"):            return "landed"
    if out.get("reason") == "terminal":   return "run dead; stop"
    if out.get("reason") == "superseded": return "??? see finding 1"
    if r.status_code == 403:          raise CompletionForged(out)   # unauthorized_completion
```

**Elixir bridge inbound endpoint** (~25 lines):

```elixir
def complete(conn, %{"ref" => r, "result" => result}) do
  ref = %{run_id: r["run_id"], task_id: r["task_id"], attempt: r["attempt"], token: r["token"]}
  {res, opts} =
    case result do
      %{"ok" => outputs} -> {{:ok, outputs}, []}
      %{"error" => reason, "permanent" => true} -> {{:error, reason}, [permanent: true]}
      %{"error" => reason} -> {{:error, reason}, []}
    end
  case Docket.complete_detached(MyApp.Docket, ref, res, opts) do
    {:applied, _run} -> json(conn, %{applied: true})
    {:unchanged, reason, _run} -> json(conn, %{applied: false, reason: reason})
    # NB: never serialize the returned run to the caller — see finding 11
    {:error, %Docket.Error{type: :unauthorized_completion}} -> send_resp(conn, 403, "forged")
    {:error, err} -> send_resp(conn, 422, inspect(err))
  end
end
```

**Echo-only identity, verified against the sketch:** the Python code never concatenates `run_id:step:node_id`, never increments `attempt`, never parses `task_id`, never computes a deadline, never derives a token. All four ref fields arrive as data and return as data. The claim holds — I could not find a single place the sketch is forced to construct an identity, and (deliberately) no documented format exists to construct one from. The bridge total is ~120 lines of Elixir plus ~40 of Python. Under the spike, the same bridge re-implemented two private string formats and shipped an unauthenticated mutation path.

**What the doc does not tell this persona:** the meaning of `"superseded"` after a completion-POST retry (finding 1), whether an old-token completion sees `superseded` or `403` (finding 2), and — for the webhook-shaped ("different connection") variant the doc itself advertises — that the token **must egress with the work order**, because the claim is its only source and a dispatcher holding it in memory loses it on restart (finding 9).

---

## Part II — findings

### 1. `:superseded` cannot answer the question the doc says it answers. — MAJOR

The doc states the unchanged reason "answers the one question an executor acts on: *did my work land, and if not, is anyone going to retry it?*" — and then defines `:superseded` as covering "duplicate completion, post-redelivery completion, **or the task already completed**."

Those are opposite answers to that exact question:

- **Duplicate of my own success** (the single most common case: my completion POST timed out after applying, and my HTTP client retried) → my work **landed**, nobody retries.
- **Post-redelivery** (my lease expired before I finished) → my work **did not land**, attempt+1 is parked, and **someone will re-execute it** — bill twice, side effects repeat, dedupe ledger matters.

Both return `{:unchanged, :superseded, run}`. The executor performing the industry-standard retry-on-timeout of its completion call lands precisely in this ambiguity and cannot distinguish "done, relax" from "discarded, the work is being redone". This is the residue of prior finding #5 — much smaller than before (the type-level split is real and good), but it survives in the corner where real network behavior concentrates, and the doc's own framing sentence promises it away. Contract defect: the vocabulary contradicts its stated purpose.

**Cheap fix with real teeth:** split on what the fence can actually see at zero storage cost — `:superseded` (a **newer attempt of this task is currently parked**: your work did not land and will be redone) vs `:settled` (**no attempt of this task is parked**: this task will not run again — your duplicate almost certainly landed). That distinction is computable from the run document at fence time, requires no memory of applied completions, and answers the doc's own question in every case that matters.

### 2. The fence's evaluation order is invoked three times and specified zero times. — MAJOR

What does a completion bearing a **rotated (old) token** get after redelivery? The doc answers three different ways:

- **Tokens / Rotation:** "reports as **unauthorized or unchanged** per the fence order below" — explicitly defers to a fence order.
- **Completion / unchanged reasons:** `:superseded` explicitly includes "post-redelivery completion" — so, unchanged.
- **Error vocabulary:** `:unauthorized_completion` is "the referenced task is **currently parked** but the token hash does not match" — and after redelivery the task *is* currently parked (at attempt+1, with a new hash the old token fails against), so this definition captures the same case.

The promised "fence order below" never appears. Whether the fence checks *attempt-match before token-verify* (→ `:superseded`) or *token-against-currently-parked-hash first* (→ `:unauthorized_completion`) is exactly the difference between an executor logging "I was redelivered, normal" and paging security about forged completions. Self-contradiction in the contract; the conformance suite ("rotated token → fenced") is too vague to pin it either.

**Fix:** one normative paragraph: *terminal run → `:terminal`; task not parked, or parked at a different attempt than the ref → `:superseded`* (or `:settled` per finding 1); *parked at exactly the ref's attempt but token hash mismatch → `:unauthorized_completion`.* Under that order `:unauthorized_completion` means precisely "right attempt, wrong secret" — always a bug or an attack, never a race — which is the alarm semantics an operator wants.

### 3. A claimed task cannot be released; dispatch failure spends the wrong budget. — MAJOR

The claim commit starts the start-to-close clock, and the doc's guidance is "claim what you can dispatch now." But dispatch can fail *after* the claim — connection refused, pool exhausted, executor deploy in progress. At that moment the claimant holds a claimed task, zero work has happened, and its only two moves are:

- `complete_detached(task, {:error, "dispatch failed"})` — **burns one attempt of the node's retry budget** on a transport hiccup. With `max_attempts: 3`, three connection refusals during an executor deploy permanently fail the node (and the run), even though the work was never attempted once.
- Do nothing — the task is stranded **claimed for the full `start_to_close_ms`** (the doc's instance default: five minutes; my sketch: ten) before redelivery, plus backoff.

Neither is "return it to pending, untouched" — the semantics every queue system exposes as nack/release/visibility-timeout-zero, and the obviously correct disposition for "I could not hand this off." The out-of-scope table names deadline *extension* as a deliberate deferral; **release is not in the table**, so by this project's own rules it is a silent gap, not a deferral. It would be one more mutation through the same fenced signal path (re-park same attempt, clear the token hash, no budget consumed) — exactly the shape the doc says makes seams cheap.

Minimum acceptable v1: add release to the out-of-scope seam table with a named disposition, and document which of the two bad options integrators should prefer meanwhile. Better: ship it; it is the same fence the claim mutation already runs in reverse.

### 4. The cross-tenant dispatcher the doc proposes cannot complete what it claims. — MAJOR

The claim section proposes `:scope` be optional even under `tenant_mode: :required`, because "the expected claimant is a platform-level dispatcher serving every tenant." But under `:required`, every public durable call must resolve an explicit `{:tenant, id}` (`lib/docket.ex` moduledoc; "tenant scope is enforced before storage access"), `complete_detached` included. The claim-payload table carries **no tenant/scope field** — while the proposed projection table explicitly has `tenant_id`/`scope_key` columns. So the flagship consumer claims a task cross-scope, receives a payload with no scope in it, and then has nothing to pass to `complete_detached`. Either completion silently bypasses tenant resolution (contradicting the instance's stated tenancy contract) or the design as written deadlocks its own primary use case. WaterCooler — named in the doc as the first claimant — is multi-tenant and hits this on day one.

**Fix:** one row in the payload table (`scope`/`tenant_id`, echoed like everything else), plus a sentence stating how `complete_detached` resolves scope for an echo-only ref. Trivial to fix; a contract defect until fixed.

### 5. The recoverability guarantee rests on a read that does not exist. — MAJOR

Settled decision 7: "Every parked task is **visible as pending work in the claim index**." The operator playbook: alarm on `:schedule_to_start_expired`, and for abandoned unbounded work "the operator's escape hatch is `cancel_run`." Walk that playbook as an operator:

- *Which runs are waiting on detached work, and for how long?* `list_runs` still cannot say — the run stays `:running` while detached tasks are outstanding (lifecycle section), identical to actively-executing runs, exactly as in prior finding #1.
- *The claim index has the rows* — but the only public operation over it is `claim_detached`, which is **pop-next and mutating**: reading it starts someone's start-to-close clock. There is no non-destructive read. The remaining option is SQL against `docket_detached_tasks`, an internal projection whose name is a "working name" in a proposal section.

So "visible in the claim index" means visible to *claimants*, not to the operator the same paragraph hands the escape hatch to. Prior finding #1 is thereby half-closed: dispatch discovery is fully solved (pop-next, indexed, fence-backed — genuinely excellent), observability discovery is still zero API. The out-of-scope table does not defer it, so by project rules: gap. A `list_detached_tasks(runtime, opts)` read (filters: state, node_ids, scope, age; a SELECT over the table this design already builds and indexes) closes it with no new storage and no scope creep — parity with what the design's own text asserts already exists.

### 6. `outputs` is never defined, and the only declaration example writes no state. — MINOR

`complete_detached`'s `result` is `{:ok, outputs}`; "outputs" appears nowhere else in the doc. From module-node semantics (and the spike's `validate_detached_write`) one infers: a map of state-field writes, durable-normalized, validated against the graph's field schemas and the node's write contract. A cold integrator cannot infer which fields a *detached* node may write, because the doc's declaration example declares config and policies but no fields and no output mapping — the one thing every real detached node needs. Also unspecified: what an invalid `outputs` returns (`{:error, :invalid_input}`? applied-then-failed? does it burn the attempt?). That last question matters operationally: an executor that returns a misshapen map needs to know whether it just consumed retry budget.

**Fix:** define `outputs` in the Completion section (shape, validation, failure mode and its budget effect), and make the Declaration example write a field.

### 7. `subscribe_detached` is a signature, not a contract. — MINOR

Missing: the wake message's term shape (I cannot write `handle_info/2` from this doc — see the guess in sketch I.a); the accepted `opts` (node filter? scope?); the return value; unsubscribe/process-death semantics; and whether a *single-VM* claimant needs the poll fallback. On that last point the doc leans two ways: "a claimant must tolerate spurious and missed wakes" (always poll) vs "poll fallback ... is the documented pattern for **multi-VM Postgres deployments**" (in-VM is reliable?). Since subscription is process-based, a dispatcher restart always opens a missed-wake window even in-VM, so the honest answer is "always poll (or always drain on init)" — the doc should just say so, plus the drain-on-startup pattern sketch I.a had to invent. All wording; the design itself is fine.

### 8. `on_deadline` has no default; the optionality of the `"detach"` block is unstated. — MINOR

The deadline table gives defaults for both deadlines (`nil`; instance 300_000 ms) but none for `on_deadline`. Is `implementation: :detached` with no `"detach"` policy block at all legal — start-to-close from the instance default, schedule-to-start unbounded, and `on_deadline` … reschedule? fail? Reschedule-vs-fail is the difference between a transient executor outage self-healing and permanently failing runs, so the default is a real semantic, not a formality. One table cell fixes it (and the compiler slice cannot be built without deciding it anyway).

### 9. Token custody for the "different connection" route is undocumented. — MINOR (trap)

Mint-at-claim means the claim response is the token's only source, ever. For the synchronous push dispatcher (claim → HTTP → complete with the response) custody is trivial. But the doc's third motivating route is exactly the asynchronous one — "the result naturally arrives somewhere other than the request connection (webhook, callback queue)". There, the completion arrives at *some* host instance, possibly after the claiming dispatcher restarted; a dispatcher that kept the token in memory has stranded the task until start-to-close expiry and forced a re-execution. The correct pattern — **the token travels with the work order and is echoed back in the callback** (as sketch I.c does) — is never stated, and "the completion secret ... stored nowhere readable" actively suggests treating it as too hot to forward. Integrators who guess wrong lose completions across every deploy. One paragraph ("the token is a capability for the executor, not a secret of the dispatcher; ship it with the work") closes the trap.

### 10. The prefetch trap reappears one hop downstream, while following the rule. — MINOR (trap)

"Claim what you can dispatch now" frames the trap as the claimant's *local queue*. My sketch obeys the letter — every claimed task is dispatched immediately via `Task.Supervisor.start_child` — and still recreates the trap: with a saturated executor fleet or a bounded HTTP pool, the drain loop happily claims 200 tasks whose requests then sit in the pool queue burning their start-to-close budgets. "Dispatch" is not "start executing." The guidance should say: **bound in-flight work and claim only when completion capacity is free** (drain until `:empty` *or* until `in_flight == max`, resume on task completion). Guide-level wording (slice 5), but it is the first thing a load test finds, and the doc's current phrasing certifies the broken version.

### 11. Completion returns hand the full run document to the caller; the egress caution stops at the snapshot. — MINOR

`{:applied, run}`, `{:unchanged, _, run}` — every completion path returns the whole run (channels included). In-VM this is fine (any process with the runtime handle can `fetch_run` anyway). But the doc's Egress caution covers only the claim snapshot, while the natural bridge (sketch I.c) sits on the completion path: a bridge that serializes the return into its HTTP response leaks full run state to executors — including to a caller whose token *failed* nothing (a probe with a garbage token against a settled task gets `{:unchanged, :superseded, run}`, no verification possible, run attached). The token authenticates mutation, not the read that rides along with rejection. Fix is one sentence extending the egress caution: completion returns are for the claimant, never for the wire.

### 12. The work order is complete. — strength

Config with materialized defaults, snapshot at planned versions, `attempt`/`max_attempts`/`prior_failures`, both timestamps and the live deadline, opaque dedupe identities, token: the executor needs **no second read** — prior findings #2 and #8 are not just closed but closed with their finding numbers cited in the payload table, which is the right way to consume a friction report. `deadline_at` in the payload enables the self-imposed timeout margin (sketch I.a `budget_ms/1`) that prior integrators could not build cleanly.

### 13. Mint-at-claim structurally deletes four prior failure classes. — strength

Forgery (#4): completion requires a 32-byte random secret learnable only via claim, constant-time compared inside the same pure fence — the prior report's "FORGED BY A STRANGER" write is now `:unauthorized_completion` with durable state untouched. Premature completion (#7's race): unrepresentable — no claim before park commit, no token before claim. Identity re-derivation (#3): unrepresentable — echo-only, no public format. Duplicate execution by racing pollers (#10): unrepresentable — claim exclusivity at the fence, loser pops next. These are deletions of bug classes, not mitigations, and the doc correctly refuses a separate authorization round-trip by putting verification inside the existing commit machinery.

### 14. Declared detached nodes delete the stub module — and quietly unlock module-free graphs. — strength

Prior finding #6 (deploy an Elixir module whose body announces it is not the implementation) is gone at the root: no module, no `call/3`, park at plan time, customer code never runs on the host for the node. Inline `config_schema` moves the executor's contract out of compiled Elixir. Consequence the doc undersells: a detached-node graph is authorable entirely from JSON (`Graph.from_map!` with no implementations registry), which is the load-bearing enabler for the standalone persona and for any graph-builder UI.

### 15. The projection is owned by the write path, and the drain loop's trap is documented at the API. — strength

`docket_detached_tasks` maintained inside the same commit transaction as the run mutation — the index every prior integrator rebuilt by hand (with drift risk) now cannot drift by construction. Pop-next via partial index + `SKIP LOCKED`, selection-is-a-hint/fence-is-truth matching the existing signal machinery, no batch claim with the reasoning written down, and the prefetch warning placed directly on the claim API (modulo finding 10's downstream blind spot). The pg_notify seam is documented at exactly the right depth for a deferral: shape, transactional semantics, pooling caveat, unchanged subscriber contract.

---

## Part III — protocol walkthrough

Each scenario: what the doc says, and whether the specified outcome is complete and unambiguous.

| scenario | doc-specified outcome | complete? | gap |
|---|---|---|---|
| Happy path | plan-time park → wake hint → claim mints token, sets deadline → `complete_detached({:ok, outputs})` → `{:applied, run}`, write applied at next barrier | yes, except `outputs` undefined | F6 |
| Duplicate completion | task no longer parked → `{:unchanged, :superseded, run}` | specified but conflates "landed" with "will be redone"; full run returned to caller | F1, F11 |
| Completion after redelivery (old token) | Tokens: "unauthorized **or** unchanged per the fence order below"; Completion: `:superseded`; Error vocab definition matches `:unauthorized_completion` | **contradictory** — fence order never given | F2 |
| Completion after cancel | index rows removed in the terminal commit; `{:unchanged, :terminal, run}` | yes — clean, and tells the executor to stop | — |
| Two claimants race one task | `SKIP LOCKED` hint + fenced claim mutation; loser's re-verify fails, pops next or `:empty`; replay mints fresh token, only committed one exists | yes — fully specified | — |
| Forged completion (guessed identity, no claim) | parked at ref attempt → constant-time hash mismatch → `%Docket.Error{type: :unauthorized_completion}`; state untouched | yes for parked tasks; a settled-task probe gets `{:unchanged, …, run}` with no verification possible | F11 |
| Schedule-to-start expiry | non-retryable `:schedule_to_start_expired`, retry budget untouched, distinct alarmable code | yes (run-level consequence — v1 permanent node failure fails the run — left to module-node inference; acceptable) | — |
| Unbounded default (no schedule-to-start) | waits forever; no `wake_at` contribution; claimable and cancellable; pruner never touches it | semantics complete; the *operator discovery* of such runs has no API | F5 |
| Start-to-close expiry, `on_deadline: "reschedule"` | redelivery: attempt+1, new park, new token, retry `backoff_ms` between expiry and re-park; budget exhaustion → node failed | yes | default `on_deadline` unspecified — F8 |
| Start-to-close expiry, `on_deadline: "fail"` | node failed | yes | F8 |
| Reported failure, no `permanent` | burns the attempt, re-parks pending attempt+1 under retry policy with backoff, or fails node on exhaustion | yes (return shape of a failure-report completion — presumably `{:applied, run}` — unstated but low-risk) | — |
| Reported failure, `permanent: true` | fails the node immediately; permanent can only shrink the budget | yes | — |
| Retry-budget exhaustion | node failed; terminal `Failure.details` carries **every** attempt's recorded reason | yes — prior #8 explicitly closed | — |
| Host restart, tasks pending | rows + run doc durable; bounded pending contribute `wake_at`; unbounded rely on index visibility | yes | F5 (visibility read) |
| Host restart, tasks claimed | absolute `deadline_at` keeps ticking; completions from token-holders land through the fence after restart; subscriptions (process-based) lost | yes, but drain-on-start is implied never stated; in-memory token custody loses async completions | F7, F9 |
| Executor crash mid-work | start-to-close expiry → disposition per `on_deadline`; late completion fenced stale | yes | F2 (which fence answer) |
| Drain loop / prefetch | claim-until-`:empty`, dispatch as you go; prefetching spends execution budget on the local queue; batch claim a named seam | documented — but "dispatch now" ≠ "executing now" | F10 |
| Claimed but cannot dispatch | *(not addressed anywhere)* | **no** — burn budget or strand for start-to-close | F3 |
| Cross-tenant claim → complete (`tenant_mode: :required`) | claim `:scope` optional (proposal); completion scope resolution unspecified; payload carries no scope | **no** | F4 |

---

## Part IV — prior friction report checklist (11 findings)

| # | prior finding (severity) | status under this design |
|---|---|---|
| 1 | No discovery/index for detached work (MAJOR) | **Partially closed.** Dispatch discovery fully solved (claim API + owned projection + wake hint). Non-destructive/operator listing still absent and not deferred in the out-of-scope table → residue is finding 5. |
| 2 | Node config needs a second fetch (MINOR) | **Closed.** `config` in the payload, defaults materialized; finding cited in the doc. |
| 3 | Hand-assembled `%Docket.Detached{}` / private formats (MAJOR) | **Closed structurally.** Echo-only ref; no public format; derivation impossible. |
| 4 | No authorization; forgery demonstrated (MAJOR) | **Closed.** Mint-at-claim, hash-only storage, constant-time verify in the fence, `:unauthorized_completion`. Only residue: fence-order ambiguity (finding 2). |
| 5 | Applied vs silently-ignored indistinguishable (MAJOR) | **Closed at the type level** (`{:applied,…}` vs `{:unchanged, reason,…}` cannot be pattern-matched into each other). Reason vocabulary reopens one corner (finding 1). |
| 6 | Elixir stub module required for Python work (MAJOR) | **Closed.** Declared detached nodes; no module, no host execution, inline `config_schema`. |
| 7 | `:detach_pending` push race — the strength | **Superseded upward.** The race is unrepresentable (token requires claim, claim requires committed park); the error type never ships and the doc says so explicitly. The typed-outcome standard the strength set is now the whole completion surface. |
| 8 | Retry budget invisible; terminal reason lost (MINOR) | **Closed.** `max_attempts`/`prior_failures` in the payload; `Failure.details` keeps every attempt's reason. |
| 9 | Deadlines not extendable, not per-run (MAJOR) | **Deliberately deferred (extension/keepalive)** — named in the out-of-scope table with its seam (one more mutation through the signal path). Split deadlines + `on_deadline` remove part of the pain (queue wait no longer eats execution budget). **Per-run deadline variance is neither solved nor named as a seam** — deadlines remain graph-version-fixed; worst-case sizing per node persists. Deferral for half, silent for the other half. |
| 10 | Concurrent pollers duplicate work; cancellation invisible (MAJOR) | **Duplicate work: closed** (claim exclusivity by construction). **Cancellation propagation: deliberately deferred** — out-of-scope table row, seam named (claim-payload validity affordance), backed by settled decision 7. |
| 11 | No public accessor for channel outputs (MINOR) | **Deliberately deferred.** Explicit out-of-scope row ("independent read-surface work; noted, not expanded here"). |

Score: 6 closed, 1 superseded-as-strength, 1 partially closed (residue = finding 5), 2 deliberately deferred, 1 half-deferred/half-silent (per-run deadlines). For a redesign responding to a friction report, that is an unusually honest hit rate — and the doc cites finding numbers at the exact payload fields and returns that close them.

---

## Part V — ergonomics verdict

### Embedded Elixir host

**Pleasant.** The whole integration is one small GenServer against three functions; the work order needs no second read; failure vs permanent maps naturally onto HTTP status classes; typed unchanged returns mean my logs can finally say "discarded" instead of lying "ok". Plan-time parking means no customer code path to babysit.

**Awkward.** I wrote `handle_info` against a guessed message shape (F7). I had no correct move when dispatch failed after claim (F3) — that decision is *the* operational policy of a dispatcher and the doc is silent. The `:error`-completion path makes transport failures and execution failures spend the same retry budget, so my node's `max_attempts` must be sized for the sum of two unrelated failure processes.

**Must build that the library should arguably provide.** Nothing structural — a first for this feature. The poll fallback, in-flight bounding, and dispatch-failure policy are all application code by design; what is missing is the *documentation* that forces me to build them correctly.

**Traps.** Dedup on `task_id` instead of `idempotency_key` (adjacent fields, opposite retry semantics — the doc already plans this for the guide, good); treating wake hints as reliable and skipping the init drain; async-dispatching everything and recreating prefetch downstream (F10); completing `{:error,…}` for transport failures until the budget dies (F3).

### Standalone Elixir service

**Pleasant.** Module-free graphs make the service genuinely generic — graph CRUD from JSON with zero custom BEAM code, impossible under the spike. The backend/migration story is small and Oban-familiar.

**Awkward.** They are building a product-sized API layer (~8–10 endpoints, auth, long-poll wake bridge) — deliberate and honestly labeled, but a prospective customer reading this doc should be told plainly that "transport-agnostic core" means *you are the transport*. Hand-pinned migration snippets and the missing outputs accessor add papercuts (both doctrine-backed deferrals).

**Traps.** Serializing completion/`unchanged` returns into HTTP responses (F11); running `tenant_mode: :required` and discovering the completion-scope hole (F4); building their ops dashboard and discovering the claim index has no read (F5).

### Python executor fleet

**Pleasant.** Echo-only identity actually survives contact with a sketch — the Python side is ~40 lines and touches no Docket concept beyond field names. `attempt == max_attempts` last-try logic, `prior_failures` for context, `deadline_at` for self-timeouts, `permanent` for "stop retrying malformed input": the executor-side vocabulary is complete and maps onto how agent/LLM workers actually behave.

**Awkward.** The completion-retry ambiguity (F1) forces every careful executor into "maybe landed, maybe redone" bookkeeping — precisely the uncertainty the typed returns were built to remove. Whether a late completion sees `superseded` or `403` (F2) decides their alerting rules, and today the doc supports both readings.

**Traps.** Retrying the completion POST and misreading `:superseded`; treating `:unauthorized_completion` as retryable (it never is); the bridge forgetting to ship the token with webhook-shaped work orders (F9).

### The two changes with the best pain-reduced-per-scope-added ratio

1. **Specify the completion fence normatively and split the unchanged vocabulary** (findings 1 + 2): one paragraph fixing the evaluation order (terminal → wrong/absent attempt → token mismatch) and one reason split (`:superseded` = newer attempt parked, will be redone; `:settled` = no attempt parked, nothing further happens). Zero storage, zero new API, pure spec text — and it eliminates both executor-facing ambiguities, the doc's only internal contradiction, and the completion-retry trap every HTTP integrator hits in week one.
2. **`list_detached_tasks/2`** (finding 5): a non-destructive filtered read (state, node_ids, scope, age) over the projection table this design already creates and indexes. One SELECT plus conformance cases; no new tables; parity with what settled decision 7 already asserts ("every parked task is visible"). It finishes prior finding #1, makes the doc's own operator playbook executable, and gives the standalone persona its dashboard endpoint for free.

Runner-up, named because its absence is a silent gap rather than a deferral: **claim release** (finding 3) — at minimum, a row in the out-of-scope seam table; ideally the one extra fenced mutation it obviously is.

---

## Severity summary

| # | Finding | Severity |
|---|---|---|
| 1 | `:superseded` conflates "your work landed" with "your work will be redone"; fails the doc's own stated purpose on the common completion-retry path | MAJOR |
| 2 | Completion fence evaluation order referenced ("per the fence order below") but never specified; three sections give conflicting answers for old-token post-redelivery completions | MAJOR |
| 3 | No way to release a claimed task; post-claim dispatch failure must burn retry budget or strand the task for the full start-to-close window; seam absent from the out-of-scope table | MAJOR |
| 4 | Cross-tenant claim proposed, but payload carries no scope and completion under `tenant_mode: :required` is unspecified — the doc's flagship consumer cannot complete what it claims | MAJOR |
| 5 | "Visible as pending work in the claim index" has no non-destructive read; operator playbook (alarm, `cancel_run`) not executable via public API; residue of prior #1 | MAJOR |
| 6 | `outputs` undefined (shape, validation, budget effect of invalid results); declaration example writes no state | MINOR |
| 7 | `subscribe_detached` underspecified: message shape, opts, lifecycle, in-VM poll-fallback question, drain-on-start pattern | MINOR |
| 8 | No default for `on_deadline`; optionality of the `"detach"` policy block unstated | MINOR |
| 9 | Token custody pattern for the async/"different connection" route undocumented; in-memory custody loses completions across restarts | MINOR |
| 10 | "Claim what you can dispatch now" certifies async dispatch that recreates the prefetch trap downstream; needs "bound in-flight, claim on capacity" | MINOR |
| 11 | Completion and unchanged returns carry the full run; egress caution covers only the claim snapshot; settled-task probes receive run state without any token check possible | MINOR |
| 12 | Work-order payload completeness — no second read; prior #2/#8 closed with citations | (strength) |
| 13 | Mint-at-claim structurally deletes forgery, premature-completion race, identity derivation, and duplicate execution (prior #3/#4/#7/#10) | (strength) |
| 14 | Declared detached nodes delete the stub module (prior #6) and unlock module-free JSON-authored graphs — the standalone persona's key enabler | (strength) |
| 15 | Projection owned by the run write path (drift impossible), SKIP-LOCKED pop-next, drain-loop trap documented at the API, pg_notify seam documented at deferral-quality | (strength) |

**No BLOCKERs.** All five MAJORs are spec defects or unnamed gaps, not architecture problems — four of the five are fixable with paragraphs and payload fields, and the fifth (the index read) is a SELECT over a table the design already builds. The settled core — declared nodes, leased pull, mint-at-claim, split deadlines, echo-only identity — held up under paper dogfooding better than any prior generation of this feature: I could not construct a scenario where the *durable* behavior is wrong, only scenarios where the contract does not say which right thing happens, or gives the integrator no correct move. Fix findings 1–5 in doc review and the implementation slices can proceed against an unambiguous contract.
