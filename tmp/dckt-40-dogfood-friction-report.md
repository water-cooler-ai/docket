# Docket detach protocol — integration friction report

**Evaluated:** `suttonmay5/dckt-40` @ `b500a80` (PR #93), Postgres backend, Elixir 1.18.4 / OTP 27, PostgreSQL 15.1.
**Scenario:** an external Python service polls an Elixir host for parked detached node work, executes it, and posts completions back — the "agent dispatch" customer.
**Rule observed:** no file inside the docket worktree was modified. Everything below was built on the public API only.

**What worked end to end.** A Python process with nothing but `urllib` started a run, discovered the parked detached task, executed the node's work, and completed it; the run advanced through `finalize` to `:done` with `summary = "finalized: HELLO DOCKET"`. Deadline expiry, retry redelivery, reported failure, and the `:detach_pending` race all behaved exactly as the docs describe. The durable core of this feature is sound. The friction is entirely in the **operational surface around it** — discovery, identity, authorization, and completion feedback.

---

## 1. There is no way to find detached work. `list_runs` cannot express the query. — MAJOR

**Trying to do:** implement `GET /tasks`: "give me every detached task waiting for an executor."

**What the API offered:** nothing that names detached work.

- A detach-parked run's status is `:running` (`RunMutation.complete_detached/6` and `loop.ex` both keep `status: :running`) — byte-identical to a run actively executing on a node right now. `Docket.Run.durable_status/0` is `:running | :waiting | :done | :failed | :cancelled`; detachment has no status of its own. Contrast with interrupts, which get `:waiting` and are therefore trivially listable.
- `list_runs/2` filters on `:status`, `:graph_id`, `:graph_hash` only (`lib/docket.ex:348-360`). None of them select detached runs.
- `Docket.RunSummary` — what `list_runs` returns — carries no `active_tasks`, so a summary can never answer "is this one detached?"
- The one authoritative signal, the `:node_detached` event, is only reachable through `list_events(runtime, run_id, opts)`, which **requires a run_id you already have**. There is no cross-run event feed to tail.
- `inspect_run` sets `wake_at` to the detach deadline, but a retry-backoff park sets `wake_at` too — it does not discriminate.

**What I had to build:** a full scan, once per poll:

```
list_runs(status: :running, limit: 1000)   # 1 query, capped at 1000
  -> fetch_run(id) for EVERY returned run  # N queries, full run documents
  -> scan run.active_tasks for %TaskState{status: :detached}
```

Measured on this machine (`agent/loadtest.py`, median of 5):

| running runs | `GET /tasks` |
|---|---|
| 1 | 3.8 ms |
| 26 | 20.5 ms |
| 51 | 34.0 ms |
| 101 | 43.2 ms |

~0.4 ms per run scanned, linear. At 10k concurrently running runs that is a ~4 s poll, **paid by every polling agent, every interval**, and it decodes every run's full channel state to read one map key. `:limit` maxes at 1000, so past that you also cursor-paginate on every poll. Worse, the cost scales with *running* runs, not *detached* ones: a deployment with 10k busy runs and 3 detached tasks pays the full 10k scan.

**The actual workaround a real integrator must adopt:** register a `checkpoint_observer` in the host, watch for `park_reason == "awaiting_detached"` / `:node_detached` events, and maintain your own `detached_tasks` index table. That works — the checkpoint carries everything needed — but it means **every customer of this feature rebuilds the same index over data Docket already has durably**, and their index can drift from `active_tasks` (it is a projection, not the source of truth). Docket owns the write path and could index this in `docket_runs` for free.

**What would fix it:** either a `:detached` durable status, or a `list_detached_tasks(runtime, opts)` read backed by an index on the runs table.

---

## 2. The task payload is *almost* right, but node config takes a second round trip. — MINOR

**Trying to do:** hand the Python executor everything it needs to do the work.

**What the API offered:** `%Docket.Run.TaskState{}` is genuinely well-designed for this. It carries `task_id`, `node_id`, `step`, `attempt`, `idempotency_key`, `snapshot` (the exact committed state the attempt was planned against), `deadline_at`, `started_at`, `failures`, and `metadata["detach_token"]`. That is nearly a complete work order.

**What is missing:** the node's **config**. `TaskState` has no config field, so `GET /tasks` has to do:

```elixir
fetch_graph(%GraphRef{graph_id: run.graph_id, graph_hash: run.graph_hash})
|> get_in([:nodes, node_id]).config
```

— a second read per distinct `graph_hash`, on a code path that is already 1+N. It is cacheable by `graph_hash` (and `Docket.Postgres.GraphCache` exists), so this is ergonomics, not a wall.

**Honest counterpoint:** the detach **token** is an adequate payload channel — the node can encode anything durable into it and Docket retains it verbatim on the task. I used it to ship `kind`/`instruction`/`dispatched_at` as JSON and it round-tripped perfectly. But `Docket.Node`'s docs frame the token as a *correlation* value ("retained on the parked task for correlation"), not as the executor's payload, and there is no guidance on size limits. Integrators will discover this use by accident. Say it explicitly in the docs, or add `config` to the parked task.

**One caution nobody flags:** `snapshot` is the *whole* committed state snapshot. Shipping it to an external executor is a data-egress decision, and there is no per-node projection to narrow it.

---

## 3. Building `%Docket.Detached{}` from outside means reimplementing a private string format. — MAJOR

**Trying to do:** turn an HTTP request body back into the completion identity.

**What the API offered:** `complete_detached/4` hard-requires a `%Docket.Detached{}` with all six `@enforce_keys`. There are exactly two constructors:

- `Docket.Detached.from_context/1` — takes the node's execution context. **Only callable inside the node body.** Useless to a poll-based dispatcher, which by definition acts after the node returned.
- `Docket.Detached.from_task/2` — takes `run_id` + the parked `%TaskState{}`. This is *precisely* the function an external dispatcher needs, and it is **`@doc false`** (`lib/docket/detached.ex:61-74`).

**What I had to fake:** hand-assemble the struct and re-derive two fields by string concatenation:

```elixir
%Docket.Detached{
  run_id: run_id, node_id: node_id, step: step, attempt: attempt,
  task_id: task_id,                       # "#{run_id}:#{step}:#{node_id}"
  idempotency_key: idempotency_key        # "#{task_id}:#{attempt}"
}
```

Docket's own Postgres backend test does the identical thing (`test/docket/postgres/backend_test.exs:330-382`, literally `idempotency_key: "#{task_id}:1"`). When the *library's own tests* hand-assemble a struct because no public constructor fits, that is the API telling you something.

**Why it matters beyond ergonomics:** `TaskState.task_id/3` and `idempotency_key/2` are public *functions*, but their **format is now load-bearing across a process and language boundary**. Every external completer in the world encodes `"{run}:{step}:{node}:{attempt}"` into its own code. Change that format and every integration breaks — silently, because a wrong `task_id` is a no-op, not an error (see #4). The struct is redundant anyway: five of its six fields are derivable from the other two.

**Fix:** make `from_task/2` public, and/or accept `complete_detached(runtime, run_id, task_id, attempt, result, opts)`.

---

## 4. Nothing authenticates a completion. I completed another agent's task with guessed fields. — MAJOR

**Trying to do:** work out what stops a bogus completer.

**What the API offered:** nothing. `complete_detached/4` fences on *identity* (`run_id`/`task_id`/`attempt` still detached) — which is a **correctness** fence, not an **authorization** fence. The `detach_token`, the one value that looks like a capability, is never consulted by `RunMutation.complete_detached/6`. It is retained purely for correlation.

**Demonstrated** (`agent.py forged`): a completer that had **never seen the token** built the identity purely from `(run_id, step, node_id, attempt)` — all of which are structured, low-entropy, and enumerable (`node_id` comes from the graph, `step` starts at 0, `attempt` starts at 1) — and won:

```
-> forged completion: HTTP 200 {'applied': True, 'run_status': 'running'}
   final: state:agent_result = 'FORGED BY A STRANGER'
          state:summary      = 'finalized: FORGED BY A STRANGER'
```

Arbitrary attacker-chosen data was written into durable run state and propagated downstream. The only entropy in the identity is `run_id`, so this is effectively "know the run id, own every detached node in it."

**What I had to build:** nothing yet — and that is the finding. For a real deployment the host must invent and enforce its own scheme (HMAC the identity, or treat the token as a bearer secret and verify it before calling `complete_detached`). Docket supplies neither, and the docs do not warn that the token is not a credential. Every integrator will assume the token is the credential, because that is what it looks like.

**Cheap fix with real teeth:** have `complete_detached` optionally verify the supplied token against `metadata["detach_token"]`. The value is already durable on the task; only the comparison is missing.

---

## 5. Applied and silently-ignored completions are indistinguishable. — MAJOR (worst practical issue)

**Trying to do:** tell the Python agent whether its result actually landed.

**What the API offered:** `{:ok, %Docket.Run{}}` — for both outcomes. Internally `RunMutation.complete_detached/6` carefully returns `{:unchanged, run}` for stale/duplicate/superseded completions and `{:ok, %Moment{}}` for applied ones. That distinction is then **flattened at the public boundary** by `finish_signal/2` (`lib/docket.ex:866-873`): the `Moment` clause and the bare `Run` clause both return `{:ok, run}`. The caller cannot tell them apart.

**Measured — every one of these returned HTTP 200 / `applied: true`, and every one was a no-op:**

| probe | returned | actually applied? |
|---|---|---|
| duplicate completion (`duplicate`) | `{:ok, run}` | no — first writer's `DUPE TEST` survived, `SECOND WINNER` discarded |
| wrong attempt +7 (`wrong-attempt`) | `{:ok, run}` | no — task still detached afterwards |
| completion after `cancel_run` (`after-cancel`) | `{:ok, run}` | no — run stayed `:cancelled` |
| loser of two concurrent agents (`concurrent`) | `{:ok, run}` | no — `AGENT-A RESULT` won, agent B discarded |

The *durable* behaviour is exactly right in all four cases — at-most-once application, correctly fenced. The problem is purely that **the agent is told it succeeded when its work was thrown away.**

Consequences an agent author cannot avoid:
- An agent that expensively computed a result (an LLM call, a paid API) cannot log, alarm on, or bill for the difference between "landed" and "discarded".
- A cancelled run is indistinguishable from a live one at completion time. My probe had to read `run_status: 'cancelled'` out of the returned run and *infer* — and that heuristic is wrong the moment a run is cancelled and a *later* superstep re-detaches.
- The only reliable check is to snapshot `checkpoint_seq` before and re-read after, or re-`inspect_run` and confirm the task is gone. Both are races and both double the query cost.

**Fix:** return `{:ok, :applied, run}` / `{:ok, :unchanged, run}`, or `{:error, %Docket.Error{type: :detach_stale}}`. The information exists one function-call below the surface and is deliberately discarded.

---

## 6. The work is in Python, but I still had to write and deploy an Elixir module. — MAJOR

**Trying to do:** make a node whose implementation lives in Python.

**What the API offered:** only a node return value can detach a run. `Docket.Node.call/3` must be implemented by a BEAM module, so there must be an Elixir module for work that contains no Elixir.

**What I had to fake** — `DogfoodHost.Nodes.AgentWork` in its entirety does nothing but announce that it is not the implementation:

```elixir
def call(state, config, context) do
  identity = Docket.Detached.from_context(context)
  if Map.get(config, "dispatch") == "push", do: push(identity, state, config)
  {:detach, Jason.encode!(%{...})}
end
```

**How it feels:** worse than it sounds, for three compounding reasons.

1. **Deploy coupling.** Adding a Python-side capability means editing Elixir, recompiling, and redeploying the host — even though no Elixir behaviour changed. Two teams and two release trains are now joined at a stub.
2. **The config schema is declared in the wrong language.** `config_schema/0` is the node's contract, and it validates the config the *Python* code consumes. I had to add `"dispatch" => Docket.Schema.string()` to an Elixir schema so a Python branch could read it. The schema of the Python executor's input lives in Elixir, permanently.
3. **`from_context/1` is a dead end for pull dispatch.** The docs say to build the identity in the node and "give that system the identity". For a *polling* agent there is nowhere to put it — the node returns and the identity is gone; the parked task keeps only the token. So `from_context/1` is only usable for **push** dispatch (#7). A poll-based integrator calls it, has nothing to do with the result, and deletes the line. That is a genuinely misleading API affordance for the most natural dispatch topology.

I do not think this is fixable without a "detached node type" declared in the graph with no module behind it. It is worth naming as the central ergonomic cost of using detach for cross-language dispatch, because it is the first thing every such integrator hits.

---

## 7. Push dispatch and `:detach_pending` — this part is genuinely excellent. — no finding

The one place the design anticipated the external case, it nailed it. Push dispatch (identity POSTed to Python from inside the node body) necessarily races the park commit. Observed (`agent/push_agent.py`):

```
completion try 1: HTTP 409 {'retryable': True,
  'error': 'task "run_...:0:agent_work" is not detached yet; retry after the detach park commits'}
completion try 2: HTTP 200 {'applied': True}
final: status=done  state:agent_result='PUSH RACE'
```

`premature_or_stale/2` correctly separates "too early, retry" from "stale, ignore" by checking current-step + not-pending + not-active. A fast external completer cannot lose its result to the dispatch/commit window. The error is typed, the message tells you what to do, and it is documented. This is the standard the rest of the surface should be held to — and it makes #5 more frustrating, not less, because it shows the team knows how to type these outcomes.

---

## 8. Attempt/retry semantics are visible — mostly. — MINOR

**Does the agent know its attempt?** Yes. `attempt` and `idempotency_key` are on the parked task, and the key changes across redeliveries, so an executor that dedupes on `idempotency_key` re-executes correctly on retry while one that dedupes on `task_id` (stable across attempts) would wrongly skip. Worth calling out in docs — it is a trap, and the two identifiers are adjacent in the payload.

**Redelivery after deadline expiry** (`agent-dies`, 60 s deadline, agent never completes):

```
REDELIVERED as run_...:0:agent_work:2 (attempt 2,
  prior_failures=[{'reason': ':detach_deadline_expired', 'attempt': 1}])
```

Clean and fully self-describing. `prior_failures` is a real gift — the agent can see it is a redelivery and why.

**Reported failure** (`error-result`): `{:error, reason}` expires the deadline, records the reason, and the node re-runs → re-detaches at attempt 2 with `prior_failures: [{attempt: 1, reason: "python agent could not do the work"}]`. Exactly as documented; timeout and reported failure share one machine.

**Budget exhaustion:** attempt 2 also expired → run `:failed` with

```
%Docket.Run.Failure{code: "node_failed", message: "node(s) agent_work failed permanently",
  details: %{"errors" => %{"agent_work" => ":detach_deadline_expired"}}}
```

**The gaps:**
- The agent cannot see its **retry budget**. `max_attempts` lives in graph policies; I had to fetch the graph and would have had to expose it myself. Nothing says "this is your last attempt" — which is exactly when an agent would choose a cheaper/safer strategy or escalate.
- The terminal `Failure` records only the **last** attempt's reason. The Python agent's own attempt-1 message (`"python agent could not do the work"`) is **absent from the terminal projection** — it lived in `active_tasks`, which is cleared on failure. For a cross-language system where the external executor knows the real cause, losing its reason at exactly the moment an operator investigates is a bad trade. (It is presumably still in the retained event log, but that is a per-run query — see #1.)

---

## 9. Deadlines cannot be extended, and cannot vary per run. — MAJOR

**Trying to do:** give a long agent task more time without pre-committing to a huge deadline for every task on that node.

**What the API offered:** nothing. There is no `extend_deadline`, no heartbeat, no keepalive — I grepped `lib/docket.ex` and `lib/docket/detached.ex` for `extend|heartbeat|renew` and found nothing. The deadline is resolved once, at park time, in `loop.ex parked_attempt/5`, as `activation.detach.deadline_ms` (graph policy, fixed at graph-authoring time) falling back to `config.detach_deadline_ms` (runtime instance config, fixed at boot). Neither is per-run.

**Consequences for agent dispatch specifically:**
- Deadlines must be sized for the **worst-case** task on that node. An agent node whose p50 is 30 s and p99 is 20 min must set 20 min, so every genuinely-crashed agent holds its run hostage for 20 minutes.
- An agent that knows it needs more time cannot say so. Its only lever is `{:error, reason}`, which burns a retry from a budget it also cannot see (#8).
- Splitting workloads by duration means **splitting nodes in the graph**, i.e. modelling infrastructure timing into business logic.

This is a deliberate design position (`docs/delivery-guarantees.md`: "A worker that crashes or never completes is recovered by the detach deadline; nothing tracks the worker process itself" — no liveness tracking is the point, and it is a defensible simplification for in-process workers). But it is precisely the assumption that breaks for external agents, whose durations are unpredictable by nature. The claim-freshness doctrine deliberately removed heartbeats for *claims*; detach deadlines for *external* work are a different problem, and the report should not conflate them.

---

## 10. Concurrent pollers duplicate work by construction. — MAJOR

**Trying to do:** run two agent replicas.

**Observed** (`agent.py concurrent`): both pollers saw the **identical** task, `run_...:0:agent_work:1`, and both executed it. Agent A's write won; agent B's identical work was discarded and reported as success (#5).

There is no lease, claim, or visibility timeout on a detached task — `TaskState` has `started_at` and `deadline_at` but nothing resembling "checked out by". This is internally consistent (Docket's claim machinery covers *runs*, not detached tasks, and detach explicitly releases the claim), and the at-most-once *commit* fence means duplication is never a correctness bug. But for agent dispatch, where the duplicated work is a paid LLM call, "correct but billed twice" is the whole cost model.

Every multi-replica integrator must therefore build an external lease keyed by `idempotency_key` (Redis, or a host table) before they can safely scale past one agent. Docket has the natural home for this — it already owns the parked task row — and offers nothing.

**Cancellation, related:** an in-flight agent has no way to learn its run was cancelled. There is no push, no long-poll, no "is this task still valid" endpoint. It must re-poll `inspect_run` on its own initiative, and if it does not, it works to completion on a cancelled run and gets `{:ok, run}` back (#5). For minutes-long agent work with real cost, that is money spent after the customer cancelled.

---

## 11. Reading a finished run's outputs requires internal struct knowledge. — MINOR

`Docket.Run` exposes no accessor for channel values. `run.channels` is a map of channel id → `%Docket.Run.ChannelState{}`, so serialising a result meant knowing to reach into `.value`, and knowing that ids are namespaced `input:*` / `state:* `/ `edge:*` — the last of which are internal plumbing that must be filtered out (my first attempt crashed on `%ChannelState{channel_id: "edge:e1"}` failing `Jason.Encoder`). The graph declares `put_output!`, but nothing maps declared outputs back to values for a caller; I hand-rolled it. A `Docket.Run.outputs/1` or `channel_value/2` would remove a paper cut every single integrator hits the moment they return a result over HTTP.

---

## Severity summary

| # | Finding | Severity |
|---|---|---|
| 1 | No discovery/index/filter for detached tasks; O(running runs) scan per poll | MAJOR |
| 3 | `%Docket.Detached{}` must be hand-assembled; `from_task/2` is `@doc false` | MAJOR |
| 4 | No authorization on completions; token is not a credential; forgery demonstrated | MAJOR |
| 5 | Applied vs. silently-ignored completions indistinguishable at the public API | MAJOR |
| 6 | Must write/deploy an Elixir stub module for work implemented in Python | MAJOR |
| 9 | Deadlines not extendable and not per-run | MAJOR |
| 10 | Concurrent pollers duplicate work; no lease primitive; cancellation invisible in flight | MAJOR |
| 2 | Node config absent from the task payload (second fetch) | MINOR |
| 8 | Retry budget invisible to the agent; external failure reason lost from terminal projection | MINOR |
| 11 | No public accessor for channel values | MINOR |
| 7 | `:detach_pending` push race — correct, typed, documented | (strength) |

**No BLOCKERs.** Everything an external agent-dispatch integration needs is *reachable* through the public API without touching docket internals — which is a real and non-obvious achievement for a first cut. But four of the seven MAJORs (#1, #4, #5, #10) are things every single such integrator must build for themselves, in the same way, over data Docket already owns durably. That is the shape of a missing layer, not missing polish.

If only two things get fixed: **#5** (make the no-op visible — the information exists and is being discarded) and **#4** (verify the token — the value is already durable on the task). Both are small, and both are currently silent failures rather than loud ones.

---

## Reproducing

```sh
open -a Postgres
createdb docket_dogfood

cd .../scratchpad/dogfood/host
mix deps.get && mix docket.gen.migration -r DogfoodHost.Repo && mix ecto.migrate -r DogfoodHost.Repo
mix run --no-halt                       # :4009

cd ../agent
python3 agent.py happy                  # end-to-end: poll -> execute -> complete -> :done
python3 agent.py duplicate              # #5
python3 agent.py wrong-attempt          # #5
python3 agent.py after-cancel           # #5, #10
python3 agent.py forged                 # #4
python3 agent.py concurrent             # #10
python3 agent.py error-result           # #8
python3 agent.py agent-dies --watch 110 # #8 deadline expiry -> redelivery
python3 push_agent.py                   # #7 :detach_pending race
python3 loadtest.py                     # #1 polling cost
```

Note: `host/.tool-versions` is copied from the docket worktree — the host will not compile under the ambient Elixir 1.17.
