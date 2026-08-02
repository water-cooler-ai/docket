# Detached task dispatch — declared nodes, leased claims, authenticated completion

Design doc for DCKT-87 under epic DCKT-86. Supersedes the DCKT-40 spike
(PR #93, closed unmerged; branch `suttonmay5/dckt-40`). The spike's durable
core — the fenced completion mutation, deadline expiry through the retry
budget, and the parked-task invariants — carries forward as donor code; its
public surface (`{:detach, token}` return values, `%Docket.Detached{}`
identity assembly, single park-time deadline) does not. Main has none of the
spike's surface, so nothing here is a compatibility question: this contract
is built fresh, and the spike branch is a reference implementation for the
parts that survived.

Inputs: the DCKT-86 decision record, the dogfood friction report
(`tmp/dckt-40-dogfood-friction-report.md` on the spike branch), and the
grilling rounds of 2026-08-01. Sections marked **Proposal** are open for doc
review; everything else restates settled decisions.

## Settled decisions

1. **Declared detached nodes.** A graph node declares detached execution
   explicitly; the runtime parks it at plan time and never executes customer
   code for it. The return-a-tuple-from-node-code seam is gone.
2. **The Elixir claim API is the OSS seam.** Discovery is pull: an executor
   claims a pending detached task through the serialized signal path. Docket
   core is transport-agnostic — no HTTP, no SDKs. The first claimant is an
   in-VM dispatcher (WaterCooler's push layer is exactly this: claim, fire a
   request at an application endpoint, complete with the response).
3. **Tokens are minted at claim.** A claim is the only way to learn a task's
   completion token, so an unclaimed task is uncompletable and a completion
   racing its own park is unrepresentable. Identity alone never completes a
   task.
4. **Echo-only identity.** Completion accepts only values handed out verbatim
   by the claim. Executors never derive an id; no id format is public
   contract.
5. **Split deadlines.** Schedule-to-start (time waiting unclaimed; optional,
   default unbounded; expiry is non-retryable) and start-to-close (time
   claimed; mandatory; the lease TTL). Start-to-close expiry is redelivery:
   attempt+1, new park, new claim, new token.
6. **Completion feedback is typed.** Applied and unchanged are distinct
   returns; unchanged carries a reason. The executor may declare a reported
   failure permanent; graph policy remains the outer bound.
7. **Recoverability guarantee.** Every parked task is visible as pending work
   in the claim index, readable without claiming via
   `list_detached_tasks/2`. An unclaimed task with no schedule-to-start
   bound waits indefinitely; it remains claimable and its run remains
   cancellable.
   The pruner touches terminal runs only, so a waiting run is never deleted
   out from under a slow executor — the operator's escape hatch for an
   abandoned run is `cancel_run`, which makes it terminal and therefore
   prunable. Fencing happens at completion only; cancellation is not
   propagated to executors in flight.
8. **Wake over poll.** Claimants may subscribe for a wake hint when work
   parks; the claim call remains the source of truth.

## When to use a detached node

Any one of these routes work to a detached node; failing all three, use a
plain node under `timeout_ms`:

- **Vehicle time.** The call would occupy a backend vehicle longer than the
  deployment is willing to lose one (a minutes-long agent or LLM call, even
  when the transport is request/response-shaped).
- **Restart economics.** A plain node's in-flight call dies with the host and
  is re-executed on recovery. When re-execution has a real cost — a paid
  model call, a non-idempotent side effect — detached work survives the
  restart untouched: the executor keeps working and its completion lands
  through the durable token-addressed path.
- **Different connection.** The result naturally arrives somewhere other than
  the request connection (webhook, callback queue, human-in-the-loop tool).

The detach → claim → dispatch → resolve choreography above the seam belongs
to the application. Docket ships the primitive: park at plan time, wake on
park commit, claim with token, fenced completion.

## Declaration

A detached node is declared with an explicit implementation type and no
module:

```elixir
graph
|> Docket.Graph.put_node!("agent_work",
  implementation: :detached,
  config: %{"endpoint" => "summarize", "style" => "terse"},
  config_schema: %{...},                     # optional, durable-normalized
  fields: %{"summary" => Docket.Schema.string()},  # state its completion writes
  policies: %{
    "retry" => %{"max_attempts" => 3, "backoff_ms" => 0},
    "detach" => %{
      "start_to_close_ms" => 600_000,
      "schedule_to_start_ms" => nil,         # unbounded queue wait (default)
      "on_deadline" => "reschedule"
    }
  }
)
```

- `implementation: :detached` normalizes to `%{type: :detached}`.
  **Rationale:** `implementation: nil` stays a compile error
  (`:missing_node_implementation`), exactly as today. A forgotten module must
  never silently become a detached node; detachment is an explicit authoring
  decision, not the absence of one.
- **Proposal — inline `config_schema`.** Module nodes keep the
  `config_schema/0` callback. A detached node has no module, so its schema —
  when it wants one — is declared inline as a node attribute, run through the
  same `Schema.normalize_durable!/2` pipeline, with defaults materialized
  into config before hashing exactly as module schemas are today. The
  attribute is optional: the authoritative contract for a detached node's
  config lives with its external executor (application layer), so Docket
  validates when asked and passes config through opaquely otherwise. Adding
  the attribute extends the durable graph document (see Migration).
- **Compiler.** Validation accepts `%{type: :detached}` with no
  `config_schema/0`/`call/3` export checks; rejects a detach policy block on
  a module node only if that is the position review settles on (see open
  question below); lowering produces a runtime node with no module/function.
  Branches, edges, and output writes validate identically to module nodes —
  a detached node's subscriptions derive from its incoming edges like any
  other node, and that read set is what its snapshot will contain.
- **Graph hash.** The declaration hashes like any node: implementation map,
  config with materialized defaults, policies, metadata all feed the
  canonical document digest. Changing a detached node's config or deadlines
  is a graph version change, as it should be — executors see versioned
  config in the claim payload.
- **Planning.** `prepare_activations` produces activations for detached nodes
  exactly as for module nodes (same identity derivation, same snapshot, same
  policy resolution). The dispatcher never spawns a worker for one: the
  superstep partitions it directly into the parked set, and the park commit
  writes the pending task. Plan-time parking, not execute-time: no customer
  code runs on the host for a detached node, ever.

Open question for review: whether `"detach"` policies on a *module* node are
rejected or ignored. Proposal: rejected (`:invalid_policy`) — the vocabulary
only means something on a detached node, and silent acceptance is how stale
policy blocks survive refactors.

## Task lifecycle

```
plan ──park──> pending ──claim──> claimed ──complete──> applied
                 │                   │
                 │ schedule-to-start │ start-to-close expiry
                 │ expiry            │   on_deadline=reschedule → pending (attempt+1)
                 ▼                   │   on_deadline=fail       → node failed
        failed (non-retryable)       ▼
                              budget exhausted → node failed
```

- The attempt number is fixed at park. A claim binds a lease to that attempt;
  it does not advance it. Redelivery (start-to-close expiry with
  `on_deadline: "reschedule"` and budget remaining) parks attempt+1 as a new
  pending task; the retry `backoff_ms` applies between expiry and the new
  park, consistent with plain-node retries.
- The run stays `:running` while detached tasks are outstanding, with
  `wake_at` set to the earliest live deadline across its tasks: a claimed
  task always contributes its start-to-close deadline; a pending task
  contributes its schedule-to-start deadline when bounded and nothing when
  unbounded. A run whose only outstanding work is an unbounded pending task
  carries no timer — its recoverability guarantee is visibility in the claim
  index (settled decision 7).
- `cancel_run` removes the run's pending/claimed detached tasks from the
  index in the same commit that makes the run terminal. Completions arriving
  after that are `{:unchanged, :terminal, run}`. Cancellation is also the
  operator path for a run waiting on work nobody will ever do: the pruner
  only ever deletes terminal runs, so cancel-then-prune is the lifecycle for
  abandoned detached work.

## The claim API

```elixir
@spec Docket.claim_detached(runtime, keyword()) ::
        {:ok, %Docket.DetachedTask{}} | :empty | {:error, Docket.Error.t()}
```

- **Pop-next.** The backend selects the next pending task; the caller does
  not enumerate and choose. Options: `:node_ids` (allowlist filter) and
  `:scope` (tenant filter). **Proposal:** `:scope` is optional even under
  `tenant_mode: :required` — the expected claimant is a platform-level
  dispatcher serving every tenant, which is the opposite posture from run
  admission (where the vehicle is system capacity and tenant fairness is the
  claim-policy engine's job). Cross-scope claim fairness is explicitly a
  seam: v1 is FIFO, and if a deployment needs interleaving it attaches at
  the same engine boundary run admission uses. No other selection
  vocabulary: routing intelligence (which endpoint, which fleet) lives in
  node config and the application's registry, not in Docket.
- **Selection order** — **Proposal:** oldest `scheduled_at` first (FIFO)
  within whatever filter is given.
- **Mechanics**, matching how every signal already commits: selection is a
  hint, the fence is the truth. The claim call selects a candidate from the
  pending index (Postgres: `FOR UPDATE SKIP LOCKED` on the projection row),
  then runs a claim mutation through `Lifecycle.signal/4` like any other run
  mutation — computed pure and lock-free over the fetched run, committed
  under the backend's `FOR UPDATE` + `checkpoint_seq` fence, replayed on
  `:stale_checkpoint`. The mutation re-verifies the task is still pending at
  the selected attempt, mints the token, stamps `claimed_at`, and sets the
  start-to-close deadline (task `deadline_at`, run `wake_at`); the commit
  transaction flips the projection row to claimed atomically with the run
  write. The mutation must be replay-safe (a replay simply mints a fresh
  token; only the committed one exists). Two concurrent claimants serialize
  at the fence; the loser's re-verification fails and its claim call pops
  the next candidate or returns `:empty`.
- **No batch claim.** A claim is a selection plus a fenced commit through
  the task's run; a batch spanning N runs is necessarily N per-run commits,
  so a native batch could only save round trips, never add atomicity. The
  v1 batch is the drain loop — claim until `:empty`, dispatch as you go:

  ```elixir
  case Docket.claim_detached(runtime) do
    {:ok, task} -> dispatch(task); drain(runtime)
    :empty -> :ok
  end
  ```

  Claim only what can start executing now: the claim commit starts the
  start-to-close clock, so any queue between claim and execution — the
  claimant's local backlog, a saturated `Task.Supervisor`, a bounded HTTP
  pool — spends the task's execution budget on waiting and manufactures
  spurious redeliveries. "Dispatched" means handed to free capacity, not
  enqueued: bound in-flight work, stop draining at the bound, and resume
  the drain when a completion frees a slot. A `:max`/batch option remains a
  named seam (below), additive if throughput ever demands it.
- **Wake hint.** `Docket.subscribe_detached(runtime, opts) :: :ok` registers
  the calling process for a best-effort message after a park commit makes
  new pending work visible (initial park and redelivery re-park alike). The
  message is `{:docket_detached, instance, node_id}` — a hint with no task
  payload; the claim call is the source of truth. Options: `:node_ids`
  (deliver only for these nodes). The subscription is process-state: the
  subscriber is monitored and dropped on death, and there is no explicit
  unsubscribe in v1. **Proposal:** in-VM delivery only in v1 — the primary
  claimant shares the VM with the park commit (decision 2), the memory
  backend is natively event-driven, and Postgres cross-VM notification
  (`pg_notify`) is a named seam, not a v1 surface.
- **Wakes are hints in every topology, including single-VM.** Because the
  subscription is process-state, a restarting claimant always has a
  missed-wake window; there is no configuration in which wakes alone are
  sufficient. The correct claimant shape is therefore always: **drain on
  startup** (claim until `:empty` before trusting any hint), then drain on
  every wake, with a coarse poll interval as the backstop. Multi-VM
  Postgres deployments without `pg_notify` simply lean on the poll leg
  harder.
- **Cross-VM wake — recommended shape** (documented for the seam; not built
  in v1). Postgres delivers `NOTIFY` only when its transaction commits, so a
  `pg_notify` issued inside the same transaction that commits the park
  (initial park and redelivery re-park alike) has exactly the wake-hint
  semantics: no notification for a rolled-back park, delivery follows the
  commit by construction. One channel per installation, derived from the
  schema prefix (e.g. `docket_detached_<prefix>`); the payload is the same
  hint the in-VM message carries — instance and node id, never task
  identity or token material, comfortably inside the `NOTIFY` payload
  limit. Each VM runs one listener (`Postgrex.Notifications`) in the
  backend's supervision tree that fans incoming notifications out to that
  VM's `subscribe_detached` subscribers — `pg_notify` is a transport
  *behind* the existing delivery layer, and the subscriber contract
  (best-effort; tolerate missed and spurious wakes) does not change when it
  ships. `LISTEN`/`NOTIFY` is lossy for disconnected listeners and does not
  survive transaction-mode pooling proxies (pgbouncer), so the poll
  fallback stays mandatory even then — the same posture Oban's Postgres
  notifier takes.
- **Observability read.** `Docket.list_detached_tasks(runtime, opts)` — a
  non-destructive, filtered, keyset-paged read over the claim index
  (filters: state, node ids, tenant scope, age), returning summary rows
  only: identity fields, state, and timestamps — no config, no snapshot, no
  token material. This is what makes the recoverability guarantee
  (settled decision 7) executable by an operator: dashboards, aging-pending
  alarms, and the cancel-abandoned-work playbook all read here. It is
  **not a dispatch surface**: a listing row is structurally non-executable
  (it carries no token and no work order), and claimants must not
  list-then-claim — `claim_detached` is the only discovery path for
  executors, and pop-next exists precisely so selection never happens in
  the caller.

### The claimed task payload

`%Docket.DetachedTask{}` is a complete work order; an executor needs no
second read to start:

| field | content |
|---|---|
| `run_id`, `graph_id`, `graph_hash` | provenance; config is already versioned by `graph_hash` |
| `tenant_id` | the run's tenant scope (`nil` when untenanted), echoed at completion — a cross-scope dispatcher passes back what it was handed, like every ref field |
| `node_id`, `step`, `attempt` | position; opaque to the executor, echoed never derived |
| `task_id`, `idempotency_key` | opaque strings for external dedup; the key changes across attempts, the id does not — dedupe external effects on the key |
| `config` | the node's config, defaults materialized (friction finding #2) |
| `snapshot` | the node's read view — the channels its incoming edges subscribe, at planned versions |
| `max_attempts`, `prior_failures` | the retry budget and history (finding #8): an executor can see "this is my last attempt" and every prior reason |
| `scheduled_at`, `claimed_at`, `deadline_at` | park time, claim time, start-to-close deadline |
| `token` | the completion secret for this claim — returned here and stored nowhere readable |

**Egress caution:** the snapshot is the node's full read view, and a claim
hands it to whatever the claimant forwards it to. The read set is declared by
the graph — a detached node's incoming edges are its egress surface. Authors
sizing what an external executor may see do it with edges, not with a
projection mechanism Docket does not have. The same caution covers the
*completion* path: every completion return — `{:applied, run}`,
`{:unchanged, reason, run}` — carries the full run document for the
claimant's convenience, including on paths where no token was verifiable (a
probe against a settled task). Those returns are for the claimant, never
for the wire: a bridge answers its HTTP caller with the outcome and reason,
not the run.

## Tokens

- **Mint at claim.** 32 bytes from `:crypto.strong_rand_bytes/1`,
  base64url-encoded, returned once in the claim payload.
- **Proposal — storage.** The raw token is never persisted anywhere; only
  its SHA-256 hash is, on the claimed `TaskState` in the run document.
  Placement rationale: the completion fence is a pure, replay-safe function
  of run state (`Lifecycle.signal`'s optimistic-commit contract), so the
  verifier the fence compares against must live *in* the run document — and
  a hash of a 256-bit random value is not secret material, so its
  reachability through run reads and the full run handed to checkpoint
  observers is harmless. What must never appear anywhere readable is the raw
  token, and it never does: minted in the claim mutation, returned once in
  the claim payload, gone. Checkpoint *metadata* projects only four
  `TaskState` fields today and does not grow a fifth; events emitted at
  claim (`:detached_claimed`) and completion carry no token fields, and
  telemetry metadata carries events, not task state.
- **Rotation.** One live token per task, replaced whenever a new claim is
  minted (which only happens for a new attempt). Redelivery invalidates the
  prior token by replacing the stored hash; a completion bearing the old
  token reports `{:unchanged, :superseded, run}` under the completion fence
  order — attempt matching runs before token verification, so a rotated
  token never masquerades as an attack.
- **Custody.** The token is the *executor's* capability, not the
  dispatcher's secret. For callback-shaped work ("different connection"),
  the token must travel with the work order and be echoed back in the
  callback: the claim is its only source, ever, so a dispatcher that holds
  it only in memory strands the task until start-to-close expiry when it
  restarts. "Never persisted" constrains Docket's storage, not the
  application's forwarding.
- **Verification.** Inside the completion mutation itself: hash the
  presented token, constant-time compare against the hash stored on the
  currently parked attempt's `TaskState`. The comparison is part of the same
  pure fence that decides applied-vs-unchanged, so it is serialized and
  replayed by the identical commit machinery — no separate authorization
  round-trip, no second source of truth.

## Deadlines

| budget | clock starts | declared by | default | expiry |
|---|---|---|---|---|
| schedule-to-start | park commit | node `"detach"` policy `"schedule_to_start_ms"` | unbounded (`nil`) | non-retryable failure `:schedule_to_start_expired` |
| start-to-close | claim commit | node policy `"start_to_close_ms"`, falling back to instance `detach_start_to_close_ms` | instance default 300_000 ms | `on_deadline` policy (default `"reschedule"`): `"reschedule"` → redelivery (attempt+1, new token, backoff applies) or `"fail"` → node failure; budget exhaustion → node failure |

- Start-to-close is mandatory: the node policy may omit it, the instance
  default fills it, and a claimed task without a deadline is unrepresentable
  (`validate_durable`) — the spike's structural invariant survives at the
  claim layer. Schedule-to-start is optional and unbounded by default; a
  pending task is allowed to wait forever, because "nobody has claimed this"
  is an operational condition, not a property of the work. The `"detach"`
  policy block is optional in its entirety: a bare
  `implementation: :detached` node gets the instance start-to-close
  default, unbounded schedule-to-start, and `on_deadline: "reschedule"` —
  a legal, fully specified declaration.
- Schedule-to-start expiry does not touch the retry budget. Re-parking an
  unclaimed task into the same queue that failed to serve it is a treadmill;
  the expiry means "no executor showed up in the time the author bounded"
  and produces a permanently failed node with a distinct error code an
  operator can alarm on. (Temporal reaches the same conclusion for its
  schedule-to-start timeout: non-retryable by construction.)
- The start-to-close deadline **is** the lease TTL. There is no separate
  lease vocabulary, no keepalive, no extension call: an executor that
  outlives its deadline loses the attempt to redelivery, and its late
  completion is fenced as stale. Deadline extension/renewal is an explicitly
  out-of-scope seam (below) — it would be one more mutation through the same
  signal path, which is why the seam is cheap to leave open and expensive to
  design prematurely.

## Completion

```elixir
@spec Docket.complete_detached(runtime, ref, result, keyword()) ::
        {:applied, run} | {:unchanged, reason, run} | {:error, Docket.Error.t()}

# ref:    %Docket.DetachedTask{} or
#         %{run_id: ..., task_id: ..., attempt: ..., token: ..., tenant_id: ...}
# result: {:ok, outputs} | {:error, reason}
# opts:   permanent: true (with {:error, ...}) — skip the remaining retry budget
```

- **`outputs` is a state update**, exactly what a module node returns in
  `{:ok, update}`: a map of state-field writes, durable-normalized and
  validated against the graph's declared fields, applied as pending writes
  at the next barrier. An invalid `outputs` map (unknown field, schema
  violation, non-durable value) is a typed error return; it does **not**
  consume the attempt or settle the task — the task stays claimed, and the
  executor may correct and resubmit within its start-to-close deadline.

- **Echo-only ref.** Every ref field is a value the claim handed over
  verbatim. A cross-language layer serializes those named fields; no format
  is documented, none is derivable, and `TaskState.task_id/3` /
  `idempotency_key/2` are not public API (finding #6/#3 closed
  structurally). Under `tenant_mode: :required` the echoed `tenant_id` is
  what resolves the completion's tenant scope, so a platform-level
  dispatcher that claimed cross-scope can complete what it claimed without
  holding any tenancy knowledge of its own. Accepting the full struct back
  is a convenience for in-VM claimants.
- **Applied vs unchanged** (finding #5, the report's worst practical issue):
  the fence's internal distinction surfaces as differently shaped returns,
  so a caller cannot pattern-match unchanged as success by accident.
  **Proposal — unchanged reasons**, each computable from the run document at
  fence time with zero stored history: `:superseded` (a newer attempt of
  this task is currently parked — this completion's work did not land and
  the task is being re-executed), `:settled` (no attempt of this task is
  parked — the task will not run again; the retry of a completion call that
  already applied lands here), and `:terminal` (the run is
  done/failed/cancelled). The reason answers the one question an executor
  acts on — "did my work land, and if not, is anyone going to retry it?" —
  including for the completion-retry case every HTTP integrator hits: a
  completion POST that timed out after applying and was retried reports
  `:settled`, not the same answer a discarded post-redelivery completion
  gets (`:superseded`).
- **Failure results.** `{:error, reason}` burns the attempt and re-parks
  pending attempt+1 under the retry policy (backoff applies), or fails the
  node when the budget is exhausted. `permanent: true` fails the node
  immediately regardless of remaining budget — the executor knows things the
  policy cannot ("this input is malformed; retrying is waste"; in an HTTP
  layer, the 4xx/5xx distinction maps here directly). Graph policy remains
  the outer bound: `permanent` can only shrink the budget, never extend it.
- **Dispatch failure after claim** (the executor was never reached —
  connection refused, pool exhausted, mid-deploy): **the claim is the
  attempt.** Claiming a task is taking responsibility for its attempt, and
  a claimant that cannot hand the work off reports that like any other
  failure — complete `{:error, "dispatch failed: ..."}` promptly; the
  attempt burns and re-parks under retry backoff. There is deliberately no
  release/nack operation: a return-to-pending that skipped the budget would
  make the attempt count lie about how many times responsibility was taken,
  and would reintroduce exactly the second lease vocabulary this contract
  refuses ("the lease IS the attempt" — settled). Stranding the claim
  instead merely reaches the same attempt+1 after the full start-to-close
  window. Size `max_attempts` knowing transport failures and execution
  failures share the one budget — that is the invariant, not a v1
  limitation.
- **Failure reasons survive to the terminal projection** (finding #8): the
  terminal `Failure.details` carries every attempt's recorded reason, not
  only the last synthesized one. The executor's own words are what an
  operator reads first; they must not be cleared with `active_tasks`.
- **The fence order (normative).** Every completion is evaluated in this
  order, inside the fenced mutation:
  1. The run is terminal → `{:unchanged, :terminal, run}`.
  2. The task is parked at exactly the ref's attempt → verify the token
     (constant-time hash compare): mismatch →
     `{:error, %Docket.Error{type: :unauthorized_completion}}`; match →
     apply the result → `{:applied, run}`.
  3. Otherwise (no such task parked, or parked at a different attempt): a
     newer attempt is parked → `{:unchanged, :superseded, run}`; nothing
     parked → `{:unchanged, :settled, run}`.

  Attempt matching precedes token verification, which fixes the meaning of
  both outcomes: `:unauthorized_completion` means exactly "right attempt,
  wrong secret" — always a bug or an attack, never a race — and a rotated
  (pre-redelivery) token reports `:superseded`, so an executor that outlived
  its lease logs a normal redelivery instead of paging security.
  `:detach_pending` does not exist in this design: a token can only be
  learned from a claim, a claim only exists after the park commit, so a
  premature completion has nothing to present. Nothing to deprecate — the
  error type never shipped.

## Storage and migration

- **Proposal — a claim-index projection table** (working name:
  `docket_detached_tasks`), one row per pending/claimed detached task:
  `run_id` (FK → `docket_runs(run_id)`, `on_delete: :delete_all`),
  `tenant_id`/`scope_key` matching the runs table's tenancy shape,
  `task_id`, `node_id`, `attempt`, `state` (`pending` | `claimed`),
  `scheduled_at`, `claimed_at`, `deadline_at`, with a partial index on
  `(scope_key, scheduled_at) WHERE state = 'pending'` serving pop-next. No
  token material lives here (the hash lives on the `TaskState`; see Tokens).
  Rows are maintained inside the same commit transaction as the run
  mutation that parks, claims, redelivers, completes, or cancels — a
  projection of `active_tasks` owned by the run write path, never written
  independently, so drift is impossible. Rationale over columns on
  `docket_runs`: a run can hold several detached tasks, so pop-next wants
  task-granular rows for `SKIP LOCKED` selection — and the row *is* the
  discovery index the friction report showed every integrator rebuilding.
- **Migration shape.** Migrations are code modules
  (`Docket.Postgres.Migrations.V0x`) behind the versioned
  `Docket.Postgres.Migration` dispatcher; the installed version is recorded
  as a comment on `docket_runs`. This design adds **V03** (the projection
  table and its indexes) and bumps `@current_version` to 3.
  `mix docket.gen.migration` stays fresh-install-only with no upgrade
  flags; the release's migration guide documents the hand-pinned
  `Migration.up(version: 3)` / `down(version: 2)` snippet, per the
  established doctrine and doc pattern (schema impact stated in the lede,
  removed APIs, mandatory declarations, upgrade steps).
- **Pruning needs no change.** The pruner deletes terminal runs only, and a
  terminal run has no live index rows (cancel/completion removed them); the
  FK cascade covers crash-window stragglers. A run parked on an unbounded
  pending task is structurally unprunable until an operator cancels it —
  that is the intended lifecycle, not a gap.
- The **memory backend** maintains the equivalent index in its in-VM state
  with identical claim/complete semantics; conformance holds both backends
  to the same behavior. (The memory backend implements the full backend
  contract already; its promotion to a shipped runtime backend is separate
  work and not part of this design.)
- The **graph document** gains the optional `config_schema` node attribute
  and the `%{type: :detached}` implementation type; the graph
  `schema_version` bumps accordingly.

## Conformance and testing

`Docket.BackendTests` grows a third composed case module (alongside `Cases`
and `TransitionCases` — proposal: `DetachedCases`) run against both
backends: park visibility (a parked task is claimable), claim exclusivity
(two racing claimants, one winner, loser pops next/empty), the normative
fence order (wrong token on the parked attempt → `:unauthorized_completion`;
rotated token post-redelivery → `:superseded`, never unauthorized; duplicate
of an applied completion → `:settled`), applied/unchanged across every
interleaving the spike already proves (duplicate, wrong attempt,
post-cancel, concurrent, post-redelivery), cross-tenant claim → echoed-scope
completion,
both deadline expiries and their dispositions, `permanent: true`,
terminal-reason preservation, wake-hint delivery, pruning of index rows,
tenancy scoping of claims, and the `list_detached_tasks` read (filters,
paging, non-mutating, summary rows free of token material and payloads). The
spike's fence tests port over as the seed of this suite.

## Out of scope — and the seam each attaches to

| out | seam it will attach to |
|---|---|
| HTTP surface, worker SDKs | the claim/complete Elixir API (WaterCooler's dispatcher is the first consumer) |
| Cancellation propagation to executors | the claim payload (a future validity affordance) — completion-time fencing is the only v1 staleness mechanism |
| Deadline extension / keepalive / renewal | one additional mutation through the serialized signal path |
| Cross-VM wake (`pg_notify`) | `subscribe_detached`'s delivery layer — recommended shape documented in the claim section |
| Batch claim | `claim_detached` options — the drain loop is the v1 pattern |
| Hosted control plane | everything above the OSS seam |
| Channel-output accessor (finding #11) | independent read-surface work; noted, not expanded here |

## Implementation slicing (proposed tickets under DCKT-86)

1. **Declaration** — `%{type: :detached}` implementation, inline
   `config_schema`, split-deadline policy vocabulary, compiler validation,
   lowering, graph schema_version bump. Compiles and hashes; no runtime.
2. **Park + storage** — plan-time parking (superstep partition, park
   commit), the claim-index projection table + memory-backend equivalent,
   backend schema version bump, migration doc snippet. Spike donor: park
   machinery, `validate_durable` invariants.
3. **Claim** — pop-next selection, claim mutation through the signal path,
   token mint/hash storage, `%Docket.DetachedTask{}` payload, tenancy
   scoping, `subscribe_detached` wake hint, `list_detached_tasks` read.
4. **Completion** — echo-only ref, token verification in the fence,
   `{:applied, ...}` / `{:unchanged, reason, ...}` returns, `permanent:`,
   terminal-reason preservation, deadline expiry dispositions wired to the
   new vocabulary. Spike donor: `RunMutation.complete_detached`, expiry
   machinery in `Runtime.Loop`.
5. **Conformance + docs** — the `BackendTests` section above, the detached
   dispatch guide (including the when-to-detach tests and the
   dedupe-on-idempotency-key trap), telemetry/events documentation.

Each slice lands on its own branch per the repo workflow; 2–4 are
sequential, 1 is independent of 2, 5 trails.
