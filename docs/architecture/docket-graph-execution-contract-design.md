# Docket: Graph Execution Contract Design

Status: reference draft
Date: 2026-06-25

Related documents:

- `docs/architecture/docket-v1-implementation-path.md`
- `docs/architecture/docket-runtime-design.md`
- `docs/architecture/docket-graph-construction-design.md`
- `docs/architecture/docket-v1-test-suite-design.md`

Implementation note: use `docket-v1-implementation-path.md` as the active v1
build sequence. This document owns the detailed execution and checkpoint
contract.

## 1. Purpose

This document narrows Docket's graph execution contract.

It focuses on the boundary between:

- the supervised runtime
- the Runtime process
- the shared execution loop
- executors
- guards
- checkpoints
- host-owned persistence
- test helpers

It does not redesign graph construction or UI projection. `Docket.Graph` remains
the canonical public graph document, and UI-specific projection remains
host-owned.

## 2. Known Execution Model

The existing design already establishes these execution rules:

- One active graph run is owned by one Runtime process.
- The Runtime consumes an internal `Docket.Runtime.Graph` materialized from a
  canonical `Docket.Graph`.
- The host application stores `Docket.Graph` and `Docket.Run` documents.
- `Docket.Run` is the public restorable run state document.
- The Runtime owns the materialized runtime graph and mutable in-memory run state
  while a run is active.
- Nodes are logical actors, not OTP processes by default.
- Nodes communicate only through channel writes.
- Channel updates become visible only after the update barrier.
- Runtime progress happens through repeated Plan -> Execution -> Update
  supersteps.
- Checkpoints are the durable boundary for committed runtime moments.
- PubSub, streams, telemetry, and UI overlays are projections, not durable truth.

This contract resolves the highest-risk gaps before implementation:

- Exact public and internal module boundaries for the shared execution loop.
- Exact supervised Runtime callback/call/message API.
- Exact state structs that are public, internal, or test-visible.

## 3. Execution Layers

The current design implies three execution layers.

```text
Execution loop
  shared plan/execute/update/checkpoint-building logic

Supervised Runtime shell
  GenServer process that owns one active run in production

Inline test shell
  calling test process that drives the same execution loop
```

The execution loop must not be duplicated. The supervised Runtime and inline test
runtime should call the same planning, validation, reducer, update, and
checkpoint-building code.

Resolved v1 contract:

- `Docket.Runtime.Loop` is the internal transition module.
- `Docket.Runtime.Algorithm` holds the pure planning, guard evaluation, reducer,
  write validation, and termination helpers used by the loop.
- `Docket.Runtime` is the supervised GenServer shell only.
- `Docket.Test` is the only test-facing facade for inline execution; tests do
  not call `Docket.Runtime.Loop` directly.
- The loop API is internal. Test helpers may return checkpoints and public
  `Docket.Run` documents, but they do not expose private transition internals
  as a public contract.

## 4. LangGraph Reference: Execution Loop Boundaries

LangGraph's Pregel implementation is a useful reference for separating public
API, loop state, task execution, and pure-ish graph algorithms.

Relevant source layout:

- `langgraph/pregel/main.py`: public `Pregel` runtime facade and graph-facing
  API.
- `langgraph/pregel/_loop.py`: mutable Pregel loop state machine.
- `langgraph/pregel/_runner.py`: concurrent task runner.
- `langgraph/pregel/_algo.py`: task preparation, write application, local reads,
  interrupt checks, and other execution algorithms.

Useful structural lessons for Docket:

- Keep the public runtime facade small. Public APIs should start/resume/read a
  run, but should not expose the loop internals directly.
- Put mutable step state in one loop/runtime state object.
- Keep task selection and write application as separate functions from the
  process shell.
- Keep task execution/commit mechanics separate from planning and channel
  reducers.
- Let the supervised Runtime and inline test runtime share the same execution
  loop.

Possible Docket translation:

```text
Docket
  public facade functions such as run/resume/get_run/resolve_interrupt

Docket.Runtime
  GenServer shell for one active run

Docket.Runtime.Loop
  internal transition functions shared by GenServer and inline tests

Docket.Runtime.Algorithm
  plan, prepare activations, apply writes, reduce channels, detect termination

Docket.Runtime.Dispatcher
  dispatch selected node executions and collect outputs
```

Resolved v1 contract:

- Keep `Docket.Runtime.Loop` and `Docket.Runtime.Algorithm` separate.
- Keep the public adapter boundary as `Docket.Executor`.
- Use a small internal dispatcher under the runtime/loop if needed, but do not
  make it a host extension point.
- The inline runtime goes through `Docket.Test`, which delegates to the same loop.

## 4.1 Internal Loop API Shape

The concrete internal API should make the boundary between process shell,
run transition functions, and pure execution algorithms explicit.

`Docket.Runtime` is the only GenServer shell. It owns the process mailbox,
registry name, lifecycle calls, task result messages, timeout messages, and
tick scheduling. It should be thin: keep the current `Docket.Run` in memory,
translate calls/messages into loop calls, dispatch selected work, track
in-flight process refs, and handle async checkpoint completion messages.

`Docket.Runtime.Loop` is not a state value that the Runtime stores. It is the
internal transition module shared by the supervised Runtime and `Docket.Test`.
Loop functions take a runtime graph plus a `Docket.Run` and return an updated
`Docket.Run` plus any internal effects the caller must perform or observe.

```elixir
Docket.Runtime.Loop.init(runtime_graph, run, opts)
Docket.Runtime.Loop.plan(runtime_graph, run, opts)
Docket.Runtime.Loop.apply_results(runtime_graph, run, task_results, opts)
Docket.Runtime.Loop.resolve_interrupt(runtime_graph, run, interrupt_id, value, opts)
```

`Loop.init/3` is the single loop entrypoint for a live run. It receives a
public `Docket.Run` document and derives what to do from that run document. A
normal `Docket.run/4` call builds a fresh `Docket.Run` from the graph input
first; `Docket.resume/4` passes the durable run document loaded by the host. A
fresh run means the loop initializes channels, tasks, frontier, and timestamps.
A saved run means the loop continues from the recorded graph execution status.

Expected return shapes:

```elixir
{:ok, Docket.Run.t()}
| {:ok, Docket.Run.t(), term()}
| {:error, Docket.Error.t()}
```

The extra return value is internal and should be limited to concrete values the
Runtime already needs, such as selected activations or async checkpoint refs to
observe or drain. The loop does not expose staged transitions as an API.

When a transition requires a checkpoint, `Docket.Runtime.Loop` builds the
checkpoint and calls the configured `Docket.Checkpoint` callback from runtime
configuration such as `use Docket, checkpoint: MyApp.DocketCheckpoint`. For
`:sync` checkpoints, the loop calls the callback before installing the
transition. If the sync checkpoint fails, the caller keeps the previous
`Docket.Run` and receives a typed checkpoint error. For `:async` checkpoints,
the loop returns the committed run and invokes the configured checkpoint
callback asynchronously. Async checkpoint failure is observable but does not
roll back the active in-memory run.

`Docket.Runtime.Algorithm` holds deterministic helper functions. It has no
mailbox, no checkpoint side effects, and no direct executor calls.

```elixir
Docket.Runtime.Algorithm.plan(graph, run, opts)
Docket.Runtime.Algorithm.prepare_activations(graph, run, plan, opts)
Docket.Runtime.Algorithm.evaluate_guard(expr, context)
Docket.Runtime.Algorithm.validate_state_update(graph, activation, update, opts)
Docket.Runtime.Algorithm.collect_state_writes(graph, activations, task_results, opts)
Docket.Runtime.Algorithm.apply_state_writes(graph, run, writes, opts)
Docket.Runtime.Algorithm.evaluate_edge_triggers(graph, run, completed_nodes, opts)
Docket.Runtime.Algorithm.detect_terminal(graph, run, opts)
Docket.Runtime.Algorithm.build_checkpoint(graph, run, events, opts)
```

`Docket.Runtime.Dispatcher`, if introduced, is also internal. It prepares the
state snapshot, node config, and runtime context, calls the configured executor
for each activation, handles local task timeout mechanics, and returns
normalized task results to the loop. It does not evaluate guards, apply reducers,
or commit checkpoints.

```elixir
Docket.Runtime.Dispatcher.dispatch(activations, graph, run, opts)
```

## 4.2 Runtime Module Responsibility Index

The runtime modules should keep narrow ownership boundaries. When implementation
pressure makes a responsibility feel convenient in two places, use this index to
decide where it belongs.

| Module | Owns | Must not own |
| --- | --- | --- |
| `Docket` | Public facade functions such as `run/4`, `resume/4`, `get_run/3`, and `resolve_interrupt/5`; runtime module lookup; public error normalization. | Mutable run state, graph execution algorithms, executor dispatch details, checkpoint persistence, or public exposure of Runtime PIDs. |
| Generated `MyApp.Docket` | Host-friendly wrappers around the configured runtime; compile-time/runtime configuration for checkpoint, executor, limits, and supervision names. | Per-run mutable state, graph compilation internals, node execution, or custom behavior that diverges from `Docket` semantics. |
| `Docket.Runtime.Supervisor` | Starting, restarting, and terminating one `Docket.Runtime` process per active run according to the runtime supervision strategy. | Run mutation, planning, checkpoint construction, executor dispatch, or graph storage. |
| `Docket.Runtime.Registry` | Mapping runtime/run identity to the active Runtime process; enforcing one active Runtime owner per run ID. | Public run reads from storage, run mutation, checkpoint handling, or eviction policy beyond registration/liveness. |
| `Docket.Runtime` | GenServer shell for one active run; mailbox ownership; lifecycle calls; tick scheduling; current `Docket.Run`; compiled runtime graph; runtime config; tracking in-flight dispatcher refs and async checkpoint completion messages. | Guard evaluation, reducer logic, write validation, graph lowering, public graph storage, checkpoint callback execution, or direct durable persistence. |
| `Docket.Runtime.Loop` | Processless transition functions over `Docket.Runtime.Graph` and `Docket.Run`; initialization from a supplied run; planning transitions; applying normalized task results; interrupt resolution; checkpoint emission/barrier semantics; returning updated runs. | GenServer callbacks, process registry, direct executor calls, telemetry/PubSub publication, or direct host storage outside the configured checkpoint callback. |
| `Docket.Runtime.Algorithm` | Deterministic graph execution helpers: plan, prepare activations, evaluate guards, validate outputs, collect writes, apply reducers, detect termination, and build checkpoint data. | Mutable process state, wall-clock reads except injected timestamps, random values, external services, executor calls, checkpoint side effects, or host callbacks. |
| `Docket.Runtime.Dispatcher` | Internal task dispatch mechanics; building the node state snapshot/config/context; calling the configured `Docket.Executor`; local/task timeout handling; normalizing node returns, raises, exits, and throws into task results. | Planning, guard evaluation, reducer application, checkpoint commit, retry policy decisions beyond attempt mechanics, or public adapter configuration. |
| `Docket.Executor` implementations | The adapter boundary for executing one runtime graph node activation locally, in a task, or later through queue/remote systems. | Mutating `Docket.Run`, applying writes, emitting checkpoints, deciding graph termination, or reading uncommitted superstep writes. |
| `Docket.Test` | Test-facing inline runtime facade; drives the same loop transitions in the calling process; returns public runs and accepted checkpoints for assertions. | A second execution interpreter, direct mutation of loop internals as public API, GenServer lifecycle semantics, or behavior that supervised Runtime cannot share. |

Additional boundary rules:

- Only `Docket.Runtime` owns a live process for a run.
- `Docket.Run` is the durable execution state for a run.
- `Docket.Runtime.Loop` owns transition semantics, not a separate stored state.
- Only `Docket.Runtime.Algorithm` owns deterministic execution semantics.
- Only `Docket.Runtime.Loop` owns checkpoint emission and commit/barrier
  semantics.
- Only the configured checkpoint callback owns durable host persistence.
- Only `Docket.Executor` implementations own node-side external execution.

## 5. Supervision And Process Topology

The existing runtime design proposes this default topology:

```text
Application supervisor
  Docket.Runtime.Registry
  Docket.Runtime.Supervisor
  Docket.ExecutorSupervisor

Runtime process per active run
  owns immutable runtime graph materialized from the supplied Docket.Graph
  keeps the current Docket.Run in memory
  owns step planning
  owns update barriers
  emits Docket.Checkpoint documents
  dispatches node execution to executors

Executor tasks or pools
  run node code
  report results back to Runtime
```

The Runtime is the only process allowed to mutate an active run. Public calls use
runtime module plus `run_id`, not PIDs.

Known public operations:

```elixir
Docket.run(runtime, graph, input, opts \\ [])
Docket.resume(runtime, graph, run, opts \\ [])
Docket.get_run(runtime, run_id, opts \\ [])
Docket.resolve_interrupt(runtime, run_id, interrupt_id, value, opts \\ [])
```

Known generated host wrappers:

```elixir
MyApp.Docket.run(graph, input, opts \\ [])
MyApp.Docket.resume(graph, run, opts \\ [])
MyApp.Docket.get_run(run_id, opts \\ [])
MyApp.Docket.resolve_interrupt(run_id, interrupt_id, value, opts \\ [])
```

The public API remains split because callers either provide input or provide a
saved run, but the implementation should collapse after the `Docket.Run`
document exists:

```elixir
run(runtime, graph, input, opts) ->
  run = build_initial_run(graph, input, opts)
  start_or_locate_runtime(runtime, graph, run, opts)

resume(runtime, graph, run, opts) ->
  validate_graph_match(graph, run)
  start_or_locate_runtime(runtime, graph, run, opts)
```

Resolved v1 contract:

- `use Docket` generates host wrappers for `run/3`, `resume/3`, `get_run/2`,
  and `resolve_interrupt/4`.
- Public calls use the configured runtime module plus `run_id`; PIDs stay
  internal.
- `run/4` and `resume/4` start or locate a Runtime through the same registry,
  supervisor, and Runtime launch path.
- `get_run/3` returns `{:error, :not_found}` when no active Runtime owns that
  `run_id`.
- Finished runtime processes may remain readable only while still registered. Once the
  Runtime exits or is evicted, `get_run/3` returns `{:error, :not_found}`.

## 6. Run Initialization Contract

Runtime initialization has a strict durable gate.

Known sequence:

```text
1. Host calls Docket.run(runtime, graph, input, opts).
2. Docket verifies/compiles the supplied Docket.Graph.
3. Docket builds a fresh Docket.Run document from the input.
4. Docket starts or locates the Runtime with that run document.
5. Runtime calls Docket.Runtime.Loop.init(runtime_graph, run, opts).
6. Loop sees a fresh run, initializes it, and emits a required
   :run_initialized checkpoint through the configured checkpoint callback.
7. The checkpoint handler upserts the host run record by run ID.
8. Only after the checkpoint handler returns :ok may node execution begin.
```

No node execution, event publication, interrupt, timer, or later step checkpoint
may occur before the initialization checkpoint succeeds.

If the initialization checkpoint fails, execution has not started and the caller
receives a checkpoint error.

Resolved v1 contract:

- `Docket.run/4` starts a Runtime in a `:starting` state.
- `Docket.run/4` creates the initial `Docket.Run`, then uses the same Runtime
  launch path as resume.
- `Loop.init/3` decides from the supplied `Docket.Run` whether it is
  initializing a fresh run or continuing a saved run. There is no explicit
  fresh/resumed mode.
- `Loop.init/3` produces an initialized public run document and synchronously
  emits the required `:run_initialized` checkpoint before any execution it is
  going to schedule.
- After the checkpoint handler returns `:ok`, the Runtime transitions to
  an active process state, schedules the first execution tick if graph execution
  can proceed, and `Docket.run/4` returns `{:ok, run}`.
- `Docket.run/4` is a start barrier, not a completion barrier.
- Initial checkpoint failure returns
  `{:error, %Docket.Error{type: :checkpoint_failed, phase: :run_initialized, reason: reason}}`.
- If the initial checkpoint fails, the starting Runtime exits and no active run is
  registered.
- `Docket.Run.status` describes graph execution state, not Runtime process
  liveness. Starting or resuming a Runtime must not blindly change a waiting,
  running, done, failed, or cancelled graph status.

## 7. Active Run Reads

`get_run/3` is part of the v1 execution API.

Known contract:

- It returns the current in-memory `Docket.Run` snapshot for an active run.
- It does not read host storage.
- It does not emit a checkpoint.
- It is observational.
- The latest accepted checkpoint remains the durable source of truth.

Resolved v1 contract:

- If no active Runtime owns the `run_id`, return `{:error, :not_found}`.
- `get_run/3` returns only the public `Docket.Run` snapshot.
- Debug metadata belongs in explicit debug/inspection helpers, not in the normal
  read API.

## 8. Resume Contract

Known contract:

- The host loads the durable `Docket.Run` document.
- The host loads the matching `Docket.Graph` document using `run.graph_id` and
  `run.graph_hash`.
- `Docket.resume/4` materializes `Docket.Runtime.Graph`.
- `Docket.resume/4` uses the same Runtime launch path as `run/4`, passing the
  durable run document instead of building a new one from input.
- The Runtime calls `Docket.Runtime.Loop.init/3`; the loop sees a saved
  `Docket.Run` and continues from it.
- Active runs stay on their original graph content hash.

Resolved v1 contract:

- `resume/4` requires `graph.id == run.graph_id` and
  `Docket.Graph.hash(graph) == run.graph_hash`.
- A mismatch returns
  `{:error, %Docket.Error{type: :graph_mismatch, reason: reason}}`.
- If the old graph content is unavailable, the host cannot resume that run and
  should return/fold that into
  `{:error, %Docket.Error{type: :graph_unavailable}}`.
- `resume/4` skips only the input-to-run prelude: it does not create a new
  `Docket.Run`, but it still passes through the same `Loop.init/3` durable
  barrier. The checkpoint handler upserts the run by ID, updating an existing
  host row if one exists.
- v1 does not support queue/remote active-task reconciliation. Local/task
  executor work that was not checkpointed is retried or failed according to retry
  policy after resume.
- Resume must not treat Runtime process startup as graph progress. It should
  preserve graph execution status unless the loop's scheduling rules actually
  advance, wait, complete, fail, or cancel the run.
- If a supplied run is already terminal, `Loop.init/3` should return the
  terminal public snapshot or a typed inactive-run error according to the public
  API contract, but it must not restart graph execution.

## 9. Superstep Contract

Each superstep has three phases.

```text
Plan
  choose active nodes from edge activations, interrupts, timers, and
  concurrency policy

Execution
  execute selected nodes against a consistent input snapshot

Update
  validate state updates, apply reducers, evaluate outgoing edge triggers,
  build events, emit checkpoint, and publish observational events
```

Visual flow:

```mermaid
flowchart TD
  runtime["Docket.Runtime<br/>owns one active run"]
  graph["Docket.Runtime.Graph<br/>immutable materialized graph"]
  run0["Docket.Run<br/>current committed run"]

  runtime --> graph
  runtime --> run0
  graph --> plan
  run0 --> plan

  subgraph loop["Docket.Runtime.Loop superstep"]
    plan["plan/3"]
    algorithm["Docket.Runtime.Algorithm<br/>select activations from edge signals, timers, interrupts"]
    decision{"Plan result"}
    dispatch["Docket.Runtime.Dispatcher<br/>execute selected node activations"]
    results["Normalized task results<br/>writes, interrupts, awaits, errors"]
    update["apply_results/4<br/>validate updates, reduce state, trigger edges"]
    events["Build Docket.Event list"]
    checkpoint["Build Docket.Checkpoint<br/>with next Docket.Run"]
    unchanged["Return current Docket.Run<br/>no step checkpoint"]

    plan --> algorithm --> decision
    decision -->|execute| dispatch --> results --> update --> events --> checkpoint
    decision -->|terminal / failure / interrupt| events
    decision -->|wait / no-op| unchanged
  end

  checkpoint --> callback["Configured Docket.Checkpoint callback"]
  callback -->|accepted| run1["Docket.Run<br/>next committed run"]
  callback -->|sync failure| run0
  unchanged --> runtime
  run1 --> runtime
  run1 --> next["Next superstep plan sees<br/>changed_channels from this update"]
```

The runtime graph stays immutable for the active run. Each superstep advances by
deriving a new public `Docket.Run` from the previous committed run. The next
plan sees channel changes only after the update barrier commits. For sync
checkpoints, that commit waits for callback acceptance; for async step
checkpoints, durable host persistence may trail the active in-memory run.

Known rules:

- Plan only sees the last completed channel state.
- Writes from one node in a superstep are invisible to other nodes in that same
  superstep.
- Edge activations emitted from Update activate nodes in the next superstep.
- State changes from Update are visible to edge guards, but do not directly
  activate arbitrary nodes without a source-completion edge candidate.
- Failed nodes prevent barrier commit by default.
- Termination occurs when there are no active nodes and no pending external
  work.

Resolved v1 contract:

- A planned activation is an internal struct containing task ID, public node ID,
  runtime graph node ID, superstep, attempt, input hash, idempotency key, readable
  channel snapshot, and deadline.
- v1 waits for all selected local/task executions to finish, interrupt, await,
  timeout, or fail before the update barrier.
- v1 does not support partial success. Any permanent node failure fails the
  superstep and commits no writes from that superstep.
- Pure no-op/wait decisions do not emit `:step_committed` checkpoints. Terminal,
  interrupt, failure, and externally resolved state changes do emit checkpoints.
- `max_supersteps` is a runtime limit. Exceeding it fails the run with a typed
  limit error before dispatching the next superstep.
- Multi-source edge barriers follow LangGraph `NamedBarrierValue` semantics.
  Each source-node completion is recorded in the barrier's seen set. The
  barrier fires when every source has completed at least once since the barrier
  last fired. Firing resets the seen set, so the barrier can fire again in
  cycles.
- Source completions are sticky across supersteps: the sources of a
  multi-source edge do not need to complete in the same superstep, and
  duplicate completions recorded before firing are idempotent.
- Barrier seen-state is committed `Docket.Run` state, so it is checkpointed and
  survives resume.

### 9.1 Worked Example: Local Executor And Guarded Edge

Example graph:

```text
input user_id
  -> FetchUser
       reads user_id
       queries host Repo through local node code
       writes user
       writes a_status

FetchUser -> PremiumStep
  guard reads committed user.premium_user == true

PremiumStep
  reads user
  writes b_status
```

Expected successful premium-user flow:

```text
Docket.run
  compile graph
  build initial Docket.Run
  start or locate Runtime with run
  Loop.init(runtime_graph, run, opts)
  Loop initializes blank state
  emit :run_initialized checkpoint
  return {:ok, run}

tick 0 / plan
  changed channels include input/start
  activate FetchUser

execute with Local executor
  Local calls FetchUser.call(state, config, context) directly
  FetchUser queries Repo
  returns updates: user, a_status

update
  validate writes
  reduce user
  reduce a_status
  evaluate FetchUser outgoing edges against committed state and changed fields
  changed channels = ["state:user", "state:a_status", "edge:edge_fetch_user_premium_step"]
  emit :step_committed checkpoint

tick 1 / plan
  premium_step edge is activated
  activate PremiumStep

execute with Local executor
  Local calls PremiumStep.call(state, config, context)
  returns update: b_status

update
  reduce b_status
  emit :step_committed checkpoint

tick 2 / plan
  no nodes activated
  no pending work
  emit :run_completed checkpoint
```

This example clarifies:

- `Docket.run/4` is a start barrier, not a completion barrier.
- the initialized run document must be checkpointed before `FetchUser` can run.
- `Docket.Executor.Local` can execute node code synchronously in the Runtime's
  current execution path.
- Edge activation and guard evaluation happen together at the update barrier.
  Successful source-node completion creates outgoing edge candidates; guards
  decide which candidates become activation signals for the next superstep.
- The guard sees the newly committed state and the changed field set from that
  update barrier.
- Terminal detection happens on the next plan after `PremiumStep` commits
  `b_status`, not inside the same update.

Resolved decisions from this example:

- Every public edge lowers to an ephemeral activation channel. The Runtime emits
  that activation after successful source-node completion, successful barrier
  commit, and guard approval when the edge has a guard. `FetchUser` never
  manually writes `edge:edge_fetch_user_premium_step`.
- Public fan-in and branch intent stays on ordinary graph records: multi-source
  edges and node-local branch groups over outgoing guarded edges. v1 keeps the
  runtime lowering simple: generated edge activation channels, compiled guards,
  and barrier semantics. The Runtime consumes the lowered `Docket.Runtime.Graph`,
  not public graph records directly.
- Guards that inspect nested values use durable data paths such as
  `path("user", ["premium_user"])`.
- Guard-false candidates emit no activation signal. v1 does not need a durable
  skipped event unless debug tracing is enabled.
- Node-facing config, the committed state snapshot, and runtime context are
  passed as separate arguments to node code. Internal compiled runtime details
  are passed to the executor, not to node code.
- `{:error, reason}` from a local node records a node failure, commits no writes
  from the superstep, and eventually emits a failed-run checkpoint if retries are
  exhausted.
- Raises, exits, and throws from local node code are normalized into node attempt
  failures.

## 10. Node Execution Contract

Known node callback shape:

```elixir
@callback call(state :: map(), config :: map(), context :: map()) ::
            {:ok, state_update :: map()}
            | {:interrupt, Docket.Interrupt.t()}
            | {:await, Docket.Await.t()}
            | {:error, term()}
```

Known call shape:

```elixir
state = committed_state_snapshot
config = node_config
context = %{
  run_id: run_id,
  node_id: node_id,
  superstep: step,
  source_versions: state_channel_versions,
  application: application_context,
  attempt: attempt,
  idempotency_key: key
}
```

Known output shape:

```elixir
{:ok, %{"field_name" => value}}
```

Resolved v1 contract:

- Keep two concepts, but name them clearly:
  - Public node: the user-authored node in `Docket.Graph`.
  - Runtime graph node: the internal compiler output used by the runtime.
- Replace the earlier proposed `NodeDef` name with
  `Docket.Runtime.Graph.Node`.
- When code must reference both public graph nodes and runtime graph nodes in the
  same scope, alias `Docket.Runtime.Graph.Node` as `RuntimeNode`.
- `context.node_id` is the public graph node ID.
- `state` is keyed by graph input/field ID, not by generic node port ID.
- the returned update map is keyed by graph field ID.
- The executor receives the runtime graph node and can inspect runtime IDs,
  generated channels, subscriptions, timeout, retry, and metadata.
- v1 node callbacks use `call/3`. Runtime structs may keep a function field for
  later, but v1 should compile-time/runtime reject unsupported function names.
- command-style returns remain a reserved extension point, but v1 rejects them
  with `:unsupported_command`.
- The runtime treats returned updates as graph field writes.
- Output validation rejects unknown update fields, invalid output values, excess
  updates, and oversized channel values before reducers run.

Node type contracts are schema-bearing:

```elixir
@callback config_schema() :: Docket.Schema.t()
```

The compiler validates config against `config_schema/0`. Runtime dispatch passes
the committed state snapshot to `call/3` and validates returned updates before
applying reducers.

## 11. Executor Contract

Known executor callback:

```elixir
@callback execute(
            task :: Docket.Run.TaskState.t(),
            node :: Docket.Runtime.Graph.Node.t(),
            state :: map(),
            config :: map(),
            context :: map(),
            opts :: keyword()
          ) ::
            {:ok, state_update :: map()}
            | {:interrupt, Docket.Interrupt.t()}
            | {:await, Docket.Await.t()}
            | {:error, term()}
```

Design-space executor families:

- `Docket.Executor.Local`
- `Docket.Executor.Task`
- `Docket.Executor.Queue`
- `Docket.Executor.Remote`

Known delivery semantics:

- `:sync`: result returned before update barrier
- `:async`: task is recorded; completion arrives later
- `:replay_only`: result must already exist in event history

Known effect rule:

- Docket cannot make arbitrary external effects exactly once.
- Docket supplies idempotency keys.
- Integrations must cooperate with idempotency and replay rules.

Resolved v1 contract:

- First implementation supports `Docket.Executor.Local` and
  `Docket.Executor.Task`.
- `Docket.Executor.Task` may use supervised tasks internally, but the superstep
  contract remains barrier-synchronous: the runtime collects all selected results
  before update.
- Queue, remote, replay-only, and late-completion protocols are post-v1.
- Timeouts become node attempt failures. Retry policy decides whether to
  dispatch another attempt or mark the failure permanent.
- Preserve the design space for a future durable queue with backpressure, where
  events or checkpoint envelopes can pile up until the host/app can consume them.

## 12. Guard Contract

Known guard rules:

- Guards filter edge activation candidates.
- Guards are deterministic.
- Guards are side-effect free.
- Guards can read committed state and the changed field set.
- Guards cannot call external services.

Design-space guard primitives:

- `changed(channel)`
- `version_at_least(channel, version)`
- `exists(channel)`
- `equals(channel, value)`
- `matches(channel, predicate)`
- `all([...])`
- `any([...])`
- `not(predicate)`
- custom application guard

Resolved v1 contract:

- Guards are durable data expressions, not function captures.
- v1 supports `changed/1`, `version_at_least/2`, `exists/1`, `equals/2`,
  `all/1`, `any/1`, `not/1`, and `path/2`.
- v1 does not support custom application guards.
- Guards are evaluated only for outgoing edge candidates produced by successful
  source-node completion or satisfied multi-source barriers.
- Change tracking is write-based, not value-diff-based, matching LangGraph
  channel versioning (`LastValue.update` bumps the channel version on every
  write with no equality check; subscribers trigger on version counters). A
  field is "changed" in a barrier when a committed write targeted it in that
  barrier, even if the written value equals the previous value.
- `changed/1` is true iff the referenced channel is in the changed set of the
  update barrier that produced the edge candidate. `version_at_least/2`
  compares monotonic per-channel version counters that advance once per
  committed write barrier.
- Paths read committed channel values only. Missing map keys/list indexes make
  `exists/1` false and make `equals/2` false rather than raising. The same
  applies to channels that have never been written: `exists/1`, `equals/2`, and
  `changed/1` are false. This is a deliberate deviation from LangGraph, which
  raises `EmptyChannelError` on empty channel reads; Docket guards are lax so
  model-generated partial state cannot crash guard evaluation.
- Invalid guard expressions are compile errors. Runtime guard evaluation errors
  fail the run with `:guard_evaluation_failed`.

Public guard constructors:

```elixir
Docket.Guard.changed(channel)
Docket.Guard.version_at_least(channel, version)
Docket.Guard.path(channel, path)
Docket.Guard.exists(ref)
Docket.Guard.equals(ref, value)
Docket.Guard.all(expressions)
Docket.Guard.any(expressions)
Docket.Guard.not(expression)
```

Each constructor returns a serializable guard expression. The compiler validates
the expression shape, field references, literal values, and path segments before
a graph can run. Runtime evaluation receives only newly committed state values,
channel versions, and the update barrier's changed field set.

## 13. Checkpoint Contract

Known checkpoint callback:

```elixir
@callback handle(
            checkpoint :: Docket.Checkpoint.t(),
            context :: Docket.Checkpoint.Context.t()
          ) :: :ok | {:error, term()}
```

Known checkpoint shape:

```elixir
%Docket.Checkpoint{
  type: :step_committed,
  delivery: :async,
  seq: checkpoint_seq,
  run: %Docket.Run{},
  events: [%Docket.Event{}],
  metadata: metadata,
  created_at: timestamp
}
```

Known rules:

- State-changing APIs gated by required sync checkpoints do not report success
  until the required checkpoint callback returns `:ok`.
- Checkpoint handlers should create or replace records by `Docket.Run.id`.
- Checkpoint handlers must not require a run row to exist before the first
  checkpoint.
- Checkpoint handlers should be safe to receive the same checkpoint more than
  once.
- Apps may persist `checkpoint.events` for replay, audit, debugging, or time
  travel.
- Apps that only need crash resume may persist only the latest
  `checkpoint.run`.

Resolved v1 contract:

- v1 supports `:sync` and `:async` checkpoint delivery modes.
- Sync checkpoints run in the Runtime execution path and must be accepted before
  the state transition is committed or the related public API reports success.
- Async checkpoints are submitted after the in-memory transition commits. The
  Runtime may continue to the next superstep while the host handles persistence,
  projection, or outbox delivery.
- Default sync checkpoint types are `:run_initialized`, `:interrupt_requested`,
  `:interrupt_resolved`, `:run_completed`, and `:run_failed`.
- Default async checkpoint type is `:step_committed`.
- A runtime may force additional checkpoint types to `:sync` when the host wants
  stronger crash-resume guarantees between ordinary supersteps.
- Docket does not enforce atomic persistence of `checkpoint.run` and
  `checkpoint.events`; the host owns that transaction inside the callback.
- The recommended durable host pattern is to persist the run and append/enqueue
  checkpoint events in one host transaction or outbox write. That outbox can
  provide backpressure and allow projections to catch up later.
- Checkpoints are only constructed and delivered between superstep phases: no
  executor work is in flight when a checkpoint callback runs. The v1
  barrier-synchronous superstep makes this structural (Plan, then dispatch and
  await all selected executions, then apply updates in memory, then
  checkpoint). This matches the LangGraph loop ordering, where `checkpointer.put`
  runs only after all tasks for the step have completed and writes are applied.
- v1 does not automatically retry failed sync checkpoint callbacks. The current
  operation returns a typed checkpoint error and no later runtime progress is
  acknowledged.
- On sync checkpoint failure the Runtime discards the uncommitted in-memory
  transition, keeps the previous committed `Docket.Run`, reports the typed
  checkpoint error, and stops. It does not attempt a `:run_failed` checkpoint
  through the same failing sink. The host's durable state remains the last
  successful checkpoint, and resume re-executes the uncommitted superstep.
- Async checkpoint callback failures are reported through debug or
  test-observable surfaces in v1 and may be retried by the host's outbox or
  callback implementation, but they do not block the active Runtime. First-class
  telemetry is post-v1.
- v1 checkpoint types are `:run_initialized`, `:step_committed`,
  `:interrupt_requested`, `:interrupt_resolved`, `:run_completed`, and
  `:run_failed`.

## 14. Failure And Recovery Contract

Known default failure policy:

```text
If any node in a superstep fails permanently, the superstep fails and no writes
from that superstep are committed.
```

Known recovery rules:

- Runtime crash recovery starts from the latest saved `Docket.Run` document.
- If memory changed but checkpoint did not succeed, callers must not have
  received success.
- Completed effects are reused from event history when the host saved it.
- Lost in-flight effects are retried or marked unknown according to executor
  policy.

Resolved v1 contract:

- Node returns of `{:error, reason}`, exceptions, exits, throws, timeouts,
  output validation failures, and guard evaluation failures are node attempt
  failures.
- Retry policy decides whether an attempt failure is retryable.
- Exhausting retry policy makes the failure permanent.
- Permanent superstep failure commits no writes from that superstep and emits a
  `:run_failed` checkpoint with node failure events.
- v1 local/task in-flight work that was not checkpointed is retried or fails
  according to retry policy after resume.
- Recovery after a crash or sync checkpoint failure re-executes the entire
  uncommitted superstep. Idempotency keys are stable across that re-execution
  because superstep numbers and attempt counters are committed run state: an
  attempt counter only advances inside a barrier that commits, so a
  never-committed superstep runs again with the same keys and cooperating
  integrations deduplicate the external effects.
- Post-v1 design space: persist successful sibling-node writes attached to the
  previous checkpoint (LangGraph's `put_writes` pending-writes pattern) so a
  re-executed superstep does not redo nodes that already succeeded. v1
  re-executes the whole superstep.

## 15. Interrupt Contract

Known interrupt return shape:

```elixir
{:interrupt,
 %Docket.Interrupt{
   id: interrupt_id,
   node_id: node_id,
   prompt: prompt,
   schema: schema,
   resume_channel: channel
 }}
```

Known rules:

- A run enters `:waiting` if no other nodes can proceed.
- Resolving an interrupt writes to the configured resume channel.
- Interrupt resolution is a durable event.
- The resume-channel write activates subscribers in the next superstep.

Resolved v1 contract:

- Interrupt schemas use the same schema representation as channel update
  schemas.
- `resolve_interrupt/5` validates the interrupt is open, writes the value to the
  configured resume channel, emits an `:interrupt_resolved` checkpoint, schedules
  the next tick, and returns after the checkpoint is accepted.
- Unknown or closed interrupts return `{:error, :not_found}`.
- Authorization remains host-owned and should happen before calling
  `resolve_interrupt/5`.
- Interrupt timeouts and deadlines are post-v1. A v1 interrupt waits
  indefinitely until the host resolves it; hosts that need expiry enforce it
  themselves and resolve or fail the run through public APIs.

## 16. Testing Contract

Known testability requirement:

- Ordinary graph execution tests should not depend on BEAM process scheduling.
- Ordinary graph execution tests should not need `Process.sleep/1`.
- Docket should expose an inline runtime for tests.
- The inline runtime executes in the calling test process.
- The inline runtime uses the same execution loop as the supervised Runtime.
- Supervised tests are reserved for lifecycle, crash recovery, late completion,
  remote executor, timer, and async-await behavior.

Known proposed test helpers:

```elixir
Docket.Test.run_inline(graph, input, opts \\ [])
Docket.Test.step_inline(run, opts \\ [])
```

Resolved v1 contract:

- Test helpers are `Docket.Test.run_inline/3` and `Docket.Test.step_inline/2`.
- Return shape:

  ```elixir
  {:ok, Docket.Run.t(), [Docket.Checkpoint.t()]}
  | {:error, Docket.Error.t(), [Docket.Checkpoint.t()]}
  ```

- Inline helpers use the configured checkpoint sink and always wait for sync
  checkpoints. By default they may also drain async checkpoints before returning
  so ordinary semantic tests can assert a complete checkpoint sequence without
  `Process.sleep/1`.
- Inline tests cover graph semantics, checkpoint ordering, reducers, guards,
  interrupts, and failure policy. Supervised tests cover lifecycle, process
  crashes, and task execution behavior.

## 17. Resolved Contract Index

The highest-priority v1 decisions are:

1. Shared execution loop module/function boundaries.
2. Runtime GenServer call/message API.
3. Runtime start, resume, shutdown, and inactive-run semantics.
4. Checkpoint callback execution and retry policy.
5. Executor sync/task protocol, with queue/remote/late completion deferred.
6. Guard expression representation.
7. Node input/output validation and error shapes.
8. Compiler-generated edge signal representation.
9. Node metadata/config visibility during execution.
10. Local executor error versus exception handling.
11. Inline test runtime API.

Post-v1 design space to preserve:

- Durable queue/backpressure support for checkpoint/event envelopes.
- Queue and remote executor delivery, completion, and reconciliation protocol.
- Replay-only execution from persisted event history.
- Commands emitted by nodes and interpreted by the host.
- Custom application guards.
- Partial success policies.
- Dynamic branch destinations, command-driven routing, and richer join policies
  beyond compiler-generated activation/barrier channels.
