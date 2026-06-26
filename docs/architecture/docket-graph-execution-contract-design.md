# Docket: Graph Execution Contract Design

Status: working draft
Date: 2026-06-25

Related documents:

- `docs/architecture/docket-runtime-design.md`
- `docs/architecture/docket-graph-construction-design.md`
- `docs/architecture/docket-v1-test-suite-design.md`

## 1. Purpose

This document narrows Docket's graph execution contract.

It focuses on the boundary between:

- the supervised runtime
- the Runner process
- the shared execution core
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

- One active graph run is owned by one Runner process.
- The Runner consumes an internal `Docket.RuntimeGraph` materialized from a
  canonical `Docket.Graph`.
- The host application stores `Docket.Graph` and `Docket.Run` documents.
- `Docket.Run` is the public restorable run snapshot.
- The Runner owns the materialized runtime graph and mutable in-memory run state
  while a run is active.
- Nodes are logical actors, not OTP processes by default.
- Nodes communicate only through channel writes.
- Channel updates become visible only after the update barrier.
- Runtime progress happens through repeated Plan -> Execution -> Update
  supersteps.
- Checkpoints are the durable boundary for committed runtime moments.
- PubSub, streams, telemetry, and UI overlays are projections, not durable truth.

This contract resolves the highest-risk gaps before implementation:

- Exact public and internal module boundaries for the shared execution core.
- Exact supervised Runner callback/call/message API.
- Exact state structs that are public, internal, or test-visible.

## 3. Execution Layers

The current design implies three execution layers.

```text
Execution core
  shared plan/execute/update/checkpoint-building logic

Supervised Runner shell
  GenServer process that owns one active run in production

Inline test shell
  calling test process that drives the same execution core
```

The execution core must not be duplicated. The supervised Runner and inline test
runner should call the same planning, validation, reducer, update, and
checkpoint-building code.

Resolved v1 contract:

- `Docket.Runner.Core` is the internal execution state machine.
- `Docket.Runner.Algorithm` holds the pure planning, guard evaluation, reducer,
  write validation, and termination helpers used by the core.
- `Docket.Runner` is the supervised GenServer shell only.
- `Docket.Test` is the only test-facing facade for inline execution; tests do
  not call `Docket.Runner.Core` directly.
- The core API is internal. Test helpers may return checkpoints and public
  `Docket.Run` snapshots, but they do not expose mutable core state as a public
  contract.

## 4. LangGraph Reference: Execution Core Boundaries

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
- Put mutable step state in one loop/runner state object.
- Keep task selection and write application as separate functions from the
  process shell.
- Keep task execution/commit mechanics separate from planning and channel
  reducers.
- Let the supervised Runner and inline test runner share the same execution
  core.

Possible Docket translation:

```text
Docket
  public facade functions such as run/resume/get_run/resolve_interrupt

Docket.Runner
  GenServer shell for one active run

Docket.Runner.Core
  internal execution state machine shared by GenServer and inline tests

Docket.Runner.Algorithm
  plan, prepare activations, apply writes, reduce channels, detect termination

Docket.Runner.Dispatcher
  dispatch selected node executions and collect outputs
```

Resolved v1 contract:

- Keep `Docket.Runner.Core` and `Docket.Runner.Algorithm` separate.
- Keep the public adapter boundary as `Docket.Executor`.
- Use a small internal dispatcher under the runner/core if needed, but do not
  make it a host extension point.
- The inline runner goes through `Docket.Test`, which delegates to the same core.

## 4.1 Internal Core API Shape

The concrete internal API should make the boundary between process shell,
state machine, and pure execution algorithms explicit.

`Docket.Runner` is the only GenServer shell. It owns the process mailbox,
registry name, lifecycle calls, task result messages, timeout messages, and
tick scheduling. It should be thin: translate calls/messages into core calls,
dispatch selected work, track in-flight process refs, and handle any async
checkpoint completion messages returned by the core.

`Docket.Runner.Core` owns mutable run progression, but it does not own a process.
It is internal and shared by the supervised Runner and `Docket.Test`.

```elixir
Docket.Runner.Core.init(runtime_graph, run, opts)
Docket.Runner.Core.plan(core, opts)
Docket.Runner.Core.apply_results(core, task_results, opts)
Docket.Runner.Core.resolve_interrupt(core, interrupt_id, value, opts)
Docket.Runner.Core.to_run(core)
```

`Core.init/3` is the single core entrypoint for a live run. It receives a
public `Docket.Run` document and derives what to do from that run's structured
Docket-owned `%Docket.Run.State{}`. A normal `Docket.run/4` call builds a run
with blank Docket-owned state from the graph input first; `Docket.resume/4`
passes the durable run document loaded by the host. Blank run state means the
core initializes channels, tasks, frontier, and timestamps. Existing run state
means the core hydrates from that state and picks up from the recorded graph
execution status.

Expected return shapes:

```elixir
{:ok, core}
| {:ok, core, term()}
| {:error, Docket.Error.t()}
```

The extra return value is internal and should be limited to concrete values the
Runner already needs, such as selected activations or async checkpoint refs to
observe or drain. The core does not expose staged transitions as an API.

When a transition requires a checkpoint, `Docket.Runner.Core` builds the
checkpoint and calls the configured `Docket.Checkpoint` callback from runtime
configuration such as `use Docket, checkpoint: MyApp.DocketCheckpoint`. For
`:sync` checkpoints, the core calls the callback before installing the
transition. If the sync checkpoint fails, the current core is left unchanged and
the caller receives a typed checkpoint error. For `:async` checkpoints, the core
commits the in-memory transition and invokes the configured checkpoint callback
asynchronously. Async checkpoint failure is observable but does not roll back
the active in-memory run.

`Docket.Runner.Algorithm` holds deterministic helper functions. It has no
mailbox, no checkpoint side effects, and no direct executor calls.

```elixir
Docket.Runner.Algorithm.plan(graph, run, opts)
Docket.Runner.Algorithm.prepare_activations(graph, run, plan, opts)
Docket.Runner.Algorithm.evaluate_guard(expr, context)
Docket.Runner.Algorithm.validate_output(graph, activation, output, opts)
Docket.Runner.Algorithm.collect_writes(graph, activations, task_results, opts)
Docket.Runner.Algorithm.apply_writes(graph, run, writes, opts)
Docket.Runner.Algorithm.detect_terminal(graph, run, opts)
Docket.Runner.Algorithm.build_checkpoint(graph, run, events, opts)
```

`Docket.Runner.Dispatcher`, if introduced, is also internal. It prepares
`Docket.Node.Input`, calls the configured executor for each activation, handles
local task timeout mechanics, and returns normalized task results to the core.
It does not evaluate guards, apply reducers, or commit checkpoints.

```elixir
Docket.Runner.Dispatcher.dispatch(activations, graph, run, opts)
```

## 4.2 Runner Module Responsibility Index

The runner modules should keep narrow ownership boundaries. When implementation
pressure makes a responsibility feel convenient in two places, use this index to
decide where it belongs.

| Module | Owns | Must not own |
| --- | --- | --- |
| `Docket` | Public facade functions such as `run/4`, `resume/4`, `get_run/3`, and `resolve_interrupt/5`; runtime module lookup; public error normalization. | Mutable run state, graph execution algorithms, executor dispatch details, checkpoint persistence, or public exposure of Runner PIDs. |
| Generated `MyApp.Docket` | Host-friendly wrappers around the configured runtime; compile-time/runtime configuration for checkpoint, executor, limits, and supervision names. | Per-run mutable state, graph compilation internals, node execution, or custom behavior that diverges from `Docket` semantics. |
| `Docket.RunnerSupervisor` | Starting, restarting, and terminating one `Docket.Runner` process per active run according to the runtime supervision strategy. | Run mutation, planning, checkpoint construction, executor dispatch, or graph storage. |
| `Docket.RunnerRegistry` | Mapping runtime/run identity to the active Runner process; enforcing one active Runner owner per run ID. | Public run reads from storage, run mutation, checkpoint handling, or eviction policy beyond registration/liveness. |
| `Docket.Runner` | GenServer shell for one active run; mailbox ownership; lifecycle calls; tick scheduling; task result and timeout messages; tracking in-flight dispatcher refs and async checkpoint completion messages. | Guard evaluation, reducer logic, write validation, graph lowering, public graph storage, checkpoint callback execution, or direct durable persistence. |
| `Docket.Runner.State` | Process-local shell state: runtime config, compiled runtime graph, current `Docket.Runner.Core` value, in-flight dispatcher refs, and async checkpoint refs being observed or drained. | Durable public run shape, channel reducer semantics, guard expression semantics, or host-owned metadata interpretation. |
| `Docket.Runner.Core` | Processless execution state machine; initialization from a supplied `Docket.Run`; planning transitions; applying normalized task results; interrupt resolution; checkpoint emission/barrier semantics; converting internal state to `Docket.Run`. | GenServer callbacks, process registry, direct executor calls, telemetry/PubSub publication, or direct host storage outside the configured checkpoint callback. |
| `Docket.Runner.Algorithm` | Deterministic graph execution helpers: plan, prepare activations, evaluate guards, validate outputs, collect writes, apply reducers, detect termination, and build checkpoint data. | Mutable process state, wall-clock reads except injected timestamps, random values, external services, executor calls, checkpoint side effects, or host callbacks. |
| `Docket.Runner.Dispatcher` | Internal task dispatch mechanics; building `Docket.Node.Input`; calling the configured `Docket.Executor`; local/task timeout handling; normalizing node returns, raises, exits, and throws into task results. | Planning, guard evaluation, reducer application, checkpoint commit, retry policy decisions beyond attempt mechanics, or public adapter configuration. |
| `Docket.Executor` implementations | The adapter boundary for executing one compiled node activation locally, in a task, or later through queue/remote systems. | Mutating `Docket.Run`, applying writes, emitting checkpoints, deciding graph termination, or reading uncommitted superstep writes. |
| `Docket.Test` | Test-facing inline runner facade; drives the same core transitions in the calling process; returns public runs and accepted checkpoints for assertions. | A second execution interpreter, direct mutation of core internals as public API, GenServer lifecycle semantics, or behavior that supervised Runner cannot share. |

Additional boundary rules:

- Only `Docket.Runner` owns a live process for a run.
- Only `Docket.Runner.Core` owns internal run progression.
- Only `Docket.Runner.Algorithm` owns deterministic execution semantics.
- Only `Docket.Runner.Core` owns checkpoint emission and commit/barrier
  semantics.
- Only the configured checkpoint callback owns durable host persistence.
- Only `Docket.Executor` implementations own node-side external execution.

## 5. Supervision And Process Topology

The existing runtime design proposes this default topology:

```text
Application supervisor
  Docket.RunnerRegistry
  Docket.RunnerSupervisor
  Docket.ExecutorSupervisor

Runner process per active run
  owns immutable runtime graph materialized from the supplied Docket.Graph
  owns Run.State
  owns step planning
  owns update barriers
  emits Docket.Checkpoint documents
  dispatches node execution to executors

Executor tasks or pools
  run node code
  report results back to Runner
```

The Runner is the only process allowed to mutate an active run. Public calls use
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
  start_or_locate_runner(runtime, graph, run, opts)

resume(runtime, graph, run, opts) ->
  validate_graph_match(graph, run)
  start_or_locate_runner(runtime, graph, run, opts)
```

Resolved v1 contract:

- `use Docket` generates host wrappers for `run/3`, `resume/3`, `get_run/2`,
  and `resolve_interrupt/4`.
- Public calls use the configured runtime module plus `run_id`; PIDs stay
  internal.
- `run/4` and `resume/4` start or locate a Runner through the same registry,
  supervisor, and Runner launch path.
- `get_run/3` returns `{:error, :not_found}` when no active Runner owns that
  `run_id`.
- Finished runners may remain readable only while still registered. Once the
  Runner exits or is evicted, `get_run/3` returns `{:error, :not_found}`.

## 6. Run Initialization Contract

Runner initialization has a strict durable gate.

Known sequence:

```text
1. Host calls Docket.run(runtime, graph, input, opts).
2. Docket verifies/compiles the supplied Docket.Graph.
3. Docket builds a Docket.Run document with blank Docket-owned state.
4. Docket starts or locates the Runner with that run document.
5. Runner calls Docket.Runner.Core.init(runtime_graph, run, opts).
6. Core sees blank run state, initializes the run, and emits a required
   :run_initialized checkpoint through the configured checkpoint callback.
7. The checkpoint handler upserts the host run record by run ID.
8. Only after the checkpoint handler returns :ok may node execution begin.
```

No node execution, event publication, interrupt, timer, or later step checkpoint
may occur before the initialization checkpoint succeeds.

If the initialization checkpoint fails, execution has not started and the caller
receives a checkpoint error.

Resolved v1 contract:

- `Docket.run/4` starts a Runner in a `:starting` state.
- `Docket.run/4` creates the initial `Docket.Run`, then uses the same Runner
  launch path as resume.
- `Core.init/3` decides from `run.state` whether it is initializing blank state
  or hydrating existing state. There is no explicit fresh/resumed mode.
- `Core.init/3` produces an initialized public run snapshot and synchronously
  emits the required `:run_initialized` checkpoint before any execution it is
  going to schedule.
- After the checkpoint handler returns `:ok`, the Runner transitions to
  an active process state, schedules the first execution tick if graph execution
  can proceed, and `Docket.run/4` returns `{:ok, run}`.
- `Docket.run/4` is a start barrier, not a completion barrier.
- Initial checkpoint failure returns
  `{:error, %Docket.Error{type: :checkpoint_failed, phase: :run_initialized, reason: reason}}`.
- If the initial checkpoint fails, the starting Runner exits and no active run is
  registered.
- `Docket.Run.status` describes graph execution state, not Runner process
  liveness. Starting or resuming a Runner must not blindly change a waiting,
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

- If no active Runner owns the `run_id`, return `{:error, :not_found}`.
- `get_run/3` returns only the public `Docket.Run` snapshot.
- Debug metadata belongs in explicit debug/inspection helpers, not in the normal
  read API.

## 8. Resume Contract

Known contract:

- The host loads the durable `Docket.Run` document.
- The host loads the matching `Docket.Graph` document using `run.graph_id` and
  `run.graph_version`.
- `Docket.resume/4` materializes `Docket.RuntimeGraph`.
- `Docket.resume/4` uses the same Runner launch path as `run/4`, passing the
  durable run document instead of building a new one from input.
- The Runner calls `Docket.Runner.Core.init/3`; the core sees existing
  Docket-owned state and hydrates from it.
- Active runs stay on their original graph version.

Resolved v1 contract:

- `resume/4` requires `graph.id == run.graph_id` and
  `graph.version == run.graph_version`.
- A mismatch returns
  `{:error, %Docket.Error{type: :graph_mismatch, reason: reason}}`.
- If the old graph version is unavailable, the host cannot resume that run and
  should return/fold that into
  `{:error, %Docket.Error{type: :graph_version_unavailable}}`.
- `resume/4` skips only the input-to-run prelude: it does not create a new
  `Docket.Run`, but it still passes through the same `Core.init/3` durable
  barrier. The checkpoint handler upserts the run by ID, updating an existing
  host row if one exists.
- v1 does not support queue/remote active-task reconciliation. Local/task
  executor work that was not checkpointed is retried or failed according to retry
  policy after resume.
- Resume must not treat Runner process startup as graph progress. It should
  preserve graph execution status unless the core's scheduling rules actually
  advance, wait, complete, fail, or cancel the run.
- If a supplied run is already terminal, `Core.init/3` should return the
  terminal public snapshot or a typed inactive-run error according to the public
  API contract, but it must not restart graph execution.

## 9. Superstep Contract

Each superstep has three phases.

```text
Plan
  choose candidate nodes from changed channels, guards, interrupts, timers, and
  concurrency policy

Execution
  execute selected nodes against a consistent input snapshot

Update
  validate writes, apply channel reducers, build events, emit checkpoint, and
  publish observational events
```

Known rules:

- Plan only sees the last completed channel state.
- Writes from one node in a superstep are invisible to other nodes in that same
  superstep.
- Channel updates from Update activate nodes in the next superstep.
- Failed nodes prevent barrier commit by default.
- Termination occurs when there are no active nodes and no pending external
  work.

Resolved v1 contract:

- A planned activation is an internal struct containing task ID, public node ID,
  compiled node ID, superstep, attempt, input hash, idempotency key, readable
  channel snapshot, and deadline.
- v1 waits for all selected local/task executions to finish, interrupt, await,
  timeout, or fail before the update barrier.
- v1 does not support partial success. Any permanent node failure fails the
  superstep and commits no writes from that superstep.
- Pure no-op/wait decisions do not emit `:step_committed` checkpoints. Terminal,
  interrupt, failure, and externally resolved state changes do emit checkpoints.
- `max_supersteps` is a runtime limit. Exceeding it fails the run with a typed
  limit error before dispatching the next superstep.

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
  start or locate Runner with run
  Core.init(runtime_graph, run, opts)
  Core initializes blank state
  emit :run_initialized checkpoint
  return {:ok, run}

tick 0 / plan
  changed channels include input/start
  activate FetchUser

execute with Local executor
  Local calls FetchUser.call(input) directly
  FetchUser queries Repo
  returns writes: user, a_status

update
  validate writes
  reduce user
  reduce a_status
  changed channels = ["state:user", "state:a_status", "edge:edge_fetch_user_premium_step"]
  emit :step_committed checkpoint

tick 1 / plan
  premium_step edge is activated
  guard reads user.premium_user == true
  activate PremiumStep

execute with Local executor
  Local calls PremiumStep.call(input)
  returns b_status

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
- the initialized run snapshot must be checkpointed before `FetchUser` can run.
- `Docket.Executor.Local` can execute node code synchronously in the Runner's
  current execution path.
- Edge activation and guard evaluation are related but distinct. The edge signal
  can activate `PremiumStep` as a candidate, while the guard decides whether the
  candidate actually runs.
- The guard sees only committed channel state from the previous update barrier.
- Terminal detection happens on the next plan after `PremiumStep` commits
  `b_status`, not inside the same update.

Resolved decisions from this example:

- Every public edge lowers to an ephemeral activation channel and a
  compiler-generated system write on successful source-node completion.
  `FetchUser` never manually writes `edge:edge_fetch_user_premium_step`.
- Branch and join sugar are canonical graph concepts, but v1 keeps the runtime
  lowering simple: generated edge activation channels, compiled guards, and
  barrier channels. The Runner consumes the lowered `Docket.RuntimeGraph`, not
  branch/join records directly.
- Guards that inspect nested values use durable data paths such as
  `path("user", ["premium_user"])`.
- Guard-false candidates are skipped for that plan. v1 does not need a durable
  skipped event unless debug tracing is enabled.
- Node-facing config from graph construction is included in `Docket.Node.Input`.
  Internal compiled runtime details are passed to the executor, not to node code.
- `{:error, reason}` from a local node records a node failure, commits no writes
  from the superstep, and eventually emits a failed-run checkpoint if retries are
  exhausted.
- Raises, exits, and throws from local node code are normalized into node attempt
  failures.

## 10. Node Execution Contract

Known node callback shape:

```elixir
@callback call(Docket.Node.Input.t()) ::
            {:ok, Docket.Node.Output.t()}
            | {:interrupt, Docket.Interrupt.t()}
            | {:await, Docket.Await.t()}
            | {:error, term()}
```

Known input shape:

```elixir
%Docket.Node.Input{
  run_id: run_id,
  node_id: node_id,
  superstep: step,
  values: readable_channel_values,
  versions: readable_channel_versions,
  context: application_context,
  config: node_config,
  attempt: attempt,
  idempotency_key: key
}
```

Known output shape:

```elixir
%Docket.Node.Output{
  writes: [Docket.Write.t()],
  commands: [Docket.Command.t()],
  metadata: map()
}
```

Resolved v1 contract:

- Keep two concepts, but name them clearly:
  - Public node: the user-authored node in `Docket.Graph`.
  - Compiled node: the internal compiler output used by the runner.
- Replace the earlier proposed `NodeDef` name with
  `Docket.RuntimeGraph.CompiledNode`.
- `Docket.Node.Input.node_id` is the public graph node ID.
- The executor receives the compiled node and can inspect runtime IDs, generated
  channels, subscriptions, write permissions, timeout, retry, and metadata.
- v1 node callbacks use `call/1`. Runtime structs may keep a function field for
  later, but v1 should compile-time/runtime reject unsupported function names.
- `commands` remain on `Docket.Node.Output` as a reserved extension point, but
  v1 rejects non-empty commands with `:unsupported_command`.
- Output validation rejects undeclared channels, unauthorized writes, invalid
  update shapes, excess writes, and oversized channel values before reducers run.

## 11. Executor Contract

Known executor callback:

```elixir
@callback execute(
            task :: Docket.Run.TaskState.t(),
            node :: Docket.RuntimeGraph.CompiledNode.t(),
            input :: Docket.Node.Input.t(),
            opts :: keyword()
          ) ::
            {:ok, Docket.Node.Output.t()}
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
  contract remains barrier-synchronous: the runner collects all selected results
  before update.
- Queue, remote, replay-only, and late-completion protocols are post-v1.
- Timeouts become node attempt failures. Retry policy decides whether to
  dispatch another attempt or mark the failure permanent.
- Preserve the design space for a future durable queue with backpressure, where
  events or checkpoint envelopes can pile up until the host/app can consume them.

## 12. Guard Contract

Known guard rules:

- Guards filter node activations.
- Guards are deterministic.
- Guards are side-effect free.
- Guards can read channel state.
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
- Paths read committed channel values only. Missing map keys/list indexes make
  `exists/1` false and make `equals/2` false rather than raising.
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
the expression shape, channel references, literal values, and path segments
before a graph can run. Runtime evaluation receives only committed channel
values, channel versions, and the previous step's changed channel set.

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
- Sync checkpoints run in the Runner execution path and must be accepted before
  the state transition is committed or the related public API reports success.
- Async checkpoints are submitted after the in-memory transition commits. The
  Runner may continue to the next superstep while the host handles persistence,
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
- v1 does not automatically retry failed sync checkpoint callbacks. The current
  operation returns a typed checkpoint error and no later runtime progress is
  acknowledged.
- Async checkpoint callback failures are reported through debug or
  test-observable surfaces in v1 and may be retried by the host's outbox or
  callback implementation, but they do not block the active Runner. First-class
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

- Runner crash recovery starts from the latest saved `Docket.Run` document.
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

## 16. Testing Contract

Known testability requirement:

- Ordinary graph execution tests should not depend on BEAM process scheduling.
- Ordinary graph execution tests should not need `Process.sleep/1`.
- Docket should expose an inline runner for tests.
- The inline runner executes in the calling test process.
- The inline runner uses the same execution core as the supervised Runner.
- Supervised tests are reserved for lifecycle, crash recovery, late completion,
  remote executor, timer, and async-await behavior.

Known proposed test helpers:

```elixir
Docket.Test.run_inline(graph, input, opts \\ [])
Docket.Test.step_inline(state_or_run, opts \\ [])
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

1. Shared execution core module/function boundaries.
2. Runner GenServer call/message API.
3. Runner start, resume, shutdown, and inactive-run semantics.
4. Checkpoint callback execution and retry policy.
5. Executor sync/task protocol, with queue/remote/late completion deferred.
6. Guard expression representation.
7. Node input/output validation and error shapes.
8. Compiler-generated edge signal representation.
9. Node metadata/config visibility during execution.
10. Local executor error versus exception handling.
11. Inline test runner API.

Post-v1 design space to preserve:

- Durable queue/backpressure support for checkpoint/event envelopes.
- Queue and remote executor delivery, completion, and reconciliation protocol.
- Replay-only execution from persisted event history.
- Commands emitted by nodes and interpreted by the host.
- Custom application guards.
- Partial success policies.
- Dynamic branch destinations, command-driven routing, and richer join policies
  beyond compiler-generated activation/barrier channels.
