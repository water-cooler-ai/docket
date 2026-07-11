# Docket Runtime: Design Rationale And Background

Status: design rationale and background, not a current spec
Date: 2026-06-25

Release note: resident per-run process and host-checkpoint sections describe
the `0.0.1` driver. v0.1.0 retains the graph/runtime semantics and processless
test helpers but makes a `Docket.Backend` the required production lifecycle
owner; the operational transition spec is authoritative.

Implementation note: this document records the research basis, mental
model, goals, and alternatives considered for the runtime. Concrete
structs and APIs are canonical in code under `lib/docket/` and in
`docket-graph-execution-contract-design.md` and `docket-compiler-design.md`;
where an earlier draft sketched a struct, this document now points at the real
module instead.

## 1. Executive Summary

This document proposes a standalone Elixir library for durable, graph-based
workflow execution inspired by Pregel, Apache Beam, LangGraph, Temporal,
Flink, Timely Dataflow, and OTP.

The library is not WaterCooler-specific. WaterCooler would be the first
consumer, but the abstraction should be useful to any Elixir system that needs
to run cyclic, parallel, stateful graphs with checkpoints, streaming updates,
human interrupts, remote execution, and replay.

The central idea is:

```text
One execution shell normally drives one active graph run. Durable backends may
briefly have an expired holder overlap the current holder after a claim steal;
the current claim token and checkpoint sequence fence durable commits.

Inside that shell, graph vertices are logical actors.
Actors read channels, write updates, and execute in bulk-synchronous steps.
Channel updates become visible only at the next step barrier.
Durable checkpoints make replay and recovery possible.
```

The runtime should not model every vertex as an OTP process by default. A vertex
is a logical actor. OTP processes are used for runtime ownership, concurrency,
remote execution, supervision, and fault containment.

The recommended architecture is:

```text
Pregel-style execution loop
  + Beam-style graph/channel/runtime abstractions
  + Temporal-style event history and replay discipline
  + Flink-style checkpoint barrier thinking
  + Timely-style logical timestamps and progress tracking
  + OTP supervision, registries, tasks, telemetry, and backpressure
```

## 2. Why This Exists

Step sequencers are easy to build and useful for early workflow systems. They
break down once the workflow is no longer a simple ordered list:

- Multiple downstream steps should run from the same result.
- Multiple upstream branches should converge.
- A step should loop until a channel or condition stabilizes.
- A human or external system should resume a graph later.
- A long-running task should checkpoint progress.
- An agent should write partial outputs while other actors continue.
- A graph should support replay, debugging, time travel, and interrupts.
- State should be reduced by declared channel semantics, not implicit ad hoc
  map mutation.
- Failure handling should be local, inspectable, and retryable.

A robust graph runtime needs a different center of gravity:

- graph vertices instead of list positions
- channels instead of one current step pointer
- activations instead of imperative advancement
- reducers instead of informal shared state mutation
- checkpoints instead of hot-path-only process memory
- durable events instead of scattered progress logs
- explicit external effects instead of hidden side effects

## 3. Research Basis

This design is not a direct port of any one system. It borrows selected ideas
from several mature designs.

### 3.1 Pregel

Google Pregel introduced a vertex-centric graph computation model. Programs run
in synchronized supersteps. In each superstep, vertices receive messages from
the previous step, compute locally, emit messages for future steps, and may halt.

Useful ideas for this library:

- Think like a vertex.
- Bulk synchronous parallel execution.
- Messages written in one step are consumed in the next step.
- Termination is global: no active vertices and no pending messages.
- Checkpoints make recovery practical.

Source: https://research.google/pubs/pregel-a-system-for-large-scale-graph-processing/

### 3.2 LangGraph Pregel

LangGraph exposes Pregel as its low-level runtime. Its docs describe actors,
channels, and repeated Plan, Execution, Update phases. Actors subscribe to
channels, read channel values, and write channel updates. Writes are invisible
until the next execution step.

Useful ideas for this library:

- Actors plus channels are the runtime primitive.
- High-level graph APIs compile down to a Pregel runtime.
- Channel types define value type, update type, and update function.
- Built-in channel families include last value, topics, and aggregates.
- Cycles are first-class.
- Interrupts, checkpoint callbacks, streams, and retry policies belong around
  the runtime loop.

Source: https://docs.langchain.com/oss/python/langgraph/pregel

### 3.3 Apache Beam

Apache Beam defines pipelines as graphs of PTransforms over PCollections. The
same abstractions cover bounded and unbounded data. Beam also has a runtime model,
windowing, triggers, state, timers, side inputs, multiple outputs, and splittable
work.

Useful ideas for this library:

- A graph definition should be portable across execution backends.
- A runtime should execute a graph without changing graph semantics.
- Data collections/channels should declare boundedness, accumulation, and
  windowing semantics.
- User functions should satisfy serialization, idempotence, and replay-friendly
  constraints when durable execution is required.
- Splittable work is a separate abstraction from ordinary node execution.

Sources:

- https://beam.apache.org/documentation/programming-guide/
- https://beam.apache.org/documentation/basics/
- https://beam.apache.org/documentation/runtime/model/

### 3.4 Temporal

Temporal workflows are durable executions backed by event history. Workflow code
must be deterministic under replay. External side effects are performed by
activities whose results are recorded and reused during replay.

Useful ideas for this library:

- Event history is a durable source of truth.
- Replay should rebuild state from recorded facts, not repeat external effects.
- Workflow logic and external activities should be separated.
- Versioning matters for long-running executions.
- Signals, timers, child workflows, and activity results are events.

Sources:

- https://docs.temporal.io/workflows
- https://docs.temporal.io/encyclopedia/event-history
- https://docs.temporal.io/workflow-definition

### 3.5 Apache Flink

Flink is a stateful stream processing runtime. Its checkpointing model uses
barriers that flow through the job graph so snapshots represent a consistent cut
of operator state and input positions.

Useful ideas for this library:

- Long-running graphs need consistent state snapshots.
- Barriers are a powerful way to reason about what state belongs to what step.
- Savepoints differ from automatic checkpoints: one is operational, one is for
  fault recovery.
- Event time, processing time, and timers should be explicit concepts.

Sources:

- https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/overview/
- https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/

### 3.6 Timely Dataflow and Differential Dataflow

Timely Dataflow emphasizes dataflow graphs, logical timestamps, progress
tracking, and cyclic computations. Differential Dataflow adds efficient
incremental maintenance as inputs change.

Useful ideas for this library:

- Logical time is more useful than wall-clock time for ordering graph progress.
- Cyclic dataflows need explicit progress tracking.
- A future version can support incremental recomputation when graph inputs,
  documents, or intermediate artifacts change.

Sources:

- https://timelydataflow.github.io/timely-dataflow/chapter_1/chapter_1_1.html
- https://timelydataflow.github.io/timely-dataflow/chapter_1/chapter_1_2.html
- https://timelydataflow.github.io/differential-dataflow/introduction.html

### 3.7 Airflow and DAG Orchestrators

Airflow models workflows as DAGs with tasks, dependencies, schedules, retries,
branching, and task instances. This is useful for scheduled batch orchestration,
but less suitable as the execution model for agentic loops because cycles and
long-lived state are not natural in a DAG.

Useful ideas for this library:

- Operational concepts matter: schedule, run, task instance, retry, timeout,
  backfill, and branch visualization.
- DAG-only systems are a useful contrast: this library should support DAGs as a
  subset, not be limited to them.

Source: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html

### 3.8 OTP, GenStage, and Broadway

Elixir already has strong primitives for processes, supervision, registries,
asynchronous tasks, telemetry, and backpressure. GenStage and Broadway show how
demand, partitioning, and ordered processing can be handled in Elixir systems.

Useful ideas for this library:

- Use processes to model runtime properties, not code organization.
- One process per active run is often the right ownership boundary.
- Use tasks or worker pools for concurrent node execution.
- Use demand/backpressure for external queues and high-throughput streams.
- Partition by run or key when ordering matters.

Sources:

- https://hexdocs.pm/elixir/GenServer.html
- https://hexdocs.pm/gen_stage/GenStage.html
- https://hexdocs.pm/broadway/Broadway.html

## 4. Design Goals

1. Provide a reusable Elixir graph building and execution library.
2. Support directed graphs with cycles.
3. Support parallel vertex execution inside a graph step.
4. Make channels the only way vertices communicate.
5. Make channel update semantics explicit and typed.
6. Keep graph definitions immutable during a run.
7. Keep active run state in one owning process.
8. Checkpoint successful state transitions before acknowledging them.
9. Record enough event history to replay, inspect, and debug.
10. Separate deterministic graph control from external side effects.
11. Support interrupts, human input, timers, and async completions.
12. Support local and remote executors through a stable adapter contract.
13. Expose telemetry and streaming events without making PubSub the source of
    truth.
14. Let applications own authorization, graph/run persistence, checkpoint
    handling, and execution adapters.
15. Let simple DAG workflows remain simple.

## 5. Non-Goals

1. Do not clone LangGraph API surface one-for-one.
2. Do not require LangChain concepts.
3. Do not make every graph node an OTP process.
4. Do not hide persistence behind global process state.
5. Do not make PubSub the durable message log.
6. Do not require distributed BEAM clustering in v1.
7. Do not require one database technology.
8. Do not prescribe LLM, agent, MCP, or WaterCooler semantics.
9. Do not solve exactly-once external side effects by magic. The runtime can
   provide idempotency keys and replay rules; integrations must cooperate.
10. Do not start with full Beam-style windows, watermarks, and splittable DoFns.
    Design for them, implement the minimal useful subset first.

## 6. Runtime Mental Model

```text
Graph definition:
  Canonical Docket.Graph document describing fields, nodes, edges, reducers,
  and policies. Published versions are host-owned and immutable.

Graph edit/build:
  Application or product code creates and updates canonical Docket.Graph values
  through Docket's graph API.

Run document:
  Canonical Docket.Run state document emitted at checkpoints.
  The host application saves it and passes it back to Docket for resume/retry.

Runtime graph:
  Internal Docket.Runtime.Graph materialized from Docket.Graph for one run.

Graph run:
  One execution of a graph definition with input, internal run state, events,
  and checkpoints.

Node:
  Logical actor. Reads a consistent state snapshot and returns partial state
  updates.

Edge:
  Public graph connection that describes activation and control flow between
  nodes.

State field:
  Public data field in graph state. Each state field has value semantics,
  update semantics, and a reducer.

Channel:
  Runtime communication and storage primitive produced by lowering state fields
  and edges. Nodes never mutate another node directly.

Superstep:
  One Plan -> Execution -> Update cycle.

Checkpoint:
  Public Docket.Checkpoint document emitted after a committed runtime moment.
  It includes the latest Docket.Run document.

Event:
  Append-only fact about run lifecycle, node execution, channel update,
  interrupt, timer, error, or external command.
```

The runtime advances by repeatedly asking:

```text
Which nodes are activated by edge/channel changes from the last completed step?
Run those nodes against a consistent state snapshot.
Collect their state updates.
Apply all updates through reducers at a barrier.
Checkpoint.
Repeat.
```

## 7. Library Shape

Docket is the package identity.

The name centers the office/process metaphor:

```text
Case        = graph run
Tray        = channel
Clerk       = node
Filing rule = reducer
Mail round  = superstep
Ledger      = event log
Stamp       = checkpoint
```

This keeps the library from sounding like a Pregel clone while still matching
the architecture: channels/trays are durable state, and nodes/clerks are
side-effecting workers that react to tray changes.

Package name:

```text
docket
```

Top-level namespace:

```elixir
Docket
```

Major v1 modules:

```text
Docket.Graph
Docket.Node
Docket.Run
Docket.Runtime
Docket.Runtime.Registry
Docket.Runtime.Supervisor
Docket.Checkpoint
Docket.Executor
Docket.Event
Docket.Interrupt
Docket.Graph.Compiler
```

Channels are internal in v1: state fields and edges lower to
`Docket.Runtime.Graph.Channel` records rather than a public channel module.

Post-v1 modules can add first-class telemetry, streaming, timers, and related
inspection surfaces without changing the v1 execution loop.

Runtime-internal graph modules:

```text
Docket.Runtime.Graph
Docket.Runtime.Graph.Node
Docket.Runtime.Graph.Channel
Docket.Runtime.Graph.Lowering
```

`Docket.Runtime.Graph` lives under `Docket.Runtime` by design: it is the
executable materialization of a public graph, not a public graph document type.

Docket owns graph construction and graph execution. Application code interfaces
with canonical `Docket.Graph` values. The supervised Runtime consumes internal
`Docket.Runtime.Graph` values materialized from those canonical graphs.
`Docket.Graph.Compiler` is the single compiler module; `compile/2` returns the
runtime graph, while verification and explanation functions only prove or
describe compilability.

## 8. Static Graph Definition

`Docket.Graph` is the canonical user-facing graph document. It is edited by
product UIs, produced by workflow compilers, and stored by host applications.
Published graph artifacts should be immutable and append-only from the host
application's perspective.

Docket lowers the canonical graph into `Docket.Runtime.Graph` when it needs to
verify a publish or start a run. The host application should not assemble
runtime graph internals or persist ad hoc runtime structure outside Docket.

See `Docket.Graph` (`lib/docket/graph.ex`) for the canonical public graph
document struct and `Docket.Runtime.Graph` (`lib/docket/runtime/graph.ex`) for
the internal runtime materialization.

Graph metadata belongs in the canonical graph or host storage metadata, not in
run state:

```elixir
%{
  origin: %{
    type: "workflow",
    id: "workflow_123"
  }
}
```

The runtime can inspect graph metadata on start, resume, replay, and debugging,
but it does not require the host application to project graph fields into
first-class database columns. A host may add projection columns, hashes, or
expression indexes if it needs operational queries.

### 8.1 Runtime Graph Node

A runtime graph node names the implementation, subscriptions, outgoing edge
references, executor settings, and normalized config for one lowered public
node. See `Docket.Runtime.Graph.Node` (`lib/docket/runtime/graph/node.ex`) for
the canonical struct.

### 8.2 Runtime Graph Channel

A runtime graph channel is the lowered storage/activation primitive behind a
public input, state field, or edge. See `Docket.Runtime.Graph.Channel`
(`lib/docket/runtime/graph/channel.ex`) for the canonical struct; v1 channel
types are `:last_value`, `:ephemeral`, and `:barrier`.

Channel schemas should be strongly typed by default. The runtime should validate
both stored values and incoming updates before applying reducers.

Schema options:

- scalar types: string, integer, float, boolean, atom enum, date/time
- structured objects with required and optional fields
- tagged unions for command and interrupt payloads
- lists with optional element schemas
- maps with typed keys or declared value schemas
- custom validators supplied by the host application

The design should remain pragmatic for agentic data. Lists of objects and
model-generated JSON often need a lax mode:

```text
strict object at the channel boundary
lax object list internals when the schema cannot know every field yet
```

Reducers do not need to be modules only. Durable graph definitions should store a
serializable reducer reference, such as a built-in reducer ID, a registered host
function ID, or a module/function reference. In-memory graphs may use direct
functions when replay and cross-node portability are not required.

### 8.3 Edges Are Public, Channels Are Runtime

Edges are the public graph construction primitive for node-to-node activation.
Users should be able to understand graph shape with ordinary edge notation:

```text
A -> B
A -> [B, C]
[A, B] -> C
A -> if condition then B else C
A -> A
```

Underneath, Docket lowers edges into activation channels, subscriptions, guards,
and barriers.

At runtime, the important relation is:

```text
source node completes successfully
update barrier commits state updates
Runtime evaluates outgoing edges against committed state and changed fields
triggered edge activation channels change
subscribed target nodes become active in the next superstep
```

Classic directed edges:

```text
A -> B
```

lower to:

```text
edge record "edge_a_b" carries from: "A", to: "B"
Runtime emits channel "edge:edge_a_b" after A succeeds and the step commits
B subscribes to channel "edge:edge_a_b"
```

Fan-out edges:

```text
A -> [B, C]
```

lower to one or more edge signal channels that activate both downstream nodes in
the next superstep.

Fan-in edges:

```text
[A, B] -> C
```

lower to a barrier or equivalent consumed-version tracking so `C` activates
after both upstream requirements are satisfied, even if `A` and `B` complete in
different supersteps.

Conditional edges:

```text
A -- if approved --> B
A -- if rejected --> C
```

lower to:

```text
edge record "edge_a_b_approved" carries from: "A", to: "B", guard: approved
edge record "edge_a_c_rejected" carries from: "A", to: "C", guard: rejected
A succeeds, creating outgoing edge candidates
Runtime evaluates each guard against committed state and changed fields
guard-true edges emit "edge:<edge_id>" activation channels
B and C subscribe to their edge activation channels
```

This keeps the public graph clear while preserving a uniform runtime: all
activation ultimately flows through channel updates.

Node-local branch groups are public grouping metadata over the same guarded edge
records. For v1, Docket does not generate branch-specific runtime channels.

## 9. Active Run State

One active run is owned by one Runtime process.

The Runtime keeps the materialized runtime graph and the current `Docket.Run` in
memory while it is active. There is no second durable runtime-state model. The
run is the execution state document.

`Docket.Runtime.Graph` is derived from the canonical `Docket.Graph` document
passed to `run`, `resume`, or `retry`. `Docket.Run` is the durable document that
is advanced through checkpoint emissions. A new public `run` call first builds a
fresh `Docket.Run` from input; resume passes the durable run loaded by the host.
`Docket.Runtime.Loop.init/3` inspects the supplied run to decide whether to
initialize a fresh run or continue a saved run.

`Docket.Run` should be a real struct with nested structs for channel, task,
and interrupt records where useful. Host applications persist and pass it
back, but they do not construct, pattern match, mutate, or depend on
Docket-owned execution internals. See `Docket.Run` (`lib/docket/run.ex`) for
the canonical struct.

If a host stores runs in a format that cannot persist Elixir structs directly,
Docket should provide explicit codecs rather than making hosts treat the run as
a public map contract. The codec names mirror the graph document codec
(`Docket.Graph.to_map/2` / `Docket.Graph.from_map/2`) so each Docket document
type has exactly one serialization entry/exit pair:

```elixir
Docket.Run.to_map(%Docket.Run{}) :: map()

Docket.Run.from_map(map()) ::
  {:ok, %Docket.Run{}} | {:error, term()}

Docket.Run.from_map!(map()) :: %Docket.Run{}
```

The wire representation is a map at the storage boundary. The public runtime
API and in-memory state remain structured `%Docket.Run{}` values.

### 9.1 Nested Run State

The nested channel, task, and interrupt records are canonical in code: see
`Docket.Run.ChannelState`, `Docket.Run.TaskState`, and
`Docket.Run.InterruptState` under `lib/docket/run/`. Timers are a post-v1
surface; `Docket.Run.timers` is a plain map placeholder in v1.

## 10. Runtime Process Topology

Default v1 topology:

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

The Runtime is the only process allowed to mutate a run.

PIDs do not leave the library API. Callers use `run_id`, `graph_id`, and
application scopes.

## 11. Superstep Algorithm

Each superstep has three main phases.

### 11.1 Plan

Inputs:

- graph definition
- current run state
- edge activation channels from the previous completed update
- outstanding interrupts/timers
- max step policy
- concurrency policy

Output:

- selected node activations
- no-op / wait / terminal decision

Pseudo-code:

```elixir
def plan(graph, run) do
  candidates =
    graph.nodes
    |> Enum.filter(fn {_id, node} ->
      subscribes_to_edge_activation?(node, run.changed_channels)
    end)
    |> Enum.reject(fn {id, _node} ->
      blocked_by_interrupt_or_active_task?(run, id)
    end)

  cond do
    run.status in [:done, :failed, :cancelled] ->
      {:terminal, run.status}

    candidates == [] and no_pending_external_work?(run) ->
      {:halt, :no_activations}

    candidates == [] ->
      {:wait, waiting_on(run)}

    true ->
      {:execute, candidates}
  end
end
```

Important rule: Plan only sees the last completed channel state. It does not see
writes produced by tasks in the same superstep.
Plan does not evaluate ordinary edge guards; guarded edges were already filtered
when the previous update barrier committed. State changes alone do not activate
nodes unless an outgoing edge from a successfully completed source node triggers.

### 11.2 Execution

The Runtime dispatches each selected node with a consistent snapshot.

```elixir
state = committed_state_snapshot
config = node_config
context = %{
  run_id: run_id,
  node_id: node_id,
  step: step,
  attempt: attempt,
  source_versions: source_versions,
  idempotency_key: idempotency_key,
  application: application_context
}
```

The node returns:

```elixir
{:ok, %{"plan" => %{...}}}
```

or:

```elixir
{:interrupt, %Docket.Interrupt{...}}
{:await, term}   # reserved post-v1; v1 treats it as a permanent node failure
{:error, reason}
```

Execution may run nodes concurrently, but updates remain buffered.

### 11.3 Update

The update barrier applies state updates after all selected nodes complete or
after the step fails according to policy.

Pseudo-code:

```elixir
def update(graph, run, task_outputs) do
  state_writes = collect_state_updates(task_outputs)
  validate_state_updates!(graph, state_writes)

  {channels, changed_fields} =
    state_writes
    |> Enum.group_by(& &1.channel)
    |> Enum.reduce({run.channels, MapSet.new()}, fn {channel_id, updates}, acc ->
      apply_channel_updates(graph, acc, channel_id, updates)
    end)

  triggered_edges =
    graph
    |> outgoing_edges_for_successful_nodes(task_outputs)
    |> evaluate_edge_triggers(channels, changed_fields)

  {channels, changed_edge_channels} =
    emit_edge_activations(channels, triggered_edges)

  changed = MapSet.union(changed_fields, changed_edge_channels)

  run =
    %{run |
      channels: channels,
      changed_channels: changed,
      pending_writes: [],
      active_tasks: %{},
      step: run.step + 1,
      updated_at: now()
    }

  events = build_events(run, task_outputs, state_writes, triggered_edges)
  checkpoint = build_checkpoint(graph, run, events)
  emit_checkpoint!(checkpoint)
  publish_committed_events!(events)

  run
end
```

Important rule: channel updates from this phase activate nodes in the next
superstep, not the current superstep.
An unguarded outgoing edge triggers after successful source-node completion and
barrier commit. A guarded outgoing edge triggers only when its source completion
candidate passes against the newly committed state and changed fields. A
multi-source edge triggers only when its source-completion barrier is satisfied,
and then applies the same guard rule if it has a guard.

## 12. Channel Semantics

Channels are the heart of the runtime, but not necessarily the primary public
graph construction concept. Public graph state fields and edges lower to
channels. A channel owns:

- stored value type
- update type
- default value
- reducer
- visibility
- retention
- checkpoint encoding

### 12.1 Channel Representation

v1 does not expose a public channel behaviour. Channel types are a closed
internal set on `Docket.Runtime.Graph.Channel`
(`lib/docket/runtime/graph/channel.ex`), and reducer/guard semantics live in
`Docket.Runtime.Algorithm`.

### 12.2 v1 Channel Types

#### LastValue (`:last_value`)

Stores the last committed value. Used for input and state channels.

Conflict policy:

- If exactly one update: accept it.
- If multiple updates in one step: error by default, or use a configured
  conflict policy.

#### Ephemeral (`:ephemeral`)

Visible for one step and then cleared. Used for generated edge activation
channels.

#### Barrier (`:barrier`)

Activates only when every source of a multi-source edge has completed since
the barrier last fired.

Useful for fan-in:

```text
run reviewer only after researcher and tester have both written
```

Richer channel families explored during research - topics, reducer-backed
aggregates, delta channels with snapshots, error-collection channels, and
command channels - are deferred post-v1 design space (see the resolved v1
scope section).

## 13. Activation and Guards

Nodes are activated by edge activation subscriptions. Guards belong to edges and
filter edge candidates during the update barrier, after source nodes complete
and after state updates commit.

```elixir
edge "reviewer", "deploy",
  guard: all([
    changed("review"),
    equals(path("review", ["status"]), "approved")
  ])
```

Design-space guard primitives:

- `changed(channel)`
- `version_at_least(channel, version)`
- `path(channel, path)`
- `exists(channel)`
- `equals(channel, value)`
- `all([...])`
- `any([...])`
- `not(predicate)`
- custom application guard

v1 guard constructors:

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

Guards are serializable data expressions. They must be deterministic and side
effect free. Guards can read the newly committed state, channel versions, and
the changed field set from the update barrier that produced the edge candidate.
They do not call external services, and v1 does not support custom application
guards or arbitrary predicate matches.

## 14. External Effects

The runtime cannot make arbitrary external effects exactly once. It can make
them controlled, recorded, and idempotent.

External effects should use one of two paths:

1. Node execution via an Executor adapter.
2. Commands emitted by nodes and interpreted by the host.

Each effect gets an idempotency key derived from committed run state:

```text
{run_id}:{step}:{node_id}:{attempt}
```

A future command protocol may append a command index for node-emitted
commands.

Executor adapters must support one of these delivery semantics:

- `:sync`: result returned before update barrier.
- `:async`: task is recorded; completion arrives later.
- `:replay_only`: result must already exist in event history.

On replay:

- completed node results are loaded from history
- external effects are not repeated unless explicitly retried
- late completions are matched by task ID and attempt

## 15. Persistence Model

Docket should support durable applications without owning their storage schema.
The persistence model is document-shaped:

- `Docket.Graph` is the graph definition document.
- `Docket.Run` is the restorable run state document.
- `Docket.Checkpoint` is the callback payload emitted when the run reaches a
  committed runtime moment.

Host applications decide where documents live and how they relate to users,
projects, workflows, sessions, messages, jobs, approvals, or audit records.
Docket does not define graph tables, run tables, event tables, or storage
adapters.

### 15.1 Docket Documents

`Docket.Graph` and `Docket.Run` are public documents:

```text
Docket.Graph
  Built and edited by application code.
  Verified by Docket before publish/run.
  Stored and versioned by the host application.
  Passed to Docket to start, resume, or retry a run.

Docket.Run
  Created by Docket when a run starts.
  Updated and emitted through Docket.Checkpoint callbacks.
  Stored by the host application.
  Passed back to Docket to resume or retry a run.
```

Both documents have stable IDs. Callers may pass `id:` when creating a graph or
starting a run. If `id:` is omitted, Docket generates an ID and stores it on the
document:

```elixir
{:ok, graph} = Docket.Graph.new(id: workflow_id, name: "Essay Review")
{:ok, graph} = Docket.Graph.new(name: "Essay Review")

{:ok, run} = MyApp.Docket.run(graph, input, id: app_run_id)
{:ok, run} = MyApp.Docket.run(graph, input)
```

Apps may use Docket document IDs as primary keys or store them alongside their
own internal IDs. Docket treats IDs as opaque stable document identity.

### 15.2 Docket.Checkpoint Behaviour

`Docket.Checkpoint` is the host callback boundary:

```elixir
defmodule Docket.Checkpoint do
  @callback handle(
              checkpoint :: Docket.Checkpoint.t(),
              context :: Docket.Checkpoint.Context.t()
            ) ::
              :ok | {:error, term()}
end
```

Applications normally configure checkpoint handling once on their Docket module:

```elixir
defmodule MyApp.Docket do
  use Docket,
    checkpoint: MyApp.DocketCheckpoint,
    executor: MyApp.DocketExecutor
end
```

The callback receives every checkpoint and may pattern match only the checkpoint
types it cares about:

```elixir
defmodule MyApp.DocketCheckpoint do
  @behaviour Docket.Checkpoint

  def handle(%Docket.Checkpoint{run: run}, _ctx) do
    MyApp.Runs.save_docket_run!(run)
    :ok
  end

  def handle(_, _ctx), do: :ok
end
```

For durable usage, checkpoint delivery has two modes:

- `:sync`: `Docket.Runtime.Loop` waits for the checkpoint callback to return
  `:ok` before committing the transition or reporting success to a related
  public API.
- `:async`: `Docket.Runtime.Loop` commits the in-memory transition, submits
  checkpoint delivery in the background, and continues execution.

Required lifecycle and public API gate checkpoints should use `:sync`. Ordinary
step/event checkpoints can use `:async` so projections, event history, and
outbox delivery do not block graph execution. Apps that want stronger crash
recovery between ordinary supersteps may configure additional checkpoint types
as `:sync`.

The golden path is still to save `checkpoint.run` and pass that run document
back to Docket after a crash. With async step checkpoints, the durable run may
lag behind the active in-memory Runtime until the host accepts those checkpoint
deliveries.

Run initialization follows the same rule. `Docket.run/4` builds a fresh
`Docket.Run` document from input and launches the Runtime with that run.
`Docket.resume/4` launches the Runtime with a durable `Docket.Run` supplied by
the host. Both paths call `Docket.Runtime.Loop.init/3`, and the loop infers the
path from the supplied run document rather than an explicit mode.

For any non-terminal run it is going to execute, `Loop.init/3` produces an
initialized public run document and emits a required `:run_initialized`
checkpoint before any node execution, event publication, interrupt, timer, or
later step checkpoint can occur. The checkpoint handler upserts by
`Docket.Run.id`: a new run creates the host row, and a resumed run updates the
existing host row. If the initialization checkpoint fails, execution has not
started and callers receive a checkpoint error.

Starting a Runtime process is not graph progress by itself. `Docket.Run.status`
describes graph execution state, not process liveness, so resume must not
blindly flip a waiting, running, done, failed, or cancelled run to `:running`.
If a supplied run is already terminal, initialization should return the terminal
snapshot or a typed inactive-run error according to the public API contract, but
it must not restart graph execution.

Checkpoint handlers should create or replace records by `Docket.Run.id`. They
must not require a run row to exist before the first checkpoint, and they should
be safe to receive the same checkpoint more than once.

### 15.3 Run Document Shape

`Docket.Run` is the public restorable run document; see `Docket.Run`
(`lib/docket/run.ex`) for the canonical struct.

Apps may inspect top-level fields such as `id`, `graph_id`, `graph_hash`,
`status`, `step`, `input`, `output`, and timestamps. The run also contains
Docket-owned execution data such as channels, tasks, interrupts, timers, and
checkpoint counters. Apps should persist the run but not interpret, pattern
match, mutate, or rebuild Docket-owned execution internals. Code that needs an
external storage format should use `Docket.Run.to_map/1` and
`Docket.Run.from_map/1` rather than treating the run as a public map contract.

The crucial persist/resume path should stay simple:

```elixir
def handle(%Docket.Checkpoint{run: run}, _ctx) do
  MyApp.Runs.save_docket_run!(run)
  :ok
end
```

After a crash:

```elixir
run = MyApp.Runs.fetch_docket_run!(run_id)
graph = MyApp.Graphs.fetch_docket_graph!(run.graph_id, run.graph_hash)

{:ok, run} = MyApp.Docket.resume(graph, run)
```

### 15.4 Event History

Events should be append-only. See `Docket.Event` (`lib/docket/event.ex`) for
the canonical struct and the v1 event types (run lifecycle, node completion
and failure, channel updates, edge triggers, and interrupts). Post-v1
protocols may add event types for planning, async/late completions, timers,
and commands.

Events are emitted with checkpoints. Apps that need replay, time travel,
debugging, or audit history may persist `checkpoint.events`. Apps that only need
crash resume may persist only the latest `checkpoint.run`. Async
`checkpoint.events` delivery is observational; failure to persist an async event
envelope must not mutate or roll back the active in-memory Runtime.

### 15.5 Checkpoint Shape

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

The checkpoint is not an internal runtime dump. It is the public notification
that a runtime moment committed, and it carries the latest restorable
`Docket.Run` document.

```text
If the app saves checkpoint.run on each required checkpoint, it can resume from
the last reported state by passing that Docket.Run document back to Docket.
```

## 16. Replay and Determinism

Replay modes:

- `:run`: hydrate from a supplied `Docket.Run` document.
- `:from_start`: rebuild from persisted event history, if the host saved it.
- `:time_travel`: hydrate from a selected historical run/checkpoint/event
  document, if the host saved one.

Deterministic parts:

- planning
- guards
- channel reducers
- update barriers
- replay of recorded node outputs

Potentially nondeterministic parts:

- LLM calls
- network calls
- file I/O
- database reads
- random values
- wall-clock time

Rule:

```text
Nondeterministic operations belong in node execution or command handlers.
Their outputs are recorded. Replay reuses recorded outputs.
```

Reducers and guards must not call external systems, read wall-clock time, or
generate random values.

## 17. Graph Versioning

Graph definitions are immutable once a run starts.

Versioning is sequential:

```text
essay-review@1
essay-review@2
essay-review@3
```

Each run document stores the graph identity needed to resolve the exact
canonical graph it started with:

- graph ID
- SHA-256 graph content hash

On runtime start, the host passes a canonical `Docket.Graph` document to Docket.
Docket hashes that graph and stores the digest on `Docket.Run.graph_hash`. On
resume, retry, replay, and time travel, the host passes both the graph document
and the `Docket.Run` document or historical checkpoint/event document it wants
Docket to hydrate from. Docket hashes the supplied graph, compares it with
`run.graph_hash`, then materializes the internal `Docket.Runtime.Graph` value
and initializes or continues the run through `Docket.Runtime.Loop.init/3`. A live
Runtime keeps that materialized runtime graph in memory. It does not recompile
the graph on every superstep.

The canonical graph document stores the metadata needed to understand how it was
produced and how it should be interpreted:

- document schema version
- public graph ID
- public field, node, edge, and output definitions
- node implementation references
- policies and runtime-relevant metadata

Graph metadata is not run state. The runtime can inspect graph metadata before
executing. Host applications may project selected metadata into database columns
for search or operations, but the durable graph contract is the immutable
canonical graph document.

If a graph definition changes, new runs capture the new graph content hash.
Active runs stay on the exact graph content hash they started with.

The library does not support run migrations in this design. There is no API for
moving a run to another graph hash, no channel transformation contract, and no
attempt to move active runs between graph definitions.

The host application should retain recent host graph artifacts. A practical
default is to keep roughly the latest 10 artifacts per graph, with host-configurable
retention.

If old graph content required by an active run is no longer available, the library
should return a typed error such as:

```elixir
{:error, {:graph_unavailable, graph_id, graph_hash}}
```

The application using the library decides how to handle that condition:
administrative restore, manual failure, run cancellation, or a product-specific
repair flow.

## 18. Host Application Responsibilities

The library should be reusable, so some responsibilities stay outside it.

The host application owns:

- authentication and authorization
- tenancy and resource quotas
- secret storage
- network and filesystem sandboxing
- executor registration and trust
- durable storage choice
- backing storage and retention policy for graph and run documents
- encryption policy
- application-specific schemas
- product UX and product records outside Docket's graph runtime
- user-facing audit logs
- billing and rate limits
- external side-effect idempotency guarantees

The runtime owns:

- graph construction API
- graph normalization into state fields, nodes, edges, and runtime channels
- graph validation
- graph document shape and generated IDs when callers omit `id:`
- run ownership
- superstep planning
- update barriers
- reducer application
- checkpoint document shape and emission timing
- event document shape
- task reconciliation
- interrupt/timer lifecycle
- telemetry event shape

The boundary should be explicit in the host Docket module:

```elixir
defmodule MyApp.Docket do
  use Docket,
    checkpoint: MyApp.DocketCheckpoint,
    executor: MyApp.GraphExecutor,
    limits: [
      max_supersteps: 100,
      max_concurrent_nodes: 8,
      max_channel_bytes: 64_000,
      max_writes_per_step: 1_000
    ]
end
```

Like other OTP-oriented libraries, this should also work naturally from a host
application supervision tree:

```elixir
children = [
  MyApp.Docket
]
```

Configured adapters are runtime configuration. Individual graph runs may pass
input, context, limits, or policy overrides, but they should not need to repeat
the checkpoint handler or executor unless the host intentionally runs multiple
named Docket runtimes.

The library must not create atoms from untrusted strings. All graph record IDs
are binaries in v1, including graph, input, field, output, node, and edge IDs.
Branch group names are binaries scoped to their source node. Runtime-generated
channel IDs are also binaries. Existing atoms may still appear as ordinary
Elixir enum/status/config values, but they are not accepted as graph IDs.

## 19. Validation and Safety

Graph compile validation should reject:

- duplicate node IDs
- duplicate channel IDs
- node subscriptions to missing channels
- invalid node config
- configured state field references that point at missing channels
- invalid output channels
- invalid input channels
- reducers that are not modules/functions accepted by policy
- cycles without an explicit max superstep, halt condition, or runtime limit
- impossible barrier channels
- unbounded retention without explicit opt-in

Runtime validation should reject:

- writes to undeclared channels
- writes whose update shape does not match channel schema
- returned update fields that do not exist in graph state
- returned update values that do not match state field schemas
- channel values larger than configured limits
- too many writes in one superstep
- supersteps beyond max limit
- late task completions whose task ID, attempt, or input hash does not match
- interrupt resolutions for closed or unknown interrupts

Safety defaults should be conservative. Applications can loosen them where they
understand the cost.

## 20. Failure Semantics

Default policy:

```text
If any node in a superstep fails permanently, the superstep fails and no writes
from that superstep are committed.
```

Configurable policies:

- fail run
- retry node
- retry superstep
- write error to error channel
- route to fallback node
- ignore and continue
- continue with successful writes only

The default should be strict because partial commit semantics are easy to get
wrong. A graph can opt into partial success when channel reducers and business
logic are designed for it.

### 20.1 Timeouts

Timeout scopes:

- node attempt timeout
- superstep timeout
- run timeout
- async await timeout
- interrupt timeout

Timers are durable events. Timer callbacks become activations through timer
channels or commands.

### 20.2 Retries

Retry policy fields:

- max attempts
- backoff
- jitter
- retryable error classifier
- max elapsed time
- idempotency mode

Retries must preserve task identity and attempt number.

### 20.3 Crash Recovery

If the Runtime crashes:

1. Supervisor restarts or caller lazily starts Runtime.
2. Host loads the latest saved `Docket.Run` document and matching
   `Docket.Graph` document.
3. Host calls `Docket.resume/4` or `MyApp.Docket.resume/3`.
4. Runtime calls `Docket.Runtime.Loop.init/3` with the saved run.
5. Runtime reconciles active tasks.
6. Completed effects are reused from event history if the host saved it.
7. Lost in-flight effects are retried or marked unknown according to executor
   policy.

If the process crashed after local memory changed but before checkpoint, callers
must not have received success. The run is resumed from the previous saved
`Docket.Run` document.

## 21. Interrupts and Human Input

Nodes may pause a run:

```elixir
{:interrupt,
 %Docket.Interrupt{
   id: interrupt_id,
   node_id: node_id,
   prompt: "Approve deployment?",
   schema: %{...},
   resume_channel: "approval"
 }}
```

The run enters `:waiting` if no other nodes can proceed.

Resolving an interrupt writes to the configured resume channel:

```elixir
Docket.resolve_interrupt(MyApp.Docket, run_id, interrupt_id, %{"approved" => true})
```

The interrupt resolution is a durable event. The write activates subscribers in
the next superstep.

## 22. Streaming and Observability

Streaming and first-class telemetry are post-v1 surfaces. The durable event log
and checkpoints remain authoritative.

Future streaming surfaces:

- telemetry events
- process-local subscriptions
- Phoenix PubSub adapter
- Broadway/GenStage adapter
- SSE/WebSocket adapter

Future telemetry examples:

```text
[:docket, :run, :start]
[:docket, :run, :stop]
[:docket, :superstep, :plan]
[:docket, :superstep, :update]
[:docket, :node, :start]
[:docket, :node, :stop]
[:docket, :checkpoint, :save]
```

Observability APIs:

- get run status
- get current channel values
- get graph topology
- list events
- stream events
- inspect pending tasks
- inspect interrupts
- render graph
- time travel to superstep
- diff checkpoints

## 23. Backpressure and Scheduling

Backpressure exists at several layers:

- per-run max concurrent nodes
- per-graph max concurrent runs
- per-executor concurrency
- per-application rate limits
- per-channel retention limits
- global scheduler capacity

The Runtime can dispatch to `Task.Supervisor` for v1. For high-throughput
or queue-backed workloads, an Executor adapter can use GenStage or Broadway.

Scheduling policies:

- FIFO by run
- fair by tenant/project
- priority by run metadata
- resource-aware by executor
- partitioned by run ID for ordering

The library should expose the hooks but keep the initial scheduler simple.

## 24. API Sketch

The public API should follow common Elixir library shape:

- a supervised runtime started from a host module such as `MyApp.Docket`
- a `Docket.child_spec/1` integration point for supervision trees
- behaviours for user extension points such as nodes, channels, checkpoints, and
  executors
- graph construction operations that validate and build graph documents
- named runtimes for applications that need more than one runtime instance
- run operations addressed by runtime name and `run_id`, not PIDs
- per-run options for input, context, limits, and policy overrides
- infrastructure adapters configured once at runtime startup
- a small test helper API for pushing test runs and asserting committed events

The child spec and test helper APIs should be documented before implementation.
See `docs/architecture/docket-graph-execution-contract-design.md` for the
companion execution contract.

### 24.1 Graph Construction

Docket owns graph construction and verification. Host applications own graph
versioning and storage. See
`docs/architecture/docket-graph-construction-design.md` for the companion public
API and lowering design.

### 24.2 Node Behaviour

```elixir
defmodule Essay.Writer do
  @behaviour Docket.Node

  @impl true
  def config_schema do
    Docket.Schema.object(%{})
  end

  @impl true
  def call(state, _config, _context) do
    {:ok, %{"draft" => "Essay about #{state["topic"]}"}}
  end
end
```

### 24.3 Invoke

```elixir
graph = MyApp.Graphs.fetch_docket_graph!("essay-review", version: 1)

{:ok, run} =
  MyApp.Docket.run(graph, %{topic: "Durable graph runtimes"},
    id: app_run_id,
    context: %{tenant_id: tenant_id}
  )

{:ok, current_run} = MyApp.Docket.get_run(run.id)
```

`MyApp.Docket.get_run/2` reads the current in-memory run document for an active run. It does
not read host storage and does not emit a checkpoint. It is observational; the
latest accepted checkpoint remains the durable source of truth.

### 24.4 Resume

```elixir
run = MyApp.Runs.fetch_docket_run!(run_id)
graph = MyApp.Graphs.fetch_docket_graph!(run.graph_id, run.graph_hash)

{:ok, run} = MyApp.Docket.resume(graph, run)

Docket.resolve_interrupt(MyApp.Docket, run.id, interrupt_id, %{"approved" => true})
```

### 24.5 Test Helpers And Inline Runtime

Most Docket runtime tests should not need to exercise supervision, GenServer
mailboxes, or BEAM scheduling. Docket should expose an inline test runtime that
executes graph transitions in the calling test process while using the same loop
runtime logic as the supervised Runtime.

Proposed test API:

```elixir
Docket.Test.run_inline(graph_or_runtime_graph, input, opts \\ [])
Docket.Test.step_inline(run, opts \\ [])
```

`run_inline/3` should:

- verify and compile a supplied `Docket.Graph`, or accept a precompiled
  `Docket.Runtime.Graph`
- create the initial `Docket.Run`
- initialize execution through `Docket.Runtime.Loop.init/3`, letting the loop
  infer fresh versus saved execution from the run document
- rely on `Loop.init/3` to synchronously emit the required
  `:run_initialized` checkpoint to the configured test checkpoint sink before
  any node execution it schedules
- execute graph transitions in the calling process until the run reaches
  `:done`, `:failed`, `:waiting`, or a configured max step limit
- return only after sync checkpoints caused by those transitions have been
  accepted or failed
- drain async checkpoints by default before returning, unless the test opts into
  production-like async delivery behavior

Recommended return shape:

```elixir
{:ok, Docket.Run.t(), [Docket.Checkpoint.t()]}
| {:error, Docket.Error.t(), [Docket.Checkpoint.t()]}
```

`step_inline/2` should drive exactly one committed superstep and return only
after that step's sync checkpoint requirements have been accepted or failed.
If the step emits an async checkpoint, the helper should drain it by default for
assertions or let tests opt into non-drained async delivery.

The inline runtime is not a second interpreter. It must call the same plan,
execution, update, validation, reducer, and checkpoint-building code as the
supervised Runtime. The difference is only the shell:

```text
supervised Runtime:
  GenServer process owns state and receives calls/messages

inline test runtime:
  calling test process owns state for the duration of the helper
```

Tests that only care about graph semantics, checkpoint ordering, reducers,
guards, interrupts, and failure policy should use the inline runtime. Tests that
care about supervision, process lifecycle, crash recovery, late completions,
remote executors, timers, or async awaits should use focused supervised or
adapter-specific tests.

No Docket test should require `Process.sleep/1` to wait for ordinary graph
progress. Test helpers should expose explicit synchronization points, and every
helper should return only after relevant sync checkpoints have succeeded or
failed. Async checkpoint delivery should be drainable explicitly.

## 25. Executor Adapter

Executors let the runtime run node work locally, remotely, or through an
application-specific system.

```elixir
defmodule Docket.Executor do
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
              | {:await, term()}
              | {:error, term()}
end
```

`{:await, term()}` is reserved for post-v1 late-completion protocols; v1
treats it as a permanent node failure.

Built-in executors:

- `Docket.Executor.Local`: direct function call.
- `Docket.Executor.Task`: supervised task with timeout.
- `Docket.Executor.Queue`: durable task queue.
- `Docket.Executor.Remote`: application-defined remote call.

WaterCooler could implement a remote executor that sends one node execution to a
connected runtime without exposing the full graph.

## 26. Graph Construction Layer

Docket's graph construction layer produces canonical `Docket.Graph` values.
Application code, workflow compilers, and graph editors use that canonical graph
shape. The Runtime uses an internal `Docket.Runtime.Graph` value materialized
from the canonical graph when a run starts.

Regardless of API shape, Docket owns graph normalization, compiler diagnostics,
runtime verification, and generated document IDs when callers omit `id:`.

The companion design in
`docs/architecture/docket-graph-construction-design.md` proposes the
canonical `Docket.Graph` API, app-owned document persistence, realtime edit
operations, diagnostics, and lowering rules from public fields, nodes, and
edges into internal runtime channels and subscriptions. UI projection is
host-owned.

### 26.1 Sequential Workflow Compiler

An ordered step list:

```text
plan -> implement -> test -> done
```

compiles to:

```text
channels:
  input
  step:plan:done
  step:implement:done
  step:test:done

nodes:
  plan subscribes input, writes step:plan:done
  implement subscribes step:plan:done, writes step:implement:done
  test subscribes step:implement:done, writes step:test:done
```

### 26.2 Conditional Gate Compiler

A gate:

```text
if review.status == "approved" then deploy else revise
```

compiles to node-local branch metadata grouping guarded edge records. Each
branch arm has a stable edge ID and lowers to an `edge:<edge_id>` activation
channel.

A future runtime could add an explicit branch node for debugging or dynamic
routing, but v1 keeps branch execution in the same channel family as every other
edge activation.

## 27. WaterCooler as First Consumer

WaterCooler should treat this library as an execution abstraction, not a module
namespace inside the product.

Current WaterCooler concepts can map like this:

| WaterCooler concept | Runtime library concept |
| --- | --- |
| workflow definition | graph definition |
| workflow step | node |
| gate | guard, branch node, or conditional channel |
| workflow run | graph run |
| current step | active frontier / changed channels |
| step result | channel write |
| workflow output | output channel |
| run history | event history |
| elicitation | interrupt |
| session log | event stream projection |
| RuntimeChannel step execution | Executor adapter |
| project context | external store or application-owned channel adapter |
| PubSub updates | streaming/notification adapter |

### 27.1 WaterCooler Boundary Rules

For WaterCooler, the server should still own:

- graph definitions derived from workflows
- run state and checkpoints
- channel values that are user-visible or gate-visible
- interrupts/elicitations
- event history
- session logs
- trigger evaluation

The external agent runtime should own:

- LLM calls
- shell/filesystem/local tools
- local working memory
- execution of a single node request

The external runtime should receive:

```text
execute node X with input snapshot Y and metadata Z
```

It should not receive:

```text
the whole graph definition
the whole run state
unbounded blackboard dumps
other tenants' data
```

### 27.2 PubSub in WaterCooler

PubSub should notify observers that something happened:

```text
run advanced
channel changed
node completed
interrupt requested
```

PubSub should not be the durable Pregel mailbox. Durable recovery comes from the
latest saved `Docket.Run` document, plus saved event history when the host wants
full replay, time travel, or audit trails.

### 27.3 Migration Path for WaterCooler

Phase 1: Library skeleton

- Create a standalone Mix package immediately, outside the WaterCooler app.
- Implement graph document structs, channel behaviours, a checkpoint
  handler/test collector, local executor, and a single Runtime.
- Add tests for Plan, Execution, Update semantics.

Phase 2: Compatibility compiler

- Compile existing WaterCooler workflows into graph definitions.
- Preserve MCP response shapes.
- Keep existing sequential workflows working.
- Represent `current_step_id` as a compatibility projection over graph state.

Phase 3: Durable host persistence

- Save `Docket.Graph` and `Docket.Run` documents in WaterCooler-owned tables.
- Persist checkpoint events where WaterCooler needs replay, debugging, or audit
  history.
- Keep CubDB for application context if desired, but not for graph run state.

Phase 4: Runtime executor

- Implement WaterCooler executor adapter over RuntimeChannel.
- Server sends node execution requests.
- Runtime returns node output, interrupt request, async await, or error.

Phase 5: Advanced graph features

- Fan-out/fan-in.
- Topic and aggregate channels.
- Interrupts.
- Timers.
- Retry policies.
- Streaming event projections.

Phase 6: Replace step sequencer

- Switch workflow MCP tools to call graph runtime.
- Keep compatibility aliases for current workflow APIs.
- Add graph-native APIs later.

## 28. Alternative Designs Considered

### 28.1 One OTP Process Per Node

Description:

- Each graph node is a GenServer.
- Nodes subscribe to PubSub topics.
- Nodes maintain local mailboxes and emit messages directly.

Benefits:

- Natural actor feel.
- Local isolation.
- Potentially good for long-lived monitors.

Problems:

- Harder barrier semantics.
- Harder deterministic replay.
- More process lifecycle overhead.
- More complicated checkpointing.
- PubSub delivery is not durable enough by itself.
- Cross-node state becomes distributed state.

Recommendation:

Use one process per run by default. Allow specialized node processes only for
long-lived resources, external subscriptions, or isolated workers.

### 28.2 DAG Orchestrator

Description:

- Model everything as acyclic tasks and dependencies.

Benefits:

- Familiar.
- Easy visualization.
- Good for scheduled batch jobs.

Problems:

- Cycles are unnatural.
- Agent loops are awkward.
- Incremental state and interrupts are bolted on.
- Fan-in semantics are usually task-status-driven instead of channel-driven.

Recommendation:

Support DAGs as a subset compiled to graph channels. Do not make DAGs the loop
runtime model.

### 28.3 Temporal-Style Code-First Workflow

Description:

- User writes ordinary Elixir workflow functions.
- Runtime records commands and replays deterministic code.

Benefits:

- Excellent developer ergonomics.
- Very strong durability model.
- Natural long-running workflow semantics.

Problems:

- Deterministic replay constraints are difficult in Elixir without a dedicated
  SDK discipline.
- Graph visualization and parallel channel semantics become secondary.
- Agent workflows often need dynamic graph inspection and externalized state.

Recommendation:

Borrow event history, activities, deterministic replay discipline, and versioning.
Keep graph/channels as the primary model.

### 28.4 Beam-Style Pure Pipeline

Description:

- PCollections and transforms are primary.
- Runtime decides execution backend.

Benefits:

- Strong portability.
- Excellent model for bounded/unbounded data.
- Mature concepts for windows, triggers, state, timers, and splittable work.

Problems:

- Heavier than needed for agentic graph runs.
- Per-run conversational state is not the usual Beam center.
- Strict data-parallel model can obscure control flow and interrupts.

Recommendation:

Borrow pipeline graph, runtime abstraction, channel/window concepts, and user-code
requirements. Do not implement full Beam.

### 28.5 Flink-Style Stateful Stream Processor

Description:

- Treat workflows as streams through stateful operators.

Benefits:

- Great for continuous event processing.
- Strong checkpointing model.
- Mature event-time and timer semantics.

Problems:

- Heavy runtime assumptions.
- Less natural for per-run agent workflows with human interrupts.
- More engine than library.

Recommendation:

Borrow checkpoint barriers, savepoint distinction, timers, and state management.

### 28.6 Timely/Differential Dataflow

Description:

- Use logical timestamps and progress tracking, potentially incremental
  recomputation.

Benefits:

- Excellent for cycles and incremental maintenance.
- Strong conceptual tools for progress.

Problems:

- Sophisticated model.
- More complexity than v1 needs.

Recommendation:

Use simple logical superstep timestamps now. Keep room for richer progress
frontiers and incremental recomputation later.

### 28.7 GenStage/Broadway Pipeline

Description:

- Use GenStage/Broadway as the primary runtime.

Benefits:

- Backpressure and concurrency are built in.
- Good queue integrations.
- Partitioning and ordering tools are available.

Problems:

- Demand streams are not the same as graph supersteps.
- Barrier semantics, replay, and channel reducers still need a graph runtime.

Recommendation:

Use GenStage/Broadway behind Executor adapters or schedulers, not as the primary
graph state model.

## 29. Resolved v1 Scope

The v1 implementation should be the smallest durable graph runtime that proves
the graph construction, compiler, runtime, checkpoint, retry, and crash-recovery
contracts.

Included in v1:

1. Canonical `Docket.Graph` and `Docket.Run` public documents.
2. Binary-only graph IDs and binary runtime-generated channel IDs.
3. Functional graph editing helpers for inputs, fields, outputs, nodes, edges,
   policies, and metadata.
4. `Docket.Graph.verify/2`, `Docket.Graph.Compiler.verify/2`, and
   `Docket.Graph.Compiler.compile/2`.
5. Internal `Docket.Runtime.Graph` materialization for inputs, state fields,
   outputs, runtime graph nodes, simple edges, fan-out, multi-source edges,
   node-local branch groups, generated activation channels, guards, and barrier
   semantics.
6. Built-in channel support required by that runtime path:
   - `LastValue`
   - `Ephemeral` edge activation channels
   - barrier/all semantics for multi-source edges
7. One Runtime process per active run plus a shared execution loop used by the
   supervised Runtime and `Docket.Test`.
8. Plan -> Execution -> Update supersteps with barrier visibility.
9. `Docket.Executor.Local` and `Docket.Executor.Task`.
10. `Docket.Checkpoint` callback behaviour with both `:sync` and `:async`
    delivery modes.
11. `Docket.Run` document emission for persist/resume.
12. Interrupt request and resolution through resume-channel writes.
13. Basic retry and timeout policies.
14. Crash recovery from the latest saved `Docket.Run` checkpoint through
    `Docket.resume/4`.

Deferred until after v1:

- telemetry as a first-class API/module
- full event-history replay, time travel, and replay-only execution
- queue and remote executors
- `Topic`, `Aggregate`, `Delta`, `Error`, and `Command` as public channel types
- node-emitted commands interpreted by the host
- custom application guards
- partial-success commit policies
- WaterCooler/sequential workflow compatibility compiler
- windowing, watermarks, and richer stream-processing semantics

v1 acceptance tests:

- Single-node graph returns output.
- Two-node chain runs in two supersteps.
- Fan-out runs nodes in parallel.
- Fan-in waits for required channels.
- Cycle terminates when node stops writing or a guard reaches a halt condition.
- Writes are invisible until the next step.
- Multiple writes use the configured v1 reducer semantics.
- Failed node prevents barrier commit by default.
- Retry succeeds without duplicating committed writes.
- Interrupt pauses and resumes through channel update.
- Runtime crash recovers from the latest saved checkpoint.
- Resume never commits uncheckpointed writes from a crashed Runtime.

## 30. Post-v1 Priorities

After the MVP, the top priority is Beam-style time and collection semantics:

- windowing
- watermarks
- triggers
- event-time vs processing-time timers
- bounded vs unbounded channels
- accumulation and discarding modes
- late data policy

These are not MVP concerns, but the channel and runtime model should avoid
closing the door on them.

Other post-MVP areas:

- durable queue-backed executor
- richer graph visualization
- checkpoint compaction
- incremental recomputation
- distributed deployment through durable executors, not distributed Erlang as a
  requirement

## 31. Settled Decisions

1. Start as a new standalone Mix package immediately.
2. Treat `Docket.Graph` and `Docket.Run` as app-owned documents.
3. Use strongly typed channel value and update schemas by default, with lax
   modes for model-generated lists of objects and other hard-to-pin shapes.
4. Store async node completions in the same event log as synchronous node
   completions.
5. Keep active runs on their original graph hash. The host application owns
   retention and recovery when old graph content is no longer available.
6. Do not require reducers to be modules only. Durable graphs need serializable
   reducer references; direct functions are acceptable for in-memory/local-only
   graphs.
7. Do not use distributed Erlang as a runtime requirement.
8. Use `Docket` as the package identity.

## 32. Strong Recommendations

1. Make state fields and edges the public graph abstraction.
2. Lower state fields and edges to runtime channels and subscriptions.
3. Make updates invisible until the barrier.
4. Keep one owner process per active run.
5. Keep node processes optional.
6. Require pure reducers and guards.
7. Record node outputs before they influence future planning.
8. Treat external effects as activities/commands with idempotency keys.
9. Make PubSub a projection, never the source of truth.
10. Make WaterCooler a consumer, not the owner of the abstraction.

The most important product bet is not "Pregel" as a name. It is the shift from:

```text
current_step_id plus gates
```

to:

```text
logical actors plus channels plus durable supersteps
```

That is the point where workflows stop being a step sequencer and become a real
graph runtime.
