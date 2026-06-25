# Docket: Elixir Durable Graph Runtime Library Design

Status: draft
Date: 2026-06-25

## 1. Executive Summary

This document proposes a standalone Elixir library for durable, graph-based
workflow execution inspired by Pregel, Apache Beam, LangGraph, Temporal,
Flink, Timely Dataflow, and OTP.

The library is not WaterCooler-specific. WaterCooler would be the first
consumer, but the abstraction should be useful to any Elixir system that needs
to run cyclic, parallel, stateful graphs with checkpoints, streaming updates,
human interrupts, remote execution, and replay.

The core idea is:

```text
One process owns one active graph run.

Inside that process, graph vertices are logical actors.
Actors read channels, write updates, and execute in bulk-synchronous steps.
Channel updates become visible only at the next step barrier.
Durable checkpoints make replay and recovery possible.
```

The runtime should not model every vertex as an OTP process by default. A vertex
is a logical actor. OTP processes are used for runtime ownership, concurrency,
remote execution, supervision, and fault containment.

The recommended architecture is:

```text
Pregel-style execution core
  + Beam-style graph/channel/runner abstractions
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
- Interrupts, checkpointers, stores, streams, and retry policies belong around
  the core runtime.

Source: https://docs.langchain.com/oss/python/langgraph/pregel

### 3.3 Apache Beam

Apache Beam defines pipelines as graphs of PTransforms over PCollections. The
same abstractions cover bounded and unbounded data. Beam also has a runner model,
windowing, triggers, state, timers, side inputs, multiple outputs, and splittable
work.

Useful ideas for this library:

- A graph definition should be portable across execution backends.
- A runner should execute a graph without changing graph semantics.
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
but less suitable as the core model for agentic loops because cycles and
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
14. Let applications supply persistence, authorization, storage, and execution
    adapters.
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

## 6. Core Mental Model

```text
Graph definition:
  Immutable description of state fields, nodes, edges, reducers, and policies.

Graph build:
  Docket-owned process that creates a validated, versioned Docket.Graph through
  a Docket-defined graph construction API.

Graph run:
  One execution of a graph definition with input, run state, history, and
  checkpoints.

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
  Durable record of graph state after a completed update barrier.

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

Major modules:

```text
Docket.Graph
Docket.Node
Docket.Channel
Docket.Run
Docket.Runner
Docket.RunnerSupervisor
Docket.GraphStore
Docket.Checkpointer
Docket.Executor
Docket.EventLog
Docket.Telemetry
Docket.Stream
Docket.Interrupt
Docket.Timer
Docket.Compiler
```

Optional higher-level APIs:

```text
Docket.StateGraph
Docket.Functional
Docket.WorkflowCompiler
```

Docket owns graph construction and graph execution. The concrete graph
construction API is a separate design decision. The low-level Runner consumes
only validated static `Docket.Graph` definitions.

## 8. Static Graph Definition

The compiled graph is immutable and versioned.

Docket owns construction of this graph. The host application calls Docket's
graph construction API; Docket produces the state fields, nodes, edges,
reducers, guards, policies, validation diagnostics, and graph version. Docket
then lowers state fields and edges into runtime channels, subscriptions, and
barriers. The host application should not assemble graph internals or persist ad
hoc graph structure outside Docket.

```elixir
defmodule Docket.Graph do
  defstruct [
    :id,
    :name,
    :version,
    :schema_version,
    :input_channels,
    :output_channels,
    nodes: %{},
    channels: %{},
    metadata: %{},
    policies: %{}
  ]
end
```

The graph document is self-contained. Compiler provenance belongs in
`metadata`, not in run state:

```elixir
%{
  compiler: %{
    name: "WaterCooler.WorkflowCompiler",
    version: "0.4.2",
    git_sha: "abc123",
    compiled_at: "2026-06-25T18:00:00Z"
  },
  origin: %{
    type: "workflow",
    id: "workflow_123",
    version: 7,
    hash: "sha256:..."
  }
}
```

The runtime can inspect this metadata on start, resume, replay, and debugging,
but it does not require the host application to project compiler fields into
first-class database columns. A host may add projection columns or expression
indexes if it needs operational queries such as "find all graph artifacts
compiled by compiler version X."

### 8.1 Node Definition

```elixir
defmodule Docket.Graph.NodeDef do
  defstruct [
    :id,
    :module,
    :function,
    :executor,
    :timeout,
    :retry,
    :cache,
    :on_error,
    subscribe: [],
    read: [],
    write: [],
    metadata: %{}
  ]
end
```

Fields:

- `id`: stable node identity within the graph.
- `module` / `function`: local implementation, if any.
- `executor`: local, remote, task queue, MCP, HTTP, or custom adapter.
- `subscribe`: channels whose version changes activate the node.
- `read`: additional channels visible to the node without activating it.
- `write`: channels the node may update.
- `timeout`: node execution timeout.
- `retry`: retry policy.
- `cache`: optional deterministic memoization policy.
- `on_error`: fail run, write error channel, retry, skip, or route.
- `metadata`: application-owned data.

### 8.2 Channel Definition

```elixir
defmodule Docket.Graph.ChannelDef do
  defstruct [
    :id,
    :type,
    :value_schema,
    :update_schema,
    :reducer,
    :visibility,
    :retention,
    :snapshot_frequency,
    :default,
    metadata: %{}
  ]
end
```

Fields:

- `id`: stable channel identity.
- `type`: channel module or built-in type.
- `value_schema`: stored value shape.
- `update_schema`: write/update shape.
- `reducer`: how pending writes become the next channel value.
- `visibility`: persistent, ephemeral, run_private, stream_public, etc.
- `retention`: how much write history to keep.
- `snapshot_frequency`: for delta channels.
- `default`: initial value.
- `metadata`: application-owned data.

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

Underneath, Docket lowers edges into channels, subscriptions, guards, and
barriers.

At runtime, the important relation is:

```text
node writes channel
channel version changes
subscribed nodes become candidates
```

Classic directed edges:

```text
A -> B
```

lower to:

```text
A writes channel "edge:A:B"
B subscribes to channel "edge:A:B"
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
A writes "branch:A" = "approved" or "rejected"
B subscribes to "branch:A" with guard value == "approved"
C subscribes to "branch:A" with guard value == "rejected"
```

This keeps the public graph clear while preserving a uniform runtime: all
activation ultimately flows through channel updates.

## 9. Active Run State

One active run is owned by one Runner process.

The Runner keeps both the immutable graph definition and mutable run state in
memory while it is active:

```elixir
defmodule Docket.Runner.State do
  defstruct [
    :graph,
    :run
  ]
end
```

`graph` is the fully realized `Docket.Graph` loaded from `Docket.GraphStore` at
runner start or recovery. `run` is the mutable `Docket.Run.State` loaded from
the checkpointer and advanced through checkpoints.

```elixir
defmodule Docket.Run.State do
  defstruct [
    :run_id,
    :graph_id,
    :graph_version,
    :status,
    :superstep,
    :started_at,
    :updated_at,
    :finished_at,
    channels: %{},
    channel_versions: %{},
    changed_channels: MapSet.new(),
    active_tasks: %{},
    pending_writes: [],
    interrupts: %{},
    timers: %{},
    history_seq: 0,
    metadata: %{}
  ]
end
```

### 9.1 Channel State

```elixir
defmodule Docket.Run.ChannelState do
  defstruct [
    :channel_id,
    :value,
    :version,
    :updated_at,
    :last_writer,
    writes_by_step: %{}
  ]
end
```

Channel version increments only when the update barrier changes the stored
value or when the channel policy says every write creates a new version.

### 9.2 Task State

```elixir
defmodule Docket.Run.TaskState do
  defstruct [
    :task_id,
    :node_id,
    :superstep,
    :attempt,
    :status,
    :input_hash,
    :started_at,
    :deadline_at,
    :executor_ref,
    :idempotency_key,
    metadata: %{}
  ]
end
```

Task state is durable enough to reconcile late completions after process restarts.

## 10. Runtime Process Topology

Default v1 topology:

```text
Application supervisor
  Docket.RunnerRegistry
  Docket.RunnerSupervisor
  Docket.ExecutorSupervisor
  Docket.Telemetry

Runner process per active run
  owns immutable graph definition loaded from GraphStore
  owns Run.State
  owns step planning
  owns update barriers
  owns checkpoint writes
  dispatches node execution to executors

Executor tasks or pools
  run node code
  report results back to Runner
```

The Runner is the only process allowed to mutate a run.

PIDs do not leave the library API. Callers use `run_id`, `graph_id`, and
application scopes.

## 11. Superstep Algorithm

Each superstep has three main phases.

### 11.1 Plan

Inputs:

- graph definition
- current run state
- changed channels from the previous completed update
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
      subscribes_to_changed_channel?(node, run.changed_channels)
    end)
    |> Enum.filter(fn {_id, node} ->
      guards_satisfied?(node, run.channels)
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

### 11.2 Execution

The Runner dispatches each selected node with a consistent snapshot.

```elixir
%Docket.Node.Input{
  run_id: run_id,
  node_id: node_id,
  superstep: step,
  values: readable_channel_values,
  versions: readable_channel_versions,
  context: application_context,
  attempt: attempt,
  idempotency_key: key
}
```

The node returns:

```elixir
{:ok, %Docket.Node.Output{
  writes: [
    %Docket.Write{channel: "plan", value: %{...}}
  ],
  commands: [
    %Docket.Command{type: :schedule_timer, payload: %{...}}
  ],
  metadata: %{}
}}
```

or:

```elixir
{:interrupt, %Docket.Interrupt{...}}
{:await, %Docket.Await{...}}
{:error, reason}
```

Execution may run nodes concurrently, but updates remain buffered.

### 11.3 Update

The update barrier applies writes after all selected nodes complete or after the
step fails according to policy.

Pseudo-code:

```elixir
def update(graph, run, task_outputs) do
  writes = collect_writes(task_outputs)
  validate_writes!(graph, writes)

  {channels, changed} =
    writes
    |> Enum.group_by(& &1.channel)
    |> Enum.reduce({run.channels, MapSet.new()}, fn {channel_id, updates}, acc ->
      apply_channel_updates(graph, acc, channel_id, updates)
    end)

  run =
    %{run |
      channels: channels,
      changed_channels: changed,
      pending_writes: [],
      active_tasks: %{},
      superstep: run.superstep + 1,
      updated_at: now()
    }

  events = build_events(run, task_outputs, writes)
  commit_transition!(graph, run, events)
  publish_committed_events!(events)

  run
end
```

Important rule: channel updates from this phase activate nodes in the next
superstep, not the current superstep.

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

### 12.1 Channel Behaviour

```elixir
defmodule Docket.Channel do
  @callback init(definition :: map()) :: {:ok, term()} | {:error, term()}

  @callback apply_updates(
              current :: term(),
              updates :: [term()],
              context :: map()
            ) :: {:ok, term()} | {:error, term()}

  @callback changed?(old :: term(), new :: term()) :: boolean()

  @callback encode(value :: term()) :: {:ok, term()} | {:error, term()}

  @callback decode(value :: term()) :: {:ok, term()} | {:error, term()}
end
```

### 12.2 Built-In Channel Types

#### LastValue

Stores the last update. Useful for simple state and edge signals.

Conflict policy:

- If exactly one update: accept it.
- If multiple updates in one step: error by default, or use a configured
  conflict policy.

#### Topic

Stores a collection of updates. Useful for fan-in, messages, and streaming
outputs.

Options:

- accumulate across steps
- clear after read
- deduplicate by key
- max length
- retention window

#### Aggregate

Stores a persistent value updated by a reducer.

Reducer requirements:

- deterministic
- associative when batching may vary
- side-effect free

#### Delta

Stores writes per step and reconstructs the value with snapshots. Useful for
large growing values such as message lists.

Options:

- snapshot frequency
- compaction policy
- reconstruction limit

#### Ephemeral

Visible for one step and then cleared. Useful for edge signals.

#### Barrier

Activates only when a configured set of upstream channels has reached required
versions or predicates.

Useful for fan-in:

```text
run reviewer only after researcher and tester have both written
```

#### Error

Collects node failures when policy routes errors instead of failing the run.

#### Command

Records external commands to be performed by the host application or executor.

## 13. Activation and Guards

A node can be activated by subscription and filtered by guards.

```elixir
node :reviewer,
  subscribe: ["research:done", "tests:done"],
  read: ["research:summary", "tests:summary"],
  guard: all([
    changed("research:done"),
    changed("tests:done")
  ])
```

Guard primitives:

- `changed(channel)`
- `version_at_least(channel, version)`
- `exists(channel)`
- `equals(channel, value)`
- `matches(channel, predicate)`
- `all([...])`
- `any([...])`
- `not(predicate)`
- custom application guard

Guards must be deterministic and side-effect free. Guards can read channel
state, not external services.

## 14. External Effects

The runtime cannot make arbitrary external effects exactly once. It can make
them controlled, recorded, and idempotent.

External effects should use one of two paths:

1. Node execution via an Executor adapter.
2. Commands emitted by nodes and interpreted by the host.

Each effect gets an idempotency key:

```text
{run_id}:{superstep}:{node_id}:{attempt}:{command_index}
```

Executor adapters must support one of these delivery semantics:

- `:sync`: result returned before update barrier.
- `:async`: task is recorded; completion arrives later.
- `:replay_only`: result must already exist in event history.

On replay:

- completed node results are loaded from history
- external effects are not repeated unless explicitly retried
- late completions are matched by task ID and attempt

## 15. Persistence Model

The runtime should support pluggable persistence.

### 15.1 GraphStore Behaviour

Graph storage is a runtime adapter boundary. Docket builds fully realized
`Docket.Graph` documents and uses the configured graph store to save and fetch
them by graph ID and graph version. Docket does not prescribe whether those
documents live in Postgres JSONB, object storage, DynamoDB, MongoDB, or another
store.

```elixir
defmodule Docket.GraphStore do
  @callback save_graph(graph :: Docket.Graph.t(), opts :: keyword()) ::
              {:ok, Docket.Graph.t()} | {:error, term()}

  @callback load_graph(graph_id, graph_version, opts :: keyword()) ::
              {:ok, Docket.Graph.t()} | {:error, term()}
end
```

The returned graph must be complete enough for Docket to execute without
joining against application workflow tables. Storage-specific references,
hashes, content-addressing, compression, and secondary indexes are host
implementation details behind the adapter.

### 15.2 Checkpointer Behaviour

```elixir
defmodule Docket.Checkpointer do
  @callback create_run(graph, input, opts) ::
              {:ok, run_id, Docket.Run.State.t()} | {:error, term()}

  @callback load_run(run_id) ::
              {:ok, Docket.Run.State.t()} | {:error, term()}

  @callback commit_transition(run_id, checkpoint, events, opts) ::
              {:ok, %{checkpoint_seq: non_neg_integer(), last_event_seq: non_neg_integer()}}
              | {:error, term()}

  @callback list_events(run_id, opts) ::
              {:ok, [Docket.Event.t()]} | {:error, term()}
end
```

The checkpointer persists run, checkpoint, and event state. It records
`graph_id` and `graph_version` from the graph used to start the run, but it does
not persist graph documents. Fully realized graph documents are saved and loaded
through `Docket.GraphStore`.

Adapters:

- `Docket.Checkpointer.Memory`
- `Docket.Checkpointer.Postgres`
- application-specific adapter

Postgres is the first durable checkpointer. The in-memory checkpointer exists
for tests, examples, and local prototyping, but it is not the durable runtime
target. SQLite, Mnesia, or other stores can be future adapters if a concrete
consumer needs them.

### 15.3 Event History

Events should be append-only.

Common event types:

- `run_started`
- `run_completed`
- `run_failed`
- `superstep_planned`
- `node_started`
- `node_completed`
- `node_failed`
- `node_awaiting`
- `async_node_completed`
- `async_node_failed`
- `channel_updated`
- `checkpoint_saved`
- `interrupt_requested`
- `interrupt_resolved`
- `timer_scheduled`
- `timer_fired`
- `command_emitted`
- `command_completed`

Event shape:

```elixir
defmodule Docket.Event do
  defstruct [
    :run_id,
    :seq,
    :type,
    :superstep,
    :node_id,
    :channel_id,
    :task_id,
    :timestamp,
    :payload,
    :metadata
  ]
end
```

### 15.4 Checkpoint Shape

```elixir
%Docket.Checkpoint{
  run_id: run_id,
  graph_id: graph_id,
  graph_version: graph_version,
  checkpoint_seq: checkpoint_seq,
  last_event_seq: last_event_seq,
  superstep: superstep,
  status: status,
  channels: encoded_channels,
  channel_versions: channel_versions,
  changed_channels: changed_channels,
  active_tasks: active_tasks,
  interrupts: interrupts,
  timers: timers,
  metadata: metadata,
  created_at: timestamp
}
```

Minimum durability rule:

```text
A state-changing API must not return success until its checkpoint and the
corresponding ordered events are durable in the same transaction.
```

The checkpointer must treat a state transition as one atomic commit:

1. validate the expected current checkpoint or fencing token
2. append all transition events with contiguous event sequence numbers
3. save the new checkpoint with the last event sequence it covers
4. commit the database transaction
5. publish telemetry or stream notifications only after commit

There is no independent durable `append_events` path for events that claim run
state changed. Events such as `node_started`, `interrupt_resolved`,
`channel_updated`, and `run_completed` are recorded with the checkpoint that
makes the corresponding state true. This avoids split-brain replay states where
a checkpoint exists without its event history or events exist for a checkpoint
that never committed.

## 16. Replay and Determinism

Replay modes:

- `:checkpoint`: load latest checkpoint, then apply later events if needed.
- `:from_start`: rebuild from event history.
- `:time_travel`: load state at a selected superstep or event sequence.

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

Each run stores only the graph identity needed to resolve the exact compiled
graph it started with:

- graph ID
- graph version

On runner start, resume, replay, and time travel, the runtime loads the fully
realized `Docket.Graph` from the configured `Docket.GraphStore` using that
identity. A live Runner keeps that immutable graph in memory and uses it for
planning, execution, validation, and checkpoint construction. It does not fetch
the graph from the store on every superstep. The graph store owns any
storage-specific references, hashes, document IDs, content-addressing, or
secondary indexes needed to retrieve and validate the artifact.

The compiled graph document stores the metadata needed to understand how it was
produced and how it should be interpreted:

- document schema version
- compiler name and version
- compiler build identifier or git SHA, when available
- origin identifier, version, and hash, when available
- channel schema versions
- node implementation references

Compiler version is provenance for the compiled graph document. It is not run
state. The runtime can inspect artifact metadata inline before executing. Host
applications may project selected metadata into database columns for search or
operations, but the durable runtime contract is the immutable graph document
loaded by graph ID and graph version.

If a graph definition changes, new runs use the new version. Active runs stay on
the exact version they started with.

The library does not support run migrations in this design. There is no API for
moving a run to another graph version, no channel transformation contract, and
no attempt to move active runs between graph versions.

The host application should retain recent graph versions. A practical default is
to keep roughly the latest 10 versions per graph, with host-configurable
retention.

If an old version required by an active run is no longer available, the library
should return a typed error such as:

```elixir
{:error, {:graph_version_unavailable, graph_id, version}}
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
- backing storage and retention policy for graph artifacts and runs
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
- graph version creation
- graph store protocol
- graph artifact save/load operations through the configured graph store
- run ownership
- superstep planning
- update barriers
- reducer application
- checkpoint protocol
- event history protocol
- task reconciliation
- interrupt/timer lifecycle
- telemetry event shape

The boundary should be explicit in configuration:

```elixir
Docket.start_link(
  name: MyApp.Docket,
  graph_store: MyApp.GraphStore,
  checkpointer: MyApp.GraphCheckpointer,
  executor: MyApp.GraphExecutor,
  authorizer: MyApp.GraphAuthorizer,
  codec: MyApp.GraphCodec,
  limits: [
    max_supersteps: 100,
    max_concurrent_nodes: 8,
    max_channel_bytes: 64_000,
    max_writes_per_step: 1_000
  ]
)
```

Like other OTP-oriented libraries, this should also work naturally from a host
application supervision tree:

```elixir
children = [
  {Docket,
   name: MyApp.Docket,
   graph_store: MyApp.GraphStore,
   checkpointer: MyApp.GraphCheckpointer,
   executor: MyApp.GraphExecutor}
]
```

Core adapters are runtime configuration. Individual graph runs may pass input,
context, limits, or policy overrides, but they should not need to repeat the
graph store, checkpointer, or executor unless the host intentionally runs
multiple named Docket runtimes.

The core library should not create atoms from untrusted strings. Graph IDs,
node IDs, and channel IDs should remain binaries or validated existing atoms
provided by application code.

## 19. Validation and Safety

Graph compile validation should reject:

- duplicate node IDs
- duplicate channel IDs
- node subscriptions to missing channels
- node writes to missing channels
- invalid output channels
- invalid input channels
- reducers that are not modules/functions accepted by policy
- cycles without an explicit max superstep, halt condition, or runtime limit
- impossible barrier channels
- unbounded retention without explicit opt-in

Runtime validation should reject:

- writes to undeclared channels
- writes whose update shape does not match channel schema
- writes from nodes not authorized to write that channel
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

If the Runner crashes:

1. Supervisor restarts or caller lazily starts Runner.
2. Runner loads latest checkpoint.
3. Runner reconciles active tasks.
4. Completed effects are reused from event history.
5. Lost in-flight effects are retried or marked unknown according to executor
   policy.

If the process crashed after local memory changed but before checkpoint, callers
must not have received success. The operation is replayed from the previous
durable checkpoint.

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

The runtime should expose streaming events, but the durable event log remains
authoritative.

Streaming surfaces:

- telemetry events
- process-local subscriptions
- Phoenix PubSub adapter
- Broadway/GenStage adapter
- SSE/WebSocket adapter

Telemetry examples:

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

The core Runner can dispatch to `Task.Supervisor` for v1. For high-throughput
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

- a supervised runtime started with `Docket.start_link/1` or `{Docket, opts}`
- a `Docket.child_spec/1` integration point for supervision trees
- behaviours for user extension points such as nodes, channels, graph stores,
  checkpointers, and executors
- graph construction operations that validate, version, and save graph definitions
- named runtimes for applications that need more than one runtime instance
- run operations addressed by runtime name and `run_id`, not PIDs
- per-run options for input, context, limits, and policy overrides
- core infrastructure adapters configured once at runtime startup
- a small test helper API for pushing test runs and asserting committed events

The child spec and test helper APIs should be documented before implementation,
but they do not need detailed design in this draft.

### 24.1 Graph Construction

Docket owns graph construction. The concrete public API for defining, editing,
validating, versioning, and saving graphs is intentionally not specified in this
runtime draft. See `docs/architecture/docket-graph-construction-design.md` for
the companion public API and lowering design.

### 24.2 Node Behaviour

```elixir
defmodule Essay.Writer do
  @behaviour Docket.Node

  @impl true
  def call(%Docket.Node.Input{values: %{topic: topic}} = input) do
    {:ok,
     %Docket.Node.Output{
       writes: [
         Docket.Write.last_value(:draft, "Essay about #{topic}")
       ],
       metadata: %{input_versions: input.versions}
     }}
  end
end
```

### 24.3 Invoke

```elixir
{:ok, run_id} =
  Docket.start(MyApp.Docket,
    graph: {"essay-review", 1},
    input: %{topic: "Durable graph runtimes"},
    context: %{tenant_id: tenant_id}
  )

{:ok, result} = Docket.await(MyApp.Docket, run_id)
```

### 24.4 Stream

```elixir
Docket.stream(MyApp.Docket, run_id)
|> Enum.each(fn event ->
  IO.inspect(event)
end)
```

### 24.5 Resume

```elixir
Docket.resume(MyApp.Docket, run_id)
Docket.resolve_interrupt(MyApp.Docket, run_id, interrupt_id, %{"approved" => true})
```

## 25. Executor Adapter

Executors let the core runtime run node work locally, remotely, or through an
application-specific system.

```elixir
defmodule Docket.Executor do
  @callback execute(
              task :: Docket.Run.TaskState.t(),
              node :: Docket.Graph.NodeDef.t(),
              input :: Docket.Node.Input.t(),
              opts :: keyword()
            ) ::
              {:ok, Docket.Node.Output.t()}
              | {:interrupt, Docket.Interrupt.t()}
              | {:await, Docket.Await.t()}
              | {:error, term()}
end
```

Built-in executors:

- `Docket.Executor.Local`: direct function call.
- `Docket.Executor.Task`: supervised task with timeout.
- `Docket.Executor.Queue`: durable task queue.
- `Docket.Executor.Remote`: application-defined remote call.

WaterCooler could implement a remote executor that sends one node execution to a
connected runtime without exposing the full graph.

## 26. Graph Construction Layer

Docket's graph construction layer produces the static `Docket.Graph` consumed by
the Runner. Its concrete public API is intentionally left open in this runtime
draft. Regardless of API shape, Docket owns normalization, validation,
versioning, provenance metadata, and saving through `Docket.GraphStore`.

The companion design in
`docs/architecture/docket-graph-construction-design.md` proposes the
`Docket.StateGraph` editable facade, compile/decompile contract, realtime edit
operations, React Flow projection, diagnostics, and lowering rules from state
fields, nodes, and edges into runtime channels and subscriptions.

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

compiles to either:

1. a branch node that reads review output and writes branch channels, or
2. guarded subscriptions on downstream nodes.

The branch node is easier to observe and debug. Guarded subscriptions are more
compact. The library should support both, and the compiler can choose.

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

PubSub should not be the durable Pregel mailbox. The durable mailbox is channel
state plus event history in the graph runtime checkpointer.

### 27.3 Migration Path for WaterCooler

Phase 1: Library skeleton

- Create a standalone Mix package immediately, outside the WaterCooler app.
- Implement graph structs, channel behaviours, in-memory checkpointer, local
  executor, and a single Runner.
- Add tests for Plan, Execution, Update semantics.

Phase 2: Compatibility compiler

- Compile existing WaterCooler workflows into graph definitions.
- Preserve MCP response shapes.
- Keep existing sequential workflows working.
- Represent `current_step_id` as a compatibility projection over graph state.

Phase 3: Durable adapter

- Add Postgres checkpointer for WaterCooler.
- Store checkpoints and event history in WaterCooler-owned tables.
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

Support DAGs as a subset compiled to graph channels. Do not make DAGs the core
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
- Runner decides execution backend.

Benefits:

- Strong portability.
- Excellent model for bounded/unbounded data.
- Mature concepts for windows, triggers, state, timers, and splittable work.

Problems:

- Heavier than needed for agentic graph runs.
- Per-run conversational state is not the usual Beam center.
- Strict data-parallel model can obscure control flow and interrupts.

Recommendation:

Borrow pipeline graph, runner abstraction, channel/window concepts, and user-code
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

- Use GenStage/Broadway as the core runtime.

Benefits:

- Backpressure and concurrency are built in.
- Good queue integrations.
- Partitioning and ordering tools are available.

Problems:

- Demand streams are not the same as graph supersteps.
- Barrier semantics, replay, and channel reducers still need a graph runtime.

Recommendation:

Use GenStage/Broadway behind Executor adapters or schedulers, not as the core
graph state model.

## 29. Recommended MVP

The smallest valuable library:

1. Static graph struct.
2. Node and Channel behaviours.
3. Built-in channels:
   - LastValue
   - Topic
   - Aggregate
   - Ephemeral
4. One Runner process per active run.
5. Plan, Execution, Update loop.
6. Local Task executor.
7. In-memory checkpointer for tests and examples.
8. Postgres checkpointer as the first durable adapter.
9. Event history.
10. Interrupt support.
11. Basic retry and timeout.
12. Telemetry.
13. Sequential workflow compiler for compatibility.

MVP acceptance tests:

- Single-node graph returns output.
- Two-node chain runs in two supersteps.
- Fan-out runs nodes in parallel.
- Fan-in waits for required channels.
- Cycle terminates when node stops writing.
- Writes are invisible until next step.
- Multiple writes reduce through channel reducer.
- Failed node prevents barrier commit by default.
- Retry succeeds without duplicating committed writes.
- Interrupt pauses and resumes through channel update.
- Runner crash recovers from latest checkpoint.
- Replay does not re-run completed node effects.

## 30. Post-MVP Priorities

After the MVP, the top priority is Beam-style time and collection semantics:

- windowing
- watermarks
- triggers
- event-time vs processing-time timers
- bounded vs unbounded channels
- accumulation and discarding modes
- late data policy

These are not MVP concerns, but the channel and runner model should avoid
closing the door on them.

Other post-MVP areas:

- durable queue-backed executor
- richer graph visualization
- checkpoint compaction
- incremental recomputation
- distributed deployment through durable executors, not distributed Erlang as a
  core requirement

## 31. Settled Decisions

1. Start as a new standalone Mix package immediately.
2. Use Postgres as the first durable checkpointer.
3. Use strongly typed channel value and update schemas by default, with lax
   modes for model-generated lists of objects and other hard-to-pin shapes.
4. Store async node completions in the same event log as synchronous node
   completions.
5. Keep active runs on their original graph version. The host application owns
   retention and recovery when an old version is no longer available.
6. Do not require reducers to be modules only. Durable graphs need serializable
   reducer references; direct functions are acceptable for in-memory/local-only
   graphs.
7. Do not use distributed Erlang as a core runtime requirement.
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
