# Docket v1 Implementation Path

Status: active implementation guide
Date: 2026-06-26

Related documents:

- `docs/architecture/docket-graph-construction-design.md`
- `docs/architecture/docket-graph-execution-contract-design.md`
- `docs/architecture/docket-v1-test-suite-design.md`
- `docs/architecture/docket-runtime-design.md`

## 1. Purpose

This document turns the current design into a lean v1 build path.

Docket has two main product and implementation flows:

```text
Build a graph document
  create/edit Docket.Graph
  verify/compile it
  publish/store it in the host app

Run a graph document
  load Docket.Graph
  create or resume Docket.Run
  execute through the Runtime
  checkpoint updated Docket.Run documents
```

The v1 implementation should stay organized around those two flows. Everything
else is support code.

## 2. v1 Outcome

By the end of v1, a host application can:

- Build a canonical `Docket.Graph` with inputs, state fields, outputs, nodes,
  edges, guards, metadata, and advisory diagnostics.
- Verify that a graph is runnable before publishing it.
- Compile a graph into an internal `Docket.Runtime.Graph`.
- Start a run from a graph and input payload.
- Resume a run from a saved `Docket.Run` snapshot and the matching graph
  version.
- Execute local/task node callbacks through a shared execution loop.
- Receive checkpoints containing public `Docket.Run` snapshots and events.
- Test construction, compilation, inline execution, checkpoints, and supervised
  lifecycle behavior without external services.

v1 is not complete until the build path and run path connect through tests:

```text
Docket.Graph
  -> Docket.Graph.Compiler.compile/2
  -> Docket.Test.run_inline/3
  -> Docket.Run + Docket.Checkpoint assertions
```

## 3. Design Center

Public durable documents:

```text
Docket.Graph
  Canonical editable graph definition.
  Stored and versioned by the host.

Docket.Run
  Canonical durable execution state.
  Emitted through checkpoints.
  Stored by the host and passed back for resume.
```

Internal derived values:

```text
Docket.Runtime.Graph
  Executable lowering of Docket.Graph.
  Not the host storage format.

Docket.Runtime.Loop
  Shared processless transition functions over Docket.Run.
  Used by supervised Runtime and Docket.Test.
```

Host-owned responsibilities:

- Graph and run storage.
- Graph publishing and immutable version policy.
- Authorization, tenancy, billing, audit, and product relationships.
- UI editor projection and live run overlays.
- External side effects performed by node code.

## 4. Flow A: Build A Graph Document

The build path creates and verifies a canonical `Docket.Graph`.

```text
new draft
  -> edit graph document
  -> inspect advisory diagnostics
  -> verify runnable shape
  -> host publishes/stores immutable graph version
```

### 4.1 Build Path Contract

The public graph API edits `Docket.Graph` values directly. There is no separate
builder schema in v1.

Required public modules:

```text
Docket.Graph
Docket.Graph.Node
Docket.Graph.Edge
Docket.Graph.Field
Docket.Graph.Output
Docket.Graph.Diagnostics
Docket.Graph.Compiler
Docket.Node
Docket.Node.Input
Docket.Node.Output
Docket.Node.Ports
Docket.Schema
Docket.Reducer
Docket.Guard
```

Required editing functions:

```elixir
Docket.Graph.new(opts \\ [])
Docket.Graph.input(graph, id, opts)
Docket.Graph.field(graph, id, opts)
Docket.Graph.output(graph, id, opts \\ [])
Docket.Graph.node(graph, id, implementation, opts)
Docket.Graph.edge(graph, from, to, opts \\ [])
Docket.Graph.join(graph, from_nodes, to_node, opts \\ [])
Docket.Graph.branch(graph, from_node, opts)
Docket.Graph.policy(graph, key, value)
Docket.Graph.metadata(graph, key, value)
Docket.Graph.diagnostics(graph, opts \\ [])

Docket.Graph.put_node(graph, id, attrs)
Docket.Graph.update_node(graph, id, attrs_or_fun)
Docket.Graph.delete_node(graph, id, opts \\ [])

Docket.Graph.put_edge(graph, id, attrs)
Docket.Graph.update_edge(graph, id, attrs_or_fun)
Docket.Graph.delete_edge(graph, id, opts \\ [])

Docket.Graph.put_field(graph, id, attrs)
Docket.Graph.update_field(graph, id, attrs_or_fun)
Docket.Graph.delete_field(graph, id, opts \\ [])
```

Draft graph documents may be incomplete. Editing helpers should preserve graph
data and attach advisory diagnostics when possible. They should only hard-error
on malformed arguments that cannot be represented as graph data.

### 4.2 Build Path Implementation Slices

Implement in this order:

1. Public structs and ID rules.
2. Functional graph editing helpers.
3. Advisory diagnostics for incomplete drafts.
4. Guard and schema/reducer value constructors.
5. Node type contracts: `config_schema/0`, `ports/1`, `call/1`, and public
   `Docket.Node.Input` / `Docket.Node.Output` structs.
6. Compiler validation for config schemas, port schemas, port bindings, and
   graph field schema compatibility.
7. Lowering to `Docket.Runtime.Graph`, including resolved input/output bindings.
8. Compiler reports that map public IDs to runtime IDs.
9. Publish-ready verification contract.

First useful milestone:

```elixir
graph =
  Docket.Graph.new(id: "essay-review", name: "Essay Review")
  |> Docket.Graph.input("topic", schema: Docket.Schema.string())
  |> Docket.Graph.field("draft",
    schema: Docket.Schema.string(),
    reducer: Docket.Reducer.last_value()
  )
  |> Docket.Graph.node("writer", Essay.Writer,
    reads: ["topic"],
    writes: ["draft"]
  )
  |> Docket.Graph.edge("$start", "writer")
  |> Docket.Graph.edge("writer", "$finish")
  |> Docket.Graph.output("draft")

{:ok, report} = Docket.Graph.Compiler.verify(graph)
{:ok, runtime_graph, report} = Docket.Graph.Compiler.compile(graph)
```

### 4.3 Build Path Boundaries

In v1, the graph construction layer must not:

- Require UI canvas JSON as the canonical graph format.
- Expose `Docket.Runtime.Graph.Node` or channel internals to ordinary
  graph editing code.
- Store raw third-party schema terms as the durable graph schema format.
- Persist graphs itself.
- Mutate published graph versions in place.
- Store UI layout, viewport, zoom, positions, or editor projection state inside
  Docket graph documents.
- Convert user-provided IDs into atoms.

## 5. Flow B: Run A Graph Document

The run path executes a verified graph and emits checkpointed run documents.

```text
host loads Docket.Graph
  -> Docket.run/4 builds initial Docket.Run
  -> compiler materializes Docket.Runtime.Graph
  -> Runtime/Loop initialize through checkpoint barrier
  -> supersteps execute nodes
  -> checkpoints emit updated Docket.Run documents
```

Resume follows the same path after the host loads the saved run:

```text
host loads Docket.Run + matching Docket.Graph version
  -> Docket.resume/4
  -> compiler materializes Docket.Runtime.Graph
  -> Loop.init/3 continues from the saved Docket.Run
  -> execution continues or returns terminal/inactive result
```

### 5.1 Run Path Contract

Required public APIs:

```elixir
Docket.run(runtime, graph, input, opts \\ [])
Docket.resume(runtime, graph, run, opts \\ [])
Docket.get_run(runtime, run_id, opts \\ [])
Docket.resolve_interrupt(runtime, run_id, interrupt_id, value, opts \\ [])
```

Generated host wrappers:

```elixir
MyApp.Docket.run(graph, input, opts \\ [])
MyApp.Docket.resume(graph, run, opts \\ [])
MyApp.Docket.get_run(run_id, opts \\ [])
MyApp.Docket.resolve_interrupt(run_id, interrupt_id, value, opts \\ [])
```

Required internal/runtime modules:

```text
Docket.Runtime.Graph
Docket.Runtime.Graph.Node
Docket.Runtime.Graph.Channel
Docket.Runtime.Graph.Lowering
Docket.Run
Docket.Runtime
Docket.Runtime.Loop
Docket.Runtime.Algorithm
Docket.Runtime.Dispatcher
Docket.Runtime.Registry
Docket.Runtime.Supervisor
Docket.Executor
Docket.Checkpoint
Docket.Event
Docket.Interrupt
Docket.Test
```

The supervised Runtime and inline test runtime must share the same loop. Do not
build a second interpreter for tests.

### 5.2 Run Path Implementation Slices

Implement in this order:

1. Public `Docket.Run`, event, checkpoint, error, write, interrupt, and node
   input/output structs.
2. `Docket.Checkpoint` callback contract and in-memory test sink.
3. `Docket.Runtime.Algorithm` planning, guard evaluation, write validation,
   reducer application, terminal detection, and checkpoint construction.
4. `Docket.Runtime.Loop.init/3`, `plan/2`, `apply_results/3`,
   `resolve_interrupt/4`, and `to_run/1`.
5. `Docket.Executor.Local` for synchronous node execution.
6. `Docket.Test.run_inline/3` and `Docket.Test.step_inline/2`.
7. Compile-and-run integration through `Docket.Test`.
8. `Docket.Runtime` GenServer shell.
9. Registry, supervisor, and public `Docket.run/4`, `resume/4`, `get_run/3`,
   and `resolve_interrupt/5`.
10. `Docket.Executor.Task` and supervised lifecycle tests.
11. ETS checkpoint sink and crash-resume coverage.

First useful milestone:

```elixir
{:ok, run, checkpoints} =
  Docket.Test.run_inline(graph, %{"topic" => "Durable graphs"},
    checkpoint: Docket.Test.Checkpoint.MemorySink
  )
```

### 5.3 Run Path Rules

The first implementation should keep these rules tight:

- `Docket.run/4` is a start barrier, not a completion barrier.
- No node execution starts before the `:run_initialized` checkpoint succeeds.
- One active run is owned by one Runtime process.
- Public APIs use runtime module and `run_id`, not Runtime PIDs.
- The Runtime keeps the current `Docket.Run` in memory while the run is active.
- Nodes communicate only through channel writes.
- User node callbacks communicate through generic input/output ports; the
  runtime maps output ports to channel writes after validation.
- Supersteps follow Plan -> Execution -> Update.
- Writes from one node are invisible to other nodes in the same superstep.
- v1 waits for all selected local/task executions before the update barrier.
- Permanent failure in a superstep commits no writes from that superstep.
- Checkpoints are the durable boundary for committed runtime moments.
- `get_run/3` reads only active Runtime memory and does not read host storage.
- `resume/4` requires `graph.id == run.graph_id` and
  `graph.version == run.graph_version`.

### 5.4 Run Path Boundaries

In v1, the runtime must not:

- Hide persistence behind global process state.
- Require Ecto, Redis, Postgres, Docker, or network services.
- Expose mutable `Docket.Runtime.Loop` state as public API.
- Treat PubSub, streams, or telemetry as durable truth.
- Claim exactly-once external effects. Docket supplies idempotency keys;
  integrations must cooperate.
- Support queue/remote executors, replay-only execution, custom guards,
  windows/watermarks, partial success policies, or command interpretation.

## 6. Compiler Lowering Needed For v1

The first runtime only needs the lowering shape required by the baseline graph
fixtures.

```text
input field
  -> input:<input_id> channel

state field
  -> state:<field_id> channel

output
  -> output projection

node
  -> Docket.Runtime.Graph.Node

edge
  -> edge:<edge_id> activation channel
  -> source node system write on success
  -> target node subscription

guarded edge
  -> edge:<edge_id> activation channel
  -> target guard expression evaluated against committed state

join
  -> member edge channels
  -> barrier:<join_id> channel or equivalent consumed-version tracking
```

Branches are public grouping sugar over guarded edge records in v1. They do not
need a `branch:<branch_id>` runtime channel.

## 7. Minimum v1 Fixtures

Build implementation and tests around small graph fixtures:

- `minimal_linear/0`: input -> copy -> output.
- `simple_edge/0`: start -> writer -> reviewer -> finish.
- `fanout/0`: one source activates two targets.
- `join/0`: two branches converge before combine.
- `guarded_edge/0`: guard chooses premium or standard path.
- `interrupt_review/0`: node requests human input, then resumes.
- `retry_then_continue/0`: flaky node succeeds before retry limit.
- `parallel_failure/0`: permanent failure prevents barrier commit.
- `cycle_counter/0`: bounded cycle proves guards and max-supersteps.

Do not add larger examples until these fixtures are green through construction,
compiler, inline execution, and the relevant supervised tests.

## 8. Test Gates

Each implementation slice should have a test gate.

| Gate | Proves |
| --- | --- |
| Graph construction | `Docket.Graph` can be built and edited as a durable document. |
| Diagnostics | Incomplete drafts are representable and advisory warnings are stable. |
| Compiler validation | Unrunnable graphs fail with public diagnostics. |
| Compiler lowering | Public IDs map to runtime channel/node IDs correctly. |
| Inline execution | The shared loop runs graph semantics without processes. |
| Checkpoints | `Docket.Run` snapshots are emitted in the right order. |
| Interrupts | Runs can wait and resume through public APIs. |
| Supervised runtime | Registry/supervisor lifecycle matches the inline semantics. |
| Crash resume | Latest saved `Docket.Run` can restart execution with matching graph version. |

Default `mix test` must remain dependency-free. No v1 test should require Ecto,
a Repo, SQL databases, Redis, Docker, network services, object storage, LLM
providers, or browser automation.

## 9. Suggested Work Sequence

This is the practical build order:

1. Add test support: deterministic IDs, deterministic clock, memory checkpoint
   sink, and graph/node fixtures.
2. Implement `Docket.Graph` structs and functional editing helpers.
3. Implement schemas, reducers, guard expressions, diagnostics, and ID
   validation.
4. Implement compiler validation and reports.
5. Implement runtime graph structs and lowering for minimal edges.
6. Implement run/checkpoint/event structs and run codecs.
7. Implement `Runtime.Algorithm` for a single linear graph.
8. Implement `Runtime.Loop` and `Docket.Test.run_inline/3`.
9. Connect graph compiler output to inline execution.
10. Add fan-out, join, guarded edge, output projection, and cycle support.
11. Add interrupts, retry, failure policy, and max-supersteps.
12. Add `Runtime` GenServer, registry, supervisor, and public run APIs.
13. Add task executor support.
14. Add ETS checkpoint sink and crash-resume tests.
15. Fill remaining end-of-v1 coverage from the test suite design.

The first end-to-end proof should be tiny:

```text
minimal_linear graph
  -> verify
  -> compile
  -> run_inline
  -> checkpoints [:run_initialized, :step_committed, :run_completed]
  -> output projection contains expected result
```

## 10. Defer Until After v1

Preserve design space for these, but do not let them block the v1 path:

- Queue and remote executors.
- Durable queue/backpressure protocol.
- Replay-only execution from persisted event history.
- Dynamic command interpretation from node outputs.
- Custom application guards.
- Full timer/window/watermark semantics.
- Partial success policies.
- Distributed BEAM clustering.
- Storage adapters owned by Docket.
- First-class UI editor or inspector packages.

## 11. Definition Of Done

v1 docs and implementation are aligned when:

- The build path has public graph construction, diagnostics, verification, and
  compiler lowering tests.
- The run path has inline execution, checkpoint ordering, interrupt, retry,
  failure, resume, and supervised lifecycle tests.
- Public APIs accept and return `Docket.Graph`, `Docket.Run`, diagnostics,
  reports, checkpoints, and typed errors.
- Host persistence remains document-shaped and host-owned.
- The runtime graph remains derived and internal.
- The README and architecture index point contributors to this implementation
  path before the longer reference documents.
