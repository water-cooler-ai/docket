# Docket: Graph Construction Public API Design

Status: draft
Date: 2026-06-25

Related document: `docs/architecture/docket-runtime-design.md`

## 1. Executive Summary

This document designs Docket's public graph construction layer.

The key naming decision is:

```text
Docket.Graph is the canonical, user-facing graph.
Docket.Run is the canonical, user-facing run snapshot document.
Docket.Graph.Runtime is the internal executable runtime graph.
```

Applications, workflow compilers, React Flow editors, and product UIs work with
`Docket.Graph`. It contains fields, nodes, edges, guards, outputs, policies,
layout, metadata, and advisory diagnostics.

The Runner does not execute `Docket.Graph` directly. When a run starts, Docket
materializes the canonical graph into an internal `Docket.Graph.Runtime` value
with channels, subscriptions, reducers, generated edge channels, barriers, and
runtime node definitions.

Docket does not own graph storage. Host applications save, version, relate, and
load graph documents however they like. Docket accepts graph documents as values,
verifies them, runs them, and emits updated run documents through checkpoints.

```text
draft/edit/save:
  Docket.Graph document -> host-owned storage

publish/verify:
  Docket.Graph.Compiler.verify/2 -> diagnostics
  -> host-owned publish/save

run:
  host loads Docket.Graph document
  -> Docket.run/3 or MyApp.Docket.run/3
  -> internal runtime compile
  -> Docket.Checkpoint emits Docket.Run document
```

`Docket.Graph.Runtime` is a derived implementation detail. Its shape can change
as the runtime improves without changing the public graph model.

## 2. Core Model

There are four related but distinct things:

```text
Docket.Graph
  User-facing graph definition document.
  Canonical.
  Editable while the host treats it as a draft.
  Immutable once the host treats it as a published version.
  Suitable for UI, workflow compilers, host-owned storage, and inspection.

Docket.Run
  User-facing durable run snapshot document.
  Emitted through Docket.Checkpoint callbacks.
  Saved by the host application.
  Passed back to Docket to resume or retry a run.

Docket.Graph.Runtime
  Internal executable graph materialization.
  Derived from Docket.Graph.
  Consumed by the Runner.
  Contains channels, NodeDef records, generated barriers, and lowering maps.

Run State
  Mutable execution state for one run.
  Owned by one Runner process.
  Contains channel values, versions, active tasks, interrupts, timers, events,
  and checkpoints.
```

The public construction layer is centered on `Docket.Graph`. The runtime graph
is not the storage format and not the user-facing API. The host stores
`Docket.Graph` and `Docket.Run` documents, not `Docket.Graph.Runtime` or
`Docket.Run.State` internals.

## 3. Design Goals

1. Make `Docket.Graph` the canonical graph used by applications.
2. Keep graph storage app-owned; Docket accepts and returns graph documents.
3. Keep `Docket.Graph.Runtime` internal and derived.
4. Support realtime graph construction through simple graph update functions.
5. Support React Flow and similar UI projections without making UI layout part
   of runtime semantics.
6. Allow incomplete drafts and advisory diagnostics while users edit.
7. Use compiler verification and compilation as the blocking gate for
   publish/run.
8. Keep published graph versions append-only and immutable.
9. Preserve stable public IDs for graph, node, edge, field, and output records.
10. Keep generated channels hidden from normal users but inspectable in runtime
    debug tools.

## 4. Non-Goals

1. Do not introduce a separate `StateGraph`, `Builder`, `NodeSpec`, or
   `EdgeSpec` schema that mirrors `Docket.Graph`.
2. Do not ask UI code to construct runtime `ChannelDef` or `NodeDef` values.
3. Do not mutate a published graph version in place.
4. Do not require every UI edit to produce a valid executable graph.
5. Do not make React Flow JSON the canonical graph model.
6. Do not make graph layout affect runtime execution.
7. Do not require a Docket graph storage behaviour.
8. Do not make hashes or compile receipts part of the core public model.

## 5. Module Shape

Recommended public modules:

```text
Docket.Graph
Docket.Graph.Node
Docket.Graph.Edge
Docket.Graph.Field
Docket.Graph.Output
Docket.Graph.Layout
Docket.Graph.Projection
Docket.Graph.Diagnostics
Docket.Graph.Compiler
Docket.Schema
Docket.Reducer
Docket.Guard
```

Recommended internal/runtime modules:

```text
Docket.Graph.Runtime
Docket.Graph.Runtime.NodeDef
Docket.Graph.Runtime.ChannelDef
Docket.Graph.Runtime.Lowering
Docket.Runner
```

The naming boundary is:

- `Docket.Graph.Node` is an editable public graph node.
- `Docket.Node` is the executable node behaviour implemented by user code.
- `Docket.Graph.Runtime.NodeDef` is an internal lowered node definition.

## 6. Docket.Graph

`Docket.Graph` is the canonical public graph document and the structure UI tools
edit.

It contains:

- graph identity, supplied with `id:` or generated by Docket
- optional published version
- input fields
- state fields
- output projections
- nodes
- edges
- joins and branch sugar, if preserved separately
- graph-level policies
- layout and UI metadata
- application metadata
- advisory diagnostics

Sketch:

```elixir
defmodule Docket.Graph do
  defstruct [
    :id,
    :name,
    :description,
    :version,
    :schema_version,
    fields: %{},
    inputs: %{},
    outputs: %{},
    nodes: %{},
    edges: %{},
    joins: %{},
    branches: %{},
    policies: %{},
    layout: %Docket.Graph.Layout{},
    metadata: %{},
    diagnostics: []
  ]
end
```

This is not a temporary builder. The functional API returns updated
`Docket.Graph` values:

```elixir
graph =
  Docket.Graph.new(id: "essay-review", name: "Essay Review")
  |> Docket.Graph.input(:topic, schema: Docket.Schema.string())
  |> Docket.Graph.field(:draft,
    schema: Docket.Schema.string(),
    reducer: Docket.Reducer.last_value()
  )
  |> Docket.Graph.node(:writer, Essay.Writer,
    reads: [:topic],
    writes: [:draft]
  )
  |> Docket.Graph.edge(:start, :writer)
  |> Docket.Graph.edge(:writer, :finish)
  |> Docket.Graph.output(:draft)
```

## 7. Public Nodes And Runtime NodeDefs

The word "node" appears in three places. They are related but not identical.

```text
Docket.Graph.Node
  Editable public node in the canonical graph.
  Used by UI, workflow compilers, diagnostics, host storage, and graph projection.
  Names public fields and public edges.

Docket.Node
  Behaviour implemented by executable node code.
  Receives Docket.Node.Input and returns Docket.Node.Output, interrupt, await,
  or error.

Docket.Graph.Runtime.NodeDef
  Internal runtime definition consumed by the Runner.
  Names runtime channels, subscriptions, guards, executor settings, and
  generated system writes.
```

Example public node:

```elixir
%Docket.Graph.Node{
  id: "writer",
  implementation: %{type: :module, module: Essay.Writer, function: :call},
  reads: ["topic"],
  writes: ["draft"],
  metadata: %{label: "Write Draft"}
}
```

Example runtime node definition:

```elixir
%Docket.Graph.Runtime.NodeDef{
  id: "writer",
  module: Essay.Writer,
  function: :call,
  subscribe: ["edge:$start:writer"],
  read: ["input:topic"],
  write: ["state:draft"],
  system_writes: [
    %{channel: "edge:writer:$finish", on: :success}
  ],
  metadata: %{
    public_node_id: "writer"
  }
}
```

The public node says "writer reads topic and writes draft." The runtime
`NodeDef` says "writer subscribes to this activation channel, reads this input
channel, may write this state channel, and emits this generated edge channel on
success."

## 8. Compile, Verify, And Materialize

`Docket.Graph.Compiler` is the single compiler module. It owns verification,
explanation, and runtime materialization.

`compile/2` is the only compiler function that returns a compiled runtime
artifact. It materializes `Docket.Graph` into an internal
`Docket.Graph.Runtime` value.

`verify/2` and `explain/2` use the same compiler rules, but they do not return a
runtime graph. They only prove that compilation would succeed or return the
diagnostics/report needed for publishing, previews, and debugging.

Applications do not normally store the runtime graph. They may verify that a
graph is runnable before publishing it:

```elixir
case Docket.Graph.Compiler.verify(graph, opts) do
  {:ok, report} ->
    {:ok, MyApp.Graphs.save!(graph, metadata), report}

  {:error, diagnostics} ->
    {:error, diagnostics}
end
```

The Runner materializes at run start:

```elixir
graph = MyApp.Graphs.fetch!("essay-review", version: 5)

{:ok, run} =
  MyApp.Docket.run(graph, input, id: app_run_id)
```

The compiler lowers user-facing concepts:

```text
input field -> input channel
state field -> state channel
edge -> generated edge channel plus subscriptions
join -> generated barrier channel
branch -> guarded edges or generated branch channel/node
output -> output channel projection
Docket.Graph.Node -> Docket.Graph.Runtime.NodeDef
```

The compile path may return an explain report for debugging, but the runtime
graph remains derived from the canonical graph. If a host wants to cache the
runtime graph, that is a host implementation detail outside Docket's required
public contract.

## 9. Unified Graph Editing API

Build-time graph construction and realtime graph editing should use the same
interface.

Every public graph editing function should take a `Docket.Graph` and return an
updated `Docket.Graph`. Advisory warnings travel with the returned graph through
`graph.diagnostics`, and callers can refresh/read them with
`Docket.Graph.diagnostics/2`.

That gives both calling styles without introducing a separate realtime patch or
operation API.

Build-time, pipe-oriented construction:

```elixir
graph =
  Docket.Graph.new(id: "essay-review", name: "Essay Review")
  |> Docket.Graph.input(:topic, schema: Docket.Schema.string())
  |> Docket.Graph.field(:draft, schema: Docket.Schema.string())
  |> Docket.Graph.node(:writer, Essay.Writer,
    reads: [:topic],
    writes: [:draft]
  )
  |> Docket.Graph.edge(:start, :writer)
  |> Docket.Graph.edge(:writer, :finish)

warnings = graph.diagnostics
```

Realtime, event-by-event editing:

```elixir
graph =
  Docket.Graph.update_node(graph, "writer", %{
    label: "Draft Writer",
    layout: %{position: %{x: 180, y: 120}}
  })

warnings = graph.diagnostics
```

The difference is cadence, not API shape. A compiler may apply many graph
functions in a pipe. A UI may apply one graph function per user action and
return the updated graph plus warnings to the client.

### 9.1 Entry Point And Inputs

`Docket.Graph.new/1` is the normal entry point for creating a graph. It is the
only graph editing function that does not take an existing graph as its first
argument.

It returns a canonical graph skeleton:

```elixir
graph = Docket.Graph.new(id: "essay-review", name: "Essay Review")
```

If `id:` is omitted, Docket generates a stable graph document ID and stores it
on `graph.id`. Host applications may use that ID as their primary graph ID or
store it alongside their own workflow, project, or version records.

That skeleton should be valid graph data, but it does not need to be runnable.
For example, a fresh graph may have diagnostics such as missing start edge,
missing nodes, or missing outputs. Those are advisory until verification.

`Docket.Graph.input/3` does not add an executable input node. It adds an input
field to the graph:

```elixir
graph =
  graph
  |> Docket.Graph.input(:topic,
    schema: Docket.Schema.string(),
    required: true
  )
```

An input field is data supplied when a run starts. Runtime lowering turns it
into an input channel. Nodes can read it by listing the input ID in `reads`.

```elixir
graph =
  graph
  |> Docket.Graph.node(:writer, Essay.Writer,
    reads: [:topic],
    writes: [:draft]
  )
```

In the canonical graph model, inputs are fields, not nodes. The executable entry
into the graph is still represented by start edges such as:

```elixir
Docket.Graph.edge(graph, :start, :writer)
```

Recommended API:

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

Docket.Graph.put_layout(graph, layout)
Docket.Graph.update_layout(graph, attrs_or_fun)
```

Compiler API:

```elixir
Docket.Graph.Compiler.verify(graph, opts \\ [])
Docket.Graph.Compiler.explain(graph, opts \\ [])
Docket.Graph.Compiler.compile(graph, opts \\ [])
```

`verify/2` should return:

```elixir
{:ok, Docket.Graph.Compiler.Report.t()}
| {:error, Docket.Graph.Diagnostics.t()}
```

`compile/2` should return:

```elixir
{:ok, Docket.Graph.Runtime.t(), Docket.Graph.Compiler.Report.t()}
| {:error, Docket.Graph.Diagnostics.t()}
```

The public API does not need a normal `decompile/2` path because host
applications store the canonical editable graph document directly.

## 10. Realtime And Build-Time Construction

Realtime construction means a graph may be temporarily incomplete or invalid
while a user is dragging nodes onto a canvas and connecting edges.

Build-time construction has the same property while a compiler is midway
through assembling a graph. A graph may be incomplete between function calls.

The editing API should be simple functional updates against `Docket.Graph`.
Callers pass an ID and the new shape, or an update function for that shape.

```elixir
graph =
  Docket.Graph.put_node(graph, "writer", %{
    label: "Writer",
    implementation: %{type: :registered, id: "essay_writer"},
    reads: ["topic"],
    writes: ["draft"],
    layout: %{position: %{x: 120, y: 80}}
  })
```

```elixir
graph =
  Docket.Graph.update_node(graph, "writer", %{
    label: "Draft Writer",
    reads: ["topic", "outline"]
  })
```

```elixir
graph =
  Docket.Graph.put_edge(graph, "edge_writer_reviewer", %{
    from: "writer",
    to: "reviewer",
    source_handle: "success",
    target_handle: "in"
  })
```

```elixir
graph =
  Docket.Graph.update_edge(graph, "edge_writer_reviewer", fn edge ->
    %{edge | guard: Docket.Guard.exists("draft")}
  end)
```

React Flow can call these helpers directly from UI events:

```text
drag node onto canvas -> put_node(graph, node_id, attrs)
move node -> update_node(graph, node_id, %{layout: new_layout})
connect handles -> put_edge(graph, edge_id, attrs)
edit edge condition -> update_edge(graph, edge_id, %{guard: guard})
delete edge -> delete_edge(graph, edge_id)
```

These functions update the canonical graph. They should not try to prove the
graph is executable.

After each update, callers can return the graph with warnings to the user or
continue piping more updates:

```elixir
graph =
  graph
  |> Docket.Graph.put_node("writer", %{label: "Writer"})
  |> Docket.Graph.put_edge("edge_start_writer", %{from: "$start", to: "writer"})

warnings = graph.diagnostics
```

Advisory diagnostics should allow incomplete work:

- a node without all required reads can exist
- an edge can be missing a guard temporarily
- a graph can have no start path temporarily
- diagnostics are warnings, hints, or incomplete-state markers

The update helpers should return a graph for ordinary incomplete or invalid
workflow states and record advisory diagnostics on that graph. They should only
raise or return hard errors for programming errors such as a malformed argument
that cannot be represented as graph data at all. Runnable correctness is checked
by `Docket.Graph.Compiler.verify/2` or `compile/2`.

## 11. Editing Existing Graph Versions

Published `Docket.Graph` versions are immutable. Editing an existing graph means
loading that canonical graph and using it as the starting point for a new
version.

Recommended flow:

```elixir
graph_v4 = MyApp.Graphs.fetch!("essay-review", version: 4)

draft =
  graph_v4
  |> Docket.Graph.put_node("reviewer", %{
    implementation: %{type: :registered, id: "essay_reviewer"},
    label: "Reviewer"
  })
  |> Docket.Graph.put_edge("edge_writer_reviewer", %{
    from: "writer",
    to: "reviewer"
  })
  |> Docket.Graph.metadata(:based_on_version, 4)

with {:ok, _report} <- Docket.Graph.Compiler.verify(draft) do
  {:ok, MyApp.Graphs.publish!(draft, metadata)}
end
```

Unpublished drafts may be modified in place in the host application's draft
store. Published versions should only be appended:

```text
published graph version 4
  -> copy/fetch canonical Docket.Graph
  -> edit draft in realtime
  -> preview advisory diagnostics
  -> verify runnable shape
  -> host saves/publishes immutable version 5
```

If an application truly needs to change an existing published artifact, that
should be an administrative repair operation with explicit audit logging, not
the normal editing path.

## 12. Docket Documents And App Persistence

Docket's public persistence contract is document-shaped, not adapter-shaped.
The two durable documents are:

```text
Docket.Graph
  Canonical graph definition document.
  Built, edited, verified, saved, loaded, and versioned by the host application.
  Passed to Docket when starting, resuming, or retrying a run.

Docket.Run
  Canonical run snapshot document.
  Emitted by Docket.Checkpoint callbacks.
  Saved by the host application.
  Passed back to Docket to resume or retry a run.
```

Host applications may embed those documents in larger records, store them as
JSONB, save them to object storage, serialize them to files, or keep them in
memory for tests. Docket does not define those tables, indexes, relationships,
or storage adapters.

Typical host records may include:

- workflow, workflow version, or graph artifact records that contain a
  `Docket.Graph` document
- run, job, session, message, or task records that contain a `Docket.Run`
  document
- user, project, tenant, approval, billing, or audit relationships outside the
  Docket document

Rules:

- `Docket.Graph.new/1` accepts `id:`. If omitted, Docket generates `graph.id`.
- `Docket.run/3` and `MyApp.Docket.run/3` accept `id:`. If omitted, Docket
  generates `run.id`.
- Apps may use Docket document IDs as primary keys or store them alongside their
  own internal IDs.
- Existing published graph documents should be treated as immutable by the host.
- Hashes, compression, content addressing, secondary indexes, and compiled
  runtime caches are host implementation details.
- The required Docket contract is only canonical documents in, canonical
  documents out.

## 13. React Flow Projection

React Flow should consume a UI projection of `Docket.Graph`, not runtime graph
internals.

Recommended projection:

```elixir
%Docket.Graph.Projection.ReactFlow{
  nodes: [
    %{
      id: "writer",
      type: "workflowNode",
      position: %{x: 120, y: 80},
      data: %{
        label: "Writer",
        implementation: %{type: :registered, id: "essay_writer"},
        reads: ["topic"],
        writes: ["draft"],
        diagnostics: []
      }
    }
  ],
  edges: [
    %{
      id: "edge_writer_reviewer",
      source: "writer",
      target: "reviewer",
      sourceHandle: "success",
      targetHandle: "in",
      data: %{guard: nil, diagnostics: []}
    }
  ],
  viewport: %{x: 0, y: 0, zoom: 1.0}
}
```

Projection API:

```elixir
Docket.Graph.Projection.to_react_flow(graph, opts \\ [])
Docket.Graph.Projection.from_react_flow(payload, opts \\ [])
```

React Flow events should call ordinary graph update helpers:

```text
onNodesChange position update -> update_node(graph, node_id, %{layout: ...})
onConnect -> put_edge(graph, edge_id, %{from: ..., to: ...})
onEdgesDelete -> delete_edge(graph, edge_id)
node property edit -> update_node(graph, node_id, attrs)
field panel edit -> put_field/update_field/delete_field
guard editor save -> update_edge(graph, edge_id, %{guard: guard})
```

The host app should persist the canonical `Docket.Graph` or its own
product-specific workflow record that can produce a `Docket.Graph`. It should
not persist React Flow JSON as the canonical workflow document.

## 14. Runtime Introspection And Realtime Overlays

Runtime introspection has two layers:

```text
static graph view:
  Docket.Graph -> React Flow projection

live run overlay:
  Run events/checkpoints/channel versions -> overlay data keyed by public IDs
```

The inspector can load the canonical graph version used by a run, project it to
React Flow, then stream committed run events and apply overlay updates.

Runtime events refer to runtime channels and `NodeDef` IDs. The runtime lowering
map maps those IDs back to public node, edge, and field IDs for UI overlays.
That lowering map can be returned by `Docket.Graph.Compiler.explain/2`,
included in debug events, or held inside the live Runner.

The UI can offer two modes:

```text
workflow mode:
  show Docket.Graph nodes, edges, fields, and user-facing statuses

runtime debug mode:
  reveal generated channels, barriers, subscriptions, and NodeDef details
```

This avoids forcing normal users to understand generated edge channels while
still giving engineers a precise runtime inspection view.

## 15. Lowering Rules

### 15.1 Inputs

Public graph:

```elixir
input :topic, schema: string()
```

Runtime:

```text
ChannelDef "input:topic"
  type: LastValue
```

### 15.2 State Fields

Public graph:

```elixir
field :draft, schema: string(), reducer: last_value()
```

Runtime:

```text
ChannelDef "state:draft"
  type: LastValue
```

### 15.3 Simple Edges

Public graph:

```elixir
edge :writer, :reviewer
```

Runtime:

```text
ChannelDef "edge:writer:reviewer"
  type: Ephemeral

NodeDef "writer"
  system_writes: ["edge:writer:reviewer"]

NodeDef "reviewer"
  subscribe: ["edge:writer:reviewer"]
```

User node code does not manually write edge signal channels. The Runner emits
compiler-generated system writes after successful node completion.

### 15.4 Fan-Out

Public graph:

```elixir
edge :researcher, [:summarizer, :tester]
```

Runtime:

```text
edge:researcher:summarizer
edge:researcher:tester
```

### 15.5 Fan-In

Public graph:

```elixir
join [:researcher, :tester], :reviewer
```

Runtime:

```text
edge:researcher:reviewer
edge:tester:reviewer
barrier:researcher+tester:reviewer
```

The canonical graph may preserve this as one join edge/group. Runtime lowering
may use multiple edge channels plus a barrier channel.

### 15.6 Conditional Edges

Public graph:

```elixir
edge :reviewer, :deploy,
  guard: equals(path(:review, [:status]), :approved)
```

Runtime:

```text
edge:reviewer:deploy
```

The target `NodeDef` subscribes to the generated edge channel and carries the
compiled guard expression.

## 16. Diagnostics And Runtime Verification

`Docket.Graph` diagnostics are advisory. They exist to help the user build and
edit the graph, not to block ordinary editing.

Advisory diagnostics:

- produce diagnostics for UI display
- allow incomplete graphs
- support realtime editing
- may warn about missing fields, disconnected nodes, missing start edges, or
  incomplete node configuration
- should use public IDs and UI paths whenever possible
- should not prevent saving or continuing to edit the graph

Runtime verification is the blocking gate. `Docket.Graph.Compiler.verify/2`
checks whether the graph can compile, and `Docket.Graph.Compiler.compile/2`
returns the compiled runtime graph. Both paths must reject graphs that cannot
run safely.

Runtime verification rejects:

- references that cannot resolve
- edges with invalid endpoints
- duplicate IDs that cannot be represented safely
- invalid start or activation paths
- schema, reducer, or guard errors
- impossible joins and barriers
- cycles without limits or halt conditions
- node implementation references rejected by policy

Runtime lowering validation:

- rejects generated channel ID collisions
- rejects subscriptions to missing channels
- rejects writes to undeclared channels
- verifies every runtime ID maps back to public intent for debugging

Diagnostics should use public IDs whenever possible, even when verification
returns them as blocking errors:

```elixir
%Docket.Graph.Diagnostic{
  severity: :error,
  code: :unknown_field,
  message: "node writer reads unknown field review",
  path: [:nodes, "writer", :reads, "review"],
  public_id: "writer",
  runtime_id: nil
}
```

## 17. Persistence Responsibilities

Docket graph and run persistence is host-owned. The host application may persist
editable drafts as:

- canonical `Docket.Graph` values
- product-specific workflow records that can produce `Docket.Graph` values

The host also persists immutable published `Docket.Graph` versions and
`Docket.Run` documents emitted by checkpoints.

Recommended split:

```text
Host graph records:
  editable Docket.Graph
  immutable published Docket.Graph versions
  collaboration metadata
  UI layout
  product ownership and permissions

Host run records:
  latest Docket.Run document
  status, ownership, and indexing fields
  project, user, session, message, or job relationships
```

Host storage does not need to know about `Docket.Graph.Runtime` or
`Docket.Run.State`. Runtime materialization happens when Docket verifies a graph,
starts a run, resumes a run, or retries a run.

## 18. WaterCooler Workflow Compiler

WaterCooler should compile workflow records into `Docket.Graph`, not directly
into runtime channels.

Mapping:

```text
workflow definition -> Docket.Graph
workflow step -> Docket.Graph.Node
gate -> guarded edge, branch, or join
step result -> graph field write
workflow canvas layout -> graph layout metadata
published workflow version -> immutable Docket.Graph version
current_step_id -> compatibility projection over run overlay/frontier
RuntimeChannel execution -> Executor adapter
```

Compatibility compiler shape:

```elixir
defmodule WaterCooler.Docket.WorkflowCompiler do
  def to_graph(workflow, opts) do
    Docket.Graph.new(id: workflow.id, name: workflow.name)
    |> add_inputs(workflow)
    |> add_fields(workflow)
    |> add_nodes(workflow)
    |> add_edges_and_gates(workflow)
    |> add_outputs(workflow)
    |> add_layout(workflow)
  end

  def verify(workflow, opts) do
    workflow
    |> to_graph(opts)
    |> Docket.Graph.Compiler.verify(opts)
  end
end
```

## 19. End-To-End Editing Flow

```text
1. User opens workflow editor.
2. Host loads latest published Docket.Graph or existing draft Docket.Graph.
3. Host projects Docket.Graph to React Flow.
4. User drags a node onto the canvas.
5. UI sends the node ID and node shape.
6. Server calls `put_node/3` on the graph.
7. Server returns updated projection plus advisory diagnostics.
8. User connects nodes.
9. UI sends the edge ID and edge shape.
10. Server calls `put_edge/3` and returns advisory diagnostics.
11. User clicks publish.
12. Docket.Graph.Compiler.verify/2 runs blocking runtime verification.
13. Host saves immutable Docket.Graph version N+1.
14. New runs use version N+1; active runs stay on their original version.
```

## 20. End-To-End Runtime Inspection Flow

```text
1. User opens run inspector.
2. Server loads the latest Docket.Run document.
3. Server fetches the canonical Docket.Graph document using run.graph_id and
   run.graph_version.
4. Server projects Docket.Graph to React Flow.
5. Server streams committed run events.
6. Server maps runtime channel/node IDs to public IDs using lowering metadata.
7. UI overlays active/completed/failed/waiting status on public nodes and edges.
8. User can switch to runtime debug mode to inspect generated channels/barriers.
```

## 21. LangGraph Reference Notes

LangGraph is a useful reference, but Docket should not copy it exactly.

In LangGraph, users define a `StateGraph` with state schema, nodes, and edges.
Compilation validates the graph and lowers it to a Pregel-style runtime graph
with channels, triggers, and writers. The direct Pregel API can also be used by
hand, but ordinary users generally work through `StateGraph`.

The useful lowering pattern is:

```text
state schema fields -> runtime channels
node -> Pregel node that reads subscribed state channels and writes updates
simple edge A -> B -> A writes to B's generated activation channel
fan-in edge [A, B] -> C -> generated join/barrier channel
conditional edge -> branch writer chooses generated activation channel(s)
```

Docket should borrow that lowering model, but expose a cleaner storage and
editing model:

```text
Docket.Graph is canonical and editable.
Docket.Run is canonical and restorable.
Docket.Graph.Runtime is internal and derived.
Host applications store Docket documents, not runtime graph internals.
```

LangGraph does not appear to expose first-class `update_node`, `delete_node`,
`update_edge`, or `delete_edge` helpers on an already compiled graph. Its
builder methods warn when called after compile because those edits are not
reflected in the compiled graph. Docket's editing story should stay centered on
simple updates to canonical `Docket.Graph` values:

```elixir
graph = Docket.Graph.update_node(graph, node_id, attrs)
graph = Docket.Graph.update_edge(graph, edge_id, attrs)
```

LangGraph's migration guidance also supports keeping active runs pinned to the
graph version they started with. Docket should keep the simple v1 rule:

```text
new edits append a new Docket.Graph version;
active runs stay on the graph version they started with.
```

Sources:

- https://docs.langchain.com/oss/python/langgraph/graph-api
- https://docs.langchain.com/oss/python/langgraph/pregel
- https://github.com/langchain-ai/langgraph/blob/main/libs/langgraph/langgraph/graph/state.py

## 22. MVP Scope

Recommended MVP:

1. `Docket.Graph` as the canonical public graph document.
2. One functional graph editing API for both build-time pipes and realtime UI
   edits.
3. Graph helpers for fields, inputs, nodes, edges, joins, outputs, policies,
   metadata, and layout.
4. App-owned graph persistence with optional `id:` generation.
5. Advisory graph diagnostics and blocking compiler verification.
6. Single `Docket.Graph.Compiler` module with `verify/2`, `explain/2`, and
   `compile/2`.
7. Internal `Docket.Graph.Runtime` materialization returned by `compile/2`.
8. React Flow projection helpers.
9. Runtime overlay mapping from events/channels back to public IDs.
10. Sequential workflow compatibility compiler through `Docket.Graph`.

## 23. Open Questions

1. Should `Docket.Graph.Compiler.verify/2` be public, or should publish/start
   APIs hide verification entirely?
2. Should branch/join sugar be preserved as first-class graph elements, or
   normalized to edges plus metadata?
3. What is the minimum React Flow projection Docket should own versus leaving
   all UI-specific projection to WaterCooler?
4. Should collaborative editing revisions be entirely host-owned?
5. Should `Docket.Graph.Runtime` live under `Docket.Graph.Runtime` or be named
   `Docket.RuntimeGraph` for extra internal separation?

## 24. Strong Recommendations

1. Treat `Docket.Graph` as the canonical public graph.
2. Treat `Docket.Graph.Runtime` as internal derived runtime materialization.
3. Keep graph persistence app-owned; Docket should accept graph documents as
   values.
4. Treat `Docket.Run` as the canonical public resume document emitted by
   checkpoints.
5. Keep published graph versions immutable and append-only.
6. Support editing existing graph versions by fetching the canonical graph and
   appending a new version after edits.
7. Make build-time and realtime graph construction use the same graph update
   helpers.
8. Make React Flow a projection of `Docket.Graph`, not the canonical workflow
   document.
9. Show generated channels only in runtime debug mode.
10. Build compiler verify, compile, explain, and project-to-React-Flow early
    because they will reveal API mistakes quickly.
