# Docket: Graph Construction Public API Design

Status: draft
Date: 2026-06-25

Related document: `docs/architecture/docket-runtime-design.md`

## 1. Executive Summary

This document designs Docket's public graph construction layer.

The key correction is:

```text
Docket.StateGraph is not a throwaway builder.

Docket.StateGraph is the user-facing, editable graph facade.
Docket.Graph is the canonical executable graph artifact.

Docket must compile StateGraph -> Docket.Graph.
Docket must decompile Docket.Graph -> StateGraph.
```

Users, product UIs, workflow compilers, and React Flow-style editors should work
with `Docket.StateGraph`: fields, nodes, edges, guards, outputs, layout, and
advisory diagnostics. The Runner should work with `Docket.Graph`: channels,
subscriptions, reducers, generated edge channels, barriers, and runtime node
definitions.

The design goal is not to create two unrelated schemas. It is to keep one
round-trippable public facade and one executable runtime artifact:

```text
StateGraph draft
  -> compile
  -> immutable executable Docket.Graph
  -> decompile
  -> editable StateGraph draft
```

That round trip is required for editing existing graph versions, realtime canvas
construction, graph previews, visual debugging, and user-facing workflow
inspection.

## 2. Core Model

There are three related but distinct things:

```text
StateGraph
  User-facing graph model.
  Editable.
  Suitable for UI and workflow compilers.
  Describes fields, nodes, edges, joins, branches, outputs, and layout.

Docket.Graph
  Canonical executable graph artifact.
  Immutable once published.
  Loaded by the Runner.
  Contains runtime channels, NodeDef records, policies, metadata, and lowering
  maps.

Run State
  Mutable execution state for one run.
  Owned by one Runner process.
  Contains channel values, versions, active tasks, interrupts, timers, events,
  and checkpoints.
```

The graph construction layer only handles the first two. It does not execute
nodes or mutate run state.

## 3. Design Goals

1. Make `Docket.StateGraph` the editable public graph facade.
2. Make `Docket.Graph` the immutable executable artifact consumed by the
   runtime.
3. Support lossless compile/decompile for Docket-authored graphs.
4. Support best-effort decompile for old or hand-authored runtime graphs.
5. Support realtime graph construction through simple StateGraph update
   functions.
6. Support React Flow and similar UI projections without making UI layout part
   of runtime execution.
7. Keep generated channels hidden from normal users but inspectable for
   debugging.
8. Let users edit existing published graph versions by decompiling them into a
   StateGraph and publishing a new graph version.
9. Keep published graph versions immutable.
10. Preserve stable public IDs and stable lowering metadata.
11. Treat StateGraph diagnostics as advisory; compile is where invalid graphs
    become blocking errors.

## 4. Non-Goals

1. Do not introduce a separate `Builder`, `NodeSpec`, or `EdgeSpec` schema that
   mirrors `StateGraph`.
2. Do not ask UI code to construct `Docket.Graph.ChannelDef` or
   `Docket.Graph.NodeDef` directly.
3. Do not mutate a published `Docket.Graph` in place.
4. Do not require every UI edit to produce a valid executable graph.
5. Do not make React Flow the canonical graph model.
6. Do not make graph layout affect runtime execution semantics.
7. Do not create a separate validation lifecycle for drafts. A StateGraph is the
   draft graph; only compile performs blocking validation.

## 5. Module Shape

Recommended construction-facing modules:

```text
Docket.StateGraph
Docket.StateGraph.Node
Docket.StateGraph.Edge
Docket.StateGraph.Field
Docket.StateGraph.Output
Docket.StateGraph.Layout
Docket.StateGraph.Projection
Docket.StateGraph.Diagnostics
Docket.Compiler
Docket.Decompiler
Docket.Schema
Docket.Reducer
Docket.Guard
```

Runtime-facing modules remain:

```text
Docket.Graph
Docket.Graph.NodeDef
Docket.Graph.ChannelDef
Docket.Runner
```

The naming should make the boundary clear:

- `StateGraph.Node` is an editable public graph node.
- `Graph.NodeDef` is a compiled runtime node definition.
- A module implementing `Docket.Node` is executable node code.

## 6. StateGraph

`Docket.StateGraph` is the public graph model and the structure UI tools edit.

It contains:

- graph identity
- optional source graph version
- input fields
- state fields
- output projections
- nodes
- edges
- joins and branch sugar, if preserved separately
- policies
- layout and UI metadata
- diagnostics
- provenance

Sketch:

```elixir
defmodule Docket.StateGraph do
  defstruct [
    :id,
    :name,
    :description,
    :source_graph_version,
    :schema_version,
    fields: %{},
    inputs: %{},
    outputs: %{},
    nodes: %{},
    edges: %{},
    policies: %{},
    layout: %Docket.StateGraph.Layout{},
    metadata: %{},
    diagnostics: []
  ]
end
```

This is not a temporary builder. The functional API returns updated
`StateGraph` values:

```elixir
graph =
  Docket.StateGraph.new("essay-review", name: "Essay Review")
  |> Docket.StateGraph.input(:topic, schema: Docket.Schema.string())
  |> Docket.StateGraph.field(:draft,
    schema: Docket.Schema.string(),
    reducer: Docket.Reducer.last_value()
  )
  |> Docket.StateGraph.node(:writer, Essay.Writer,
    reads: [:topic],
    writes: [:draft]
  )
  |> Docket.StateGraph.edge(:start, :writer)
  |> Docket.StateGraph.edge(:writer, :finish)
  |> Docket.StateGraph.output(:draft)
```

## 7. Public Nodes And Runtime NodeDefs

The word "node" appears in three places. They are related but not identical.

```text
StateGraph.Node
  Editable public node in the graph facade.
  Used by UI, workflow compilers, validation, and decompile.
  Names public fields and public edges.

Docket.Node
  Behaviour implemented by executable node code.
  Receives Docket.Node.Input and returns Docket.Node.Output, interrupt, await,
  or error.

Docket.Graph.NodeDef
  Compiled runtime definition consumed by the Runner.
  Names runtime channels, subscriptions, guards, executor settings, and
  generated system writes.
```

Example public node:

```elixir
%Docket.StateGraph.Node{
  id: "writer",
  implementation: %{type: :module, module: Essay.Writer, function: :call},
  reads: ["topic"],
  writes: ["draft"],
  metadata: %{label: "Write Draft"}
}
```

Example compiled runtime node definition:

```elixir
%Docket.Graph.NodeDef{
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

The difference is the level of abstraction:

- The public node says "writer reads topic and writes draft."
- The runtime `NodeDef` says "writer subscribes to this edge channel, reads
  this input channel, may write this state channel, and emits this system edge
  channel on success."

## 8. Compile And Decompile

Docket must support both directions.

Compile:

```elixir
{:ok, docket_graph, report} =
  Docket.Compiler.compile(state_graph,
    version: 4,
    origin: %{type: "workflow", id: "workflow_123", version: 7}
  )
```

Decompile:

```elixir
{:ok, state_graph, report} =
  Docket.Decompiler.decompile(docket_graph)
```

The compiler lowers user-facing concepts:

```text
input field -> input channel
state field -> state channel
edge -> generated edge channel plus subscriptions
join -> generated barrier channel
branch -> guarded edges or generated branch node/channel
output -> output channel projection
StateGraph.Node -> Graph.NodeDef
```

The decompiler reverses that lowering using metadata stored in `Docket.Graph`.
For Docket-authored graphs, this should be lossless. For old, imported, or
hand-authored `Docket.Graph` values that lack facade metadata, decompile may be
best-effort and should return warnings.

## 9. Round-Trip Metadata

To support editing, `Docket.Graph` must preserve public graph intent.

Recommended additions to `Docket.Graph.metadata`:

```elixir
%{
  facade: %{
    type: "Docket.StateGraph",
    schema_version: 1,
    state_graph: encoded_state_graph
  },
  lowering: %{
    fields: %{
      "topic" => "input:topic",
      "draft" => "state:draft"
    },
    nodes: %{
      "writer" => "writer"
    },
    edges: %{
      "edge_public_1" => "edge:$start:writer",
      "edge_public_2" => "edge:writer:$finish"
    },
    generated_channels: %{
      "edge:$start:writer" => %{kind: :edge, public_edge_id: "edge_public_1"},
      "edge:writer:$finish" => %{kind: :edge, public_edge_id: "edge_public_2"}
    }
  }
}
```

The embedded `state_graph` gives Docket a lossless editable facade. The lowering
map lets tooling explain how the facade became the runtime graph.

The Runner does not execute the embedded facade. It executes the compiled
runtime fields in `Docket.Graph`. The facade is for editing, inspection,
projection, and decompile.

## 10. Functional StateGraph API

Recommended API:

```elixir
Docket.StateGraph.new(id, opts \\ [])
Docket.StateGraph.input(graph, id, opts)
Docket.StateGraph.field(graph, id, opts)
Docket.StateGraph.output(graph, id, opts \\ [])
Docket.StateGraph.node(graph, id, implementation, opts)
Docket.StateGraph.edge(graph, from, to, opts \\ [])
Docket.StateGraph.join(graph, from_nodes, to_node, opts \\ [])
Docket.StateGraph.branch(graph, from_node, opts)
Docket.StateGraph.policy(graph, key, value)
Docket.StateGraph.metadata(graph, key, value)
Docket.StateGraph.diagnostics(graph, opts \\ [])

Docket.StateGraph.put_node(graph, id, attrs)
Docket.StateGraph.update_node(graph, id, attrs_or_fun)
Docket.StateGraph.delete_node(graph, id, opts \\ [])

Docket.StateGraph.put_edge(graph, id, attrs)
Docket.StateGraph.update_edge(graph, id, attrs_or_fun)
Docket.StateGraph.delete_edge(graph, id, opts \\ [])

Docket.StateGraph.put_field(graph, id, attrs)
Docket.StateGraph.update_field(graph, id, attrs_or_fun)
Docket.StateGraph.delete_field(graph, id, opts \\ [])

Docket.StateGraph.put_layout(graph, layout)
Docket.StateGraph.update_layout(graph, attrs_or_fun)
```

Compiler/decompiler API:

```elixir
Docket.Compiler.compile(state_graph, opts \\ [])
Docket.Compiler.explain(state_graph_or_graph, opts \\ [])
Docket.Compiler.publish(runtime, state_graph, opts \\ [])

Docket.Decompiler.decompile(docket_graph, opts \\ [])
Docket.Decompiler.to_state_graph(docket_graph, opts \\ [])
```

`compile/2` should return:

```elixir
{:ok, Docket.Graph.t(), Docket.Compiler.Report.t()}
| {:error, Docket.StateGraph.Diagnostics.t()}
```

`decompile/2` should return:

```elixir
{:ok, Docket.StateGraph.t(), Docket.Decompiler.Report.t()}
| {:error, Docket.StateGraph.Diagnostics.t()}
```

## 11. Realtime Construction

Realtime construction means a graph may be temporarily incomplete or invalid
while a user is dragging nodes onto a canvas and connecting edges.

The editing API should be simple functional updates against `StateGraph`.
Callers pass an ID and the new shape, or an update function for that shape.

```elixir
Docket.StateGraph.put_node(graph, "writer", %{
  label: "Writer",
  implementation: %{type: :registered, id: "essay_writer"},
  reads: ["topic"],
  writes: ["draft"],
  layout: %{position: %{x: 120, y: 80}}
})
```

```elixir
Docket.StateGraph.update_node(graph, "writer", %{
  label: "Draft Writer",
  reads: ["topic", "outline"]
})
```

```elixir
Docket.StateGraph.put_edge(graph, "edge_writer_reviewer", %{
  from: "writer",
  to: "reviewer",
  source_handle: "success",
  target_handle: "in"
})
```

```elixir
Docket.StateGraph.update_edge(graph, "edge_writer_reviewer", fn edge ->
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

These functions update the StateGraph. They should not try to prove the graph is
executable.

Example from a controller or LiveView handler:

```elixir
graph =
  graph
  |> Docket.StateGraph.put_node("writer", %{
    implementation: %{type: :registered, id: "essay_writer"},
    label: "Writer",
    layout: %{position: %{x: 120, y: 80}}
  })
  |> Docket.StateGraph.put_edge("edge_start_writer", %{
    from: "$start",
    to: "writer"
  })
```

After updating, callers can ask for advisory diagnostics:

```elixir
warnings = Docket.StateGraph.diagnostics(graph)
```

StateGraph diagnostics should allow incomplete work:

- a node without all required reads can exist
- an edge can be missing a guard temporarily
- a graph can have no start path temporarily
- diagnostics are warnings, hints, or incomplete-state markers

The update helpers should only fail when the requested shape cannot be applied
to the StateGraph data structure at all. Graph correctness is checked when
compiling.

## 12. Editing Existing Graphs

Published `Docket.Graph` versions are immutable. Editing an existing graph means
decompiling a version into a mutable `StateGraph`.

Recommended flow:

```elixir
{:ok, graph} =
  Docket.GraphStore.load_graph("essay-review", 4, opts)

{:ok, draft, _report} =
  Docket.Decompiler.decompile(graph)

draft =
  draft
  |> Docket.StateGraph.put_node("reviewer", %{
    implementation: %{type: :registered, id: "essay_reviewer"},
    label: "Reviewer"
  })
  |> Docket.StateGraph.put_edge("edge_writer_reviewer", %{
    from: "writer",
    to: "reviewer"
  })

{:ok, new_graph, _report} =
  Docket.Compiler.compile(draft, version: 5)
```

Unpublished drafts may be modified in place in the host application's draft
store. Published graph artifacts should not be modified in place because active
runs may depend on their exact graph version.

This gives the product UX a natural model:

```text
published version 4
  -> decompile to StateGraph
  -> edit draft in realtime
  -> preview advisory diagnostics
  -> publish immutable version 5
```

If an application truly needs to change an existing published artifact, that
should be an administrative repair operation with explicit audit logging, not
the normal editing path.

## 13. React Flow Projection

React Flow should not consume raw `Docket.Graph` by default. It should consume a
UI projection of `StateGraph`.

Recommended projection:

```elixir
%Docket.StateGraph.Projection.ReactFlow{
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
Docket.StateGraph.Projection.to_react_flow(state_graph, opts \\ [])
Docket.StateGraph.Projection.from_react_flow(payload, opts \\ [])
```

React Flow events should call ordinary StateGraph update helpers:

```text
onNodesChange position update -> update_node(graph, node_id, %{layout: ...})
onConnect -> put_edge(graph, edge_id, %{from: ..., to: ...})
onEdgesDelete -> delete_edge(graph, edge_id)
node property edit -> update_node(graph, node_id, attrs)
field panel edit -> put_field/update_field/delete_field
guard editor save -> update_edge(graph, edge_id, %{guard: guard})
```

The host app should persist the editable `StateGraph` or its own
product-specific workflow record. It should not persist React Flow JSON as the
canonical workflow document. React Flow JSON is a view format.

## 14. Runtime Introspection And Realtime Overlays

Runtime introspection has two layers:

```text
static graph view:
  Docket.Graph -> Docket.Decompiler.decompile/2 -> StateGraph -> React Flow

live run overlay:
  Run events/checkpoints/channel versions -> overlay data keyed by public IDs
```

The inspector should decompile the graph once when the run page opens, then
stream committed run events and apply overlay updates.

Example overlay:

```elixir
%{
  nodes: %{
    "writer" => %{
      status: :completed,
      last_started_at: "...",
      last_finished_at: "...",
      attempt: 1
    },
    "reviewer" => %{
      status: :active
    }
  },
  edges: %{
    "edge_writer_reviewer" => %{
      activated_at_superstep: 2,
      channel_version: 1
    }
  },
  fields: %{
    "draft" => %{
      channel: "state:draft",
      version: 3,
      preview: "..."
    }
  }
}
```

Runtime events refer to runtime channels and `NodeDef` IDs. The lowering map in
`Docket.Graph.metadata.lowering` maps those runtime IDs back to public node,
edge, and field IDs for UI overlays.

The UI can offer two modes:

```text
workflow mode:
  show StateGraph nodes, edges, fields, and user-facing statuses

runtime debug mode:
  reveal generated channels, barriers, subscriptions, and NodeDef details
```

This avoids forcing normal users to understand generated edge channels while
still giving engineers a precise runtime inspection view.

## 15. Lowering Rules

### 15.1 Inputs

StateGraph:

```elixir
input :topic, schema: string()
```

Runtime:

```text
ChannelDef "input:topic"
  type: LastValue
```

### 15.2 State Fields

StateGraph:

```elixir
field :draft, schema: string(), reducer: last_value()
```

Runtime:

```text
ChannelDef "state:draft"
  type: LastValue
```

### 15.3 Simple Edges

StateGraph:

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

StateGraph:

```elixir
edge :researcher, [:summarizer, :tester]
```

Runtime:

```text
edge:researcher:summarizer
edge:researcher:tester
```

### 15.5 Fan-In

StateGraph:

```elixir
join [:researcher, :tester], :reviewer
```

Runtime:

```text
edge:researcher:reviewer
edge:tester:reviewer
barrier:researcher+tester:reviewer
```

The public facade may preserve this as one join edge/group. Runtime lowering may
use multiple edge channels plus a barrier channel.

### 15.6 Conditional Edges

StateGraph:

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

## 16. Diagnostics And Compile Validation

`StateGraph` diagnostics are advisory. They exist to help the user build and
edit the graph, not to block ordinary editing.

StateGraph diagnostics:

- produces diagnostics for UI display
- allows incomplete graphs
- supports realtime editing
- may warn about missing fields, disconnected nodes, missing start edges, or
  incomplete node configuration
- should use public IDs and UI paths whenever possible
- should not prevent saving or continuing to edit the StateGraph

Compile validation is the blocking gate. When `Docket.Compiler.compile/2`
converts a `StateGraph` into an immutable executable `Docket.Graph`, it must
reject graphs that cannot run safely.

Compile validation rejects:

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
- verifies every runtime ID maps back to public intent
- verifies decompile metadata is present for Docket-authored graphs

Diagnostics should use public IDs whenever possible, even when compile returns
them as blocking errors:

```elixir
%Docket.StateGraph.Diagnostic{
  severity: :error,
  code: :unknown_field,
  message: "node writer reads unknown field review",
  path: [:nodes, "writer", :reads, "review"],
  public_id: "writer",
  runtime_id: nil
}
```

## 17. Persistence Responsibilities

The host application may persist editable drafts as:

- full encoded `StateGraph`
- product-specific workflow records that compile into `StateGraph`

Docket's graph store persists immutable executable `Docket.Graph` artifacts.

Recommended split:

```text
Host draft store:
  editable StateGraph
  collaboration metadata
  UI layout
  product ownership and permissions

Docket GraphStore:
  immutable Docket.Graph version
  embedded StateGraph facade
  lowering map
  executable runtime channels and NodeDefs
```

This lets product editing remain flexible while ensuring the runtime always has
a complete executable artifact.

## 18. WaterCooler Workflow Compiler

WaterCooler should compile workflow records into `Docket.StateGraph`, not
directly into runtime channels.

Mapping:

```text
workflow definition -> StateGraph
workflow step -> StateGraph.Node
gate -> guarded edge, branch, or join
step result -> StateGraph field write
workflow canvas layout -> StateGraph layout metadata
published workflow version -> immutable Docket.Graph version
current_step_id -> compatibility projection over run overlay/frontier
RuntimeChannel execution -> Executor adapter
```

Compatibility compiler shape:

```elixir
defmodule WaterCooler.Docket.WorkflowCompiler do
  def to_state_graph(workflow, opts) do
    workflow.id
    |> Docket.StateGraph.new(name: workflow.name)
    |> add_inputs(workflow)
    |> add_fields(workflow)
    |> add_nodes(workflow)
    |> add_edges_and_gates(workflow)
    |> add_outputs(workflow)
    |> add_layout(workflow)
  end

  def compile(workflow, opts) do
    workflow
    |> to_state_graph(opts)
    |> Docket.Compiler.compile(opts)
  end
end
```

## 19. End-To-End Editing Flow

```text
1. User opens workflow editor.
2. Host loads latest published Docket.Graph or existing draft StateGraph.
3. If loading Docket.Graph, Docket.Decompiler.decompile/2 returns StateGraph.
4. Host projects StateGraph to React Flow.
5. User drags a node onto the canvas.
6. UI sends the node ID and node shape.
7. Server calls `put_node/3` on the StateGraph.
8. Server returns updated projection plus diagnostics.
9. User connects nodes.
10. UI sends the edge ID and edge shape.
11. Server calls `put_edge/3` and returns advisory diagnostics.
12. User clicks publish.
13. Docket.Compiler.compile/2 runs blocking validation and lowering.
14. GraphStore saves immutable Docket.Graph version N+1.
15. New runs use version N+1; active runs stay on their original version.
```

## 20. End-To-End Runtime Inspection Flow

```text
1. User opens run inspector.
2. Server loads run checkpoint and graph identity.
3. Server loads immutable Docket.Graph.
4. Server decompiles Docket.Graph to StateGraph.
5. Server projects StateGraph to React Flow.
6. Server maps runtime channel/node IDs to public IDs using lowering metadata.
7. Server streams committed run events.
8. UI overlays active/completed/failed/waiting status on public nodes and edges.
9. User can switch to runtime debug mode to inspect generated channels/barriers.
```

## 21. LangGraph Reference Notes

LangGraph is a useful reference, but Docket should not copy it exactly.

In LangGraph, users define a `StateGraph` with state schema, nodes, and edges.
Compilation validates the graph and lowers it to a Pregel-style runtime graph
with channels, triggers, and writers. The direct Pregel API can also be used by
hand, but ordinary users generally work through `StateGraph`.

The important lowering pattern is:

```text
state schema fields -> runtime channels
node -> Pregel node that reads subscribed state channels and writes updates
simple edge A -> B -> A writes to B's generated activation channel
fan-in edge [A, B] -> C -> generated join/barrier channel
conditional edge -> branch writer chooses generated activation channel(s)
```

That pattern is exactly the mental model Docket should borrow:

```text
StateGraph is the editable facade.
Docket.Graph is the executable lowered artifact.
```

The place Docket should intentionally differ is editing. LangGraph does not
appear to expose first-class `update_node`, `delete_node`, `update_edge`, or
`delete_edge` helpers on an already compiled graph. Its builder methods warn
when called after compile because those edits are not reflected in the compiled
graph. Docket needs a stronger editing story:

```elixir
graph = Docket.Decompiler.decompile(compiled_graph)
graph = Docket.StateGraph.update_node(graph, node_id, attrs)
graph = Docket.StateGraph.update_edge(graph, edge_id, attrs)
{:ok, new_compiled_graph, report} = Docket.Compiler.compile(graph, version: :next)
```

So for Docket, compiled graphs remain immutable, but existing graph versions can
be viewed and edited by round-tripping through `StateGraph`. A user editing a
graph in React Flow is editing the `StateGraph` facade, not mutating Pregel
channels or runtime node definitions directly.

LangGraph's migration guidance also supports this split. Completed runs can
move to an entirely new topology, but interrupted or resumable runs are more
sensitive to node renames/removals because execution may resume inside a node
that no longer exists. Docket should keep the simple v1 rule:

```text
new edits publish a new Docket.Graph version;
active runs stay on the graph version they started with.
```

Sources:

- https://docs.langchain.com/oss/python/langgraph/graph-api
- https://docs.langchain.com/oss/python/langgraph/pregel
- https://github.com/langchain-ai/langgraph/blob/main/libs/langgraph/langgraph/graph/state.py

## 22. MVP Scope

Recommended MVP:

1. `Docket.StateGraph` as the editable public graph struct.
2. Functional StateGraph API for fields, inputs, nodes, edges, joins, outputs,
   policies, metadata, and layout.
3. `Docket.Compiler.compile/2` from `StateGraph` to `Docket.Graph`.
4. `Docket.Decompiler.decompile/2` from `Docket.Graph` to `StateGraph`.
5. Embedded facade metadata and lowering maps in `Docket.Graph`.
6. Simple StateGraph update helpers for realtime construction.
7. Advisory StateGraph diagnostics and blocking compile validation.
8. React Flow projection helpers.
9. Runtime overlay mapping from events/channels back to public IDs.
10. Sequential workflow compatibility compiler through `StateGraph`.

## 23. Open Questions

1. Should the embedded `StateGraph` facade be stored inline in
   `Docket.Graph.metadata`, content-addressed beside the graph, or both?
2. Should Docket own only simple StateGraph update helpers, or also provide
   optional convenience helpers for common UI edits?
3. Should branch/join sugar be preserved as first-class StateGraph elements, or
   normalized to edges plus metadata?
4. What is the minimum React Flow projection Docket should own versus leaving
   all UI-specific projection to WaterCooler?
5. Should decompile of hand-authored `Docket.Graph` values be best-effort only,
   or should Docket require all executable graphs to carry facade metadata?
6. Should collaborative editing revisions be entirely host-owned?

## 24. Strong Recommendations

1. Treat `StateGraph` as the editable facade, not a builder wrapper.
2. Remove separate `Builder`, `NodeSpec`, and `EdgeSpec` schemas unless a future
   implementation proves they are necessary.
3. Require Docket-authored `Docket.Graph` artifacts to contain enough metadata
   to decompile losslessly.
4. Keep published `Docket.Graph` versions immutable.
5. Support editing existing graphs by decompiling to StateGraph and publishing a
   new graph version.
6. Make simple StateGraph update helpers the foundation for realtime graph
   construction.
7. Make React Flow a projection of `StateGraph`, not the canonical workflow
   document.
8. Use lowering maps to power runtime introspection overlays.
9. Show generated channels only in runtime debug mode.
10. Build compile, decompile, explain, and project-to-React-Flow early because
    they will reveal API mistakes quickly.
