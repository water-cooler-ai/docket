# Docket Graph Construction Design

Status: implemented reference
Date: 2026-06-25

Related documents:

- [Compiler design](docket-compiler-design.md)
- [Runtime rationale](docket-runtime-design.md)

## Public And Internal Models

`Docket.Graph` is the canonical public graph document. Application code,
workflow importers, and graph editors use it to describe inputs, state fields,
outputs, nodes, edges, guards, policies, metadata, and diagnostics.

`Docket.Runtime.Graph` is the internal executable materialization produced by
`Docket.Graph.Compiler`. Applications do not construct or persist runtime graph
records.

`Docket.Run` is the durable execution-state document encoded by the configured
backend and returned through committed reads. Applications retain run IDs and
business projections; they do not persist a second run document or pass one
back to a resident runtime.

## Construction And Publication Flow

```text
author or edit
  -> Docket.Graph
  -> Docket.Graph.verify/2

publish
  -> MyApp.Docket.save_graph/2
  -> effective immutable graph version in the configured backend
  -> Docket.GraphRef

start
  -> MyApp.Docket.start_run(graph_ref, input)
  -> backend-owned durable run
  -> run ID or committed run result, depending on testing mode

inspect
  -> fetch_run / inspect_run / list_events / await_run
```

Publication snapshots node configuration defaults, compiles the effective
graph, stores that exact content-addressed graph through the backend, and
returns a `Docket.GraphRef`. Starting a durable run requires that reference so
the run remains pinned to the exact effective graph hash.

Applications may keep unpublished drafts or product-specific workflow records
outside Docket. Published effective graph versions and run state belong to the
configured backend.

## Document Shape

A graph contains:

- `id`, optional display name and description, and `schema_version`
- input fields and state fields
- output projections
- executable nodes
- edges, including multi-source joins and guarded branches
- graph policies and application metadata
- transient compiler diagnostics

Graph semantic content is durable data. Open application content such as config
and metadata uses JSON-like strings, numbers, booleans, lists, and string-keyed
maps. Docket structs and supported descriptor atoms are encoded through the
private versioned codec. Functions, PIDs, references, and arbitrary terms are
not durable graph content.

Compiler diagnostics are transient. They are omitted from durable identity and
replaced on every verification.

## IDs And Identity

Public graph IDs are non-empty binaries matching the syntax enforced by
`Docket.Graph`. Inputs and state fields share the graph state namespace. Node
and edge IDs use separate namespaces. `"$start"` and `"$finish"` are reserved
edge endpoints and cannot be node IDs.

Docket generates the graph ID when `Docket.Graph.new/1` omits one. Tests may
inject an ID generator for deterministic fixtures. Record-editing functions
take their public record IDs explicitly.

The compiler hashes the exact deterministic bytes of the effective published
graph. The full SHA-256 digest is the durable content identity; shortened forms
are display-only. Applications use `Docket.GraphRef` rather than recomputing a
hash from an editable graph.

## Editing API

Build-time construction and realtime editing use the same functional API.

Non-bang functions return:

```elixir
{:ok, Docket.Graph.t()} | {:error, Docket.Graph.Error.t()}
```

Bang functions return the updated graph or raise `Docket.Graph.Error`.

The API includes put/update/delete operations for inputs, fields, outputs,
nodes, and edges plus graph policy and metadata helpers. A successful edit
clears stale diagnostics. Editing validates that the operation can be
represented as graph data; it does not require the intermediate graph to be
runnable.

```elixir
graph =
  Docket.Graph.new!(id: "essay-review", name: "Essay Review")
  |> Docket.Graph.put_input!("topic",
    schema: Docket.Schema.string(),
    required: true
  )
  |> Docket.Graph.put_field!("draft", schema: Docket.Schema.string())
  |> Docket.Graph.put_node!("writer", implementation: Essay.Writer)
  |> Docket.Graph.put_edge!("start-writer", from: "$start", to: "writer")
  |> Docket.Graph.put_edge!("writer-finish", from: "writer", to: "$finish")
```

Incomplete drafts remain valid values. `Docket.Graph.verify/2` is the explicit
gate that attaches compiler diagnostics and decides whether a graph is
publishable/runnable.

## Fields And Outputs

Inputs are initial run data, not executable nodes. State fields hold committed
runtime values and declare a schema, optional default, and optional reducer.
Fields without an explicit reducer use last-value semantics.

Outputs are read-only projections over committed input or state fields. An
output inherits its source schema unless it declares a compatible schema. An
output does not create a writable runtime channel.

## Nodes

`Docket.Graph.Node` is the public graph record. `Docket.Node` is the callback
contract implemented by application modules. `Docket.Runtime.Graph.Node` is the
internal lowered node definition.

A node module exports `config_schema/0` and `call/3`. Publication materializes
configuration defaults into the effective graph. Runtime callbacks receive the
public node ID, committed state snapshot, normalized config, and execution
context; they never receive graph editor projection data.

## Edges, Joins, And Branches

An edge source is `"$start"`, one node ID, or a list of node IDs. Its target is
one node ID or `"$finish"`.

- One source and one target form an ordinary edge.
- Multiple edge records from one source form fan-out.
- A list-form source forms a fan-in barrier.
- A guard controls whether a completed source emits the edge activation.
- A node branch group names related outgoing edge IDs for editing and
  inspection; it does not create a separate runtime node.

Each public edge lowers to a generated activation channel. List-form sources
lower to a barrier channel that tracks source completion. Public branch intent
is preserved in lowering metadata for editors and runtime overlays.

## Verification And Compilation

```elixir
Docket.Graph.verify(graph, opts \\ [])
Docket.Graph.Compiler.verify(graph, opts \\ [])
Docket.Graph.Compiler.compile(graph, opts \\ [])
```

Verification returns the authored graph with fresh diagnostics. Compilation
returns a derived `Docket.Runtime.Graph` on success. Both entry points use the
same validation and lowering pipeline.

Graph verification covers record shape, durable content, schemas, reducers,
node contracts and config, policies, references, guards, topology, cycles,
generated IDs, and lowered graph invariants. Concrete input and node output
values are runtime concerns.

## Durable Facade Example

```elixir
{:ok, graph_ref} = MyApp.Docket.save_graph(graph, tenant_id: tenant_id)

{:ok, run} =
  MyApp.Docket.start_run(
    graph_ref,
    %{"topic" => "compiler design"},
    tenant_id: tenant_id
  )

{:ok, committed_run} = MyApp.Docket.fetch_run(run.id, tenant_id: tenant_id)
```

The exact success value from `start_run` depends on runtime testing mode, but
the durable boundary is unchanged: graph publication and accepted run moments
commit through the backend before they are reported as durable.

## UI Projection Boundary

Canvas position, viewport, selection state, collaboration cursors, and other
editor-only data remain application-owned projection state keyed by public graph
IDs. They do not belong in `Docket.Graph` and do not affect graph identity.

Editors apply semantic changes through `Docket.Graph` functions and run
verification when they need diagnostics. Runtime inspectors fetch the graph by
the run's pinned `GraphRef`, read committed run/event state through the durable
facade, and map runtime IDs back to public IDs through compiler lowering
metadata.

## Ownership Boundary

The application owns:

- authorization and tenant/project relationships
- unpublished draft storage and collaborative editing
- product-specific workflow records
- UI projection and business indexes
- external effects performed by node code

The configured Docket backend owns:

- effective immutable graph versions
- durable run state and scheduling metadata
- retained run events
- claim/checkpoint fencing, recovery, and signals

Docket core owns the graph document schema, editing functions, compiler,
runtime transition semantics, and public durable facade.
