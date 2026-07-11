# Docket Compiler Design

Status: reference design (implemented)
Date: 2026-06-28

Related documents:

- `docs/architecture/docket-graph-construction-design.md`
- `docs/architecture/docket-graph-execution-contract-design.md`

## 1. Purpose

This document defines the compiler contract for Docket v1.

The compiler is the bridge between:

```text
Docket.Graph
  canonical editable public graph document

Docket.Runtime.Graph
  internal executable graph materialization consumed by the Runtime
```

The compiler owns:

- publish/run verification for `Docket.Graph`
- graph-attached compiler diagnostics
- runtime graph lowering
- public-to-runtime ID mapping
- runtime graph self-validation
- the first testable boundary between the builder/design layer and runtime layer

The compiler does not own graph editing, graph storage, run storage, node
execution, checkpoint persistence, UI canvas projection, or runtime process
state.

## 2. Current Repository State

The compiler described by this document is implemented.

- `Docket.Graph` is the canonical editable graph document, and graph editing
  helpers clear stale diagnostics.
- The compiler privately computes SHA-256 identity from the exact versioned
  deterministic ETF bytes of the effective graph.
- `Docket.Graph.verify/2` delegates to `Docket.Graph.Compiler.verify/2`.
- `Docket.Graph.Compiler.verify/2` and `compile/2` run the real validation and
  lowering passes under `lib/docket/graph/compiler/`.
- `Docket.Runtime.Graph`, `Docket.Runtime.Graph.Node`,
  `Docket.Runtime.Graph.Channel`, and `Docket.Runtime.Graph.Lowering` are the
  internal runtime graph structs produced by `compile/2`.
- `Docket.Schema`, `Docket.Reducer`, `Docket.Guard`, and `Docket.Node` exist as
  public value and behavior contracts.

## 3. Design Position

Docket has one public graph model:

```text
Docket.Graph
```

It is used by:

- hand-written Elixir construction
- workflow importers and compatibility compilers
- realtime editors
- host-owned graph persistence
- publish validation
- run startup
- run resume hash checks

Docket has one internal executable graph model:

```text
Docket.Runtime.Graph
```

It is used by:

- `Docket.Runtime.Loop`
- `Docket.Runtime.Algorithm`
- `Docket.Runtime.Dispatcher`
- inline `Docket.Test` execution
- supervised `Docket.Runtime`

The compiler is the only path from the public graph document to the internal
runtime graph.

```text
Docket.Graph
  -> Docket.Graph.Compiler.verify/2
  -> graph with fresh diagnostics

Docket.Graph
  -> Docket.Graph.Compiler.compile/2
  -> Docket.Runtime.Graph
```

The builder/design layer should stay simple: graph editing functions update
data and clear stale diagnostics. The compiler decides whether that data can
run.

## 4. Compiler Goals

1. Preserve `Docket.Graph` as the canonical host-stored graph document.
2. Allow incomplete drafts until explicit verification or compilation.
3. Make verification deterministic and diagnostic-rich.
4. Make `compile/2` the only public path that returns a runtime graph.
5. Keep runtime graph structures internal and derived.
6. Give every runtime node/channel/edge activation a stable mapping back to
   public graph intent.
7. Catch graph shape, schema, implementation, config, guard, and lowering
   problems before a run starts.
8. Keep runtime validation focused on actual run data and node results.
9. Make compile behavior testable without supervisors, databases, network
   services, LLMs, or host app infrastructure.
10. Keep room for future optimizations such as compiled graph caching without
    making caches part of the v1 public contract.

## 5. Non-Goals

1. Do not introduce a second public builder schema.
2. Do not require UI code to construct runtime channels or runtime nodes.
3. Do not persist `Docket.Runtime.Graph` as the canonical graph format.
4. Do not store compile receipts on `Docket.Graph` in v1.
5. Do not make compile mutate a published host graph artifact.
6. Do not allow raw function captures or non-durable terms in graph semantics.
7. Do not make runtime executor/checkpoint/storage adapters part of graph
   compilation.
8. Do not let compile perform node execution or external service calls.

## 6. Public API Contract

Keep the public compiler surface small:

```elixir
Docket.Graph.verify(graph, opts \\ [])
Docket.Graph.Compiler.verify(graph, opts \\ [])
Docket.Graph.Compiler.compile(graph, opts \\ [])
```

Return shapes:

```elixir
verify(graph, opts)
  :: {:ok, Docket.Graph.t()}
   | {:error, Docket.Graph.t()}

compile(graph, opts)
  :: {:ok, Docket.Runtime.Graph.t()}
   | {:error, Docket.Graph.t()}
```

`verify/2` and `compile/2` must share the same validation rules. `compile/2`
runs validation first, and only lowers when blocking diagnostics are absent.

`verify/2` returns the graph document with fresh diagnostics attached.

- On `{:ok, graph}`, diagnostics may contain warnings or info diagnostics.
- On `{:error, graph}`, diagnostics contain at least one `:error` diagnostic.

`compile/2` returns the internal runtime graph on success. It should not return
a public compile report in v1. If implementation needs a richer internal result,
keep it private to the compiler.

## 7. Internal Pipeline

The compiler should run a fixed pipeline. Each phase receives an immutable
compiler context and appends diagnostics or derived facts.

```text
1. Ingest graph
2. Validate public document
3. Validate references and topology
4. Validate node contracts and config
5. Validate guards, schemas, reducers, and outputs
6. Analyze cycles and run-safety policies
7. Lower to runtime graph
8. Validate runtime graph invariants
9. Return verified graph or runtime graph
```

Suggested internal modules:

```text
Docket.Graph.Compiler
  public verify/compile facade

Docket.Graph.Compiler.Context
  private pass state, ID indexes, diagnostics, derived facts

Docket.Graph.Compiler.Validation
  public graph validation passes

Docket.Graph.Compiler.Lowering
  runtime graph materialization

Docket.Graph.Compiler.Diagnostics
  diagnostic builders and path helpers
```

These modules are implementation suggestions, not public API. Keep only
`Docket.Graph.Compiler` public.

## 8. Phase Details

### 8.1 Ingest Graph

Inputs:

- a `Docket.Graph` value
- compiler opts

Responsibilities:

- reject non-graph arguments with a hard `FunctionClauseError` or
  `ArgumentError`
- initialize deterministic pass ordering
- build public ID indexes
- normalize diagnostic handling
- never trust stale `graph.diagnostics`

The compiler should ignore existing diagnostics and produce a fresh diagnostic
list for the returned graph.

### 8.2 Validate Public Document

Validate graph-level shape:

- supported `schema_version`
- binary graph ID
- durable graph content
- map-shaped collections
- valid public ID syntax for every record
- no input/state field ID collision
- output IDs may mirror source field IDs
- node and edge IDs remain separate namespaces

Graph editing helpers already return or raise `Docket.Graph.Error` for
malformed IDs when using the public API. The compiler still validates loaded
graphs because hosts may deserialize old, manually edited, or externally
generated data.

### 8.3 Validate Fields, Schemas, And Reducers

Rules:

- input fields require a valid `Docket.Schema`
- state fields require a valid `Docket.Schema`
- state fields with no reducer lower to `Docket.Reducer.last_value()`
- inputs lower to last-value input channels
- reducer descriptors must be serializable Docket reducer terms
- v1 supports `:last_value` only
- defaults must validate against their field schema when present
- required fields without defaults must be provided at run start

Compile does not validate a concrete run input payload. Runtime start does.

### 8.4 Validate Outputs

Rules:

- every output source must reference an input or state field
- if output schema is omitted, inherit the source field schema
- if output schema is present, it must be compatible with the source field
- output projection IDs are public output IDs, not state field IDs
- output projection cannot write state

Outputs are projections over committed run state. They are not executable nodes.

### 8.5 Validate Nodes

Rules:

- every node in `:publish` and `:run` profiles must have an implementation
- v1 supports `%{type: :module, module: module, function: :call}`
- v1 rejects unsupported implementation types with diagnostics
- module implementations must be loadable
- module implementations must satisfy `Docket.Node` by exporting
  `config_schema/0` and `call/3`
- `config_schema/0` must exist and return a valid `Docket.Schema`
- v1 node callbacks use `call/3`; unsupported function names are compile errors
- node config is validated against `config_schema/0`
- config defaults are applied during lowering, not stored back into
  `Docket.Graph`
- node policies are validated for known keys and durable values

If `config_schema/0` raises, exits, or returns malformed data, compile should
surface a diagnostic on the node rather than crashing the compiler.

Node implementation validation may be policy-sensitive later. For v1, keep the
rules strict and local.

### 8.6 Validate Edges

Rules:

- every edge has a binary ID
- `from` is `"$start"`, a node ID, or a non-empty list of node IDs
- `to` is `"$finish"` or a node ID
- `"$start"` is allowed only as a source
- `"$finish"` is allowed only as a target
- every node source and node target exists
- edge source lists cannot include duplicate sources
- self-loops are allowed, but analyzed as cycles
- every edge guard, if present, is a valid `Docket.Guard`

Start edges seed initial activation after run initialization. Finish edges mark
terminal intent but do not replace normal "no active work" termination.

### 8.7 Validate Branch Groups

Branch groups are node-local editing and inspection metadata over outgoing edge
IDs.

Rules:

- branch group names are unique within the node
- branch group edge IDs exist
- every grouped edge has `from` equal to the branch owner node
- grouped edges should normally have guards; unguarded grouped edges should
  receive a warning in v1, not an error
- an edge can appear in only one branch group for a source node unless a future
  explicit aliasing feature is added

The runtime does not need separate branch channels in v1. Branch groups lower
through ordinary guarded edge activation channels and lowering metadata.

### 8.8 Validate Guards

Guards are durable data expressions.

Rules:

- supported ops: `:changed`, `:version_at_least`, `:path`, `:exists`,
  `:equals`, `:all`, `:any`, `:not`
- guard channel references resolve to input or state fields
- `path/2` starts from a valid field reference
- path segments are strings, atoms, or integers
- `all/1` and `any/1` contain only guard expressions
- guard literals are durable graph values
- guard expressions are side-effect free and deterministic

Compile rejects invalid guard expressions. Runtime guard evaluation errors
should be rare; if they happen, they fail the run as runtime errors.

### 8.9 Validate Topology

Rules:

- graph must have at least one edge from `"$start"` to a node
- all runnable nodes must be reachable from `"$start"`
- all edge targets must be reachable from the start frontier
- nodes with no outgoing edge are allowed and terminate by quiescence
- an explicit edge to `"$finish"` is recommended but not required in v1
- unreachable nodes are errors in `:publish` and `:run` profiles
- disconnected draft work can still be saved by the host because graph editing
  does not require verification

This mirrors LangGraph's "no orphaned nodes" compile stance while preserving
Docket's ability to store incomplete drafts before verification.

### 8.10 Analyze Cycles

Docket supports cycles. The compiler should not reject cycles by default.

Rules:

- detect strongly connected components
- require a global max-supersteps limit from graph policy or runtime default
- warn on cycles with no guarded edge or no apparent halt condition
- allow cycles in v1 when max-supersteps is enforced
- allow a stricter future profile to reject unguarded cycles

Runtime remains the final safety net. If a cycle does not halt, the run fails
with a max-supersteps error rather than running forever.

### 8.11 Lower To Runtime Graph

Lowering turns public records into runtime primitives.

Required runtime channels:

```text
input:<input_id>
state:<field_id>
edge:<edge_id>
```

Required runtime nodes:

```text
Docket.Graph.Node
  -> Docket.Runtime.Graph.Node
```

Required runtime graph content:

- source graph ID
- optional source graph hash
- input channels
- state channels
- generated edge activation channels
- runtime nodes with subscriptions and outgoing edges
- output projections
- policies normalized for runtime
- lowering metadata

Example lowering:

```text
input "topic"
  -> channel "input:topic"

field "draft"
  -> channel "state:draft"

edge "edge_start_writer", from "$start", to "writer"
  -> channel "edge:edge_start_writer"
  -> initial activation seed
  -> writer subscribes to "edge:edge_start_writer"

edge "edge_writer_finish", from "writer", to "$finish"
  -> channel "edge:edge_writer_finish"
  -> writer outgoing edge
  -> finish activation record
```

Multi-source edge lowering:

```text
edge "edge_ready", from ["writer", "tester"], to "reviewer"
  -> channel "edge:edge_ready"
  -> barrier/all source completion tracking
  -> reviewer subscribes to "edge:edge_ready"
```

Guarded edge lowering:

```text
edge "edge_approved", from "reviewer", to "publish", guard: ...
  -> channel "edge:edge_approved"
  -> guard expression attached to runtime edge descriptor
  -> publish subscribes to "edge:edge_approved"
```

The runtime, not user node code, emits activation writes to generated edge
channels after source completion, reducer commit, barrier satisfaction, and
guard approval.

### 8.12 Validate Runtime Graph

After lowering, validate internal invariants:

- runtime IDs are unique
- every subscription points at an existing channel
- every outgoing edge points at an existing edge descriptor/channel
- every runtime node maps back to a public node
- every generated edge channel maps back to a public edge
- output projections reference existing runtime channels
- start activations reference existing target nodes
- finish activations have no node subscription
- no user writable output targets a generated edge channel

If this phase fails, return diagnostics. Runtime graph invariant failures are
compiler bugs or unsupported graph shapes, but they should still surface as
diagnostics when possible.

## 9. Runtime Graph Shape

The exact structs can evolve, but the compiler should target this conceptual
shape:

```elixir
%Docket.Runtime.Graph{
  id: runtime_graph_id,
  graph_id: public_graph_id,
  graph_hash: graph_hash_or_nil,
  channels: %{runtime_channel_id => %Docket.Runtime.Graph.Channel{}},
  nodes: %{runtime_node_id => %Docket.Runtime.Graph.Node{}},
  edges: %{public_edge_id => runtime_edge_descriptor},
  outputs: %{public_output_id => output_projection},
  policies: normalized_policies,
  lowering: %Docket.Runtime.Graph.Lowering{}
}
```

The existing architecture docs already name these internal modules:

```text
Docket.Runtime.Graph
Docket.Runtime.Graph.Node
Docket.Runtime.Graph.Channel
Docket.Runtime.Graph.Lowering
```

`edges` may initially be plain internal descriptors inside
`Docket.Runtime.Graph`; introduce a separate runtime edge struct only if it
removes meaningful complexity.

## 10. Lowering Metadata

Lowering metadata is required, not optional.

It supports:

- diagnostics
- runtime debug views
- live run overlays
- event mapping
- test assertions
- generated channel explainability

Recommended shape:

```elixir
%Docket.Runtime.Graph.Lowering{
  public_to_runtime: %{
    inputs: %{"topic" => "input:topic"},
    fields: %{"draft" => "state:draft"},
    nodes: %{"writer" => "node:writer"},
    edges: %{"edge_start_writer" => "edge:edge_start_writer"},
    outputs: %{"draft" => "output:draft"}
  },
  runtime_to_public: %{
    "input:topic" => {:input, "topic"},
    "state:draft" => {:field, "draft"},
    "node:writer" => {:node, "writer"},
    "edge:edge_start_writer" => {:edge, "edge_start_writer"}
  },
  generated: %{
    "edge:edge_start_writer" => %{
      kind: :activation_channel,
      public_edge_id: "edge_start_writer"
    }
  }
}
```

Runtime node IDs can either be the public node ID or `node:<node_id>`. The
important rule is consistency. If channel IDs are namespaced, node IDs should
also be namespaced internally to avoid ambiguity in debug tooling.

Recommended v1 runtime ID policy:

```text
node:<node_id>
input:<input_id>
state:<field_id>
edge:<edge_id>
output:<output_id>
```

The public node ID remains what node callbacks receive in runtime context.

## 11. Diagnostics And Error Surfacing

Representable graph invalidity should surface as `Docket.Graph.Diagnostic`
values, not exceptions.

Use diagnostics for:

- missing or unknown references
- invalid graph topology
- missing node implementations
- unsupported implementation types
- invalid node config
- invalid schema/reducer/guard descriptors
- unreachable nodes
- generated runtime ID collisions
- lowering invariant failures

Use hard errors for:

- wrong function argument types outside the graph data model
- impossible internal compiler states that cannot be represented safely
- programmer misuse of low-level internal APIs

Diagnostic fields:

```elixir
%Docket.Graph.Diagnostic{
  severity: :error | :warning | :info,
  code: atom(),
  message: String.t(),
  path: [term()],
  public_id: String.t() | nil,
  runtime_id: String.t() | nil,
  metadata: map()
}
```

Path rules:

- Prefer public graph paths.
- Include public IDs when available.
- Include runtime IDs only for lowering/runtime graph invariants.
- Do not expose internal stack traces in the message.
- Store exception class/reason in metadata when useful for debugging.

Example:

```elixir
%Docket.Graph.Diagnostic{
  severity: :error,
  code: :unknown_edge_target,
  message: "edge edge_writer_reviewer targets unknown node reviewer",
  path: [:edges, "edge_writer_reviewer", :to],
  public_id: "edge_writer_reviewer"
}
```

Diagnostic code families (verify names against
`lib/docket/graph/compiler/`):

| Family      | Codes                                                                                                                                                                             |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Graph shape | `:unsupported_schema_version`, `:non_durable_graph_value`                                                                                                                         |
| IDs         | `:invalid_public_id`, `:duplicate_state_id`, `:reserved_id`                                                                                                                       |
| Fields      | `:missing_field_schema`, `:invalid_schema`, `:invalid_reducer`, `:invalid_field_default`                                                                                          |
| Outputs     | `:unknown_output_source`, `:incompatible_output_schema`                                                                                                                           |
| Nodes       | `:missing_node_implementation`, `:invalid_node_implementation`, `:unsupported_node_implementation`, `:node_module_not_loaded`, `:invalid_node_config_schema`, `:invalid_node_config` |
| Edges       | `:unknown_edge_source`, `:unknown_edge_target`, `:empty_edge_sources`, `:duplicate_edge_source`, `:invalid_start_endpoint`, `:invalid_finish_endpoint`                            |
| Branches    | `:unknown_branch_edge`, `:branch_edge_source_mismatch`, `:duplicate_branch_edge`, `:unguarded_branch_edge` (warning)                                                              |
| Guards      | `:invalid_guard`                                                                                                                                                                  |
| Policies    | `:invalid_policy`                                                                                                                                                                 |
| Topology    | `:no_entrypoint`, `:unreachable_node`, `:unbounded_cycle`, `:unguarded_cycle` (warning), `:dead_end_node` (warning), `:no_terminal_edge` (warning)                                |
| Lowering    | `:runtime_id_collision`, `:missing_runtime_channel`, `:lowering_invariant_failed`                                                                                                 |

Run APIs should normalize compiler errors into runtime-facing typed errors once
`Docket.Error` exists:

```elixir
{:error, %Docket.Error{
  type: :graph_compile_failed,
  diagnostics: graph.diagnostics
}}
```

Until `Docket.Error` exists, `Docket.run/4` can return the graph with attached
diagnostics or a temporary typed tuple. The final v1 shape should not make
callers scrape exception messages.

## 12. Compiler And Runtime Boundary

Compile-time validation protects graph shape.

Runtime validation protects actual data.

Compile-time responsibilities:

- graph references
- topology
- node implementation availability
- node config schema and config values
- field/output schemas
- reducer descriptors
- guard descriptors
- generated runtime IDs

Runtime responsibilities:

- input payload validation at run start
- node return shape validation
- state update field validation
- state update value validation
- reducer application and reducer failures
- guard evaluation against committed values
- max-supersteps failure
- checkpoint failure behavior
- retry, timeout, interrupt, await, and node execution errors

This split keeps compile deterministic and side-effect free while still
protecting the runtime from bad data and bad node returns.

## 13. Builder And Design Relationship

The current builder/design is intentionally not a LangGraph-style mutable
builder.

Current Docket flow:

```text
graph =
  Docket.Graph.new!(...)
  |> Docket.Graph.put_input!(...)
  |> Docket.Graph.put_node!(...)
  |> Docket.Graph.put_edge!(...)

case Docket.Graph.verify(graph) do
  {:ok, graph} -> publish graph
  {:error, graph} -> display graph.diagnostics
end
```

The compiler should reinforce that design:

- edit helpers do not validate runnable correctness
- non-bang edit helpers return `{:ok, graph}` or `{:error, reason}`
- bang edit helpers return `graph` or raise `Docket.Graph.Error`
- successful edits clear stale diagnostics
- verify attaches diagnostics to the graph
- compile returns an internal derived runtime graph
- host apps store `Docket.Graph`, not `Docket.Runtime.Graph`
- host UI projection is keyed by public IDs, not generated runtime IDs

Workflow importers and compatibility compilers should compile into
`Docket.Graph`, then call `Docket.Graph.verify/2`. They should not bypass the
compiler by constructing runtime graph internals directly.

## 14. Testing Strategy

Compiler tests are the bridge between graph construction tests and runtime
execution tests. The suite exists under `test/docket/graph/compiler/` and
covers validation diagnostics, policy validation, lowering, lowering metadata,
generated IDs, and determinism; compile-and-run integration is exercised
through the inline runtime tests.

The standing rule remains: no compiler test may need Ecto, a database, Redis,
Docker, network access, LLM credentials, browser automation, or a host app.

## 15. Implementation Sequence

Recommended order:

1. Add runtime graph structs with no execution behavior.
2. Replace compiler stub with a context and diagnostics collector.
3. Implement graph/ID/reference validation.
4. Implement schema, reducer, output, and guard validation.
5. Implement node implementation and config validation.
6. Implement topology validation and cycle analysis.
7. Implement minimal lowering for inputs, fields, outputs, nodes, and simple
   edges.
8. Add runtime graph invariant validation.
9. Add fan-out, finish edges, guarded edges, branch metadata, and fan-in barrier
   lowering.
10. Connect `Docket.Graph.Compiler.compile/2` to `Docket.Test.run_inline/3` once
    the inline runtime exists.
11. Normalize compile failures through `Docket.Error` when public run APIs land.

Each step should have tests before expanding the next lowering feature.

## 16. Open Decisions

These should be resolved during implementation, but they should not block the
initial compiler slice:

1. Runtime node ID format: public node ID versus `node:<node_id>`. This document
   recommends `node:<node_id>` internally while preserving public node IDs in
   node callback context.
2. Finish semantics: v1 should allow quiescence without explicit `$finish`, but
   warn when no explicit finish path exists if that proves useful in product UI.
3. Preview profile: initial compiler can implement only default `:publish`.
   Add `:preview` only when editor UX needs non-blocking severity policy.
4. Runtime edge struct: start with internal descriptors inside
   `Docket.Runtime.Graph`; split into a struct if tests become unclear.
5. Cycle strictness: v1 should allow cycles with a runtime max-supersteps guard
   and warn on obviously unguarded cycles. A stricter future policy can reject
   them.

## 17. Definition Of Done

The compiler design is implemented when:

- `Docket.Graph.verify/2` returns fresh diagnostics from real validation.
- `Docket.Graph.Compiler.compile/2` returns a `Docket.Runtime.Graph` for valid
  baseline fixtures.
- Invalid graphs fail through diagnostics, not exception messages.
- Runtime graph lowering is deterministic.
- Every generated runtime ID maps back to public graph intent.
- Compiler validation, lowering, metadata, and determinism tests are green.
- The first compile-and-run inline test proves:

```text
Docket.Graph
  -> verify
  -> compile
  -> Docket.Test.run_inline/3
  -> Docket.Run/checkpoint assertions
```

At that point the compiler is a real system boundary rather than a stub.
