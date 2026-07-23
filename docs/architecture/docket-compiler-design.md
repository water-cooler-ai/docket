# Docket Compiler Design

Status: implemented reference
Date: 2026-06-28

Related documents:

- [Graph construction](docket-graph-construction-design.md)
- [Reducer design](docket-reducers-design.md)

## Boundary

`Docket.Graph` is the canonical editable graph document.
`Docket.Runtime.Graph` is the internal executable materialization. The compiler
is the only supported path between them.

The compiler owns:

- canonicalization and deterministic graph identity
- publish/run validation and graph-attached diagnostics
- node configuration-schema materialization
- public-to-runtime lowering
- lowered graph invariant checks

Graph editing, durable storage, node execution, run transitions, authorization,
and UI projection remain outside the compiler.

## Public API

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

`verify/2` and `compile/2` run the same pipeline, including lowering and
lowered-result validation when public validation has no blocking diagnostic.
`verify/2` returns the authored graph with a fresh diagnostic list. `compile/2`
returns the runtime graph on success.

Supported options:

- `:profile` accepts `:publish` or `:run`; both use the same rules in v0.1.
- `:max_supersteps` supplies the runtime cycle bound when the graph does not
  declare a `"max_supersteps"` policy.

Representable graph invalidity returns diagnostics. Wrong argument types and
invalid compiler options raise because they are caller errors outside the graph
data model.

## Canonical Identity And Publication

Compiler ingest removes transient diagnostics, normalizes the graph through the
private durable codec, and validates the exact versioned deterministic ETF
bytes. The SHA-256 digest of the effective bytes is the graph hash.

Publication snapshots each node module's `config_schema/0` once, materializes
configuration defaults into an effective graph document, hashes that document,
and stores it through the configured backend. Execution fetches the exact
effective graph pinned by `Docket.GraphRef` and compiles it without reapplying
publication defaults.

This separation makes hashes independent of transient diagnostics and prevents
changes in module-provided defaults from changing an already-published graph.

## Pipeline

The implemented pipeline is:

1. Canonicalize the authored or effective graph and validate durable encoding.
2. Fetch node configuration schemas once for the compile.
3. Materialize publication defaults when compiling an authored graph for
   publication.
4. Validate graph records, references, topology, node contracts, policies,
   guards, schemas, reducers, outputs, and cycles.
5. Lower the effective graph to `Docket.Runtime.Graph`.
6. Validate runtime IDs, subscriptions, generated channels, output projections,
   and lowering maps.

The implementation is split across:

- `Docket.Graph.Compiler.Canonical`
- `Docket.Graph.Compiler.NodeContracts`
- `Docket.Graph.Compiler.Validation`
- `Docket.Graph.Compiler.Lowering`
- `Docket.Graph.Compiler.RuntimeValidation`
- `Docket.Graph.Compiler.Diagnostics`

These modules are internal. `Docket.Graph.Compiler` is the public compiler
facade.

## Validation Contract

### Graph And IDs

- `schema_version` must be supported.
- Graph, field, output, node, and edge IDs use the public binary ID syntax.
- `"$start"` and `"$finish"` are reserved edge endpoints.
- Inputs and state fields share a namespace; outputs may mirror a source ID.
- Graph semantic content must be encodable by the durable codec.

Editing helpers reject malformed operations early, but compile validates the
whole loaded document because graphs may come from durable storage, imports, or
manual construction.

### Fields, Outputs, And Reducers

- Inputs and state fields require valid `Docket.Schema` values.
- A state field without a reducer uses `Docket.Reducer.last_value()`.
- Supported reducer types are `:last_value`, `:first_value`, `:append`,
  `:merge`, `:sum`, and `:union`.
- Reducer/schema pairings and reducer options are validated.
- Defaults must satisfy the field schema. Accumulating reducers receive their
  natural zero when no explicit default is present.
- Outputs project a declared input or state field and inherit its schema unless
  a compatible schema is supplied.

Concrete run input and node writes are runtime data, so their values are
validated by the runtime rather than by graph compilation.

### Nodes

- Every runnable node has a module implementation using `call/3`.
- The module must export `config_schema/0` and `call/3`.
- `config_schema/0` must return a valid `Docket.Schema`.
- Configuration is validated against that schema.
- Node policies validate the implemented `"timeout_ms"` and `"retry"` shapes.
  `"on_error"` remains reserved and is rejected.

Exceptions, exits, and malformed returns from `config_schema/0` become
node-scoped diagnostics rather than compiler crashes.

### Edges, Guards, And Topology

- An edge source is `"$start"`, a node ID, or a non-empty unique list of node
  IDs. Its target is a node ID or `"$finish"`.
- Multi-source edges use barrier semantics.
- Branch groups are node-local metadata over outgoing edge IDs.
- Guards are durable `Docket.Guard` expressions with resolvable field
  references.
- Runnable nodes must be reachable from `"$start"`.
- Quiescence is a valid terminal condition; an explicit `"$finish"` edge is not
  required.
- Cycles are allowed when an effective max-supersteps bound exists. Unguarded
  cycles produce a warning.

## Lowering Contract

Runtime IDs are deterministic and namespaced:

```text
node:<node_id>
input:<input_id>
state:<field_id>
edge:<edge_id>
output:<output_id>
```

Inputs and fields lower to last-value runtime channels. The field's reducer
controls how committed writes update its value. Each edge lowers to an
activation channel; list-form sources use a barrier channel. Runtime nodes carry
subscriptions, outgoing public edge IDs, normalized config, and policies.

`Docket.Runtime.Graph.Lowering` records both directions of the public/runtime ID
mapping plus generated activation-channel and branch metadata. Runtime events,
debug views, and tests use this mapping to refer back to public graph intent.

Runtime edge descriptors remain plain internal maps. They contain the public
edge ID, generated channel ID, normalized source list, target, guard, and barrier
flag.

## Diagnostics And Runtime Errors

Each compiler call replaces stale diagnostics. A successful verification may
contain warnings; a failed verification contains at least one error.

Diagnostics use public graph paths and IDs whenever possible. Runtime IDs appear
only for lowering invariant failures. Internal exceptions are not exposed as
diagnostic messages.

The durable facade normalizes a compiler failure to
`%Docket.Error{type: :invalid_graph}` and stores the diagnostic list in
`error.details.diagnostics`. Backend execution vehicles treat an incompatible
stored graph as an abandon/poison condition according to their configured
budget; they do not bypass compiler validation.

## Compiler/Runtime Split

Compile-time validation protects static graph shape and implementation
contracts. Runtime validation protects input values, node results, reducer
application, guard evaluation, retry/timeout behavior, interrupts, and the
max-supersteps limit.

The split keeps compilation deterministic and side-effect free apart from the
single configuration-schema read per node module. Node execution, storage,
network access, and external services are never part of compilation.

Compiled graphs are derived values rather than durable public documents. The
PostgreSQL vehicle cache may reuse a compiled graph under its local generation,
but cache entries are invalidated independently of the graph's durable content
identity.

## Verification

Compiler tests cover diagnostics, policies, lowering, generated IDs, lowering
metadata, durable canonicalization, and determinism under
`test/docket/graph/compiler/`. Compile-and-run behavior is exercised through
the processless `Docket.Test` runtime. Compiler tests require no backend,
database, network service, or supervisor.
