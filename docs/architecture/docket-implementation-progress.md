# Docket Implementation Progress

Status: active progress note
Date: 2026-06-27

This document tracks what has actually landed in code for the v1 MVP. It is not
the design source of truth; use `docket-v1-implementation-path.md` for the
planned build sequence.

## Implemented

### Graph Document Skeleton

The public graph document has been introduced as `Docket.Graph`.

Implemented fields:

- graph identity and descriptive fields
- `schema_version`
- `inputs`
- `fields`
- `outputs`
- `nodes`
- `edges`
- `policies`
- `metadata`
- `diagnostics`

Implemented public graph record structs:

- `Docket.Graph.Node`
- `Docket.Graph.Edge`
- `Docket.Graph.Field`
- `Docket.Graph.Output`
- `Docket.Graph.Diagnostic`
- `Docket.Graph.Error`

### Graph Editing API

The current public editing API uses explicit `put_*`, `update_*`, and
`delete_*` functions. The earlier shorthand `node/edge/field/input/output`
style is not implemented.

Implemented functions:

- `Docket.Graph.new/1`
- `Docket.Graph.new!/1`
- `Docket.Graph.put_input/4`
- `Docket.Graph.put_input!/4`
- `Docket.Graph.put_field/4`
- `Docket.Graph.put_field!/4`
- `Docket.Graph.put_output/4`
- `Docket.Graph.put_output!/4`
- `Docket.Graph.put_node/4`
- `Docket.Graph.put_node!/4`
- `Docket.Graph.put_edge/4`
- `Docket.Graph.put_edge!/4`
- `Docket.Graph.update_node/4`
- `Docket.Graph.update_node!/4`
- `Docket.Graph.update_edge/4`
- `Docket.Graph.update_edge!/4`
- `Docket.Graph.update_field/4`
- `Docket.Graph.update_field!/4`
- `Docket.Graph.delete_node/3`
- `Docket.Graph.delete_node!/3`
- `Docket.Graph.delete_edge/3`
- `Docket.Graph.delete_edge!/3`
- `Docket.Graph.delete_field/3`
- `Docket.Graph.delete_field!/3`
- `Docket.Graph.policy/4`
- `Docket.Graph.policy!/4`
- `Docket.Graph.metadata/4`
- `Docket.Graph.metadata!/4`
- `Docket.Graph.diagnostics/2`
- `Docket.Graph.to_map/2`
- `Docket.Graph.from_map/2`
- `Docket.Graph.from_map!/2`
- `Docket.Graph.hash/2`
- `Docket.Graph.verify/2`

Non-bang graph edit functions return `{:ok, graph}` or
`{:error, %Docket.Graph.Error{}}`. Bang edit functions return the graph or raise
`Docket.Graph.Error`. Edits clear stale diagnostics, but they do not compile or
diagnose the graph. Verification is explicit through `Docket.Graph.verify/2`.

### Graph Serialization And Hashing

`Docket.Graph.to_map/2` and `Docket.Graph.from_map/2` are the only public
entry/exit points for serialized graph documents (the v1 JSON-safe wire
format, implemented internally by `Docket.Graph.Serializer`).
`Docket.Graph.hash/2` is a SHA-256 digest over the canonical JSON encoding of
`to_map/1`, so hashes survive host storage round trips and library upgrades.

Graphs are free-form in memory, Ecto-style: editing helpers store content
exactly as given and perform no durability validation (the one edit-time
rewrite is the module / `{module, function}` implementation construction
shorthand). Canonicalization happens at the serialization boundary:
`to_map/2` coerces open content the way a JSON encoder would (atom keys and
values become strings, silently) and rejects terms with no JSON
representation (tuples, keyword lists, pids, refs, functions, structs) with
`:non_durable_value`. Hashes are computed from the canonical document, so
`hash(from_map!(to_map(graph))) == hash(graph)` holds for every dumpable
graph; graphs whose open content is already canonical also satisfy
`from_map!(to_map(graph)) == graph` on struct equality. Guards nested in
plain guard-argument positions are wrapped in a reserved `"$guard"` wire tag;
`$`-prefixed map keys are reserved in durable content. `from_map/2` validates
documents strictly (schema version, unknown keys, enum-like values,
implementation atoms via `String.to_existing_atom/1`) and never creates
atoms.

### Compiler Boundary

The compiler boundary exists but is not implemented beyond the public function
shape.

Implemented functions:

- `Docket.Graph.Compiler.verify/2`
- `Docket.Graph.Compiler.compile/2`

Current behavior:

- `verify/2` returns `{:error, graph_with_diagnostics}` with a placeholder
  compiler diagnostic.
- `compile/2` returns `{:error, graph_with_diagnostics}`.
- There is no compiler report struct.
- There is no `explain/2` function.

### Node Contracts

The executable node behavior has been introduced.

Implemented modules:

- `Docket.Node`

Implemented callbacks:

- `config_schema/0`
- `call/3`

### Public Value Constructors

The initial public value constructors used by graph records and node contracts
exist.

Implemented modules:

- `Docket.Schema`
- `Docket.Reducer`
- `Docket.Guard`

Implemented schema constructors:

- `Docket.Schema.string/1`
- `Docket.Schema.float/1`
- `Docket.Schema.map/1`
- `Docket.Schema.object/2`
- `Docket.Schema.enum/2`

Implemented reducer constructors:

- `Docket.Reducer.last_value/1`

Implemented guard constructors:

- `Docket.Guard.changed/1`
- `Docket.Guard.version_at_least/2`
- `Docket.Guard.path/2`
- `Docket.Guard.exists/1`
- `Docket.Guard.equals/2`
- `Docket.Guard.all/1`
- `Docket.Guard.any/1`
- `Docket.Guard.not/1`

### Tests

Initial graph construction tests exist in `test/docket/graph_test.exs`.

Current coverage checks:

- graph construction with public structs
- multi-source edge joins and node-local branch metadata
- kind-scoped public IDs for fields/outputs and nodes/edges
- realtime-style put/update/delete editing
- guard/schema/reducer value construction
- public ID argument errors
- to_map/from_map round-trip law on a rich graph
- free-form in-memory content with atom-to-string coercion at to_map and
  rejection of non-representable terms
- nested guard `$guard` wire tagging and reserved `$` key rejection
- schema no-default sentinel vs explicit-nil default round trip
- strict from_map document validation (versions, unknown keys, unknown
  enum-like values, unknown implementation modules)
- SHA-256 graph hashing over the canonical JSON encoding of the wire
  document, stable across to_map/from_map round trips
- graph verification attaching compiler diagnostics
- edit helpers clearing stale diagnostics

## Not Yet Implemented

- real compiler validation
- runtime graph lowering
- schema compatibility checks
- node config validation
- reachability checks
- cycle and max-step validation
- multi-source edge barrier lowering
- node-local branch group validation and lowering
- `Docket.Run`
- checkpoint contracts
- runtime loop
- inline test runtime
- public run/resume APIs
- supervised runtime process tree

## Current Test Status

Latest local check:

```text
mix test
21 tests, 0 failures
```
