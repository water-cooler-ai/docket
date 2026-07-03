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
- `Docket.Graph.hash/2`
- `Docket.Graph.verify/2`

Non-bang graph edit functions return `{:ok, graph}` or
`{:error, %Docket.Graph.Error{}}`. Bang edit functions return the graph or raise
`Docket.Graph.Error`. Edits clear stale diagnostics, but they do not compile or
diagnose the graph. Verification is explicit through `Docket.Graph.verify/2`.

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
- SHA-256 graph hashing over canonical graph content
- graph verification attaching compiler diagnostics
- edit helpers clearing stale diagnostics

### Compiler (Attempt 1)

The compiler slice from `docket-compiler-design.md` is implemented; decisions
specific to this attempt are recorded in `docket-compiler-v1-attempt-1.md`.

- `Docket.Graph.Compiler.verify/2` and `compile/2` run the real pipeline:
  document, field/schema/reducer, output, node, edge, branch, guard,
  topology, and cycle validation, then lowering and runtime graph
  self-validation. Both share the same rules; diagnostics are always fresh.
- `Docket.Runtime.Graph`, `Docket.Runtime.Graph.Node`,
  `Docket.Runtime.Graph.Channel`, and `Docket.Runtime.Graph.Lowering` exist
  as derived internal structs with no execution behavior.
- Lowering generates `input:`/`state:`/`edge:` channels, `node:` runtime
  nodes with subscriptions and outgoing edges, `output:` projections,
  barrier channels for multi-source edges, guard descriptors on runtime
  edges, and required lowering metadata in both directions. Branch groups
  lower to metadata only.
- `Docket.Schema.validate/2` provides the minimal v1 validation engine used
  for field defaults and node config.
- Compilation is deterministic; compiling the same graph twice yields
  identical runtime graphs regardless of map insertion order.
- Test support landed under `test/support`: node fixtures, graph fixtures
  (`minimal_linear/0` through `cycle_counter/0` plus invalid variants), and
  a `Docket.Test.Case` with `assert_diagnostic/3` helpers.

## Not Yet Implemented

- `Docket.Run`
- checkpoint contracts
- runtime loop
- inline test runtime (`Docket.Test.run_inline/3`)
- compile-and-run integration tests (blocked on the inline runtime)
- public run/resume APIs
- supervised runtime process tree

## Current Test Status

Latest local check:

```text
mix test
119 tests, 0 failures
```
