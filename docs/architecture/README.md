# Docket Architecture Docs

These documents record the design rationale behind Docket. The code and its
module docs are the authoritative reference for the current API; read these
when you want to understand *why* the contracts are shaped the way they are.

## Reading Order

1. `docket-operational-transition-spec.md`
   - The transition from the current `0.0.x` core runtime package to the
     `0.1.0` operational runtime with the `Docket.Postgres` backend —
     Oban-like in shape, one package, self-contained on optional Ecto and
     Postgres dependencies.
2. `docket-graph-construction-design.md`
   - The public `Docket.Graph` document: editing API, serialization and hash
     contract, ID rules, and the host storage boundary.
3. `docket-compiler-design.md`
   - Compiler verification, diagnostics, and lowering from the public graph
     to the internal runtime graph.
4. `docket-graph-execution-contract-design.md`
   - The execution contract: runtime loop, public run APIs, checkpoints,
     executors, guards, failures, and interrupts.
5. `docket-reducers-design.md`
   - Why the v1.1 reducer contract folds the prior committed value, and the
     rationale behind list-write concatenation, natural zeros, and
     reducer-aware write validation.
6. `docket-runtime-design.md`
   - Long-form research and background: goals, alternatives considered
     (Pregel, LangGraph, Temporal), and future design space.

## Current Core Shape

Durable public documents:

- `Docket.Graph`: the graph definition document that host applications build,
  edit, verify, publish, store, and later pass to Docket. Public topology is
  represented by edge records; fan-in joins are multi-source edges, and branch
  groups live on source nodes.
- `Docket.Run`: the durable execution state document that Docket emits through
  checkpoints and host applications persist for reads, resume, audit, and
  recovery.

Derived internal runtime values:

- `Docket.Runtime.Graph`: compiled executable form of a `Docket.Graph`.
- `Docket.Runtime.Loop`: processless transition functions over
  `Docket.Runtime.Graph` and `Docket.Run`, shared by the supervised Runtime and
  inline tests.

Host-owned surfaces:

- Graph and run persistence.
- Graph versioning and publish workflow.
- Authorization and tenant/project ownership.
- UI projections for editors and live run overlays.
- External effects performed by node code or adapters.

## Documentation Rule

When an implementation decision changes a contract, update the focused design
doc if the rationale changed; keep API details in module docs, not here. Keep
this index small: it should route people, not repeat the design.
