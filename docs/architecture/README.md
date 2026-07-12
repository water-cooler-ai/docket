# Docket Architecture Docs

Operational instrumentation and correctness boundaries are documented in the
[telemetry guide](../telemetry.md).
Production status, configuration, failure recovery, and tested SQL are in the
[PostgreSQL operations guide](../postgres-operations.md).

These documents record the design rationale behind Docket. The code and its
module docs are the authoritative reference for the current API; read these
when you want to understand *why* the contracts are shaped the way they are.

## Reading Order

1. `migration-0.0.1-to-0.1.0.md`
   - Drain-and-cut-over instructions for existing 0.0.1 adopters.
2. `docket-operational-transition-spec.md`
   - A public-facing guide to the implemented PostgreSQL data model, queue
     semantics, claim fencing, tenancy, migrations, and backend operations.
3. `docket-v0.1.0-spec-lock-audit.md`
   - A historical pre-cutover audit retained to explain the final sequencing.
4. `docket-graph-construction-design.md`
   - The public `Docket.Graph` editing API, ID rules, publication boundary,
     and private effective-graph identity contract.
5. `docket-compiler-design.md`
   - Compiler verification, diagnostics, and lowering from the public graph
     to the internal runtime graph.
6. `docket-graph-execution-contract-design.md`
   - Historical 0.0.1 execution contract: the removed resident runtime and
     host-checkpoint APIs, plus still-relevant executor, guard, failure, and
     interrupt rationale.
7. `docket-reducers-design.md`
   - Why the v1.1 reducer contract folds the prior committed value, and the
     rationale behind list-write concatenation, natural zeros, and
     reducer-aware write validation.
8. `docket-runtime-design.md`
   - Historical 0.0.1 runtime research and background: goals, alternatives
     considered (Pregel, LangGraph, Temporal), and future design space.

## Release-Line Boundary

The graph programming model is continuous across the release lines: node
modules, graphs, schemas, reducers, interrupts, executors, run serialization,
and `Docket.Test` helpers carry forward. The lifecycle owner changes:

- `0.0.1`: the host checkpoint callback persisted runs and the host explicitly
  resumed resident per-run processes.
- `0.1.0`: one required backend owns persistence, scheduling, recovery, and
  signals. The old supervised `run` / `resume` / `get_run` path is absent.

The older graph-construction and execution-contract documents below record the
`0.0.1` host-owned boundary and are superseded for v0.1.0 production guidance.

## Current Core Shape

Durable public documents:

- `Docket.Graph`: the graph definition document that host applications build,
  edit, and verify, and that the backend publishes as an immutable effective
  version before starting a run. Public topology is
  represented by edge records; fan-in joins are multi-source edges, and branch
  groups live on source nodes.
- `Docket.Run`: the durable execution state document encoded by the backend's
  run store and returned through committed reads.

Derived internal runtime values:

- `Docket.Runtime.Graph`: compiled executable form of a `Docket.Graph`.
- `Docket.Runtime.Loop`: processless transition functions over
  `Docket.Runtime.Graph` and `Docket.Run`, shared by backend vehicles and
  inline tests.

Application-owned surfaces:

- Authorization and tenant/project ownership.
- UI projections for editors and live run overlays.
- External effects performed by node code or adapters.

## Documentation Rule

When an implementation decision changes a contract, update the focused design
doc if the rationale changed; keep API details in module docs, not here. Keep
this index small: it should route people, not repeat the design.
