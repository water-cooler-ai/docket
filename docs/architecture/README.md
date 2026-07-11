# Docket Architecture Docs

These documents record the design rationale behind Docket. The code and its
module docs are the authoritative reference for the current API; read these
when you want to understand *why* the contracts are shaped the way they are.

## Reading Order

1. `docket-operational-transition-spec.md`
   - The transition from the current `0.0.x` core runtime package to the
     `0.1.0` operational runtime with the `Docket.Postgres` backend —
     Oban-like in shape, one package, self-contained on optional Ecto and
     Postgres dependencies. v0.1.0 has one backend-owned production lifecycle;
     the v0.0.1 host-owned supervised driver is migration history only.
2. `docket-v0.1.0-spec-lock-audit.md`
   - The final DCKT-1 architecture, pluggability, lifecycle-status, ticket,
     and dependency audit that produced transition-spec revision 8.
3. `docket-graph-construction-design.md`
   - The public `Docket.Graph` editing API, ID rules, publication boundary,
     and private effective-graph identity contract.
4. `docket-compiler-design.md`
   - Compiler verification, diagnostics, and lowering from the public graph
     to the internal runtime graph.
5. `docket-graph-execution-contract-design.md`
   - The execution contract: runtime loop, public run APIs, checkpoints,
     executors, guards, failures, and interrupts.
6. `docket-reducers-design.md`
   - Why the v1.1 reducer contract folds the prior committed value, and the
     rationale behind list-write concatenation, natural zeros, and
     reducer-aware write validation.
7. `docket-runtime-design.md`
   - Long-form research and background: goals, alternatives considered
     (Pregel, LangGraph, Temporal), and future design space.

## Release-Line Boundary

The graph programming model is continuous across the release lines: node
modules, graphs, schemas, reducers, interrupts, executors, run serialization,
and `Docket.Test` helpers carry forward. The lifecycle owner changes:

- `0.0.1`: the host checkpoint callback persists runs and the host explicitly
  resumes resident per-run processes.
- `0.1.0`: a required `Docket.Backend` owns graph/run persistence, scheduling,
  recovery, and signals. The old supervised `run` / `resume` / `get_run` path
  is removed by DCKT-37 after the operational replacement is complete.

The older graph-construction and execution-contract documents below record the
`0.0.1` host-owned boundary. Where they conflict with the transition spec and
its post-lock amendments, the transition spec governs v0.1.0.

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
