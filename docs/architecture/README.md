# Docket Architecture Docs

Operational instrumentation and correctness boundaries are documented in the
[telemetry guide](../telemetry.md).
Production status, configuration, failure recovery, and inspection are in the
[PostgreSQL operations guide](../postgres-operations.md).

Module docs are authoritative for the current API. Architecture documents
capture cross-module contracts and the rationale that is not useful at an
individual function boundary.

## Current guides and contracts

- [Delivery guarantees](../delivery-guarantees.md) — durable transactions,
  replay, external effects, event export, and best-effort callbacks.
- [PostgreSQL operations](../postgres-operations.md) — production setup,
  configuration, migration, recovery, and inspection.
- [TenantFair claim policy](docket-tenant-fair.md) — state model, sticky
  admission, FIFO, bounded ring traversal, formal fair-rotation contract,
  release evidence, rollout, and nonclaims.
- [ClaimPolicy boundary](docket-claim-policy.md) — the one-statement internal
  seam and Legacy/TenantFair selection and interlock.
- [0.0.1 to 0.1.0 migration](migration-0.0.1-to-0.1.0.md) — drain-and-cut-over
  instructions for old host-owned persistence adopters.

Graph and compiler internals are documented in
[graph construction](docket-graph-construction-design.md),
[compiler design](docket-compiler-design.md), and
[reducers](docket-reducers-design.md).

## Future planning

- [Future roadmap](../future-roadmap.md) — the general project-wide home for
  future features, improvements, investigations, and research across every
  Docket area.
- [v0.1.1 roadmap](../roadmap-v0.1.1.md) — version-focused composability,
  ergonomics, and runtime follow-up themes.

## Historical and research material

- [v0.1.0 spec-lock audit](docket-v0.1.0-spec-lock-audit.md) is the historical
  pre-cutover sequencing audit.
- [Historical graph execution contract](docket-graph-execution-contract-design.md)
  records the 0.0.1 resident-runtime boundary and the execution semantics that
  carried forward.
- [Runtime rationale](docket-runtime-design.md) summarizes the current runtime
  shape, its research influences, and the retired 0.0.1 process boundary.

## Release-Line Boundary

The graph programming model is continuous across the release lines: node
modules, graphs, schemas, reducers, interrupts, executors, and `Docket.Test`
helpers carry forward. The lifecycle owner changes:

- `0.0.1`: the host checkpoint callback persisted runs and the host explicitly
  resumed resident per-run processes.
- `0.1.0`: one required backend owns persistence, scheduling, recovery, and
  signals. The old supervised `run` / `resume` / `get_run` path is absent.

The historical execution contract is not production guidance. The runtime
rationale labels the retired 0.0.1 boundary separately from its current
architecture summary. Graph construction and compiler design are current.

## Current Core Shape

Durable public documents:

- `Docket.Graph`: the graph definition document that host applications build,
  edit, and verify, and that the backend publishes as an immutable effective
  version before starting a run. Public topology is
  represented by edge records; fan-in joins are multi-source edges, and branch
  groups live on source nodes.
- `Docket.Run`: the durable execution state document encoded by the backend's
  run store and returned through committed reads.

Public read projection:

- `Docket.GraphVersion` and `Docket.GraphVersionPage`: lightweight,
  tenant-scoped metadata for retained graph versions and their stable
  newest-first keyset page. Exact documents are read separately with a
  `Docket.GraphRef`.
- `Docket.RunSummary` and `Docket.RunPage`: lightweight, tenant-scoped run
  collection rows and their stable newest-first keyset page.
- `Docket.EventPage`: a keyset page of a run's retained events, returned by the
  tenant-scoped `Docket.list_events/3` reader alongside the retention bounds and
  the run's latest committed event sequence from one snapshot.

Derived internal runtime values:

- `Docket.Runtime.Graph`: compiled executable form of a `Docket.Graph`.
- `Docket.Runtime.Loop`: processless transition functions over
  `Docket.Runtime.Graph` and `Docket.Run`, shared by backend vehicles and
  inline tests.

Application-owned surfaces:

- Authorization and tenant/project ownership.
- UI projections for editors and live run overlays.
- External effects performed by node code or adapters.
