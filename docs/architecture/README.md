# Docket Architecture Docs

Status: active index
Date: 2026-06-26

## Start Here

Docket v1 has two primary flows:

1. Build a graph document.
2. Run a graph document.

Use `docket-v1-implementation-path.md` as the active build guide. The older
design documents remain useful reference notes, but they are no longer the
first thing to read when deciding what to implement next.

## Reading Order

1. `docs/architecture/docket-v1-implementation-path.md`
   - The lean v1 spine.
   - Defines the build path, run path, implementation slices, and test gates.
2. `docs/architecture/docket-implementation-progress.md`
   - Tracks what has actually landed in code for the MVP so far.
3. `docs/architecture/docket-compiler-design.md`
   - Compiler verification, diagnostics, runtime graph lowering, LangGraph
     audit notes, and compiler test strategy.
4. `docs/architecture/docket-graph-construction-design.md`
   - Public `Docket.Graph` document, graph editing API, compiler boundary, ID
     rules, lowering rules, and host storage boundary.
5. `docs/architecture/docket-graph-execution-contract-design.md`
   - Runtime, execution loop, public run APIs, checkpoints, executors, guards,
     failures, interrupts, and inline test runtime contract.
6. `docs/architecture/docket-v1-test-suite-design.md`
   - Test layers, fixtures, in-memory/ETS checkpoint helpers, and v1 coverage
     progression.
7. `docs/architecture/docket-runtime-design.md`
   - Longer research and architecture background. Use it for rationale and
     future design space, not as the day-to-day implementation checklist.

## Canonical v1 Shape

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

When a v1 implementation decision changes:

- Update `docket-v1-implementation-path.md` if it changes the build sequence,
  scope, or active contract.
- Update the focused reference doc if it changes low-level API or runtime
  details.
- Keep this index small. It should route people, not repeat the whole design.
