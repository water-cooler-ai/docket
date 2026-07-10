# Changelog

All notable changes to `docket` are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/).

Each v0.1.0 ticket updates the Unreleased section in its own PR.

## 0.1.0 — Unreleased

The first operational release line: Docket owns the durable graph-run
lifecycle through a self-contained Postgres backend. Work accumulates on the
`v0.1.0` branch. The design source of truth is
`docs/architecture/docket-operational-transition-spec.md` (revision 8) plus
the DCKT-1 issue tree; entries below reflect what has landed so far.

### Added

- `Docket.Backend`: one backend bundle as the public storage substitution
  boundary, supplying compatible transaction, graph, run-aggregate, event,
  and supervision capabilities (DCKT-8, #12).
- Substrate-neutral storage ports (DCKT-8, #12):
  - `Docket.Storage` — the shared backend transaction boundary
    (`transaction/2`);
  - `Docket.Storage.Graphs` — immutable, content-addressed canonical graph
    save/fetch;
  - `Docket.Storage.Runs` — the run-row aggregate: insert/fetch/inspect,
    atomic batched due/expired claims with poison outcomes, token-guarded
    heartbeat/release, mandatory token-and-sequence fenced commit, serialized
    mutation, and poison recovery;
  - `Docket.Storage.Events` — append-only persistence of already-assigned
    events.
- Explicit `:system | :tenantless | {:tenant, id}` scope on every run/event
  storage operation; missing tenant input never implies privileged access
  (DCKT-8, #12).
- In-memory conformance backend exercising the full bundle contract,
  including overlapping-transaction publication (test support) (DCKT-8, #12).
- Postgres substrate scaffold behind optional dependencies: versioned
  migrations (`Docket.Postgres.Migration`, v01), `docket_graph_versions` /
  `docket_runs` / `docket_events` schemas, and `mix docket.gen.migration`
  (DCKT-13, #10).
- `Docket.Event`: metadata-only `:checkpoint_committed` event type and the
  `types/0` helper (DCKT-8, #12).
- `docs/architecture/docket-operational-transition-spec.md` revision 8 and
  the v0.1.0 spec-lock audit (DCKT-32, #13).

### Changed

- One `docket` package: `Docket.Postgres.*` compiles only when the host
  supplies optional `ecto_sql`/`postgrex`; the core keeps no hard Postgres
  dependency (DCKT-7, #8/#9).
- Version bumped to `0.1.0-dev`; release work branches from and merges back
  to `v0.1.0` (DCKT-7, #8).
- Module docs restated as API truth: design rationale moved out of module
  docs and comments into the design docs (DCKT-13, #10).
- `Docket.Node` documentation clarifies the four failure-signaling forms and
  their identical normalization (DCKT-8, #12).

### Removed

- `docket_checkpoints` table and its Ecto schema: `docket_runs.checkpoint_seq`
  is the run fence, recovery reads the run row, and retained events provide
  history. Exactly three operational tables remain (DCKT-28, #11).

## 0.0.1 — 2026-07-08

Initial core runtime line (in-process, storage-free): typed graph
construction and compiler, superstep runtime with checkpoints and interrupts,
host-owned checkpoint committer, reducers, local and task executors,
telemetry, and deterministic test helpers.
