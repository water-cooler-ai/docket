# Historical Docket v0.1.0 Pre-Cutover Audit

Date: 2026-07-11

Status: historical pre-cutover snapshot. The released v0.1.0 lifecycle and
deterministic modes supersede its lifecycle and testing-gap findings. See the
module docs, PostgreSQL guide, and migration guide for the current production
boundary.

This audit compared a pre-cutover `v0.1.0` branch with the architecture
previously described by the operational transition spec. Its tables record
repository state on 2026-07-11 and are intentionally not current. The code and
module documentation remain authoritative.

## Verdict

The durable data model, transaction boundaries, and public PostgreSQL bundle
are implemented and closely match the locked architecture. `Docket.Postgres`
assembles the dispatcher, vehicles, notifier, and pruner into one supervised
configuration while leaving Repo ownership with the host.

The PostgreSQL guide documents the landed design and keeps the still-present
legacy lifecycle and downstream release work distinct from this operational
backend boundary.

## State by area

> **Historical snapshot:** every state in this table is the state observed
> before the final v0.1.0 testing and lifecycle changes landed. “Missing” and “Not done” below are not
> claims about the released v0.1.0 code.

| Area | State in pre-cutover snapshot | Evidence |
| --- | --- | --- |
| Backend contract | Implemented | `Docket.Backend` bundles storage, graph, run, event, context, and supervision capabilities. |
| Public PostgreSQL bundle | Implemented | `Docket.Postgres` fixes the storage capabilities and owns their operational supervision. |
| Versioned migration | Implemented | `Docket.Postgres.Migration` and schema version 1 create three tables with constraints and indexes. |
| Graph store | Implemented | Immutable effective graph save/fetch with content-address conflict checks. |
| Run aggregate store | Implemented | Scoped reads, insert, mutation, bounded claims, fencing, release, claim refresh, and poison retry. |
| Event store | Implemented | Assigned event append in the lifecycle transaction. |
| Lifecycle composer | Implemented | `Docket.Lifecycle` owns start, moment commit, and signal transaction recipes. |
| Durable facade | Implemented against a backend | Publication, start, reads, signals, poison retry, and bounded await are exercised with the conformance backend. |
| Dispatcher | Implemented | Demand-bounded, jittered polling and lease launch/release behavior exist. |
| Execution vehicle | Implemented | `Docket.Postgres.Vehicle` fetches and compiles the graph for a lease (with an optional generation-checked cache), validates it against the host attempt maximum, and drains fenced moments. Runtime-owned finite deadlines bound executor callbacks; orphan TTL and fencing recover crashed or stolen claims. |
| Backend supervision assembly | Implemented | A one-for-all execution subtree couples dispatcher accounting to its vehicle supervisor; notifier and pruner are isolated siblings. |
| Deterministic backend test mode | Missing at snapshot; implemented for v0.1.0 | The snapshot had no public PostgreSQL drain/manual testing API. |
| Pruning/retention | Implemented | `Docket.Postgres.Pruner` performs locked, bounded cleanup under the bundle's required explicit policy. |
| Legacy production API removal | Not done at snapshot; completed for v0.1.0 | The snapshot still exposed `run`, `resume`, `get_run`, and `checkpoint:`. |

## Decisions verified in code

### One backend is the substitution boundary

`Docket.Runtime.Supervisor` accepts `backend:` and rejects independently
configured transaction or store modules. It resolves one backend context and
adds the backend's child specification to the runtime tree.

This preserves atomicity: graph, run, and event capabilities must understand
the same transaction context. The store behaviours remain independently
testable without pretending arbitrary stores can be safely mixed.

### A run row is one durable aggregate

`Docket.Storage.Runs` owns every operation that changes the scheduling or
commit-authority tuple. PostgreSQL may split schemas and codecs internally,
but claim and graph-state fencing update the same row.

The version 1 migration enforces the five durable statuses and the valid
status/schedule/claim/poison shapes with database constraints. Separate partial
indexes serve ready, expired-claim, and poison inspection paths.

### Runtime transitions are pre-commit values

`Docket.Runtime.Moment` represents one proposed commit boundary. The lifecycle
composer persists the run and events, then creates checkpoint notifications
only after success. Retry parking remains `running`, preserves active task
state, and schedules a future wake without sleeping in the durable path.

### Graph identity is the effective document

Publication snapshots node schema defaults and hashes the canonical effective
graph. Durable identity is `{graph_id, graph_hash}`. Starting or recovering a
run compiles the retained effective document locally without injecting defaults
from newer node code. Compiled runtime graphs aren't stored.

### Scope is explicit

All run and event operations require `:system`, `:tenantless`, or
`{:tenant, tenant_id}`. Public tenant modes resolve to one of those scopes
before a storage call, preventing an omitted tenant from becoming privileged
access.

## Important differences from the proposed release

### The backend is configured as one bundle

```elixir
use Docket,
  repo: MyApp.Repo,
  backend: Docket.Postgres,
  pruner: [
    interval_ms: :timer.hours(1),
    event_retention_ms: :timer.hours(24 * 30),
    run_retention_ms: :timer.hours(24 * 90),
    batch_size: 1_000
  ]
```

The bundle validates Repo/prefix, tenant mode, operational policies, observer
modules, notifier mode, and explicit retention at startup. Applications
cannot substitute individual stores through this boundary.

### Claiming advances through node-local vehicles

The dispatcher launches `Docket.Postgres.Vehicle` under a dedicated task
supervisor. Each uncached claim fetches and compiles its effective graph on the
executing node, then reuses that runtime graph for its moment drain.

### Historical finding: the legacy lifecycle was still active

At the time of this snapshot, the branch still supported the `0.0.1` host
checkpoint committer and resident per-run processes. Durable APIs coexisted
with `run`, `resume`, and `get_run`; documentation therefore had to call this
transitional compatibility, not a completed single-lifecycle release.

This finding was resolved before v0.1.0. The released boundary has no
legacy production lifecycle.

### Historical finding: some operational promises remained design work

At the time of this snapshot, notification-assisted wakes, pruning, bounded
drains, and production assembly had landed, while deterministic manual/drain
test modes remained separate product work. v0.1.0 subsequently implemented
the deterministic backend test modes.

## Release gate

The remaining release work is downstream hardening and cutover: broader
release-gate invariants and telemetry, documented deterministic test modes,
and removal of the transitional legacy facade before v0.1.0.

## Verification performed

- At the snapshot commit, `mix test --include postgres` passed: 615 tests, 0
  failures. This count is historical, not the v0.1.0 release-gate total.
- The suite includes live bundle assembly, facade, migration, lifecycle,
  notification, vehicle, retention, tenant-isolation, and poison-recovery
  coverage against PostgreSQL.
- Repository inspection confirms that every PostgreSQL module is conditionally
  compiled behind optional `ecto_sql` and `postgrex` dependencies.
