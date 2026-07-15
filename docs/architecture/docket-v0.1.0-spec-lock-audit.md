# Historical Docket v0.1.0 Pre-Cutover Audit

Date: 2026-07-11

Status: historical pre-cutover snapshot. DCKT-37 superseded its lifecycle and
testing-gap findings after the assembled backend and deterministic modes
landed. See the module docs, PostgreSQL guide, and migration guide for the
current v0.1.0 production boundary.

This audit compared the pre-DCKT-37 `v0.1.0` branch with the architecture
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
> before DCKT-24 and DCKT-37 landed. “Missing” and “Not done” below are not
> claims about the released v0.1.0 code.

| Area | State in pre-cutover snapshot | Evidence |
| --- | --- | --- |
| Backend contract | Implemented | `Docket.Backend` bundles storage, graph, run, event, context, and supervision capabilities. |
| Public PostgreSQL bundle | Implemented | `Docket.Postgres` fixes the storage capabilities and owns their operational supervision. |
| Versioned migration | Implemented | `Docket.Postgres.Migration` schema version 1 creates three tables, scopes graph ownership, and installs constraints and indexes. |
| Graph store | Implemented | Tenant-owned immutable effective graph save/exact/latest/list reads with content-address conflict checks. |
| Run aggregate store | Implemented | Scoped reads, insert, mutation, bounded claims, fencing, release, claim refresh, and poison retry. |
| Event store | Implemented | Assigned event append in the lifecycle transaction. |
| Lifecycle composer | Implemented | `Docket.Lifecycle` owns start, moment commit, and signal transaction recipes. |
| Durable facade | Implemented against a backend | Publication, start, reads, signals, poison retry, and bounded await are exercised with the shared-test backend. |
| Dispatcher | Implemented | Demand-bounded, jittered polling and lease launch/release behavior exist. |
| Execution vehicle | Implemented | `Docket.Postgres.Vehicle` fetches and compiles the graph for a lease (with an optional generation-checked cache), validates it against the host attempt maximum, and drains fenced moments. Runtime-owned finite deadlines bound executor callbacks; orphan TTL and fencing recover crashed or stolen claims. |
| Backend supervision assembly | Implemented | A one-for-all execution subtree couples dispatcher accounting to its vehicle supervisor; notifier and pruner are isolated siblings. |
| Deterministic backend test mode | Missing at snapshot; implemented by DCKT-24 | The snapshot had no public PostgreSQL drain/manual testing API. |
| Pruning/retention | Implemented | `Docket.Postgres.Pruner` performs locked, bounded cleanup under the bundle's required explicit policy. |
| Legacy production API removal | Not done at snapshot; completed by DCKT-37 | The snapshot still exposed `run`, `resume`, `get_run`, and `checkpoint:`. |

## Decisions verified in code

### One backend is the substitution boundary

`Docket.Runtime.Supervisor` accepts `backend:` and rejects independently
configured transaction or store modules. It resolves one backend context and
adds the backend's child specification to the runtime tree.

This preserves atomicity: graph, run, and event capabilities must understand
the same transaction context. The store behaviours remain independently
testable without pretending arbitrary stores can be safely mixed.

### A run row is one durable aggregate

`Docket.Backend.RunStore` owns every operation that changes the scheduling or
commit-authority tuple. PostgreSQL may split schemas and codecs internally,
but claim and graph-state fencing update the same row.

The version 1 migration enforces the five durable statuses and the valid
status/schedule/claim/poison shapes with database constraints. Separate partial
indexes serve ready, expired-claim, and poison inspection paths. Generated
scope keys and the scoped graph-version foreign key enforce graph ownership.

### Runtime transitions are pre-commit values

`Docket.Runtime.Moment` represents one proposed commit boundary. The lifecycle
composer persists the run and events, then creates checkpoint notifications
only after success. Retry parking remains `running`, preserves active task
state, and schedules a future wake without sleeping in the durable path.

### Graph identity is the effective document

Publication snapshots node schema defaults and hashes the canonical effective
graph. Durable identity is `{owner_scope, graph_id, graph_hash}`; the public
`GraphRef` carries the content address while every operation supplies owner
scope separately. Starting or recovering a run compiles the retained effective
document locally without injecting defaults from newer node code. Compiled
runtime graphs aren't stored.

### Scope is explicit

All graph, run, and event operations carry explicit scope. Graph operations
accept only the owning `:tenantless` or `{:tenant, tenant_id}` scope; internal
execution derives it from the already-authorized run rather than performing an
unscoped lookup. Public tenant modes resolve scope before a storage call,
preventing an omitted tenant from becoming privileged access.

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

This finding was resolved by DCKT-37. The released v0.1.0 boundary has no
legacy production lifecycle.

### Historical finding: some operational promises remained design work

At the time of this snapshot, notification-assisted wakes, pruning, bounded
drains, and production assembly had landed, while deterministic manual/drain
test modes remained separate product work. DCKT-24 subsequently implemented
the deterministic backend test modes.

## Release gate

The remaining release work is downstream hardening and cutover: broader
release-gate invariants and telemetry, documented deterministic test modes,
and the deliberate DCKT-37 removal of the transitional legacy facade.

## Verification performed

- At the snapshot commit, `mix test --include postgres` passed: 615 tests, 0
  failures. This count is historical, not the v0.1.0 release-gate total.
- The suite includes live bundle assembly, facade, migration, lifecycle,
  notification, vehicle, retention, tenant-isolation, and poison-recovery
  coverage against PostgreSQL.
- Repository inspection confirms that every PostgreSQL module is conditionally
  compiled behind optional `ecto_sql` and `postgrex` dependencies.

## DCKT-43 lock reopening (2026-07-12)

The DCKT-1 spec lock reopens narrowly to add two public read/interchange
surfaces. Nothing in the durable data model, transaction boundaries, claim
fencing, or persistence codecs changes; ETF persistence is unchanged.

### Retained-event reader contract

`Docket.list_events/3` (and the generated `list_events/2` host wrapper) reads a
tenant-scoped, keyset page of retained durable events for a run, backed by the
new `Docket.Backend.EventStore.list_events/4` backend callback with memory and
Postgres implementations. The final contract:

- Events return in ascending sequence order, restricted to sequences greater
  than `:after_seq` (default `0`) and bounded by `:limit` (default `250`, an
  integer in `1..1000`). Invalid options are rejected before storage; a wrong
  tenant and an unknown run both report `{:error, :not_found}`.
- Sequence gaps are legal. Persistence filtering and retention pruning both
  leave holes, so pages and retention bounds are never promised contiguous.
- An undecodable stored row is a typed `%Docket.Error{type: :corrupt_event_row}`
  and is never silently skipped.
- The result is a `Docket.EventPage`: `events`, `next_after_seq`, `has_more?`,
  `oldest_available_seq`, `latest_available_seq`, and `latest_seq` — the owning
  run's latest committed event sequence, present even when history is fully
  pruned, so a fully pruned history is detectable as `latest_seq > 0` with
  `latest_available_seq == nil`. The Postgres reader gathers the page rows,
  MIN/MAX retention bounds, and run state in one SQL statement so every field
  reflects one consistent snapshot.

This is the durable repair source for observer gaps: `checkpoint_observers:`
are best-effort and may drop or duplicate, while the reader exposes what
durably committed within the retained window. Delivery still requires an
application-owned durable cursor and idempotent downstream handling.

### Authored graph interchange contract

`Docket.Graph.to_map/2`, `from_map/2`, and `from_map!/2` (through the strict
`Docket.Graph.Serializer`) return, and validate, a JSON-safe, string-keyed map
with an explicit `schema_version: 1`. The final contract:

- Strict validation rejects unknown structural keys, unknown enum values,
  malformed tagged expressions, unsupported versions, and non-portable values;
  `$`-prefixed keys are reserved for tagged expressions.
- Executable node implementations resolve only through an explicit host
  `implementations:` registry of stable string identifiers, validated eagerly
  for unambiguous reverse mapping. Loading never creates or reaches
  implementation atoms. Failures are typed `Docket.Graph.Error` codes
  `:invalid_registry`, `:unregistered_implementation`, and
  `:unknown_implementation`.
- This is the editable AUTHORED graph document. It carries no `Docket.GraphRef`
  hash; `save_graph` still materializes node defaults and privately encodes and
  hashes the effective graph, so re-saving after defaults change may yield a
  different effective reference.

Non-goals held firm: no public `Docket.Run` map codec, no public
`Docket.Graph.hash`, no Jason dependency (hosts own JSON encode/decode), and no
change to the private ETF persistence codec.

## Public query surface reopening (2026-07-12)

The release lock also reopens the read surface without changing durable graph
or run identity:

- `fetch_graph` accepts only an exact `Docket.GraphRef` and returns the effective
  document. `fetch_latest_graph_ref` selects a graph ID's newest distinct
  tenant-owned version, while `list_graph_versions` returns lightweight
  metadata ordered by `published_at DESC, graph_hash DESC` with the same stable
  cursor. Re-saving a version is idempotent and does not reorder it. A
  `GraphRef` is scope-relative and never an authorization credential.
- `list_runs` returns a `Docket.RunPage` of lightweight `Docket.RunSummary`
  projections. The query is always SQL-scoped to `:tenantless` or the required
  tenant, supports graph and durable-status filters, and paginates newest first
  with the immutable `(started_at, run_id)` key. `fetch_latest_run` is the
  limit-one form of the same scoped query.
- `fetch_event` reads one retained positive event sequence.
  `fetch_latest_event` reads the highest retained sequence, returning
  `{:ok, nil}` when the owning run is visible but no event rows survive
  retention. Missing or wrong-scope runs remain indistinguishable as
  `{:error, :not_found}`.

The storage behaviors declare each operation, and the memory and PostgreSQL
backends implement the same return, scope, ordering, and retention semantics.
