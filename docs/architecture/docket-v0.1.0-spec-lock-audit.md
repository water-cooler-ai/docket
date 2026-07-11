# Docket v0.1.0 Implementation Audit

Date: 2026-07-11

This audit compares the `v0.1.0` branch with the architecture previously
described by the operational transition spec. It records repository state,
not ticket intent. The code and module documentation remain authoritative.

## Verdict

The durable data model and transaction boundaries are implemented and closely
match the locked architecture. The branch is not yet the proposed operational
release: it lacks the public `Docket.Postgres` backend bundle that assembles
the dispatcher and vehicle into one supervised configuration.

The documentation previously obscured that distinction. It described the
target configuration as usable, called the legacy lifecycle removed when it
still exists, and mixed implementation details with future milestones. The
PostgreSQL guide now documents the landed design and names the remaining gaps.

## State by area

| Area | Current state | Evidence |
| --- | --- | --- |
| Backend contract | Implemented | `Docket.Backend` bundles storage, graph, run, event, context, and supervision capabilities. |
| Public PostgreSQL bundle | Missing | No `Docket.Postgres` module implements `Docket.Backend`. |
| Versioned migration | Implemented | `Docket.Postgres.Migration` and schema version 1 create three tables with constraints and indexes. |
| Graph store | Implemented | Immutable effective graph save/fetch with content-address conflict checks. |
| Run aggregate store | Implemented | Scoped reads, insert, mutation, bounded claims, fencing, release, heartbeat, and poison retry. |
| Event store | Implemented | Assigned event append in the lifecycle transaction. |
| Lifecycle composer | Implemented | `Docket.Lifecycle` owns start, moment commit, and signal transaction recipes. |
| Durable facade | Implemented against a backend | Publication, start, reads, signals, poison retry, and bounded await are exercised with the conformance backend. |
| Dispatcher | Implemented | Demand-bounded, jittered polling and lease launch/release behavior exist. |
| Execution vehicle | Implemented | `Docket.Postgres.Vehicle` fetches and compiles the graph for a lease (with an optional generation-checked cache), drains fenced moments, and abandons, releases, or parks the run. Claim freshness during long supersteps is configurable: strict timeout alignment by default, opt-in token-guarded heartbeat with stale-result rejection. |
| Backend supervision assembly | Missing | Nothing wires Repo context, dispatcher, and vehicle launch into `Docket.Postgres.child_spec/1`. |
| Deterministic backend test mode | Missing | There is no public PostgreSQL drain/manual testing API. |
| Pruning/retention | Implemented substrate | `Docket.Postgres.Pruner` performs locked, bounded event/run cleanup and retains each graph ID's newest ten unreferenced revisions; DCKT-25 still owns public bundle configuration and supervision assembly. |
| Legacy production API removal | Not done | `run`, `resume`, `get_run`, and `checkpoint:` remain in the public supervised runtime. |

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

### The backend cannot currently be configured as documented

This target does not work on the current branch:

```elixir
use Docket, repo: MyApp.Repo, backend: Docket.Postgres
```

`Docket.Runtime.Supervisor` validates the backend callbacks at startup, and
`Docket.Postgres` does not exist. The individual PostgreSQL modules are a
foundation, not yet the public bundle promised by the release design.

### Claiming does not yet advance a graph

The dispatcher accepts a `launch` callback and correctly accounts for
capacity, polling, failures, and shutdown. There is no production callback
implementation. Therefore the repository proves queue coordination but not
end-to-end cold recovery or multi-node graph execution.

### The legacy lifecycle remains active

The branch still supports the `0.0.1` host checkpoint committer and resident
per-run processes. Durable APIs coexist with `run`, `resume`, and `get_run`.
Documentation must call this transitional compatibility, not a completed
single-lifecycle release.

### Some operational promises remain design work

The prior spec named notification-assisted wakes, pruning, deterministic drain
modes, and production operational assembly. They have not landed and are not
part of the current public contract. Polling exists; PostgreSQL notification
does not. Retained events exist; a public consumer/export API does not.

## Release gate

Before calling `0.1.0` an operational PostgreSQL release, the repository needs
at minimum:

1. `Docket.Postgres` implementing the backend bundle and validating its
   configuration;
2. supervision wiring and an end-to-end PostgreSQL test proving start,
   dispatch, crash recovery, signal, and terminal completion;
3. a documented deterministic test mode; and
4. a deliberate decision to remove or explicitly retain the legacy facade.

Pruning and notification-assisted wakeups may be separately scoped, but the
release documentation must not imply they exist until their code lands.

## Verification performed

- `mix test --exclude integration` passes: 500 tests, 0 failures, 61 excluded.
- The excluded set includes live PostgreSQL coverage, so that result verifies
  the core and conformance behavior but is not an end-to-end database release
  certification.
- Repository inspection confirms that every PostgreSQL module is conditionally
  compiled behind optional `ecto_sql` and `postgrex` dependencies.
