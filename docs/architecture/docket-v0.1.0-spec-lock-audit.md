# Docket v0.1.0 Spec-Lock Audit

Date: 2026-07-09

Amended: 2026-07-10 by operational spec lock amendment 1. Durable identity is
`{graph_id, graph_hash}` over an effective canonical graph with node schema
defaults materialized before hashing. Compiled runtime graphs are node-local,
ephemeral values compiled once per vehicle claim and reused for that drain;
compiler ABI and distributed compiled artifacts are not run identity.
Local compilation validates an effective document but never adds defaults
introduced after publication; those require a new publication and graph hash.
`max_supersteps` remains optional: unbounded cyclic jobs are valid. A graph
policy limit changes graph identity; a host runtime limit does not. DCKT-34 owns the
amendment and DCKT-35 owns the unresolved pre-execution compilation-failure
claim disposition.

Scope: DCKT-1 and all 29 descendant issues, the operational transition spec,
the active DCKT-8 branch, the v0.1 migration/schema, runtime lifecycle code,
and the storage conformance backend.

## Verdict

The run-row/Postgres direction is sound, and the graph/run/event entity split
is the right foundation. The issue tree is not safe to lock unchanged. The
review found six contract blockers:

1. Public configuration pretends independently selected stores are safely
   interchangeable even though atomicity requires one compatible backend.
2. No component is named as the owner of cross-store lifecycle composition.
3. The Coordinator behaviour cannot express atomic batched due-claim, attempt,
   steal, and poison semantics required by DCKT-15/DCKT-17.
4. A pre-commit proposal is represented as a committed public checkpoint, and
   durable initialization has no no-delivery proposal seam.
5. Metadata-only checkpoint history has no concrete `Docket.Event` type or
   sequence-allocation rule.
6. The run lifecycle/status model loses terminal failure detail, hides poison
   from public inspection, admits durable `created`, and includes an undefined
   operational `blocked` state.

The proposed revision 8 of
`docs/architecture/docket-operational-transition-spec.md` resolves these
points. Linear remains unchanged pending explicit authorization for external
writes.

## Locked Architecture Decisions

### Backend is the plugin boundary

Public configuration accepts one bundle, such as `backend: Docket.Postgres`.
The bundle supplies a compatible transaction boundary, graph store,
run-aggregate store, event store, and supervision tree. Those capabilities
remain focused and independently testable, but they are not arbitrary
mix-and-match configuration.

This is genuine pluggability: the whole backend can be substituted without
claiming that two unrelated stores can somehow share an atomic transaction.

### The run row is one aggregate

All operations enforcing `docket_runs` invariants belong to the run-aggregate
port: insert/fetch/inspect, atomic `claim_due`, heartbeat/release, mandatory
token-fenced commit, serialized mutation, scheduling, and poison recovery.
Postgres may split codec, claim SQL, and queries into internal modules, but a
separately configurable Coordinator is a false boundary because both sides
must mutate and fence the same row.

The dispatcher owns demand and launching returned leases. It does not own SQL
eligibility, attempt accounting, or a second poison mutation.

### One lifecycle composer

`Docket.Lifecycle` owns the three run/event transaction recipes:

- start: initialized run insert + retained events;
- advance: fenced run commit + retained events; and
- signal: serialized run mutation + retained events.

Stores never call other stores. Facades, vehicles, and durable test drivers
delegate to the same composer. `Docket.Postgres.Storage` remains a transaction
boundary only.

Graph publication is a separate explicit transaction before start. It stores
the effective canonical graph document; lifecycle never writes graph data.

### Runtime produces moments, not committed checkpoints

Initialization, advancement, and signals produce one substrate-neutral
pre-commit `Docket.Runtime.Moment` at a time. A moment contains the proposed
run, assigned events, checkpoint metadata, and explicit disposition:

```text
:continue
{:park, :immediate, reason}
{:park, :external, reason}
{:park, {:at, timestamp}, reason}
{:park, :terminal, reason}
```

The vehicle owns the drain loop: propose one moment, commit it, then continue.
The storage contract does not own runtime disposition vocabulary. A public
committed checkpoint is created/delivered only after durable transaction
success.

At the original lock, the legacy sync checkpoint callback remained a
veto-capable host-owned committer beside separately configured durable
`checkpoint_observers`. The one-production-lifecycle amendment below
supersedes that compatibility decision for v0.1.0: only best-effort
after-commit observers remain.

### Checkpoint history has a concrete representation

Each moment allocates one metadata-only `:checkpoint_committed` event from the
run's `event_seq`, after its runtime facts. Metadata includes `checkpoint_seq`,
checkpoint type, graph step, park reason, and wake disposition.

`checkpoint_seq` remains the run-row fence. EventStore appends assigned event
identities and never allocates with `MAX(seq)` or reuses `checkpoint_seq`.
Filtering events may leave sequence gaps; that is valid.

Retry control commits use an explicit `:retry_scheduled` checkpoint type and
remain graph status `running`.

### Tenancy is optional; scope is never implicit

Every run/event operation receives exactly one scope:

```text
:system                 # dispatcher and recovery only
:tenantless             # tenant_mode: :none; tenant_id IS NULL
{:tenant, tenant_id}    # tenant_mode: :required
```

A missing keyword never implies privileged access. This prevents a tenantless
public instance or facade omission from reading tenant-owned rows.

## Status Audit

### Durable graph status

```text
running | waiting | done | failed | cancelled
```

- `running` covers ready, claimed, timer-scheduled, budget-yielded, and
  retry-backoff positions; those are derived queue conditions.
- `waiting` means no autonomous work can proceed and an external graph
  mutation is required. `await_run` stops here.
- `done`, `failed`, and `cancelled` remain distinct because success, graph
  failure, and intentional cancellation have different API and retry meaning.

Collapsing terminal states into `finished` needs another outcome discriminator.
Replacing them with `done_at` / `failed_at` / `cancelled_at` creates three
nullable state columns plus exactly-one constraints. One enum plus
`finished_at` is the smaller, stronger sum type.

Keep the existing `done` spelling for 0.0.x compatibility. `created` is only a
private initialization sentinel: never durable, never checkpointed, and never
cancellable. `interrupted`, `retrying`, `scheduled`, `ready`, and `claimed`
are derived facts, not statuses.

### Failure is data, not state inference

Add a top-level JSON-safe `Docket.Run.failure` and promoted `failure` JSONB
column. It is present exactly when status is `failed`.

Today `Runtime.Loop.fail/4` stores only `status: :failed` and `finished_at`; the
reason exists only in a `:run_failed` event. Because event policy may be
`:none`, a fetched failed run can permanently lose its cause.

Do not use graph failure to represent retryable node attempts, poison, API
errors, fence loss, or observer failure.

### Poison is an orthogonal timestamped condition

Replace:

```text
attempts
operational_status = active | blocked | poisoned
operational_error
```

with:

```text
claim_attempts
poisoned_at
poison_reason
```

There is no stored `active` value and no undefined `blocked` state.
`poisoned_at IS NULL` is normal. Do not infer poison from the current configured
maximum because configuration may later change.

`claim_attempts` counts claims actually launched since the last committed run
mutation. A row below the maximum is claimed, incremented, and launched. If a
later claim is required when the count is already at the maximum, it is
poisoned without launch. With maximum 3, exactly three vehicles may run.

Cancellation clears current poison because terminal rows have no dispatch
health. `retry_poisoned_run` checks terminal first, then clears poison and
reschedules a non-terminal run. An already-unpoisoned non-terminal call may be
idempotent success.

### Operational state must be visible

Keep `fetch_run` returning the pure graph document. Add `inspect_run` returning
a `RunInfo` projection with run, wake, claimed time (not token), claim attempts,
and poison facts. `await_run` returns a typed operational halt when poison is
present rather than timing out on a graph-semantically `running` run.

## Database Invariants

DCKT-29 should enforce:

- stored status is exactly the five durable values; never `created`;
- `started_at` is present for every row;
- `finished_at` is present iff status is terminal;
- `failure` is present iff status is `failed`;
- `output` is present only for `done`;
- claim token/time and poison time/reason are paired;
- waiting and terminal rows have no claim, wake, or poison;
- poisoned rows are running and have no claim or wake;
- non-poisoned running rows have exactly one of wake or claim;
- step, checkpoint sequence, and claim attempts are non-negative.

Claim acquisition clears `wake_at`; park atomically clears the claim and sets
the next wake. Ready-unclaimed and expired-claim recovery use separate indexes
and scans. Positive dispatcher eligibility is:

```sql
status = 'running' AND poisoned_at IS NULL
```

Add both relational safety edges:

- runs `(graph_id, graph_hash)` -> graph versions, delete restricted;
- events `run_id` -> runs `run_id`, delete cascaded.

For v0.1, events may be pruned earlier than their run but never outlive it;
otherwise they lose tenant/graph scope. Longer-lived audit export needs an
explicit schema/product contract.

## DCKT-8 Merge Blockers

The active branch must not merge as the conformance foundation until:

- its backend transaction publication is serialized or compare-and-swapped;
  the current unlocked snapshot then blind replace can erase a concurrent
  transaction;
- advance commit requires a non-nil current token and exact
  `new_seq == expected_seq + 1`;
- scope is required and typed rather than absent `tenant_id => any`;
- the run-aggregate claim contract can express batched due/expired selection
  and poison outcomes;
- `:checkpoint_committed` representation is tested; and
- the Postgres graph FK change is moved to DCKT-29 (or ticket ownership is
  explicitly amended), while `:run_cancelled` types remain owned by DCKT-9.

The current memory backend is useful as a test fixture but does not yet prove
transactional conformance under overlap.

## Ticket Change Set

### P0 contract/schema blockers

- **DCKT-1:** point to one merged rev-8 spec commit; replace Coordinator,
  operational enum, and rev-6 wording.
- **DCKT-8:** backend bundle, one run aggregate, typed scope, mandatory token,
  `inspect_run`, concurrency-safe conformance transaction.
- **DCKT-9:** no cancellable `created`; cancellation remains a terminal outcome;
  return runtime moments.
- **DCKT-10:** define `Runtime.Moment`; include no-delivery initialization;
  expose one commit boundary, not a speculative multi-step drain.
- **DCKT-11:** own `:checkpoint_committed` allocation from `Run.event_seq` and
  add retry checkpoint identity.
- **New core slice:** durable five-state vocabulary, `Run.failure`, transition
  matrix, and `RunInfo` inspection. No current ticket fully owns this work.
- **DCKT-29:** failure/poison fields, full lifecycle CHECKs, event FK, positive
  eligibility shape.

### P1 implementation alignment

- **DCKT-12:** one backend bundle and named `Docket.Lifecycle`; explicit scope;
  separate committer/observer configuration; `inspect_run`/poisoned await.
- **DCKT-14:** stores/codecs only; no start-orchestration or compilation
  ownership. Document/measure input-channel duplication for v0.1.
- **DCKT-15:** fold into RunStore claim aggregate (or rename as the full claim
  capability); atomically return claimed versus poisoned leases.
- **DCKT-16:** consume runtime moments; mandatory token; assigned event
  identities; no observer delivery before outer commit.
- **DCKT-17:** exact claim-attempt rule, clear wake on claim, two indexed paths,
  dispatcher owns demand only.
- **DCKT-18:** terminal-first poison recovery, cancel clears poison, lifecycle
  composer owns signal transaction.
- **DCKT-19:** RunStore issues transactional `pg_notify`; PostgreSQL delivers
  after commit. Poll remains correctness.
- **DCKT-20:** vehicle fetches and compiles the effective document once per
  claim, then owns propose/commit/continue and reuses that runtime graph for
  the drain. A release-scoped node-local cache is optional only.
- **DCKT-21:** events cannot outlive runs; event retention is capped by run
  retention; graph pruning follows run deletion.
- **DCKT-24:** each moment uses the production logical transaction boundary;
  no transaction spans node execution or a whole drain.
- **DCKT-25:** facade/supervision assembly only; delegate all lifecycle recipes.

### P2 release verification

- **DCKT-23:** telemetry terms use `claim_attempts` and poison facts; include
  observer loss/failure semantics.
- **DCKT-26:** transition/constraint matrix, mixed tenantless/tenant/system
  access, exact poison examples, event FK, poisoned await, transactional notify,
  and concurrent conformance publication.
- **DCKT-27:** document derived operational views rather than statuses; durable
  observer limitations; inspect/await poison; exact scope and retention rules.

## Dependency Corrections

At minimum:

- DCKT-16 is blocked by DCKT-14, not merely related.
- DCKT-19 waits for final start/commit/signal scheduling hooks.
- DCKT-23 waits for DCKT-21 prune behavior.
- DCKT-24 waits for DCKT-25 if it tests named operational APIs.
- DCKT-25 waits for DCKT-19 if “full backend tree” includes notifier behavior.
- DCKT-26 waits for DCKT-19, DCKT-21, DCKT-23, and DCKT-25.
- DCKT-27 waits for DCKT-19, DCKT-22, DCKT-23, DCKT-24, and the release-gate
  suite it claims to document.

## Verification

- `mix test`: 355 tests, 0 failures, 2 excluded.
- The excluded cases are live-Postgres tests. Plain CI therefore does not yet
  prove migration constraints or concurrency behavior; DCKT-26 must add an
  explicit live-Postgres leg.
- `git diff --check` is clean for the proposed rev-8 spec.


# Final Lock Review (2026-07-09, second pass)

A second, final review ran over the complete DCKT-1 tree (26 issues), the
rev-8 working-tree spec, the DCKT-8 branch contracts, and the WaterCooler
consumer. Eight independent architecture/boundary/consistency reviewers plus a
four-analyst status audit; every finding was adversarially verified through
two refutation lenses before acceptance (4 raw claims were refuted and
dropped).

## Architecture verdict

Sound to lock after the fixes below. The pluggability thesis held under
direct attack: the neutral contracts carry no Ecto/Postgrex/SKIP-LOCKED/
pg_notify leakage, the memory conformance backend proves whole-bundle
substitution, `Docket.Lifecycle` is a real single owner of the three
transaction recipes, and no store orchestrates another store.

## Resolved contract defects (amended into rev 8 before landing)

1. **`release_claim` (blocker).** Spec §6, DCKT-15, and the committed port
   disagreed three ways on whether a matching release touches `wake_at`.
   Resolution: the committed code was right — a matching release clears the
   claim and records `wake_at = now()`. Anything else strands a non-poisoned
   `running` row with neither claim nor wake, violating the §5 CHECK
   ("exactly one of a wake or a claim"). A matching release cannot hit
   `waiting`/terminal rows (no claim to match) and is a no-op when a steal or
   signal won. Spec §6 amended; DCKT-15/DCKT-8 aligned.
2. **`resolve_interrupt` vs poison (blocker).** DCKT-18 ("do not heal
   poison"), spec §8 (`wake_at = now`), and the §5 no-wake-while-poisoned
   CHECK could not all hold. Resolution: successful graph-signal mutations
   clear current poison facts (matching the committed `mutate_run` port and
   `cancel_run`'s existing "clear current poison"). Spec §8 amended with the
   rule and rationale; DCKT-18 line removed.
3. **`resolve_interrupt` terminal precondition (major).** The status × signal
   matrix omitted terminal runs with a still-open interrupt. Resolution:
   terminal-first ordering, returning `:inactive_run` before the interrupt
   lookup, matching `cancel_run` and the existing core guard. Spec §8 table
   amended; DCKT-18/DCKT-9 aligned.
4. **Rev-8 landing owner (major).** "Revision 8 must land on v0.1.0" had no
   owning ticket while DCKT-8 depended on it being "separately landed". A
   dedicated landing ticket now owns committing the amended rev-8 spec + this
   audit to `v0.1.0`, blocking DCKT-8.
5. **Undeclared Lifecycle dependency (major).** DCKT-14/16/18/20 consume the
   DCKT-12 `Docket.Lifecycle` composer in hard acceptance items without a
   blocker edge (DCKT-25 had it). Blocker relations added.
6. **Poison introspection index (minor).** DCKT-29 dropped the
   `operational_status` index without re-adding the §5
   `partial (poisoned_at) WHERE poisoned_at IS NOT NULL` index. Added to
   DCKT-29 scope with a query-plan assertion.

## Status audit — final ruling: keep `running | waiting | done | failed | cancelled`

The five-value flat enum was challenged from four independent directions
(minimalist steelman, defender, industry precedent, mechanical consumer
inventory across both repos), judged, and the judgment attacked by three
skeptics. All four analysts — including the one instructed to argue for
trimming — converged on keeping the model; zero skeptics refuted. The full
reasoning is recorded in the spec §5 decision record ("why five flat values
survive the trim audit"): every trim relocates a SQL-enforceable or
typed-column invariant into opaque JSON or application-only derivation, so
the five-value set is the minimum-viable model measured in total moving
parts. `:created` remains a transient initializer sentinel rejected by
durable storage; poison remains operational health outside status.

Consumer fixes identified in WaterCooler (applied in that repo): drop the
durable `created` default/value, fix the MCP run-list filter enum (admitted
`created`, omitted `cancelled`), add a `cancelled` color mapping, and log
`run_cancelled` in the run-event timeline.


# Spec-Lock Sign-Off (2026-07-09, third pass)

Six reviewers over the current Linear tree (32 issues), the amended
working-tree spec, and the branch contracts: four epic-scoped, one
cross-cutting ownership/fusion/graph/boring analysis, one regression pass
against both prior audits. Every blocker/major/minor finding was re-verified
against the quoted source before acceptance.

## Verdict

**Sound to lock.** All 50 fix items from the two prior passes verified as
landed. The ownership map is single-owner clean (bundle seam DCKT-8,
Lifecycle DCKT-12, Moment DCKT-10, state model DCKT-31, schema invariants
DCKT-29, claim aggregate DCKT-8/15, demand-only dispatcher DCKT-17, vehicle
DCKT-20, signals DCKT-9/18, retention DCKT-21, spec landing DCKT-32). The
80-edge blocking graph matches every ticket's declared relations exactly, is
acyclic, and contains all eleven audit-required corrections. The fusion audit
found no Coordinator-in-costume and no atomicity-fused capability presented
as configurable; the boring audit found no gold-plating and no operability
gap. The residual defects below are wording/coverage drift, not architecture.

## Spec amendments applied this pass (uncommitted, land via DCKT-32)

1. §9 Signal Run recipe now clears current poison alongside the live claim,
   matching the amended §8 rule; the previous recipe could produce a
   `wake_at = now` + `poisoned_at` row violating the §5 CHECK.
2. §6 no longer frames a claimed-but-non-runnable run as a routine release;
   it is the invariant violation §9 and DCKT-20 already require.
3. §6 states explicitly that a matching claim release records an immediate
   wake but deliberately relies on poll, not a notification site.

## Ticket changes required before lock (applied to Linear 2026-07-09)

- **DCKT-16 (major):** cede transactional `pg_notify` entirely to DCKT-19 —
  remove the notify scope bullet and the "run/events/disposition/notify
  atomic" acceptance (DCKT-16 lands before DCKT-19 and cannot test notify).
  Also widen "successful graph mutation resets `claim_attempts`" to any
  committed run mutation including `:retry_scheduled` parks.
- **DCKT-26 (major):** add the pass-2 amended scenarios: poisoned
  `resolve_interrupt` clears poison and commits its immediate wake, and
  terminal-first `resolve_interrupt` returns `:inactive_run` with a
  still-open interrupt. Only the cancel-side twins are present today.
- **DCKT-30/DCKT-10 (major):** the whole-ticket `30 blocks 10` edge inverts
  `Runtime.Moment` ownership — DCKT-30's scope "produces one DCKT-10
  `Runtime.Moment`" consumes the type DCKT-10 defines. Keep the edge and
  narrow DCKT-30 to the durable state model and retry-park semantics
  (requirements language); move moment-producing acceptance into DCKT-10's
  "durable retry moment" scope, which DCKT-30's own prose already names as
  the DCKT-10 half.
- **DCKT-17 (minor):** own the §6 coalescing rule — at most one in-flight
  claim poll; a notify burst collapses into one pending poll.
- **DCKT-15 (minor):** add the §6 bounded-claim fence: materialized CTE (or
  equivalent) with a plan/concurrency assertion that the claim UPDATE cannot
  expand beyond the selected candidates.
- **DCKT-25 (minor):** add `DCKT-21 blocks DCKT-25` — the assembled bundle
  supervises the pruner but nothing orders the pruner first.
- **DCKT-12 (minor):** own the disposition→schedule mapping line: Lifecycle
  maps each moment disposition to the `Storage.Runs` schedule effect,
  keeping external and terminal parks distinct (asserted by DCKT-8/16,
  claimed by neither).
- **DCKT-20 (minor):** add a direct `DCKT-15 blocks DCKT-20` edge
  (`release_claim`/token operations are consumed in hard acceptance items).
- **DCKT-1 (minor):** `:checkpoint_committed` allocates from the run's
  `event_seq` (assigned as an `Event.seq`), not "from independent
  `Docket.Event.seq`" — align with owner DCKT-11 and the spec.
- **DCKT-14 or DCKT-29 (nit):** name `Docket.Postgres.Schemas.Run` as owned
  cleanup so the retired `operational_status`/`attempts`/`operational_error`
  fields cannot survive by mutual assumption.
- **DCKT-9 (nit):** attribute poison-clearing to the run store's serialized
  `mutate_run`, not "backend lifecycle wiring".
- **DCKT-3 (nit):** annotate the epic "Contains" items owned by sibling
  epics (RunStore mutation/poison recovery → DCKT-18; Lifecycle → DCKT-12).
- **Cosmetic:** stale pre-rework slugs in DCKT-1/3/13/15/28 hrefs; neutral
  contracts say `MAX(seq)` where "the maximum already-stored sequence" is
  the substrate-neutral phrasing.

## DCKT-8 merge blocker addendum

The committed contract placed commit's error-precedence rule on the wrong
callback: `fetch_run`'s doc carried the proposal-validation paragraph (it
takes no proposal and cannot return `:invalid_commit`) while `commit`'s doc
never locked `:invalid_commit`-before-`:not_found`. The ticket's acceptance
already demanded the correct placement; `memory_backend.ex` behavior was
already correct. Fixed on the branch in this pass, along with the
substrate-neutral rewording of the events contract's sequence-derivation
prohibition.

## Lock

All third-pass fixes above are applied: three spec amendments in the working
tree, thirteen ticket edits and two new blocker edges in Linear
(DCKT-21 → DCKT-25, DCKT-15 → DCKT-20), the DCKT-1 lock statement and lock
comment, and the two branch contract-doc corrections. **v0.1.0 is locked as
of 2026-07-09.** DCKT-32 lands this audit and the amended revision-8 spec on
`v0.1.0`; contract or boundary changes after this point reopen the lock with
a new audit entry.

# Post-Lock Amendment (2026-07-10): Backend Configuration Name

**Decision:** Rename the unreleased public durable runtime option from
`storage: BackendModule` to `backend: BackendModule`. The resolved internal
context is correspondingly named `:backend_context`, and the backend child is
named under `Runtime.Backend`.

**Rationale:** The configured module implements `Docket.Backend` and owns the
compatible transaction, graph, run, event, context, and supervision
capabilities. Calling that bundle `storage` inaccurately implied that callers
were selecting one `Docket.Storage` implementation rather than the complete
durable backend boundary. The `Docket.Storage` capability and the backend's
`storage/0` callback retain their names because they specifically represent
the transaction boundary within the bundle.

**Compatibility:** v0.1.0 remains unreleased, so no compatibility alias is
provided. Supplying the former `storage:` option fails at runtime-supervisor
startup rather than being silently ignored.

**Ownership:** DCKT-12 owns this amendment because it introduces the durable
public facade and its backend resolution. The amendment changes naming only;
the locked single-bundle architecture and capability contracts are unchanged.

# Post-Lock Amendment (2026-07-10): One Production Lifecycle

**Decision:** v0.1.0 requires one `Docket.Backend` for every supervised
production instance. The `0.0.1` host-owned checkpoint driver and its public
`run`, `resume`, and live `get_run` facade are removed once the Postgres
operational replacement and deterministic backend test modes are complete.
`resolve_interrupt` becomes exclusively storage-backed. No compatibility
aliases or configuration-dependent dispatch remain.

**Rationale:** The two drivers have different transaction owners, recovery
models, read semantics, and sources of truth. Keeping both would make Docket's
durability guarantee configuration-dependent and permanently duplicate its
supervision, signal, documentation, and test contracts. Backend ownership is
the product boundary; Postgres is the first-party paved road, while the core
runtime remains substrate-neutral behind `Docket.Backend` and
`Docket.Lifecycle`.

**Narrow compatibility break:** Node modules and graph definitions carry over
unchanged. `Docket.Node`, `Docket.Graph`, `Docket.Schema`, reducers,
interrupts, executors, compiler/serialization APIs, `Docket.Run.to_map` /
`from_map`, and `Docket.Test.run_inline` and related processless helpers remain
public. Adopters replace only lifecycle ownership: remove their checkpoint
committer and host tables, install Docket's migration, configure
`repo:`/`backend:`, publish graphs to `GraphRef`, replace `run` with
`start_run`, replace `get_run` with `fetch_run`/`inspect_run`, and delete
host-owned `resume` orchestration.

**Migration:** Because `0.0.1` persistence is host-defined, the supported
migration is an explicit drain-and-cut-over rather than a universal automatic
database migration or indefinite old-wire compatibility. Retained events are
the durable integration path; checkpoint observers are best-effort
after-commit notifications only.

**Landing and blockers:** DCKT-37 owns the removal and migration-doc changes.
It is blocked by DCKT-25 (assembled operational facade/backend) and DCKT-24
(deterministic backend test modes), and it blocks DCKT-26 so the final
release-gate suite tests the single production lifecycle. Historical `0.0.1`
design documents remain useful background but are superseded for v0.1.0
production execution.
