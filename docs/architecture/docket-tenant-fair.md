# TenantFair claim policy

TenantFair is Docket's one PostgreSQL tenant-scheduling design. It combines an
authoritative unfinished-tenant ring with sticky logical-run admission. There
is no alternate TenantFair scheduler or compatibility ABI.

The configured `default_max_active_runs` is the default number of logical runs
that may be admitted for one tenant. A per-tenant Admin override may replace
that value. When the cap is one, the oldest queued run remains the admitted
run until it finishes, enters an external wait, schedules into the future, is
poisoned, or is interrupted; only then may the next queued run be promoted. A
cooperative immediate yield releases only the transient claim: the same run
retains admission and reacquires ahead of queued runs.

## Derived states and invariant

`docket_runs.tenant_admitted_at` is an internal nullable timestamp. It does not
add a public run status.

- **queued:** healthy `running`, due, unclaimed, and unadmitted;
- **admitted-ready:** healthy `running`, due, unclaimed, and admitted;
- **admitted-claimed:** healthy `running`, claimed, and admitted; and
- **inactive:** future-scheduled, externally waiting, poisoned, or terminal,
  with no admission.

The normal-state invariant is:

```text
live_claim_count(scope) <= admitted_run_count(scope) <= max_active_runs(scope)
```

A cap decrease may create debt, so existing admissions are not preempted. The
engine promotes no queued work while admitted count is at or above the cap.

## Admission and within-tenant FIFO

Promotion is the only transition that creates an admission. Under the tenant
partition lock, the claim function freshly counts admitted healthy rows and
promotes only while capacity remains. Promotion order is `(wake_at, internal
id)` among due, healthy, unclaimed, unadmitted rows.

Already-admitted eligible work is served before promotion. A queued candidate
may be promoted only from a contiguous locked and rechecked FIFO prefix; a
locked or stale head may underfill a visit but cannot be bypassed. No candidate
cursor rotates past a blocked head.

Two permanently runnable admitted runs at cap two intentionally keep later
runs queued. Cross-tenant ring fairness does not imply within-tenant time
slicing beyond the admitted cohort.

## Admission lifetime

Cooperative drain yield, generic immediate claim release, refresh,
reacquisition, vehicle replacement, and expired steal preserve the original
admission timestamp. Future scheduling, external waiting, terminal completion,
failure, cancellation, host-incompatible abandon/backoff, interruption, and
poison clear it. Waking a previously unadmitted run leaves it queued.

Every transition is atomic with lifecycle state and retains the existing claim
token and checkpoint-sequence fences. A stale holder cannot clear a newer
admission, and transaction rollback persists neither side.

## Authoritative unfinished membership

`docket_claim_schedule` has one immutable-position row per claim partition:

- `scope_key` is the primary and foreign key;
- `ring_position` is a positive unique monotonic identity;
- `unfinished_count` is the exact count of committed `docket_runs` rows for the
  scope whose durable status is nonterminal (`running` or `waiting`); and
- the partial `(ring_position)` index contains exactly rows where
  `unfinished_count > 0`.

`unfinished_count` is not the admitted-run count. It includes ready runs,
future timers, externally parked/waiting runs, claims, and poisoned running
rows. The ring is therefore an authoritative superset of current claim
eligibility. Dormant positions may produce unsuccessful inspections, but
nonterminal work cannot be omitted from membership.

The row remains at zero instead of being deleted, preserving its ring position
across idle periods. A run-table trigger maintains the counter in the same
transaction. Scope is immutable, positive unfinished count prevents deleting a
partition with unfinished work, direct counter updates are rejected, underflow
fails closed, and truncating either authoritative table is rejected.

## Bounded cross-tenant traversal

| Name | Value | Boundary |
| --- | ---: | --- |
| `S` | 32 | unfinished-ring positions inspected per qualifying call |
| `Q` | 8 | lease or poison outcomes per nonempty partition grant |
| `K` | 16 | exact-partition run keys admitted to one lock-attempt window |
| `M` | 8 | run rows one grant may finally mutate; equal to `Q` |

One inspection creates at most one grant. A call is bounded by 32 grants, 512
exact-key run-lock attempts, and 256 returned outcomes or mutation inputs;
caller demand normally lowers those ceilings. These constants bound logical
work, not elapsed time or query-plan behavior.

The candidate population is the partial unfinished-ring index. One recursive
keyset seek reads the next position after the durable global cursor and wraps
to the first position. While demand remains, it performs exactly `S` visits,
including repeated wrap when the active ring has fewer than `S` positions.

Discovery uses an MVCC read of schedule membership. Each visit separately
attempts exact partition authority, so a locked partition is one explicit
failed inspection. Run locking freezes no more than `K` exact IDs before using
`SKIP LOCKED`; it cannot scan an unbounded locked prefix to manufacture `K`
successful locks.

For one partition, the engine reads bounded admitted-ready, queued-ready, and
expired pages, reserves ready/expired class service before truncating the
attempt set, and mutates at most `Q` locked rows. Every locked row is
authoritatively rechecked before mutation.

## Lock graph

The required order is:

```text
admission: policy + scan cursor -> partition authority -> run rows
lifecycle: run row -> schedule counter
discovery: MVCC read of schedule ring; no schedule-row lock
```

The counter lives in the schedule sidecar because lifecycle paths already lock
a run before recording its terminal transition. Putting it on partition cap
authority would create a run-to-partition edge against admission's
partition-to-run order. Admission never locks a schedule row and then seeks a
run.

## Schema, migration, and rollout

The current schema owns the marker and its shape constraint; partial indexes
for admitted count, admitted-ready order, queued-ready order, and admitted
expired-claim order; partition and schedule authority; lifecycle triggers; and
the sole seven-argument `docket_tenant_fair_claim` function.

The stopped host-schema upgrade backfills every healthy claimed row from
`claimed_at`. Unclaimed rows remain queued, and an over-cap tenant becomes
admission debt rather than being trimmed. Fresh and host-schema-V1 migrations,
rollback to host schema V1, custom prefixes, and failed transactional upgrades
are covered by PostgreSQL tests.

Stop all Docket writers, apply the generated migration, and restart a
homogeneous application version. Previously applied unreleased development
schemas must be recreated with the current migration; the implementation does
not carry alternate claim functions or shape-detection branches for them.

With PostgreSQL tenancy enabled (`tenant_mode: :required`), the configured
claim-policy implementation must be TenantFair and must explicitly set
`default_max_active_runs`. Legacy admission remains available only for
tenantless operation.

## Administration and observability

The public configuration name is `default_max_active_runs`; Admin values use
`max_active_runs`. Database columns named `max_active` remain internal.
`Admin.get_effective/2` returns effective cap and versions plus token-free
aggregate `queued`, `admitted_ready`, `admitted_claimed`, and `debt` counts.
Metrics and ordinary trace labels never contain tenant, run, or claim-token
identity.

## Research lineage

The sticky release model is informed by [Solid Queue concurrency
controls](https://github.com/rails/solid_queue/blob/98af3c9e6d1740afbf1df34958c38e36c634b046/README.md#concurrency-controls):
its [claimed-execution release
path](https://github.com/rails/solid_queue/blob/98af3c9e6d1740afbf1df34958c38e36c634b046/app/models/solid_queue/claimed_execution.rb#L65-L84)
returns the same job to ready without releasing its concurrency semaphore.
Solid Queue does not supply Docket's strict FIFO, no-TTL residency, or bounded
ring theorem; those are Docket contracts.

[Oban Pro Smart Engine global
partitioning](https://oban.pro/docs/pro/Oban.Pro.Engines.Smart.html#module-global-partitioning)
is precedent for cluster-wide per-tenant partitions and fair partition
selection. Oban limits executing jobs and may burst; Docket instead caps sticky
logical run identities and provides no borrowing mode.

PostgreSQL documents that [`SKIP LOCKED` produces an inconsistent queue-like
view](https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE)
and that successive commands in [Read
Committed](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED)
receive fresh snapshots. Docket infers—not PostgreSQL—that `SKIP LOCKED` alone
provides neither FIFO nor a structural work bound. The frozen exact-ID set,
contiguous queue ordinal, older-head recheck, and partition authority supply
those additional guarantees.

Additional primary sources for active-ring maintenance and bounded discovery:

- [RFC 970 fair round-robin queues](https://www.rfc-editor.org/rfc/rfc970.html)
- [RFC 8290 active-queue maintenance](https://www.rfc-editor.org/rfc/rfc8290.html#section-4)
- [PostgreSQL trigger behavior](https://www.postgresql.org/docs/current/trigger-definition.html)
- [PostgreSQL trigger-maintained summary table](https://www.postgresql.org/docs/current/plpgsql-trigger.html)
- [PostgreSQL partial indexes](https://www.postgresql.org/docs/current/indexes-partial.html)

These sources support the design lineage; they do not prove Docket's
database-specific fairness theorem. The fixed-window proof, lock-order
invariants, and executable comparison with Legacy remain project-owned.
