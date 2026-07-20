# PostgreSQL TenantFair claim policy

TenantFair provides exact per-owner logical-run admission plus conditional
bounded cross-partition rotation. It does not promise completion order,
wall-clock latency, CPU or resource equality, strict round robin, dynamic-
population fairness, weighted or preferred share, borrowing, reclaim, or
unconditional starvation freedom.

## Configuration and engine boundary

TenantFair is the sole PostgreSQL scheduler for required tenancy:

```elixir
use Docket,
  backend: Docket.Postgres,
  repo: MyApp.Repo,
  tenant_mode: :required,
  claim_policy: [
    implementation: Docket.Postgres.ClaimPolicy.TenantFair,
    default_max_active_runs: 4
  ]
```

`default_max_active_runs` is a required integer in `1..2_147_483_647`. It
initializes an unset database default; the persisted default and per-scope Admin
overrides are authoritative afterward. The engine is selected once per backend
instance, not per claim call.

Legacy remains the omitted-policy default only for `tenant_mode: :none`. It is
tenant-blind and never creates admission markers. Required tenancy rejects
Legacy and requires TenantFair with an explicit cap.

`Docket.Postgres.RunStore.claim_due/3` remains the only admission entrypoint.
It executes one prefix-local `docket_tenant_fair_claim` statement through the
internal ClaimPolicy plan/decode/observe seam. TenantFair and Legacy both lock
the singleton policy row and may proceed only in their own `admission_mode`, so
the two engines cannot admit concurrently.

## Fairness domain and authority

One fairness domain is one physical PostgreSQL database plus one resolved
Docket schema. Prefix or `search_path` aliases that resolve to the same physical
schema name the same domain; different schemas are different domains.

The owner scope is `:tenantless` or `{:tenant, tenant_id}`. Tenantless maps to
the empty `scope_key` and is one ordinary partition, not a wildcard or separate
scheduling class.

Schema V2 contains four authorities:

- `docket_claim_policy`: singleton engine mode, persisted default cap, and the
  domain-global `scan_ring_position` cursor;
- `docket_claim_partitions`: per-scope cap override, version, and
  outcome-backed `admission_epoch`;
- `docket_claim_schedule`: stable ring position and exact unfinished count for
  every partition; and
- `docket_runs.tenant_admitted_at`: durable logical-run admission identity.

The database, not application memory or telemetry, is authoritative.
Every run-creation transaction atomically creates its owner partition; rollback
leaves neither the run nor a new partition behind.

## Sticky admission

`tenant_admitted_at` is internal and does not add a public run status. The
derived states are:

- **queued:** healthy `running`, due, unclaimed, and unadmitted;
- **admitted-ready:** healthy `running`, due, unclaimed, and admitted;
- **admitted-claimed:** healthy `running`, claimed, and admitted; and
- **inactive:** future-scheduled, externally waiting, poisoned, or terminal,
  with no admission.

An admitted identity is a healthy `running` row with non-null
`tenant_admitted_at`; its claim token may be present or absent. In TenantFair
mode, normally:

```text
live_claim_count(scope) <= admitted_run_count(scope) <= max_active_runs(scope)
```

Lowering a cap below the admitted count creates non-preemptive debt. Existing
admissions are not revoked and remain eligible for reacquisition, but no queued
run may be promoted until the admitted count falls below the new cap.

### FIFO promotion

Promotion is the only null-to-non-null admission transition. Under partition
authority, TenantFair freshly counts admitted healthy rows and may promote only
when capacity remains. Promotion order is `(wake_at, internal id)` among due,
healthy, unclaimed, unadmitted rows.

Already-admitted eligible work is served before promotion. A queued candidate
must come from a contiguous locked and rechecked FIFO prefix. A locked or stale
head may underfill a visit but cannot be bypassed, and no candidate cursor
rotates around it. A permanently runnable admitted cohort may therefore keep
later same-owner work queued; cross-partition rotation is not within-partition
time slicing.

An admitted poison outcome releases its marker. An unadmitted poison candidate
must be the authoritative FIFO head and requires a free admission slot.

### Marker lifetime

The following retain the original marker:

- cooperative drain yield;
- generic immediate claim release;
- claim refresh and reacquisition;
- vehicle replacement; and
- expired-claim steal.

The following clear it atomically with the lifecycle change:

- future scheduling or external waiting;
- terminal completion, failure, or cancellation;
- interruption or another immediate unclaimed signal;
- host-incompatible abandon/backoff; and
- poison.

Waking a previously unadmitted run leaves it queued. Existing claim-token and
checkpoint-sequence fences remain authoritative: a stale holder cannot clear a
newer claim or admission, and transaction rollback persists neither side.

## Authoritative unfinished ring

`docket_claim_schedule` has one immutable-position row per owner partition:

- `scope_key` is the primary and foreign key;
- `ring_position` is a positive, unique, monotonic identity;
- `unfinished_count` exactly counts committed nonterminal `running` or
  `waiting` rows for the scope; and
- the partial ring index contains exactly rows with `unfinished_count > 0`.

The count includes ready work, future timers, externally waiting work, live or
expired claims, and poisoned `running` rows. The ring is therefore an
authoritative superset of current eligibility, not a hint or cache. Dormant
positions may yield unsuccessful inspections, but nonterminal work cannot be
omitted.

A zero-count row remains to preserve its position. Transactional triggers
maintain the counter. Scope and position are immutable; direct counter writes,
underflow, truncation, or deleting a partition with unfinished work fail
closed. There is no reconciliation path.

## Bounded traversal and locking

The ratified logical-work budgets are:

| Name | Value | Boundary |
| --- | ---: | --- |
| `S` | 32 | unfinished-ring inspections per unfilled qualifying call |
| `Q` | 8 | lease or poison outcomes per nonempty partition grant |
| `K` | 16 | structural run IDs admitted to one exact-lock window |
| `M` | 8 | run rows one grant may mutate; equal to `Q` |

One inspection creates at most one grant. One call is bounded by 32 grants,
512 exact-key run-lock attempts, and 256 outcomes or mutation inputs. Caller
demand may lower those ceilings. These are logical-work bounds, not elapsed-
time or query-plan guarantees.

One recursive keyset walk reads the next positive ring position after the
durable cursor and wraps to the first. Filling demand stops immediately.
Otherwise a qualifying call with a nonempty materialized ring consumes all
`S` inspections, including repeated wrap when the ring has fewer than `S`
positions. An empty ring produces no inspection and leaves the cursor
unchanged.

Discovery reads schedule membership through MVCC. Each visit separately tries
exact partition authority, so a partition-lock miss is a committed unsuccessful
inspection. Run locking freezes no more than `K` structural IDs before using
`SKIP LOCKED`; it cannot scan an unbounded locked prefix or substitute structural
row `K + 1`. Every locked row is authoritatively rechecked before mutation, and
one grant mutates at most `Q` rows.

Ready/expired class behavior remains portable: demand one honors its advisory
preference with fallback; demand of at least two reserves an outcome for each
nonempty class before filling remaining demand, and that reservation carries
across partition grants. Poison consumes demand and `Q` but creates no claim
token. Stable age/ID order applies within the choices left by partition rotation
and class reservation.

The lock graph is:

```text
admission: policy + scan cursor -> partition authority -> run rows
lifecycle: run row -> schedule counter
discovery: MVCC read of schedule ring; no schedule-row lock
```

Admission requires a writable Read Committed transaction. Unsupported
transaction modes, engine conflict, or policy-cursor timeout fail closed before
inspection. TenantFair preserves a lower caller `lock_timeout`; otherwise it
caps its waits at 250 ms. A schema-version or validated startup-shape mismatch
blocks startup. A partition-lock miss,
or a visit with no surviving exact run lock or outcome, is an unsuccessful
inspection that advances the cursor. An individual run-lock miss does not
prevent another locked candidate in the same visit from producing a grant.
Rollback persists no cursor, epoch, admission, or outcome work.

The one-statement rule is a client boundary, not a single-snapshot rule. After
locking partition authority, the function's later Read Committed command gets a
fresh snapshot for admitted count and mutation. Partition serialization plus
that fresh recheck protects the final slot across concurrent callers.

## Fixed-window fair-rotation contract

The theorem targets one low-volume partition `t`. A qualification window opens
immediately before the first qualifying TenantFair scan after both of these
facts commit:

1. `t` has positive `unfinished_count` and occupies the unfinished ring; and
2. an authoritative recheck would permit at least one outcome for `t`.

It closes at the database linearization point of `t`'s first committed
nonempty grant.

For one eligible window:

- `P` is the minimal fixed cohort consisting of `t` plus every partition that
  grants before `t`;
- `C` is the fixed, complete, duplicate-free cyclic set of positive
  `unfinished_count` positions and contains `P`;
- only `t` must remain continuously admissible; competitors may be
  intermittently eligible;
- no partition outside `P` grants before `t`; and
- policy, engine, schema/function identity, budgets, and the supplied `L`
  remain fixed.

A zero-to-positive or positive-to-zero ring change, cap or administrative
policy change, engine/schema/function change, loss of target admissibility,
error, or rollback makes the window ineligible. A positive-to-positive
unfinished-count change does not change `C`. An ineligible event never turns a
pending or failed window into a pass.

Target admissibility uses the production mutation rules:

- admitted-ready reacquisition and admitted expired steal are count-neutral
  and remain permitted at or above the cap;
- queued promotion requires the authoritative FIFO head and a fresh admitted
  count below the effective cap;
- admitted poison is permitted and releases its marker;
- queued poison must be the authoritative FIFO head and requires a free slot;
  and
- cap debt excludes queued promotion but not otherwise-valid admitted work.

Loss of partition authority, loss of every viable exact candidate lock, or a
stale/empty mutation race does not redefine the population. It is a failed
target inspection counted by `L`.

### Units

- An **inspection** is one visit to the next positive ring position in durable
  cursor order. When `H < S`, a call may wrap and revisit a position. Lock
  skip, cap denial, dormancy, staleness, or empty recheck is unsuccessful.
- A **grant** is one committed acquisition of partition authority followed by
  `1..Q` committed lease or poison outcomes. Zero outcomes is not a grant.
- An **outcome** is a committed admitted-ready lease, queued-promotion lease,
  expired admitted replacement lease, or poison.
- A **qualifying call** is a committed TenantFair statement that owns and
  advances the domain cursor. Failure before cursor authority, error, or
  rollback contributes no call, inspection, grant, outcome, cursor movement,
  or service epoch.

Let:

- `A = |P|`;
- `H = |C|`, where `1 <= A <= H`;
- `S = 32` inspections per unfilled qualifying call;
- `Q = 8` outcomes per grant, further limited by remaining demand; and
- `L >= 0` be the maximum consecutive complete target inspections that may
  fail before the next target inspection commits a grant.

`L` covers the complete outcome opportunity: partition-lock loss, loss of every
exact candidate lock, and stale/empty mutation races all consume a failed
target inspection. Finite lock hold time alone does not establish a numeric
`L`.

### Scheduler invariants

1. One durable circular cursor is serialized across every poller in the
   fairness domain.
2. Cursor movement is contiguous. Every inspected position advances it,
   including denial, staleness, emptiness, and lock skip.
3. Filling demand stops immediately; otherwise a nonempty qualifying call
   consumes its full `S`, including wrap.
4. Before the first target inspection and between consecutive target
   inspections, each competitor in `P` may receive at most one grant.
5. At most `L` consecutive target inspections fail; the next target inspection
   commits a grant.

`admission_epoch` is outcome-backed service evidence, not the scan cursor. It
advances exactly once per committed nonempty grant, regardless of whether that
grant returns one or `Q` outcomes, and never for cap rejection, staleness,
emptiness, lock skip, error, or rollback. Cursor movement, epoch increment, and
outcomes share one transaction.

### Bounds

Count competitor work strictly after the window opens and strictly before the
target grant:

```text
competing grants   <= (L + 1) * (A - 1)
competing outcomes <= Q * (L + 1) * (A - 1)
```

Count qualifying calls through and including the target-grant call:

```text
qualifying calls
  <= (L + 1) * ((A - 1) + ceil((H - A + 1) / S))
```

The smaller `(L + 1) * ceil(H / S)` expression is not a claim-call bound when
a competitor can fill demand and end a call before the target is inspected.

These formulas bound logical admission units only. Continued polling means
qualifying committed calls continue; it does not imply a millisecond, queue-
wait, completion-time, throughput, CPU, memory, I/O, processing-time, or
unconditional starvation guarantee.

## Recovery boundary

Recovery and fair rotation are separate guarantees. Claim TTL permits another
caller to steal an expired claim but does not revoke sticky admission. Expired
steal, cooperative release, and reacquisition are count-neutral and may remain
valid target opportunities at or above the cap.

These paths preserve claim fencing and retry opportunities. They do not
establish `L`, a queue-wait bound, or wall-clock fairness. The theorem applies
only when its qualification and complete-opportunity assumptions are
independently proven.

## Conditional separation from Legacy

The deterministic control uses ordinary ready work, demand one, and stable
age/ID order. For `N >= 2`, seed one hot partition with `N` older rows, then one
low-volume target row, and complete each outcome before the next call.

Legacy selects all `N` older rows first, so bypass is exactly `N`. On the
equivalent two-partition TenantFair trace with `L = 0`, the hot partition gets
at most one grant or `Q` outcomes before the target grant, independently of
`N`.

This is separation on one eligible frozen trace, not global dominance for
dynamic membership, other class/demand mixes, latency, or throughput.

## Schema, migration, and administration

Schema version 2 installs the engine's policy and partition
authority, stable unfinished ring, marker and partial indexes, lifecycle
triggers, serialized cursor, and the sole seven-argument claim function.

The stopped V1-to-V2 upgrade backfills `tenant_admitted_at = claimed_at` only
for healthy claimed rows. Unclaimed rows remain queued; an over-cap tenant
becomes debt rather than being trimmed. Fresh installs apply V1 and V2 in one
host migration. Custom prefix, failed transactional upgrade, populated
backfill, concurrent first partition creation, and rollback/down to V1 are
release-gated.

The supported rollout is stopped and homogeneous:

1. stop every Docket dispatcher and run writer;
2. apply the generated transactional migration;
3. deploy one homogeneous application version and engine configuration; and
4. restart processing.

Startup requires schema version 2 plus the validated marker-column and sole
seven-argument function-signature shape.
Online migration, mixed old binaries, readiness ledgers, activation ceremonies,
and audited mode history are outside v0.1.0.
A binary that predates the engine interlock cannot be made safe by new database
code alone.

The Admin surface provides `get_default/1`, `put_default/2,3`,
`put_override/3,4`, `reset_override/2,3`, and `get_effective/2`. Writes accept an
optional `:expected_version` CAS value; caps must be integers in
`1..2_147_483_647`. Effective reads expose token-free `queued`,
`admitted_ready`, `admitted_claimed`, and `debt` counts.

## Observability and performance evidence

Production emits identity-free aggregate ClaimPolicy, run-store claim,
attempt, poison, and bounded admission-release telemetry. Singleton policy-
cursor contention is the only proven `:policy_cursor` contention phase.
Tenant, scope, run, graph, cursor, and claim-token identity are forbidden as
ordinary metric labels.

Production invokes the claim function with trace mode disabled and filters all
inspection rows/internal columns before decoding public outcomes. Trusted
direct inspection may explicitly enable the identity-bearing raw trace.
Aggregate telemetry cannot reconstruct per-target bypass.

Timing percentiles, query plans, and the PostgreSQL scorecard may detect
regressions but cannot satisfy fairness correctness. See the
[telemetry guide](../telemetry.md) and [benchmark guide](../benchmarks.md) for
their operational contracts.

The scorecard runs its required-tenancy fairness scenario only with TenantFair.
Claim ceiling and fast/slow scenarios run both registered policy variants.

## Correctness evidence

The repository includes implementation-level evidence for:

- ring, cursor, class, budget, and epoch mechanics in
  [`tenant_fair_ring_test.exs`](https://github.com/water-cooler-ai/docket/blob/2638bb15bc44f4920f6d40b219f4046651a0359c/test/docket/postgres/tenant_fair_ring_test.exs);
- the demand-aware formulas and trace-oracle invariants in
  [`fair_rotation_oracle_test.exs`](https://github.com/water-cooler-ai/docket/blob/2638bb15bc44f4920f6d40b219f4046651a0359c/test/docket/postgres/fair_rotation_oracle_test.exs);
- cap-two/cap-ten identities, FIFO, debt, steal, poison, interlock, and rollback
  in [`claim_policy_tenant_fair_test.exs`](https://github.com/water-cooler-ai/docket/blob/2638bb15bc44f4920f6d40b219f4046651a0359c/test/docket/postgres/claim_policy_tenant_fair_test.exs);
- Admin CAS/effective counts, engine selection, startup shape, and supervised
  admission in
  [`claim_policy_admin_test.exs`](https://github.com/water-cooler-ai/docket/blob/2638bb15bc44f4920f6d40b219f4046651a0359c/test/docket/postgres/claim_policy_admin_test.exs),
  [`claim_policy_test.exs`](https://github.com/water-cooler-ai/docket/blob/2638bb15bc44f4920f6d40b219f4046651a0359c/test/docket/postgres/claim_policy_test.exs),
  [`backend_test.exs`](https://github.com/water-cooler-ai/docket/blob/2638bb15bc44f4920f6d40b219f4046651a0359c/test/docket/postgres/backend_test.exs), and
  [`dispatcher_test.exs`](https://github.com/water-cooler-ai/docket/blob/2638bb15bc44f4920f6d40b219f4046651a0359c/test/docket/postgres/dispatcher_test.exs);
- marker lifecycle in
  [`run_store_test.exs`](https://github.com/water-cooler-ai/docket/blob/2638bb15bc44f4920f6d40b219f4046651a0359c/test/docket/postgres/run_store_test.exs);
- fresh/populated/prefix/rollback/concurrent-creation migration paths in
  [`migration_test.exs`](https://github.com/water-cooler-ai/docket/blob/2638bb15bc44f4920f6d40b219f4046651a0359c/test/docket/postgres/migration_test.exs).

CI runs the ordinary, core-only, PostgreSQL 13, and PostgreSQL 17 test jobs.
Timing, randomized soak, and scenario count remain regression evidence rather
than proof of the fixed-window assumptions.
