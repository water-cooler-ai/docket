# Docket Tenant Claim Fairness

Status: implementation design for the PostgreSQL durable runtime after `0.1.0`;
the identity, vocabulary, instance-configuration, and bounded observation
contracts are locked, while the TenantFair engine and its schema remain
unimplemented.

This document turns the tenant-fairness roadmap item into an implementation
plan. The migration version and later query-tuning values remain provisional.

## Summary

Docket will provide moderate, database-wide fairness for detached durable runs
by treating each tenant as a scheduling partition. The scheduler will:

- bound the number of live claims a tenant may hold;
- select work fairly across tenants before selecting the next run within a
  tenant;
- support database-resident, dynamically updated limits for product tiers;
- distinguish a tenant's preferred concurrency, absolute ceiling, and relative
  weight;
- let otherwise-idle capacity be borrowed without preempting a node halfway
  through execution;
- preserve the existing run-row queue, claim-token fence, orphan recovery,
  poison, and bounded-drain contracts.

The first implementation should deliver strict dynamic claim caps and fair
partition selection. Weighted service accounting and borrowing should follow
as separately measured slices. This sequencing keeps exact safety enforcement
small before adding work-conserving policy.

## Why claims are the fairness unit

Docket's durable `:running` status is not an execution state. It includes
ready, future-scheduled, claimed, and poisoned runs. Counting all running rows
would make sleeping retries and scheduled timers consume tenant capacity.

A **live claim** is the initial fairness unit because it represents a run that
currently owns execution authority and normally occupies a vehicle slot. A
claim starts when `claim_due/3` installs a token and ends when a fenced commit,
release, cancellation, poison transition, or successful steal replaces or
clears that authority.

An expired claim still counts as one live claim until another claimant steals
it. Expiry alone does not revoke its token. Stealing an expired claim replaces
an existing allocation and must not consume an additional tenant slot.

Claim-count fairness is intentionally narrower than resource fairness. One run
may execute more parallel tasks or consume more CPU, memory, I/O, or external
service time than another. Later service accounting can improve scheduling,
but the first release should be described precisely as **tenant claim
fairness**.

## Trusted partition identity

The fairness partition is the persisted `docket_runs.scope_key`. Docket derives
that value only from the owner scope resolved by the configured runtime:

- `tenant_mode: :required` requires a non-empty `tenant_id` operation option
  and resolves it to `{:tenant, tenant_id}`;
- `tenant_mode: :none` resolves public operations to `:tenantless`; and
- PostgreSQL stores the resolved owner as `tenant_id` and generates
  `scope_key`, using the tenant ID for tenant-owned rows and the empty string
  for tenantless rows.

The host is responsible for authenticating and authorizing the principal from
which it supplies `tenant_id`; Docket cannot establish that application-level
trust. Once the host calls Docket, however, the scheduling identity is not
caller-extensible. Run input, graph fields, node output, checkpoint state,
event payloads, execution context, tier names, and other run-controlled data
cannot select or override `scope_key`. ClaimPolicy implementations read the
persisted generated value and never re-derive a partition from a run document
or payload.

The tenantless empty string is one real scheduling partition, not a wildcard or
an absence of policy. Empty tenant IDs are invalid, so it cannot collide with a
tenant-owned partition. A product tier may resolve numeric policy for many
tenants but is never itself a partition key.

The fairness domain is one physical PostgreSQL database and resolved table
schema. Every dispatcher that addresses those same prefixed Docket tables
coordinates over the same `scope_key` partitions. `prefix: nil` follows the
connection `search_path`; operators must account for the possibility that it
resolves to the same physical schema as an explicit prefix such as `"public"`.

## Goals

1. A hot tenant cannot occupy every dispatcher vehicle while another tenant
   has eligible work.
2. Tenant caps are enforced across all dispatchers sharing one PostgreSQL
   database and prefix, not independently per BEAM node.
3. A tenant with a deep backlog cannot hide another tenant's first eligible
   run.
4. Product tiers can change a tenant's policy without restarting Docket.
5. Selection is stable within a tenant by eligibility time and run ID.
6. Once capacity becomes admissible, a deep tenant backlog cannot hide another
   eligible tenant.
7. An idle fleet can remain work-conserving when borrowing is enabled.
8. Vehicles continue to hold no database connection while node code runs.
9. Tenant identity never becomes a telemetry label.
10. Existing scope checks, claim fencing, expiry, poison, retry, cancellation,
    and terminal transitions remain authoritative.

## Non-goals

- Strict global FIFO ordering across tenants.
- A bounded queue-latency or completion-time SLA.
- Preempting a node or superstep midway through execution.
- Equal CPU, memory, I/O, task-count, or monetary cost in the first release.
- A queue, process, or worker pool per tenant.
- Using a product tier as the partition key. All tenants on a `pro` tier must
  not share one scheduling partition.
- Replacing producer admission control, rate limiting, autoscaling, or backlog
  management.
- Supporting arbitrary partition expressions before `tenant_id` has proven
  the storage and scheduling contract.
- Guaranteed minimum concurrency, throughput, start latency, or physical
  resource isolation. Those require an explicit fleet-capacity reservation or
  preemption model that Docket does not currently have.

## Fairness vocabulary

The following terms and units are normative for implementation, telemetry, and
SLO work:

| Term | Operational definition | Unit |
|------|------------------------|------|
| claim | Execution authority represented by a non-null claim token on one running, non-poisoned run row. Installing a token starts a claim; a fenced clear, terminal/cancel transition, poison transition, or token-replacing steal ends that particular authority. | event |
| active claim / live claim | A `status = 'running'`, non-poisoned run with a non-null claim token at the observation instant. Expiry alone does not remove it; a successful steal replaces one active claim with one active claim. | claims |
| active claim residency | Time from token installation until that token is cleared or replaced, measured with database timestamps. | microseconds |
| preferred concurrency / `preferred_active` | The active-claim threshold below which an eligible partition receives preferred admission. It is neither reserved capacity nor a minimum guarantee. | claims |
| hard cap / `max_active` | The exact maximum active claims additive ready admission may leave in one partition. Expired replacement does not add a claim. | claims |
| weight | A positive integer relative-service coefficient. Ratios are meaningful only among continuously eligible partitions in the same active set; implementations scale fractional host ratios to a common integer base. | dimensionless integer |
| entitlement | A partition's normalized target service share within an active set: `weight / sum(active weights)`. Before weighted service is enabled, entitlement means only preferred-before-borrowed admission and does not imply a numeric share guarantee. | ratio in `[0, 1]` |
| borrowing | An active-claim count above `preferred_active` and at or below `max_active`, admitted only when borrowing is enabled and no preferred admission is hidden. | claims; classification is boolean per claim at admission |
| claim wait | For a ready claim, database `claimed_at - wake_at`. Expired recovery wait is reported separately from the expiry boundary and is not mixed into ready claim wait. | milliseconds |
| concurrency share | A partition's active claims divided by total active claims in the fairness domain. Windowed concurrency share is the time integral of that ratio divided by window duration. | ratio in `[0, 1]` |
| processing-time share | A partition's measured service time divided by total measured service time for the same signal and window. The signal must be named, initially claim-residency or executor-task time; the two are never combined implicitly. | ratio in `[0, 1]` |
| service skew | `observed processing-time share - entitlement` for one partition over a stated active-set window. Positive values mean excess service and negative values mean deficient service; aggregate reports may use the maximum absolute value. | percentage points |
| bounded reclaim lag | From a previously quiet eligible partition crossing below `preferred_active` until its next preferred admission, conditional on capacity turnover. A numeric upper bound exists only when maximum residual slice duration, poll delay, and admission-transaction delay are all bounded. | milliseconds |

The policy deliberately separates three controls that are often conflated:

- `preferred_active` is the tenant's preferred non-borrowing threshold. It is
  an admission preference, not a reservation, guarantee, or held-aside fleet
  capacity. A tenant is borrowing when it holds more authoritative claims than
  this value.
- `max_active` is the tenant's absolute live-claim ceiling. It is never
  exceeded by a new ready claim, even when the fleet is otherwise idle.
- `weight` is the tenant's relative service share while multiple tenants
  contend. A weight of four should receive approximately four times the
  service of a continuously backlogged weight-one tenant over a sufficiently
  long interval, subject to integer slots and indivisible execution slices.

This distinction allows product tiers such as:

| Tier       | `preferred_active` | `max_active` | `weight` |
|------------|-------------------:|-------------:|---------:|
| Free       |                  1 |            2 |        1 |
| Pro        |                  5 |           10 |        4 |
| Enterprise |                 20 |           50 |       16 |

These numbers are examples, not Docket defaults. The host application owns
the mapping from plans to resolved numeric policy.

## Instance configuration contract

Tenant fairness remains opt-in for compatibility. ClaimPolicy is an
instance-selected boundary, so fairness configuration belongs to the selected
implementation at the top level, not inside dispatcher mechanics:

```elixir
use Docket,
  backend: Docket.Postgres,
  tenant_mode: :required,
  dispatcher: [concurrency: 100],
  claim_policy: [
    implementation: Docket.Postgres.ClaimPolicy.TenantFair,
    partition_by: :tenant_id,
    default_preferred_active: 2,
    default_max_active: 2,
    default_weight: 1,
    borrowing: false
  ]
```

This is the locked configuration shape, not a claim that TenantFair is already
selectable. The source-owned configuration value is validated independently;
the later hard-cap phase adds the selectable engine and schema. Merely accepting
the configuration must not silently run Legacy under a TenantFair name or imply
that caps, fair rotation, weights, or borrowing are active.

Initial validation rules:

- `partition_by` is required and must be `:tenant_id`.
- `default_preferred_active`, `default_max_active`, and `default_weight` are
  required. Their example values are not library defaults.
- preferred and maximum values are integers in `0..2_147_483_647`, matching
  the future PostgreSQL signed `integer` columns.
- `preferred_active <= max_active`.
- `weight` is an integer in `1..2_147_483_647`. Integer weights avoid
  inconsistent floating-point ordering between SQL and application code.
- `borrowing` is boolean and defaults to `false`.
- `max_active > preferred_active` remains valid when borrowing is disabled;
  the excess capacity is dormant until borrowing is enabled.
- Fairness may be enabled in `tenant_mode: :none`; all tenantless runs then
  form one partition. It is useful mainly so one configuration shape can be
  tested in tenantless deployments.

The existing top-level dispatcher `concurrency` remains the per-runtime
vehicle ceiling. With several runtime instances, their aggregate concurrency
can exceed that value. Tenant claim limits are database-wide within one
physical `(database, schema, scope_key)`. An explicit schema prefix is the
namespace boundary; `prefix: nil` follows the connection `search_path` and may
refer to the same tables as an explicit `"public"` prefix.

Because Docket does not coordinate or reserve a database-wide total vehicle
pool, the sum of tenant `preferred_active` values may exceed available fleet
capacity. The setting therefore cannot be sold or documented as reserved
concurrency.

### Configuration integration

The public option surface is a closed whitelist. Top-level `:claim_policy` is
already an instance-owned option; `Docket.Postgres.context/1` passes its
implementation options to the selected implementation's `init/2` exactly once.
TenantFair owns the closed nested key set and range/relationship validation.
Unknown, duplicate, or invalid values fail context construction before backend
children start.

The resolved ClaimPolicy value is then reused unchanged by dispatcher polling,
manual and inline drain, recovery, and transaction-scoped contexts. Per-call
options cannot replace it. No `:claim_partitions` dispatcher key or second
effective-policy constructor is added. Integer-only weights represent
fractional ratios by scaling every weight to a common base.

Legacy remains the default when `:claim_policy` is omitted. Configuration
acceptance, engine selection, and database policy activation are separate
rollout steps; a partially deployed fleet must never interpret the same
database policy with instance-local fallback defaults.

## Database model

A migration adds library-owned policy and partition tables:

```text
docket_claim_policy
  id                    integer primary key, fixed value 1
  preferred_active      integer not null
  max_active            integer not null
  weight                integer not null
  borrowing             boolean not null
  policy_version        bigint not null
  updated_at            timestamptz not null
```

This prefix-local singleton is the authoritative default policy for every
dispatcher sharing the tables. Runtime configuration supplies the desired
bootstrap policy, but a dispatcher must not silently substitute its own
defaults after the database row exists. Startup should create or verify the
row and reject a mismatched policy fingerprint/version rather than allowing
mixed rolling-deploy semantics.

```text
docket_claim_partitions
  scope_key             text primary key
  preferred_active      integer null
  max_active            integer null
  weight                integer null
  borrowing             boolean null
  state                 text not null default 'running'
  override_version      bigint not null default 0
  last_claimed_at       timestamptz null
  next_ready_at_hint    timestamptz null
  next_expired_at_hint  timestamptz null
  last_policy_event_id  text null
  inserted_at           timestamptz not null
  updated_at            timestamptz not null
```

`scope_key` uses the same canonical representation already stored on run rows:
the empty string represents tenantless ownership, and a non-empty tenant ID
represents a tenant. Empty tenant IDs are already invalid, so the two forms do
not collide.

Nullable override columns mean "use the database default." This permits the
host to reset an override without deleting scheduling history or racing a run
insert. Effective policy is resolved inside the claim transaction with
`COALESCE(partition_override, database_default)`. Resolving a nullable value
against per-BEAM configuration is forbidden: two versions in a rolling deploy
could otherwise enforce different ceilings despite correctly serialized row
locks.

The table stores no tier or billing-plan name. Docket consumes only resolved
execution policy. This prevents the runtime from becoming coupled to the
host's subscription model.

### Partition-row lifecycle

- The migration backfills one row for every distinct run `scope_key`.
- Starting a run uses `INSERT ... ON CONFLICT DO NOTHING` to idempotently
  create its partition row before inserting the run, in the same outer
  transaction. Concurrent first runs may briefly contend on the unique insert
  but do not take the long-lived admission lock path.
- An operator may create or update a policy before that tenant starts a run.
- Partition rows are not deleted through the normal policy API. Resetting all
  override columns to `NULL` returns a tenant to defaults while preserving its
  scheduling cursor.
- Ready/expired timestamps are recoverable liveness hints, not authority.
  Insert and claim paths may update them synchronously when they already hold
  the partition-first lock order. Run-first commit/release/refresh paths emit
  an asynchronous repair signal and never acquire the partition row afterward.
  Periodic reconciliation repairs lost signals and stale hints.
- A foreign key from `docket_runs.scope_key` to the partition table should use
  `ON DELETE RESTRICT` if it can be introduced without weakening the existing
  graph/run composite constraints.
- The online migration order is: create tables, seed the database default,
  backfill distinct scope keys, add the foreign key as `NOT VALID` where
  appropriate, then validate it separately before enabling fair claims.
- Large installations batch the distinct-scope backfill and build new run
  indexes concurrently where PostgreSQL migration constraints permit.
- `RunStore.insert_run/5` itself must make partition upsert and run insertion
  atomic, including direct store calls outside the ordinary lifecycle. If a
  nested transaction is not used, the weaker allowed outcome—harmless orphan
  partition rows—must be explicit and tested.

Version 1 accepts dormant partition-row accumulation bounded by historical
tenant cardinality; the existing pruner does not delete these rows. Inspection
must expose their count. A later GC may remove an unreferenced dormant row only
after a safety window and with a defined reset of rotation/active-set history.

### Run indexes

Keep the existing global ready and expired indexes until query plans show they
are unnecessary. Add tenant-leading partial indexes for partition-local
selection and live-claim accounting:

```sql
(scope_key, wake_at, id)
WHERE status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NULL
  AND wake_at IS NOT NULL

(scope_key, claimed_at, id)
WHERE status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NOT NULL
```

The second index supports both expired-candidate selection and bounded counts
of authoritative live claims for a tenant.

## Dynamic tenant policy

The PostgreSQL backend should expose a trusted operator API conceptually like:

```elixir
Docket.Postgres.put_tenant_claim_policy(context, tenant_id,
  preferred_active: 5,
  max_active: 10,
  weight: 4,
  borrowing: true,
  expected_version: 7,
  event_id: "subscription-event-123"
)

Docket.Postgres.reset_tenant_claim_policy(context, tenant_id)
```

Exact placement and naming remain an implementation decision. This is an
administrative control-plane operation, not a tenant-scoped data-plane call.
The host must authorize it before invoking Docket.

A subscription handler can translate a plan change into a versioned,
idempotent compare-and-swap:

```text
billing event -> host tier mapping -> resolved numeric policy -> versioned CAS
```

The database row is the claim-time source of truth. An Elixir callback, cache,
or external billing lookup must not be the authoritative admission check:
separate BEAM nodes can observe different cached values, an external call
would enter the claim hot path, and a callback cannot atomically coordinate
concurrent claimers.

The control plane must also record a monotonic version, source event ID,
effective timestamp, and actor/source audit record; reject stale events; apply
bounded safety rails; and offer a dry-run/effective-policy read. Policy
transactions must remain short so `SKIP LOCKED` claimers do not repeatedly
skip one tenant.

Policy changes use the same partition-row lock as claim admission:

- an upgrade is visible to the next claim after the update commits;
- a downgrade does not preempt existing work;
- while current live claims are at or above the new ceiling, no additional
  ready run is claimed;
- `max_active: 0` is a numeric ban on additive ready claims, not a complete
  administrative pause;
- expired-claim recovery remains possible because a steal replaces, rather
  than adds, one live claim.

Administrative state is separate from quota:

- `running`: normal ready admission and recovery.
- `hold_new`: no additive ready claims; existing claims and expired recovery
  continue.
- `drain`: no additive ready claims or expired steals; existing holders finish
  and release through normal bounded runtime behavior. This is non-preemptive
  unless a later execution-control contract adds cooperative cancellation.

State transitions do not replay, cancel, or mutate graph state. Their exact
interaction with retained claims and manual drains must be covered by the
storage contract before the operator API ships.

The tenant partition key must be derived by the trusted host from an immutable
account or billing principal, not accepted as an arbitrary caller-selected
fairness identity. Otherwise one customer can create many tenant IDs to obtain
many caps and scheduling turns. Bound key length, partition creation rate, and
the number of partitions attributable to one higher-level account.

## Atomic claim enforcement

Enforcement belongs in `RunStore.claim_due/3`. Dispatcher-local vehicle counts
still determine how many leases that dispatcher can accept, but they cannot
enforce a database-wide tenant cap.

One claim statement or short transaction performs the following work:

1. Determine dispatcher demand from the supplied `policy.limit`.
2. Discover eligible tenant partitions without allowing the first tenant's
   backlog to consume the entire candidate window.
3. Rank a bounded set of partition keys by fairness state, then the
   partition's oldest eligible row and stable scope key. This discovery is a
   hint, not an authority decision.
4. Re-sort the chosen keys by ascending `scope_key` and lock their partition
   rows using `FOR NO KEY UPDATE OF partition SKIP LOCKED`.
5. Resolve each locked partition's effective policy.
6. Count its current authoritative live claims.
7. Select expired claims eligible for steal. A steal replaces an allocation
   and does not require a free tenant slot.
8. Compute ready capacity as
   `max(effective_max_active - live_claims, 0)`.
9. Select at most that many ready runs for the tenant, ordered by
   `(wake_at, id)`.
10. Interleave selected partitions so each receives one outcome before a
    second outcome is assigned, subject to weight and available demand.
11. Lock run candidates with `FOR UPDATE SKIP LOCKED` and apply the existing
    token/attempt/poison update.
12. Advance partition scheduling state using a database-authored logical round
    or timestamp only for partitions whose final `RETURNING` produced an
    actual lease or poison outcome.
13. Return leases and poisoned outcomes through the existing claim batch.

The load-bearing invariant is:

> No additive ready admission or expired steal for a partition may occur
> without first holding that partition's row lock.

No future fast path, including expired recovery, may bypass it.

The locked-partition CTE is the serialization proof, not just an ordering
suggestion: every live-count and ready-selection lateral query must depend on
the locked row. At PostgreSQL `READ COMMITTED`, a statement uses a start
snapshot. A competing additive claimant is safe because it cannot acquire the
same partition lock and is skipped; a design that waits for the lock and then
counts from its older statement snapshot is unsafe.

`FOR NO KEY UPDATE` conflicts with policy and claim admission while remaining
compatible with the `KEY SHARE` lock a child insert may take for the proposed
foreign key. `FOR UPDATE` would unnecessarily block new-run inserts.

All transactions that need both partition and run locks acquire them in the
same order: ascending partition `scope_key`, then ascending run ID. Discovery
may rank by fairness, but the bounded chosen set is re-sorted before locking.
Bulk policy operations do the same. Commit and release paths must not
introduce the inverse run-then-partition ordering when weighted accounting is
added.

`SKIP LOCKED` means a dispatcher that loses one tenant's partition lock can
continue serving other tenants. It also means exact global FIFO is neither
provided nor desired. A persistently locked policy row can be skipped
indefinitely, so transactions are bounded and telemetry tracks consecutive
skip count and oldest skipped age. No starvation bound is claimed without a
separate escalation mechanism.

PostgreSQL does not allow a locking clause at the same query level as
`DISTINCT` or window functions. Candidate-head discovery therefore occurs in
an unlocked CTE/subquery, followed by a simple join to partition rows and the
locking clause. Eligibility is rechecked when actual run rows are updated.

### Candidate discovery

The current global `LIMIT demand` scans can return only rows from a hot tenant.
The fair query must choose partition heads before filling additional tenant
slots, but it must not rank every queued run on each poll.

`ROW_NUMBER() OVER (PARTITION BY scope_key)` and global `DISTINCT ON` are
benchmark baselines, not production candidates. They inspect the eligible
backlog and exhibit both poor scaling and a concurrency trap: a first CTE can
rank rows that another poller already locked, after which a second
`SKIP LOCKED` CTE returns fewer or zero outcomes while other eligible work
exists. PostgreSQL 18 B-tree skip scan does not remove this high-cardinality
tenant-head problem; it is most effective with few distinct leading values.

The expected production shape is:

1. Use the partition rotation cursor plus recoverable
   `next_ready_at_hint`/`next_expired_at_hint` to scan a bounded, oversampled
   page of likely eligible partition rows rather than enumerate the run
   backlog.
2. Lock the chosen partitions in `scope_key` order with
   `FOR NO KEY UPDATE SKIP LOCKED`.
3. For each locked partition, use tenant-leading indexes and bounded `LATERAL`
   queries to validate and fill its actual ready/expired candidates.
4. Continue through bounded cursor pages until dispatcher demand is met, the
   query work budget is exhausted, or no candidate partitions remain. Do not
   stop merely because the first ranked page was locked.
5. Repair stale hints from actual query results. Use a recursive-CTE loose-scan
   over the tenant-leading index as a reconciliation/fallback candidate and
   benchmark it against the hint path.

This deliberately pays write/repair complexity to keep the claim hot path
bounded. The hints may create conservative delay but never authorize a claim;
periodic fallback discovery is required so a lost update cannot strand work.

The implementation must include adversarial plans with one tenant holding
thousands of oldest rows and another holding one recent row, and concurrency
tests where several pollers rank the same first page while additional eligible
partitions exist.

Use explicit `last_claimed_at ASC NULLS FIRST` if the first release keeps a
timestamp cursor; PostgreSQL's default ascending null order would put
never-served partitions last. Use a database timestamp rather than a
dispatcher clock, and define how all rows selected in one batch advance. A
logical round/sequence is preferable if timestamps cannot express stable
rotation cleanly. Dispatcher jitter or a weighted-random scan phase should
reduce every poller racing the same hot partition; fairness is evaluated as
bounded service skew over a window, not exact per-poll rotation.

### Ready versus expired progress

The current claim policy prevents ready and expired candidate classes from
starving each other. Tenant partitioning must preserve that property:

- at batch demand of two or more, serve both non-empty classes when possible;
- at demand one, retain the existing alternating class preference;
- an expired claim at a tenant ceiling remains recoverable;
- poisoning an exhausted expired claim clears an existing allocation rather
  than consuming a new one.

Data-modifying CTEs share one snapshot and cannot observe sibling changes
except through `RETURNING`. If poisoning expired claims should immediately
free ready capacity in the same statement, capacity must be derived from that
`RETURNING` result. The simpler first release may conservatively underfill and
recover the freed slot on the next poll; it must document and test that choice.

Fairness is applied first across tenants and then reconciled with class
progress. Neither dimension may be implemented with a global candidate limit
that hides the other.

## Strict-cap scheduling: first release

The first implementation slice uses:

- exact `max_active` enforcement;
- `borrowing: false`;
- one partition outcome per fair round before additional outcomes;
- `last_claimed_at`, oldest eligibility time, and `scope_key` as stable
  partition ordering inputs;
- FIFO-like `(eligible_at, run_id)` ordering within a tenant.

This produces moderate round-robin fairness without promising proportional
resource consumption. `weight` and `preferred_active` may be stored and
exposed in this slice, but must be documented as inactive until their
algorithms are enabled.

## Active-set weighted service scheduling

Round-robin claims are unfair when execution costs vary, but a lifetime
`virtual_service / weight` counter is not a correct solution. A new tenant
would begin at zero and could monopolize admissions while catching up; a
long-idle tenant could re-enter with stale history; and changing a weight would
retroactively reinterpret all prior service.

Phase 3 therefore requires a separate algorithm specification before `weight`
is activated. At minimum it must define:

- the fairness domain, initially continuously backlogged tenants during a
  contention interval;
- a system virtual-time or bounded recent-service model;
- idle-to-backlogged placement, such as
  `tenant_vruntime = max(tenant_vruntime, system_min_vruntime)`;
- system-idle behavior;
- tenant arrival, departure, and reactivation;
- weight changes that affect future service without reweighting history;
- admission-time charging followed by actual-cost reconciliation, so a large
  batch of uncharged expensive claims cannot all appear cheapest;
- an unfairness bound or measured error stated as a function of maximum slice
  cost, claim batch size, and aggregate vehicle concurrency.

The algorithm may draw from start-time fair queueing, stride scheduling,
decayed recent usage, or two-dimensional fair queueing, but Docket must not
claim classical WFQ/DRR guarantees directly. Those analyses assume different
server and work-size models from a concurrent fleet executing unknown-cost
claims.

Service should be charged using observed bounded work, not a tenant's
self-reported estimate.

Candidate cost signals, in increasing fidelity, are:

1. one unit per claim outcome;
2. committed moments or supersteps;
3. claim-residency microseconds;
4. executor task runtime;
5. normalized multi-resource consumption.

The first metric must name the resource objective. Claim-residency time
approximates **vehicle occupancy**, not CPU or monetary cost; it can charge
remote waits and overlap stale execution after steal. Executor task time
better represents parallel executor consumption but requires trustworthy
cross-executor reporting. The implementation should gather both before
choosing the policy input.

Accounting must define:

- whether retry/abandon time is charged;
- how claim refreshes split one long residency into bounded charges;
- how crashed claims are charged before or during steal;
- active-set re-entry and state renormalization;
- a single partition/run lock order for atomic updates.

Do not add lifetime `virtual_service` to the first migration. The later
algorithm adds only the state it proves necessary, such as tenant vruntime,
system minimum virtual time, last service time, and an algorithm version.

Service charging is deliberately asynchronous. Existing commit, refresh,
release, and abandon paths update run rows directly; making them update the
partition afterward would create the default inverse of admission's
partition-then-run lock order. Those paths therefore publish idempotent usage
facts to an async aggregator and never touch the partition policy/cursor row.
The aggregator locks only partition/accounting state and applies future
selection preference eventually. Lost or duplicate facts are detected through
durable sequence/idempotency keys and reconciliation.

This makes the weighted preference approximate while leaving `max_active`
exact. Measure false preference, convergence, and starvation explicitly. The
async path relieves commit/accounting contention; it does not relieve the
load-bearing admission lock on a hot partition. Dispatcher jitter, randomized
scan phase, bounded lock batches, and short transactions mitigate that
contention.

## Borrowing and burst capacity

Borrowing is work-conserving use of capacity between `preferred_active` and
`max_active`. It is not permission to exceed `max_active`.

Admission runs in two logical phases:

1. **Preferred phase:** prioritize eligible tenants below `preferred_active`,
   ordered by the active-set weighted policy once available, or the fair
   rotation cursor before then.
2. **Borrowing phase:** if dispatcher capacity remains and no preferred claim
   is hidden, admit tenants above preferred but below `max_active`, using the
   same ordering.

These need not be two literal SQL passes. With one outcome per tenant per
round, a single ordering key—below-preferred flag first, then active-set score
and stable tie-breakers—can express both phases while the absolute ceiling
remains a filter.

A borrowed claim should be identifiable in the lease and durable operational
state, for example with a `claim_class` of `preferred` or `borrowed`. Durable
classification preserves the admission decision for audit and recovery, but
it is historical: current borrowed usage is derived from current policy and
claim count because later releases or policy changes can reclassify the live
allocation.

Borrowed execution is not interrupted halfway through a superstep. When
another tenant becomes eligible, new preferred work wins future admissions;
existing borrowers return capacity only when an execution slice releases.
Claim expiry and fencing revoke database authority but do not themselves stop
a stale process from consuming physical resources.

Only promise bounded reclamation if Docket enforces a hard wall-clock release
or cooperative-cancellation bound for every borrowed slice. Under that
assumption:

```text
reclamation delay <= maximum residual slice duration
                   + bounded preferred rounds
                     * (partition lock/discovery bound
                        + claim poll delay
                        + admission transaction delay)
```

The exact population and formula are `RECLAIM-1` below. Without every term and
the database-authored preferred-epoch sequence, there is no numeric or eventual
liveness guarantee: the policy merely prefers eligible work at future
qualifying admissions after actual capacity release. Finite attempt and drain
settings must have documented numeric bounds before they support a reclamation
SLO.

Borrowing is a global scheduling mode plus a per-tenant permission to exceed
`preferred_active`; unused preferred capacity is not owned or reserved, so
there is no separate lending entitlement in this model. `borrowing: false`
stops that tenant at `preferred_active`. Validate contradictory combinations
and document that `max_active > preferred_active` is unused while borrowing is
disabled.

Borrowing ships only after strict caps are correct and tests demonstrate its
actual reclamation behavior under continuous contention.

## Interaction with lifecycle transitions

Fairness changes scheduling, not graph semantics.

- **Immediate or future release:** clearing a claim immediately frees one
  tenant slot. No separate active counter needs decrementing.
- **Retain claim:** the run continues to consume the same slot. Bounded drain
  and attempt limits prevent indefinite residency.
- **Expired steal:** token replacement keeps the tenant's live-claim count
  unchanged, but the stale and replacement vehicles may physically overlap
  until the stale process stops or reaches another fence.
- **Poison:** clears the claim and frees the slot.
- **Cancellation or terminal commit:** clears the claim and frees the slot.
- **External waiting:** has no live claim and consumes no tenant slot.
- **Retry parking or timer scheduling:** has no live claim and consumes no
  tenant slot.
- **Vehicle launch failure:** the existing fenced release returns capacity.
- **Dispatcher shutdown:** unlaunched leases are released; launched vehicles
  drain under the existing supervisor contract.

The run rows remain authoritative. The partition table must not introduce an
`active_count` in the first release. If a future performance study requires a
counter, it must be reconstructable from live run claims and repaired by an
idempotent reconciliation operation.

Accordingly, `max_active` is an exact ceiling on current authoritative claims,
not necessarily on simultaneously executing processes, CPU use, executor
tasks, downstream calls, or cost. A true physical concurrency guarantee would
require acknowledged revocation or cooperative cancellation in addition to
claim fencing.

## Backlog and admission protection

Fair claiming protects execution opportunity but does not prevent one tenant
from creating unbounded run rows, index growth, vacuum pressure, retention
cost, or expensive partition-head discovery. Initial production scope must
therefore expose, per trusted tenant identity:

- ready and total nonterminal run depth;
- oldest eligible age;
- run-creation rate;
- capped/held admission state;
- a host hook or explicit result for producer backpressure.

Optional queued-run and enqueue-rate limits are separate from `max_active` and
must define whether start requests are rejected, delayed, or accepted under a
host-owned policy. Also protect the database against correlated demand from
many individually compliant tenants; per-tenant limits alone are not global
overload control.

Partition rows need a dormant retention/compaction policy. Any history reset
must preserve active-set fairness semantics and cannot delete a row referenced
by a run. High-cardinality and Sybil resistance belong in the first production
threat model, even if hierarchical scheduling ships later.

## Manual and inline testing modes

`Docket.Postgres.drain_runs/2` must use the same effective tenant policy as the
supervised dispatcher. It may claim one run per loop for deterministic tests,
but it must not bypass a tenant cap or reset fairness state. A test-only bypass
would make production behavior impossible to reproduce.

The in-memory test backend should implement the substrate-neutral selection
contract where practical. PostgreSQL-specific race, lock, and query-plan tests
remain in the PostgreSQL suite.

## Inspection and operator controls

Add a trusted, bounded inspection projection for one partition:

```text
scope / tenant
effective preferred_active
effective max_active
effective weight
borrowing enabled
administrative state
authoritative claims
actively executing vehicles
expired/unrecovered claims
currently above preferred count
ready runs (bounded or approximate if documented)
oldest ready age
last claimed time
policy version
policy source: default | override
```

This is an operator API, not an unscoped tenant enumeration API. Hosts can
expose a subset to customers, such as current active claims, limit, and oldest
queued age.

Useful controls include:

- update or reset policy;
- hold new admission or drain through explicit administrative state;
- list partitions currently capped, with bounded pagination;
- reconcile any future denormalized accounting;
- observe a downgrade that is temporarily over its new ceiling.

## Telemetry

TenantFair uses the facade-owned
`[:docket, :postgres, :claim_policy, :admission, :observation]` event. A plan
declares the closed `:tenant_fair_v1` schema and a successful decoder returns
one complete summary, including for a no-op. Legacy does not opt in, the
existing generic admission event remains exact, and no implementation-supplied
metadata is copied to the new event.

Emit aggregate measurements only:

- eligible partition candidates;
- partition rows locked and skipped;
- partitions capped at `max_active`;
- partitions below preferred;
- policy-source and administrative-state counts (policy versions remain
  numeric inspection/audit inputs, not labels);
- preferred and borrowed admission decisions;
- ready and expired outcomes;
- logical candidate rows examined and selected outcomes;
- ready claim-wait and expired recovery-wait count/sum/max in integer
  milliseconds.

The concurrency harness separately owns false-admission and false-rejection
audit results; they are not inferred from an ordinary claim event.

The full measurement names, units, invariants, metadata enums, error
availability shape, and success/no-op/partial/under-claim/poison/steal matrix
are normative in [`../telemetry.md`](../telemetry.md). `under_claimed` is set
only by a bounded trusted audit; `outcomes < demand` alone is ordinary partial
fulfillment. A returned CTE count is `candidate_rows_examined`, not physical
PostgreSQL rows scanned. Physical rows/buffers belong to benchmark `EXPLAIN`
output.

Do not emit tenant ID, run ID, claim token, graph ID/hash, raw `scope_key`,
tier, host account identity, policy version, or arbitrary error text as an
admission telemetry label. A host that needs tenant-level audit information
can use trusted logs or the bounded inspection API with its own access,
pagination, retention, and cardinality controls.

`FOR ... SKIP LOCKED` deliberately does not wait for a row lock, and Ecto query
or pool-checkout duration is not a substitute for lock wait. The direct event
owns `skipped_partitions`. A lock-skip delay duration remains a bounded
harness/inspection input unless a later schema provides a database-authored
first-consecutive-skip timestamp; absent history is omitted, never emitted as
zero.

Identity-free aggregates cannot produce per-partition fairness shares. For one
named database/schema domain and window, trusted bounded inspection owns:

```text
instant_concurrency_share_i(t) = active_claims_i(t) / domain_active_claims(t)
windowed_concurrency_share_i = time average of instant_concurrency_share_i(t)
processing_share_i,signal = service_time_i,signal / domain_service_time_signal
entitlement_i = active_weight_i / sum(active_set_weights)
windowed_entitlement_i = time average of entitlement_i over the named window
service_skew_i_pp = 100 * (processing_share_i,signal - windowed_entitlement_i)
```

Windowed concurrency requires time-aligned partition and domain active-claim
samples/intervals; a ratio of their separate time integrals is not equivalent
when total domain concurrency changes. Shares are ratios in `[0, 1]`; skew is
percentage points. Every report names the population, window, and one service
signal. Claim residency uses database-authored integer microseconds;
executor-task native duration is converted to integer microseconds before
aggregation. The current mutable `claimed_at` freshness timestamp is not an
immutable claim-start fact; residency requires future durable token-install/end
evidence. The two signals are never combined implicitly, and zero denominators
are undefined rather than coerced to zero.

## Fairness SLO and regression-budget contract

This section is the normative TenantFair reporting contract. It publishes targets
without pretending that the unimplemented TenantFair engine already emits
production evidence. A target marked **activation-gated** is a release
qualification target: a report must return `not_qualified` until every named
signal and bound exists. Missing input, an unavailable observation, a zero
denominator, or a right-censored episode is never converted to zero or silently
excluded.

### Domain, populations, and windows

One contention domain is one physical PostgreSQL database plus resolved Docket
table schema. `prefix: nil` must be resolved before reporting. Domains, schemas,
engine versions, policy versions, or service signals are never combined in one
denominator.

For partition `i` at time `t`, reports distinguish these populations:

- **ready-admissible** means administrative state `running`, at least one
  non-poisoned ready row is globally eligible, and authoritative live claims
  are below `max_active`. A transiently held partition/run lock does not remove
  it from this global population;
- **preferred-eligible** means ready-admissible and live claims are below
  `preferred_active`;
- **immediately lockable** means the bounded control can acquire the required
  partition/run locks at that instant. It is reported separately from global
  eligibility so persistent lock contention cannot hide a starving partition;
- **expired-recoverable** means an expired authoritative claim can be replaced;
  it is not additive ready capacity;
- **considered**, **locked**, and **skipped** describe only the bounded plan
  represented by the same-named TenantFair v1 measurements. In particular,
  `eligible_partitions` means considered candidates, not every globally
  eligible partition;
- **weighted active set** means the complete fixed set of every partition in
  the fully covered domain that is continuously service-demanding during the
  complete post-warmup window: each remains administrative `running` and
  continuously has ready backlog or an authoritative claim consuming the named
  service. Reaching `max_active` while being served does not remove it from this
  set. Omitting any partition satisfying the predicate invalidates the report;
  and
- **physically executing** means runtime activations or executor tasks still
  running. This is not the authoritative-claim population.

A **qualifying preferred epoch** is a future database-authored, domain-wide,
monotonic sequence value attached to one completed, successful, observation-
available admission decision. It qualifies only when the trusted inspection
proves at least `preferred_slot_floor > 0` distinct preferred-ready partition
slots remained after expired/poison outcomes, capacity constraints, and lock
races. Partial/no-op decisions that do not prove that floor do not advance the
qualified sequence and instead remain coverage/lock evidence. An episode
starting between epochs uses the first one at or after its database-authored
start time. The current runtime has no such sequence or proof, so epoch targets
are activation-gated rather than inferred from telemetry arrival order.

A **low-volume episode** begins when a previously non-eligible partition becomes
preferred-eligible with ready depth in `1..low_volume_ready_limit` (the
qualification default is `1`). It ends at its first preferred admission, loss
of eligibility, or policy/admin-state change. The latter two endings are
right-censored and remain explicit report counts. A partition lock that remains
held does not end the episode: it creates an unresolved lock-skip episode and
eventually a breach. If no qualifying capacity/admission opportunity occurs,
the episode remains visible but is not eligible for a quanta target; its wall
age and capacity-starved status are still reported.

Production rollups use half-open 15-minute episode-start windows `[start, end)`
and retain a separate daily compliance rollup. Episodes starting in a window
are followed beyond `end` until resolution or their target deadline; a report
generated before that deadline marks them `pending`, never successful. Data
loss, loss of eligibility, or policy/admin changes before resolution are
right-censored and make coverage ineligible rather than improving the result.
Qualification and benchmark windows are one complete controlled run, excluding
declared warmup, and must contain at least 1,000 committed measured
transactions for percentile or skew decisions. Smoke profiles verify shape
only. Administrative `hold_new`/`drain` intervals, unavailable/error
observations, database failover/migration intervals, and mixed engine/policy
versions are reported as exclusions with counts and durations. They are not in
successful SLO denominators.

### Required report input

Default telemetry stays identity-free. A cardinality-bounded trusted collector
therefore supplies a versioned `tenant_fair_report_input/v1` record for each
authorized domain/window. It contains only the partitions admitted by the
collector's access and pagination policy and names that coverage explicitly:

```text
header:
  domain_database, domain_schema, window_start, window_end
  coverage_complete, partitions_covered, coverage_limit, coverage_truncated
  implementation, engine_version, policy_version, service_signal
  observation_successes, observation_errors, observation_unavailable
  exclusions_by_reason[{reason, count, duration_ms}]

admission_decisions[] keyed by (decision_sequence, transaction_id, partition):
  optional preferred_epoch, preferred_slot_floor
  preferred_eligible_partition_count
  policy_version, effective_preferred_active, effective_max_active
  effective_weight, admin_state
  ready_admission_permitted_before_cap
  authoritative_live_before, non_additive_claim_clears
  replacement_steals, authoritative_live_at_ready_decision
  additive_ready_admissions, authoritative_live_after
  cap_denial_decision, immediately_lockable_control_rows

low_volume_episodes[] keyed by episode_id:
  partition, started_at, episode_start_epoch, preferred_admission_at
  preferred_admission_epoch, ready_depth, preferred_slot_floor
  eligible_partition_peak, deadline_epoch, optional deadline_at
  resolution, censor_reason
  capacity_starved, immediately_lockable_at_start
low_volume_lag_epoch_histogram, low_volume_pending_count
low_volume_normalized_lag_histogram
low_volume_censored_count, low_volume_capacity_starved_count

lock_skip_episodes[] keyed by episode_id:
  partition, first_consecutive_skip_at, start_epoch, skip_resolved_at
  resolution_epoch, preferred_slot_floor, eligible_partition_peak
  deadline_epoch, optional deadline_at, resolution, censor_reason
lock_skip_epoch_histogram, unresolved_skip_count, censored_skip_count
lock_normalized_skip_histogram

service_window:
  fixed_active_set, fixed_active_set_complete
  active_claim_intervals, domain_active_claim_intervals
  partitions[{partition, partition_service_us, active_weight_intervals}]
  active_set_service_us, peak_service_concurrency
  max_batch_outcomes, enforced_max_service_quantum_us
  max_service_quantum_enforcement_mechanism

wait_window:
  covered_ready_claim_wait_ms_count, covered_ready_claim_wait_ms_sum
  covered_ready_claim_wait_ms_max, ready_claim_wait_histogram_ms
  candidate_coverage_complete, candidate_observation_errors
  candidate_observation_unavailable
  ready_claim_wait_reference_histogram_ms, reference_count
  reference_coverage_complete, reference_observation_errors
  reference_observation_unavailable
  candidate_workload_policy_fingerprint, reference_workload_policy_fingerprint
  wait_reference_artifact_id, wait_reference_approved
  expired_recovery_wait_histogram_ms

reclaim_episodes[] keyed by episode_id:
  partition, borrowed_capacity_at_start, below_preferred_eligible_at
  episode_start_epoch, preferred_admission_at, preferred_admission_epoch
  preferred_slot_floor, eligible_partition_peak, deadline_epoch
  optional deadline_at
  resolution, censor_reason
reclaim_episode_count, reclaim_unresolved_count, reclaim_censored_count

enforced_bounds:
  partition_lock_hold{H_ms, enforcement_mechanism}
  residual_service_quantum{R_ms, enforcement_mechanism}
  poll_delay{P_ms, enforcement_mechanism}
  admission_transaction{T_ms, enforcement_mechanism}

stale_owner_episodes[] keyed by (run, stolen_token, replacement_token):
  steal_at, stale_owner_stopped_at, overlap_ms

benchmark_comparison:
  benchmark_candidate, benchmark_profile, normalized_workload_fingerprint
  benchmark_environment_id, benchmark_postgres_fingerprint
  reference_runtime_fingerprint, candidate_runtime_fingerprint
  reference_source_sha256, candidate_source_sha256
  reference_query_sha256, candidate_query_sha256
  reference_ddl_sha256, candidate_ddl_sha256
  reference_runtime_parity, candidate_runtime_parity
  reference_status, candidate_status
  typed_hash_transition_approvals[{kind: query|ddl,
    reference_sha256, candidate_sha256, approved}]
  query_reference_artifact, query_candidate_artifact, reference_approved
```

An SLO decision requires `coverage_complete = true` for the globally eligible
population within the declared bounded scan/pagination limit. A truncated or
access-subset report remains useful diagnostics but cannot pass a fairness SLO,
because an omitted starving partition could otherwise improve every result.
Each percentile requires at least 1,000 values in its own named population, not
merely 1,000 unrelated admission transactions; smaller sets are
`insufficient_population`. The cap audit records the policy version read inside
the serialized transaction.
The lockability control used to classify a false denial is executed inside the
bounded correctness harness. A false cap denial is a decision labeled cap-
denied even though `L_decision_i < M_i`, administrative/policy state permits
ready admission (`ready_admission_permitted_before_cap = true`), and the control
proves at least one ready row is immediately lockable. Service intervals are
clipped to the window.
Claim residency requires immutable token-install/end evidence; mutable
`claimed_at` is not sufficient. Histogram bucket boundaries and counts are
part of the input and their totals must equal the corresponding TenantFair v1
wait counts for the same covered population. A histogram percentile is the
least bucket upper bound whose cumulative count reaches `ceil(q * count)`; it
is a conservative bucket bound, not a reconstructed raw-sample value. Epoch
histograms use this same nearest-rank bucket rule. Bounds are usable only when
the report also names the enforcing configuration or mechanism; a configured
timeout that does not cover the whole interval is not an enforced bound.

`stale_owner_episodes[].overlap_ms` is measured per stale owner from the
replacement steal until that owner acknowledges stop. Aggregate stale-owner
milliseconds sum per-owner durations and therefore preserve multiplicity. An
absent stop acknowledgement is unresolved, not zero. Executor-task skew
qualification requires these episodes to be absent or included in the measured
service and `peak_service_concurrency`.

The collector rejects internally inconsistent input: decision keys are unique;
episode histogram totals equal the matching resolved array classifications;
pending/censored/starved/unresolved counters equal their array states;
`reclaim_episode_count` equals the reclaim array length;
`active_set_service_us = sum(partitions[].partition_service_us)`; and the fixed
active-set keys equal the partition keys exactly. Current/reference wait
histogram totals equal their recorded counts. Benchmark selection uses the same
`benchmark_candidate` row in both summaries, and all selected measurements,
eligibility checks, query hashes, samples, and `plan_path` come from that row.
Zero audited cap decisions or zero reclaim episodes is `no_population`, not a
vacuous pass. FAIR/LOCK epoch finalization sets
`deadline_epoch_i = start_epoch_i + 2 * round_budget_epochs_i`. For reclaim,
when every bound exists,
`deadline_at_i = below_preferred_eligible_at_i + budget_ms_i`; otherwise it has
no numeric deadline or guarantee and its epoch deadline is diagnostic only.
Every unequal query or DDL hash pair requires exactly one matching approved
typed transition entry; equal pairs require none.

### Published targets

Let `A_i` be additive ready admissions for partition `i`, `L_decision_i` its
authoritative live claims after any claim-clearing/replacement outcomes and at
the serialized ready-capacity decision, and `M_i` the effective `max_active`
read in that transaction. The audit requires
`L_decision_i = L_before_i - non_additive_claim_clears_i` and
`L_after_i = L_decision_i + A_i`; every replacement steal has zero net effect
and therefore appears only in `replacement_steals_i`. This transition
reconciliation prevents a mislabeled steal from hiding additive authority.
Let `E_peak` be the maximum globally preferred-eligible partition count during
an episode, `S_floor` the minimum proven `preferred_slot_floor` across its
qualifying epochs, and:

```text
round_budget_epochs = ceil(E_peak / S_floor) + 1
```

The extra epoch is the explicit indivisible-batch/lock-race allowance. It is
not a promise of strict per-poll rotation. Budgets are computed per episode;
population percentiles use `normalized_lag_i = lag_epochs_i /
round_budget_epochs_i`, so episodes with different active sets and slot floors
are never compared to one unindexed budget. In `FAIR-2`, `C_service_max` is
`service_window.peak_service_concurrency`, `B_max` is
`service_window.max_batch_outcomes`, and `Q_max_us` is
`service_window.enforced_max_service_quantum_us`; its enforcement evidence is
`max_service_quantum_enforcement_mechanism`. `H_ms`, `R_ms`, `P_ms`, and
`T_ms` come from their like-named `enforced_bounds` objects and are unusable
without the adjacent enforcement mechanism.

| ID and class | Formula and unit | Window and population | Published target | Required signal and current status |
| --- | --- | --- | --- | --- |
| `CAP-1` safety invariant | `violations = count(A_i > max(M_i - L_decision_i, 0) or L_decision_i != L_before_i - non_additive_claim_clears_i or L_after_i != L_decision_i + A_i)`; committed decisions | Every serialized ready-capacity decision. Replacement steals are zero-net and excluded from `A_i`; an already-over-cap downgrade cannot add ready authority. | `violations = 0`. This is exact correctness, not a percentile. | Trusted serialized transition audit plus concurrent correctness harness. TenantFair engine activation-gated. |
| `CAP-2` cap-denial correctness | `false_denial_rate = count(cap_denial and L_decision_i < M_i and ready_admission_permitted_before_cap and immediately_lockable_control_rows > 0) / count(cap_denial)`; ratio | Every bounded audited cap-denial decision; zero decisions is `no_population`. | Numerator `= 0`. | `cap_denied_partitions` supplies descriptive volume only; exact numerator comes from the decision/control audit. Activation-gated. |
| `FAIR-1` low-volume service lag | `lag_epochs_i = preferred_admission_epoch_i - episode_start_epoch_i`; `normalized_lag_i = lag_epochs_i / round_budget_epochs_i`; epochs and ratio. Also report wall age/censor reason. | 15-minute episode-start and daily windows; >=1,000 low-volume episodes, including pending/censored/capacity-starved counts. | p99 normalized lag `<= 1`, maximum `<= 2`; pending, censored, and capacity-starved counts must all be zero for a final pass. | Database-authored epoch/episode history. No current production numeric claim. |
| `FAIR-2` bounded service skew | `skew_i_pp = 100 * (partition_service_us / active_set_service_us - windowed_entitlement_i)`; percentage points. `B_pp = 100 * ((C_service_max + B_max) * Q_max_us / active_set_service_us)`. `Q_max_us` is an enforced maximum accounting quantum for the named signal; `C_service_max` measures that signal's actual concurrency. | One complete >=15-minute fixed-signal window; the complete fixed continuously service-demanding weighted active set; numerator and denominator include only that set. | `max_i(abs(skew_i_pp)) <= B_pp` and `fixed_active_set_complete = true`. For claim residency, token-residency charges must be durably split at `Q_max_us`; for executor task time, stale work must be included and physically bounded. | Trusted complete-coverage service/weight intervals. Currently `not_qualified`. |
| `LOCK-1` preferred-partition lock-skip delay | `skip_delay_ms_i = skip_resolved_at_i - first_consecutive_skip_at_i`; `skip_epochs_i = resolution_epoch_i - start_epoch_i`; normalize by that episode's round budget. | 15-minute episode-start and daily windows; >=1,000 preferred-eligible skip episodes, including unresolved/censored episodes. | p99 normalized skip epochs `<= 1`, maximum `<= 2`, unresolved/censored counts `= 0`. With per-epoch enforced `H_ms`, `P_ms`, `T_ms`: each episode's millisecond budget is `round_budget_epochs_i * (H_ms + P_ms + T_ms)`; p99 normalized milliseconds `<= 1`, maximum `<= 2`. | DB-authored skip/epoch history. `skipped_partitions` alone is incidence, not duration. No current numeric lock-delay SLO. |
| `WAIT-1` ready claim wait | `mean_ms = covered_ready_claim_wait_ms_sum / covered_ready_claim_wait_ms_count`; p95/p99 from matching histograms; milliseconds. | One complete controlled >=1,000-ready-lease candidate compared with an immutable approved complete >=1,000-sample reference; identical buckets and workload/policy fingerprints. | Candidate/reference p95 ratio `<= 1.20`, p99 ratio `<= 1.30`; both coverage-complete with zero error or unavailable observations. This is a qualification regression budget, not a universal wake-to-claim bound. | Trusted current/reference artifact IDs, coverage/errors, counts, fingerprints, and histograms; reconcile candidate with TenantFair totals when whole-domain. |
| `QUERY-1` claim-query latency | For the named `benchmark_candidate`, `p95_ratio = candidate.query_us.p95 / reference.query_us.p95`, likewise p99; ratio and microseconds. | Approved prior artifact for the same candidate, normalized workload fingerprint, PostgreSQL fingerprint, and environment ID; both non-smoke, correctness-eligible, zero-error, and independently >=1,000 samples. Query/DDL hashes match or have an approved transition. | p95 ratio `<= 1.20`, p99 ratio `<= 1.30`. Benchmark regression budget only. | Complete benchmark artifact sets. Current `runtime_parity: false` permits prototype-to-prototype evidence only. |
| `RECLAIM-1` authoritative preferred-admission reclaim lag | Per episode `reclaim_lag_ms_i = preferred_admission_at_i - below_preferred_eligible_at_i`; `budget_ms_i = R_ms + round_budget_epochs_i * (H_ms + P_ms + T_ms)`. | Every borrowed-capacity-at-start episode in 15-minute episode-start and daily windows, including unresolved/censored episodes. | Only if every bound, epoch, and timestamp exists: `count(reclaim_lag_ms_i > budget_ms_i) = 0` and unresolved/censored counts `= 0`. Otherwise `no_numeric_guarantee`; preference is conditional on an actual release and no finite liveness promise is made. | Trusted borrowed-capacity crossing/epoch/admission history. This targets authoritative admission only; borrowing/current runtime is not qualified. |

`CAP-1` limits net additive authoritative claim count, not the creation of a
replacement token and not physical execution. A successful steal replaces one
authority, but the stale owner can overlap the replacement
until it stops or reaches another fence; repeated steals can multiply that
physical overlap. `stale_owner_overlap_*` is therefore a separate report-only
inspection signal with no current target. CPU, executor tasks, downstream calls,
and external effects are never CAP-1 numerators.

`RECLAIM-1` likewise ends at a new authoritative preferred admission; it does
not claim that a stale borrowed executor, process, CPU slot, or downstream call
has stopped. A physical-capacity reclaim objective would additionally require
stop acknowledgement for every implicated `stale_owner_episode` and has no
current target.

### Worked report

This illustrative report uses only the fields above, the TenantFair v1 event,
and the complete benchmark v1 artifact sets (`summary.json`, `manifest.json`,
samples, and plans). It demonstrates unavailable handling; it is not evidence
from a shipped TenantFair engine.

```text
domain/window: docket_prod_1/public, [12:00, 12:15)
coverage: complete=true, partitions=20/limit=1000, truncated=false
          observation_successes=600, errors=0, unavailable=0
          exclusions_by_reason=[]

CAP-1: 40 additive decisions have (L_before,M,clears,A,L_after)=(1,2,0,1,2)
       12 denied decisions have (2,2,0,0,2); all 52 share policy_version=7
       capacity/transition violations=0                          PASS
CAP-2: all 12 denied decisions have L_decision=2,M=2
       cap_denial_decisions=12, false_cap_denial_decisions=0
       false_denial_rate=0/12=0                                 PASS

FAIR-1: all 1000 episodes have E_peak=20, S_floor=5
        per-episode round_budget=ceil(20/5)+1=5
        lag_epoch_histogram={1:200,2:400,3:300,4:99,7:1}
        normalized p99=4/5=.8, max=7/5=1.4
        count=1000, pending/censored/starved=0                   PASS
LOCK-1: skip_epoch_histogram={1:500,2:300,3:199,8:1}
        normalized p99=3/5=.6, max=8/5=1.6
        count=1000, unresolved/censored=0                       PASS
        H_ms/P_ms/T_ms not all present; millisecond target       NOT_QUALIFIED

WAIT-1: ready count=1000, sum=1200000 ms, max=3900 ms
        reference_artifact_id=wait-ref-2026-07-01-approved
        shared bucket bounds=[1000,1800,2000,2500,3000,3500,3900]
        candidate counts=[600,0,350,0,40,0,10]
        reference counts=[700,250,0,40,0,10,0]
        candidate/reference count=1000/1000, coverage=true/true
        errors=0/0, unavailable=0/0, workload_policy_fingerprint=7c31/7c31
        p95/p99 candidate=2000/3000, reference=1800/2500 ms
        exact ratios=2000/1800 and 3000/2500 (gate before rounding) PASS

FAIR-2: service_signal=claim_residency, active_set_service=100000000 us
        C_service_max=10, B_max=5, Q_max absent                  NOT_QUALIFIED
RECLAIM-1: episodes=8, unresolved=0, censored=0
           R_ms/H_ms/T_ms absent; numeric target                 NO_NUMERIC_GUARANTEE

QUERY-1: candidate=hint_cursor, profile=scale
         candidate_artifact=/evidence/current/summary.json
         reference_artifact=/evidence/reference/summary.json
         artifact_set_complete=true/true
         candidate/reference samples=1600/1600
         candidate/reference eligible=true/true, errors=0/0
         normalized_workload_fingerprint=19af, environment_id=pg-scale-a
         postgres_fingerprint=pg16/settings-7c31, reference_approved=true
         runtime/source/query/DDL fingerprints match
         status=exploratory_pre_runtime_prototype/
                exploratory_pre_runtime_prototype, runtime_parity=false/false
         approved reference p95/p99=80000/120000 us
         candidate p95/p99=92000/150000 us
         ratios=1.15/1.25                                       PASS (prototype only)
```

The event reconciliation checks for this report are also explicit: because the
illustrated histogram covers the whole successful-ready population, its count
equals `sum(ready_claim_wait_ms_count) = 1000`; a collector covering only an
authorized subset instead reports both covered and event-total counts and does
not claim equality. Cap-denial volume equals the covered sum of
`cap_denied_partitions = 12`, and excluded/unavailable observations are stated
in the header. The per-partition cap and fairness numerators come only from the
bounded trusted input, never from identity-free aggregate events.

### Acceptance traceability

| Acceptance criterion | Contract or evidence |
| --- | --- |
| Tenant identity cannot be selected from untrusted run payload data. | [Trusted partition identity](#trusted-partition-identity) and persisted generated `scope_key`. |
| All policy concepts have precise operational definitions and units. | [Fairness vocabulary](#fairness-vocabulary), population definitions above, and TenantFair v1 measurements in [`../telemetry.md`](../telemetry.md). |
| Configuration uses the closed whitelist and fails clearly when invalid. | [Instance configuration contract](#instance-configuration-contract) and `Docket.Postgres.ClaimPolicy.TenantFair.Config`. |
| Benchmarks cover queued-row/tenant cardinality, dormant tenants, and hot contention. | Benchmark `manifest.json.config`, deterministic seed manifest, contention, hot-contention, reconciliation, and cap audits documented in [`../postgres-operations.md`](../postgres-operations.md). |
| SLOs can be computed from emitted aggregate telemetry plus documented bounded trusted inspection inputs. | TenantFair v1 event inputs plus the explicitly bounded `tenant_fair_report_input/v1` crosswalk above. Identity-bearing per-partition values never become default metric labels; unavailable activation gates remain visible. |
| The harness reproduces ranking-CTE plus `SKIP LOCKED` under-claim. | `Docket.Test.ConcurrentAdmissionHarness.reproduce_known_bad_underclaim!/2` in `test/support/postgres/concurrent_admission_harness.ex`, invoked from `test/docket/postgres/run_store_test.exs`, plus benchmark `contention`, `control_lockable_rows`, `avoidable_underclaim_transactions`, and `performance_eligibility` outputs. |

Operational dashboards should compare:

- total versus quiet-tenant queue age;
- per-tier aggregate saturation;
- capped partition count;
- borrowed capacity and reclamation delay;
- claim throughput and p95/p99 claim latency;
- logical candidate rows examined per lease, with physical scan work taken
  from benchmark `EXPLAIN` output;
- vehicle utilization;
- execution-time share versus claim-count share.

Aggregate tier metrics can hide one starving tenant. Use bounded top-K or
sampled tenant diagnostics in the trusted inspection plane rather than tenant
IDs in ordinary telemetry labels.

## Correctness tests

### Strict admission

- Concurrent dispatchers cannot exceed one tenant's `max_active`.
- Pause one claimant after it locks a partition and before it updates run rows;
  concurrent claimers must skip that tenant and cannot exceed the cap.
- Race an expired steal and an additive ready claim for the same at-cap
  partition; both paths must obey the same partition-lock invariant.
- Limits remain database-wide across multiple runtime supervisors.
- Mixed BEAM configuration cannot create mixed effective defaults; startup
  verifies the database policy version/fingerprint.
- Different PostgreSQL prefixes have independent accounting.
- A tenantless deployment forms exactly one partition.
- `max_active: 0` admits no new ready claims but still permits replacement
  steals unless administrative state says otherwise.
- An upgrade takes effect after commit without restart.
- A downgrade is non-preemptive and blocks new ready claims until below the
  new ceiling.
- A policy update racing a claim produces an outcome valid under one complete
  serialized policy, never a torn mixture.
- Out-of-order and duplicate policy events are rejected or idempotent through
  version/event checks.
- Supervised dispatch and manual drain resolve the same database policy and
  cannot bypass caps through separate policy construction.

### Recovery and lifecycle

- An expired claim can be stolen while its tenant is exactly at the cap.
- Steal does not increment the tenant's active allocation.
- Poison, release, cancellation, waiting, terminal commit, and retry parking
  expose capacity exactly once.
- A stale token cannot release or commit capacity owned by a newer token.
- Launch failure and dispatcher shutdown do not leak claims.
- Claim refresh and retained moments do not create additional slots.

### Selection fairness

- Thousands of oldest rows from one tenant cannot hide another tenant's first
  eligible row.
- Stable within-tenant ordering uses eligibility time and run ID.
- Continuously eligible ready and expired classes both make progress.
- A locked tenant partition does not prevent other tenants from being served.
- If the first ranked/oversampled candidate page is locked, the poll continues
  to later pages and claims other eligible partitions within its work budget.
- Lost or stale ready/expired hints are repaired by fallback reconciliation and
  cannot strand a run indefinitely.
- Never-served partitions sort before previously served partitions.
- Only a final returned claim/poison outcome advances the rotation cursor.
- A persistently locked partition produces observable skip streak and age.
- With more eligible tenants than dispatcher demand, measured service skew
  remains within the stated window bound despite lock races; exact per-poll
  rotation is not required.

### Weighted and borrowing phases

- Long-run service converges toward configured weight ratios within a stated
  error bound for indivisible slices.
- A new tenant and a tenant returning from idle enter at the specified system
  virtual time rather than monopolizing from a zero lifetime score.
- A weight change affects future service without retroactively dividing all
  historical usage.
- A high claim count with short work and a low claim count with slow work are
  distinguished by service accounting.
- Idle capacity is usable up to `max_active` when borrowing is enabled.
- New admissions prefer newly eligible tenants below `preferred_active`.
- Reclamation is measured and described as bounded only when a hard numeric
  maximum residual slice duration is enforced.
- No tenant exceeds `max_active` during borrowing.
- Crash, expiry, and steal preserve admission-time borrowed/preferred audit
  classification while inspection derives current above-preferred usage.

### Isolation

- Fairness never broadens tenant read or mutation scope.
- A wrong tenant and an unknown run remain indistinguishable to public reads.
- Policy administration is unavailable through ordinary tenant-scoped calls.
- Telemetry contains no high-cardinality tenant identity.

## Performance and scale tests

Benchmark at least these database shapes:

- one hot tenant with a very deep ready backlog;
- many tenants with one ready run each;
- many tenants at their cap;
- mixed ready and expired claims;
- concurrent claim, release, commit, steal, and policy-update traffic;
- high-cardinality dormant partition rows;
- a prefix-local runtime sharing a Repo with the default prefix;
- borrowing enabled with only one active tenant, then with a quiet tenant
  becoming eligible.

Record `EXPLAIN (ANALYZE, BUFFERS)` plans, rows scanned per selected lease,
partition-lock contention, pool checkout time, query p95/p99, and commit
throughput. Set explicit regression thresholds for hint-page size,
oversampling, recursive loose-scan reconciliation, and bounded lateral fills.
Keep window/`DISTINCT ON` plans as failure baselines rather than likely
production choices.

## Rollout plan

### Phase 0 — contract and measurement

- Approve vocabulary and option names.
- Define trusted tenant identity derivation and cardinality safeguards.
- Define fairness objectives, numeric slice assumptions, and public claim
  versus physical-execution semantics.
- Add claim-residency and queue-age measurements needed for baseline traces.
- Capture production-like tenant backlog and execution-cost distributions.
- Define fairness success metrics and query-latency budgets.
- Prototype the partition-hint/cursor claim path and its recursive loose-scan
  reconciliation against the window/`DISTINCT ON` baseline.

### Phase 1 — exact dynamic hard caps and control plane

- Add and backfill `docket_claim_partitions`.
- Add recoverable partition eligibility hints and reconciliation.
- Add tenant-leading partial indexes.
- Add database-authoritative defaults, policy versioning, trusted override
  CAS, audit history, and explicit administrative hold state.
- Enforce database-wide `max_active` in the atomic claim path.
- Preserve expired-claim recovery at the cap.
- Add inspection and aggregate telemetry.
- Add backlog depth/age/creation-rate inspection and a host backpressure hook.
- Ship with borrowing disabled.

### Phase 2 — fair partition rotation

- Select bounded partition pages before deeper tenant candidates; continue
  past locked first pages.
- Persist a minimal scheduling cursor.
- Add dispatcher jitter/randomized scan phase and prove bounded service skew
  under hot-partition lock contention.
- Benchmark hint paging, loose-scan reconciliation, and lateral fill at high
  cardinality.

Phases 1 and 2 may ship together if the query remains reviewable and measured;
neither should ship alone if a global candidate limit can still hide tenants.

### Phase 3 — preferred threshold and borrowing

- Activate `preferred_active` and `borrowing` without describing preferred
  capacity as reserved.
- Persist admission-time preferred versus borrowed classification for audit.
- Implement preferred and borrowing admission phases.
- Measure reclamation and stale-owner physical overlap.

### Phase 4 — active-set weighted service

- Activate `weight`.
- Write and approve the separate active-set algorithm specification.
- Select and validate a named service-cost objective.
- Define idle re-entry, admission charging, weight changes, and state
  normalization.
- Implement idempotent asynchronous service aggregation without adding
  partition writes to run commit/release/refresh/abandon paths.
- Publish contention-domain, convergence, and maximum-unfairness expectations.

### Phase 5 — advanced isolation, only if demanded

- Evaluate enforced queued-run and enqueue-rate admission limits beyond the
  Phase 1 inspection/backpressure surface.
- Evaluate shuffle sharding for worker blast-radius isolation.
- Evaluate hierarchical account/project partitions.
- Evaluate dominant-resource accounting if executors can report trustworthy
  CPU, memory, accelerator, or cost units.

## Decisions still required

1. Final public names for partition configuration and policy administration.
2. Final administrative-state semantics for retained claims, recovery steals,
   and manual drains.
3. Whether strict caps and fair rotation are one release or two internal
   milestones.
4. Exact hint-maintenance, cursor-paging, oversampling, and loose-scan fallback
   parameters that satisfy the query-work budget.
5. The active-set weighted algorithm, first cost objective, admission charge,
   and accounting boundary.
6. Whether borrowed classification must be a run column or can be preserved
   through another durable operational record.
7. The documented bound for reclamation after a quiet tenant becomes ready.
8. Whether effective limits may be tier-aggregated in telemetry without
   exposing a host-defined tier label to Docket.

## Research basis

The design follows established patterns rather than treating fairness as a
single algorithm:

- Amazon SQS Fair Queues identifies tenants on shared work, measures both
  in-flight concurrency and recent processing-time share, deprioritizes noisy
  tenants while quiet work exists, and remains work-conserving when it does
  not: <https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fair-queues-detailed.html>
- Shreedhar and Varghese's Deficit Round Robin applies fair queueing to
  variable-size, indivisible service units:
  <https://openscholarship.wustl.edu/cse_research/339/>
- Start-time Fair Queueing provides weighted, hierarchical service without
  requiring the next quantum length at selection time:
  <https://conferences.sigcomm.org/sigcomm/1996/papers/goyal.pdf>
- Dominant Resource Fairness explains why equal slot counts cease to be fair
  when workloads consume heterogeneous resources:
  <https://www2.eecs.berkeley.edu/Pubs/TechRpts/2011/EECS-2011-18.pdf>
- Hadoop YARN separates maximum capacity, parallel-application limits, user
  weights, and elastic use of idle capacity:
  <https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html>
- Kubernetes Kueue separates nominal quota, weighted fair sharing, borrowing,
  and preemption of borrowed workloads:
  <https://kueue.sigs.k8s.io/docs/concepts/preemption/>
- Amazon's multi-tenant and asynchronous-system guidance motivates
  database-wide admission, bounded concurrency, backpressure, dynamic quotas,
  and avoiding one queue or process per high-cardinality tenant:
  <https://d1.awsstatic.com/builderslibrary/pdfs/fairness-in-multi-tenant-systems-david-yanacek.pdf>
  and
  <https://d1.awsstatic.com/builderslibrary/pdfs/avoiding-insurmountable-queue-backlogs.pdf>
- PostgreSQL documents why `SKIP LOCKED` is appropriate for queue-like access,
  why it gives an inconsistent view, how `READ COMMITTED` snapshots interact
  with concurrent updates, and the row-lock conflict matrix used by the
  admission proof:
  <https://www.postgresql.org/docs/current/sql-select.html>,
  <https://www.postgresql.org/docs/current/transaction-iso.html>, and
  <https://www.postgresql.org/docs/current/explicit-locking.html>
- AWS Batch and Linux CFS demonstrate recent/active-set service accounting and
  sleeper re-entry instead of dividing an unbounded lifetime counter by a
  weight:
  <https://docs.aws.amazon.com/batch/latest/userguide/fair-share-scheduling.html>
  and
  <https://www.kernel.org/doc/html/latest/scheduler/sched-design-CFS.html>
- Microsoft's 2DFQ addresses unknown and high-variance request cost in
  concurrent multi-tenant cloud execution:
  <https://www.microsoft.com/en-us/research/publication/2dfq-two-dimensional-fair-queuing-for-multi-tenant-cloud-services/>
- Hatchet's PostgreSQL multi-tenant queue demonstrates why read-time window
  ranking both scans the backlog and can under-claim when a later
  `SKIP LOCKED` step encounters rows chosen by concurrent pollers; its
  write-maintained sequencing design motivates making Docket's partition
  eligibility/cursor state load-bearing rather than an optional escape hatch:
  <https://hatchet.run/blog/multi-tenant-queues>
- PostgreSQL's loose-index-scan guidance records recursive CTE emulation as a
  candidate where native index behavior does not efficiently enumerate
  distinct leading values:
  <https://wiki.postgresql.org/wiki/Loose_indexscan>

These systems differ in their ability to preempt, predict work size, or observe
multiple resources. Docket therefore adopts their separations of concern while
retaining claim fencing and bounded, non-preemptive execution as its own
correctness boundary where a numeric runtime bound is actually enforced.
