# TenantFair schema-v3 active-ring decision

This document ratifies the DCKT-76 schema and bounded-query inputs for the
v0.1.0 TenantFair MVP. Schema v3 adds one table: an authoritative
unfinished-tenant ring; the serialized scan position lives on the existing
singleton policy row. DCKT-78 must implement the scheduler and DCKT-79 must feed
database-authored traces into the DCKT-75 proof oracle.

Schema v3 does not establish a numeric `L`. A finite lock hold does not bound
the number of failed target inspections, and `SKIP LOCKED` alone is not a
starvation guarantee.

## Decision and evidence

Fair schedulers conventionally maintain explicit active-flow queues and service
them in rotation. FQ-CoDel, for example, adds a queue to an active list when
work arrives and removes it when the queue becomes inactive. PostgreSQL
documents transactional triggers as a mechanism for maintaining summary state
and partial indexes as automatically maintained subsets of one table.

Those sources support active-set maintenance and round-robin scheduling; they
do not prove Docket's database-specific fairness theorem. Docket's fixed-window
proof, lock-order invariants, and executable comparison with Legacy remain
project-owned obligations.

Primary sources:

- [RFC 970 fair round-robin queues](https://www.rfc-editor.org/rfc/rfc970.html)
- [RFC 8290 active-queue maintenance](https://www.rfc-editor.org/rfc/rfc8290.html#section-4)
- [PostgreSQL trigger behavior](https://www.postgresql.org/docs/current/trigger-definition.html)
- [PostgreSQL trigger-maintained summary table](https://www.postgresql.org/docs/current/plpgsql-trigger.html)
- [PostgreSQL partial indexes](https://www.postgresql.org/docs/current/indexes-partial.html)
- [PostgreSQL `SKIP LOCKED` limitations](https://www.postgresql.org/docs/current/sql-select.html)

The rejected eligibility-hint design had no primary-source precedent showing
that a fairness-critical scheduler population should be intentionally lossy and
recovered by periodic repair. It also made an incomplete ring ineligible for
the theorem, so repair added operational state without strengthening the proof.

## Ratified constants

| Name | Value | Unit and boundary |
| --- | ---: | --- |
| `S` | 32 | unfinished-ring positions inspected per qualifying call |
| `Q` | 8 | lease or poison outcomes per nonempty partition grant |
| `K` | 16 | exact-partition structural run keys admitted to one lock-attempt window |
| `M` | 8 | run rows one grant may finally mutate; equal to `Q` |

One inspection can create at most one grant. A call is therefore bounded by 32
grants, 512 exact-key run-lock attempts, and 256 returned outcomes or run-row
mutation inputs. Caller demand normally lowers those ceilings.

The constants bound logical work. They do not turn timing, buffer counts, or a
query plan into fairness proof.

## Authoritative unfinished membership

`docket_claim_schedule` has one immutable-position row per claim partition:

- `scope_key` is the primary and foreign key;
- `ring_position` is a positive unique monotonic identity;
- `unfinished_count` is the exact count of committed `docket_runs` rows for the
  scope whose durable status is nonterminal (`running` or `waiting`);
- the ready candidate-continuation tuple remains a paired nullable
  `(eligible_at, run_id)` keyset for bounded poison progress; and
- the partial `(ring_position)` index contains exactly rows where
  `unfinished_count > 0`.

`unfinished_count` is not the exact-cap live-claim count. It includes ready
runs, future timers, externally parked/waiting runs, healthy and expired claims,
and poisoned running rows. This makes the ring an authoritative superset of
current claim eligibility. Dormant positions may produce unsuccessful
inspections, but nonterminal work cannot be omitted from ring membership.

The schedule row remains at zero instead of being deleted, preserving the
tenant's ring position across idle periods. A run-table trigger maintains the
counter in the same transaction:

- nonterminal insert or terminal-to-nonterminal transition: `+1`;
- nonterminal-to-terminal transition or nonterminal delete: `-1`; and
- every other update: `0`.

Rollback persists neither the run transition nor the counter delta. A run's
scope is immutable, and the positive unfinished count prevents deleting a
partition that still owns running or parked/resumable work. Terminal states are
absorbing, so terminal history does not retain partition authority. Ordinary
application updates to the counter are rejected and underflow fails closed. As
with every schema invariant, the threat model trusts the schema owner not to
install DDL that bypasses triggers or constraints.

## Lock graph

The counter lives in a sidecar instead of `docket_claim_partitions` because
lifecycle paths already lock a run before recording its terminal transition.
Putting the counter on cap authority would create a run-to-partition edge
against admission's partition-to-run order.

The required order is:

```text
admission: policy/interlock + scan position -> partition authority -> run rows
lifecycle: run row -> schedule counter
discovery: MVCC read of schedule ring; no schedule-row lock
```

Admission must never lock a schedule row and then seek a run. A concurrent
terminal transition may remove a position after discovery; the later
authoritative partition/run recheck simply records an unsuccessful inspection.

## Circular traversal and proof population

The candidate population is the partial unfinished-ring index. One recursive
keyset seek reads the next raw position after the durable cursor, falling back
to the first position at wrap. When demand remains unfilled it performs exactly
`S` visits, including repeated wrap when `H < S`.

For a qualifying DCKT-75 window:

- `C` is the fixed duplicate-free ring of scopes with `unfinished_count > 0`;
- `H = |C|`, including currently dormant or capped scopes;
- `P` contains the continuously admissible target plus every partition that
  grants during the window (competitors may be intermittently eligible), and
  `P` is a subset of `C`; and
- any zero-to-positive or positive-to-zero membership change makes the window
  ineligible; positive-to-positive count changes do not change `C`.

The DCKT-75 grant, outcome, and call formulas are unchanged. A larger dormant
population increases `H` and therefore the call bound, but it does not make
per-call work depend on tenant or queue cardinality.

Discovery does not use `FOR UPDATE SKIP LOCKED`. DCKT-78 must attempt exact
partition authority after reading each position so a locked partition is an
explicit failed inspection. Run locking accepts no more than `K` exact IDs
before `SKIP LOCKED`; it cannot scan an unbounded locked prefix to manufacture
`K` successful locks.

## Exact-scope candidates and continuation

For one inspected partition, ready and expired queries each perform an
exact-scope seek in the existing class index and return at most `K` structural
rows. DCKT-78 ranks only their at-most-`2K` union, feeds at most `K` exact IDs to
the lock shape, and feeds at most `Q` locked IDs to mutation.

If a capped ready page contains no permitted poison outcome, the scheduler
persists its last `(eligible_at, run_id)` in the schedule row. The next
inspection seeks after that tuple and then wraps once, still returning at most
`K` rows. Normal below-cap selection starts from the oldest row and ignores or
clears the continuation after a grant or cap relief. Expired work is
count-neutral at cap, so v0.1 adds no expired continuation state.

## Rejected alternatives

- Global eligible-run grouping and rank-before-lock perform work that grows
  with queued rows before returning a bounded page.
- Ready/expired eligibility hints plus repair make scheduler reachability
  temporarily incomplete and add two cursor authorities, cadence semantics,
  migrations, tests, telemetry, and failure modes.
- Storing `unfinished_count` on cap authority creates a reverse run-to-partition
  lock edge from terminal lifecycle writes.
- Deleting the schedule row at zero churns ring identity and complicates
  concurrent zero-to-one activation.
- `ORDER BY ... LIMIT n FOR UPDATE SKIP LOCKED` is not a work bound because it
  may scan arbitrarily many locked rows to return `n` unlocked rows.

## Reproducible evidence

Run:

```sh
mix run bench/postgres/dckt_76_query_plans.exs
```

The harness records `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plans and checks
fixed root cardinality, selected indexes, recursive ceilings, exact-key lock
attempts, and absence of selected sequential or bitmap heap scans. One fixture
contains 20,000 unfinished positions plus one hot tenant; a second contains
20,000 registered partitions but only 120 unfinished positions. Both return
exactly `S = 32` positions with 63 index visits and zero filtered rows. The
`H < S` fixture proves repeated wrap.

The evidence is a bounded-work and index-shape gate. Full-cycle call count is
honestly `ceil(H / S)` in the no-grant case; elapsed latency must be measured under
real deployments. Any narrower readiness accelerator remains post-MVP until
observed dormant unfinished populations or latency prove it necessary; it may
not become the only route by which unfinished work is discovered.

## Migration boundary

Schema v3 is additive on the stopped homogeneous v0.1 development line. The
migration locks policy, partition authority, then runs; creates and backfills
missing v2 partition/schedule membership; backfills exact unfinished counts;
installs lifecycle maintenance; and adds the scan position to the existing
singleton policy row transactionally. Fresh, v2-to-v3,
host-v1-to-current, downgrade-to-v2, prefix, rollback, and concurrent-creation
paths are covered by the PostgreSQL suite.
