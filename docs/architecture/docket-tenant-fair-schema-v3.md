# TenantFair schema-v3 budgets and query shapes

This document ratifies the DCKT-76 inputs for the v0.1.0 TenantFair MVP. It is
a schema and bounded-discovery decision, not a claim that the current admission
function already satisfies the DCKT-75 theorem. DCKT-77 must maintain complete
hints, DCKT-78 must implement the serialized scheduler, and DCKT-79 must feed
database-authored traces into the proof oracle.

Schema v3 does not establish a numeric `L`. A finite lock hold does not bound
the number of failed target inspections, and `SKIP LOCKED` alone is not a
starvation guarantee. The strict Legacy separation remains the conditional,
fixed-population, demand-one, `L = 0` trace frozen by DCKT-75.

## Ratified constants

The constants are internal v0.1 engine constants, not user-facing weights or
share controls. `A` and `H` remain observed proof-window populations and are
never configuration values.

| Name | Value | Unit and boundary |
| --- | ---: | --- |
| `S` | 32 | committed scheduling positions inspected per qualifying scan call |
| `Q` | 8 | committed lease or poison outcomes per nonempty partition grant |
| `K` | 16 | oldest structural ready/expired keys read per exact partition and admitted to one bounded lock-attempt window |
| `M` | 8 | run rows one grant may finally mutate; equal to `Q` |
| `R_ready` | 32 | unique ready-index scope heads inspected by one reconciliation invocation |
| `R_expired` | 32 | unique expired-index scope heads inspected by one reconciliation invocation |
| repair cadence | 32 | qualifying scan calls per class; expired is staggered by 16 calls |

One inspection can create at most one grant. A qualifying call is therefore
bounded by 32 grants, 512 exact-key run-lock attempts, and 256 returned outcomes
or run-row mutation inputs. Metadata writes are not included in that last
ceiling; DCKT-78 must ratify its complete write shape. Caller demand normally
lowers those ceilings. `Q` is a per-grant bound, not a per-call bound.

The fixed values keep one logical scan page small while allowing the existing
ready/expired batch reservation to use more than one outcome. They are
centralized in `TenantFair.Budgets`; changing one requires new plan evidence,
the DCKT-75 proof inputs, and the DCKT-79 oracle fixtures to move together.

These constants bound discovery, explicit lock attempts, and mutation inputs.
They do not reinterpret timing, buffer use, or the current schema-v2 live-count
aggregate as fairness proof. DCKT-78 still owns the authoritative cap, attempt
class, poison, state, and eligibility rechecks and must preserve poison progress
without hiding unbounded work behind a `LIMIT ... SKIP LOCKED` query.

## Durable state

Schema v3 is additive to schema v2:

- `docket_claim_schedule` has one trigger-created row per claim partition.
  `scope_key` is the primary/foreign key and `ring_position` is a positive,
  unique, monotonic identity. Existing v2 partitions are backfilled once.
- `may_have_ready_at` and `may_have_claimed_at` are conservative timestamps,
  not authority. The latter deliberately does not say "expired": expiry also
  depends on the caller's orphan cutoff.
- `ready_dirty` and `claimed_dirty` keep uncertain state in the candidate
  cohort as a stale positive. The generated `in_cohort` value feeds one partial
  `ring_position` index, so a partition with both classes still occupies one
  duplicate-free position.
- ready and expired candidate-continuation tuples are non-authoritative
  `(eligible_at, run_id)` keysets. They let DCKT-78 advance a zero-outcome
  structural page without moving service epoch or the domain scan cursor.
  Pair constraints prevent half-written cursor tokens, and run IDs are not
  foreign keys so deletion leaves a valid keyset gap.
- `docket_claim_scan_cursor` is the one domain-global scan authority. Its
  position is not a foreign key: a cursor over an identity gap remains valid
  and does not add a reverse lock edge.
- ready and expired reconciliation each have their own singleton table, row
  lock, last inspected `scope_key`, monotonic wrap count, and next-due scan
  sequence. Their keysets match the leading key of the existing class indexes.
  They are not aliases of the admission cursor and do not unnecessarily
  serialize each other.
- `docket_claim_partitions.admission_epoch` remains the only service epoch. It
  is separate from every cursor and is capable of committing atomically with
  the scan cursor and run outcomes. Schema v3 adds no second service ledger.

An `AFTER INSERT` trigger on `docket_claim_partitions` creates ring membership
in the same transaction. It covers RunStore, Admin, tenantless ownership,
direct supported creation, concurrent first writers, prefixes, and rollback.
Direct deletion or movement of a live partition's membership is rejected;
deleting the owning partition cascades its membership atomically. Fixed proof
windows exclude that cohort churn. The trigger creates membership only;
DCKT-77 owns lifecycle hints and repair.

The lock order remains outer policy/interlock, scan cursor, partition
authority, run rows, then any sidecar hint write. Scan discovery reads the
sidecar through MVCC and never locks it before partition authority. This avoids
a sidecar-to-partition edge against future run-to-sidecar lifecycle writers.

## Selected traversal

The candidate cohort is the single partial ring index, not a union of ready and
expired indexes. One recursive keyset seek reads the next raw position after
the durable cursor, falling back to the first position at wrap. It repeats for
exactly `S` visits when demand remains unfilled. Thus `H < S` revisits the
circular cohort instead of silently reducing the budget to `min(H, S)`.

Discovery does not use `FOR UPDATE SKIP LOCKED`. DCKT-78 must attempt partition
authority after reading each raw position, so a locked partition is an explicit
failed inspection that advances the cursor. Likewise, run locking accepts no
more than `K` exact IDs before `SKIP LOCKED`; it cannot scan past an unbounded
locked prefix to manufacture `K` successful locks. Final mutation input is
truncated to `Q` exact IDs.

For an inspected partition, the selected ready and expired candidate queries
each perform one exact-scope seek in the matching class index and return at
most the oldest `K` structural rows by eligible time and ID. DCKT-78 must rank
only their at-most `2K` union for class reservation, preference, attempt class,
and cap state, then feed at most `K` exact IDs to the lock shape and at most `Q`
locked IDs to mutation.

If a capped ready page has no permitted poison outcome, DCKT-78 must persist
its last `(wake_at, id)` in the schedule row without advancing service epoch.
The next target inspection uses the bounded continuation query: one exact-scope
seek after that tuple plus a residual seek from the start, returning at most
`K` rows across one wrap. Thus older cap-denied ordinary rows cannot pin the
page ahead of a later poison row. Normal below-cap ready selection ignores the
continuation and still starts at the oldest row; after a grant or cap relief,
DCKT-78 must clear or continue to ignore the saved token so normal age order
resumes. Expired work has the same continuation primitive. The number of failed
pages before a deep poison remains a trace-specific input to `L`, not a
universal numeric guarantee, but progress is possible for a fixed queue without
an unbounded attempt-class filter. The frozen Legacy separation trace uses
ordinary oldest ready heads and is inside the first page.

Ready and expired reconciliation are independent bounded recursive loose-index
walks. Each step seeks the next distinct scope head through the existing
`(scope_key, wake_at, id)` or `(scope_key, claimed_at, id)` partial index, then
performs one equality-on-`scope_key` due probe. One invocation reads at most its
class-local `R` unique heads across one wrap. Deep queues and partitions with
no row in that class therefore do not change the per-invocation work. For
`N_class` distinct scope heads represented in a stable class index, a complete
pass takes at most `ceil(N_class / R_class)` invocations. That recovery bound is
separate from all DCKT-75 grant, outcome, and scan-call bounds.

`scan_call_sequence` is the repair-cadence clock. DCKT-78 must increment it once
for every committed cursor-owned TenantFair poll statement, including an empty
cohort, and not for pre-authority failure or rollback. Ready is first due at
sequence 0, expired at 16, and each class advances its due value by 32. This
lets DCKT-77 repair a missing hint even when the admission cohort is empty.

## Rejected alternatives

- Schema-v2 eligible-run `UNION`, global grouping, and epoch ordering perform
  work that grows with eligible rows and tenants before returning a page.
- A global rank-before-lock window does proportional ranking work and lets
  contention invalidate the already-ranked page.
- Filtering two class indexes and merging them at poll time risks duplicate
  ring positions and a global distinct/sort.
- Sweeping durable partition membership for reconciliation makes a complete
  repair pass proportional to dormant partitions. The selected loose-index
  walk visits only unique class-index scope heads.
- `ORDER BY ... LIMIT n FOR UPDATE SKIP LOCKED` is not a work bound: PostgreSQL
  may scan arbitrarily many locked rows to return `n` unlocked rows.
- Filtering clean dormant membership before an unindexed limit is not a work
  bound. The selected `in_cohort` partial index pages the positive/dirty keyspace
  itself.

## Reproducible evidence

Run:

```sh
mix run bench/postgres/dckt_76_query_plans.exs
```

The harness saves `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plans plus normalized
machine summaries. The gate asserts required indexes, absence of selected
sequential/bitmap heap scans, recursive row ceilings, selected-index tuple
ceilings, scan-filter removals, and root cardinality. The committed PostgreSQL
15.1 run uses 20,000 partitions,
121 positive/dirty positions, a 50,000-row hot queue, 10,000 one-row tenants,
20,000 future timers, 10,000 expired rows, 60 stale/empty positives, cursor
wrap, an `H = 1 < S` repeated-wrap fixture, a locked first partition, and a
locked first exact run key.

| Shape | Returned rows | Index evidence | Rows removed by filters |
| --- | ---: | --- | ---: |
| rejected global grouping | 32 | mixed global scans | 30,000 |
| rejected rank-before-lock | 16 | global window | 89,984 |
| selected circular cohort traversal | 32 (`S`) | cohort ring index | 0 |
| selected `H < S` circular traversal | 32 visits | cohort ring index | 0 |
| selected ready/expired loose reconciliation | 32 (`R`) | scope-first class index | 0 |
| selected exact-partition candidate page | at most 16 (`K`) | scope-first class index | 0 |
| selected candidate continuation | 16 across at most one wrap | scope-first class index | 0 |
| selected cursor lock | 1 | cursor primary key | 0 |
| selected exact partition lock while locked | 0 | partition primary key | 0 |
| selected exact lock attempt with first key locked | 15 of `K = 16` | run primary key | 0 |
| selected mutation input | 8 (`Q`) | bounded input | 0 |

The machine-readable artifact is
[`bench/postgres/evidence/dckt-76-query-plans.json`](../../bench/postgres/evidence/dckt-76-query-plans.json).
Timing and buffers are regression evidence only. The logical DCKT-75 theorem,
qualification checks, finite `L`, and Legacy counterexample are not inferred
from these plans.

## Migration and release boundary

Fresh migrations install v3 and roll down to v1. `--upgrade-from-v1` installs
the current schema with v2 as its immediate rollback point;
`--upgrade-from-v2` installs and removes only v3. The supported migration is
transactional, stopped, and homogeneous. Tests cover fresh install, populated
v2 backfill, host v1-to-current, v3-to-v2, custom prefixes, rollback, schema
equivalence, and concurrent first partition membership.

This slice adds no weighted shares, borrowing, dynamic-population promise,
online rollout, evidence platform, admission engine, hint lifecycle, or
production high-cardinality trace.
