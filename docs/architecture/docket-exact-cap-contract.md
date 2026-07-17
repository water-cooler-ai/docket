# Exact-cap and fair-rotation admission contract

This document records the PostgreSQL TenantFair guarantees for Docket v0.1.0.
The exact-cap sections describe the implemented schema-v2 safety boundary. The
fair-rotation sections freeze the stronger DCKT-75 contract that schema v3 and
the later admission implementation must satisfy. Schema v2 does not yet satisfy
that fairness proof: it globally groups eligible runs and advances
`admission_epoch` after unsuccessful visits.

The contract intentionally excludes online rollout, governance, audit,
reporting, weighted service, preferred share, and borrowing. Those are
post-MVP concerns.

## Authority and scope

The cap applies independently to each owner scope: `:tenantless` maps to the
empty scope key and `{:tenant, tenant_id}` maps to that tenant ID. A live claim
is a healthy `running` row with a non-null claim token. The effective cap is a
partition override when present, otherwise the persisted default.

The database is authoritative. `default_max_active` in application config is
used only to initialize an unset persisted default. Later changes go through
`Docket.Postgres.ClaimPolicy.Admin`.

One fairness domain is one physical PostgreSQL database and resolved schema
containing the Docket tables. Configured prefixes or `search_path` aliases that
resolve to the same physical schema name the same domain. Tenantless ownership,
whose `scope_key` is the empty string, is one ordinary partition in that
domain; it is neither a wildcard nor a separate scheduling class.

## Exact-cap invariants

- Additive ready claims never make a scope's live count exceed its effective
  cap, including with concurrent callers from independent Repo pools.
- Recovering an expired claim replaces an existing live claim and is therefore
  count-neutral. It must not create an extra ready slot.
- Lowering a cap below the current live count creates admission debt. Existing
  work continues, but no new ready claim is admitted until the count is below
  the new cap.
- Poison resolution remains possible at the cap, consumes one unit of demand,
  and does not install a claim token.
- Every run creation transaction atomically creates its owner partition. A
  rolled-back run creation leaves no partition behind.
- Schema-v2 bounded discovery rotates every considered partition, including a
  cap-denied one, so a full scope cannot permanently pin a later eligible scope
  under continued polling. This is a qualitative progress property, not the
  schema-v3 bounded-bypass proof.
- Partition authority is locked before run rows. Live count, scope, ready or
  expired class, wake/cutoff, attempt class, claim state, and capacity are
  freshly rechecked before mutation.
- Admission runs only in a writable Read Committed transaction. Unsupported
  isolation, read-only transactions, engine-interlock conflict, and required
  authority-lock failure remain fail closed.
- Legacy and TenantFair cannot admit concurrently. The one-statement
  ClaimPolicy/RunStore boundary, claim-batch result, prefixes, tenantless
  ownership, manual and inline drain, dispatcher polling, and
  transaction-scoped calls remain compatible.

The one-statement rule is a client boundary, not a single-snapshot rule. The
prefix-qualified `VOLATILE` function locks partition authority in one internal
command and obtains a fresh Read Committed snapshot for live count and run
mutation in a later command. Partition locking serializes the final-slot
decision across callers.

## Fair-rotation qualification window

The proof targets one low-volume partition `t`. A qualification window opens
immediately before the first qualifying TenantFair scan after both of these
facts have committed:

1. `t` has a complete normal-path eligibility hint; and
2. an authoritative recheck would permit at least one outcome for `t`.

The window closes at the database linearization point of `t`'s first committed
grant. The qualification population is frozen for this v0.1 proof:

- `P` is a fixed set containing `t` and every partition allowed to receive a
  grant during the window;
- every member of `P` is continuously admissible throughout the window;
- no partition outside `P` receives a grant; and
- `C` is a fixed, duplicate-free cyclic hint population containing `P`.

Population or hint churn, a cap or administrative-policy change, engine or
schema change, loss of target admissibility, incomplete target hints, or a
rolled-back/error statement makes the window ineligible. It never turns a
pending or failed window into a pass. A later contract may publish a union-over-
window churn bound; v0.1 does not infer one from maximum simultaneous tenants.

Continuously admissible means that at each target mutation opportunity at
least one row survives the authoritative rechecks and is permitted by the
existing rules:

- an ordinary ready claim requires fresh live count below the effective cap;
- an ordinary expired steal is count-neutral and remains permitted at the cap;
- a ready or expired poison outcome remains permitted at the cap and installs
  no token; and
- cap debt excludes ordinary ready work but not an otherwise valid steal or
  poison outcome.

A transient partition or run lock does not make the target inadmissible. Lock
and mutation contention is accounted for by `L` below rather than hidden by
changing the population.

## Units and scheduler invariants

The following terms are normative:

DCKT-76 clarified the word "inspection" as a cursor visit rather than unique
membership: `C` remains duplicate-free, while a call with `H < S` may revisit a
position after a complete wrap. This is an erratum to the original "distinct"
wording, not a change to the round/no-repeat invariant or any formula below.

- An **inspection** is one visit to a hint position in durable cursor order.
  Membership in `C` is unique, but when `H < S` the cursor may complete a wrap
  and revisit a position in the same call. Lock skip, cap denial, stale
  evidence, or an empty recheck is an unsuccessful inspection.
- A **grant** is one committed acquisition of partition authority followed by
  `1..Q` committed lease or poison outcomes from that partition. A zero-outcome
  locked visit is not a grant.
- An **outcome** is one returned and committed ready lease, expired replacement
  lease, ready poison, or expired poison. Poison counts toward `Q` and caller
  demand but creates no live claim.
- A **qualifying scan call** is a successfully committed TenantFair statement
  that owns and advances the domain cursor. Calls that fail before cursor
  authority, fail closed, or later roll back contribute no scan, inspection,
  grant, outcome, cursor movement, or service epoch.

Let:

- `A = |P|`, the fixed continuously admissible grant population;
- `H = |C|`, the fixed unique hint positions, including stale-positive
  positions, so `1 <= A <= H`;
- `S >= 1`, the maximum hint-position visits a qualifying call may inspect;
- `Q >= 1`, the maximum outcomes one grant may return, further limited by the
  caller's remaining `policy.limit`; and
- `L >= 0`, the maximum consecutive target inspections that may fail before
  the next target inspection commits a grant.

`L` covers the complete outcome opportunity, not merely finite wall-clock lock
hold time. Partition-lock loss, run-lock loss, and a stale/empty mutation race
all consume a failed target inspection. "Every lock is eventually released"
does not establish a numeric `L`, because arbitrarily many other scans may
finish during one finite lock hold.

The scheduler must provide these domain-global invariants:

1. One durable circular scan cursor is linearized across all pollers. A caller
   cannot use a stale private cursor page to create duplicate service rounds.
2. Cursor movement is contiguous. Every inspected position advances the scan
   cursor, including denial, staleness, emptiness, and lock skip; an uninspected
   position is never skipped merely because caller demand was filled.
3. Unless caller demand is filled by an outcome, a qualifying call consumes
   its full `S`-position budget. Residual budget continues across cursor wrap.
4. Before the first target inspection and between consecutive target
   inspections, each other member of `P` may receive at most one grant. This is
   the load-bearing round/no-repeat rule; outcome-backed service epochs alone
   do not imply it.
5. At most `L` consecutive target inspections fail to produce an outcome; the
   next target inspection commits a grant.

TenantFair partition order supersedes Legacy's global age-first order. The
portable ready/expired class behavior does not change: demand one honors its
advisory preference with fallback, and demand of at least two reserves an
outcome for each nonempty class before filling remaining demand. That
reservation is carried across grants. Stable age/ID ordering still applies
within the choices left by partition rotation and the class reservation.

## Bounded-bypass proof obligation

Count in the cursor's database serialization order, strictly after the window
opens and strictly before the target grant. There are at most `L + 1` target-
inspection intervals. The round/no-repeat rule permits each of the other
`A - 1` partitions at most one grant per interval. Therefore:

```text
other-partition grants   <= (L + 1) * (A - 1)
other-partition outcomes <= Q * (L + 1) * (A - 1)
```

The claim-call bound must account for demand-stop behavior. In one target-
inspection interval, up to `A - 1` competitor grants can each fill demand and
end a call. The target plus the remaining `H - A` non-granting hint positions
consume full inspection-budget calls. Counting through and including the
target-grant call:

```text
qualifying scan calls
  <= (L + 1) * ((A - 1) + ceil((H - A + 1) / S))
```

The smaller `(L + 1) * ceil(H / S)` expression is not a claim-call bound when
calls stop after filling demand. For example, with `A = H = 2`, `S = 2`,
`L = 0`, and demand one, a competitor grant can end call one and the target is
not reached until call two. The smaller expression is valid only for a
separate discovery operation that always consumes a full budget independently
of mutation demand; v0.1 makes no such claim.

These are logical admission units, not elapsed time. Continued polling means
that qualifying committed calls continue to occur. The formulas imply no
millisecond, queue-wait, completion-time, throughput, CPU, memory, I/O,
processing-time, physical-execution, or unconditional starvation guarantee.

## Service accounting and cursor separation

`admission_epoch` is outcome-backed service evidence, not the scan cursor and
not an ordering substitute for the round/no-repeat invariant. In schema v3 it
advances exactly once per committed nonempty grant, regardless of whether the
grant returned one outcome or `Q` outcomes. It does not advance for denial,
staleness, cap rejection, emptiness, lock skip, error, or rollback.

The independent scan cursor advances for every committed inspection. Cursor
movement, any grant's `admission_epoch` increment, and its run outcomes are in
the same transaction; rollback persists none of them. The schema-v2 behavior
that advances `admission_epoch` after every considered locked partition is
provisional and must be replaced, not reinterpreted as v0.1 fairness evidence.

## Strict improvement over Legacy

The deterministic counterexample uses only ordinary ready work, demand one,
and stable age/ID ordering. Seed one hot partition with `N` older ready rows and
then make one low-volume partition ready. After each outcome, complete it before
the next call so both engines see continued capacity.

Legacy selects the `N` older hot rows before the low-volume row. Its tenant
bypass is therefore exactly `N` outcomes on this trace; choosing `N` larger
than any proposed constant disproves a backlog-independent Legacy bound.

For TenantFair on the same frozen two-partition trace with `L = 0`, the hot
partition receives at most one grant, or at most `Q` outcomes, before the
low-volume partition's grant, independently of `N`. Timing measurements may
compare the engines on the same machine, but latency and throughput are not the
correctness oracle.

## Proof evidence and telemetry boundary

The deterministic PostgreSQL harness is the correctness oracle. Its test-only,
identity-bearing trace must use a database-authored scan sequence, absolute
cursor before/after positions, per-call demand, and visit ordinal, and record
the inspected partition, lock/result disposition, outcomes, and before/after
service epoch. Caller timestamps, mailbox arrival, ordinary telemetry order,
and benchmark duration cannot order concurrent grants.

The current `FairRotationOracle` helper is the pure numeric scaffold: it checks
cursor continuity, per-call demand/budget use, nonempty grants, epoch deltas,
round/no-repeat, and the three bounds. Schema-v3 integration work must feed it
database-authored traces and separately prove fixed hint/admissibility fixture
coverage and the inherited live safety/class invariants.

Together, the trace oracle and the named live integration suites check:

- fixed and complete populations, worst cursor placement, wrap, and `L` target
  failures;
- `1..Q` outcomes per grant and zero outcomes for unsuccessful inspections;
- exactly one service-epoch increment per grant and none otherwise;
- contiguous committed cursor movement and no duplicate competitor grant
  between target inspections;
- all three bounds above; and
- every exact-cap, steal, debt, poison, lock-order, fail-closed, prefix,
  tenantless, interlock, class, and transaction invariant.

Default telemetry remains identity-free and is operational evidence only. The
future fair-rotation observation reports bounded aggregate scan pages,
positions inspected, cursor advances/wraps, partition locks/skips, grants,
leases, poison outcomes, cap denials, stale/empty visits, hint repairs,
reconciliation work, service-epoch advances, and work-budget exhaustion. It
never emits tenant, scope, run, graph, cursor token, or claim-token identity as
an ordinary metric label. Aggregate telemetry cannot reconstruct per-target
bypass and never replaces the deterministic trace.

## Engine interlock

The single `docket_claim_policy` row contains `admission_mode`. TenantFair
changes it to `tenant_fair` while holding the policy row; Legacy must hold the
same row and proceed only while the value is `legacy`. This prevents newly
deployed Legacy and TenantFair instances from admitting at the same time.

This is not an old-binary rollout protocol. Upgrading an existing installation
requires stopping all Docket writers and dispatchers, applying the required
migrations, deploying one homogeneous application version, and then
restarting. A binary that predates the interlock cannot be made safe by new
database code.

## Administration

The MVP administration surface is deliberately small:

- `get_default/1` and `put_default/3`;
- `put_override/4` and `reset_override/3`; and
- `get_effective/2`.

Writes accept an optional non-negative `expected_version` and return `:stale`
on compare-and-swap mismatch. Caps are positive PostgreSQL integers. There are
no actors, receipts, event replay, legal holds, exports, approval workflows,
policy history tables, cap zero, hold/drain states, weights, preferred share,
or borrowing in v0.1.0.

## Migration boundary

Schema version 2 installs the policy row, partition table, ordinary supporting
indexes, and exact-cap claim function in one host-owned transactional
migration. During the v1-to-v2 migration, the runs table is locked against
concurrent inserts while existing scope keys are backfilled.

The exact-cap cleanup rewrote the unreleased DCKT-68 version-2 migration rather
than adding a conversion for its discarded development schema. A local or test
database that applied that earlier development migration must be recreated, or
rolled back with matching old code before adopting the rewritten migration. No
released v2 database uses that discarded shape.

Schema version 3 is the additive DCKT-76 migration. It installs the unique
scheduling ring, one scan cursor, independent class reconciliation cursors,
and conservative hint fields without weakening schema-v2 safety. The current
binary requires schema version 3; version 2 is its immediate rollback point
and version 1 remains the older host upgrade waypoint. The ratified constants,
query shapes, and plan evidence are in
[TenantFair schema-v3 budgets and query shapes](docket-tenant-fair-schema-v3.md).

The supported upgrade remains stopped and homogeneous. Online migration and
readiness, governance, audit/evidence platforms, enterprise rollout,
preemption, reclaim guarantees, weighted service, borrowing, and resource or
processing-time fairness are explicitly deferred.
