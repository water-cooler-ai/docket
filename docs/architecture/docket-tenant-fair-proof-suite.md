# TenantFair proof-suite design

This document defines the executable evidence required to close DCKT-79. It
turns the DCKT-47 exact-cap invariants and the DCKT-49 frozen TenantFair claims
into a layered test suite. The normative behavior remains in
`docket-exact-cap-contract.md`, `docket-tenant-fair.md`, and
`docket-claim-policy.md`; this document describes how to prove it.

The suite proves a conditional, fixed-window logical bound. It does not prove a
production value for `L`, elapsed-time fairness, throughput, dynamic-population
fairness, strict round robin, or unconditional starvation freedom.

## Proof stack

The release claim is valid only when every lower layer is green. A higher layer
must not compensate for missing evidence below it.

1. **DCKT-47 safety base**
   - additive ready admission leaves the live count at or below the effective
     cap, including the final-slot race across independent Repo pools;
   - an expired steal is count-neutral;
   - a cap decrease creates non-preemptive debt and blocks additive admission
     until the live count is below the new cap;
   - ready and expired poison may progress at the cap without installing a
     claim token;
   - partition creation, run creation, and rollback are atomic;
   - partition authority is locked before run rows and mutation facts are
     freshly rechecked; and
   - engine interlock, transaction-mode checks, and authority failures are
     fail closed.
2. **Schema-v2 ring authority**
   - every partition has one immutable positive ring position;
   - `unfinished_count` equals an independent count of committed nonterminal
     runs;
   - the positive rows form the complete, duplicate-free ring `C`;
   - a zero-count row retains its identity but is not traversed; and
   - the singleton policy row is the only scan-cursor authority.
3. **Bounded-work mechanics**
   - `S = 32`, `Q = M = 8`, and `K = 16`;
   - an unfilled call visits exactly `S` positions;
   - one visit admits at most `K` structural IDs to exact locking and mutates
     at most `Q` rows; and
   - one call therefore attempts at most 512 exact run locks and accepts at
     most 256 outcome or mutation inputs, independent of queue depth.
4. **Scheduler local laws**
   - one durable cursor order is serialized across all pollers;
   - traversal is contiguous in sparse absolute ring order;
   - every inspected position advances the cursor, including lock skip, cap
     denial, staleness, dormancy, and emptiness;
   - filled demand stops immediately, while unfilled demand consumes `S`,
     including repeated wrap when `H < S`;
   - before the first target inspection and between target inspections, each
     competitor grants at most once;
   - every grant returns `1..Q` outcomes; and
   - `admission_epoch` advances once per committed nonempty grant and never for
     an unsuccessful inspection.
5. **Conditional bounded-bypass theorem**
   - the window is qualified from database evidence;
   - the supplied finite `L` is checked, never inferred from lock duration;
   - every qualifying trace satisfies all three frozen bounds; and
   - Legacy exhibits the specified backlog-dependent counterexample while
     TenantFair remains backlog-independent on the equivalent frozen trace.
6. **Compatibility and release matrix**
   - ClaimPolicy, RunStore, Admin, migration, prefix, tenantless, class,
     transaction, drain, dispatcher, ordinary, core-only, and live PostgreSQL
     suites remain green.

## Evidence ownership

The proof runner may choose fixture parameters and state the `target` and `L`.
It may not author facts that decide whether a trace passes.

| Fact | Required authority |
| --- | --- |
| Call order and identity | committed database journal under cursor authority |
| Commit or rollback | presence or absence of the transaction's journal rows |
| Cursor and visit order | production trace plus boundary cursor snapshots |
| Ring `C` and `H` | full ordered schedule snapshot and independent nonterminal counts |
| Cohort `P` and `A` | derive minimally as target plus actual pre-target grantees |
| Policy and engine | policy-row fingerprint and monotonic change evidence |
| Function/schema identity | resolved OIDs and function-definition hash |
| Target admissibility | fixture-specific authoritative recheck witness |
| Outcomes | trace identities cross-checked against durable run/token/poison state |
| Service | full partition epoch map, not only the declared cohort |
| Logical work | visit, candidate, exact-lock, and mutation counters |

Caller timestamps, task completion order, mailbox order, telemetry order,
transaction IDs, benchmark duration, `committed: true`, and
`instrumentation_complete: true` are not proof evidence.

## Test-only committed journal

Qualified database tests use a test-only wrapper and journal installed in the
isolated test schema. They are not production migration objects.

The wrapper participates in the same policy-row serialization as the
production claim function, invokes the production function, allocates a
database-authored scan-call ordinal while that authority is held, and writes
call, inspection, and outcome rows in the same transaction. A rollback leaves
no journal rows, cursor advance, epoch delta, continuation, or run mutation.

The committed journal contains at least:

```text
call:
  window_id, call_token, scan_call_seq, xid, demand
  cursor_start, cursor_end, inspection_count, outcome_count
  schema_oid, function_oid, function_hash, admission_mode, policy_version
  S, Q, K, M

inspection:
  call_token, visit_ordinal, cursor_before, cursor_after
  ring_position, scope_key, disposition
  ready_structural_count, expired_structural_count
  attempt_set_count, exact_lock_attempt_count, locked_count
  mutation_input_count, outcome_count
  epoch_before, epoch_after

outcome:
  call_token, visit_ordinal, outcome_ordinal
  run_id, scope_key, work_class, outcome_kind
  claim_token or poison identity
```

`scan_call_seq` must be strictly increasing and gap-free within the qualified
window. Cursor snapshots and row-count or digest checks reject omitted calls,
omitted trailing inspections, and reordered `H = 1` or full-wrap calls that a
cursor-only comparison could miss. Validation covers every inspection in the
target-grant call; only bypass counting stops immediately before the target
grant.

Trace mode must be an instrumentation-only view of production behavior. A
differential test seeds identical isolated schemas and compares trace on/off
results after removing trace-only columns. Durable cursor, epochs, continuation,
run mutations, claim/poison results, and ordering must match. A test-only
failpoint may coordinate a stale race only when explicitly enabled in trace
mode, must restore the caller's prior transaction settings, and must have a
negative control proving an ordinary production call cannot activate it.

## Qualification evidence

The harness opens a window only after all fixture facts have committed and no
pre-window claim call remains in flight. It records:

- the full ordered ring with exact `unfinished_count` values;
- an independent `COUNT` of nonterminal runs for every scope;
- the full cap, policy-version, epoch, and continuation map;
- resolved schema and function fingerprints; and
- one target-admissibility witness for the fixture's outcome class.

Boundary equality alone is insufficient because a fact may change and revert.
Test-only monotonic audit counters record zero/positive ring transitions,
policy/engine/cap changes, and target-affecting mutations for the duration of
the window. Alternatively, a fixture may structurally freeze a fact and prove
that proof workers lack authority to change it. The qualifier compares
positive membership for `C`; positive-to-positive unfinished-count changes are
allowed, while joins and leaves invalidate the window.

Target witnesses cover all contract-permitted forms:

- ordinary ready below cap;
- expired replacement at the cap and during cap debt;
- ready poison at the cap and during cap debt; and
- expired poison at the cap and during cap debt.

A partition lock miss, exact run-lock miss, or stale mutation race is a failed
target inspection counted by `L`; it does not make the target inadmissible.
Loss of every authoritative target outcome opportunity does invalidate the
window.

## Suite modules

Keep four evidence layers visible rather than accumulating one large proof
module.

### Pure oracle

`FairRotationOracleTest` owns formulas and the trace language. Move its helper
outside the PostgreSQL dependency guard so core-only CI can enumerate small
`A`, `H`, `S`, `Q`, `L`, demand, cursor, wrap, and disposition combinations.
It rejects malformed or strategically weakened evidence before any live test
depends on it.

### Ring and bounded-work kernel

`TenantFairRingTest` owns schema authority, exact unfinished counts, stable and
sparse positions, traversal, `S/Q/K/M`, exact locks, no `K + 1` substitution,
continuation, class reservation, poison, and epoch atomicity. It is
implementation-kernel evidence, not by itself a fairness theorem.

### DCKT-47 safety and integration

`ClaimPolicyTenantFairTest` and the existing RunStore/Admin/migration suites own
the inherited safety base: final-slot races, debt, steal, poison, fresh
rechecks, engine interlock, fail-closed modes, migration, prefix, tenantless,
transaction, drain, dispatcher, and global ready/expired behavior.

### Qualified database windows

`FairRotationDatabaseProofTest` contains only end-to-end DCKT-49 witnesses:

- two-partition `L = 0`;
- partition-lock, exact-run-lock, and stale-recheck `L = 1` cases;
- a mixed-mechanism `L = 2` case plus the same trace rejected with `L = 1`;
- a multi-competitor `A >= 3` no-repeat trace;
- `H = 1`, `H < S`, `H = S`, `H = S + 1`, sparse positions, worst cursor
  placement, and a long dormant population;
- saturated `Q` outcome and demand-aware call bounds;
- ordinary ready, expired, ready-poison, and expired-poison targets;
- positive-to-positive count changes that remain eligible;
- joins, leaves, policy/engine/function changes, target-admissibility loss,
  SQL errors, rollback, and trace gaps that become ineligible; and
- tenantless and custom-prefix qualified windows.

Legacy separation uses identical ordinary-ready, demand-one fixtures at
`N = 2`, `10`, and `1000`, completes each outcome before the next call, and
asserts selected run identities. `N = 1` may remain a smoke case but is not
part of the formal `N >= 2` separation.

## Shared support

Split proof support by responsibility:

- `FairRotationOracle`: pure arithmetic and trace validation;
- `TenantFairCase`: isolated database lifecycle and the second Repo pool;
- `TenantFairFixtures`: exact ring/run/cap state builders;
- `TenantFairTrace`: the canonical trace columns and raw invocation;
- `FairRotationWindow`: journal, qualification, snapshots, and durable-state
  cross-checks; and
- `PostgresBarrier`: backend-PID capture plus row/advisory lock and
  `pg_stat_activity`/`pg_locks` phase verification.

Timeouts bound a failed test; they never establish fairness. A mailbox may
release a worker, but PostgreSQL lock or wait state must prove that the worker
reached the intended phase.

## Required falsification controls

A proof suite must demonstrate that it rejects plausible false proofs.

### Proof-integrity controls

- shuffle two committed `H = 1` calls and omit one zero-grant full-wrap call;
- replay rolled-back rows while claiming they committed;
- truncate the target-grant call after the target inspection;
- fabricate an outcome, duplicate a run identity across calls, or omit a
  grant that changed an epoch;
- supply a strict superset of observed `P` to inflate `A`;
- join then leave the ring, change then restore policy, and make the target
  inadmissible then admissible between equal boundary snapshots; and
- introduce trace-only scheduling behavior and require the trace/production
  differential gate to catch it.

### Safety controls

- a known-bad stale-snapshot implementation over-admits a final slot under a
  deterministic barrier while the production implementation does not;
- mutants that count an expired steal as additive, admit ready work during
  debt, install a poison token, or remove partition authority fail; and
- unfinished-count mutants or direct writes, underflow, truncate, scope
  change, rollback, and concurrent creation are detected by independent state.

### Scheduler and work controls

- reject skipped/repeated cursor positions, early stop, post-demand scans,
  duplicate competitor grants, zero or `Q + 1` grants, epoch-per-outcome, and
  `L + 1` target failures;
- reject the smaller incorrect `ceil(H / S)` claim-call formula on the
  demand-one counterexample; and
- use known-bad global rank-before-lock and `K + 1` substitution shapes to
  prove hidden logical work cannot be inferred from emitted row counts alone.

## Claim-to-test anchors

| Claim | Primary live anchor | Required extension |
| --- | --- | --- |
| Exact final slot | `claim_policy_tenant_fair_test.exs` independent-pool race | stale-snapshot known-bad control |
| Debt, steal, poison | `claim_policy_tenant_fair_test.exs` | full state-transition matrix and durable deltas |
| Ring authority | `tenant_fair_ring_test.exs` | per-window independent-count snapshot |
| `S/Q/K/M` work | `tenant_fair_ring_test.exs` | journal candidate/lock/mutation counters |
| Cursor/rollback | `tenant_fair_ring_test.exs` | committed scan-call sequence and gap checks |
| Formula language | `fair_rotation_oracle_test.exs` | exhaustive small traces and falsification mutations |
| Live bounds and `L` | `fair_rotation_database_proof_test.exs` | qualified journal, `L=2`, smaller-`L` rejection |
| Legacy separation | `fair_rotation_database_proof_test.exs` | formal `N=2`, identity assertions |
| Migration/Admin/API | existing migration, Admin, ClaimPolicy, RunStore, drain, and dispatcher suites | remain release-gating |

## CI and completion gate

Use `:postgres`, `:tenant_fair`, `:proof`, and `:adversarial` tags, with one
contract identifier on each anchor test.

1. Ordinary and core-only lanes run the pure oracle and non-database suites.
2. PostgreSQL 13 and 17 run the focused ring, exact-cap, Admin, migration, and
   database-proof modules.
3. The full `mix test --include postgres` matrix remains the release
   non-regression gate.
4. PostgreSQL 17 repeats only deterministic adversarial cases across several
   ExUnit seeds.
5. Timing, warm-cache, random soak, and query-plan experiments remain
   diagnostic and non-oracle.

DCKT-79 is complete only when every claim in this document has a named anchor,
all proof-integrity controls fail for the intended reason, the trace/production
differential is clean, and the ordinary, core-only, and PostgreSQL release
matrices pass. Scenario count or a green full suite alone is not sufficient.
