# DCKT-47 Exact-Cap Delivery Plan

Status: epic orchestration plan for the `v0.1.0` release line

This document coordinates DCKT-47, the safety and control-plane work required
to ship database-wide exact tenant claim caps. It records delivery order,
review gates, and evidence requirements. It is not the normative storage,
control-plane, or admission-serialization contract. DCKT-63 owns those
decisions, and implementation tickets must follow the approved
[exact-cap contract](docket-exact-cap-contract.md) rather than treating
examples or questions here as normative behavior.

The design background and phased product direction remain in
[Tenant-Aware Claim Fairness](docket-tenant-claim-fairness-design.md). This plan
narrows the first delivery slice to exact dynamic hard caps, their trusted
administration, safe activation, and the evidence needed to operate them.

## Scope and non-goals

DCKT-47 coordinates one prefix-local, PostgreSQL-authoritative exact-cap path:

- versioned partition and policy storage;
- atomic partition lifecycle during run creation and backfill;
- a trusted, auditable dynamic-policy control plane;
- online schema readiness and prefix-wide activation interlocks;
- exact-cap enforcement in the TenantFair claim engine;
- concurrency, query-plan, inspection, activation, and rollback evidence; and
- compatibility proof for mixed-version deployments before activation.

The epic does not define the later fair-rotation, borrowing, or weighted-service
algorithms. It does not make `preferred_active`, `weight`, or borrowing an
enforced Phase 1 promise; replace host authorization; introduce caller-chosen
tenant identities; or turn administrative state into execution cancellation.
It also does not use this orchestration plan to decide the unresolved DCKT-63
storage, admission-serialization, locking, readiness, or compatibility
contract.

## Ticket dependency graph

In the following edges, `A -> B` means that B depends directly on A. These are
the validated ticket dependencies:

```text
DCKT-63 -> DCKT-64
DCKT-64 -> DCKT-65, DCKT-66, DCKT-67
DCKT-65 -> DCKT-67, DCKT-68
DCKT-66 -> DCKT-71, DCKT-68
DCKT-66 -> DCKT-72
DCKT-67 -> DCKT-72, DCKT-68
DCKT-72 -> DCKT-71, DCKT-68
DCKT-71 -> DCKT-68, DCKT-70
DCKT-68 -> DCKT-69, DCKT-70
DCKT-69 -> DCKT-70
```

Readiness requires an initialized database-authoritative default from DCKT-66
before DCKT-72 can prove the prefix ready. DCKT-63 therefore confirms
`DCKT-66 -> DCKT-72` as a real semantic and tracker dependency, not merely
stack ancestry.

Git and GitHub use a deterministic linear extension of the graph. This keeps
every pull request ticket-local and makes both fork legs ancestors of their
join without merge commits:

1. DCKT-63 — freeze the exact-cap storage, control-plane, and
   admission-serialization contract.
2. DCKT-64 — add the v2 claim-policy and partition schema.
3. DCKT-65 — add atomic claim-partition lifecycle and run creation.
4. DCKT-66 — add versioned dynamic claim-policy administration.
5. DCKT-67 — backfill partitions after dual-write is established.
6. DCKT-72 — add online indexes, foreign-key validation, and readiness proof.
7. DCKT-71 — add the prefix-wide activation interlock.
8. DCKT-68 — add the exact-cap TenantFair engine.
9. DCKT-69 — prove concurrency behavior and query plans.
10. DCKT-70 — publish inspection, activation, and rollback guidance.

This serialized order adds ancestry between some semantically independent fork
legs; it does not add a product dependency. If fork work begins concurrently
from a common parent, rebase the later branch onto the earlier branch before
opening its final stacked comparison. The join starts from that rebased later
branch. Do not duplicate a sibling with cherry-picks or join the legs with a
merge commit.

## Branch and pull-request topology

The epic branch is rooted at `v0.1.0`. Its real documentation change allows the
draft epic pull request to exist before implementation while keeping ticket
work out of the seed commit.

| Pull request | Base | Head |
| --- | --- | --- |
| DCKT-47 epic (draft) | `v0.1.0` | `codex/dckt-47` |
| DCKT-63 | `codex/dckt-47` | `codex/dckt-63` |
| DCKT-64 | `codex/dckt-63` | `codex/dckt-64` |
| DCKT-65 | `codex/dckt-64` | `codex/dckt-65` |
| DCKT-66 | `codex/dckt-65` | `codex/dckt-66` |
| DCKT-67 | `codex/dckt-66` | `codex/dckt-67` |
| DCKT-72 | `codex/dckt-67` | `codex/dckt-72` |
| DCKT-71 | `codex/dckt-72` | `codex/dckt-71` |
| DCKT-68 | `codex/dckt-71` | `codex/dckt-68` |
| DCKT-69 | `codex/dckt-68` | `codex/dckt-69` |
| DCKT-70 | `codex/dckt-69` | `codex/dckt-70` |

Every ticket branch therefore descends from `codex/dckt-47`, and every ticket
pull request compares only its work with the immediately preceding head. The
DCKT-47 pull request remains a draft orchestration root during implementation;
descendant ticket diffs do not appear in its comparison until the later landing
phase advances the epic branch.

No pull request is merged during implementation. Base updates use rebases, not
merge commits, and must preserve the order above. Once a branch has dependent
heads, coordinate any history rewrite through the whole descendant stack and
use lease-protected pushes. A green or approved intermediate pull request is a
review gate, not permission to land it early.

## Per-ticket review gates

Each gate is intentionally high level. Where behavior depends on the contract,
“approved contract” means the normative DCKT-63 result, not a choice made by
this plan.

| Ticket | Review gate |
| --- | --- |
| DCKT-63 | Approves one internally consistent storage, control-plane, and admission-serialization contract and closes every risk listed below with an exact decision table, transaction and lock model, migration/readiness semantics, and mixed-version capability proof. No runtime implementation is hidden in the contract PR. |
| DCKT-64 | Installs the approved v2 policy/partition shape with prefix-safe, versioned migrations and database invariants. The migration alone cannot activate the new engine. Upgrade and rollback behavior are tested. |
| DCKT-65 | Creates partition state atomically with run creation under the approved authority and lock rules. Its conflict path never updates an Admin-created row. Concurrent Admin/lifecycle creation, rollback, tenant scope, and existing lifecycle behavior remain covered. |
| DCKT-66 | Implements authorized, versioned, idempotent policy administration, including the exact absent-partition virtual version-zero/materialization CAS, stale-write rejection, effective-policy inspection, bootstrap/default handling, and durable receipt/audit behavior exactly as approved. Data-plane callers do not gain control-plane authority. |
| DCKT-67 | Establishes dual-write before an idempotent, restartable, bounded backfill. Concurrent run creation cannot leave gaps or overwrite newer state, and mixed old/new binaries remain within the approved compatibility envelope. |
| DCKT-72 | Builds required indexes online, validates the mandatory foreign key without an unsafe table-wide rollout step, and proves prefix readiness. DCKT-66 default bootstrap is a confirmed prerequisite and readiness must reject an uninitialized default. |
| DCKT-71 | Prevents partial or premature activation across one prefix. The interlock is database-authoritative, checks the approved schema/data/engine capabilities, and fails closed with actionable inspection evidence. |
| DCKT-68 | Enforces exact caps in one approved serialized admission decision while preserving replacement-steal, downgrade, administrative-state, fencing, poison, and bounded-batch semantics. Legacy behavior remains available until the interlock permits activation. |
| DCKT-69 | Demonstrates cap safety and progress under concurrent claim, release, commit, expiry, policy update, and activation pressure on PostgreSQL 13 and 17. A deterministic known-bad control uses barriers to enforce this order: T2 establishes its statement snapshot while T1 holds the partition; T1 commits; only then does T2 attempt the partition lock, immediately acquire the current row without waiting or triggering `SKIP LOCKED`, and retain the stale snapshot for same-statement aggregate or candidate reads. On both versions, the production path proves no over-admission under this schedule and, separately, prompt, bounded handling of genuinely held locks. Approved query shapes and bounded work have saved plan and benchmark evidence. |
| DCKT-70 | Documents effective-state inspection, staged activation, failure diagnosis, and rollback for operators. Every command or tool is exercised against the shipped surface; documentation does not claim runtime behavior that tooling alone cannot execute. |

Every implementation pull request also updates focused documentation and the
Unreleased changelog when it changes a shipped contract. Tests should name the
invariant they establish and include negative or failure-injection coverage at
new authority, transaction, compatibility, and activation boundaries.

## Validation matrix

Review is cumulative: a ticket runs the narrow tests for its changes and all
applicable rows below. The final DCKT-70 stack tip must pass the entire matrix
from one source revision.

| Gate | Required validation | Evidence |
| --- | --- | --- |
| Default/full | Fetch dependencies, compile with warnings as errors, check formatting, and run the default ExUnit suite with optional PostgreSQL dependencies present. | Exact source SHA, Elixir/OTP versions, commands, and test/compile result. |
| Core-only | With `DOCKET_CORE_ONLY=1`, fetch the dependency-free graph, force-compile with warnings as errors, and run the default suite; then restore the normal dependency graph. | Exact source SHA, environment, commands, and proof no PostgreSQL module leaked into core. |
| PostgreSQL 13 | Compile with warnings as errors and run `mix test --include postgres` against PostgreSQL 13. | Server major/settings identity, source and migration versions, commands, test result, and retained failure artifacts. |
| PostgreSQL 17 | Repeat the complete live PostgreSQL suite against PostgreSQL 17. | The same identity and result fields as PostgreSQL 13; do not substitute a newer-only pass for the minimum-version gate. |
| Tenant-fair benchmark | On both PostgreSQL 13 and 17, run `bench/postgres/tenant_fair_claim.exs -- --profile smoke --check` with an isolated database. For performance claims, also use a non-smoke, comparable artifact pair under the benchmark guide's fingerprint and sample requirements. | Complete artifact root: manifest, raw samples, summary, selected plans, query/DDL hashes, environment identity, and correctness eligibility. |

The canonical local commands for the non-database legs are:

```sh
mix deps.get
mix format --check-formatted
mix compile --warnings-as-errors
mix test

DOCKET_CORE_ONLY=1 mix deps.get
DOCKET_CORE_ONLY=1 mix compile --force --warnings-as-errors
DOCKET_CORE_ONLY=1 mix test
mix deps.get
```

The PostgreSQL 13 and 17 release legs each run the following after compiling
with warnings as errors against that server version:

```sh
mix test --include postgres
DOCKET_BENCH_DATABASE_URL=postgres://USER@HOST/DATABASE \
  mix run bench/postgres/tenant_fair_claim.exs -- --profile smoke --check
```

The smoke profile is a correctness, bounded-work, artifact, and coarse-plan
gate. It is not statistically meaningful latency or throughput proof. Any
performance comparison must obey the environment, workload, query/DDL hash,
sample-count, and eligibility rules in the PostgreSQL operations guide.

## Evidence and traceability

Each pull request description must identify its DCKT ticket, its exact base and
head, direct dependency tickets, approved DCKT-63 contract revision, changed
invariants, migrations or durable formats, and the validation rows it ran.
Link review claims to stable test names or invariant IDs rather than reporting
only a green aggregate. Record every skipped matrix row with the downstream
ticket that will close it; a skip is not a pass.

Database evidence includes the PostgreSQL major and relevant settings, schema
prefix, migration versions, old/new binary or engine identities where
compatibility is under test, and enough commands and logs to reproduce the
result. Concurrency claims retain seeds, worker counts, timing/failure-injection
controls, and the observed invariant outcome. Benchmark evidence retains the
whole immutable artifact directory, not a copied summary or screenshot.

When a later ticket changes an earlier assumption, update the contract or the
owning focused design document first, then cite that revision from the later
pull request. The stacked diff and ticket links must let a reviewer trace each
shipped behavior from decision, through migration and implementation, to test
and operator guidance.

## Contract risks resolved by DCKT-63

The [exact-cap contract](docket-exact-cap-contract.md) closes the following
questions. The list remains here as the review checklist for dependent
implementation; examples in this plan cannot override the selected answers.

1. **Configuration and bootstrap authority.** Which database value is
   authoritative before a tenant-specific row exists; who may initialize or
   change it; how concurrent first use resolves; and how the default becomes
   inspectable and ready for activation.
2. **Audit retention versus replay uniqueness.** How long policy audit records
   live; whether source-event idempotency keys may be pruned; and what durable
   mechanism continues to reject a replay after audit retention removes its
   original record.
3. **Foreign-key and readiness semantics.** Which rows and relationships must
   exist and be validated before a prefix is ready; how dual-write/backfill,
   online index creation, foreign-key validation, and default initialization
   compose; and what happens after later drift.
4. **Exact decision table.** The complete outcomes for ready admission,
   expired replacement, upgrades, downgrades, zero caps, holds, drains,
   missing/corrupt policy state, stale writes, and transitions with retained
   claims, including transaction-visible counter and audit effects.
5. **Lock modes and order.** The precise rows, PostgreSQL lock modes, and global
   acquisition order for policy updates, partition lifecycle, claim decisions,
   backfill, and activation so correctness does not depend on an accidental
   query plan and deadlock risk is bounded.
6. **`READ COMMITTED` snapshot and lock serialization.** T2 can establish its
   statement snapshot while T1 holds the partition, then T1 can commit before
   T2 attempts the partition lock. T2 immediately acquires the current row: it
   does not wait, and `SKIP LOCKED` does not trigger. Aggregate or candidate
   reads in T2's statement can nevertheless retain the earlier snapshot. Lock
   order, `SKIP LOCKED`, and row data-dependencies are insufficient. The
   selected model preserves DCKT-46 with one call to a `VOLATILE` database
   function, then separates its nonblocking lock and fresh-snapshot count into
   different internal commands.
7. **Old-binary and engine-capability proof.** How the system identifies every
   writer and claimer capability relevant to a prefix, what mixed-version
   window is supported, how stale processes expire from the proof, and why an
   old binary cannot bypass or corrupt activated exact-cap state.
8. **Operational execution versus tooling.** Which operations are real runtime
   or migration capabilities versus inspection, planning, or dry-run tools;
   who executes activation and rollback; and which failure modes can be
   automatically repaired rather than merely reported.

Dependent tickets must implement these approved resolutions. A proposed change
updates the normative contract first and passes the same adversarial review;
an implementation cannot silently replace the authority model.
