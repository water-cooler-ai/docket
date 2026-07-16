# DCKT-47 Exact-Cap Storage and Control-Plane Contract

Status: normative DCKT-63 contract for the DCKT-47 delivery slice

This document is the single authority for the DCKT-47 exact-cap storage,
bootstrap, administration, readiness, activation, and admission-serialization
contract. Where the broader tenant-fairness design or ClaimPolicy rollout guide
disagrees with this document, the reconciled DCKT-47 passages in those files
and this contract supersede the older proposal. Later tickets may change this
contract only in a separately reviewed specification change made before the
dependent implementation.

DCKT-47 is a safety and control-plane milestone. It can conservatively
under-claim, and bounded discovery can temporarily hide eligible partitions.
It makes no FAIR or LOCK bounded-lag promise. DCKT-49 owns rotation, candidate
hints/cursors, loose scans, and fairness SLOs; DCKT-48 owns preferred share,
borrowing, and reclaim lag; DCKT-45 owns weighted service. Completing DCKT-47
does not complete DCKT-44.

## Safety statement

For one physical PostgreSQL database and resolved schema prefix, let `L(k)` be
the number of rows for `scope_key = k` that are `running`, non-poisoned, and
have a non-null claim token. Expiry alone does not remove a live claim.

The exact-cap invariant is:

- an **additive** ready admission may change `L(k)` only from
  `pre_count < effective_max_active` to
  `post_count <= effective_max_active`;
- an expired-token steal replaces authority and is count-neutral;
- poison, release, cancellation, and terminal commit may only reduce `L(k)`;
- a policy downgrade or activation over pre-existing Legacy claims may create
  over-cap debt, but every later admission is non-increasing until the debt
  drains; and
- policy and administrative changes never terminate an existing holder.

`max_active` is therefore an exact ceiling on new durable claim authority, not
on simultaneously executing processes. A stale worker can overlap its
replacement physically until its next fence.

For one locked partition decision, let `L` be the fresh live count, `M` the
locked effective `max_active`, `D` remaining dispatcher demand after
count-neutral/poison outcomes, and `A_ready` the number of successful additive
updates. The mutation must enforce
`A_ready <= min(max(M - L, 0), D)` and therefore
`L + A_ready <= max(L, M)`, with a strict postcondition of `<= M` whenever
`L < M` before admission.

## Selected authority model

DCKT-47 preserves DCKT-46 exactly: `RunStore.claim_due/3` issues one client
query containing one top-level PostgreSQL statement, and callers, portable
results, ClaimPolicy callbacks, and the data-only plan boundary do not change.
The TenantFair plan's statement is a single call of the prefix-qualified
`docket_tenant_fair_claim_v1(...)` set-returning database function. The function
is `VOLATILE`, `PARALLEL UNSAFE`, `SECURITY INVOKER`, has a fixed safe
`search_path`, and uses fully qualified identifiers. The top-level statement
computes one bounded candidate partition-key array and calls the function
exactly once with that array; row-driven, lateral, or repeated function
invocation is forbidden. The array is a non-authoritative hint and is sorted
again inside the function before locking.

The function contains multiple ordered internal SQL commands. This distinction
is load-bearing: the one-statement contract is a client/RunStore boundary, not
a requirement that all database work share the top-level statement snapshot.
At PostgreSQL `READ COMMITTED`, each internal query of a `VOLATILE` procedural
function receives a fresh snapshot. The function first acquires nonblocking
authority locks and only then, in a later internal command, counts live claims
and mutates run rows. PostgreSQL documents both that successive Read Committed
commands can see intervening commits and that `VOLATILE` functions obtain a
fresh snapshot for each query they execute:

- [PostgreSQL 13 Read Committed isolation](https://www.postgresql.org/docs/13/transaction-iso.html#XACT-READ-COMMITTED)
- [PostgreSQL 13 function volatility and snapshots](https://www.postgresql.org/docs/13/xfunc-volatility.html)
- [`SKIP LOCKED` row-lock behavior](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE)
- [row-lock conflict modes](https://www.postgresql.org/docs/17/explicit-locking.html#LOCKING-ROWS)

Every admission rejects transaction isolation other than `read committed`
before mutation with
`{:error, {:claim_policy_unavailable, :unsupported_isolation}}`. It also
rejects read-only transactions. A transaction-scoped caller otherwise uses
the same function and locks; its changes and locks live until the caller's
outer commit or rollback. A returned lease is provisional and must not launch
work until that outer commit succeeds. Any database serialization/deadlock
failure aborts and retries the whole caller transaction; retrying only the
function call is forbidden. Long caller transactions are allowed but
observable as contention and are operationally discouraged.

### Why the obvious alternatives are not authority

The following designs were evaluated and are rejected for DCKT-47:

- A same-statement CTE that locks a partition and then aggregates live run
  rows is unsafe. Lock order, `SKIP LOCKED`, materialization, and syntactic data
  dependency do not refresh the statement-start snapshot.
- An `active_count` on the partition row would make every release, poison,
  terminal transition, cancellation, and token replacement participate in the
  counter protocol. Existing run-first lifecycle operations would either take
  the inverse partition lock, introduce deadlocks, or permit drift. A repaired
  counter cannot be exact while it is stale.
- A slot table has the same dual-authority problem: every token clear and
  replacement must atomically maintain both run and slot authority, while old
  binaries know only the run token.
- Multiple application-issued queries or an application-managed transaction
  would revise DCKT-46 and expose Repo/query authority to the ClaimPolicy seam.
- `SERIALIZABLE` would require a new portable retry contract and does not make
  non-participating Legacy writers safe.

Run rows remain the only live-claim authority. No counter, slot, trigger, or
repair process may authorize admission in DCKT-47.

## The stale-snapshot schedule

The mandatory known-bad control is this `READ COMMITTED` schedule at cap one:

1. T1 holds the partition lock and has established or is establishing one
   additive claim.
2. T2 starts its single statement and establishes the statement snapshot while
   T1 is still open.
3. T1 commits before T2 reaches its partition-lock plan node.
4. T2 immediately locks the current partition row. It did not wait and
   `SKIP LOCKED` did not fire, because the conflicting lock was gone.
5. A same-statement aggregate can still see T2's older snapshot, count zero,
   and incorrectly admit a second run.

The production function has only two outcomes at the corresponding boundary:

- if its internal partition-lock command runs while T1 holds the row,
  `SKIP LOCKED` skips it and no count or candidate query runs for that key; or
- if T1 has committed, the lock command acquires the current row, and the later
  internal count command takes a new snapshot that includes T1's commit.

DCKT-69 must reproduce the five-step bad control on PostgreSQL 13 and 17 and
must prove both production outcomes with barriers. SQL-shape or CTE-dependency
assertions are supplementary evidence, never the serialization proof.

## Prefix-owned schema

DCKT-64 owns the transactional schema and source-owned codecs. All names below
are exact. Each table is created inside the resolved prefix; `prefix: nil` is
resolved to its physical schema before identifiers are quoted. Nothing is
shared across prefixes.

### `docket_claim_policy`

This table always contains the singleton row `id = 1`. On a fresh or upgraded
v2 schema it is explicitly uninitialized:

```text
id                    smallint primary key default 1, check id = 1
preferred_active      integer null
max_active            integer null
weight                integer null
borrowing             boolean null
policy_version        bigint not null default 0, check >= 0
initialized_at        timestamptz null
updated_at            timestamptz not null default CURRENT_TIMESTAMP
```

The four policy fields are either all null or all present. Version zero is
equivalent to the all-null, uninitialized tuple; an initialized tuple has
version at least one. Present values satisfy
`0 <= preferred_active <= max_active <= 2_147_483_647`, positive integer
`weight`, and boolean `borrowing`. Database checks enforce
`(all_null AND policy_version = 0 AND initialized_at IS NULL) OR
(all_present AND policy_version >= 1 AND initialized_at IS NOT NULL)` plus the
numeric relationship.

Only `max_active` is enforced by DCKT-47. `preferred_active`, `weight`, and
`borrowing` are validated and preserved for forward compatibility but are
inert: they cannot affect discovery, ordering, admission, telemetry claims, or
SLOs in this milestone.

### `docket_claim_partitions`

```text
scope_key             text primary key
preferred_active      integer null
max_active            integer null
weight                integer null
borrowing             boolean null
admin_state           varchar(16) not null default 'running'
                      check in ('running', 'hold_new', 'drain')
partition_version     bigint not null default 0, check >= 0
admission_epoch       bigint not null default 0, check >= 0
inserted_at           timestamptz not null default CURRENT_TIMESTAMP
updated_at            timestamptz not null default CURRENT_TIMESTAMP
```

The override fields are one atomic tuple: all null means inherit the complete
database tuple, and all present means use the complete override. Partial
overrides and per-column `COALESCE` are forbidden and database-constrained.
Present tuples obey the same validation as the default. `admin_state` is
partition-local and does not inherit from the default. DCKT-47 adds no
eligibility hints, rotation cursor, service counter, or last-event shortcut.
The partition check is exactly `all_null OR (all_present AND numeric_valid)`.

`admission_epoch` is a monotonic serialization witness, not a live counter. An
admission that has nonblocking authority to evaluate additive work increments
it before the later fresh-snapshot count, even if rechecks ultimately produce
no lease. This makes each additive decision visibly mutate the current
authority row and provides a second failure signal if an unsupported
higher-isolation implementation ever bypasses the explicit isolation guard.
Release, steal-only, poison-only, commit, and repair paths do not change it.

Rows are not deleted by normal administration. The empty `scope_key` is the
one tenantless partition; a non-empty key is the canonical persisted tenant ID.
No payload, graph, checkpoint, node output, tier, or caller-provided scheduling
field can derive it.

For Admin CAS purposes, a normalized `scope_key` with no physical row has one
exact virtual before-image: all override fields null, `admin_state = 'running'`,
`partition_version = 0`, and `admission_epoch = 0`. `partition_version` is the
partition's control version; there is no separate `control_version` column.
This virtual row inherits the complete initialized default and is distinguishable
from a physical row only through the inspection field `partition_present`.

### Audit, replay receipts, gate, rollout, and capabilities

`docket_claim_policy_events` is append-only audit history with a monotonic
`audit_id`, target kind/key, operation, actor, source/event ID, request
fingerprint, before/after tuple and state, before/after version, database time,
and activation/readiness facts where applicable. It enforces prefix-wide
uniqueness on `(source, event_id)`. Rows are never updated.

`docket_claim_policy_receipts` has prefix-wide primary key
`(source, event_id)`. It stores the request fingerprint, ordered target hashes,
and the redacted original CAS result required to answer replay after later
updates. It never stores raw `scope_key` or owner-scope values. After a matching
request fingerprint proves the caller supplied the identical canonical target
list, the API reconstructs `target` only from that request and combines it with
the stored ordered versions/outcome. Receipts are not audit presentation and
are never removed while the prefix exists. Audit pruning therefore cannot
reopen an idempotency key.

`docket_claim_policy_holds` stores legal-hold references separately so audit
events remain immutable. `docket_claim_audit_exports` records completed export
watermarks used by pruning.

`docket_claim_admission_gate` is the low-churn singleton admission authority.
It alone stores readiness, admission mode, the required function contract, and
their epochs. `docket_claim_rollout` is a separate mutable operational ledger
for schema generation, dual-write attestation, backfill cursor, index validity,
foreign-key state, and missing-count evidence. Backfill progress never locks or
rewrites the admission gate. Schema generation 2 does not imply ready, and
ready does not imply active.

`docket_claim_capabilities` contains bounded, expiring registrations for
upgraded prefix writers/claimers: opaque instance ID, binary fingerprint,
writer contract, activation-gate contract, TenantFair function contract, last
heartbeat, and expiry. Expired rows cannot satisfy activation preflight and are
boundedly prunable. They do not contain tenant identity.

The complete auxiliary column sets, types, nullability, defaults, checks, and
initial values are frozen below. `bytea(32)` means `bytea` plus
`octet_length(value) = 32`.

```text
docket_claim_policy_receipts
  source                          varchar(64) not null
  event_id                        varchar(255) not null
  request_fingerprint             bytea(32) not null
  target_kind                     varchar(16) not null
                                  check in ('default', 'partition', 'bulk',
                                            'activation', 'readiness', 'audit')
  target_fingerprints             bytea[] not null, check cardinality > 0
  outcome                         varchar(16) not null
                                  check in ('applied', 'unchanged', 'demoted')
  previous_versions               bigint[] not null
  versions                        bigint[] not null
  audit_id                        bigint not null
  created_at                      timestamptz not null default CURRENT_TIMESTAMP
  primary key (source, event_id)
  check equal nonzero cardinality of target_fingerprints/previous_versions/versions
  check every fingerprint is 32 bytes and every version is >= 0

docket_claim_policy_events
  audit_id                        bigint generated identity primary key
  target_kind                     varchar(16) not null
                                  check in ('default', 'partition', 'bulk',
                                            'activation', 'readiness', 'audit')
  target_keys                     text[] not null
  operation                       varchar(32) not null
  actor                           varchar(255) not null
  source                          varchar(64) not null
  event_id                        varchar(255) not null
  request_fingerprint             bytea(32) not null
  before_value, after_value       jsonb not null
  before_versions, after_versions bigint[] not null
  mode_epoch                      bigint null
  occurred_at                     timestamptz not null default CURRENT_TIMESTAMP
  unique (source, event_id)
  check equal nonzero cardinality of target_keys/before_versions/after_versions

docket_claim_policy_holds
  hold_id                         uuid primary key
  first_audit_id, last_audit_id   bigint not null
  reason                          varchar(512) not null
  actor                           varchar(255) not null
  source                          varchar(64) not null
  event_id                        varchar(255) not null
  created_at                      timestamptz not null default CURRENT_TIMESTAMP
  check first_audit_id > 0 and last_audit_id >= first_audit_id
  unique (source, event_id)

docket_claim_audit_exports
  export_id                       uuid primary key
  through_audit_id                bigint not null check > 0
  location_fingerprint            bytea(32) not null
  actor                           varchar(255) not null
  source                          varchar(64) not null
  event_id                        varchar(255) not null
  completed_at                    timestamptz not null default CURRENT_TIMESTAMP
  unique (source, event_id)

docket_claim_assertions
  assertion_id                    uuid primary key
  assertion_kind                  varchar(32) not null
                                  check in ('dual_write', 'old_binaries_absent')
  evidence_fingerprint            bytea(32) not null
  actor                           varchar(255) not null
  source                          varchar(64) not null
  event_id                        varchar(255) not null
  asserted_at                     timestamptz not null default CURRENT_TIMESTAMP
  expires_at                      timestamptz null
  audit_id                        bigint not null
  unique (source, event_id)
  check (assertion_kind = 'dual_write' AND expires_at IS NULL) OR
        (assertion_kind = 'old_binaries_absent' AND expires_at > asserted_at)

docket_claim_rollout
  id                              smallint primary key default 1, check id = 1
  schema_generation               integer not null default 2, check = 2
  dual_write_assertion_id         uuid null references docket_claim_assertions
                                  on update restrict on delete restrict
  backfill_phase                  varchar(24) not null default 'not_started'
                                  check in ('not_started', 'running',
                                            'reconciling', 'complete')
  backfill_cursor                 bigint null check is null or >= 0
  backfill_batches, backfill_rows bigint not null default 0, check >= 0
  backfill_completed_at           timestamptz null
  backfill_last_error             varchar(512) null
  ready_index_valid               boolean not null default false
  live_index_valid                boolean not null default false
  fk_disposition                  varchar(16) not null default 'absent'
                                  check in ('absent', 'not_valid', 'validated')
  missing_partition_count         bigint null check is null or >= 0
  verified_default_fingerprint    bytea(32) null
  verified_at                     timestamptz null
  updated_at                      timestamptz not null default CURRENT_TIMESTAMP

docket_claim_admission_gate
  id                              smallint primary key default 1, check id = 1
  readiness                       varchar(16) not null default 'not_ready'
                                  check in ('not_ready', 'ready')
  readiness_epoch                 bigint not null default 0, check >= 0
  admission_mode                  varchar(16) not null default 'legacy'
                                  check in ('legacy', 'tenant_fair')
  mode_epoch                      bigint not null default 0, check >= 0
  required_function_contract      integer not null default 1, check = 1
  updated_at                      timestamptz not null default CURRENT_TIMESTAMP

docket_claim_capabilities
  instance_id                     uuid primary key
  binary_fingerprint              bytea(32) not null
  writer_contract                 integer not null check >= 0
  gate_contract                   integer not null check >= 0
  function_contract               integer not null check >= 0
  last_seen_at                    timestamptz not null
  expires_at                      timestamptz not null check > last_seen_at
```

A fresh install and a v1 upgrade both contain exactly three singleton facts:
the uninitialized policy row `(version 0, all policy fields null)`, the rollout
row `(generation 2, no assertion, phase not_started, null cursor, zero counts,
both index flags false, FK absent, missing count/default fingerprint/
verification null)`, and the
gate row `(not_ready, readiness epoch 0, legacy, mode epoch 0, function
contract 1)`. Every other v2 table is empty.

No other auxiliary foreign key exists. In particular, receipt/assertion/export
`audit_id` values and audit target keys are deliberately not foreign keys:
audit pruning must not delete receipts or assertions, and retained audit must
not keep a partition row alive. Their integrity comes from the atomic writer,
receipt uniqueness, immutable audit IDs, and export/prune checks.

### Run relationship and indexes

DCKT-65 makes partition assurance and run insertion one transaction for every
`RunStore.insert_run/5` path. After validating and serializing the run, lifecycle
creation performs one plain, non-locking MVCC partition-existence read:

- If the partition is already committed and visible, the writer skips the
  partition insert. It therefore does not wait behind an admission
  transaction's uncommitted `admission_epoch` update.
- If the partition is absent or a concurrent first writer's row is not yet
  visible, the lifecycle writer executes `INSERT ... ON CONFLICT DO NOTHING`.
  Concurrent first lifecycle/Admin inserts may wait only for ordinary
  uniqueness arbitration. The insert supplies only the canonical new-row
  values: null overrides, `admin_state = 'running'`, `partition_version = 0`,
  and `admission_epoch = 0`. On conflict it updates nothing, so an Admin-created
  row, control version, and admission epoch remain unchanged.

A failed or rolled-back run insert removes a partition newly inserted by that
transaction. A previously committed partition is never mutated by this path.

One `insert_run/5` call materializes at most one partition and therefore needs
no multi-partition ordering. A caller that deliberately starts runs for several
previously unseen scopes inside one outer transaction owns the ordinary
PostgreSQL uniqueness-ordering risk: acquire those scopes in ascending
canonical `scope_key` order or retry a detected deadlock. The lifecycle writer
does not reorder a caller's outer transaction, and this contract does not claim
global deadlock freedom for arbitrary multi-scope transactions.

DCKT-72 owns these exact tenant-leading partial indexes:

```sql
CREATE INDEX CONCURRENTLY docket_runs_scope_ready_claim_index
ON docket_runs
(scope_key, wake_at, id)
WHERE status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NULL
  AND wake_at IS NOT NULL;

CREATE INDEX CONCURRENTLY docket_runs_scope_live_claim_index
ON docket_runs
(scope_key, claimed_at, id)
WHERE status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NOT NULL;
```

The second index is the live-count and expired-candidate authority path.

The mandatory constraint is named
`docket_runs_scope_key_claim_partition_fkey`, relates
`docket_runs(scope_key)` to `docket_claim_partitions(scope_key)`, uses
`ON UPDATE RESTRICT ON DELETE RESTRICT`, and must be validated before readiness.
There is no waiver path. Readiness also requires fleet-wide
dual-write, completed backfill, a final zero-missing reconciliation, both valid
indexes, and an initialized default. If later verification finds an invalid FK,
invalid required index, missing partition, or changed default invariant, it
takes the admission gate exclusively, rechecks the failure, changes readiness
to `not_ready`, and increments `readiness_epoch`. In-flight admissions that
already hold the shared gate finish first; every later admission fails closed.
Before detection, a missing partition can only hide its work because no claim
command runs without a partition authority row.

## Ownership by module and ticket

| Responsibility | Concrete owner | Delivery ticket |
| --- | --- | --- |
| Transactional tables, checks, initial singleton rows, codecs, quoted identifiers, guarded down migration | `Docket.Postgres.Migrations.V02` and v2 schemas | DCKT-64 |
| Atomic partition upsert with run creation | `Docket.Postgres.RunStore.insert_run/5` | DCKT-65 |
| Public administration, bootstrap, CAS, receipts, audit export/hold/pruning | `Docket.Postgres.ClaimPolicy.Admin` | DCKT-66 |
| Bounded partition backfill and rollout cursor | v2 backfill operation | DCKT-67 |
| Online indexes, mandatory FK validation, readiness verification/demotion | v2 online migration/readiness operation | DCKT-72 |
| Capability registration, gate-aware Legacy, activate/deactivate CAS and preflight | activation interlock | DCKT-71 |
| `docket_tenant_fair_claim_v1`, TenantFair plan/decoder/observation | `Docket.Postgres.ClaimPolicy.TenantFair` plus its versioned database function | DCKT-68 |
| Barrier races, bad control, plans, PostgreSQL 13/17 evidence | exact-cap correctness suite | DCKT-69 |
| Exercised operator runbook and final traceability | operations documentation | DCKT-70 |

No DCKT-63 change creates a table, function, migration, API, or runtime path.

## Public administrative contract

The public control plane is `Docket.Postgres.ClaimPolicy.Admin`. It accepts
only a fully configured PostgreSQL backend context. It never accepts a bare
Repo, raw prefix, or raw `scope_key`. A target is `:tenantless` or
`{:tenant, non_empty_tenant_id}` and is normalized through the same owner-scope
code as run creation.

All Admin, Activation, Readiness, backfill, and audit mutators own their short
root-context transaction. They reject a transaction-scoped context, including
an otherwise clean outer transaction, with
`{:error, :transaction_context_forbidden}`. They therefore cannot follow a
caller-held run/default/partition lock and reverse the authority order. Bounded
inspection reads may use a transaction context but never mutate or lock an
authority row.

The exact functions are:

```elixir
bootstrap_default(context, policy, opts)
put_default(context, policy, opts)
put_override(context, owner_scope, policy, opts)
reset_override(context, owner_scope, opts)
put_state(context, owner_scope, admin_state, opts)
apply_partition_changes(context, changes, opts)
get_default(context)
get_effective(context, owner_scope)
get_prefix_state(context)
list_events(context, opts)
export_events(context, opts)
put_legal_hold(context, opts)
delete_legal_hold(context, hold_id, opts)
prune_events(context, opts)
```

### DCKT-66 Admin option and result freeze

The following details are normative for the DCKT-66 surface. They close option,
pagination, and external-export questions that the function list alone does not
answer.

- A valid Admin context is the resolved map returned by
  `Docket.Postgres.context/1`, including its Postgres-bundle marker, physical
  prefix, Repo, resolved ClaimPolicy, and factory-minted provenance bound to
  that exact Repo, physical prefix, resolved identifier set, and per-policy
  configuration. `ClaimPolicy.new/2` does not accept or copy provenance supplied
  by its input context, and there is no public provenance binding function. A
  bare Repo, a map assembled through documented public constructors from a
  Repo/raw prefix or arbitrary value, or a map containing a ClaimPolicy built
  separately through `ClaimPolicy.new/2` is not an Admin context.
  Copying an already valid resolved context remains valid. This in-VM provenance
  is structural misuse hardening, not an authentication or unforgeability
  boundary: arbitrary host code that can inspect a valid context can copy its
  opaque terms or reconstruct equivalent data. Before any mutator SQL, Admin also checks both the
  transaction-context marker and `Repo.in_transaction?/0`; either condition
  returns `{:error, :transaction_context_forbidden}`. Inspection reads accept
  the resolved transaction context.
- Policy and change maps have exactly the documented atom keys; unknown,
  string, duplicate keyword, and partial fields are rejected. Admin uses the
  same non-empty canonical owner-scope normalization as the existing stores;
  this ticket does not add an Admin-only tenant-length rule. Versions are PostgreSQL signed
  bigint values. A mutation requires `0 <= expected_version < 2^63 - 1` so its
  exact `N + 1` is representable. Source, event ID, actor, and legal-hold reason
  are non-empty valid UTF-8, reject NUL, and use UTF-8 byte limits matching the
  numeric schema widths (deliberately stricter than PostgreSQL's character-counting
  `varchar(n)`). Admin uses a
  fixed one-second `lock_timeout` and five-second `statement_timeout`; callers
  cannot weaken or expand them through options.
- Every event-bearing operation uses a versioned, length-delimited canonical
  request encoding. Version 1 covers operation, binary-ascending normalized
  targets (Elixir binary byte order, independent of database collation), every
  expected version, complete payload, source, and event ID. Actor is required,
  bounded, and audited but deliberately excluded from replay identity, as
  frozen above; retrying an identical event with a different actor returns the
  original receipt. The SHA-256
  digest of that encoding is the request fingerprint. Target fingerprints use
  a domain-separated version of the same encoding. A receipt is checked before
  authority acquisition and rechecked after every lock or uniqueness wait. If
  an audit/receipt uniqueness insert nevertheless loses, the whole transaction
  is rolled back and the request is classified once from a fresh transaction;
  no partially applied mutation can survive.
- `get_default/1` returns the complete tuple plus `version`, `initialized_at`,
  and `updated_at`, or `{:error, :not_initialized}`. `get_effective/2` has the
  exact fields stated below plus `readiness_epoch` and `admission_epoch` and is
  evaluated in one read-only `READ COMMITTED` transaction using one SQL
  statement. `get_prefix_state/1` returns the rollout columns, initialized
  default tuple/version/fingerprint, gate columns, live/total capability
  counts, audit/export watermarks, and dormant-partition count. It never claims
  readiness from those observations.
- `list_events/2` accepts only `after_audit_id` (default zero) and `limit`
  (default 100, maximum 500). It returns ascending immutable event maps and a
  `next_after_audit_id` cursor. Before/after JSON is decoded to maps/lists; raw
  scope keys occur only in this trusted audit surface.
- External export is deliberately two-step. The host first keyset-reads and
  durably writes audit through an ID, then calls `export_events/2` with
  `through_audit_id`, a 32-byte `location_fingerprint`, and actor/source/event.
  The call does not perform external I/O and does not return audit contents; it
  records the host's explicit completion attestation. A new watermark cannot
  move backward or exceed the current audit high watermark. The first
  attestation asserts complete external coverage from prefix history start
  through its ID; later attestations assert complete continuation through the
  new ID. The applied result contains `export_id`, `through_audit_id`, and its
  audit ID. Exact replay reconstructs that result from the lifetime receipt.
- `put_legal_hold/2` requires `first_audit_id`, `last_audit_id`, and `reason`.
  Its deterministic receipt-derived UUID makes replay reconstructable without
  storing raw targets in the receipt. Ranges must be positive, ordered, and no
  later than the current audit high watermark. Overlap is allowed and means
  union protection. `delete_legal_hold/3` requires that exact UUID; deleting
  one overlapping range does not weaken another.
- `prune_events/2` requires `cutoff: DateTime.t()`, actor/source/event, and a
  limit defaulting to 100 and capped at 500. It selects ascending IDs strictly
  older than the cutoff and no later than the contiguous export watermark,
  skips the union of legal holds, deletes that keyset only, then appends a new
  audit event outside the selected set. Its result contains `deleted_count`,
  `last_deleted_audit_id`, and the new audit ID. Receipts, export attestations,
  holds, and every live policy/gate/rollout/partition row are outside the delete
  statement and cannot be mutated by pruning.

Fresh default and partition CAS results and replays retain the exact shapes
already specified below. Fresh audit-control results use `outcome: :applied`;
matching replays use `outcome: :replayed` with the original result under
`original`. Invalid option/map input returns a bounded operation-family error
(`:invalid_admin_options`, `:invalid_policy`, `:invalid_target`, or
`:invalid_audit_options`) and never includes database text or another target.
Malformed bulk containers, entries, and duplicate normalized targets return
`:invalid_partition_changes`, `:invalid_partition_change`, and
`:duplicate_partition_target`, respectively. A database statement canceled by
the fixed five-second bound returns `{:error, :admin_timeout}` and is never
misreported as a row-lock timeout.

Every mutator requires bounded `source`, `event_id`, and `actor`. Every CAS
also requires `expected_version`. Bootstrap requires `expected_version: 0` and
is the only operation that can initialize the singleton. `ClaimPolicy.init/2`
is data-only and never calls bootstrap, inspects the database, or substitutes
instance configuration. TenantFair instance `default_*` values are reviewed
configuration inputs only; the host must pass chosen values explicitly to
`bootstrap_default/3`.

For a target at version `N`, a matching CAS commits exactly `N + 1`, its audit
event, and its receipt in one transaction. `apply_partition_changes/3` is
limited to 100 distinct targets, rejects duplicates, locks normalized
`scope_key` values ascending, and is all-or-nothing. Reset writes all four
override fields to null, preserves the row, state, and history, and increments
`partition_version`. A state change also increments `partition_version`.

An absent normalized partition is not `:not_found`. It has the virtual
before-image and actual control version zero defined above. Consequently,
`expected_version: 0` is the only CAS that can materialize it; any other
expected version conflicts with `actual: 0` and performs no durable insert.
Within the Admin transaction, single and bulk operations tentatively insert
every missing canonical version-zero row in ascending `scope_key` order with
`ON CONFLICT DO NOTHING`, then lock all target rows in that same order and
evaluate every CAS. A conflict rolls back every tentative insert. Only after
all versions match does the transaction apply the requested mutation:

- `put_override` writes the complete override, preserves the initial
  `admin_state: :running`, and commits partition version 1;
- `reset_override` materializes the inherited null override and running state
  and commits partition version 1; this is an applied structural mutation, not
  an unchanged result;
- `put_state` materializes the inherited null override, writes the requested
  state (including `:running`), and commits partition version 1.

There is no special absent-row no-op. A first reset or a first request for
`:running` still creates the canonical authority row because successful Admin
CAS establishes durable control ownership. Its audit before-image is the
virtual inherited/running version-zero record, its after-image is the physical
version-one record, and its receipt stores the ordinary redacted `:applied`
result. Exact replay returns that original result even after later changes.
Version conflict, validation, lock-timeout, or transaction rollback leaves the
row absent and creates neither audit nor receipt.

All partition Admin mutations, including a complete first override, first
require the singleton default to be initialized. If it is not, they return
`{:error, :not_initialized}` before row materialization and create neither
audit nor receipt.

Concurrent materialization rechecks after uniqueness arbitration: if a
DCKT-65 lifecycle upsert wins first, Admin locks its version-zero row and
applies normally; if another Admin wins, the loser observes the winner's new
version and returns the exact conflict; if Admin wins first, DCKT-65's
`ON CONFLICT DO NOTHING` preserves every Admin field. Bulk arbitration follows
the same rule for every target and rolls back all materializations and changes
if any post-insert recheck conflicts.

Each bulk change is exactly one of:

```elixir
%{owner_scope: owner_scope, expected_version: n,
  operation: {:put_override, complete_policy}}
%{owner_scope: owner_scope, expected_version: n, operation: :reset_override}
%{owner_scope: owner_scope, expected_version: n,
  operation: {:put_state, :running | :hold_new | :drain}}
```

The request fingerprint covers the operation name, normalized sorted target
list, every target's expected version, complete policy/state payload, source,
and event ID. A bulk conflict reports all mismatches in sorted target order as
`{:error, {:version_conflict, %{conflicts: [%{target: owner_scope,
expected: n, actual: m}]}}}`. No target changes when any conflict exists.
For an absent target, that entry's `actual` is exactly zero. A successful bulk
result reports `previous_version: 0` and `version: 1` for each target it
materialized, in the same normalized order as the target/version arrays in the
receipt and audit event.

Successful first application returns:

```elixir
{:ok,
 %{
   outcome: :applied,
   target: :default | owner_scope | [owner_scope],
   previous_version: non_neg_integer | [%{target: owner_scope, version: n}],
   version: pos_integer | [%{target: owner_scope, version: n + 1}],
   audit_id: pos_integer
 }}
```

A repeat of the same `(source, event_id)` and identical canonical request
fingerprint is checked before current-version CAS and returns:

```elixir
{:ok, %{outcome: :replayed, original: original_applied_result}}
```

This remains true after intervening mutations and audit pruning. Reuse with a
different fingerprint returns
`{:error, {:event_conflict, %{source: source, event_id: event_id}}}` and changes
nothing. A fresh event with a stale version returns
`{:error, {:version_conflict, %{target: target, expected: n, actual: m}}}` and
changes nothing. Bootstrap after initialization returns
`{:error, {:already_initialized, current_version}}`; a pre-bootstrap effective
read or mutation that needs the default returns `{:error, :not_initialized}`.
Invalid, unauthorized-context, not-ready, and bounded lock-timeout errors make
no receipt or audit event.

After a connection loss with unknown commit status, a control-plane caller
retries the identical source/event request; the durable receipt returns the
original result or the transaction applies once. It must not invent a new
event ID until that resolution. Admission has no source-event receipt: an
unknown claim commit is handled by durable token inspection and the existing
orphan-TTL/fencing recovery path. Blind immediate re-admission cannot exceed a
cap because any committed token is included in the next fresh live count.

Host authentication, authorization, billing-to-policy mapping, and actor
identity are application responsibilities. Docket validates the configured
administrative context, target form, bounded fields, tuple constraints, CAS,
and replay identity; it does not add a capability token that could be mistaken
for host authorization. The host must restrict access to the Admin API and to
resolved backend contexts from code that is not authorized to administer policy.

`get_effective/2` returns the complete effective tuple, `policy_source`,
default and partition versions, `partition_present`, state, live count, debt
`max(live_count - max_active, 0)`, readiness, mode, and epoch from one bounded
read transaction. An absent target returns `partition_present: false`,
`partition_version: 0`, `policy_source: :default`, and `state: :running`; a
physical row returns `partition_present: true`. `get_prefix_state/1` reports
schema generation, every online
phase, FK disposition, missing count, index validity, initialized default
fingerprint/version, capability summary, readiness, mode/epoch, audit/export
watermarks, and dormant partition count. Reads never activate, repair,
bootstrap, or prune.

The prefix interlock surface is
`Docket.Postgres.ClaimPolicy.Activation` with exact functions
`register_capability/3`, `attest_old_binaries_absent/2`, `preflight/1`,
`activate/2`, and `deactivate/2`.
`preflight/1` is an advisory, non-locking read that always returns this exact
shape for a valid context:

```elixir
{:ok,
 %{
   activatable: boolean,
   mode: :legacy | :tenant_fair,
   mode_epoch: non_neg_integer,
   readiness: :not_ready | :ready,
   readiness_epoch: non_neg_integer,
   required_function_contract: 1,
   live_capability_count: non_neg_integer,
   old_binary_assertion_expires_at: DateTime.t() | nil,
   reasons: sorted_reason_atoms
 }}
```

Here `activatable` is true exactly when `reasons` is empty. The report is not
an authorization token: activation reacquires the gate and rechecks every
predicate. Its reason set is the lexical-order subset of
`[:capability_mismatch, :function_contract_mismatch, :not_ready,
:old_binary_assertion_expired]`.
Activate/deactivate require `expected_epoch`, `source`, `event_id`, and `actor`;
activate additionally requires the unexpired old-binary assertion reference.
A matching CAS commits `expected_epoch + 1` and returns the same applied/replay
result shape above with `target: :activation` and `version` equal to the new
epoch. If a fresh event requests the already-current mode with the current
expected epoch, no epoch changes; it appends an `activation_unchanged` audit
event/receipt and returns `{:ok, %{outcome: :unchanged, target: :activation,
previous_version: epoch, version: epoch, audit_id: audit_id}}`. Stale epoch and
event reuse use the same exact conflict results.

Readiness is changed only through
`Docket.Postgres.ClaimPolicy.Readiness.attest_dual_write/2` and `verify/2`.
Both attestations require source/event/actor and a bounded external deployment
evidence fingerprint. The dual-write assertion is durable and has no expiry;
the old-binary-absence assertion requires a future expiry and must still be
live when activation takes its gate lock. Verification is the only operation
that can set `readiness: :ready` or demote it after drift.
`verify/2` requires `expected_readiness_epoch`, source/event/actor, and the
approved DDL fingerprints. Promotion returns the normal applied result with
`target: :readiness`; a successful repeat at the current epoch returns the
normal unchanged result. Drift demotion returns the exact `:demoted` result
defined below. A failing verification when already not-ready returns
`{:error, {:not_ready, sorted_reason_atoms}}` and creates no receipt.

Exact non-CAS failures are:

| Condition | Result |
| --- | --- |
| bare Repo, unresolved or wrong backend context | `{:error, :invalid_admin_context}` |
| mutator called inside an outer transaction | `{:error, :transaction_context_forbidden}` |
| admission not `read committed` | `{:error, {:claim_policy_unavailable, :unsupported_isolation}}` |
| admission in read-only transaction | `{:error, {:claim_policy_unavailable, :read_only_transaction}}` |
| data-plane gate/default or all-authority contention | `{:error, {:claim_policy_unavailable, :lock_contention}}` |
| selected engine is not authorized by current mode | `{:error, {:claim_policy_unavailable, :inactive_engine}}` |
| admission sees uninitialized default | `{:error, {:claim_policy_unavailable, :not_initialized}}` |
| control-plane lock timeout | `{:error, {:lock_timeout, authority}}`, where `authority` is exactly `:gate`, `:rollout`, `:default`, or `{:partition, owner_scope}` |
| activation while gate readiness is not ready | `{:error, {:activation_precondition_failed, :not_ready}}` |
| missing/expired old-binary assertion | `{:error, {:activation_precondition_failed, :old_binary_assertion_expired}}` |
| live capability missing required contract | `{:error, {:activation_precondition_failed, :capability_mismatch}}` |
| installed/caller function contract differs from gate | `{:error, {:claim_policy_unavailable, :function_contract_mismatch}}` |
| admission after readiness drift demotion | `{:error, {:claim_policy_unavailable, :not_ready}}` |
| readiness verification failure | `{:error, {:not_ready, sorted_reason_atoms}}` |
| malformed legal-hold UUID | `{:error, :invalid_hold_id}` |
| legal-hold UUID is well formed but absent | `{:error, :legal_hold_not_found}` |
| legal-hold range exceeds current committed audit high watermark | `{:error, :invalid_audit_range}` |
| export completion moves backward or exceeds committed audit high watermark | `{:error, :invalid_export_watermark}` |
| pruning has no completed export watermark | `{:error, :audit_export_required}` |

These errors create no receipt or audit event. CAS conflict/replay errors remain
the exact tuples defined above. `sorted_reason_atoms` is the lexical-order
subset of `[:backfill_incomplete, :default_fingerprint_changed,
:default_uninitialized, :dual_write_unattested, :foreign_key_unvalidated,
:gate_contract_invalid, :live_index_invalid, :missing_partitions,
:ready_index_invalid, :schema_generation]`; arbitrary database text is never
returned.

Audit-control failure precedence is bounded input/UUID validation, lifetime
receipt replay/conflict, rollout lock, post-lock replay/conflict, then the
range/watermark/existence predicate. Every failure in the table leaves policy,
partition, gate, rollout, audit, receipt, hold, and export state unchanged.

## Audit retention and privacy

Audit events are retained indefinitely unless a host invokes the distinct
trusted prune API. `prune_events/2` requires a database timestamp cutoff, a
completed export watermark covering every candidate, and a batch limit of at
most 500. It keysets by `audit_id`, skips legal-held events, deletes no receipt,
and cannot update policy, partition, rollout, or mode state. Legal holds are
separate append/delete control-plane facts with their own actor/source/event
receipts. Automatic age-based pruning is forbidden.

Every transaction that allocates a claim-policy `audit_id` takes the rollout
singleton `FOR UPDATE` before its policy/default/partition authority and before
identity allocation. This is the prefix-local audit commit-order barrier:
while one event-producing Admin transaction is open, no later allocator can
publish or attest a higher audit ID. Export completion and pruning take the
same barrier, so a visible high watermark cannot omit an in-flight lower event.
Future assertion, readiness, activation, or other control-plane writers that
allocate from this audit identity must participate in the same barrier in the
global gate -> rollout -> default -> partition order. Capability heartbeats and
other operations that allocate no audit ID remain lock-free under this rule.

Raw tenant IDs in partition rows and target-specific audit events can be
personal or confidential data. They never appear in metrics, logs, receipt
keys, capability rows, or error text. Receipts store a request hash and bounded
result, not the raw target. Export access and retention are host-controlled.
Dormant partition rows persist after runs and overrides disappear, so capacity
planning and erasure procedures must count them explicitly. DCKT-47 ships no
partition GC; destructive erasure requires proof of no referenced run and is a
later reviewed contract.

## Lock order and conflict contract

The total authority order is:

```text
admission gate -> rollout ledger -> database default -> partition rows ascending -> run IDs ascending
```

An operation omits rows it does not need but never reverses the remaining
order. Discovery reads, assertion/audit/receipt inserts, and capability
heartbeats do not grant authority and cannot justify reordering. PostgreSQL row
locks are held to transaction end. Mutators cannot run in a caller transaction,
so no earlier data-plane lock can precede this order.

| Operation | Gate row | Rollout row | Default row | Partition rows | Run rows | Contention behavior |
| --- | --- | --- | --- | --- | --- | --- |
| activation-aware Legacy admission | `FOR SHARE SKIP LOCKED` | none | none | none | existing `FOR UPDATE SKIP LOCKED` | held gate or all run rows skipped: prompt unavailable error |
| TenantFair admission | `FOR SHARE SKIP LOCKED` | none | `FOR SHARE SKIP LOCKED` | `FOR NO KEY UPDATE SKIP LOCKED`, ascending | `FOR UPDATE SKIP LOCKED`, ascending | gate/default/all-authority skip: prompt unavailable; partial locks: bounded partial batch |
| bootstrap/default CAS | `FOR SHARE NOWAIT` | `FOR UPDATE` | `FOR UPDATE` | none | none | bounded lock timeout, no mutation; rollout serializes audit allocation/commit order |
| partition/reset/state/bulk CAS | `FOR SHARE NOWAIT` | `FOR UPDATE` | `FOR SHARE NOWAIT` | `FOR NO KEY UPDATE`, ascending | none | bounded lock timeout, no mutation; rollout serializes audit allocation/commit order |
| dual-write attestation | none | `FOR UPDATE` | none | none | none | exact rollout timeout; assertion/receipt atomic with ledger link |
| backfill batch/final reconciliation | `FOR SHARE NOWAIT` | `FOR UPDATE` under one advisory runner | none | insert-only `ON CONFLICT DO NOTHING` | read-only keyset | activation excludes new batch; bounded batch/statement timeout |
| readiness promote or drift demote | `FOR UPDATE` | `FOR UPDATE` | `FOR SHARE` | read-only reconciliation | read-only verification | waits out admission with bounded timeout; gate change and evidence commit atomically |
| activate/deactivate | `FOR UPDATE` | `FOR SHARE` | `FOR SHARE` | none | none | waits out admission and rollout writer with bounded timeout; commits mode atomically |
| old-binary assertion | none | `FOR UPDATE` | none | none | none | audit-producing assertion participates in commit-order serialization |
| capability heartbeat | none | none | none | none | none | insert/upsert only; allocates no audit ID and is never authority by itself |
| audit export completion, legal-hold add/delete, audit pruning | none | `FOR UPDATE` | none | none | none | bounded rollout timeout; replay rechecked after the lock; policy/gate/partition state is never touched |
| run creation | none | none | none | plain MVCC existence read; only absent/invisible rows use unique `INSERT ... ON CONFLICT DO NOTHING`; mandatory FK takes `KEY SHARE` | insert | committed row: no partition-row wait; competing first writer: uniqueness may wait within configured timeout |
| refresh/release/commit/abandon/retry-poison | none | none | none | none | existing fenced run update | never introduces run-then-partition order |

`FOR SHARE` conflicts with a default or gate update while allowing concurrent
admissions. `FOR NO KEY UPDATE` conflicts with another partition admission or
policy update but remains compatible with the `KEY SHARE` lock used by an FK
check. `FOR UPDATE` on run rows preserves existing token fencing.

Data-plane admission never waits for a row-level gate, default, partition, or
run authority lock. `SKIP LOCKED` applies only to row locks, so DCKT-69 must
also bound table-lock acquisition with `lock_timeout`. Control-plane mutation
may wait only inside its documented short timeout and never continues after a
timeout.

Gate or default contention returns
`{:error, {:claim_policy_unavailable, :lock_contention}}`. If bounded discovery
found eligible partitions but every authority partition was skipped, the same
error is returned. The same error is returned when eligible candidates were
observed but every candidate run lock was skipped. Partial progress can return
a smaller batch plus bounded skip observations. A true empty eligible set
returns the ordinary empty batch.
This distinction prevents manual drain from reporting lock contention as queue
exhaustion: manual and inline drain propagate the exact unavailable error,
while supervised dispatch retries with normal bounded jitter.

DCKT-66/DCKT-71 must test every mutator with a transaction-scoped context both
before and after a caller has locked or changed a run; every case rejects before
issuing SQL. DCKT-69 must separately prove that a transaction-scoped admission
holds gate/default/partition/run locks to outer completion, publishes its lease
only after commit, and leaves no token/epoch change after rollback or exception.

## Admission command protocol

Inside `docket_tenant_fair_claim_v1`, each numbered item that says "command"
is a separate internal SQL command and therefore a fresh `READ COMMITTED`
snapshot:

1. Validate read-write `read committed` isolation, bounded arguments, function
   contract version, and demand.
2. **Command:** acquire the singleton gate `FOR SHARE SKIP LOCKED`; fail
   promptly unless mode is `tenant_fair`, readiness is true, and the stored
   required function contract equals the function's compiled contract.
3. **Command:** acquire the initialized default `FOR SHARE SKIP LOCKED`; fail
   promptly if absent, uninitialized, or contended.
4. Accept the top-level statement's bounded tenant-blind candidate-key array.
   It is a hint and makes no progress/fairness promise in DCKT-47.
5. **Command:** normalize/re-sort keys ascending and acquire partition rows with
   `FOR NO KEY UPDATE SKIP LOCKED`. Missing or skipped rows cannot feed any
   later count or candidate command.
6. **Command:** increment `admission_epoch` on successfully locked keys that
   can evaluate additive ready work. This is a serialization witness and does
   not reserve capacity.
7. For each successfully locked key, in bounded order, **command:** resolve the
   complete default-or-override tuple and count current authoritative live
   claims using the live index. No top-level or earlier-command aggregate is
   accepted.
8. **Command:** select bounded ready and expired candidates for that key, lock
   run IDs ascending with `FOR UPDATE SKIP LOCKED`, recheck eligibility, and
   apply the decision table. Additive selections are limited to
   `max(cap - live_count, 0)`; all successful additive updates for the key are
   in that one command.
9. Continue until demand or the documented key/query-work budget is exhausted.
   Return unchanged leases/poisoned outcomes plus bounded decoder observation
   columns. The function must be invoked exactly once by the plan.

Each live count and candidate command is parameterized by a key stored only in
the successfully locked in-function key set. Merely joining a CTE, however, is
not considered proof; barriers establish the actual command/snapshot order.

## Unified decision table

"Ready poison" means an unclaimed ready run has exhausted the attempt limit;
"expired poison" means a live expired run has exhausted it. Administrative
state controls admission only. It never changes the existing fenced results of
refresh, release, commit, abandon, or poison recovery.

| Operation / condition | `running` | `hold_new` | `drain` | Durable/live-count effect | Exact caller result |
| --- | --- | --- | --- | --- | --- |
| ready, `live < cap`, attempts remain | add up to `min(cap-live, demand)` | no additive claim | no additive claim | running adds within cap; others unchanged | lease(s) in ordinary claim batch, or empty when no selected row |
| ready, `live >= cap`, including cap zero/debt | no additive claim | no additive claim | no additive claim | unchanged | ordinary empty batch unless another outcome fills demand |
| ready attempt-limit poison | poison | poison | poison | no token installed; live unchanged | poisoned outcome consumes one batch demand |
| expired token, attempts remain | replace token | replace token | no steal | running/hold count-neutral; drain unchanged | replacement lease, or no outcome in drain |
| expired attempt-limit poison | poison and clear | poison and clear | poison and clear | live decreases by one | poisoned outcome consumes one batch demand |
| current non-expired token | ineligible | ineligible | ineligible | unchanged | no outcome |
| candidate changes before its run lock | recheck and skip | recheck and skip | recheck and skip | unchanged | no outcome; not classified as contention |
| gate/default unavailable, wrong mode, not-ready, or function mismatch | fail closed | fail closed | fail closed | unchanged | exact `claim_policy_unavailable` error |
| candidate partition lock skipped | skip key | skip key | skip key | unchanged; key is never counted | partial batch, or lock-contention error when every authority key skipped |
| eligible run locks all skipped | skip rows | skip rows | skip rows | unchanged | lock-contention error, never an exhaustion empty batch |
| `refresh_claim` with current token | allowed | allowed | allowed | `claimed_at` advances; live unchanged | `:ok` |
| `refresh_claim` with stale/missing token | allowed | allowed | allowed | unchanged | `{:error, :claim_lost}` |
| `release_claim` with current token | allowed | allowed | allowed | clears token, immediate wake; live decreases one | `:ok` |
| `release_claim` with stale/missing token | allowed | allowed | allowed | unchanged; newer token untouched | `:ok` (idempotent) |
| valid current-fence commit | allowed | allowed | allowed | retained claim stays count-neutral; park/terminal/cancel clears one | `{:ok, run}` |
| stale token/sequence commit | allowed | allowed | allowed | unchanged | `{:error, :stale_fence}`; malformed is `:invalid_commit`, missing is `:not_found` |
| current pre-execution abandon | allowed | allowed | allowed | clears one; reschedules or poisons | `{:ok, :rescheduled}` or `{:ok, :poisoned}` |
| stale/advanced abandon | allowed | allowed | allowed | unchanged | `{:ok, :stale}` |
| `retry_poisoned_run`, non-terminal | clear poison and wake | clear poison and wake but hold admission | clear poison and wake but drain admission | no token; attempts/abandons reset; live unchanged | `{:ok, run}`; already-unpoisoned is identical idempotent success |
| `retry_poisoned_run`, missing/terminal | same | same | same | unchanged | `{:error, :not_found}` or `{:error, :inactive_run}` |
| root-context claim statement commits | same admission rules | same | same | returned token is committed | claim batch lease is usable on return |
| transaction-scoped claim, callback later commits | same admission rules | same | same | token and all authority locks remain provisional through outer callback | backend returns `{:ok, value}`; lease becomes usable only after this return |
| transaction-scoped claim, callback errors/raises/rolls back | same admission rules | same | same | token/epoch changes roll back and locks release | backend returns/raises existing rollback result; provisional lease must be discarded |
| manual drain true exhaustion | same admission rules | same | same | no mutation | `{:ok, summary}` |
| manual drain claim contention/not-ready/mismatch | same admission rules | same | same | no bypass | exact claim error propagates as `{:error, reason}`, not an empty summary |
| inline drain claim contention/not-ready/mismatch | same admission rules | same | same | enclosing committed start/signal remains durable; drain does not bypass | inline facade returns the same `{:error, reason}` |

Ready and expired poison outcomes consume batch demand exactly as Legacy does.
At demand two or more the engine preserves ready/expired class progress when
both are visible in the bounded page; at demand one it preserves the existing
class preference/fallthrough. DCKT-47 does not promise that tenant-blind
discovery exposes every class or partition.

Normal polling, direct configured RunStore calls, recovery, supervised
dispatch, inline drain, and manual drain use this same table. No testing mode
can bypass mode, readiness, cap, state, poison, or lock behavior.

## Linearization proofs

### Global default changes

Every TenantFair admission holds the default row `FOR SHARE` before any
partition lock and until transaction end. A default CAS takes the conflicting
`FOR UPDATE` lock. If admission locks first, it completes under the old tuple
before the CAS can commit. If CAS commits first, the later admission internal
lock/read command sees the new tuple. No partition scan is needed to linearize
an inherited default change. A rolled-back CAS is invisible.

### Partition override and state changes

Admission and partition mutation take the same partition row in conflicting
`FOR NO KEY UPDATE` modes after gate/default locks. The winner completes before
the loser; admission either uses the complete before tuple/state or the
complete after tuple/state. Full-tuple checks prevent cross-version mixtures.

### Additive admissions

All additive claimers for one key serialize on its partition row. After the
winner holds and advances its `admission_epoch`, its later count command has a
fresh snapshot. No other
participating admission can add a token until the winner ends. It installs at
most `cap - live_count` tokens. Concurrent lifecycle operations can only clear
authority, so an unseen or intervening clear causes conservative under-claim,
not over-admission.

### Steal, poison, and stale tokens

An expired row already contributes one to the live count. Its run-row
`FOR UPDATE` lock makes token replacement atomic, so a successful steal leaves
one non-null token and increments no capacity. Attempt-limit poison clears the
token and reduces the count. A stale worker is rejected by the existing token
fence. A concurrent release or commit makes the eligibility recheck fail or
reduces the count.

### Downgrade and debt

Default and override downgrades linearize at their authority rows. They do not
touch run rows. If the post-change live count exceeds the cap, debt is reported
as `live_count - cap`; ready capacity is zero, steals remain neutral except in
`drain`, and clears reduce debt. Thus no admission after the committed
downgrade can increase the count until it is below cap.

### Release and terminal transitions

Release, cancellation, terminal commit, and non-admission poison keep their
existing single run-row fenced updates and never acquire a partition row. This
avoids inverse lock order. Because those operations only clear live authority,
they commute safely with a count performed while the partition is locked: a
count can be current or conservatively high, never dangerously low because of
the clear.

### Activation and engine exclusion

Activation's exclusive gate conflicts with every participating admission's
shared gate. Therefore activation either waits out a pre-existing admission
before changing mode, or changes mode first and makes every later Legacy gate
predicate fail. New attempts do not queue behind the exclusive holder; they
return promptly. The exclusive transaction rechecks readiness, default,
capabilities, external old-binary assertion, and function contract under the
same lock as the mode/epoch update. The external assertion is necessary because
an activation-unaware binary cannot be proven absent by a table it never
writes. This proof excludes mixed participating engines; fleet drain/absence
excludes the non-participating predecessor.

## Readiness, capability, and activation

Readiness is a durable verified fact, not schema presence. DCKT-72 may set
`ready = true` only when all of these hold in one verification:

1. schema generation 2 and every expected constrained table exist;
2. the database default is initialized and its fingerprint is recorded;
3. fleet-wide partition dual-write has an operator attestation;
4. DCKT-67 backfill and final zero-missing reconciliation are complete;
5. both exact tenant-leading indexes exist, are valid, and match the approved
   predicates;
6. the mandatory FK exists and is validated; and
7. the required gate and function-contract metadata exist.

`verify/2` first gathers non-authoritative evidence, then takes the gate
`FOR UPDATE`, the rollout row `FOR UPDATE`, and the initialized default
`FOR SHARE`, and re-runs every decisive catalog/count/fingerprint predicate
under that transaction. Promotion from not-ready to ready increments
`readiness_epoch`, stores the exact evidence/default fingerprint in rollout,
and returns an applied result. A successful repeat while already ready stores
no state change and returns the exact `:unchanged` result shape.

If verification fails while the gate is ready, the same exclusive transaction
rechecks the failure, sets not-ready, increments `readiness_epoch`, updates the
rollout evidence, appends a `readiness_demoted` audit/receipt, and returns
`{:ok, %{outcome: :demoted, target: :readiness, previous_version: n,
version: n + 1, reasons: sorted_reason_atoms, audit_id: audit_id}}`. It does not
change `admission_mode`: an active TenantFair prefix remains selected but
immediately fail-closed. In-flight shared-gate admissions finish before the
demotion commits; all attempts beginning afterward return the exact not-ready
error. No guarantee is made before drift is detected, but missing partitions
remain intrinsically non-admissible. Repair plus a later successful verification
promotes readiness and permits the still-selected engine again.

This confirms a real semantic dependency `DCKT-66 -> DCKT-72`: readiness
cannot be established before explicit default bootstrap. The delivery graph
and tracker must contain that edge.

Capability registrations prove only participating upgraded processes. An
activation-unaware old binary cannot be made visible retroactively, so the
database must never infer old-binary absence from heartbeats. Activation
requires both:

- no unexpired registered writer/claimer lacks the required partition-write,
  gate, and TenantFair function contracts; and
- an explicit operator assertion that activation-unaware binaries are absent
  or the fleet was stopped/drained, recorded with actor, deployment evidence
  reference, expiry, and source/event receipt.

An expired capability cannot satisfy preflight. An expired old-binary
assertion also blocks activation. This is the honest boundary between a
database interlock and fleet orchestration.

Activation takes the gate `FOR UPDATE`, which waits with a bounded timeout for
all activation-aware in-flight admissions holding `FOR SHARE`. New admissions
skip the queued/exclusive gate. Under that lock activation re-verifies
readiness, default fingerprint, capabilities, operator assertion, and installed
function contract; then it changes `legacy -> tenant_fair`, increments the mode
epoch exactly once, and appends audit/receipt state atomically. After commit,
gate-aware Legacy returns unavailable and TenantFair may admit. The function
reads and returns the current epoch under the gate; it never trusts a cached
mode or default.

Gate-aware Legacy does not need the TenantFair count function, but its existing
claim update must data-depend on a successfully acquired gate row whose current
mode is `legacy`. If activation commits after Legacy's statement snapshot but
before its gate lock attempt, PostgreSQL's Read Committed row-lock recheck sees
the updated gate row, the `mode = 'legacy'` predicate fails, and no run update
can execute. If Legacy acquires the shared gate first, activation waits for that
admission transaction to end. This same-row mode check is sufficient because
Legacy makes no cross-row cap decision; it does not rehabilitate a stale
same-statement live-count aggregate.

Legacy claims that remain at activation are counted by TenantFair. If they
exceed a cap they become ordinary non-preemptive debt. No ordinary mixed
Legacy/TenantFair admission or per-instance canary is permitted for one active
prefix.

## Two-release rollout and rollback

Release A is compatibility/readiness only:

1. install transactional v2 schema with an uninitialized default;
2. deploy partition-upserting writers and activation-aware Legacy everywhere;
3. register/heartbeat capabilities and prove activation-unaware binaries
   absent before final reconciliation;
4. explicitly bootstrap the reviewed default through the admin API;
5. run bounded backfill, online indexes, mandatory FK validation, and readiness
   verification; and
6. leave mode `legacy`. No exact-cap promise exists yet.

Release B contains the validated function and TenantFair engine:

1. deploy the matching function contract and TenantFair-capable binaries while
   the gate still authorizes only Legacy;
2. gather fresh capabilities and external old-binary absence evidence;
3. stop or fail closed any process that cannot meet preflight;
4. activate once through the CAS API; and
5. verify mode/epoch, debt, observations, and caller coverage before declaring
   the exact-cap guarantee.

Hot fallback is forbidden while claiming caps remain enforced. Rollback is:

1. stop new TenantFair polling and wait out or terminate admission statements;
2. call the audited deactivate CAS, which takes the exclusive gate, changes
   `tenant_fair -> legacy`, and increments epoch;
3. explicitly declare the exact-cap guarantee abandoned at that commit; and
4. restart gate-aware Legacy and inspect existing fences/claims.

Existing claim tokens remain fenced and are not mass-cleared. Destructive v2
down/teardown is refused while mode is TenantFair, readiness or retained
receipts exist, or runs reference partitions. Teardown requires a stopped
fleet, mode Legacy, exported audit, explicit receipt/partition data-loss
acknowledgement, and a separately invoked destructive operation.

## Runtime, migration, and tooling boundary

Only shipped runtime/migration APIs can change truth:

- V02 and the online migration create/validate schema objects;
- Admin APIs bootstrap and mutate policy/state/audit;
- readiness operations verify and mark phases;
- capability heartbeat records participating binaries;
- activate/deactivate changes mode; and
- ClaimPolicy plans perform admission.

Inspection, SQL examples, benchmark harnesses, preflight reports, dry runs, and
DCKT-70 commands report or invoke those surfaces; they cannot claim to repair,
activate, validate, or roll back by observation alone. DCKT-70 must exercise every
documented mutating command against the shipped API and label external fleet,
authorization, export, legal, and privacy work as host-owned.

## Acceptance traceability

| DCKT-63 criterion / contradiction family | Contract resolution | Evidence owner |
| --- | --- | --- |
| Concrete schema/bootstrap/readiness/activation ownership | Exact schema and ownership table | DCKT-64 through DCKT-72 |
| Separate global and partition linearization | Conflicting default-share/update and partition `NO KEY UPDATE` proofs | DCKT-66, DCKT-69 |
| Nonblocking lock order and stale snapshot | Total lock table, volatile-function command snapshots, mandatory bad schedule | DCKT-68, DCKT-69 |
| Full tuple, CAS, reset, replay uniqueness | Database checks, exact Admin functions/results, durable receipts | DCKT-64, DCKT-66 |
| Audit retention and legal hold | Immutable events, separate receipts/holds/exports, bounded prune | DCKT-66, DCKT-70 |
| Ready/steal/poison/recovery/drain/transaction/contention | Unified decision table and isolation/manual-drain results | DCKT-68, DCKT-69 |
| FK/readiness composition and later drift | Mandatory validated FK, separate gate/ledger, exclusive atomic demotion | DCKT-67, DCKT-72 |
| Old-binary/capability proof | Expiring capability leases plus non-inferable external absence assertion | DCKT-71, DCKT-69 |
| Two-release activation and rollback | Gate CAS sequence; mixed admission and hot fallback forbidden | DCKT-71, DCKT-70 |
| Prefix, retention, and privacy | Prefix-local objects, raw-key restrictions, dormant-row posture | DCKT-64 through DCKT-70 |
| Tooling versus execution | Explicit shipped mutator boundary | DCKT-70 |
| Later fairness non-goals | DCKT-49 rotation, DCKT-48 borrowing, DCKT-45 weighting are inert/handed off | DCKT-70 |

The contract is complete only when review accepts the selected fresh-snapshot
function proof. A future implementation that collapses its internal commands
back into one snapshot, adds a counter/slot authority, permits non-Read-
Committed execution, or bypasses the gate requires a new contract review.
