# Exact-cap admission contract

This document records the v0.1.0 guarantees that the PostgreSQL TenantFair
claim engine must preserve. It intentionally excludes online rollout,
governance, audit, reporting, weighted service, and borrowing. Those are
post-MVP concerns.

## Authority and scope

The cap applies independently to each owner scope: `:tenantless` maps to the
empty scope key and `{:tenant, tenant_id}` maps to that tenant ID. A live claim
is a healthy `running` row with a non-null claim token. The effective cap is a
partition override when present, otherwise the persisted default.

The database is authoritative. `default_max_active` in application config is
used only to initialize an unset persisted default. Later changes go through
`Docket.Postgres.ClaimPolicy.Admin`.

## Required invariants

- Additive ready claims never make a scope's live count exceed its effective
  cap, including with concurrent callers from independent Repo pools.
- Recovering an expired claim replaces an existing live claim and is therefore
  count-neutral. It must not create an extra ready slot.
- Lowering a cap below the current live count creates admission debt. Existing
  work continues, but no new ready claim is admitted until the count is below
  the new cap.
- Poison resolution remains possible at the cap and consumes demand without
  installing a claim token.
- Every run creation transaction atomically creates its owner partition. A
  rolled-back run creation leaves no partition behind.
- Bounded discovery rotates considered partitions, including cap-denied ones,
  so a full scope cannot permanently pin a later eligible scope.
- Ready and expired eligibility, attempt class, and claim state are rechecked
  in the mutating command. A row that changed after discovery is not claimed
  from stale evidence.
- Admission runs only in a writable Read Committed transaction. Unsupported
  isolation, read-only transactions, and policy lock contention fail closed.

## Serialization

TenantFair discovers a bounded ordered page of eligible partition keys and
calls the prefix-qualified `docket_tenant_fair_claim_v1` function once. The
function locks partition authority with `SKIP LOCKED`, obtains a fresh live
count, and selects and mutates run rows in a later Read Committed command.
Partition locking serializes the final-slot decision across callers.

The function considers at most `demand + 1` partitions from the bounded page.
After each considered partition it increments `admission_epoch`; discovery
orders by that epoch, then scope key. This provides bounded cross-scope
rotation without a separate scheduler or reporting ledger.

## Engine interlock

The single `docket_claim_policy` row contains `admission_mode`. TenantFair
changes it to `tenant_fair` while holding the policy row; the Legacy statement
must hold the same row and proceed only while the value is `legacy`. This
prevents newly deployed Legacy and TenantFair instances from admitting at the
same time.

This is not an old-binary rollout protocol. Upgrading an existing installation
requires stopping all Docket writers and dispatchers, applying schema version
2, deploying one homogeneous application version, and then restarting. A
binary that predates the interlock cannot be made safe by new database code.

## Administration

The MVP administration surface is deliberately small:

- `get_default/1` and `put_default/3`;
- `put_override/4` and `reset_override/3`; and
- `get_effective/2`.

Writes accept an optional non-negative `expected_version` and return `:stale`
on compare-and-swap mismatch. Caps are positive PostgreSQL integers. There are
no actors, receipts, event replay, legal holds, exports, approval workflows,
or policy history tables in v0.1.0.

## Migration boundary

Schema version 2 installs the policy row, partition table, ordinary supporting
indexes, and claim function in one host-owned transactional migration. During
the v1-to-v2 migration, the runs table is locked against concurrent inserts
while existing scope keys are backfilled. The current binary requires schema
version 2 at runtime; schema version 1 is only an upgrade/rollback waypoint for
the previous binary.

This branch rewrites the unreleased DCKT-68 version-2 migration rather than
adding a version-3 conversion for its discarded development schema. A local or
test database that already applied DCKT-68 v2 must be recreated, or rolled back
with that matching code before adopting this branch. No released v2 database
is supported by this pre-0.1.0 cleanup.
