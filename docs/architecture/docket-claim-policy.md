# PostgreSQL ClaimPolicy boundary

`Docket.Postgres.RunStore.claim_due/3` is the only PostgreSQL admission
entrypoint. ClaimPolicy is an internal engine seam behind RunStore, not a
second store API and not a per-call option.

## Ownership

Each backend instance resolves one ClaimPolicy during context construction.
The dispatcher, manual and inline drains, recovery, and transaction contexts
reuse that value. RunStore validates portable claim input, executes one
statement, and returns the decoded result. The implementation owns selection
SQL, row decoding, poison behavior, and claim observations.

An implementation supplies:

- `init/2` to validate instance options;
- `build_plan/3` to return a data-only `%ClaimPolicy.Plan{}`;
- `decode/3` to return a lease/poison batch or bounded policy error; and
- `observe/5` for implementation-owned telemetry.

The plan builder receives quoted table identifiers but no Repo or query
callback. RunStore executes exactly one top-level PostgreSQL statement and
always invokes the decoder from the already-selected implementation. Decoder
and observation failures cannot switch engines.

## Legacy

`Docket.Postgres.ClaimPolicy.Legacy` preserves the tenant-blind ready/expired
claim algorithm, demand-one preference, `FOR UPDATE SKIP LOCKED`, claim-token
installation, expired steals, and maximum-attempt poison behavior in one
statement.

Legacy also participates in the minimal engine interlock. It takes a shared
lock on `docket_claim_policy` and admits only while `admission_mode` is
`legacy`. A TenantFair mode, a skipped policy lock, a read-only transaction,
or non-Read-Committed isolation fails closed before run mutation.

Legacy remains the default for `tenant_mode: :none` when `claim_policy:` is
omitted. PostgreSQL `tenant_mode: :required` rejects Legacy and requires the
TenantFair implementation with an explicit cap.

## TenantFair

`Docket.Postgres.ClaimPolicy.TenantFair` is the sole required-tenancy engine.
It invokes the prefix-local claim function once; the function owns sticky
admission, bounded ring traversal, locking, mutation, and cursor/epoch
accounting. The [TenantFair claim policy](docket-tenant-fair.md) covers its
configuration, mechanics, formal bounds, rollout, nonclaims, and correctness
evidence.

Engine choice is instance-level. Deployments must not mix binaries that predate
the `admission_mode` interlock. Current stopped-upgrade commands and operational
inspection live in the [PostgreSQL operations guide](../postgres-operations.md).

## Test contract

The shared ClaimPolicy and live RunStore matrices verify implementation
selection, one-statement execution, decoded lease persistence, PostgreSQL error
preservation, transaction behavior, and telemetry. The
[TenantFair correctness evidence](docket-tenant-fair.md#correctness-evidence)
lists its policy-specific tests and current validation status.
