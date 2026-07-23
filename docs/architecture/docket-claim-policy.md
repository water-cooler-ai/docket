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

The optional startup configuration callback receives a narrow query executor.
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
`legacy`. A `windowed` admission mode, a skipped policy lock, a read-only
transaction, or non-Read-Committed isolation fails closed before run
mutation.

Legacy remains the default for `tenant_mode: :none` when `claim_policy:` is
omitted. PostgreSQL `tenant_mode: :required` rejects Legacy and requires the
WindowedInterleave implementation.

## WindowedInterleave

`Docket.Postgres.ClaimPolicy.WindowedInterleave` is the sole required-tenancy
engine. One set-based claim statement samples active scopes in random order
and admits due work breadth-first across them: every sampled scope's
first-ranked run is considered before any scope's second-ranked run.
Admission is sticky within a scope — admitted due work ranks ahead of
unadmitted work, so in-flight runs are driven to completion before new runs
start — and no per-tenant cap is configured. The engine claims only under
the `windowed` admission mode, which startup normalizes last-boot-wins, and
takes no policy-row lock beyond the shared admission gate, so concurrent
dispatchers admit in parallel. Fairness across tenants is statistical rather
than deterministic. The module documentation is the authoritative contract.

Engine choice is instance-level. All instances sharing a domain must use one
homogeneous version and claim-policy configuration. Deployments must not mix
binaries that predate the `admission_mode` interlock. Operational details are
in the [PostgreSQL operations guide](../postgres-operations.md).

## Test contract

The shared ClaimPolicy and live RunStore matrices verify implementation
selection, one-statement execution, decoded lease persistence, PostgreSQL error
preservation, transaction behavior, and telemetry. Policy-specific coverage
lives with each implementation's test modules.
