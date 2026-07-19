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

Legacy remains the default when `claim_policy:` is omitted.

## TenantFair

TenantFair adds sticky per-owner logical-run admission. Schema V2 supplies the
unfinished-tenant ring; schema V3 adds the admission marker and replaces the
ring function while preserving the frozen bounded-bypass rotation contract:

```elixir
use Docket,
  backend: Docket.Postgres,
  repo: MyApp.Repo,
  tenant_mode: :required,
  claim_policy: [
    implementation: Docket.Postgres.ClaimPolicy.TenantFair,
    default_max_active_runs: 4
  ]
```

`default_max_active_runs` is its only implementation option. It bootstraps an unset
database default; persisted values and partition overrides remain authoritative.

The current statement invokes `docket_tenant_fair_claim` once. Under the
serialized policy cursor it materializes a bounded positive-ring walk, attempts
partition authority, freezes bounded exact run IDs, and rechecks them before
mutation. It serves already-admitted ready or expired work before promoting the
oldest due queued run. Cooperative vehicle release and expired steals retain
the logical admission; future scheduling, external waiting, poison, and
terminal transitions release it. Two progress rules remain separate:

- the engine advances the domain-global circular scan cursor for every
  committed unfinished-ring visit, including lock skip, denial, dormancy, and
  emptiness; and
- it advances partition `admission_epoch` exactly once only for a
  committed nonempty grant; the epoch must never drive scan traversal.

DCKT-78 must linearize the cursor across independent pollers and enforce the
round/no-repeat rule: between target inspections, another partition may
receive at most one grant. A grant must return between one and the ratified
`Q` outcomes; a zero-outcome locked visit is an unsuccessful inspection.
TenantFair partition order will supersede Legacy global age-first order while
preserving the portable ready/expired reservation, demand-one
preference/fallback, poison, and stable within-choice age/ID behavior.

The exact bound, frozen qualification population, finite-opportunity
assumption, demand-aware scan-call formula, Legacy counterexample, and proof
oracle are normative in the linked contract. In particular, a finite lock hold
alone is not a numeric liveness bound, and timing benchmarks are not correctness
evidence.

See [Exact-cap and fair-rotation admission contract](docket-exact-cap-contract.md)
for the normative invariants and upgrade boundary.

## Configuration and rollout

The engine choice is instance-level, while its cap is database-wide. Do not
run a mixed deployment that includes binaries predating the `admission_mode`
interlock.

For an existing v1 installation:

1. stop Docket dispatchers and all run writers;
2. apply the generated transactional v1-to-current migration;
3. deploy one homogeneous application version;
4. configure every instance for the same engine; and
5. restart processing.

This stopped upgrade is the v0.1.0 operational contract. Online schema changes,
fleet attestations, readiness ledgers, activation ceremonies, audited mode
history, and hot mixed-version rollout are deferred.

Schema V02 contains TenantFair authority and ring state. Schema V03 is an
additive, stopped migration from transient claim caps to sticky admitted-run
caps. See [TenantFair schema-v2 active-ring decision](docket-tenant-fair-schema-v2.md)
for the ring and [TenantFair schema-v3 sticky admission](docket-tenant-fair-schema-v3.md)
for the admission lifetime and migration boundary.

## Test contract

The shared ClaimPolicy and live RunStore matrices verify implementation
selection, one-statement execution, decoded lease persistence, PostgreSQL error
preservation, transaction behavior, and telemetry. TenantFair adds live tests
for concurrent final-slot enforcement, cap reduction, sticky yield/reclaim,
FIFO promotion, expired recovery, cross-scope rotation, capped-head progress,
and the Legacy interlock. The ring suite must additionally prove linearized cursor traversal, no-repeat
rounds, lock/empty target failures, the bounded-bypass formulas, outcome-backed
epochs, the deterministic Legacy counterexample, and every inherited safety
invariant.
