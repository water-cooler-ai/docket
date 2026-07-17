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

TenantFair adds exact per-owner caps. Schema v3 and its replacement claim
function must add the frozen bounded-bypass rotation contract:

```elixir
use Docket,
  backend: Docket.Postgres,
  repo: MyApp.Repo,
  tenant_mode: :required,
  claim_policy: [
    implementation: Docket.Postgres.ClaimPolicy.TenantFair,
    default_max_active: 4
  ]
```

`default_max_active` is its only implementation option. It bootstraps an unset
database default; persisted values and partition overrides remain authoritative.

The current schema-v2 statement discovers a bounded result page only after a
global eligible-run grouping, orders it by `admission_epoch`, and invokes
`docket_tenant_fair_claim_v1` once. Inside the function, a fresh Read Committed
command locks partition authority, counts live claims, and selects and mutates
rows. Ready admission is limited to available capacity and expired steals are
count-neutral. Schema v2 advances every considered partition's epoch, including
cap-denied and empty visits. That mechanism is exact-cap-safe but is not the
DCKT-75 fairness proof.

Schema v3 separates two kinds of durable state:

- a domain-global circular scan cursor advances for every committed inspected
  hint position, including lock skip, denial, staleness, and emptiness; and
- partition `admission_epoch` advances exactly once only for a committed
  nonempty grant and never drives scan traversal.

The cursor is linearized across independent pollers. Between target
inspections, another partition may receive at most one grant. A grant returns
between one and the ratified `Q` outcomes; a zero-outcome locked visit is an
unsuccessful inspection. TenantFair partition order supersedes Legacy global
age-first order, while the portable ready/expired reservation, demand-one
preference/fallback, poison, and stable within-choice age/ID behavior remain.

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
2. apply the generated transactional v1-to-v2 migration;
3. deploy one homogeneous application version;
4. configure every instance for the same engine; and
5. restart processing.

This stopped upgrade is the v0.1.0 operational contract. Online schema changes,
fleet attestations, readiness ledgers, activation ceremonies, audited mode
history, and hot mixed-version rollout are deferred.

## Test contract

The shared ClaimPolicy and live RunStore matrices verify implementation
selection, one-statement execution, decoded lease persistence, PostgreSQL error
preservation, transaction behavior, and telemetry. TenantFair adds live tests
for concurrent final-slot enforcement, cap reduction, expired recovery,
cross-scope rotation, capped-head progress, and the Legacy interlock. The
schema-v3 suite must additionally prove linearized cursor traversal, no-repeat
rounds, lock/empty target failures, the bounded-bypass formulas, outcome-backed
epochs, the deterministic Legacy counterexample, and every inherited safety
invariant.
