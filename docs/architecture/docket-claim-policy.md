# PostgreSQL ClaimPolicy boundary

`Docket.Postgres.ClaimPolicy` is the internal phase-0 admission boundary for
detached durable runs. It exists so tenant-aware admission can replace the
legacy global selector without changing dispatcher, synchronous drain,
vehicle, executor, or focused run-store contracts.

## Stable caller and storage contracts

The supervised dispatcher and `Docket.Postgres.drain_runs/1` both resolve one
opaque ClaimPolicy value from backend configuration. For every poll they pass
the same runtime inputs: `now`, demand `limit`, `orphan_ttl_ms`,
`max_claim_attempts`, and the advisory ready/expired `preference`.

The selected implementation receives the effective portable claim policy and
must return the existing `Docket.Backend.RunStore.claim_batch()` result. It
delegates through the unchanged storage operation:

```elixir
RunStore.claim_due(context, :system, effective_policy)
```

One ClaimPolicy admission must remain one atomic PostgreSQL operation. An
implementation must not read capacity or candidates into application memory
and later issue a separate claim write. Fairness-specific policy fields may
select a different statement inside the run store, but the admission decision
and claim-token installation share one database operation.

## Implementation contract

An implementation declares `@behaviour Docket.Postgres.ClaimPolicy` and
provides:

- `init/1`, which validates implementation-specific backend configuration and
  returns opaque instance state;
- `claim_due/5`, which receives the configured run-store module, backend
  context, `:system` scope, effective portable policy, and the state returned by
  `init/1`.

Implementations preserve batch/error passthrough, all base policy fields, claim
fencing, ready/expired recovery, poison behavior, and the one-operation
admission invariant. The reusable source-owned contract under
`test/support/claim_policy_tests.ex` must run against every implementation.
PostgreSQL-specific locking and SQL-plan assertions remain in the run-store
suite.

## Selection and rollout

The backend defaults to `Docket.Postgres.ClaimPolicy.Legacy`, which delegates
to the existing tenant-blind SQL unchanged. An alternate is selected only in
instance-owned backend configuration:

```elixir
use Docket,
  backend: Docket.Postgres,
  repo: MyApp.Repo,
  claim_policy: [implementation: MyApp.DocketClaimPolicy]
```

The backend loads the module, checks `init/1` and `claim_due/5`, and runs
`init/1` before supervision starts. Manual and inline drains use the resolved
instance value; per-call options cannot switch implementations. A rolling
deployment must therefore treat implementation selection as backend rollout
configuration, not request data.

To roll back phase 0, remove the option or explicitly select
`Docket.Postgres.ClaimPolicy.Legacy`. Phase 0 adds no schema or fairness
behavior, so rollback requires no migration. Later implementations that depend
on schema or database policy versions must define their own compatibility and
mixed-version rollout checks.

Every admission emits
`[:docket, :postgres, :claim_policy, :admission]` with the bounded selected
module in `metadata.implementation`. Existing run-store and dispatcher events
remain present; dispatcher poll metadata also names the resolved ClaimPolicy.
Tenant, run, graph, token, and raw scope identities remain absent from metric
labels.
