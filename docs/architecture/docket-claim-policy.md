# PostgreSQL ClaimPolicy boundary

`Docket.Postgres.RunStore.claim_due/3` is the stable PostgreSQL admission
entrypoint. ClaimPolicy is an internal engine seam behind that entrypoint, not
an alternate caller-facing store.

## Final call and ownership direction

Every production, recovery, manual, and inline admission follows one direction:

```text
dispatcher or synchronous drain
  -> RunStore.claim_due(context, :system, effective_policy)
    -> resolve the ClaimPolicy already stored in context
      -> selected implementation builds one data-only plan
        -> RunStore executes exactly one PostgreSQL statement
          -> the same selected implementation decodes the rows
            -> unchanged {:ok, claim_batch} | {:error, reason}
```

Admission rejects a bare Repo or `%{repo: repo}` because neither identifies a
configured backend instance. A runtime instance constructs its configured
policy once. The root context, dispatcher, manual/inline drain, and every
transaction-scoped context reuse that exact resolved value; per-call options
cannot replace it. Runtime startup passes the resolved root context separately
to `Docket.Backend.child_spec/2`; it is never injected into the backend's
option keyword list.

RunStore owns only policy-neutral context resolution, one query execution, and
generic result plumbing. It does not contain candidate SQL, ordering, caps,
poison rules, row decoding, selection statistics, or implementation-specific
claim telemetry.

Before plan construction, the boundary converts `now` to UTC at microsecond
precision. Every implementation therefore receives a value that can be bound
directly as `:utc_datetime_usec`, regardless of the caller clock's timezone or
declared precision; implementations must not depend on the caller's original
timezone representation.

## Implementation contract

An implementation declares `@behaviour Docket.Postgres.ClaimPolicy` and
provides four callbacks:

- `init/2` validates implementation configuration once per backend instance.
  It receives the normalized prefix and quoted identifiers, but no Repo or
  query capability, and returns opaque instance state.
- `build_plan/3` receives quoted identifiers, the facade-validated portable
  claim policy, and instance state. It returns one
  `Docket.Postgres.ClaimPolicy.Plan` without performing database I/O.
- `decode/3` receives only the rows from that plan's statement, the plan's
  data-only decoder contract, and instance state. It returns the portable
  claim batch plus bounded observation data.
- `observe/5` owns implementation-specific result, selection, attempt, poison,
  and error observations.

The facade validates that a plan contains one non-empty SQL statement, a
parameter list, a data-only decoder contract, and bounded data-only observation
metadata. A plan cannot supply a module or function that changes decoder
dispatch: decoding remains bound to the implementation selected in the
backend context. The per-admission builder receives no Repo, RunStore module,
query function, or executor callback.

A decoder exception or invalid return becomes
`{:error, {:claim_policy_decode_failed, reason}}` rather than escaping the
portable RunStore result contract. Because the atomic statement may already
have installed claim tokens, ordinary orphan-TTL recovery remains the safety
net for such an implementation defect. Observation callbacks are isolated:
they cannot change an already-decoded result or suppress the facade's generic
admission event.

Each admission must be one atomic PostgreSQL operation: one RunStore-issued
client query containing one top-level PostgreSQL statement. No callback
receives a Repo, backend/storage context, query function, RunStore module, or
executor callback, and the plan is data-only. The callbacks do receive the
sanitized plan context documented above. PostgreSQL statements may call
database functions, and trusted implementation code could obtain a globally
known Repo on its own, so this is an extension contract rather than a security
sandbox. The boundary never injects an application-side query capability.

Every implementation is registered once in the source-owned
`test/support/claim_policy_matrix.ex`; both the focused ClaimPolicy suite and
the live RunStore suite consume that registry. Each implementation runs the
same pure contract from `test/support/claim_policy_tests.ex`. The matrix covers policy construction and
selected-implementation binding, portable input validation, one-statement plan
shape, exact decoded batch passthrough, decoder failures, bounded observation
data, and the generic admission telemetry metadata. The source-owned
`test/support/claim_policy_run_store_tests.ex` integration contract separately
proves for every registered implementation that RunStore executes one selected
plan query, returns the complete decoded lease backed by the persisted claim,
and preserves PostgreSQL error class and metadata.
Instance-level selection across runtime paths remains covered by the direct
backend tests.

Both the matrix and its implementation fixtures are test-only; `test` is not
included in the package allowlist.

## Legacy engine

`Docket.Postgres.ClaimPolicy.Legacy` is the sole owner of the current
tenant-blind engine. Its plan retains the separate bounded ready and expired
index scans, materialized CTE fences, `FOR UPDATE SKIP LOCKED`, shared demand
limit, demand-one preference/fallthrough, multi-demand class progress,
claim-token installation, expired steal behavior, and max-attempt poison
mutation in one statement.

Legacy also owns parameter normalization, lease and poison decoding, candidate
and selection statistics, eligibility ages, fallback and steal accounting,
attempt/poison events, and the existing
`[:docket, :postgres, :run_store, :claim]` event. The facade adds exactly one
`[:docket, :postgres, :claim_policy, :admission]` event identifying the bounded
selected implementation. No event includes tenant, run, graph, or token
identity as metric metadata.

## Adding TenantFair

A future `Docket.Postgres.ClaimPolicy.TenantFair` should:

1. validate its instance configuration and required schema compatibility in
   `init/2`;
2. construct one atomic fairness statement in `build_plan/3` using only the
   supplied quoted identifiers and normalized portable policy;
3. return a data-only decoder and bounded observation contract in its Plan;
4. decode the unchanged lease/poison batch shape in `decode/3`;
5. emit bounded engine-specific observations in `observe/5`; and
6. run the reusable ClaimPolicy contract plus direct, transaction, supervised,
   manual-drain, concurrency, query-plan, and telemetry suites.

Selecting it requires only instance configuration:

```elixir
use Docket,
  backend: Docket.Postgres,
  repo: MyApp.Repo,
  claim_policy: [implementation: MyApp.TenantFairClaimPolicy]
```

No policy-specific change belongs in RunStore, dispatcher, synchronous drain,
vehicle, executor, or the portable `Docket.Backend.RunStore` contract. Rolling
back phase 0 means removing the option or explicitly selecting Legacy; this
boundary itself adds no migration, index, cap, or tenant-fairness behavior.

## Rollout and rollback

When `:claim_policy` is omitted from an instance, context construction selects
`Docket.Postgres.ClaimPolicy.Legacy`. This default is resolved once for that
instance and then preserved in root and transaction contexts. A bare Repo or an
unresolved `%{repo: repo}` is not an implicit Legacy instance and is rejected
for admission.

Roll out an alternate implementation in stages:

1. deploy its code and any separately reviewed schema prerequisites while all
   instances still omit `:claim_policy` and therefore use Legacy;
2. configure one canary instance with the top-level
   `claim_policy: [implementation: Module, ...]` option and restart it;
3. confirm startup accepts the implementation configuration, then monitor the
   generic admission event by `implementation` and `result` together with that
   engine's bounded observations; and
4. expand the same validated instance configuration through the fleet.

The switch is instance-level. Dispatcher, recovery, manual drain, and inline
drain calls cannot override it. Roll back by restarting affected instances
after removing `:claim_policy` or explicitly selecting
`Docket.Postgres.ClaimPolicy.Legacy`. Phase 0 needs no ClaimPolicy migration,
so rollback restores the legacy engine without a database downgrade.
