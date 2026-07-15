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

Direct calls with a bare Repo or `%{repo: repo}` have no backend instance from
which to resolve configuration, so they select `ClaimPolicy.Legacy`. A runtime
instance constructs its configured policy once. The root context, dispatcher,
manual/inline drain, and every transaction-scoped context reuse that exact
resolved value; per-call options cannot replace it.

RunStore owns only policy-neutral context resolution, one query execution, and
generic result plumbing. It does not contain candidate SQL, ordering, caps,
poison rules, row decoding, selection statistics, or implementation-specific
claim telemetry.

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

The structural guarantee is one RunStore-issued client query containing one
top-level PostgreSQL statement. No callback receives a Repo, context, query
function, RunStore module, or executor callback, and the plan is data-only.
PostgreSQL statements may call database functions, so implementations remain
trusted backend code, but they cannot obtain an application-side query
capability through the ClaimPolicy contract.

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
