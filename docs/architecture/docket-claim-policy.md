# PostgreSQL ClaimPolicy boundary

`Docket.Postgres.RunStore.claim_due/3` is the stable PostgreSQL admission
entrypoint. ClaimPolicy is an internal engine seam behind that entrypoint, not
an alternate caller-facing store.

The portable RunStore method set, instance-level selection rule, and caller
result remain unchanged. Internally, `decode/3` is additively widened to return
bounded data-only policy errors with observations as well as successful
batches; this is an internal result-algebra extension, not a new portable
method or per-call implementation override.

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
provides four required callbacks plus one optional activation callback:

- `init/2` validates implementation configuration once per backend instance.
  It receives the normalized prefix and quoted identifiers, but no Repo or
  query capability, and returns opaque instance state.
- `build_plan/3` receives quoted identifiers, the facade-validated portable
  claim policy, and instance state. It returns one
  `Docket.Postgres.ClaimPolicy.Plan` without performing database I/O.
- `decode/3` receives only the rows from that plan's statement, the plan's
  data-only decoder contract, and instance state. It returns either the
  portable claim batch or a data-only policy error, plus bounded observation
  data. The error form lets one atomic statement distinguish a fail-closed
  gate result from ordinary empty eligibility without adding a RunStore policy
  branch.
- `observe/5` owns implementation-specific result, selection, attempt, poison,
  and error observations.
- `activation_contract/1`, when implemented, identifies the resolved instance
  as the TenantFair engine for the frozen database-function contract. It must
  return exactly `%{engine: :tenant_fair, function_contract: 1}` for contract
  v1. Missing, malformed, or raising callbacks are treated as no activation
  contract. This metadata is control-plane proof only: it cannot select an
  implementation, replace the context-bound policy, or alter a RunStore call.

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

The facade accepts the error variant only when both the reason and bounded
observation are data-only. A PID, function, query capability, or other opaque
runtime value in the reason is normalized as an invalid decoder return. The
shared implementation contract exercises both the alternate engine's valid
policy-error variant and a rejected non-data reason.

Each admission must be one atomic PostgreSQL operation: one RunStore-issued
client query containing one top-level PostgreSQL statement. No callback
receives a Repo, backend/storage context, query function, RunStore module, or
executor callback, and the plan is data-only. The callbacks do receive the
sanitized plan context documented above. PostgreSQL statements may call
database functions, and trusted implementation code could obtain a globally
known Repo on its own, so this is an extension contract rather than a security
sandbox. The boundary never injects an application-side query capability.

For DCKT-47, "one statement" does not mean "one MVCC snapshot." The approved
TenantFair plan computes one bounded candidate-key array and invokes one
prefix-qualified `VOLATILE PARALLEL UNSAFE SECURITY INVOKER` database function
exactly once. That function acquires nonblocking gate/default/partition locks
in ordered internal commands, advances the partition decision witness, and
uses a later internal command's fresh `READ COMMITTED` snapshot for live count
and mutation. A bare CTE that locks and aggregates under the top-level snapshot
is explicitly non-conforming. Non-Read-Committed and read-only transactions
fail closed; transaction-scoped leases remain provisional until outer commit.

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

The activation-aware plan first acquires the prefix gate with
`FOR SHARE SKIP LOCKED`. Every candidate and update data-depends on the locked
row authorizing `legacy`; a skipped gate returns lock contention and a locked
TenantFair-mode gate returns inactive-engine. Pre-activation selection,
decoding, telemetry, poison, and portable caller results are otherwise
unchanged.

The same one statement materializes its transaction context before the gate.
Repeatable Read or Serializable returns `:unsupported_isolation` without
feeding any modifying CTE. PostgreSQL rejects a data-modifying CTE in a
read-only transaction even when its input is empty; RunStore therefore maps
only SQLSTATE `25006` at the generic query boundary to
`:read_only_transaction`. No implementation-specific retry or second client
query is added.

Legacy also owns parameter normalization, lease and poison decoding, candidate
and selection statistics, eligibility ages, fallback and steal accounting,
attempt/poison events, and the existing
`[:docket, :postgres, :run_store, :claim]` event. The facade adds exactly one
`[:docket, :postgres, :claim_policy, :admission]` event identifying the bounded
selected implementation. No event includes tenant, run, graph, or token
identity as metric metadata.

## TenantFair engine

`Docket.Postgres.ClaimPolicy.TenantFair`:

1. validates only its data-only instance configuration in `init/2` and returns
   `%{engine: :tenant_fair, function_contract: 1}` from
   `activation_contract/1`; readiness,
   schema, mode, and function contract are checked inside admission;
2. constructs one atomic fairness statement in `build_plan/3` using only the
   supplied quoted identifiers and normalized portable policy;
3. returns a data-only decoder and bounded observation contract in its Plan;
4. decodes the unchanged lease/poison batch shape in `decode/3`;
5. emits bounded engine-specific observations in `observe/5`; and
6. runs the reusable ClaimPolicy contract plus direct, transaction, supervised,
   manual-drain, concurrency, query-plan, and telemetry suites.

Its locked instance-configuration shape is:

```elixir
use Docket,
  backend: Docket.Postgres,
  repo: MyApp.Repo,
  tenant_mode: :required,
  dispatcher: [concurrency: 100],
  claim_policy: [
    implementation: Docket.Postgres.ClaimPolicy.TenantFair,
    partition_by: :tenant_id,
    default_preferred_active: 2,
    default_max_active: 2,
    default_weight: 1,
    borrowing: false
  ]
```

TenantFair owns validation of those implementation options in `init/2`.
Fairness configuration does not belong under `:dispatcher`: dispatcher
concurrency is a per-runtime vehicle ceiling, whereas TenantFair policy is an
instance-selected PostgreSQL admission engine configuration shared by every
admission path. Claim-time authority always comes from the locked database
default or one complete partition override; the instance `default_*`, weight,
and borrowing values never rescue an uninitialized database default or change
contract-v1 admission behavior.

The outer statement discovers a distinct, sorted, bounded `text[]` of eligible
partition keys without tenant preference and passes it as argument six to
exactly one canonical function call. Inside the function, fresh READ COMMITTED
commands acquire gate and default `FOR SHARE SKIP LOCKED`, partitions in
ascending key order with `FOR NO KEY UPDATE SKIP LOCKED`, and selected run IDs
in ascending order with `FOR UPDATE SKIP LOCKED`. The locked partition and a
fresh live-claim count serialize additive ready admission. Expired steals are
count-neutral; ready and expired poison each consume demand. `hold_new` blocks
only additive ready leases, while `drain` also blocks ordinary steals; both
states still permit poison resolution.

For each locked key, exactly one set-based data-modifying CTE command owns run
selection and mutation. Its four disjoint FIFO raw lanes—ready poison, ready
ordinary, expired poison, and expired ordinary—are each limited by current
remaining demand before state/cap disposition. After disposition, poison-first
FIFO ranking reduces the decision source to at most remaining demand per work
class. The entire at-most-`2 * remaining` union is then sorted by ID and fed to
one `SKIP LOCKED` command; there is no later ID limit that can truncate one
class before preference or reservation. This also preserves the call-wide
`2 * original_demand` lock budget; eligibility is repeated at lock and update.
At demand two or greater, a page-wide reservation preserves one outcome for
each visible class when possible, even when the other class has multiple rows
in an earlier key. Demand one considers only the single hinted key and applies
preference with within-key fallthrough.

The bounded algorithm deliberately does not refill or revisit a processed
key. It may therefore return a partial batch after candidate-page truncation,
per-lane limits, the lock budget, a held partition or run, concurrent
invalidation/deletion, state/cap denial, or an unused class reservation. Those
are bounded under-fill cases, not proof of avoidable under-claim. Exact
contention is returned only when mutation-eligible work existed, no run lock or
outcome was acquired anywhere, every omission remains eligible on the later
fresh recheck, and no invalidation occurred.

The function returns zero or more discriminated outcome rows followed by
exactly one aggregate summary. Gate, default, isolation, read-only, and exact
all-skipped contention failures return one data-only error sentinel and no
outcome or summary. Function-internal SQLSTATE `55P03` is caught at the
subtransaction boundary, so earlier epoch or claim mutations roll back before
that sentinel is returned; other SQLSTATEs propagate. A caller-owned
transaction keeps all successful claim authority provisional until its outer
commit and rolls it back on error or raise.

No policy-specific branch belongs in RunStore, dispatcher, synchronous drain,
vehicle, executor, or the portable `Docket.Backend.RunStore` contract. Rolling
back exact-cap mode follows the prefix-wide deactivation protocol below; merely
changing one instance's option while the prefix is active is not a safe
fallback.

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

That generic alternate-engine canary applies only before a prefix promises
exact caps. DCKT-47 supersedes it for TenantFair activation: activation-aware
Legacy and TenantFair may coexist as deployed code, but the prefix gate permits
only one engine to admit. Ordinary mixed-engine canaries and hot fallback are
forbidden. Exact-cap rollback stops TenantFair admission, changes the audited
prefix mode/epoch under the exclusive gate, explicitly abandons the cap
guarantee, and only then resumes gate-aware Legacy.
