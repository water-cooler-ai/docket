# Docket telemetry

Docket emits operational spans and durable domain facts below `[:docket, ...]`.
Span and pruner durations use native monotonic units; reporters convert them.
Counts and gauges are measurements. Use
`Docket.Telemetry.metric_metadata/2` for metric labels. Raw domain events may
contain identities for logs and traces, but those values must never become
labels. Claim tokens are never emitted.

## Operational catalog

| Event | Measurements | Bounded metric metadata |
| --- | --- | --- |
| `[:docket, :lifecycle, :transaction, :stop|:exception]` | `duration` | `operation`, `result` |
| `[:docket, :store, :operation, :stop|:exception]` | `duration` | `operation`, `result` |
| `[:docket, :checkpoint, :observer, :stop]` | `duration` | `checkpoint_type`, `result`, `durable_success` |
| `[:docket, :checkpoint, :observer, :failure]` | `count` | `checkpoint_type`, `result`, `durable_success` |
| `[:docket, :postgres, :dispatcher, :state]` | `concurrency`, `demand`, `in_flight`, `poll_active`, `poll_pending` | none |
| `[:docket, :postgres, :dispatcher, :poll]` | `duration`, `demand`, `concurrency`, `in_flight`, `leases`, `poisoned` | `claim_policy`, `result`, `source` |
| `[:docket, :postgres, :dispatcher, :launch]` | `duration` | `result` |
| `[:docket, :postgres, :dispatcher, :shutdown]` | `duration`, `vehicles_remaining` | `result` |
| `[:docket, :postgres, :notification]` | `count` | `result` |
| `[:docket, :postgres, :run_store, :claim]` | duration, candidate/selection ages and counts, `demand`, `leases`, `poisoned`, `steals`, `claim_attempts` | `preference`, `fallback`, `result` |
| `[:docket, :postgres, :claim_policy, :admission]` | `duration`, `demand`, `leases`, `poisoned` | `implementation`, `result` |
| `[:docket, :postgres, :claim_policy, :admission, :observation]` | TenantFair v1 aggregate partition, policy, outcome, and wait measurements described below | `implementation`, `schema`, `result`, `observation_status`, `admission_class`, `work_class`, `batch_shape`, `policy_source`, `admin_state` |
| `[:docket, :postgres, :claim, :attempt]` | `count`, `claim_attempts` | `result` |
| `[:docket, :postgres, :claim, :poisoned]` | `count` | `reason` |
| `[:docket, :postgres, :claim, :operation]` | `duration`, optionally `matched` | `operation`, `result` |
| `[:docket, :postgres, :claim, :fence_lost]` | `count` | `stage`, `result` |
| `[:docket, :postgres, :graph_cache, :fetch]` | `duration` | `result` |
| `[:docket, :postgres, :graph, :fetch, :stop|:exception]` | `duration` | `result` |
| `[:docket, :postgres, :graph, :compile, :stop|:exception]` | `duration` | `result` |
| `[:docket, :postgres, :run_codec]` | `duration`, `bytes` | `operation`, `result` |
| `[:docket, :postgres, :store]` | `duration`, `attempted_rows`, `selected_rows`, `encoded_bytes` as applicable | `operation`, `result` |
| `[:docket, :node, :execution]` | `duration`, `attempt` | `result` |
| `[:docket, :lifecycle, :committed]` | `count`, `checkpoint_seq`, `step` | `checkpoint_type`, `disposition`, `result` |
| `[:docket, :postgres, :vehicle, :stop|:exception]` | `duration` | `result` |
| `[:docket, :postgres, :vehicle, :discard]` | `count` | `stage`, `result` |
| `[:docket, :postgres, :vehicle, :crash]` | `count`, `claim_held_ms`, `claim_attempt` | `result` |
| `[:docket, :postgres, :vehicle, :drain]` | `committed_moments`, `elapsed_ms`, `claim_held_ms`, `claim_attempt` | `outcome`, `budget` |
| `[:docket, :postgres, :pruner, :pass]` | deleted-row counts and `duration` | `result` |

Lifecycle and nested store spans share a raw `lifecycle_ref`; the metric
projection drops it. Committed facts and observer work start only after the
outer transaction succeeds. Stale fences and rollbacks therefore emit no
committed fact. Observer completion means only that the callback returned
after durable success, not that an external system durably accepted an effect.
Claim selection/attempt and store events are operational attempt facts and may
describe work later rolled back; only `[:docket, :lifecycle, :committed]` and
the domain events are durable-success facts.

Use Ecto repository telemetry for checkout, queue, and query duration. Active
vehicles are execution processes, not checked-out connections: slow node work
can raise `in_flight` without holding a database connection.

Claims fence durable state only. A stolen claim can execute node code and
external effects more than once even though only one moment commits. External
effects require their own stable idempotency scheme.

## Tenant fairness telemetry boundary

The DCKT-58 contract defines tenant fairness over the persisted PostgreSQL
`scope_key`, but raw `scope_key` and `tenant_id` are forbidden as ordinary
admission telemetry metadata and metric labels. Run ID, graph ID/hash, claim
token, tier, host account identity, policy version, and arbitrary error text are
forbidden for the same reason. This admission restriction does not remove
identity from durable domain events, whose raw metadata contract is separate.

The generic `[:docket, :postgres, :claim_policy, :admission]` event remains
exactly four measurements and two metadata keys for every implementation.
Legacy emits no TenantFair observation event and retains all of its existing
events unchanged.

### TenantFair v1 admission observation

A future TenantFair plan opts into the source-owned `:tenant_fair_v1` schema.
Every successful decode, including a no-op, must return one complete aggregate
summary. The ClaimPolicy facade validates that summary against the portable
lease/poison batch and emits
`[:docket, :postgres, :claim_policy, :admission, :observation]`. Implementation
private observation fields are not copied. All event metadata is derived by the
facade from fixed numeric fields, and
`Docket.Telemetry.metric_metadata/2` rejects both unknown keys and values
outside the fixed enums.

The event's direct measurements are:

| Measurement | Meaning and unit |
| --- | --- |
| `duration` | Whole ClaimPolicy admission operation in native monotonic units, matching the generic event. It is not PostgreSQL lock wait or pool checkout time. |
| `demand`, `leases`, `poisoned`, `outcomes`, `unfilled_demand`, `steals` | Counts. An outcome is one returned lease or poison; an expired lease is a token-replacing steal. `unfilled_demand` is descriptive and does not by itself prove avoidable under-claim. |
| `eligible_partitions`, `locked_partitions`, `skipped_partitions` | Distinct partition counts considered, successfully locked, and skipped by the bounded plan. |
| `cap_denied_partitions` | Count of distinct locked, eligible partitions denied additive ready admission because authoritative live claims reached `max_active`; it is not denied backlog rows or a cap-violation count. |
| `below_preferred_partitions` | Count of locked eligible partitions below `preferred_active`. |
| `default_policy_partitions`, `override_policy_partitions` | Counts of locked partitions consulting each policy source. |
| `running_partitions`, `hold_new_partitions`, `drain_partitions` | Counts of locked partitions in each administrative state. |
| `preferred_admissions`, `borrowed_admissions` | Counts of additive ready leases by admission class. Expired replacement steals are neither. |
| `ready_leases`, `ready_poisoned`, `expired_leases`, `expired_poisoned` | Outcome counts by work class and disposition. |
| `candidate_rows_examined` | Logical candidate rows materialized/considered by the bounded TenantFair plan. It is not PostgreSQL physical rows scanned; physical work and buffers belong to benchmark `EXPLAIN` output. |
| `under_claimed` | `0` or `1`. It is `1` only when a bounded, trusted audit proves that eligible lockable work was avoidably left unserved. Ordinary `outcomes < demand` remains a partial result, not automatically under-claim. |
| `ready_claim_wait_ms_count|sum|max` | Ready-lease wait samples in integer milliseconds. Each sample is database `claimed_at - wake_at`; the count equals `ready_leases`. |
| `expired_recovery_wait_ms_count|sum|max` | Expired outcome recovery samples in integer milliseconds from the prior claim's expiry boundary to its lease/poison resolution. These are never mixed with ready wait. |
| optional `partition_lock_skip_delay_ms_count|sum|max` | Integer milliseconds from a proven database-authored first consecutive skip to observation. The fields are absent unless that history exists; they are never fabricated as zero or inferred from query duration. |

`count/sum/max` supports a count, mean, and maximum. It cannot reconstruct a
claim-level percentile. A percentile SLO must use fixed histogram buckets or a
documented bounded inspection histogram with the same population and window.

True row-lock blocking time is not a direct TenantFair v1 signal:
`FOR ... SKIP LOCKED` deliberately skips instead of waiting, and Ecto query or
checkout duration includes unrelated work. Until a later database-authored
contention history exists, partition-lock delay is owned by the bounded
concurrency harness/inspection plane; default telemetry directly owns the
`skipped_partitions` count only.

Metadata vocabularies are fixed:

- `schema`: `:tenant_fair_v1`;
- `result`: `:ok | :error`;
- `observation_status`: `:available | :unavailable`;
- `admission_class`: `:none | :preferred | :borrowed | :mixed`;
- `work_class`: `:none | :ready | :expired | :mixed`;
- `batch_shape`: `:error | :no_op | :full | :partial | :under_claim`;
- `policy_source`: `:none | :default | :override | :mixed`; and
- `admin_state`: `:none | :running | :hold_new | :drain | :mixed`.

An opted-in SQL, decoder, or observation-contract error emits the detail event
with only `duration` and `demand`, `result: :error`, and
`observation_status: :unavailable`; database-derived fields are absent rather
than falsely zero. The generic error event is still emitted. Observation
callback failures cannot suppress either facade-owned event.

The path shapes intentionally allow poison and steal to overlap other paths:

| Path | Available observation shape |
| --- | --- |
| full success | `batch_shape: :full`; `outcomes == demand` |
| no-op | `batch_shape: :no_op`; zero outcomes, while discovery/cap/skip counts may be non-zero |
| legitimate partial result | `batch_shape: :partial`; `unfilled_demand > 0`, `under_claimed == 0` |
| proven avoidable under-claim | `batch_shape: :under_claim`; `under_claimed == 1` |
| poison | `ready_poisoned + expired_poisoned > 0`; may still fill demand |
| steal | `steals == expired_leases > 0`; replacement does not add concurrency |
| error | `batch_shape: :error`, `observation_status: :unavailable`; DB aggregates absent |

Fairness reports use the following locked meanings:

- ready claim wait is database `claimed_at - wake_at`, in milliseconds;
  expired recovery wait is measured separately from the expiry boundary;
- concurrency share is one partition's active claims divided by all active
  claims in the same physical database/schema fairness domain, either at an
  instant or time-integrated over a named window;
- processing-time share names one service signal, initially claim-residency or
  executor-task time, and divides a partition's measured time by the domain
  total for the same signal and window; and
- service skew is observed processing-time share minus normalized active-set
  weight entitlement, reported in percentage points.

Those per-partition values belong to a trusted, cardinality-bounded inspection
or offline aggregation plane, not default metrics. A report must name one
physical database/schema fairness domain, one partition population, and one
window. For partition `i`, the required bounded inputs and derivations are:

```text
instant concurrency share_i
  = active_claims_i / domain_active_claims

windowed concurrency share_i
  = (1 / window_duration_ms)
    * integral over the window of (active_claims_i(t) / domain_active_claims(t)) dt

processing-time share_i,signal
  = partition_service_time_i,signal / domain_service_time_signal

entitlement_i
  = active_weight_i / sum(active_set_weights)

windowed entitlement_i
  = time average of entitlement_i over the named window

service skew_i_pp
  = 100 * (processing-time share_i,signal - windowed entitlement_i)
```

Concurrency and processing shares are ratios in `[0, 1]`; service skew is in
percentage points. Windowed concurrency therefore requires time-aligned
partition and domain active-claim samples/intervals; a ratio of their separate
time integrals is not equivalent when domain concurrency changes. The bounded
inspection row also supplies effective weight and active-set weight sum, named
window boundaries, and service-time numerator/denominator for one named signal.
`:claim_residency` uses database-authored integer microseconds;
`:executor_task` converts native monotonic duration to integer microseconds
before window aggregation. They are separate signals and are never combined
implicitly. The current mutable `claimed_at` freshness timestamp is not an
immutable claim-start fact, so residency inspection requires future durable
token-install/end evidence rather than deriving a false duration from the
current row. Zero denominators produce an undefined/no-population result, not a
numeric zero.

Ordinary TenantFair events emit only aggregate counts/durations and bounded
enums needed to detect cap denial, skipped partitions, under-claim, and query
delay. A host may join trusted inspection output to its own tenant/account
model under its own access, pagination, retention, and cardinality controls.

Preferred-capacity reclaim lag is measured from an eligible partition crossing
below `preferred_active` until its next preferred admission. It is not assigned
a numeric SLO unless the maximum residual service quantum, database-authored
preferred-epoch/slot-floor contract, repeated partition lock/discovery delay,
poll delay, and whole-admission transaction delay all have enforceable numeric
bounds. The full operational vocabulary and conditional formula live in
[`architecture/docket-tenant-claim-fairness-design.md`](architecture/docket-tenant-claim-fairness-design.md).

The normative target matrix, bounded `tenant_fair_report_input/v1` inspection
schema, unavailable/right-censored rules, worked report, and DCKT-50
traceability live in that document's
[fairness SLO and regression-budget contract](architecture/docket-tenant-claim-fairness-design.md#fairness-slo-and-regression-budget-contract).
Default telemetry supplies aggregate volume and wait totals; trusted inspection
supplies identity-bearing episode, cap-audit, aligned interval, histogram, and
bound inputs; the benchmark supplies only correctness-gated prototype query
regression evidence. These sources are reconciled by window and count, never
silently substituted for one another. In particular:

- `cap_denied_partitions` is not a cap violation or false-denial count;
- `eligible_partitions` is bounded-plan candidate volume, not a global tenant
  population;
- wait count/sum/max cannot yield p95/p99 without a matching histogram;
- query or checkout duration is not partition lock wait; and
- a missing optional duration or bound produces `not_qualified`, not zero.

## Benchmark derivations

- Throughput: rate of `lifecycle.committed.count`; checkpoint and superstep
  advancement come from its `checkpoint_seq` and `step` measurements.
- End-to-end and database latency percentiles: histogram the native `duration`
  measurements on lifecycle, store, poll, launch, graph, codec, and node events.
- Active work and demand: last-value gauges from dispatcher `state.in_flight` and
  `state.demand`. Compare with Ecto checkout telemetry; active node work must not
  imply a checked-out connection.
- Claim health: rates and histograms for claim `leases`, `steals`, `poisoned`,
  `claim_attempts`, operation results, `matched`, fence-loss stage, and vehicle
  `claim_held_ms`.
- Cache behavior: cache hit ratio is `hit / (hit + miss + incompatible +
  generation_invalidated)` from graph-cache fetch results.
- Logical write amplification: sum store `attempted_rows` and `encoded_bytes`
  per committed lifecycle count, correlated by the raw `lifecycle_ref`; these
  describe requested work and may include rolled-back or idempotent attempts.
  Use Ecto query telemetry and database statistics when physical affected-row
  or WAL amplification is required. Reads use `selected_rows` and are excluded.
- Retry and park behavior: group committed lifecycle counts by `disposition`;
  `:retry`, `:timer`, and `:budget` are durable outcomes, not proposed work.
- Prune impact: compare deleted-row and cascade rates and pass latency with
  lifecycle throughput and store bytes before and after each pass.

The vehicle drain's `elapsed_ms` and `claim_held_ms` are explicit millisecond
business measurements. Fields named `duration` use native telemetry monotonic
units.
