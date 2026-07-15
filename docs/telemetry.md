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
telemetry metadata and metric labels. Run ID, graph ID/hash, claim token, tier,
and host account identity are forbidden for the same reason. The current
generic ClaimPolicy event remains bounded by implementation and result; DCKT-59
adds TenantFair aggregate measurements after the engine observation shape is
available.

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
or offline aggregation plane, not default metrics. Ordinary TenantFair events
emit only aggregate counts/durations and bounded enums needed to detect cap
denial, partition-lock contention, under-claim, and query delay. A host may join
trusted inspection output to its own tenant/account model under its own access,
retention, and cardinality controls.

Preferred-capacity reclaim lag is measured from an eligible partition crossing
below `preferred_active` until its next preferred admission. It is not assigned
a numeric SLO unless maximum residual slice duration, poll delay, and admission
transaction delay all have enforceable numeric bounds. The full operational
vocabulary and formulas live in
[`architecture/docket-tenant-claim-fairness-design.md`](architecture/docket-tenant-claim-fairness-design.md).

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
