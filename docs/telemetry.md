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

## Fair-rotation evidence boundary

The provisional schema-v2 admission engine emits only the generic ClaimPolicy
and claim events in the catalog above, even when running on schema v3. It does
not expose the cursor, visit, grant, or service-epoch evidence needed to prove
the DCKT-75 bounded-bypass contract. Its query duration and the existing
TenantFair timing score are not substitutes for that evidence.

Schema v3 installs the durable state and fixed budgets but does not yet change
the current generic admission telemetry. DCKT-78 must keep the generic event
and add one bounded, identity-free fair-rotation observation whose aggregate
measurements cover:

- configured inspection budget `S` and grant outcome limit `Q`;
- scan pages, unfinished-ring visits inspected, cursor advances, and wraps;
- partition locks, lock skips, grants, leases, poison outcomes, and total
  outcomes;
- cap-denied, stale, and empty visits;
- unfinished-ring membership transitions and explicit work-budget exhaustion; and
- `admission_epoch` advances.

For every available committed observation:

```text
outcomes = leases + poisoned
outcomes <= Q * grants
grants <= locked visits
admission_epoch advances = grants
cursor advances = unfinished-ring positions inspected
```

A denial, stale/empty visit, or lock skip contributes no grant and no service-
epoch advance. A statement that fails before scan authority, or later rolls
back with its caller, supplies no committed cursor/grant evidence. Operational
attempt telemetry may still describe such work, so collectors must not
silently treat attempt counts as durable proof.

Metadata remains bounded enums such as implementation, result, and observation
availability. Tenant ID, raw `scope_key`, run or graph identity, cursor token,
and claim token are forbidden as ordinary metric labels. Per-target bypass
requires an identity-bearing, database-ordered trace in the deterministic test
or trusted inspection plane; aggregate telemetry cannot reconstruct it.

The normative populations, units, formulas, exclusions, and Legacy control are
in the [exact-cap and fair-rotation admission contract](architecture/docket-exact-cap-contract.md).
Timing and query-plan benchmarks remain separate regression evidence and never
replace that correctness oracle.

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
