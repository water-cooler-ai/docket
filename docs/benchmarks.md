# Postgres benchmarks

`mix docket.bench` provides two production-backed measurements. The
`empty_one_step`/`smoke` scenario exercises the assembled `Docket.Postgres`
runtime from dispatcher claim through supervised vehicle and fenced lifecycle
commit. The `claim_only` scenario deliberately bypasses dispatcher and vehicle
work to isolate concurrent `RunStore.claim_due` behavior.

The current bounded smoke slice is intentionally exploratory:

```console
mix docket.bench --scenario smoke --runs 10 --concurrency 2 \
  --pool-size 5 --output results/smoke.json
```

A repeated concurrency/pool matrix runs sequentially with one isolated
database and Repo configuration per trial:

```console
mix docket.bench --scenario smoke --runs 1000 --warmup 100 --repetitions 3 \
  --concurrency-matrix 1,2,5,10,25 --pool-size-matrix 2,5,10 \
  --format ndjson --output results/saturation.ndjson
```

NDJSON contains one point record per matrix cell per repetition followed by
one suite-summary record. JSON preserves the original singleton artifact shape and wraps
multi-point runs in a suite containing raw points and per-cell distributions.
Per-cell repetition summaries report min, median, max, mean, and spread. They
do not label a handful of repetition-level observations as p95/p99; the suite
instead summarizes each trial's within-run p50/p95 latency values across
repetitions.

Each invocation creates a uniquely named Postgres database, migrates and seeds
it, stages every run at one common future due time, starts the production
supervision tree, and begins measurement at that due boundary. It then waits
for terminal telemetry without polling the database, verifies SQL invariants,
writes JSON atomically, and removes the database. Set `--database-url` to a
Postgres server on which the configured user may create databases. Never point
the task at production credentials.

Artifacts include the commit and dirty state, runtime and database versions,
durability-related Postgres settings, effective pool and dispatcher settings,
parameters, observed throughput, and invariant results. Timing distributions
use nearest-rank percentiles and always include the sample count. The report
separates:

- burst activation to first durable commit, first commit to terminal commit,
  and activation to terminal commit offsets;
- per-lease ready age at claim-scan start, post-claim observation offset, and
  expired-claim overdue age;
- claim scan, claim query/queue/decode, dispatcher poll/launch, vehicle,
  lifecycle transaction, node, graph, and Repo timings;
- claims, empty polls, maximum in-flight vehicles, committed moments, and
  query counts;
- durable rows/events, logical encoded bytes, database size, WAL, and
  `pg_stat_database` deltas.

The activation-based distributions describe a whole staged cohort, not just
per-run service time. Their maximum is the last run to reach that milestone;
the activation-to-terminal maximum therefore equals the measured burst
duration by definition and will grow as a backlog waits behind limited
vehicle concurrency. Use `first_commit_to_terminal_us` to separate the
post-admission durable execution span from that pre-first-commit queueing.

Claim scan timing is the complete client-side store operation. Claim query
timing is Ecto/Postgrex-observed protocol timing, not server-exclusive SQL
execution. WAL and database-statistics deltas can include concurrent server
activity and delayed statistics updates. For the vehicle scenario, physical
deltas intentionally begin before runtime startup and therefore include the
reported pre-activation empty polls; the activation-based latency and
throughput distributions exclude those polls.

The current collector retains full per-event observations. Artifacts report
the capture mode and event count, but do not estimate observer cost; summed
concurrent callback time is not a valid wall-time correction. Run benchmarks
in a dedicated, otherwise quiescent BEAM because non-correlated global Docket
telemetry from another runtime can contaminate operational distributions.

Warmup is expressed as runs per repetition and is excluded from measured rows,
events, WAL, and timing. A nonzero warmup leaves the compiled graph cache warm;
without warmup, the cache is cold only at activation and warms during the
burst. The artifact records that initial state. Matrix execution rotates point
order deterministically from the recorded seed to reduce configuration-order
bias.

Artifacts remain `exploratory` even with warmup and repetitions because the
harness cannot prove controlled hardware, steady-state arrivals, fault
coverage, or multi-node comparability. Observed throughput and tail
percentiles are useful diagnostics, not a maximum or portable capacity claim.

The isolated one-pass claim drain can be exercised independently:

```console
mix docket.bench --scenario claim_only --runs 100000 --concurrency 8 \
  --pool-size 10 --batch-size 100 --ready-ratio 1:1 \
  --repetitions 3 --output results/claims.json
```

Here `--concurrency` means direct claim workers and `--runs` is backlog size.
Every worker uses one frozen claim clock, and leases remain held until the
trial ends so a row cannot be selected twice. This measures one backlog drain,
not a replenished claim/release ceiling. The report separates ready and
expired outcomes, batch fill, claim/query/queue/decode latency, WAL and table
deltas, and exact claim/token/attempt/event invariants. Ready age in this
scenario is explicitly age at the frozen claim clock; the burst-to-claim
offset is the metric for backlog waiting after activation.

Only `smoke`, `empty_one_step`, and `claim_only` are implemented today. Other benchmark suites
(cyclic drain, blocked vehicles, fairness, cache, freshness,
real multi-node scaling, notify/poll, amplification, and soak) are rejected
rather than silently approximated. `--event-policy none` is also rejected:
v0.1 persists lifecycle events and has no production event-suppression mode.

Ordinary `mix test` never runs the benchmark. Long saturation and soak runs
belong on scheduled dedicated hardware; performance gates must compare at
least three repetitions with matching hardware and Postgres fingerprints.
