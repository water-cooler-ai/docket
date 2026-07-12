# Postgres benchmarks

`mix docket.bench` exercises the assembled `Docket.Postgres` runtime: the
dispatcher claims durable work, supervised vehicles fetch and compile the
graph, nodes execute through the task supervisor, and lifecycle moments commit
through the real fenced transaction.

The current bounded smoke slice is intentionally exploratory:

```console
mix docket.bench --scenario smoke --runs 10 --concurrency 2 \
  --pool-size 5 --output results/smoke.json
```

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

- per-lease ready `wake_at`-to-claim lag and expired-claim overdue age;
- claim scan, claim query/queue/decode, dispatcher poll/launch, vehicle,
  lifecycle transaction, node, graph, and Repo timings;
- claims, empty polls, maximum in-flight vehicles, committed moments, and
  query counts;
- durable rows/events, logical encoded bytes, database size, WAL, and
  `pg_stat_database` deltas.

Claim scan timing is the complete client-side store operation. Claim query
timing is Ecto/Postgrex-observed protocol timing, not server-exclusive SQL
execution. WAL and database-statistics deltas can include concurrent server
activity and delayed statistics updates.

Artifacts are classified as `exploratory` because the smoke scenario is one
small staged burst on uncontrolled hardware, without warmup, repetitions,
steady-state load, saturation points, fault injection, or multi-node coverage.
Its observed throughput and tail percentiles are useful diagnostics, not a
maximum or portable capacity claim.

Only `smoke` and `empty_one_step` are implemented today. Other benchmark suites
(claim ceiling, cyclic drain, blocked vehicles, fairness, cache, freshness,
real multi-node scaling, notify/poll, amplification, and soak) are rejected
rather than silently approximated. `--event-policy none` is also rejected:
v0.1 persists lifecycle events and has no production event-suppression mode.

Ordinary `mix test` never runs the benchmark. Long saturation and soak runs
belong on scheduled dedicated hardware; performance gates must compare at
least three repetitions with matching hardware and Postgres fingerprints.
