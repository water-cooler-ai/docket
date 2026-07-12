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
it before timing, starts the production supervision tree, waits for durable
completion, verifies SQL invariants, writes JSON atomically, and removes the
database. Set `--database-url` to a Postgres server on which the configured user
may create databases. Never point the task at production credentials.

Artifacts include the commit and dirty state, runtime and database versions,
durability-related Postgres settings, effective pool and dispatcher settings,
parameters, observed throughput, and invariant results. They are classified
as `exploratory`; observed throughput is not a maximum or a portable capacity
claim.

Only `smoke` and `empty_one_step` are implemented today. Other benchmark suites
(claim ceiling, cyclic drain, blocked vehicles, fairness, cache, freshness,
real multi-node scaling, notify/poll, amplification, and soak) are rejected
rather than silently approximated. `--event-policy none` is also rejected:
v0.1 persists lifecycle events and has no production event-suppression mode.

Ordinary `mix test` never runs the benchmark. Long saturation and soak runs
belong on scheduled dedicated hardware; performance gates must compare at
least three repetitions with matching hardware and Postgres fingerprints.
