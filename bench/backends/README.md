# Backend benchmarks

Runs the same workloads against `Docket.SQLite` and `Docket.Postgres` through
the real supervised runtime (runner/dispatcher, vehicles, node tasks), so the
numbers include everything a host sees, not just store calls.

This is a comparison and regression harness. `bench/postgres/scorecard.exs`
remains the Postgres-specific suite for claim-policy fairness and surge
behaviour.

## Running

```sh
# SQLite only (no services needed)
mix run bench/backends/bench.exs -- --backend sqlite

# Both backends. Use a throwaway database: the harness drops and recreates it
# for every scenario.
docker run -d --name docket-bench-pg -e POSTGRES_PASSWORD=postgres -p 55432:5432 postgres:16
DOCKET_BENCH_DATABASE_URL=postgres://postgres:postgres@localhost:55432/docket_bench \
  mix run bench/backends/bench.exs -- --backend both
```

Each run writes `tmp/bench/backends/<branch>-<unix time>.json` (override with
`--output`) and prints a table.

## Comparing branches

```sh
git checkout main && mix run bench/backends/bench.exs -- --output tmp/bench/base.json
git checkout my-branch && mix run bench/backends/bench.exs -- --output tmp/bench/head.json
mix run bench/backends/compare.exs -- tmp/bench/base.json tmp/bench/head.json
```

`compare.exs` prints runs/s, p50, and p99 for each backend/scenario pair with
the percentage change. Run each side two or three times before trusting a
difference under ~10%; laptop timing noise is about that large.

## Scenarios

**Drain** scenarios seed a backlog of runs with a `testing: :manual` runtime
(so nothing executes), then start the production runtime and time it until
every run has completed. They measure how fast the backend works through
queued work.

| Scenario     | Workload                                         | Runs     |
| ------------ | ------------------------------------------------ | -------- |
| `throughput` | one no-op node                                   | `n`      |
| `chain`      | `--chain` no-op nodes in sequence (default 10)   | `n / 4`  |
| `fanout`     | `--fanout` parallel no-op nodes and a join (8)   | `n / 4`  |
| `io_bound`   | one node sleeping `--sleep-ms` (20)              | `n / 4`  |

They report `runs_per_sec`, `commits_per_sec`, and `commits_per_run`, counted
from `[:docket, :lifecycle, :committed]` telemetry, plus `completion_ms`
(when each run finished, measured from runtime start). `io_bound` also reports
`ideal_runs_per_sec` = concurrency × 1000 / sleep_ms, the ceiling if commit
overhead were zero.

**Live** scenarios drive a running runtime:

| Scenario         | Measures                                                                 |
| ---------------- | ------------------------------------------------------------------------ |
| `enqueue`        | `start_run` rate from one caller while runs execute; start→done latency  |
| `interrupt`      | `n / 2` runs park on an interrupt; resolve all; resolve→done latency     |
| `serial_latency` | one run at a time (up to 500): the wake path with no queueing            |

## Options

| Option          | Default                    | Meaning                                     |
| --------------- | -------------------------- | ------------------------------------------- |
| `--backend`     | `sqlite`                   | `sqlite`, `postgres`, or `both`             |
| `--scenarios`   | all                        | comma-separated list                        |
| `--n`           | 2000                       | base run count                              |
| `--concurrency` | 10                         | runner/dispatcher concurrency               |
| `--chain`       | 10                         | steps in `chain`                            |
| `--fanout`      | 8                          | width of `fanout`                           |
| `--sleep-ms`    | 20                         | node sleep in `io_bound`                    |
| `--synchronous` | `normal`                   | SQLite `PRAGMA synchronous`                 |
| `--label`       | current branch             | stored in the result file                   |
| `--output`      | `tmp/bench/backends/…json` | result path                                 |
| `--timeout-ms`  | 300000                     | per-scenario completion deadline            |

## Reading the numbers

* Completion is detected through a checkpoint observer and then confirmed
  against storage with `list_runs`; a scenario that loses a run fails.
* Postgres in Docker on macOS runs inside a VM, so every query crosses a
  virtual network and its fsyncs hit virtualized storage. SQLite here is an
  in-process library writing to the local disk. Compare SQLite against itself
  across branches; treat SQLite-vs-Postgres on a laptop as indicative only.
* On macOS, `synchronous: :full` issues `fsync`, which does not force the
  drive cache to flush (`F_FULLFSYNC`). It costs little here for that reason.
