# Postgres benchmarks

`mix docket.bench` provides seven production-backed scenario families. The
`empty_one_step`/`smoke` scenario exercises the assembled `Docket.Postgres`
runtime from dispatcher claim through supervised vehicle and fenced lifecycle
commit. The `claim_only` scenario deliberately bypasses dispatcher and vehicle
work to isolate concurrent `RunStore.claim_due` behavior. The
`blocked_vehicles` scenario holds a saturated first wave inside production
node execution, samples BEAM and Repo pressure, probes unrelated short SQL
work, then releases the gate and drains the backlog.

`steady_arrival` pre-stages a fixed open-loop schedule. Three comparative
scenarios test slow/fast, parked/resident, and bounded-cycle/one-step
fairness.

The current bounded smoke slice is intentionally exploratory:

```console
mix docket.bench --scenario smoke --runs 10 --concurrency 2 \
  --pool-size 5 --output results/smoke.json
```

After atomically writing the complete JSON or NDJSON artifact, the task prints
a compact scenario-aware summary to the terminal. It labels activation-based
values as queue-inclusive cohort offsets, separates terminalization after the
first durable commit, and uses p50/max instead of a tail label for distributions
with fewer than 20 observations. The artifact remains the complete,
machine-readable record.

While trials run, the task reports live progress on standard error. An
interactive terminal gets an animated ticker with a trial counter and the
current trial's elapsed time; non-interactive output falls back to one
started line and one PASS/FAIL line per trial, so long suites never look
stalled.

A repeated concurrency/pool matrix runs sequentially with one isolated
database and Repo configuration per trial:

```console
mix docket.bench --scenario smoke --runs 1000 --warmup 100 --repetitions 3 \
  --concurrency-matrix 1,2,5,10,25 --pool-size-matrix 2,5,10 \
  --format ndjson --output results/saturation.ndjson
```

NDJSON contains one point record per matrix cell per repetition followed by
one suite-summary record. JSON always writes one suite envelope — `kind`
`benchmark_suite` with raw points and per-cell summaries — even for a single
trial, so consumers never branch on artifact shape.
Per-cell repetition summaries report min, median, max, mean, and spread. They
do not label a handful of repetition-level observations as p95/p99; the suite
instead summarizes each trial's within-run p50/p95 latency values across
repetitions.

With at least three successful concurrency cells for a pool size, each backed
by at least three successful repetitions, the suite reports a capacity-knee
diagnostic: the first adjacent point where throughput gains fall below the
recorded threshold, throughput regresses, or tail latency crosses its growth
threshold. Cells below the repetition minimum remain exploratory and produce
no safe-concurrency recommendation. The diagnostic records the baseline, peak
throughput point, detected knee, and last tested concurrency before that knee.
This is a reproducible heuristic over the supplied matrix, not a universal
maximum; a knee that is not observed means only that the tested range ended
first. The generic heuristic is disabled for `steady_arrival` because its
drain-inclusive throughput can conceal overload during the arrival window.

Every cell surfaces repetition medians for claim, Repo queue/query,
dispatcher, vehicle, lifecycle, and node p95 timing, plus available database
activity and lock/wait evidence. At a detected knee, conservative attribution
checks identify supported contributors such as Repo-pool queueing, claim
scanning, lifecycle commits, node execution, dispatcher polling, or database
pressure. When signals do not cross the declared thresholds, the result is
explicitly `inconclusive`. Nested spans overlap, database active time is
aggregate backend activity rather than CPU utilization, and its snapshot scope
does not match the measured wall-time denominator. It is therefore context
only: database-pressure attribution requires a positive wait/lock signal.
Boundary lock gauges can miss transient waits, so attribution remains a
diagnostic hypothesis rather than proof of an exclusive cause.

Each point carries a flat `headline` block: the scenario's most
decision-relevant values under stable, unit-suffixed keys — throughput, the
key p50/p95 offsets, and per-cohort comparisons where a scenario has cohorts.
Values a trial did not produce are omitted rather than written as null. The
headline is a convenience projection for scripted cross-run comparison; the
nested measurement tree remains the complete record, and distribution sample
counts and caveats live there.

Each invocation creates a uniquely named Postgres database, migrates and seeds
it, stages either a common future due time or the selected scenario's fixed
arrival schedule, starts the production supervision tree, and begins
measurement at the first due boundary. It then waits for terminal telemetry
without polling the database, verifies SQL invariants, writes JSON atomically,
and removes the database. Set `--database-url` to a Postgres server on which
the configured user may create databases. Never point the task at production
credentials.

Artifacts include the commit and dirty state; Elixir, OTP, ERTS, OS, kernel,
architecture, scheduler, CPU, RAM, cgroup/container, and Postgres versions;
best-effort storage class, filesystem, mount, and capacity details for the
Postgres data directory; and durability, WAL/checkpoint, autovacuum, memory,
planner, parallelism, and observability settings from `pg_settings`. Settings
also record their source and pending-restart state. Unsupported settings and
host facts are written explicitly as `unavailable`, and
`DOCKET_BENCH_STORAGE_CLASS` / `DOCKET_BENCH_POOLER_MODE` may supply facts the
host cannot discover safely. Effective pool and dispatcher settings,
parameters, observed throughput, and invariant results remain part of every
point. Timing distributions use nearest-rank percentiles and always include
the sample count. The report separates:

- burst activation to first durable commit, first commit to terminal commit,
  and activation to terminal commit offsets;
- per-lease ready age at claim-scan start, post-claim observation offset, and
  expired-claim overdue age;
- claim scan, claim query/queue/decode, dispatcher poll/launch, vehicle,
  lifecycle transaction, node, graph, and Repo timings;
- claims, empty polls, maximum in-flight vehicles, committed moments, and
  query counts;
- durable rows/events, logical encoded bytes, database size, WAL, expanded
  `pg_stat_database` deltas, and before/after `pg_stat_activity`/`pg_locks`
  contention gauges.

The activation-based distributions describe a whole staged cohort, not just
per-run service time. Their maximum is the last run to reach that milestone;
the activation-to-terminal maximum therefore equals the measured burst
duration by definition and will grow as a backlog waits behind limited
vehicle concurrency. `first_commit_to_terminal_us` isolates terminalization
after the first durable checkpoint; it is not the whole per-run service time.

Claim scan timing is the complete client-side store operation. Claim query
timing is Ecto/Postgrex-observed protocol timing, not server-exclusive SQL
execution. WAL and database-statistics deltas can include concurrent server
activity and delayed statistics updates. For the vehicle scenario, physical
deltas intentionally begin before runtime startup and therefore include the
reported pre-activation empty polls; the activation-based latency and
throughput distributions exclude those polls.

The activity and lock readings are point-in-time boundary gauges, not a wait
event trace; short lock or I/O spikes can occur entirely between them. The
cumulative database counters separately expose deadlocks, conflicts,
temporary-file use, I/O timing, and session timing when the running Postgres
version provides those fields. The environment reports whether
`pg_stat_statements` is installed, visible, and shared-preloaded, but does not
create the extension or collect query text. The harness also does not run
`EXPLAIN ANALYZE` automatically because doing so can execute modifying claim
queries and would not be an observation-only diagnostic.

The default collector keeps exact counters while retaining a deterministic,
bounded reservoir per telemetry event and a bounded, evenly spaced sample of
run correlations. Artifacts distinguish total observations from retained
distribution samples and record the configured bound. Large bursts therefore
do not retain every callback observation, and reported percentiles describe
the retained bounded sample rather than an unbounded full trace. Artifact
schema version 5 separates `exact_global_counts`,
`retained_per_run_shape_evidence`, retained distribution sample counts, and
`full_population_uniqueness`. Exact raw event totals remain available above the
correlation bound, but uniqueness is explicitly `unavailable` unless every run
correlation was indexed. `telemetry_checks_pass` combines the applicable raw
count and retained-sample checks without claiming full-population per-run proof.

Smoke and `empty_one_step` can opt into a paired collector-sensitivity check:

```console
mix docket.bench --scenario smoke --runs 1000 --observer-abba \
  --repetitions 3 --output results/observer-abba.json
```

For every matrix cell and requested repetition, `--observer-abba` runs four
isolated trials in `bounded instrumented / counters-only control /
counters-only control / bounded instrumented` order. The two adjacent AB and
BA pairs report raw instrumented-minus-control throughput and duration deltas;
the ordinary cell summary and concurrency-knee analysis use only the bounded
instrumented trials. The counters-only control retains exact raw aggregate
event counts but no distributions or per-run uniqueness proof. Its success
requires those raw counts plus the same SQL invariants; it does not infer
uniqueness from event totals. It still attaches telemetry handlers and is
therefore not an observer-free runtime. Paired differences are
diagnostics, not a causal correction, and must not be subtracted from workload
latency. This flag does not control sampler cost and is rejected for blocked,
claim-only, comparative, and steady-arrival scenarios. Run all benchmarks
in a dedicated, otherwise quiescent BEAM because unrelated global Docket
telemetry can still contaminate operational counters.

Warmup is expressed as runs per repetition and is excluded from measured rows,
events, WAL, and timing. A nonzero warmup leaves the compiled graph cache warm;
without warmup, the cache is cold only at activation and warms during the
burst. The artifact records that initial state. Matrix execution rotates point
order deterministically from the recorded seed to reduce configuration-order
bias.

Artifacts remain `exploratory` even with warmup and repetitions because the
harness cannot prove controlled hardware, an external long-lived arrival
producer, fault coverage, or multi-node comparability. Observed throughput and
tail percentiles are useful diagnostics, not a maximum or portable capacity
claim.

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
expired outcomes, claim/query/queue/decode latency, WAL and table deltas, and
exact claim/token/attempt/event invariants. Total, empty, and nonempty scan
counts and mean rows per nonempty scan are exact streaming aggregates. Full
versus partial batch-fill counts and the rows-per-scan distribution describe
only the bounded retained claim-scan sample. Ready age in this scenario is
explicitly age at the frozen claim clock; the burst-to-claim offset is the
metric for backlog waiting after activation.

To measure resident vehicles during slow external-style node work:

```console
mix docket.bench --scenario blocked_vehicles --runs 100 \
  --concurrency 50 --pool-size 5 --hold-ms 500 \
  --sample-interval-ms 20 --max-samples 256 --probe-count 3 \
  --output results/blocked.json
```

`--runs` must be at least the largest configured concurrency, and the hold
must be at most half the orphan TTL to leave a reclaim-safety margin.
`--sample-interval-ms` is at least 5 ms. The first
saturated wave blocks behind a benchmark-controlled gate for at least
`--hold-ms`; once released, the gate remains open so the remaining backlog can
drain. These are running, claimed vehicles—not durably parked runs. The report
records activation-to-block, release-to-first-commit, release-to-terminal,
short-query round-trip/query/queue/decode latency, exact plateau
SQL/process/freshness invariants, and a bounded time
series of dispatcher in-flight work, blocked calls, Repo readiness/queueing,
derived unclaimed common-due backlog and `wake_at` age, run queues, process
counts, memory, sampler lateness/self-time, and key mailboxes. Forced samples
at stable-hold start and immediately before release guarantee plateau resource
coverage even when the periodic interval is longer than the hold. Explicit
phase offsets locate plateau fill, stable hold, gate fan-out, and vehicle
quiescence; the timeline's summary covers the whole sampled run. Runtime
configuration records both the staged activation target and the observed
ready-to-activation lead, with a 250 ms minimum enforced before every measured
burst.

Repo pool samples use `DBConnection.get_connection_metrics`. Capacity minus
ready connections is deliberately labeled `busy_or_unavailable_connections`;
it is not claimed to be an exact checkout count. The stable plateau also runs
tagged `SELECT 1` probes. Their timings and Ecto query/queue/decode components
are reported separately. Probe and plateau-control queries are excluded from
workload Repo-query counts, while both remain inside physical Postgres deltas.
Timeline compaction merges the lowest-weight adjacent pair, retaining balanced
temporal coverage while preserving each bucket's start/end, represented
sample count, numeric first/last/delta/min/max, last-observed offset, and
sample-count-weighted mean. Counter and high-watermark semantics are declared
separately. Missed ticks, unavailable metrics, forced/scheduled sample counts,
sampling-end offset, sampler self-time, and serial duty cycle make sampling
quality visible. Those diagnostics do not replace a paired sampler-on/off
control when making capacity claims.

Claim age is checked at the plateau and immediately before release, and every
vehicle's reported claim-hold duration must remain below the orphan TTL. A
point that has become stealable is written as a failed result rather than
presented as a valid blocked-vehicle measurement.

To exercise open-loop arrivals instead of a common-due burst:

```console
mix docket.bench --scenario steady_arrival --duration 30s \
  --arrival-rate 100 --concurrency 10 --pool-size 5 \
  --sample-interval-ms 20 --max-samples 256 \
  --output results/steady-arrival.json
```

The harness creates every run before timing and assigns uniform due times
inside the arrival window. No producer waits for completions or adapts its
rate. `--arrival-rate` derives the run count from the duration; when it is
omitted, `--runs` determines both the schedule and offered rate. Durations use
an integer `ms`, `s`, or `m` suffix. Warmup and workload-specific options are
rejected.

Artifacts compare offered rate with the exact count of terminal commits whose
telemetry timestamps fall inside the arrival window. The exact terminal-drain
rate and duration end at the last `run_completed` checkpoint telemetry
timestamp; the later polling wakeup is reported separately as completion
detection delay and is not charged to workload throughput. Because every due
offset is strictly inside the window, exact
aggregate outstanding work at its end is scheduled arrivals minus those
terminal commits. A bounded sampler separately records ready backlog,
in-service claims, future scheduled work, completions, and oldest-due lag.
Forced SQL samples at activation, window end, and after polling detects the
final completion record their requested offset, observed sample-start delay,
callback duration, and full
observation interval. Their status split may reflect any time through callback
completion and is explicitly not exact boundary state. The due cutoff and
oldest-due lag use the callback-start wall clock, and the oldest-due change uses
the actual span between sample starts. The reported backlog rate is
finite-window net due-not-terminal accumulation from an empty pre-arrival
boundary, not an instantaneous derivative or proof of unsustainable growth.
Completion lag is
due-time to terminal commit for the collector's retained deterministic
correlation sample. Sampling queries are tagged control work and excluded from
workload query counts, but they still consume Repo/Postgres capacity and can be
delayed by saturation. The suite exposes offered/achieved rate, exact terminal
outstanding work, and backlog/lag trends, but intentionally emits no generic
safe-capacity recommendation for this finite-window scenario. Inspect sampler
duty and use repetitions before making a capacity claim.

Three comparative fairness bursts exercise heterogeneous cohorts staged at a
common due time. Runs alternate between cohorts so neither workload is hidden
behind a deliberately homogeneous prefix:

```console
mix docket.bench --scenario mixed_service_times --runs 100 \
  --concurrency 10 --pool-size 5 --hold-ms 500 --slow-percent 50 \
  --output results/mixed-service-times.json

mix docket.bench --scenario parked_wait_vs_blocking_wait --runs 100 \
  --concurrency 10 --pool-size 5 --hold-ms 500 \
  --output results/parked-vs-blocking.json

mix docket.bench --scenario cyclic_vs_one_step --runs 100 \
  --concurrency 10 --pool-size 5 \
  --cycle-moments 24 --drain-max-moments 4 \
  --output results/cyclic-vs-one-step.json
```

`mixed_service_times` assigns ten percent of the cohort by default (at least
one run) a blocking node and keeps the remainder one-step fast runs.
`--slow-percent` accepts 1 through 99, including a 50/50 mix; rounding always
preserves at least one run in each cohort. The artifact records requested and
actual shares, configured slots, the maximum slow-slot occupancy implied by
cohort size, and whether there are enough slow runs to fill every slot. When
there are not, the point remains valid but carries an explicit warning that it
cannot demonstrate all-slot slow-run hogging.
`parked_wait_vs_blocking_wait` splits the cohort between resident node sleeps
and a one-failure retry whose backoff has the same duration; retry resumes are
expected to produce a second ready claim. `cyclic_vs_one_step` compares a
requested multi-iteration cycle with one-step controls while applying the real
vehicle drain budget. The legacy-named `--cycle-moments` option is preserved
for CLI compatibility but counts cycle iterations: each iteration increments
the counter and traverses a separate decision superstep, so it is not an exact
committed-moment count. Defaults use 12 iterations and a four-moment drain, so
the workload is designed to cross the drain boundary before completion; the
artifact verifies yield and reacquisition totals only at aggregate scope.
`--drain-max-elapsed-ms` optionally adds the cooperative wall-clock boundary;
when omitted, the count boundary is isolated. Cycle and drain controls are
rejected for every other scenario, and `cycle-moments` must exceed
`drain-max-moments`.

Each artifact reports per-cohort activation-to-terminal,
activation-to-first-claim, and first-claim-to-terminal distributions in
addition to the aggregate measurements, plus each cohort's queue share: the
median activation-to-first-claim offset as a percentage of the median
activation-to-terminal offset. The first-claim/terminal split separates
waiting for a vehicle from per-run service, so cohort convoys — one cohort's
long node work occupying every vehicle while another cohort's ready backlog
waits — are visible directly. The retained first-claim distribution includes
p95, p99, and maximum offsets. Per-run normalized slowdown divides the
queue-inclusive activation-to-terminal offset by first-claim-to-terminal
duration; the mixed-service artifact also compares fast and slow cohort p50
and p95 slowdown. It additionally projects the fast cohort's per-run p50/p95
normalized slowdown, first-claim p95, queue-inclusive terminal p95, and
post-claim service p95. An all-fast phase is not silently run inside the same
timed interval; the artifact records that limitation and points to a matched
`empty_one_step` run when a separate control is required. Terminal rank is
explicitly relative to the retained deterministic correlation sample. When
every correlation is retained, an
additional rank-minus-staged-ordinal distribution shows which cohort overtook
or fell behind its staged position; that delta is omitted for sampled cohorts.
Retained cohort/population counts make the scope explicit. Retained claim
counts, subsequent-claim offsets, and subsequent ready age make retry
reacquisition visible instead of folding it invisibly into the service window.
The cyclic artifact additionally reports exact aggregate budget-yield counts
by fired limit, aggregate claim reacquisitions after yield, exact observed
lifecycle/checkpoint commit totals, and retained-correlation-sample per-run
checkpoint-count frequencies. The frequency artifact records sampled and
population run counts and must not be treated as a global distribution once
the collector bound is active. The one-step cohort also reports
wait-to-first-claim, queue-inclusive terminal, and post-claim p95 tails.
Yield/reacquisition telemetry deliberately carries no run identity, so its
invariants make aggregate—not per-run—claims. Its graph records the requested
iteration count and a separate terminal `max_supersteps` guard sized above that
workload; drain yielding is operational fairness and is never substituted with
terminal graph failure.

First-claim observations use the claim-scan telemetry emit time, so leases
from one batch share one observation timestamp. For retried runs the first
acquired claim opens the service window, which therefore includes any retry
backoff. Suite summaries carry per-cohort medians across repetitions, and the
terminal summary prints cohort terminal offsets, first-claim waits, per-run
service, normalized slowdown, retained-sample terminal rank, retained
subsequent claim counts, and queue share. These are burst characterizations,
not a starvation-freedom or bounded-wait guarantee.

Only `smoke`, `empty_one_step`, `claim_only`, `blocked_vehicles`,
`steady_arrival`, and those three comparative scenarios are implemented
today. Other benchmark suites
(replenished-arrival fairness, cache, freshness,
real multi-node scaling, notify/poll, amplification, and soak) are rejected
rather than silently approximated. `--event-policy none` is also rejected:
v0.1 persists lifecycle events and has no production event-suppression mode.

Ordinary `mix test` never runs the benchmark. Long saturation and soak runs
belong on scheduled dedicated hardware; performance gates must compare at
least three repetitions with matching hardware and Postgres fingerprints.
