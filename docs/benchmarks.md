# Benchmarks

Docket ships two source-checkout benchmark suites under `bench/`. Neither is
part of the public API or the Hex package contract; both require a dedicated
PostgreSQL database via `DOCKET_BENCH_DATABASE_URL`.

- **Scorecard** (`bench/postgres/scorecard.exs`, this document): system-level
  scenarios against the real supervised runtime, condensed into named 0–100
  scores with invariant gates.
- **Tenant-fair claim** (`bench/postgres/tenant_fair_claim.exs`, documented in
  `docs/postgres-operations.md`): SQL-prototype comparison for the TenantFair
  admission engine's candidate queries. A different layer — it measures
  candidate statements, not the running system.

## Scorecard

```
DOCKET_BENCH_DATABASE_URL=postgres://user@localhost:5432/docket_bench \
  mix run bench/postgres/scorecard.exs -- --profile local
```

Options:

| Switch | Meaning |
| --- | --- |
| `--profile smoke\|local\|scale` | workload size and score targets (default `local`) |
| `--only a,b` | run a subset of scenarios |
| `--output PATH` | artifact directory (default `tmp/bench/postgres/scorecard/<run-id>/`) |
| `--check` | raise on any invariant violation (CI mode; never gates on timing) |
| `--keep-schema` | keep the scratch schema for inspection |
| `--seed N` | deterministic interleave seed |

Each run creates an isolated scratch schema (`docket_bench_<pid>_<uniq>`),
installs the production migration into it, runs the selected scenarios
sequentially, prints one scorecard table, and writes `manifest.json` and
`scorecard.json` artifacts.

### Scenarios and scores

Every scenario drives the real `Docket.Runtime.Supervisor` with the
`Docket.Postgres` backend (poll-only dispatch) and computes its metrics from
durable rows, not from telemetry: each run's wait is
`finished_at − max(staged due time, runtime start)`, so seeding overhead
never counts as queue time. Scores are 0–100; the mapping from raw measurements to a
score is stated per scenario and every threshold below is an explicit
calibration knob in the profile.

| Metric | Scenario | Score definition |
| --- | --- | --- |
| Throughput | drain N immediately-due one-step runs at fixed concurrency | `100 · min(1, runs_per_sec / target_runs_per_sec)` |
| Concurrency scaling | same per-slot workload at increasing concurrency levels | `100 · clamp(E)` where `E = (T(c_max)/T(c_min)) / (c_max/c_min)` |
| Claim efficiency | direct `claim_due` workers drain a frozen backlog (no runtime) | `100 · min(1, claims_per_sec / target_claims_per_sec)` |
| Tenant fairness | one hot tenant seeds 60% of the backlog first; light tenants after | `100 · clamp(1 − light_p95_wait / drain_time)` |
| Fast/slow fairness | fast cohort alone vs fast cohort behind a slow cohort | 100 at slowdown ≤ `good`, linear to 0 at ≥ `bad` |
| Surge resilience | steady arrivals at 40% of measured capacity plus a mid-window burst | `100 · min(1, ideal_recovery / measured_recovery)` |

### Claim-policy dimension

The claim-sensitive rows — claim efficiency, tenant fairness, and fast/slow
fairness — run once per entry in the `claim_policies` registry
(`bench/support/scorecard/config.ex`) and carry the policy name in the
scenario column, e.g. `60% hot tenant @16 [legacy]`. Each entry's config is
passed straight through as the runtime's `claim_policy` backend option. The
registry currently holds only the default legacy implementation; when the
TenantFair engine ships, adding one entry produces side-by-side scored rows
for both policies, turning the fairness rows into a direct policy comparison.

Interpretation caveats:

- **Throughput and claim-efficiency targets are machine-relative.** They exist
  so a number becomes a trend line on one machine; comparing scores across
  hardware is meaningless. Ratio-based scores (scaling, fairness, surge) are
  dimensionless and travel better.
- **Both fairness rows are expected to score low** while the legacy
  tenant-blind claim policy is the default: the scenarios construct exactly
  the head-of-line convoys the TenantFair design
  (`docs/architecture/docket-tenant-claim-fairness-design.md`) exists to fix,
  and the rows are the regression hooks for that work. A low score here is an
  honest baseline, not a defect in the run.
- **Claim efficiency measures the raw claim path under concurrent callers.**
  Production serializes admission through one dispatcher per instance, so this
  is a ceiling probe, not a production simulation.
- **Latency in drain scenarios is queue plus service time** by construction
  (all runs due at once), which is what the fairness comparisons need.

### Invariants

After every scenario the suite asserts, via SQL on the scratch schema: no
duplicate active claim tokens, no active claims after drain, exact terminal
accounting for all seeded runs, no stranded non-terminal runs, no poisoned
runs, unique event sequence per run, and exactly one terminal event per run.
Correctness gates are absolute: any violation forces the scenario's score to
zero-out as `GATED` and `--check` raises. A fast result can never outrank a
wrong one.

### Profiles

`smoke` finishes in well under a minute and is the CI shape
(`--profile smoke --check`); `local` is the default developer profile;
`scale` grows backlogs roughly an order of magnitude for dedicated runs.
Exact knobs live in `bench/support/scorecard/config.ex`.

## Relationship to the DCKT-38 exploratory harness

The `codex/dckt-38-v010` branch carries the full exploratory benchmark
harness (saturation matrices, knee analysis, blocked-vehicle plateaus,
observer-effect controls). The scorecard supersedes it for regression
tracking; the harness remains the tool for open-ended capacity exploration.
Scenario lineage: `empty_one_step → throughput`, knee matrix → `concurrency`,
`claim_only → claim_ceiling`, `mixed_service_times → fast_slow`,
`steady_arrival → surge`; `tenant_fairness` is new with the DCKT-58/59
fairness contracts.
