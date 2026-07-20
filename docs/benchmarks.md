# Benchmarks

Docket ships one source-checkout benchmark suite under `bench/`. It is not part
of the public API or Hex package contract and requires a dedicated PostgreSQL
database via `DOCKET_BENCH_DATABASE_URL`.

- **Scorecard** (`bench/postgres/scorecard.exs`): system-level
  scenarios against the real supervised runtime, condensed into named 0–100
  scores with invariant gates.

## Scorecard

```sh
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

The drain scenarios drive the real `Docket.Runtime.Supervisor` with the
`Docket.Postgres` backend (poll-only dispatch). Claim efficiency instead calls
`RunStore.claim_due/3` directly with concurrent workers and intentionally
retains the resulting claims. Metrics come from durable rows, not telemetry;
for drain scenarios each run's wait is
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
fairness — are expanded from the `claim_policies` registry
(`bench/support/scorecard/config.ex`) and carry the policy name in the
scenario column, e.g. `60% hot tenant @16 [legacy]`. Each entry's config is
passed straight through as the runtime's `claim_policy` backend option. The
registry includes both Legacy and TenantFair. The TenantFair benchmark cap is
set above the largest frozen-backlog fixture because claim efficiency retains
claims by design; deterministic PostgreSQL tests, rather than scorecard rows,
remain the sticky-cap correctness evidence.

The current tenant-fairness scenario requires tenant-scoped storage, so only
TenantFair is a valid engine for that row. The registry still expands a Legacy
variant that required-tenancy validation rejects; do not treat the full
tenant-fairness scorecard as runnable release evidence until the harness skips
or replaces that invalid combination. The formal Legacy comparison is the
deterministic `N = 2, 10, 1000` ordinary-ready trace in the
[TenantFair Legacy separation](architecture/docket-tenant-fair.md#conditional-separation-from-legacy),
not a timing score.

Interpretation caveats:

- **Throughput and claim-efficiency targets are machine-relative.** They exist
  so a number becomes a trend line on one machine; comparing scores across
  hardware is meaningless. Ratio-based scores (scaling, fairness, surge) are
  dimensionless and travel better.
- **Fairness scores depend on the selected claim policy.** Keep comparisons on
  the same database and machine, and treat them as regression signals rather
  than release evidence.
- **Claim efficiency measures the raw claim path under concurrent callers.**
  Production serializes admission through one dispatcher per instance, so this
  is a ceiling probe, not a production simulation.
- **Latency in drain scenarios is queue plus service time** by construction
  (all runs due at once), which is what the fairness comparisons need.

### Invariants

Drain scenarios assert, via SQL on the scratch schema, no duplicate active
claim tokens, no active claims after drain, exact terminal accounting, no
stranded or poisoned runs, unique event sequence per run, and exactly one
terminal event per run. Claim efficiency has a separate frozen-backlog
invariant: every seeded run remains nonterminal and claimed exactly once, with
no poison or terminal events. Any applicable violation forces the scenario's
score to zero as `GATED`, and `--check` raises. A fast result can never outrank
a wrong one.

### Profiles

`smoke` finishes in well under a minute and is the optional local plumbing
check (`--profile smoke --check`); it is not a v0.1 CI release gate. `local` is
the default developer profile; `scale` grows backlogs roughly an order of
magnitude for dedicated runs. Exact knobs live in
`bench/support/scorecard/config.ex`.
