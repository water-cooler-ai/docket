# Contributing to Docket

## Release lines and PR targets

- `main` is the current stable line.
- `v0.1.0` is the active release branch for the 0.1.0 operational runtime
  (see [docs/architecture/docket-operational-transition-spec.md](docs/architecture/docket-operational-transition-spec.md)).
  **All 0.1.0 work targets `v0.1.0`, not `main`.** The release branch merges
  back to `main` when 0.1.0 ships.

## Optional Postgres dependencies

Docket is one package. The core runtime depends only on `telemetry`; the
Postgres backend (`Docket.Postgres.*`) sits behind optional `ecto_sql` and
`postgrex` dependencies and compiles only when the host application already
has them. Core-only hosts must compile Docket cleanly with zero warnings.

### Conditional compilation pattern

Every file under `lib/docket/postgres/` wraps its module in a compile-time
guard so it is skipped entirely when the optional dependencies are absent:

```elixir
if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunStore do
    # ...
  end
end
```

Rules:

- The `if` wraps the whole file — no partial modules, no runtime fallbacks.
- Never reference a `Docket.Postgres.*` module from core (`Docket.*`) code;
  the dependency arrow points only from the backend into the core.
- Known caveat: Mix does not recompile a dependency when the host later adds
  `ecto_sql`/`postgrex`. Hosts enabling the backend after first compile must
  run `mix deps.clean docket --build && mix deps.get`. Document this in the
  backend's installation docs when it ships.

### CI matrix

CI builds and tests three release gates (`.github/workflows/ci.yml`):

- **full** — optional deps present, the default local experience.
- **core** — `DOCKET_CORE_ONLY=1`, which drops `ecto_sql`/`postgrex` from
  `deps/0` in `mix.exs` to mirror a core-only host.
- **live Postgres** — PostgreSQL 13 and 17 exercise migrations, constraints,
  concurrency, recovery, retention, notification fallback, query plans, and a
  bounded tenant-fair claim benchmark smoke check.

Every gate compiles with `--warnings-as-errors`. The full and core gates run
the default suite; the live Postgres gate includes the tests tagged
`:postgres`. To reproduce the core gate locally:

```sh
DOCKET_CORE_ONLY=1 mix deps.get
DOCKET_CORE_ONLY=1 mix compile --force --warnings-as-errors
DOCKET_CORE_ONLY=1 mix test
```

(Re-run `mix deps.get` without the variable afterwards to restore
`mix.lock` — the core-only `deps.get` prunes the optional entries.)

### Postgres-backed tests

Tests tagged `:postgres` (migration round trips and RunStore claim/concurrency
coverage) need PostgreSQL 13 or newer and are excluded by default. Opt in with:

```sh
mix test --include postgres
```

Each live suite uses a dedicated database on localhost (your OS username, no
password) so destructive migration setup cannot race another suite. Generated
databases are removed after the test invocation; explicitly configured
databases are left in place. The defaults are
`docket_migration_test_<os-pid>`, `docket_run_store_test_<os-pid>`,
`docket_storage_test_<os-pid>`, `docket_graph_store_test_<os-pid>`,
`docket_lifecycle_storage_test_<os-pid>`, `docket_event_store_test_<os-pid>`,
`docket_vehicle_storage_test_<os-pid>`, and `docket_notifier_test_<os-pid>`.
Additional assembled-backend and retention suites use
`docket_pruner_test_<os-pid>`, `docket_backend_test_<os-pid>`, and
`docket_backend_sandbox_test_<os-pid>`. The shared backend matrix uses
`docket_shared_backend_test_<os-pid>`.
Override them with the corresponding `DOCKET_TEST_DATABASE_URL`,
`DOCKET_RUN_STORE_TEST_DATABASE_URL`, `DOCKET_STORAGE_TEST_DATABASE_URL`,
`DOCKET_GRAPH_STORE_TEST_DATABASE_URL`,
`DOCKET_LIFECYCLE_STORAGE_TEST_DATABASE_URL`,
`DOCKET_EVENT_STORE_TEST_DATABASE_URL`,
`DOCKET_VEHICLE_STORAGE_TEST_DATABASE_URL`, and
`DOCKET_NOTIFIER_TEST_DATABASE_URL`, `DOCKET_PRUNER_TEST_DATABASE_URL`,
`DOCKET_BACKEND_TEST_DATABASE_URL`, and
`DOCKET_BACKEND_SANDBOX_TEST_DATABASE_URL`, and
`DOCKET_SHARED_BACKEND_TEST_DATABASE_URL`. PostgreSQL 13 is the
implementation minimum because claim SQL uses materialized CTEs and the
built-in `gen_random_uuid()`.

### Tenant-fair claim benchmark

The high-cardinality claim benchmark is an exploratory, source-checkout-only
tool. It is not part of Docket's packaged runtime and its provisional policy,
partition, hint, and index DDL does not describe a shipped migration. The
runner creates an owned scratch schema, installs the real Docket tables there,
and adds only the benchmark-local provisional objects needed to compare claim
query shapes. Point it at a database where that schema lifecycle is safe:

```sh
DOCKET_BENCH_DATABASE_URL=postgres://localhost/docket_bench \
  mix run bench/postgres/tenant_fair_claim.exs -- --profile smoke --check
```

PostgreSQL 13 or newer is supported; CI exercises both the minimum (13) and
reference (17) environments. The larger local profiles are for investigation,
not routine CI.
See the [PostgreSQL operations guide](docs/postgres-operations.md#tenant-fair-claim-benchmark)
for profiles, artifacts, metric definitions, and interpretation constraints.

The CI smoke profile checks deterministic result, cap-safety, bounded
under-claim, artifact, and coarse plan invariants. Do not add latency,
percentile, throughput, exact planner-cost, or machine-relative buffer gates to
CI. Performance comparisons require a controlled database host, the same
resolved profile and seed, and the environment metadata saved with each
artifact.
