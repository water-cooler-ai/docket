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

CI builds and tests two legs (`.github/workflows/ci.yml`):

- **full** — optional deps present, the default local experience.
- **core** — `DOCKET_CORE_ONLY=1`, which drops `ecto_sql`/`postgrex` from
  `deps/0` in `mix.exs` to mirror a core-only host.

Both legs must pass `mix compile --warnings-as-errors` and the full test
suite. To reproduce the core leg locally:

```sh
DOCKET_CORE_ONLY=1 mix deps.get
DOCKET_CORE_ONLY=1 mix compile --force --warnings-as-errors
DOCKET_CORE_ONLY=1 mix test
```

(Re-run `mix deps.get` without the variable afterwards to restore
`mix.lock` — the core-only `deps.get` prunes the optional entries.)

### Postgres-backed tests

Tests tagged `:postgres` (the migration up/down round trip) need a live
Postgres and are excluded by default. Opt in with:

```sh
mix test --include postgres
```

The connection defaults to `postgres://localhost:5432/docket_migration_test`
(your OS username, no password); override with `DOCKET_TEST_DATABASE_URL`.
The test database is dropped and recreated on every run.
