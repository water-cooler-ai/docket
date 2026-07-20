# Migrating from Docket 0.0.1 to 0.1.0

Docket 0.1.0 keeps node modules, graph definitions, schemas, reducers,
interrupts, and executors unchanged. The production lifecycle changes from a
host checkpoint callback and resident per-run processes to one configured
backend that owns persistence, scheduling, recovery, and signals.

## Drain and cut over

1. Stop new 0.0.1 starts, drain or terminate active runs, and stop old writers.
2. Delete the host checkpoint handler and Docket-specific host persistence.
3. Generate and run the host migration:

   ```sh
   mix docket.gen.migration -r MyApp.Repo
   mix ecto.migrate -r MyApp.Repo
   ```

   Then configure the complete backend:

   ```elixir
   defmodule MyApp.Docket do
     use Docket,
       repo: MyApp.Repo,
       backend: Docket.Postgres,
       pruner: [
         interval_ms: :timer.hours(1),
         event_retention_ms: :timer.hours(24 * 30),
         run_retention_ms: :timer.hours(24 * 90),
         batch_size: 1_000
       ]
   end
   ```

   Add the configured facade after the Repo in the application supervision
   tree:

   ```elixir
   children = [MyApp.Repo, MyApp.Docket]
   ```

   For a non-`public` PostgreSQL schema, generate with the same prefix configured
   on the Docket facade:

   ```sh
   mix docket.gen.migration -r MyApp.Repo --prefix automation
   ```

   Required-tenancy adopters must also configure TenantFair with an explicit
   `default_max_active_runs`; see the
   [current PostgreSQL migration and rollout guide](../postgres-operations.md#existing-v1-installations).
   The generated fresh migration installs the current schema. Existing
   schema-V1 Docket installations instead use `--upgrade-from-v1`; stop every
   Docket writer before the upgrade. The
   runtime refuses to start backend children when the recorded schema version
   does not match the current library.

4. Publish each unchanged graph with `save_graph/2` and retain its `GraphRef`.
5. Replace `run` with `start_run`, and replace `get_run` with `fetch_run` or
   `inspect_run`. Delete host-owned `resume` orchestration; backend claims
   recover persisted work.
6. Move best-effort notifications to `checkpoint_observers:`. Consume retained
   events when delivery itself must be durable.

Host-defined 0.0.1 schemas and checkpoint handlers differ between adopters, so
Docket cannot provide a universal automatic database migration. The supported
transition is an explicit drain and cut-over, not a compatibility alias,
dual-write period, or public Run-map serialization bridge.
