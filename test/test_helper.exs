if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  Application.put_env(:docket, Docket.Postgres.TestRepo,
    url:
      System.get_env(
        "DOCKET_TEST_DATABASE_URL",
        "postgres://localhost:5432/docket_migration_test"
      ),
    pool_size: 2,
    log: false
  )
end

# Migration round-trip tests need a live Postgres. Opt in with:
#
#     mix test --include postgres
#
# The connection defaults to postgres://localhost:5432/docket_migration_test
# (OS username); override with DOCKET_TEST_DATABASE_URL. The database is
# dropped and recreated on every run.
ExUnit.start(exclude: [:postgres])
