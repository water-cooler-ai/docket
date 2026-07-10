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

  Application.put_env(:docket, Docket.Postgres.RunStoreTestRepo,
    url:
      System.get_env(
        "DOCKET_RUN_STORE_TEST_DATABASE_URL",
        "postgres://localhost:5432/docket_run_store_test"
      ),
    pool_size: 10,
    log: false
  )
end

# Postgres migration and RunStore tests need a live Postgres. Opt in with:
#
#     mix test --include postgres
#
# The isolated databases default to docket_migration_test and
# docket_run_store_test on localhost (OS username, no password). Override them
# with DOCKET_TEST_DATABASE_URL and DOCKET_RUN_STORE_TEST_DATABASE_URL. Both
# databases are dropped and recreated by their suites.
ExUnit.start(exclude: [:postgres])
