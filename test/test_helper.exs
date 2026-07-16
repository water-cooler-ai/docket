generated_database_repos =
  if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
    repo_specs = [
      {Docket.Postgres.TestRepo, "DOCKET_TEST_DATABASE_URL", "docket_migration_test", 2},
      {Docket.Postgres.RunStoreTestRepo, "DOCKET_RUN_STORE_TEST_DATABASE_URL",
       "docket_run_store_test", 10},
      {Docket.Postgres.ClaimPolicyAdminTestRepo, "DOCKET_CLAIM_POLICY_ADMIN_TEST_DATABASE_URL",
       "docket_claim_policy_admin_test", 10},
      {Docket.Postgres.StorageTestRepo, "DOCKET_STORAGE_TEST_DATABASE_URL", "docket_storage_test",
       10},
      {Docket.Postgres.GraphStoreTestRepo, "DOCKET_GRAPH_STORE_TEST_DATABASE_URL",
       "docket_graph_store_test", 10},
      {Docket.Postgres.LifecycleStorageTestRepo, "DOCKET_LIFECYCLE_STORAGE_TEST_DATABASE_URL",
       "docket_lifecycle_storage_test", 10},
      {Docket.Postgres.EventStoreTestRepo, "DOCKET_EVENT_STORE_TEST_DATABASE_URL",
       "docket_event_store_test", 10},
      {Docket.Postgres.VehicleStorageTestRepo, "DOCKET_VEHICLE_STORAGE_TEST_DATABASE_URL",
       "docket_vehicle_storage_test", 2},
      {Docket.Postgres.NotifierTestRepo, "DOCKET_NOTIFIER_TEST_DATABASE_URL",
       "docket_notifier_test", 10},
      {Docket.Postgres.PrunerTestRepo, "DOCKET_PRUNER_TEST_DATABASE_URL", "docket_pruner_test",
       10},
      {Docket.Postgres.BackendTestRepo, "DOCKET_BACKEND_TEST_DATABASE_URL", "docket_backend_test",
       10},
      {Docket.Postgres.SharedBackendTestRepo, "DOCKET_SHARED_BACKEND_TEST_DATABASE_URL",
       "docket_shared_backend_test", 10}
    ]

    Enum.reduce(repo_specs, [], fn {repo, environment_variable, database, pool_size}, generated ->
      {url, generated?} =
        case System.fetch_env(environment_variable) do
          {:ok, url} -> {url, false}
          :error -> {"postgres://localhost:5432/#{database}_#{System.pid()}", true}
        end

      Application.put_env(:docket, repo,
        url: url,
        pool_size: pool_size,
        log: false
      )

      if generated?, do: [repo | generated], else: generated
    end)
  else
    []
  end

generated_database_repos =
  if Code.ensure_loaded?(Ecto.Adapters.SQL.Sandbox) and
       Code.ensure_loaded?(Docket.Postgres.BackendSandboxTestRepo) do
    repo = Docket.Postgres.BackendSandboxTestRepo

    {url, generated?} =
      case System.fetch_env("DOCKET_BACKEND_SANDBOX_TEST_DATABASE_URL") do
        {:ok, url} -> {url, false}
        :error -> {"postgres://localhost:5432/docket_backend_sandbox_test_#{System.pid()}", true}
      end

    Application.put_env(:docket, repo,
      url: url,
      pool: Ecto.Adapters.SQL.Sandbox,
      pool_size: 2,
      log: false
    )

    if generated?, do: [repo | generated_database_repos], else: generated_database_repos
  else
    generated_database_repos
  end

# Postgres migration and RunStore tests need a live Postgres. Opt in with:
#
#     mix test --include postgres
#
# Each live suite and BEAM invocation uses its own isolated database on localhost
# (OS username, no password). Auto-generated databases are removed after the
# suite; explicitly configured databases are left in place. Override the URLs with
# DOCKET_TEST_DATABASE_URL, DOCKET_RUN_STORE_TEST_DATABASE_URL,
# DOCKET_STORAGE_TEST_DATABASE_URL, DOCKET_GRAPH_STORE_TEST_DATABASE_URL,
# DOCKET_LIFECYCLE_STORAGE_TEST_DATABASE_URL, DOCKET_EVENT_STORE_TEST_DATABASE_URL,
# DOCKET_VEHICLE_STORAGE_TEST_DATABASE_URL, DOCKET_NOTIFIER_TEST_DATABASE_URL,
# DOCKET_PRUNER_TEST_DATABASE_URL, and DOCKET_BACKEND_TEST_DATABASE_URL.
# The shared backend matrix uses DOCKET_SHARED_BACKEND_TEST_DATABASE_URL.
ExUnit.start(exclude: [:postgres])

ExUnit.after_suite(fn _result ->
  postgres_included? =
    ExUnit.configuration()
    |> Keyword.fetch!(:include)
    |> Enum.any?(fn
      :postgres -> true
      {:postgres, _value} -> true
      _filter -> false
    end)

  if postgres_included? do
    Enum.each(generated_database_repos, fn repo ->
      _ = Ecto.Adapters.Postgres.storage_down(repo.config())
    end)
  end
end)
