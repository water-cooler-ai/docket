if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.MigrationTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.TestRepo

    @v1 20_260_716_000_101
    @v2 20_260_716_000_102
    @private 20_260_716_000_103

    defmodule InstallV1 do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(version: 1)
      def down, do: Docket.Postgres.Migration.down(version: 1)
    end

    defmodule UpgradeV2 do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(version: 2)
      def down, do: Docket.Postgres.Migration.down(version: 2)
    end

    defmodule InstallPrivate do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(prefix: "docket_private")
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_private")
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok
    end

    test "fresh v2 installs only current policy and partition authority" do
      :ok = Ecto.Migrator.up(TestRepo, @v2, UpgradeV2, log: false)

      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 2

      assert Enum.sort(owned_claim_tables("public")) ==
               ["docket_claim_partitions", "docket_claim_policy"]

      assert [[1, "legacy", nil, 0, nil]] =
               TestRepo.query!(
                 "SELECT id, admission_mode, max_active, policy_version, initialized_at " <>
                   "FROM docket_claim_policy"
               ).rows

      assert function_count("public") == 1

      assert Enum.sort(scope_indexes("public")) ==
               [
                 "docket_runs_scope_expired_index",
                 "docket_runs_scope_live_index",
                 "docket_runs_scope_ready_index"
               ]
    end

    test "ordinary v1-to-v2 upgrade backfills existing scopes transactionally" do
      :ok = Ecto.Migrator.up(TestRepo, @v1, InstallV1, log: false)

      insert_v1_graph_and_run("tenant-a", "run-a")
      insert_v1_graph_and_run(nil, "run-system")

      :ok = Ecto.Migrator.up(TestRepo, @v2, UpgradeV2, log: false)

      assert TestRepo.query!("SELECT scope_key FROM docket_claim_partitions ORDER BY scope_key").rows ==
               [[""], ["tenant-a"]]
    end

    test "v2 down removes only exact-cap objects and leaves v1 data" do
      :ok = Ecto.Migrator.up(TestRepo, @v2, UpgradeV2, log: false)
      :ok = Ecto.Migrator.down(TestRepo, @v2, UpgradeV2, log: false)

      assert owned_claim_tables("public") == []
      assert function_count("public") == 0
      assert TestRepo.query!("SELECT count(*) FROM docket_runs").rows == [[0]]
    end

    test "supports an explicit prefix without cross-schema objects" do
      :ok = Ecto.Migrator.up(TestRepo, @private, InstallPrivate, log: false)

      assert Enum.sort(owned_claim_tables("docket_private")) ==
               ["docket_claim_partitions", "docket_claim_policy"]

      assert owned_claim_tables("public") == []
      assert function_count("docket_private") == 1
    end

    defp owned_claim_tables(prefix) do
      TestRepo.query!(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1 AND table_name LIKE 'docket_claim_%'
        ORDER BY table_name
        """,
        [prefix]
      ).rows
      |> List.flatten()
    end

    defp function_count(prefix) do
      TestRepo.query!(
        """
        SELECT count(*)::integer
        FROM pg_proc
        JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
        WHERE pg_namespace.nspname = $1 AND proname = 'docket_tenant_fair_claim_v1'
        """,
        [prefix]
      ).rows
      |> hd()
      |> hd()
    end

    defp scope_indexes(prefix) do
      TestRepo.query!(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = $1 AND indexname LIKE 'docket_runs_scope_%_index'
        ORDER BY indexname
        """,
        [prefix]
      ).rows
      |> List.flatten()
    end

    defp insert_v1_graph_and_run(tenant_id, run_id) do
      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, 'graph', $2, $3, CURRENT_TIMESTAMP)
        """,
        [tenant_id, "hash-#{run_id}", <<1>>]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        VALUES ($1, $2, 'graph', $3, 'running', $4,
                1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP)
        """,
        [run_id, tenant_id, "hash-#{run_id}", <<1>>]
      )
    end
  end
end
