if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.MigrationTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.TestRepo

    @v1 20_260_716_000_101
    @v2 20_260_716_000_102
    @private 20_260_716_000_103
    @private_v1 20_260_716_000_104
    @private_v2 20_260_716_000_105
    @failed_v2 20_260_716_000_106

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

    defmodule InstallPrivateV1 do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(prefix: "docket_private", version: 1)
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_private", version: 1)
    end

    defmodule UpgradePrivateV2 do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(prefix: "docket_private", version: 2)
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_private", version: 2)
    end

    defmodule FailedUpgradeV2 do
      use Ecto.Migration

      def up do
        Docket.Postgres.Migration.up(version: 2)
        flush()
        raise "forced v2 rollback"
      end

      def down, do: :ok
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

    test "fresh v2 and a populated v1-to-v2 upgrade have equivalent schemas" do
      :ok = Ecto.Migrator.up(TestRepo, @v2, UpgradeV2, log: false)
      fresh = schema_signature("public")

      :ok = Ecto.Migrator.up(TestRepo, @private_v1, InstallPrivateV1, log: false)
      insert_v1_graph_and_run("tenant-a", "run-a", "docket_private")
      :ok = Ecto.Migrator.up(TestRepo, @private_v2, UpgradePrivateV2, log: false)

      assert schema_signature("docket_private") == fresh

      assert TestRepo.query!("SELECT scope_key FROM docket_private.docket_claim_partitions").rows ==
               [["tenant-a"]]
    end

    test "database constraints reject invalid exact-cap authority" do
      :ok = Ecto.Migrator.up(TestRepo, @v2, UpgradeV2, log: false)

      rejected = [
        "UPDATE docket_claim_policy SET admission_mode = 'invalid' WHERE id = 1",
        "UPDATE docket_claim_policy SET max_active = 0, policy_version = 1, " <>
          "initialized_at = CURRENT_TIMESTAMP WHERE id = 1",
        "UPDATE docket_claim_policy SET policy_version = -1 WHERE id = 1",
        "INSERT INTO docket_claim_partitions (scope_key, max_active) VALUES ('bad-cap', 0)",
        "INSERT INTO docket_claim_partitions (scope_key, partition_version) " <>
          "VALUES ('bad-version', -1)",
        "INSERT INTO docket_claim_partitions (scope_key, admission_epoch) " <>
          "VALUES ('bad-epoch', -1)"
      ]

      Enum.each(rejected, fn statement ->
        error = assert_raise Postgrex.Error, fn -> TestRepo.query!(statement) end
        assert error.postgres.code == :check_violation
      end)

      assert [["legacy", nil, 0, nil]] =
               TestRepo.query!(
                 "SELECT admission_mode, max_active, policy_version, initialized_at " <>
                   "FROM docket_claim_policy"
               ).rows

      assert TestRepo.query!("SELECT count(*) FROM docket_claim_partitions").rows == [[0]]
    end

    @tag timeout: 20_000
    test "v1 inserts serialize before the v2 backfill snapshot on distinct connections" do
      :ok = Ecto.Migrator.up(TestRepo, @v1, InstallV1, log: false)
      parent = self()

      writer =
        Task.async(fn ->
          TestRepo.checkout(
            fn ->
              TestRepo.transaction(fn ->
                [[writer_backend]] = TestRepo.query!("SELECT pg_backend_pid()").rows
                insert_v1_graph_and_run("tenant-late", "run-late")
                send(parent, {:writer_ready, self(), writer_backend})

                receive do
                  :observe_migration -> :ok
                after
                  5_000 -> raise "timed out waiting for the migration backend"
                end

                migration_backend = await_migration_lock_wait!(writer_backend)
                send(parent, {:migration_waiting, self(), migration_backend})

                receive do
                  :commit_writer -> :committed
                after
                  5_000 -> raise "timed out waiting to commit the v1 insert"
                end
              end)
            end,
            timeout: 10_000
          )
        end)

      writer_pid = writer.pid
      assert_receive {:writer_ready, ^writer_pid, writer_backend}, 5_000

      migrator =
        Task.async(fn ->
          Ecto.Migrator.up(TestRepo, @v2, UpgradeV2, log: false, migration_lock: false)
        end)

      send(writer_pid, :observe_migration)
      assert_receive {:migration_waiting, ^writer_pid, migration_backend}, 5_000
      refute migration_backend == writer_backend

      send(writer_pid, :commit_writer)

      assert {:ok, :committed} = Task.await(writer, 5_000)
      assert :ok = Task.await(migrator, 10_000)

      assert TestRepo.query!("SELECT scope_key FROM docket_claim_partitions").rows ==
               [["tenant-late"]]
    end

    test "a failed transactional v2 upgrade preserves the v1 schema and data" do
      :ok = Ecto.Migrator.up(TestRepo, @v1, InstallV1, log: false)
      insert_v1_graph_and_run("tenant-a", "run-a")

      assert_raise RuntimeError, "forced v2 rollback", fn ->
        Ecto.Migrator.up(TestRepo, @failed_v2, FailedUpgradeV2, log: false)
      end

      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 1
      assert owned_claim_tables("public") == []
      assert scope_indexes("public") == []
      assert function_count("public") == 0
      assert v1_runs("public") == [["run-a", "tenant-a"]]
    end

    test "a populated custom-prefix upgrade backfills tenant and tenantless scopes" do
      :ok = Ecto.Migrator.up(TestRepo, @private_v1, InstallPrivateV1, log: false)
      insert_v1_graph_and_run("tenant-a", "run-a", "docket_private")
      insert_v1_graph_and_run(nil, "run-system", "docket_private")

      :ok = Ecto.Migrator.up(TestRepo, @private_v2, UpgradePrivateV2, log: false)

      assert TestRepo.query!(
               "SELECT scope_key FROM docket_private.docket_claim_partitions " <>
                 "ORDER BY scope_key"
             ).rows == [[""], ["tenant-a"]]

      assert v1_runs("docket_private") ==
               [["run-a", "tenant-a"], ["run-system", nil]]
    end

    test "v2 down removes only exact-cap objects and preserves populated v1 data" do
      :ok = Ecto.Migrator.up(TestRepo, @v1, InstallV1, log: false)
      insert_v1_graph_and_run("tenant-a", "run-a")
      insert_v1_graph_and_run(nil, "run-system")
      :ok = Ecto.Migrator.up(TestRepo, @v2, UpgradeV2, log: false)
      :ok = Ecto.Migrator.down(TestRepo, @v2, UpgradeV2, log: false)

      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 1
      assert owned_claim_tables("public") == []
      assert function_count("public") == 0
      assert v1_runs("public") == [["run-a", "tenant-a"], ["run-system", nil]]
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

    defp schema_signature(prefix) do
      columns =
        TestRepo.query!(
          """
          SELECT table_name, column_name, ordinal_position, udt_name, is_nullable,
                 column_default, is_generated, generation_expression
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name LIKE 'docket_%'
          ORDER BY table_name, ordinal_position
          """,
          [prefix]
        ).rows

      constraints =
        TestRepo.query!(
          """
          SELECT relation.relname, authority.conname, authority.contype::text,
                 pg_get_constraintdef(authority.oid, true)
          FROM pg_constraint AS authority
          JOIN pg_class AS relation ON relation.oid = authority.conrelid
          JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
          WHERE namespace.nspname = $1 AND relation.relname LIKE 'docket_%'
          ORDER BY relation.relname, authority.conname
          """,
          [prefix]
        ).rows

      indexes =
        TestRepo.query!(
          """
          SELECT tablename, indexname, indexdef
          FROM pg_indexes
          WHERE schemaname = $1 AND tablename LIKE 'docket_%'
          ORDER BY tablename, indexname
          """,
          [prefix]
        ).rows

      sequences =
        TestRepo.query!(
          """
          SELECT relation.relname, configuration.seqstart, configuration.seqincrement,
                 configuration.seqmax, configuration.seqmin, configuration.seqcache,
                 configuration.seqcycle
          FROM pg_sequence AS configuration
          JOIN pg_class AS relation ON relation.oid = configuration.seqrelid
          JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
          WHERE namespace.nspname = $1 AND relation.relname LIKE 'docket_%'
          ORDER BY relation.relname
          """,
          [prefix]
        ).rows

      functions =
        TestRepo.query!(
          """
          SELECT procedure.proname, pg_get_function_identity_arguments(procedure.oid),
                 pg_get_function_result(procedure.oid), pg_get_functiondef(procedure.oid)
          FROM pg_proc AS procedure
          JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
          WHERE namespace.nspname = $1 AND procedure.proname LIKE 'docket_%'
          ORDER BY procedure.proname
          """,
          [prefix]
        ).rows

      %{
        columns: normalize_schema(columns, prefix),
        constraints: normalize_schema(constraints, prefix),
        indexes: normalize_schema(indexes, prefix),
        sequences: normalize_schema(sequences, prefix),
        functions: normalize_schema(functions, prefix)
      }
    end

    defp normalize_schema(rows, prefix) do
      Enum.map(rows, fn row ->
        Enum.map(row, fn
          value when is_binary(value) ->
            value
            |> String.replace(~s("#{prefix}".), "")
            |> String.replace("#{prefix}.", "")

          value ->
            value
        end)
      end)
    end

    defp await_migration_lock_wait!(writer_backend) do
      deadline = System.monotonic_time(:millisecond) + 5_000
      poll_migration_lock_wait!(writer_backend, deadline)
    end

    defp poll_migration_lock_wait!(writer_backend, deadline) do
      rows =
        TestRepo.query!(
          """
          SELECT pid
          FROM pg_stat_activity
          WHERE datname = current_database()
            AND pid <> $1
            AND $1 = ANY(pg_blocking_pids(pid))
          """,
          [writer_backend]
        ).rows

      case rows do
        [[migration_backend] | _rest] -> migration_backend
        [] -> retry_migration_lock_wait!(writer_backend, deadline)
      end
    end

    defp retry_migration_lock_wait!(writer_backend, deadline) do
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(10)
        poll_migration_lock_wait!(writer_backend, deadline)
      else
        raise "the v2 migration did not wait on the docket_runs table lock"
      end
    end

    defp v1_runs(prefix) do
      TestRepo.query!(~s(SELECT run_id, tenant_id FROM "#{prefix}"."docket_runs" ORDER BY run_id)).rows
    end

    defp insert_v1_graph_and_run(tenant_id, run_id, prefix \\ "public") do
      graph_versions = ~s("#{prefix}"."docket_graph_versions")
      runs = ~s("#{prefix}"."docket_runs")

      TestRepo.query!(
        """
        INSERT INTO #{graph_versions}
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, 'graph', $2, $3, CURRENT_TIMESTAMP)
        """,
        [tenant_id, "hash-#{run_id}", <<1>>]
      )

      TestRepo.query!(
        """
        INSERT INTO #{runs}
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
