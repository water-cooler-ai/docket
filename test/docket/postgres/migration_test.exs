if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.MigrationTest do
    # Runs against a live Postgres; excluded by default. See test_helper.exs.
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.TestRepo

    defmodule InstallDocket do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule InstallDocketPrefixed do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(prefix: "docket_private")
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_private")
    end

    @migration_version 20_260_709_000_001
    @prefixed_migration_version 20_260_709_000_002

    @tables ~w(docket_events docket_graph_versions docket_runs)

    @run_columns ~w(
      id run_id tenant_id graph_id graph_hash status step input output
      metadata state checkpoint_seq latest_checkpoint_type claim_token
      claimed_at wake_at attempts operational_status operational_error
      inserted_at started_at updated_at finished_at
    )

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok
    end

    test "migrates up, down, and back up cleanly on a fresh Postgres" do
      assert :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      assert tables("public") == @tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 1
      assert columns("docket_runs") == Enum.sort(@run_columns)

      assert nullable?("docket_runs", "tenant_id")

      indexes = indexes("docket_runs")

      assert indexes["docket_runs_run_id_index"] =~ "CREATE UNIQUE INDEX"

      assert indexes["docket_runs_tenant_id_status_index"] =~
               "WHERE (tenant_id IS NOT NULL)"

      assert indexes["docket_runs_tenant_id_graph_id_status_index"] =~
               "WHERE (tenant_id IS NOT NULL)"

      assert indexes["docket_runs_wake_at_index"] =~ "WHERE (wake_at IS NOT NULL)"

      assert indexes["docket_runs_operational_status_index"] =~
               "WHERE (operational_status <> 'active'::text)"

      assert indexes["docket_runs_status_updated_at_index"] =~ "(status, updated_at)"

      assert indexes("docket_graph_versions")["docket_graph_versions_graph_id_graph_hash_index"] =~
               "CREATE UNIQUE INDEX"

      assert foreign_key("docket_runs", "docket_runs_graph_version_fkey") ==
               {"docket_graph_versions", ["graph_id", "graph_hash"], ["graph_id", "graph_hash"]}

      assert indexes("docket_events")["docket_events_run_id_seq_index"] =~
               "CREATE UNIQUE INDEX"

      assert_row_round_trip()

      assert :ok = Ecto.Migrator.down(TestRepo, @migration_version, InstallDocket, log: false)

      assert tables("public") == []
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 0

      assert :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      assert tables("public") == @tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 1
    end

    test "migrates up and down inside a dedicated schema prefix" do
      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @prefixed_migration_version,
                 InstallDocketPrefixed,
                 log: false
               )

      assert tables("docket_private") == @tables
      assert tables("public") == []

      assert Docket.Postgres.Migration.migrated_version(
               repo: TestRepo,
               prefix: "docket_private"
             ) == 1

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @prefixed_migration_version,
                 InstallDocketPrefixed,
                 log: false
               )

      assert tables("docket_private") == []
    end

    defp assert_row_round_trip do
      now = DateTime.utc_now()

      assert {:ok, _version} =
               %{graph_id: "g1", graph_hash: "abc123", graph: %{"nodes" => []}}
               |> Docket.Postgres.Schemas.GraphVersion.changeset()
               |> TestRepo.insert()

      assert {:ok, run} =
               %{
                 run_id: "run_1",
                 graph_id: "g1",
                 graph_hash: "abc123",
                 status: :running,
                 input: %{"prompt" => "hello"},
                 state: %{"channels" => %{}, "version" => 1}
               }
               |> Docket.Postgres.Schemas.Run.changeset()
               |> TestRepo.insert()

      assert run.operational_status == :active
      assert run.metadata == %{}
      assert run.step == 0
      assert run.checkpoint_seq == 0
      assert run.attempts == 0
      assert run.tenant_id == nil
      assert run.output == nil

      assert {:error, changeset} =
               %{
                 run_id: "run_1",
                 graph_id: "g1",
                 graph_hash: "abc123",
                 status: :running,
                 input: %{"prompt" => "hello"},
                 state: %{"channels" => %{}, "version" => 1}
               }
               |> Docket.Postgres.Schemas.Run.changeset()
               |> TestRepo.insert()

      assert {"has already been taken", _meta} = changeset.errors[:run_id]

      assert {:ok, _event} =
               %{
                 run_id: "run_1",
                 seq: 1,
                 type: :run_initialized,
                 step: 0,
                 occurred_at: now
               }
               |> Docket.Postgres.Schemas.Event.changeset()
               |> TestRepo.insert()
    end

    defp tables(schema) do
      %{rows: rows} =
        TestRepo.query!(
          """
          SELECT table_name FROM information_schema.tables
          WHERE table_schema = $1 AND table_name LIKE 'docket_%'
          """,
          [schema]
        )

      rows |> List.flatten() |> Enum.sort()
    end

    defp columns(table) do
      %{rows: rows} =
        TestRepo.query!(
          """
          SELECT column_name FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = $1
          """,
          [table]
        )

      rows |> List.flatten() |> Enum.sort()
    end

    defp nullable?(table, column) do
      %{rows: [[answer]]} =
        TestRepo.query!(
          """
          SELECT is_nullable FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
          """,
          [table, column]
        )

      answer == "YES"
    end

    defp indexes(table) do
      %{rows: rows} =
        TestRepo.query!(
          "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = $1",
          [table]
        )

      Map.new(rows, fn [name, def] -> {name, def} end)
    end

    defp foreign_key(table, constraint) do
      %{rows: [[foreign_table, local_columns, foreign_columns]]} =
        TestRepo.query!(
          """
          SELECT foreign_table.relname,
                 array_agg(local_column.attname ORDER BY key_position.ordinality),
                 array_agg(foreign_column.attname ORDER BY key_position.ordinality)
          FROM pg_constraint
          JOIN pg_class local_table ON local_table.oid = pg_constraint.conrelid
          JOIN pg_class foreign_table ON foreign_table.oid = pg_constraint.confrelid
          JOIN unnest(pg_constraint.conkey, pg_constraint.confkey)
            WITH ORDINALITY AS key_position(local_num, foreign_num, ordinality) ON true
          JOIN pg_attribute local_column
            ON local_column.attrelid = local_table.oid
           AND local_column.attnum = key_position.local_num
          JOIN pg_attribute foreign_column
            ON foreign_column.attrelid = foreign_table.oid
           AND foreign_column.attnum = key_position.foreign_num
          WHERE local_table.relname = $1
            AND pg_constraint.conname = $2
            AND pg_constraint.contype = 'f'
          GROUP BY foreign_table.relname
          """,
          [table, constraint]
        )

      {foreign_table, local_columns, foreign_columns}
    end
  end
end
