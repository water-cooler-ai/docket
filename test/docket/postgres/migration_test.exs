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

    defmodule InstallDocketReservedPrefix do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(prefix: "select")
      def down, do: Docket.Postgres.Migration.down(prefix: "select")
    end

    @migration_version 20_260_709_000_001
    @prefixed_migration_version 20_260_709_000_002
    @reserved_prefix_migration_version 20_260_709_000_003

    @tables ~w(docket_events docket_graph_versions docket_runs)

    @run_columns ~w(
      id run_id tenant_id graph_id graph_hash status step state
      checkpoint_seq latest_checkpoint_type claim_token
      claimed_at wake_at claim_attempts claim_abandons poisoned_at poison_reason
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
      install!()

      assert tables("public") == @tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 1
      assert columns("docket_runs") == Enum.sort(@run_columns)

      assert column_type("docket_graph_versions", "graph") == "bytea"

      assert column_type("docket_runs", "state") == "bytea"

      assert column_type("docket_runs", "poison_reason") == "text"
      assert column_type("docket_events", "payload") == "bytea"
      assert column_type("docket_events", "metadata") == "bytea"

      assert nullable?("docket_runs", "tenant_id")
      refute nullable?("docket_runs", "started_at")

      indexes = indexes("docket_runs")

      assert indexes["docket_runs_run_id_index"] =~ "CREATE UNIQUE INDEX"

      assert indexes["docket_runs_tenant_id_status_index"] =~
               "WHERE (tenant_id IS NOT NULL)"

      assert indexes["docket_runs_tenant_id_graph_id_status_index"] =~
               "WHERE (tenant_id IS NOT NULL)"

      ready = indexes["docket_runs_wake_at_id_index"]

      assert ready =~ "btree (wake_at, id)"
      assert ready =~ "status = 'running'"
      assert ready =~ "poisoned_at IS NULL"
      assert ready =~ "claim_token IS NULL"
      assert ready =~ "wake_at IS NOT NULL"

      expired = indexes["docket_runs_claimed_at_id_index"]

      assert expired =~ "btree (claimed_at, id)"
      assert expired =~ "status = 'running'"
      assert expired =~ "poisoned_at IS NULL"
      assert expired =~ "claim_token IS NOT NULL"

      assert indexes["docket_runs_poisoned_at_index"] =~ "WHERE (poisoned_at IS NOT NULL)"

      assert indexes["docket_runs_status_updated_at_index"] =~ "(status, updated_at)"

      assert indexes("docket_graph_versions")["docket_graph_versions_graph_id_graph_hash_index"] =~
               "CREATE UNIQUE INDEX"

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

      assert {:ok, _} = insert_run([], "docket_private")

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @prefixed_migration_version,
                 InstallDocketPrefixed,
                 log: false
               )

      assert tables("docket_private") == []
    end

    test "quotes a valid prefix that is also a Postgres keyword" do
      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @reserved_prefix_migration_version,
                 InstallDocketReservedPrefix,
                 log: false
               )

      assert tables("select") == @tables

      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo, prefix: "select") == 1

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @reserved_prefix_migration_version,
                 InstallDocketReservedPrefix,
                 log: false
               )

      assert tables("select") == []
    end

    test "accepts every valid lifecycle row shape" do
      install!()

      now = DateTime.utc_now()

      valid_shapes = [
        ready_running: [],
        claimed_running: [wake_at: nil, claim_token: uuid(), claimed_at: now],
        poisoned_running: [
          wake_at: nil,
          poisoned_at: now,
          poison_reason: "max_claim_attempts_exceeded"
        ],
        waiting: [status: "waiting", wake_at: nil],
        done: [status: "done", wake_at: nil, finished_at: now],
        failed: [status: "failed", wake_at: nil, finished_at: now],
        cancelled: [status: "cancelled", wake_at: nil, finished_at: now]
      ]

      for {shape, overrides} <- valid_shapes do
        assert {:ok, _} = insert_run(overrides), "expected valid shape #{shape} to insert"
      end
    end

    test "rejects every invalid lifecycle tuple through raw SQL" do
      install!()

      now = DateTime.utc_now()

      # Each case is one minimal mutation off a valid row, so exactly the
      # named constraint fires. A non-running row carrying poison violates
      # the waiting/terminal-idle and poisoned-shape constraints
      # inseparably, so that case accepts either name.
      invalid_tuples = [
        {"unknown status", [status: "created", wake_at: nil], ~w(docket_runs_status_check)},
        {"running with finished_at", [finished_at: now], ~w(docket_runs_finished_at_check)},
        {"done without finished_at", [status: "done", wake_at: nil],
         ~w(docket_runs_finished_at_check)},
        {"claim token without claimed_at", [wake_at: nil, claim_token: uuid()],
         ~w(docket_runs_claim_pair_check)},
        {"claimed_at without claim token", [claimed_at: now], ~w(docket_runs_claim_pair_check)},
        {"poisoned_at without poison_reason", [wake_at: nil, poisoned_at: now],
         ~w(docket_runs_poison_pair_check)},
        {"poison_reason without poisoned_at", [poison_reason: "stuck"],
         ~w(docket_runs_poison_pair_check)},
        {"waiting with a wake", [status: "waiting"], ~w(docket_runs_waiting_terminal_idle_check)},
        {"waiting with a claim",
         [status: "waiting", wake_at: nil, claim_token: uuid(), claimed_at: now],
         ~w(docket_runs_waiting_terminal_idle_check)},
        {"waiting with poison",
         [
           status: "waiting",
           wake_at: nil,
           poisoned_at: now,
           poison_reason: "stuck"
         ], ~w(docket_runs_waiting_terminal_idle_check docket_runs_poisoned_shape_check)},
        {"terminal with a wake", [status: "done", finished_at: now],
         ~w(docket_runs_waiting_terminal_idle_check)},
        {"poisoned with a claim",
         [
           wake_at: nil,
           poisoned_at: now,
           poison_reason: "stuck",
           claim_token: uuid(),
           claimed_at: now
         ], ~w(docket_runs_poisoned_shape_check)},
        {"poisoned with a wake", [poisoned_at: now, poison_reason: "stuck"],
         ~w(docket_runs_poisoned_shape_check)},
        {"running with both wake and claim", [claim_token: uuid(), claimed_at: now],
         ~w(docket_runs_running_schedule_check)},
        {"running with neither wake nor claim", [wake_at: nil],
         ~w(docket_runs_running_schedule_check)},
        {"negative step", [step: -1], ~w(docket_runs_counters_check)},
        {"negative checkpoint_seq", [checkpoint_seq: -1], ~w(docket_runs_counters_check)},
        {"negative claim_attempts", [claim_attempts: -1], ~w(docket_runs_counters_check)},
        {"negative claim_abandons", [claim_abandons: -1], ~w(docket_runs_counters_check)}
      ]

      for {label, overrides, constraints} <- invalid_tuples do
        assert {:error, %Postgrex.Error{postgres: %{code: :check_violation} = pg}} =
                 insert_run(overrides),
               "expected #{label} to raise a check violation"

        assert pg.constraint in constraints,
               "expected #{label} to violate one of #{inspect(constraints)}, " <>
                 "got #{inspect(pg.constraint)}"
      end

      assert {:error, %Postgrex.Error{postgres: %{code: :not_null_violation} = pg}} =
               insert_run(started_at: nil)

      assert pg.column == "started_at"
    end

    test "referential integrity binds runs to graphs and events to runs" do
      install!()

      # A run cannot reference a graph version that was never published.
      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation} = pg}} =
               insert_run(graph_hash: "unpublished")

      assert pg.constraint == "docket_runs_graph_hash_fkey"

      assert {:ok, _} = insert_run([])

      # A referenced graph version cannot be deleted; an unreferenced one can.
      insert_graph_version!("g_unused", "hash_unused")

      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation}}} =
               TestRepo.query(
                 "DELETE FROM docket_graph_versions WHERE graph_id = 'g1'",
                 []
               )

      assert %{num_rows: 1} =
               TestRepo.query!(
                 "DELETE FROM docket_graph_versions WHERE graph_id = 'g_unused'",
                 []
               )

      # An event cannot reference a missing run.
      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation} = pg}} =
               insert_event("no_such_run", 1)

      assert pg.constraint == "docket_events_run_id_fkey"

      # Deleting a run cascades to its events.
      {:ok, %{rows: [[run_id]]}} =
        insert_run(status: "done", wake_at: nil, finished_at: DateTime.utc_now())

      assert {:ok, _} = insert_event(run_id, 1)
      assert {:ok, _} = insert_event(run_id, 2)

      assert %{num_rows: 1} =
               TestRepo.query!("DELETE FROM docket_runs WHERE run_id = $1", [run_id])

      assert %{rows: [[0]]} =
               TestRepo.query!("SELECT count(*) FROM docket_events WHERE run_id = $1", [run_id])
    end

    test "dispatch and poison-introspection scans use their partial indexes" do
      install!()

      ready_scan = """
      SELECT id FROM docket_runs
      WHERE status = 'running' AND poisoned_at IS NULL
        AND claim_token IS NULL AND wake_at <= now()
      ORDER BY wake_at, id
      LIMIT 10
      """

      expired_scan = """
      SELECT id FROM docket_runs
      WHERE status = 'running' AND poisoned_at IS NULL
        AND claim_token IS NOT NULL AND claimed_at < now() - interval '1 minute'
      ORDER BY claimed_at, id
      LIMIT 10
      """

      poison_scan = """
      SELECT id FROM docket_runs
      WHERE poisoned_at IS NOT NULL
      ORDER BY poisoned_at
      """

      assert explain(ready_scan) =~ "docket_runs_wake_at_id_index"
      assert explain(expired_scan) =~ "docket_runs_claimed_at_id_index"
      assert explain(poison_scan) =~ "docket_runs_poisoned_at_index"
    end

    defp install! do
      assert :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
    end

    defp assert_row_round_trip do
      now = DateTime.utc_now()

      assert {:ok, _version} =
               %{graph_id: "g1", graph_hash: "abc123", graph: <<131, 106>>}
               |> Docket.Postgres.Schemas.GraphVersion.changeset()
               |> TestRepo.insert()

      assert {:ok, run} =
               %{
                 run_id: "run_1",
                 graph_id: "g1",
                 graph_hash: "abc123",
                 status: :running,
                 state: <<131, 106>>,
                 started_at: now,
                 wake_at: now
               }
               |> Docket.Postgres.Schemas.Run.changeset()
               |> TestRepo.insert()

      assert run.step == 0
      assert run.checkpoint_seq == 0
      assert run.claim_attempts == 0
      assert run.poisoned_at == nil
      assert run.poison_reason == nil
      assert run.tenant_id == nil

      assert {:error, changeset} =
               %{
                 run_id: "run_1",
                 graph_id: "g1",
                 graph_hash: "abc123",
                 status: :running,
                 state: <<131, 106>>,
                 started_at: now,
                 wake_at: now
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
                 payload: <<131, 116, 0, 0, 0, 0>>,
                 metadata: <<131, 116, 0, 0, 0, 0>>,
                 occurred_at: now
               }
               |> Docket.Postgres.Schemas.Event.changeset()
               |> TestRepo.insert()
    end

    # Inserts a docket_runs row through raw SQL — bypassing changesets on
    # purpose — as a ready `running` row unless overridden. Publishes the
    # success.
    defp insert_run(overrides, prefix \\ "public") do
      now = DateTime.utc_now()
      ensure_graph_version(prefix)

      base = %{
        run_id: "run_#{System.unique_integer([:positive])}",
        tenant_id: nil,
        graph_id: "g1",
        graph_hash: "abc123",
        status: "running",
        step: 0,
        state: <<131, 106>>,
        checkpoint_seq: 0,
        latest_checkpoint_type: nil,
        claim_token: nil,
        claimed_at: nil,
        wake_at: now,
        claim_attempts: 0,
        claim_abandons: 0,
        poisoned_at: nil,
        poison_reason: nil,
        inserted_at: now,
        started_at: now,
        updated_at: now,
        finished_at: nil
      }

      row = Map.merge(base, Map.new(overrides))
      columns = Map.keys(base)
      placeholders = Enum.map_join(1..length(columns), ", ", &"$#{&1}")

      TestRepo.query(
        """
        INSERT INTO #{prefix}.docket_runs (#{Enum.join(columns, ", ")})
        VALUES (#{placeholders})
        RETURNING run_id
        """,
        Enum.map(columns, &Map.fetch!(row, &1))
      )
    end

    defp insert_event(run_id, seq) do
      now = DateTime.utc_now()

      TestRepo.query(
        """
        INSERT INTO docket_events
          (run_id, seq, type, step, payload, metadata, occurred_at, inserted_at)
        VALUES ($1, $2, 'node_completed', 0, $3, $3, $4, $4)
        """,
        [run_id, seq, <<131, 116, 0, 0, 0, 0>>, now]
      )
    end

    defp ensure_graph_version(prefix) do
      insert_graph_version!("g1", "abc123", prefix)
    end

    defp insert_graph_version!(graph_id, graph_hash, prefix \\ "public") do
      TestRepo.query!(
        """
        INSERT INTO #{prefix}.docket_graph_versions (graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        """,
        [graph_id, graph_hash, <<131, 106>>, DateTime.utc_now()]
      )
    end

    defp uuid, do: Ecto.UUID.dump!(Ecto.UUID.generate())

    # SET LOCAL and EXPLAIN must run on the same pooled connection, so both
    # happen inside one transaction. Seq scans are disabled because the
    # planner never picks an index on an empty table otherwise.
    defp explain(sql) do
      {:ok, plan} =
        TestRepo.transaction(fn ->
          TestRepo.query!("SET LOCAL enable_seqscan = off")

          %{rows: rows} = TestRepo.query!("EXPLAIN #{sql}")

          Enum.map_join(rows, "\n", &List.first/1)
        end)

      plan
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

    defp column_type(table, column) do
      %{rows: [[data_type]]} =
        TestRepo.query!(
          """
          SELECT data_type FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
          """,
          [table, column]
        )

      data_type
    end

    defp indexes(table) do
      %{rows: rows} =
        TestRepo.query!(
          "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = $1",
          [table]
        )

      Map.new(rows, fn [name, def] -> {name, def} end)
    end
  end
end
