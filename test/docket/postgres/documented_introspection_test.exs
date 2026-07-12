if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.DocumentedIntrospectionTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.TestRepo

    @migration_version 20_260_712_000_027
    @queries_path Path.expand("../../../docs/postgres-introspection.sql", __DIR__)

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

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      :ok
    end

    test "every documented revision-8 introspection query executes" do
      queries = documented_queries()

      assert Map.keys(queries) |> Enum.sort() ==
               ~w(
                 expired_claims
                 fresh_in_flight_claims
                 graph_references
                 invalid_unscheduled_rows
                 oldest_wake
                 poisoned_runs
                 ready_backlog
                 retained_terminal_failures
                 retention_candidates
               )

      Enum.each(queries, fn {name, sql} ->
        assert %Postgrex.Result{} = TestRepo.query!(sql, [], log: false), name
      end)
    end

    test "documented queries classify revision-8 operational fixtures" do
      TestRepo.transaction(fn ->
        seed_operational_fixtures!()
        queries = documented_queries()

        assert run_ids(query!(queries, "ready_backlog")) == ["ready"]
        assert run_ids(query!(queries, "expired_claims")) == ["expired"]

        assert run_ids(query!(queries, "fresh_in_flight_claims")) ==
                 ["fresh-boundary"]

        assert run_ids(query!(queries, "poisoned_runs")) == ["poisoned"]
        assert %{rows: [[%DateTime{}]]} = query!(queries, "oldest_wake")
        assert %{rows: []} = query!(queries, "invalid_unscheduled_rows")

        graph_refs = query!(queries, "graph_references")
        assert Enum.sum(Enum.map(graph_refs.rows, &List.last/1)) == 7

        assert run_ids(query!(queries, "retained_terminal_failures")) == ["failed-old"]
        assert %{rows: [[1, 1]]} = query!(queries, "retention_candidates")
      end)
    end

    test "documented search-path mechanism works for a prefixed install" do
      TestRepo.query!("CREATE SCHEMA docket_private", [], log: false)

      :ok =
        Ecto.Migrator.up(
          TestRepo,
          @migration_version + 1,
          InstallDocketPrefixed,
          log: false
        )

      TestRepo.transaction(fn ->
        TestRepo.query!("SET LOCAL search_path TO docket_private, public", [], log: false)

        Enum.each(documented_queries(), fn {name, sql} ->
          assert %Postgrex.Result{} = TestRepo.query!(sql, [], log: false), name
        end)
      end)
    end

    test "invalid-row query detects a tuple rejected by revision-8 constraints" do
      assert {:error, :verified} =
               TestRepo.transaction(fn ->
                 seed_graph!()

                 TestRepo.query!(
                   """
                   ALTER TABLE docket_runs
                   DROP CONSTRAINT docket_runs_waiting_terminal_idle_check
                   """,
                   [],
                   log: false
                 )

                 TestRepo.query!(
                   """
                   INSERT INTO docket_runs (
                     run_id, graph_id, graph_hash, status, state, wake_at,
                     inserted_at, started_at, updated_at
                   ) VALUES (
                     'invalid-waiting-wake', 'graph', 'hash', 'waiting',
                     decode('00', 'hex'), CURRENT_TIMESTAMP,
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                   )
                   """,
                   [],
                   log: false
                 )

                 assert run_ids(query!(documented_queries(), "invalid_unscheduled_rows")) == [
                          "invalid-waiting-wake"
                        ]

                 TestRepo.rollback(:verified)
               end)
    end

    defp documented_queries do
      @queries_path
      |> File.read!()
      |> String.split(~r/^-- name: /m, trim: true)
      |> Enum.drop(1)
      |> Map.new(fn section ->
        [name | sql] = String.split(section, "\n")
        {String.trim(name), sql |> Enum.join("\n") |> String.trim()}
      end)
    end

    defp query!(queries, name), do: TestRepo.query!(Map.fetch!(queries, name), [], log: false)

    defp run_ids(%Postgrex.Result{columns: columns, rows: rows}) do
      index = Enum.find_index(columns, &(&1 == "run_id"))
      Enum.map(rows, &Enum.at(&1, index))
    end

    defp seed_operational_fixtures! do
      seed_graph!()

      TestRepo.query!(
        """
        INSERT INTO docket_runs (
          run_id, graph_id, graph_hash, status, state, checkpoint_seq,
          claim_token, claimed_at, wake_at, claim_attempts,
          poisoned_at, poison_reason, inserted_at, started_at, updated_at, finished_at
        ) VALUES
          ('ready', 'graph', 'hash', 'running', decode('00', 'hex'), 1,
           NULL, NULL, CURRENT_TIMESTAMP - INTERVAL '1 second', 0,
           NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL),
          ('scheduled', 'graph', 'hash', 'running', decode('00', 'hex'), 1,
           NULL, NULL, CURRENT_TIMESTAMP + INTERVAL '1 hour', 0,
           NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL),
          ('fresh-boundary', 'graph', 'hash', 'running', decode('00', 'hex'), 1,
           gen_random_uuid(), CURRENT_TIMESTAMP - INTERVAL '60 seconds', NULL, 1,
           NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL),
          ('expired', 'graph', 'hash', 'running', decode('00', 'hex'), 1,
           gen_random_uuid(), CURRENT_TIMESTAMP - INTERVAL '61 seconds', NULL, 1,
           NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL),
          ('poisoned', 'graph', 'hash', 'running', decode('00', 'hex'), 1,
           NULL, NULL, NULL, 5,
           CURRENT_TIMESTAMP, 'max_claim_attempts_exceeded',
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL),
          ('failed-old', 'graph', 'hash', 'failed', decode('00', 'hex'), 3,
           NULL, NULL, NULL, 0,
           NULL, NULL, CURRENT_TIMESTAMP - INTERVAL '100 days',
           CURRENT_TIMESTAMP - INTERVAL '100 days', CURRENT_TIMESTAMP - INTERVAL '100 days',
           CURRENT_TIMESTAMP - INTERVAL '100 days'),
          ('done-recent', 'graph', 'hash', 'done', decode('00', 'hex'), 2,
           NULL, NULL, NULL, 0,
           NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [],
        log: false
      )

      TestRepo.query!(
        """
        INSERT INTO docket_events (
          run_id, seq, type, step, payload, metadata, occurred_at, inserted_at
        ) VALUES (
          'failed-old', 1, 'run_failed', 1, decode('00', 'hex'), decode('00', 'hex'),
          CURRENT_TIMESTAMP - INTERVAL '31 days', CURRENT_TIMESTAMP - INTERVAL '31 days'
        )
        """,
        [],
        log: false
      )
    end

    defp seed_graph! do
      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions (graph_id, graph_hash, graph, inserted_at)
        VALUES ('graph', 'hash', decode('00', 'hex'), CURRENT_TIMESTAMP)
        """,
        [],
        log: false
      )
    end
  end
end
