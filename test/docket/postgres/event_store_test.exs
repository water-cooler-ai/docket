if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.EventStoreTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.{EventStore, GraphStore, RunStore}
    alias Docket.Postgres.EventStoreTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.{Event, GraphVersion, Run}

    @migration_version 20_260_710_000_024
    @now ~U[2026-07-10 12:00:00Z]

    defmodule InstallDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    setup_all do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      :ok
    end

    setup do
      TestRepo.delete_all(Event)
      TestRepo.delete_all(Run)
      TestRepo.delete_all(GraphVersion)

      {:ok, graph, runtime} =
        Docket.Graph.Compiler.compile_for_publication(Docket.Graph.new!(id: "graph"))

      :ok = GraphStore.save_graph(TestRepo, "graph", runtime.graph_hash, graph)

      run = %Docket.Run{
        id: "run",
        graph_id: "graph",
        graph_hash: runtime.graph_hash,
        status: :running,
        input: %{},
        checkpoint_seq: 1,
        event_seq: 1,
        started_at: db_time(@now),
        updated_at: db_time(@now)
      }

      assert {:ok, ^run} =
               RunStore.insert_run(
                 TestRepo,
                 {:tenant, "t1"},
                 run,
                 :run_initialized,
                 db_time(@now)
               )

      %{run: run}
    end

    test "appends versioned deterministic events idempotently", %{run: run} do
      event = event(run, 2)
      assert :ok = EventStore.append_events(TestRepo, {:tenant, "t1"}, run.id, [event])
      assert :ok = EventStore.append_events(TestRepo, {:tenant, "t1"}, run.id, [event])
      assert TestRepo.aggregate(Event, :count) == 1
      row = TestRepo.one!(Event)
      assert Docket.DurableCodec.decode!(row.payload, :event) == event.payload
      assert Docket.DurableCodec.decode!(row.metadata, :event) == event.metadata
    end

    test "rejects conflicting replay and preserves the winner", %{run: run} do
      event = event(run, 2)
      assert :ok = EventStore.append_events(TestRepo, :system, run.id, [event])

      assert {:error, :event_conflict} =
               EventStore.append_events(TestRepo, :system, run.id, [
                 %{event | payload: %{"other" => true}}
               ])

      assert TestRepo.aggregate(Event, :count) == 1
    end

    test "reports invalid identity fields precisely", %{run: run} do
      assert {:error, :invalid_event_sequence} =
               EventStore.append_events(TestRepo, :system, run.id, [%{event(run, 2) | seq: 0}])

      assert {:error, :invalid_events} =
               EventStore.append_events(TestRepo, :system, run.id, [
                 %{event(run, 2) | timestamp: nil}
               ])

      assert {:error, :event_run_mismatch} =
               EventStore.append_events(TestRepo, :system, run.id, [
                 %{event(run, 2) | run_id: "other"}
               ])
    end

    test "enforces tenant ownership and validates empty appends without lookup", %{run: run} do
      assert {:error, :not_found} =
               EventStore.append_events(TestRepo, :tenantless, run.id, [event(run, 2)])

      assert {:error, :not_found} =
               EventStore.append_events(TestRepo, {:tenant, "t2"}, run.id, [event(run, 2)])

      assert :ok = EventStore.append_events(TestRepo, {:tenant, "t2"}, "missing", [])
    end

    defp event(run, seq),
      do: %Docket.Event{
        run_id: run.id,
        seq: seq,
        type: :checkpoint_committed,
        step: 0,
        timestamp: @now,
        payload: %{"value" => 1},
        metadata: %{"checkpoint_seq" => 1}
      }

    defp db_time(datetime),
      do: datetime |> DateTime.to_unix(:microsecond) |> DateTime.from_unix!(:microsecond)
  end
end
