if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.EventStoreTest do
    use ExUnit.Case, async: false

    import Ecto.Query

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
                 event(run, 3),
                 %{event | payload: %{"other" => true}}
               ])

      assert TestRepo.aggregate(Event, :count) == 1
      refute TestRepo.exists?(from(row in Event, where: row.seq == 3))
    end

    test "batches new and replayed events and verifies every assigned sequence", %{run: run} do
      first = event(run, 2)
      second = %{event(run, 3) | type: :node_completed, node_id: "node"}
      third = event(run, 4)

      assert :ok = EventStore.append_events(TestRepo, :system, run.id, [first])
      assert :ok = EventStore.append_events(TestRepo, :system, run.id, [first, second, third])
      assert TestRepo.aggregate(Event, :count) == 3

      assert {:error, :event_conflict} =
               EventStore.append_events(TestRepo, :system, run.id, [
                 second,
                 %{second | payload: %{"different" => true}}
               ])

      assert TestRepo.aggregate(Event, :count) == 3
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

      assert {:error, :invalid_events} =
               EventStore.append_events(TestRepo, :system, run.id, [
                 %{event(run, 2) | payload: nil}
               ])

      assert {:error, :invalid_events} =
               EventStore.append_events(TestRepo, :system, run.id, [
                 %{event(run, 2) | metadata: %{"pid" => self()}}
               ])

      assert TestRepo.aggregate(Event, :count) == 0
    end

    test "enforces tenant ownership and validates empty appends without lookup", %{run: run} do
      assert {:error, :not_found} =
               EventStore.append_events(TestRepo, :tenantless, run.id, [event(run, 2)])

      assert {:error, :not_found} =
               EventStore.append_events(TestRepo, {:tenant, "t2"}, run.id, [event(run, 2)])

      assert :ok = EventStore.append_events(TestRepo, {:tenant, "t2"}, "missing", [])
    end

    describe "list_events pages retained events with retention-aware bounds" do
      test "enforces tenant ownership through the owning run", %{run: run} do
        append(run, [2, 3])
        opts = %{after_seq: 0, limit: 10}

        assert {:ok, %Docket.EventPage{}} =
                 EventStore.list_events(TestRepo, {:tenant, "t1"}, run.id, opts)

        assert {:ok, %Docket.EventPage{}} =
                 EventStore.list_events(TestRepo, :system, run.id, opts)

        assert {:error, :not_found} =
                 EventStore.list_events(TestRepo, {:tenant, "t2"}, run.id, opts)

        assert {:error, :not_found} =
                 EventStore.list_events(TestRepo, :tenantless, run.id, opts)

        assert {:error, :not_found} =
                 EventStore.list_events(TestRepo, :system, "missing", opts)
      end

      test "returns an empty page beyond the latest sequence", %{run: run} do
        run = reinsert(run, event_seq: 3)
        append(run, [2, 3])

        assert {:ok, page} =
                 EventStore.list_events(TestRepo, :system, run.id, %{after_seq: 3, limit: 10})

        assert page.events == []
        assert page.next_after_seq == 3
        refute page.has_more?
        assert page.oldest_available_seq == 2
        assert page.latest_available_seq == 3
        assert page.latest_seq == 3
      end

      test "honors default and boundary limits", %{run: run} do
        append(run, [2, 3, 4])

        assert {:ok, full} =
                 EventStore.list_events(TestRepo, :system, run.id, %{after_seq: 0, limit: 250})

        assert Enum.map(full.events, & &1.seq) == [2, 3, 4]
        refute full.has_more?

        assert {:ok, one} =
                 EventStore.list_events(TestRepo, :system, run.id, %{after_seq: 0, limit: 1})

        assert Enum.map(one.events, & &1.seq) == [2]
        assert one.next_after_seq == 2
        assert one.has_more?
      end

      test "paginates across pages using next_after_seq", %{run: run} do
        append(run, [1, 2, 3, 4, 5])

        assert {:ok, first} =
                 EventStore.list_events(TestRepo, :system, run.id, %{after_seq: 0, limit: 2})

        assert Enum.map(first.events, & &1.seq) == [1, 2]
        assert first.next_after_seq == 2
        assert first.has_more?

        assert {:ok, second} =
                 EventStore.list_events(TestRepo, :system, run.id, %{
                   after_seq: first.next_after_seq,
                   limit: 2
                 })

        assert Enum.map(second.events, & &1.seq) == [3, 4]
        assert second.has_more?

        assert {:ok, third} =
                 EventStore.list_events(TestRepo, :system, run.id, %{
                   after_seq: second.next_after_seq,
                   limit: 2
                 })

        assert Enum.map(third.events, & &1.seq) == [5]
        refute third.has_more?
      end

      test "tolerates ordinary sequence gaps", %{run: run} do
        run = reinsert(run, event_seq: 5)
        append(run, [1, 2, 5])

        assert {:ok, page} =
                 EventStore.list_events(TestRepo, :system, run.id, %{after_seq: 0, limit: 10})

        assert Enum.map(page.events, & &1.seq) == [1, 2, 5]
        assert page.oldest_available_seq == 1
        assert page.latest_available_seq == 5
        assert page.next_after_seq == 5
        refute page.has_more?
      end

      test "reflects retention pruning of low sequences", %{run: run} do
        run = reinsert(run, event_seq: 4)
        append(run, [1, 2, 3, 4])

        TestRepo.delete_all(from(e in Event, where: e.seq < 3))

        assert {:ok, page} =
                 EventStore.list_events(TestRepo, :system, run.id, %{after_seq: 0, limit: 10})

        assert Enum.map(page.events, & &1.seq) == [3, 4]
        assert page.oldest_available_seq == 3
        assert page.latest_available_seq == 4
        assert page.latest_seq == 4
      end

      test "keeps latest_seq after a fully pruned history", %{run: run} do
        run = reinsert(run, event_seq: 4)
        append(run, [1, 2, 3, 4])

        TestRepo.delete_all(Event)

        assert {:ok, page} =
                 EventStore.list_events(TestRepo, :system, run.id, %{after_seq: 0, limit: 10})

        assert page.events == []
        assert page.oldest_available_seq == nil
        assert page.latest_available_seq == nil
        assert page.next_after_seq == 0
        refute page.has_more?
        assert page.latest_seq == 4
      end

      test "reports a typed error for an undecodable row", %{run: run} do
        append(run, [2, 3])
        TestRepo.update_all(from(e in Event, where: e.seq == 3), set: [payload: <<0, 1, 2>>])

        assert {:error, %Docket.Error{type: :corrupt_event_row, details: %{seq: 3}}} =
                 EventStore.list_events(TestRepo, :system, run.id, %{after_seq: 0, limit: 10})
      end

      test "keeps per-call bounds coherent as appends interleave pagination", %{run: run} do
        run = reinsert(run, event_seq: 5)
        append(run, [1, 2, 3])

        assert {:ok, first} =
                 EventStore.list_events(TestRepo, :system, run.id, %{after_seq: 0, limit: 2})

        assert Enum.map(first.events, & &1.seq) == [1, 2]
        assert first.latest_available_seq == 3
        assert first.has_more?

        append(run, [4, 5])

        assert {:ok, second} =
                 EventStore.list_events(TestRepo, :system, run.id, %{
                   after_seq: first.next_after_seq,
                   limit: 2
                 })

        assert Enum.map(second.events, & &1.seq) == [3, 4]
        assert second.latest_available_seq == 5
        assert second.has_more?
      end
    end

    defp append(run, seqs) do
      events = Enum.map(seqs, &event(run, &1))
      assert :ok = EventStore.append_events(TestRepo, :system, run.id, events)
    end

    defp reinsert(run, opts) do
      TestRepo.delete_all(Event)
      TestRepo.delete_all(Run)
      updated = %{run | event_seq: Keyword.fetch!(opts, :event_seq)}

      assert {:ok, ^updated} =
               RunStore.insert_run(
                 TestRepo,
                 {:tenant, "t1"},
                 updated,
                 :run_initialized,
                 db_time(@now)
               )

      updated
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
