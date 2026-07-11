if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.PrunerTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres

    alias Docket.Postgres.{GraphStore, Pruner, RunCodec, RunStore}
    alias Docket.Postgres.PrunerTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.{Event, GraphVersion, Run}
    alias Docket.Run.Failure

    @migration_version 20_260_711_000_027
    @now ~U[2026-07-11 12:00:00.000000Z]

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
      :ok
    end

    test "prunes events before retained runs, cascades terminal runs, and cleans only their graph" do
      {old_graph, old_hash} = save_graph!("workflow", %{"version" => 1})
      {current_graph, current_hash} = save_graph!("workflow", %{"version" => 2})

      for version <- 3..11 do
        save_graph!("workflow", %{"version" => version})
      end

      {_published_graph, published_hash} = save_graph!("published", %{})

      old = DateTime.add(@now, -20, :day)
      recent = DateTime.add(@now, -1, :day)
      old_event = DateTime.add(@now, -2, :day)

      insert_run!("running", :running, current_hash, old, wake_at: old)
      insert_run!("waiting", :waiting, current_hash, old)
      insert_run!("done-retained", :done, current_hash, recent)

      failed =
        insert_run!("failed-retained", :failed, current_hash, recent,
          failure: Failure.new("node_failed", "worker failed")
        )

      insert_run!("cancelled-retained", :cancelled, current_hash, recent)
      insert_run!("done-expired", :done, old_hash, old)
      insert_run!("done-expired-shared", :done, current_hash, old)

      insert_event!("running", 1, old_event)
      insert_event!("failed-retained", 1, old_event)
      insert_event!("failed-retained", 2, @now)
      insert_event!("done-expired", 1, @now)

      # Saving an existing historical version is idempotent, not a retention
      # lease. Its final retained run still governs its lifetime.
      :ok = GraphStore.save_graph(TestRepo, "workflow", old_hash, old_graph)

      handler = "pruner-test-#{System.unique_integer([:positive])}"
      parent = self()

      :ok =
        :telemetry.attach(
          handler,
          [:docket, :postgres, :pruner, :pass],
          fn name, measurements, metadata, _ ->
            send(parent, {:telemetry, name, measurements, metadata})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok,
              %{
                events_deleted: 2,
                runs_deleted: 2,
                cascade_events_deleted: 1,
                graphs_deleted: 1
              }} = Pruner.prune(TestRepo, policy())

      assert_receive {:telemetry, [:docket, :postgres, :pruner, :pass], measurements,
                      %{result: :ok}}

      assert measurements.events_deleted == 2
      assert measurements.runs_deleted == 2
      assert measurements.cascade_events_deleted == 1
      assert measurements.graphs_deleted == 1
      assert is_integer(measurements.duration)

      assert run_ids() ==
               ~w(cancelled-retained done-retained failed-retained running waiting)

      assert event_keys() == [{"failed-retained", 2}]
      assert {:ok, ^failed} = RunStore.fetch_run(TestRepo, :system, failed.id)
      assert {:ok, ^current_graph} = GraphStore.fetch_graph(TestRepo, "workflow", current_hash)
      assert {:error, :not_found} = GraphStore.fetch_graph(TestRepo, "workflow", old_hash)

      # The newest ten revisions survive without runs. The separate graph ID
      # has only one revision, so it is protected by the same rule.
      assert {:ok, _graph} = GraphStore.fetch_graph(TestRepo, "published", published_hash)

      assert {:ok,
              %{
                events_deleted: 0,
                runs_deleted: 0,
                cascade_events_deleted: 0,
                graphs_deleted: 0
              }} = Pruner.prune(TestRepo, policy())
    end

    test "batch size bounds selected rows and strict cutoffs retain exact-boundary rows" do
      {_graph, graph_hash} = save_graph!("workflow", %{})
      insert_run!("active", :running, graph_hash, DateTime.add(@now, -30, :day), wake_at: @now)

      cutoff = DateTime.add(@now, -1, :day)

      for seq <- 1..3 do
        insert_event!("active", seq, DateTime.add(cutoff, -seq, :microsecond))
      end

      insert_event!("active", 4, cutoff)

      assert {:ok, %{events_deleted: 2, runs_deleted: 0}} =
               Pruner.prune(TestRepo, policy(batch_size: 2))

      assert {:ok, %{events_deleted: 1, runs_deleted: 0}} =
               Pruner.prune(TestRepo, policy(batch_size: 2))

      assert event_keys() == [{"active", 4}]
    end

    test "advisory lock makes concurrent same-schema passes skip safely" do
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            assert {:ok, %{rows: [[true]]}} =
                     TestRepo.query(
                       "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
                       ["docket:pruner:public"]
                     )

            send(parent, :lock_held)
            receive do: (:release -> :ok)
          end)
        end)

      assert_receive :lock_held
      assert {:skipped, :locked} = Pruner.prune(TestRepo, policy())
      assert {:skipped, :locked} = Pruner.prune(%{repo: TestRepo, prefix: "public"}, policy())
      send(holder.pid, :release)
      assert {:ok, :ok} = Task.await(holder)
    end

    test "skips a run locked by an append, then reports the later cascade exactly" do
      {_graph, graph_hash} = save_graph!("workflow", %{})
      old = DateTime.add(@now, -20, :day)
      insert_run!("expired", :done, graph_hash, old)
      parent = self()

      appender =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            insert_event!("expired", 1, @now)
            send(parent, :event_inserted)
            receive do: (:commit -> :ok)
          end)
        end)

      assert_receive :event_inserted
      prune = Task.async(fn -> Pruner.prune(TestRepo, policy()) end)
      Process.sleep(25)
      send(appender.pid, :commit)
      assert {:ok, :ok} = Task.await(appender)

      assert {:ok, %{runs_deleted: 0, cascade_events_deleted: 0, graphs_deleted: 0}} =
               Task.await(prune)

      assert {:ok, %{runs_deleted: 1, cascade_events_deleted: 1, graphs_deleted: 0}} =
               Pruner.prune(TestRepo, policy())
    end

    test "skips a graph locked by a concurrent run insert without rolling back the pass" do
      {graph, graph_hash} = save_graph!("workflow", %{"version" => 1})

      for version <- 2..11 do
        save_graph!("workflow", %{"version" => version})
      end

      old = DateTime.add(@now, -20, :day)
      insert_run!("expired", :done, graph_hash, old)
      parent = self()

      inserter =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            insert_run!("new-running", :running, graph_hash, @now, wake_at: @now)
            send(parent, :run_inserted)
            receive do: (:commit -> :ok)
          end)
        end)

      assert_receive :run_inserted
      prune = Task.async(fn -> Pruner.prune(TestRepo, policy()) end)
      Process.sleep(25)
      send(inserter.pid, :commit)
      assert {:ok, :ok} = Task.await(inserter)

      assert {:ok, %{runs_deleted: 1, graphs_deleted: 0}} = Task.await(prune)
      assert {:ok, ^graph} = GraphStore.fetch_graph(TestRepo, "workflow", graph_hash)
      assert {:ok, _run} = RunStore.fetch_run(TestRepo, :system, "new-running")
    end

    test "supervised pruner runs immediately and on its recurring tokenized timer" do
      {_graph, graph_hash} = save_graph!("workflow", %{})
      insert_run!("active", :running, graph_hash, DateTime.add(@now, -20, :day), wake_at: @now)
      insert_event!("active", 1, DateTime.add(@now, -2, :day))

      name = Module.concat(__MODULE__, "Instance#{System.unique_integer([:positive])}")

      start_supervised!(
        {Pruner,
         name: name,
         context: TestRepo,
         interval_ms: 25,
         event_retention_ms: :timer.hours(24),
         run_retention_ms: :timer.hours(24 * 10),
         batch_size: 10,
         clock: fn -> @now end}
      )

      assert eventually(fn -> event_keys() == [] end)
      insert_event!("active", 2, DateTime.add(@now, -2, :day))
      assert eventually(fn -> event_keys() == [] end)
    end

    test "rejects destructive or internally inconsistent policies" do
      assert_raise ArgumentError, ~r/event retention must not exceed run retention/, fn ->
        Pruner.prune(TestRepo, policy(event_retention_ms: 2, run_retention_ms: 1))
      end

      for invalid <- [-1, 0.5, nil] do
        assert_raise ArgumentError, fn ->
          Pruner.prune(TestRepo, policy(batch_size: invalid))
        end
      end
    end

    defp policy(overrides \\ []) do
      Map.merge(
        %{
          now: @now,
          event_retention_ms: :timer.hours(24),
          run_retention_ms: :timer.hours(24 * 10),
          batch_size: 100
        },
        Map.new(overrides)
      )
    end

    defp save_graph!(graph_id, metadata) do
      {:ok, graph, runtime} =
        Docket.Graph.Compiler.compile_for_publication(
          Docket.Graph.new!(id: graph_id, metadata: metadata)
        )

      :ok = GraphStore.save_graph(TestRepo, graph_id, runtime.graph_hash, graph)
      {graph, runtime.graph_hash}
    end

    defp insert_run!(run_id, status, graph_hash, updated_at, opts \\ []) do
      terminal? = status in [:done, :failed, :cancelled]

      run = %Docket.Run{
        id: run_id,
        graph_id: "workflow",
        graph_hash: graph_hash,
        status: status,
        input: %{},
        output: if(status == :done, do: %{"ok" => true}),
        failure: Keyword.get(opts, :failure),
        started_at: DateTime.add(updated_at, -1, :second),
        updated_at: updated_at,
        finished_at: if(terminal?, do: updated_at)
      }

      {:ok, attrs} = RunCodec.dump(run)

      attrs =
        Map.merge(attrs, %{
          tenant_id: Keyword.get(opts, :tenant_id),
          latest_checkpoint_type: nil,
          claim_token: nil,
          claimed_at: nil,
          wake_at: Keyword.get(opts, :wake_at),
          claim_attempts: 0,
          claim_abandons: 0,
          poisoned_at: nil,
          poison_reason: nil,
          inserted_at: updated_at
        })

      assert {1, _} = TestRepo.insert_all(Run, [attrs])
      run
    end

    defp insert_event!(run_id, seq, inserted_at) do
      encoded = Docket.DurableCodec.encode!(:event, %{})

      assert {1, _} =
               TestRepo.insert_all(Event, [
                 %{
                   run_id: run_id,
                   seq: seq,
                   type: :checkpoint_committed,
                   step: 0,
                   payload: encoded,
                   metadata: encoded,
                   occurred_at: @now,
                   inserted_at: inserted_at
                 }
               ])
    end

    defp run_ids do
      Run |> order_by([run], run.run_id) |> select([run], run.run_id) |> TestRepo.all()
    end

    defp event_keys do
      Event
      |> order_by([event], [event.run_id, event.seq])
      |> select([event], {event.run_id, event.seq})
      |> TestRepo.all()
    end

    defp eventually(fun, attempts \\ 100)
    defp eventually(fun, 0), do: fun.()

    defp eventually(fun, attempts) do
      if fun.() do
        true
      else
        Process.sleep(10)
        eventually(fun, attempts - 1)
      end
    end
  end
end
