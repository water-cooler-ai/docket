if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.LifecycleStorageTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.{GraphStore, RunStore, Storage}
    alias Docket.Postgres.LifecycleStorageTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.{Event, GraphVersion, Run}
    alias Docket.Runtime.Moment

    @migration_version 20_260_710_000_023
    @now ~U[2026-07-10 12:00:00.123456Z]

    defmodule InstallDocket do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule FailingEvents do
      alias Docket.Postgres.Storage
      alias Docket.Postgres.Schemas.Event

      def append_events(ctx, :tenantless, run_id, [event | _rest]) do
        {repo, prefix} = Storage.context!(ctx)

        event
        |> event_attrs(run_id)
        |> Event.changeset()
        |> repo.insert!(prefix: prefix)

        {:error, :injected_event_failure}
      end

      defp event_attrs(event, run_id) do
        %{
          run_id: run_id,
          seq: event.seq,
          type: event.type,
          step: event.step,
          node_id: event.node_id,
          channel_id: event.channel_id,
          task_id: event.task_id,
          payload: :erlang.term_to_binary(event.payload, [:deterministic]),
          metadata: :erlang.term_to_binary(event.metadata, [:deterministic]),
          occurred_at: event.timestamp
        }
      end
    end

    defmodule NoopEvents do
      def append_events(_ctx, :tenantless, _run_id, _events), do: :ok
    end

    defmodule FailingBackend do
      def storage, do: Docket.Postgres.Storage
      def runs, do: Docket.Postgres.RunStore
      def events, do: Docket.Postgres.LifecycleStorageTest.FailingEvents
    end

    defmodule NoopBackend do
      def storage, do: Docket.Postgres.Storage
      def runs, do: Docket.Postgres.RunStore
      def events, do: Docket.Postgres.LifecycleStorageTest.NoopEvents
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

    test "a later Events-capability failure rolls back run, wake, and partial event only" do
      {graph_id, graph_hash, document} = publish_graph!("rollback-graph")
      moment = initialization_moment("rollback-run", graph_id, graph_hash)
      backend = {FailingBackend, %{repo: TestRepo}}

      assert {:error, :injected_event_failure} =
               Docket.Lifecycle.start(backend, :tenantless, moment)

      assert {:error, :not_found} = RunStore.fetch_run(TestRepo, :system, moment.run.id)
      assert TestRepo.aggregate(Run, :count) == 0
      assert TestRepo.aggregate(Event, :count) == 0
      assert {:ok, ^document} = GraphStore.fetch_graph(TestRepo, graph_id, graph_hash)
      assert TestRepo.aggregate(GraphVersion, :count) == 1
    end

    test "an already-filtered/no-op Events capability commits run and wake without an event row" do
      {graph_id, graph_hash, _document} = publish_graph!("no-events-graph")
      moment = initialization_moment("no-events-run", graph_id, graph_hash)
      expected_run = moment.run
      backend = {NoopBackend, %{repo: TestRepo}}

      assert {:ok, ^moment} = Docket.Lifecycle.start(backend, :tenantless, moment)
      assert {:ok, ^expected_run} = RunStore.fetch_run(TestRepo, :tenantless, moment.run.id)

      assert {:ok, %Docket.RunInfo{run: run, wake_at: @now}} =
               RunStore.inspect_run(TestRepo, :tenantless, moment.run.id)

      assert run == moment.run
      assert TestRepo.aggregate(Event, :count) == 0
    end

    defp publish_graph!(graph_id) do
      graph = Docket.Graph.new!(id: graph_id)
      graph_hash = Docket.Graph.hash(graph)

      assert {:ok, :published} =
               Storage.transaction(TestRepo, fn ctx ->
                 with :ok <- GraphStore.save_graph(ctx, graph_id, graph_hash, graph) do
                   {:ok, :published}
                 end
               end)

      {graph_id, graph_hash, graph}
    end

    defp initialization_moment(run_id, graph_id, graph_hash) do
      run = %Docket.Run{
        id: run_id,
        graph_id: graph_id,
        graph_hash: graph_hash,
        status: :running,
        input: %{},
        started_at: @now,
        updated_at: @now
      }

      Moment.propose(
        run,
        :run_initialized,
        [Moment.event_entry(:run_initialized, 0)],
        :continue,
        @now
      )
    end
  end
end
