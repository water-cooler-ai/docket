if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.LifecycleStorageTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres

    alias Docket.Postgres.{EventStore, GraphStore, RunCodec, RunStore, Storage}
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

    defmodule RealBackend do
      def storage, do: Docket.Postgres.Storage
      def runs, do: Docket.Postgres.RunStore
      def events, do: Docket.Postgres.EventStore
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

    test "real lifecycle commit atomically advances the fenced run and assigned events" do
      {graph_id, graph_hash, _document} = publish_graph!("commit-graph")
      initial = initialization_moment("commit-run", graph_id, graph_hash)
      backend = {RealBackend, %{repo: TestRepo}}

      assert {:ok, ^initial} = Docket.Lifecycle.start(backend, :tenantless, initial)

      assert {:ok, %{leases: [lease], poisoned: []}} =
               RunStore.claim_due(TestRepo, :system, %{
                 now: @now,
                 limit: 1,
                 orphan_ttl_ms: 60_000,
                 max_claim_attempts: 3
               })

      later = DateTime.add(@now, 1, :second)

      next =
        Moment.propose(
          initial.run,
          :step_committed,
          [Moment.event_entry(:node_completed, 1, node_id: "node")],
          :continue,
          later
        )

      assert {:ok, ^next} =
               Docket.Lifecycle.commit_moment(
                 backend,
                 :tenantless,
                 next,
                 initial.run.checkpoint_seq,
                 lease.claim_token
               )

      expected_run = next.run
      assert {:ok, ^expected_run} = RunStore.fetch_run(TestRepo, :tenantless, next.run.id)

      assert %{claim_attempts: 0, wake_at: nil, claimed_at: later_claimed} =
               RunStore.inspect_run(TestRepo, :tenantless, next.run.id) |> elem(1)

      assert %DateTime{} = later_claimed
      assert TestRepo.aggregate(Event, :count) == 4

      assert Enum.map(TestRepo.all(from(event in Event, order_by: event.seq)), & &1.seq) == [
               1,
               2,
               3,
               4
             ]

      assert :ok = EventStore.append_events(TestRepo, :tenantless, next.run.id, next.events)
      assert TestRepo.aggregate(Event, :count) == 4

      [event | _] = next.events
      conflicting = %{event | payload: %{"different" => true}}

      assert {:error, :event_conflict} =
               EventStore.append_events(TestRepo, :tenantless, next.run.id, [conflicting])

      assert {:error, :stale_fence} =
               Docket.Lifecycle.commit_moment(
                 backend,
                 :tenantless,
                 next,
                 initial.run.checkpoint_seq,
                 lease.claim_token
               )

      assert TestRepo.aggregate(Event, :count) == 4
    end

    test "event failure rolls a successful advance back to its prior claim and run" do
      {graph_id, graph_hash, _document} = publish_graph!("advance-rollback-graph")
      initial = initialization_moment("advance-rollback-run", graph_id, graph_hash)
      noop_backend = {NoopBackend, %{repo: TestRepo}}

      assert {:ok, ^initial} = Docket.Lifecycle.start(noop_backend, :tenantless, initial)

      assert {:ok, %{leases: [lease]}} =
               RunStore.claim_due(TestRepo, :system, %{
                 now: @now,
                 limit: 1,
                 orphan_ttl_ms: 60_000,
                 max_claim_attempts: 3
               })

      next =
        Moment.propose(
          initial.run,
          :step_committed,
          [],
          :continue,
          DateTime.add(@now, 1, :second)
        )

      assert {:error, :injected_event_failure} =
               Docket.Lifecycle.commit_moment(
                 {FailingBackend, %{repo: TestRepo}},
                 :tenantless,
                 next,
                 initial.run.checkpoint_seq,
                 lease.claim_token
               )

      initial_run = initial.run
      assert {:ok, ^initial_run} = RunStore.fetch_run(TestRepo, :tenantless, initial.run.id)

      assert %{claim_attempts: 1, wake_at: nil} =
               RunStore.inspect_run(TestRepo, :tenantless, initial.run.id) |> elem(1)

      assert TestRepo.aggregate(Event, :count) == 0
    end

    test "serialized cancellation commits its events, revokes a claim, and clears poison" do
      {graph_id, graph_hash, _document} = publish_graph!("signal-graph")
      initial = initialization_moment("signal-run", graph_id, graph_hash)
      backend = {RealBackend, %{repo: TestRepo}}

      assert {:ok, ^initial} = Docket.Lifecycle.start(backend, :tenantless, initial)

      TestRepo.update_all(
        from(run in Run, where: run.run_id == ^initial.run.id),
        set: [
          wake_at: nil,
          claim_attempts: 3,
          poisoned_at: @now,
          poison_reason: "max_claim_attempts_exceeded"
        ]
      )

      cancelled_at = DateTime.add(@now, 1, :second)

      assert {:ok, %Moment{run: cancelled}} =
               Docket.Lifecycle.signal(backend, :tenantless, initial.run.id, fn run ->
                 Docket.Runtime.RunMutation.cancel_run(run, cancelled_at)
               end)

      assert cancelled.status == :cancelled
      assert cancelled.checkpoint_seq == initial.run.checkpoint_seq + 1

      assert {:ok,
              %Docket.RunInfo{
                run: ^cancelled,
                wake_at: nil,
                claimed_at: nil,
                claim_attempts: 0,
                poisoned_at: nil,
                poison_reason: nil
              }} = RunStore.inspect_run(TestRepo, :tenantless, initial.run.id)

      assert Enum.map(TestRepo.all(from(event in Event, order_by: event.seq)), & &1.type) == [
               :run_initialized,
               :checkpoint_committed,
               :run_cancelled,
               :checkpoint_committed
             ]
    end

    test "event failure rolls back the entire serialized signal mutation" do
      {graph_id, graph_hash, _document} = publish_graph!("signal-rollback-graph")
      initial = initialization_moment("signal-rollback-run", graph_id, graph_hash)
      noop_backend = {NoopBackend, %{repo: TestRepo}}

      assert {:ok, ^initial} = Docket.Lifecycle.start(noop_backend, :tenantless, initial)

      lease =
        RunStore.claim_due(TestRepo, :system, %{
          now: @now,
          limit: 1,
          orphan_ttl_ms: 60_000,
          max_claim_attempts: 3
        })
        |> then(fn {:ok, %{leases: [lease]}} -> lease end)

      before = TestRepo.one!(from(run in Run, where: run.run_id == ^initial.run.id))

      assert {:error, :injected_event_failure} =
               Docket.Lifecycle.signal(
                 {FailingBackend, %{repo: TestRepo}},
                 :tenantless,
                 initial.run.id,
                 fn run ->
                   Docket.Runtime.RunMutation.cancel_run(run, DateTime.add(@now, 1, :second))
                 end
               )

      assert TestRepo.one!(from(run in Run, where: run.run_id == ^initial.run.id)) == before
      current_claim = RunStore.current_claim(initial.run.id, lease.claim_token)
      assert TestRepo.exists?(from(run in Run, where: ^current_claim))
      assert TestRepo.aggregate(Event, :count) == 0
    end

    test "a committed signal makes previously claimed advance work lose its fence" do
      {graph_id, graph_hash, _document} = publish_graph!("signal-fence-graph")
      initial = initialization_moment("signal-fence-run", graph_id, graph_hash)
      backend = {RealBackend, %{repo: TestRepo}}

      assert {:ok, ^initial} = Docket.Lifecycle.start(backend, :tenantless, initial)

      assert {:ok, %{leases: [lease]}} =
               RunStore.claim_due(TestRepo, :system, %{
                 now: @now,
                 limit: 1,
                 orphan_ttl_ms: 60_000,
                 max_claim_attempts: 3
               })

      advance =
        Moment.propose(
          initial.run,
          :step_committed,
          [],
          :continue,
          DateTime.add(@now, 1, :second)
        )

      assert {:ok, %Moment{run: %{status: :cancelled} = cancelled}} =
               Docket.Lifecycle.signal(backend, :tenantless, initial.run.id, fn run ->
                 Docket.Runtime.RunMutation.cancel_run(run, DateTime.add(@now, 2, :second))
               end)

      handler_id = {__MODULE__, self(), :fence_loser}

      :ok =
        :telemetry.attach(
          handler_id,
          [:docket, :checkpoint, :committed],
          fn name, measurements, metadata, pid ->
            send(pid, {:committed_telemetry, name, measurements, metadata})
          end,
          self()
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert {:error, :stale_fence} =
               Docket.Lifecycle.commit_moment(
                 backend,
                 :tenantless,
                 advance,
                 initial.run.checkpoint_seq,
                 lease.claim_token
               )

      refute_receive {:committed_telemetry, _, _, _}
      assert {:ok, ^cancelled} = RunStore.fetch_run(TestRepo, :tenantless, initial.run.id)
      assert TestRepo.aggregate(Event, :count) == 4
    end

    test "interrupt resolution atomically persists events and wakes a poisoned run" do
      authored = Docket.Test.Fixtures.Graphs.interrupt_review()
      {:ok, document, rtg} = Docket.Graph.Compiler.compile_for_publication(authored)
      {:ok, waiting, _checkpoints} = Docket.Test.run_inline(authored, %{})
      [interrupt_id] = Map.keys(waiting.interrupts)
      running = %{waiting | id: "resolve-poisoned", status: :running}

      publish_document!(document, rtg)

      insert_persisted_run!(running, :interrupt_requested,
        wake_at: nil,
        claim_attempts: 3,
        poisoned_at: @now,
        poison_reason: "max_claim_attempts_exceeded"
      )

      before = TestRepo.one!(from(run in Run, where: run.run_id == ^running.id))

      assert {:error, %Docket.Error{type: :invalid_input}} =
               Docket.Lifecycle.signal(
                 {RealBackend, %{repo: TestRepo}},
                 :tenantless,
                 running.id,
                 &Docket.Runtime.RunMutation.resolve_interrupt(
                   rtg,
                   &1,
                   interrupt_id,
                   42,
                   DateTime.add(@now, 1, :second)
                 )
               )

      assert TestRepo.one!(from(run in Run, where: run.run_id == ^running.id)) == before
      assert TestRepo.aggregate(Event, :count) == 0

      assert {:ok, %Moment{run: resolved}} =
               Docket.Lifecycle.signal(
                 {RealBackend, %{repo: TestRepo}},
                 :tenantless,
                 running.id,
                 &Docket.Runtime.RunMutation.resolve_interrupt(
                   rtg,
                   &1,
                   interrupt_id,
                   "approved",
                   DateTime.add(@now, 2, :second)
                 )
               )

      assert resolved.status == :running
      assert resolved.interrupts[interrupt_id].status == :resolved

      assert {:ok,
              %Docket.RunInfo{
                run: ^resolved,
                wake_at: %DateTime{},
                claimed_at: nil,
                claim_attempts: 0,
                poisoned_at: nil,
                poison_reason: nil
              }} = RunStore.inspect_run(TestRepo, :tenantless, running.id)

      assert Enum.map(TestRepo.all(from(event in Event, order_by: event.seq)), & &1.type) == [
               :interrupt_resolved,
               :channel_updated,
               :checkpoint_committed
             ]

      assert {:error, %Docket.Error{type: :already_resolved}} =
               Docket.Lifecycle.signal(
                 {RealBackend, %{repo: TestRepo}},
                 :tenantless,
                 running.id,
                 &Docket.Runtime.RunMutation.resolve_interrupt(
                   rtg,
                   &1,
                   interrupt_id,
                   "approved",
                   DateTime.add(@now, 3, :second)
                 )
               )

      assert TestRepo.aggregate(Event, :count) == 3
    end

    test "persisted terminal runs reject resolution before interrupt lookup" do
      authored = Docket.Test.Fixtures.Graphs.interrupt_review()
      {:ok, document, rtg} = Docket.Graph.Compiler.compile_for_publication(authored)
      {:ok, waiting, _checkpoints} = Docket.Test.run_inline(authored, %{})
      [interrupt_id] = Map.keys(waiting.interrupts)
      running = %{waiting | id: "terminal-resolve", status: :running}
      {:ok, cancelled_moment} = Docket.Runtime.RunMutation.cancel_run(running, @now)
      cancelled = cancelled_moment.run

      publish_document!(document, rtg)
      insert_persisted_run!(cancelled, :run_cancelled, wake_at: nil)
      before = TestRepo.one!(from(run in Run, where: run.run_id == ^cancelled.id))

      assert {:error, %Docket.Error{type: :inactive_run}} =
               Docket.Lifecycle.signal(
                 {RealBackend, %{repo: TestRepo}},
                 :tenantless,
                 cancelled.id,
                 &Docket.Runtime.RunMutation.resolve_interrupt(
                   rtg,
                   &1,
                   interrupt_id,
                   "approved",
                   DateTime.add(@now, 1, :second)
                 )
               )

      assert TestRepo.one!(from(run in Run, where: run.run_id == ^cancelled.id)) == before
      assert TestRepo.aggregate(Event, :count) == 0
    end

    test "concurrent cancellations serialize to one committed checkpoint" do
      {graph_id, graph_hash, _document} = publish_graph!("concurrent-cancel-graph")
      initial = initialization_moment("concurrent-cancel-run", graph_id, graph_hash)
      backend = {RealBackend, %{repo: TestRepo}}
      parent = self()

      assert {:ok, ^initial} = Docket.Lifecycle.start(backend, :tenantless, initial)

      first =
        Task.async(fn ->
          Docket.Lifecycle.signal(backend, :tenantless, initial.run.id, fn run ->
            send(parent, :first_cancel_locked)

            receive do
              :commit_first_cancel ->
                Docket.Runtime.RunMutation.cancel_run(run, DateTime.add(@now, 1, :second))
            end
          end)
        end)

      assert_receive :first_cancel_locked

      second =
        Task.async(fn ->
          Docket.Lifecycle.signal(backend, :tenantless, initial.run.id, fn run ->
            Docket.Runtime.RunMutation.cancel_run(run, DateTime.add(@now, 2, :second))
          end)
        end)

      refute Task.yield(second, 50)
      send(first.pid, :commit_first_cancel)

      assert {:ok, %Moment{run: cancelled}} = Task.await(first, 1_000)
      assert {:ok, ^cancelled} = Task.await(second, 1_000)
      assert cancelled.status == :cancelled
      assert cancelled.checkpoint_seq == initial.run.checkpoint_seq + 1

      assert Enum.map(TestRepo.all(from(event in Event, order_by: event.seq)), & &1.type) == [
               :run_initialized,
               :checkpoint_committed,
               :run_cancelled,
               :checkpoint_committed
             ]
    end

    test "an advance that commits first is observed by later cancellation" do
      {graph_id, graph_hash, _document} = publish_graph!("advance-first-graph")
      initial = initialization_moment("advance-first-run", graph_id, graph_hash)
      backend = {RealBackend, %{repo: TestRepo}}

      assert {:ok, ^initial} = Docket.Lifecycle.start(backend, :tenantless, initial)

      assert {:ok, %{leases: [lease]}} =
               RunStore.claim_due(TestRepo, :system, %{
                 now: @now,
                 limit: 1,
                 orphan_ttl_ms: 60_000,
                 max_claim_attempts: 3
               })

      advanced =
        Moment.propose(
          initial.run,
          :step_committed,
          [],
          :continue,
          DateTime.add(@now, 1, :second)
        )

      assert {:ok, ^advanced} =
               Docket.Lifecycle.commit_moment(
                 backend,
                 :tenantless,
                 advanced,
                 initial.run.checkpoint_seq,
                 lease.claim_token
               )

      assert {:ok, %Moment{run: cancelled}} =
               Docket.Lifecycle.signal(backend, :tenantless, initial.run.id, fn run ->
                 assert run.checkpoint_seq == advanced.run.checkpoint_seq
                 Docket.Runtime.RunMutation.cancel_run(run, DateTime.add(@now, 2, :second))
               end)

      assert cancelled.status == :cancelled
      assert cancelled.checkpoint_seq == advanced.run.checkpoint_seq + 1
      assert {:ok, ^cancelled} = RunStore.fetch_run(TestRepo, :tenantless, initial.run.id)
    end

    defp publish_graph!(graph_id) do
      authored = Docket.Graph.new!(id: graph_id)
      {:ok, graph, runtime_graph} = Docket.Graph.Compiler.compile_for_publication(authored)
      graph_hash = runtime_graph.graph_hash

      assert {:ok, :published} =
               Storage.transaction(TestRepo, fn ctx ->
                 with :ok <- GraphStore.save_graph(ctx, graph_id, graph_hash, graph) do
                   {:ok, :published}
                 end
               end)

      {graph_id, graph_hash, graph}
    end

    defp publish_document!(document, rtg) do
      assert {:ok, :published} =
               Storage.transaction(TestRepo, fn ctx ->
                 with :ok <- GraphStore.save_graph(ctx, document.id, rtg.graph_hash, document) do
                   {:ok, :published}
                 end
               end)
    end

    defp insert_persisted_run!(run, checkpoint_type, overrides) do
      {:ok, attrs} = RunCodec.dump(run)

      attrs
      |> Map.merge(%{
        tenant_id: nil,
        latest_checkpoint_type: checkpoint_type,
        claim_token: nil,
        claimed_at: nil,
        wake_at: nil,
        claim_attempts: 0,
        poisoned_at: nil,
        poison_reason: nil
      })
      |> Map.merge(Map.new(overrides))
      |> Run.changeset()
      |> TestRepo.insert!()
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
