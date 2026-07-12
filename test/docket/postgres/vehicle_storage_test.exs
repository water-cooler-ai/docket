if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.VehicleStorageTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.{Graph, Lifecycle, Reducer, RunInfo, Schema}
    alias Docket.Graph.Compiler
    alias Docket.Postgres.{GraphStore, RunStore, Vehicle}
    alias Docket.Postgres.Schemas.{Event, GraphVersion, Run}
    alias Docket.Postgres.VehicleStorageTestRepo, as: TestRepo
    alias Docket.Runtime.{Loop, RunMutation}
    alias Docket.Test.Fixtures.{Graphs, Nodes}

    @migration_version 20_260_711_000_024
    @now ~U[2026-07-11 12:00:00.000000Z]

    defmodule InstallDocket do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule Backend do
      def storage, do: Docket.Postgres.Storage
      def graphs, do: Docket.Postgres.GraphStore
      def runs, do: Docket.Postgres.RunStore
      def events, do: Docket.Postgres.EventStore
    end

    defmodule FailingEvents do
      def append_events(_ctx, :system, _run_id, _events), do: {:error, :injected_event_failure}
    end

    defmodule FailingEventsBackend do
      def storage, do: Docket.Postgres.Storage
      def graphs, do: Docket.Postgres.GraphStore
      def runs, do: Docket.Postgres.RunStore
      def events, do: Docket.Postgres.VehicleStorageTest.FailingEvents
    end

    defmodule RelayRuns do
      defdelegate fetch_run(ctx, scope, run_id), to: Docket.Postgres.RunStore
      defdelegate release_claim(ctx, scope, run_id, token, now), to: Docket.Postgres.RunStore
      defdelegate abandon_claim(ctx, scope, run_id, token, policy), to: Docket.Postgres.RunStore
      defdelegate claim_due(ctx, scope, policy), to: Docket.Postgres.RunStore
      defdelegate commit(ctx, scope, proposal), to: Docket.Postgres.RunStore

      def refresh_claim(ctx, scope, run_id, token, now) do
        result = Docket.Postgres.RunStore.refresh_claim(ctx, scope, run_id, token, now)

        case Process.whereis(:storage_heartbeat_relay) do
          nil -> :ok
          pid -> send(pid, {:refreshed, result})
        end

        result
      end
    end

    defmodule RelayBackend do
      def storage, do: Docket.Postgres.Storage
      def graphs, do: Docket.Postgres.GraphStore
      def runs, do: Docket.Postgres.VehicleStorageTest.RelayRuns
      def events, do: Docket.Postgres.EventStore
    end

    setup_all do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)

      {:ok, migrator} = TestRepo.start_link()
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      :ok = Supervisor.stop(migrator)

      # Pool size 1 makes the zero-connections-during-node-execution proof
      # exact: any held connection starves every other query.
      start_supervised!({TestRepo, pool_size: 1})
      :ok
    end

    setup do
      TestRepo.delete_all(Event)
      TestRepo.delete_all(Run)
      TestRepo.delete_all(GraphVersion)

      context = %{repo: TestRepo}
      %{context: context, backend: {Backend, context}}
    end

    test "multi-step run drains to done with one commit transaction per moment", %{
      context: context,
      backend: backend
    } do
      rtg = publish!(context, Graphs.simple_edge())
      run = start_run!(backend, rtg, %{"topic" => "docket"})
      lease = claim!(context, @now)

      handler = attach_query_counter!()

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(lease, vehicle_opts(context, clock: fn -> @now end))

      :telemetry.detach(handler)
      commits = Agent.get(query_counter(), &Map.get(&1, "commit", 0))

      assert {:ok, done} = RunStore.fetch_run(context, :system, run.id)
      assert done.status == :done
      assert commits == done.checkpoint_seq - lease.checkpoint_seq
      assert done.checkpoint_seq > lease.checkpoint_seq + 1
    end

    test "slow node holds zero checked-out database connections", %{
      context: context,
      backend: backend
    } do
      rtg = publish!(context, Graphs.blocking())
      run = start_run!(backend, rtg, %{})
      lease = claim!(context, DateTime.utc_now())
      supervisor = start_supervised!({Task.Supervisor, name: __MODULE__.BlockingSup})

      opts =
        vehicle_opts(context,
          context: %{coordinator: self()},
          task_supervisor: supervisor
        )

      assert {:ok, vehicle} = Vehicle.launch(lease, opts)
      monitor = Process.monitor(vehicle)

      assert_receive {:blocked, node_pid, "blocker", 1}, 5_000

      # Pool size is 1: this query succeeds only if the vehicle holds no
      # checked-out connection while the node executes.
      assert %{rows: [[1]]} = TestRepo.query!("SELECT 1", [], timeout: 2_000)

      send(node_pid, :release)
      assert_receive {:DOWN, ^monitor, :process, ^vehicle, :normal}, 5_000

      assert {:ok, done} = RunStore.fetch_run(context, :system, run.id)
      assert done.status == :done
    end

    test "heartbeat refreshes the claim from a companion process while node work blocks", %{
      context: context,
      backend: backend
    } do
      rtg = publish!(context, Graphs.blocking())
      run = start_run!(backend, rtg, %{})
      lease = claim!(context, DateTime.utc_now())
      supervisor = start_supervised!({Task.Supervisor, name: __MODULE__.HeartbeatSup})
      Process.register(self(), :storage_heartbeat_relay)

      opts =
        [backend: {RelayBackend, context}, graph_cache: false]
        |> Keyword.merge(
          context: %{coordinator: self()},
          task_supervisor: supervisor,
          heartbeat: [interval_ms: 50]
        )

      assert {:ok, vehicle} = Vehicle.launch(lease, opts)
      monitor = Process.monitor(vehicle)

      assert_receive {:blocked, node_pid, "blocker", 1}, 5_000

      # Pool size is 1 and node work holds no connection, so the companion
      # heartbeat's refresh gets the pool to itself while the node blocks.
      assert_receive {:refreshed, :ok}, 5_000
      assert_receive {:refreshed, :ok}, 5_000

      assert {:ok, info} = RunStore.inspect_run(context, :system, run.id)
      assert DateTime.compare(info.claimed_at, lease.claimed_at) == :gt

      send(node_pid, :release)
      assert_receive {:DOWN, ^monitor, :process, ^vehicle, :normal}, 5_000

      assert {:ok, done} = RunStore.fetch_run(context, :system, run.id)
      assert done.status == :done
    end

    test "killed vehicle leaves the claim; recovery resumes from the last committed moment", %{
      context: context,
      backend: backend
    } do
      graph =
        Graph.new!(id: "commit-then-block")
        |> Graph.put_field!("first", schema: Schema.string(), reducer: Reducer.last_value())
        |> Graph.put_field!("out", schema: Schema.string(), reducer: Reducer.last_value())
        |> Graph.put_node!("writer",
          implementation: Nodes.WriteStatic,
          config: %{field: "first", value: "committed"}
        )
        |> Graph.put_node!("blocker",
          implementation: Nodes.SleepsUntilReleased,
          config: %{field: "out", value: "released"}
        )
        |> Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
        |> Graph.put_edge!("edge_writer_blocker", from: "writer", to: "blocker")
        |> Graph.put_edge!("edge_blocker_finish", from: "blocker", to: "$finish")
        |> Graph.put_output!("out", [])

      rtg = publish!(context, graph)
      run = start_run!(backend, rtg, %{})
      lease = claim!(context, DateTime.utc_now())
      supervisor = start_supervised!({Task.Supervisor, name: __MODULE__.KillSup})

      opts =
        vehicle_opts(context,
          context: %{coordinator: self()},
          task_supervisor: supervisor
        )

      assert {:ok, vehicle} = Vehicle.launch(lease, opts)
      monitor = Process.monitor(vehicle)
      assert_receive {:blocked, _node_pid, "blocker", 1}, 5_000

      Process.exit(vehicle, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^vehicle, :killed}, 5_000

      # The writer superstep is committed; the blocker superstep never was.
      assert {:ok, killed} = RunStore.fetch_run(context, :system, run.id)
      assert killed.status == :running
      assert killed.channels["state:first"].value == "committed"

      assert {:ok, %RunInfo{claimed_at: %DateTime{}}} =
               RunStore.inspect_run(context, :system, run.id)

      # The abandoned claim expires and is stolen, then the drain completes.
      # The writer commit reset the attempt count, so the steal is attempt 1.
      steal_lease = claim!(context, DateTime.utc_now(), orphan_ttl_ms: 0)
      assert steal_lease.claim_attempt == 1
      refute steal_lease.claim_token == lease.claim_token

      assert {:ok, second} = Vehicle.launch(steal_lease, opts)
      second_monitor = Process.monitor(second)
      assert_receive {:blocked, node_pid, "blocker", 1}, 5_000
      send(node_pid, :release)
      assert_receive {:DOWN, ^second_monitor, :process, ^second, :normal}, 5_000

      assert {:ok, done} = RunStore.fetch_run(context, :system, run.id)
      assert done.status == :done
      assert done.output["out"] == "released"

      # No duplicated writer superstep: event sequences are contiguous and
      # the writer completed exactly once.
      events = TestRepo.all(Event)
      seqs = events |> Enum.map(& &1.seq) |> Enum.sort()
      assert seqs == Enum.to_list(1..length(seqs))
      completed_nodes = for event <- events, event.type == :node_completed, do: event.node_id
      assert Enum.count(completed_nodes, &(&1 == "writer")) == 1
    end

    test "retryable failure parks with :retry_scheduled and resumes at the deadline", %{
      context: context,
      backend: backend
    } do
      graph =
        Graph.new!(id: "retry-park")
        |> Graph.put_field!("out", schema: Schema.string(), reducer: Reducer.last_value())
        |> Graph.put_node!("flaky",
          implementation: Nodes.FlakyThenSucceeds,
          config: %{failures: 1.0, field: "out", value: "done"},
          policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 60_000}}
        )
        |> Graph.put_edge!("edge_start_flaky", from: "$start", to: "flaky")
        |> Graph.put_edge!("edge_flaky_finish", from: "flaky", to: "$finish")
        |> Graph.put_output!("out", [])

      rtg = publish!(context, graph)
      run = start_run!(backend, rtg, %{})
      lease = claim!(context, @now)
      deadline = DateTime.add(@now, 60_000, :millisecond)

      assert {:ok, {:parked, {:at, ^deadline}}} =
               Vehicle.drain(lease, vehicle_opts(context, clock: fn -> @now end))

      assert {:ok, parked} = RunStore.fetch_run(context, :system, run.id)
      assert parked.status == :running
      assert map_size(parked.active_tasks) == 1
      assert map_size(parked.timers) == 1

      assert {:ok, %RunInfo{wake_at: ^deadline, claimed_at: nil}} =
               RunStore.inspect_run(context, :system, run.id)

      resumed_lease = claim!(context, deadline)

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(resumed_lease, vehicle_opts(context, clock: fn -> deadline end))

      assert {:ok, done} = RunStore.fetch_run(context, :system, run.id)
      assert done.status == :done
      assert done.output["out"] == "done"
    end

    test "interrupt parks externally; resolution wakes and the drain finishes", %{
      context: context,
      backend: backend
    } do
      rtg = publish!(context, Graphs.interrupt_review())
      run = start_run!(backend, rtg, %{})
      lease = claim!(context, @now)

      assert {:ok, {:parked, :external}} =
               Vehicle.drain(lease, vehicle_opts(context, clock: fn -> @now end))

      assert {:ok, waiting} = RunStore.fetch_run(context, :system, run.id)
      assert waiting.status == :waiting

      assert {:ok, %RunInfo{wake_at: nil, claimed_at: nil}} =
               RunStore.inspect_run(context, :system, run.id)

      [interrupt_id] =
        for {id, interrupt} <- waiting.interrupts, interrupt.status == :open, do: id

      resolve_at = DateTime.utc_now()

      assert {:ok, _moment} =
               Lifecycle.signal(backend, :tenantless, run.id, fn current ->
                 RunMutation.resolve_interrupt(rtg, current, interrupt_id, "approve", resolve_at)
               end)

      # An immediate wake is recorded at the database clock, so reclaim with
      # real time rather than the fixed test instant.
      resume_now = DateTime.add(DateTime.utc_now(), 1, :second)
      resumed_lease = claim!(context, resume_now)

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(resumed_lease, vehicle_opts(context, clock: fn -> resume_now end))

      assert {:ok, done} = RunStore.fetch_run(context, :system, run.id)
      assert done.status == :done
      assert done.channels["state:applied"].value == "approve"
    end

    test "compilation failure abandons with a future wake; repeats poison", %{
      context: context,
      backend: backend
    } do
      rtg = publish!(context, Graphs.minimal_linear())
      run = start_run!(backend, rtg, %{"value" => "hello"})
      lease = claim!(context, @now)

      opts =
        vehicle_opts(context,
          clock: fn -> @now end,
          compiler: fn graph, _opts -> {:error, graph} end,
          abandon_backoff_ms: 1_000,
          jitter: fn _limit -> 0 end,
          max_claim_abandons: 1
        )

      assert {:ok, {:abandoned, :rescheduled, {:graph_compilation_failed, _diagnostics}}} =
               Vehicle.drain(lease, opts)

      wake = DateTime.add(@now, 1_000, :millisecond)

      assert {:ok,
              %RunInfo{
                wake_at: ^wake,
                claimed_at: nil,
                claim_attempts: 0,
                claim_abandons: 1
              }} = RunStore.inspect_run(context, :system, run.id)

      second_lease = claim!(context, wake)

      assert {:ok, {:abandoned, :poisoned, {:graph_compilation_failed, _diagnostics}}} =
               Vehicle.drain(second_lease, Keyword.put(opts, :clock, fn -> wake end))

      assert {:ok,
              %RunInfo{
                poisoned_at: %DateTime{},
                poison_reason: "max_claim_abandons_exceeded"
              }} = RunStore.inspect_run(context, :system, run.id)

      # Poison recovery clears the disposition for a fixed deployment.
      assert {:ok, _run} = RunStore.retry_poisoned_run(context, :tenantless, run.id, wake)

      third_lease = claim!(context, wake)

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(third_lease, vehicle_opts(context, clock: fn -> wake end))

      assert {:ok, done} = RunStore.fetch_run(context, :system, run.id)
      assert done.status == :done
    end

    test "a signal between claim and fetch is fence loss, not an invariant violation", %{
      context: context,
      backend: backend
    } do
      rtg = publish!(context, Graphs.minimal_linear())
      run = start_run!(backend, rtg, %{"value" => "hello"})
      lease = claim!(context, @now)

      cancel_at = DateTime.add(@now, 1, :second)

      assert {:ok, _moment} =
               Lifecycle.signal(backend, :tenantless, run.id, fn current ->
                 RunMutation.cancel_run(current, cancel_at)
               end)

      assert {:ok, :fence_lost} =
               Vehicle.drain(lease, vehicle_opts(context, clock: fn -> cancel_at end))

      assert {:ok, cancelled} = RunStore.fetch_run(context, :system, run.id)
      assert cancelled.status == :cancelled
      assert {:ok, %RunInfo{claimed_at: nil}} = RunStore.inspect_run(context, :system, run.id)
    end

    test "event append failure rolls back the moment and releases the claim", %{
      context: context,
      backend: backend
    } do
      rtg = publish!(context, Graphs.minimal_linear())
      run = start_run!(backend, rtg, %{"value" => "hello"})
      lease = claim!(context, @now)
      before_events = TestRepo.aggregate(Event, :count)

      failing = vehicle_opts(context, clock: fn -> @now end)
      failing = Keyword.put(failing, :backend, {FailingEventsBackend, context})

      assert {:ok, {:discarded, :injected_event_failure}} = Vehicle.drain(lease, failing)

      assert {:ok, unchanged} = RunStore.fetch_run(context, :system, run.id)
      assert unchanged.checkpoint_seq == lease.checkpoint_seq
      assert unchanged.status == :running
      assert TestRepo.aggregate(Event, :count) == before_events

      assert {:ok, %RunInfo{claimed_at: nil, wake_at: %DateTime{}}} =
               RunStore.inspect_run(context, :system, run.id)

      # The released run drains cleanly under the real backend.
      retry_lease = claim!(context, @now)

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(retry_lease, vehicle_opts(context, clock: fn -> @now end))

      assert {:ok, done} = RunStore.fetch_run(context, :system, run.id)
      assert done.status == :done
    end

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    test "drain-budget yield commits run, events, claim release, wake, and notify atomically",
         %{context: context, backend: backend} do
      rtg = publish!(context, Graphs.endless_cycle())
      run = start_run!(backend, rtg, %{})
      lease = claim!(context, @now)

      listen!()

      assert {:ok, {:parked, :immediate}} =
               Vehicle.drain(
                 lease,
                 vehicle_opts(context, clock: fn -> @now end, drain_budget: [max_moments: 2])
               )

      assert_receive {:notification, _pid, _ref, "docket_wake", ""}, 2_000

      assert {:ok, running} = RunStore.fetch_run(context, :system, run.id)
      assert running.status == :running
      assert running.checkpoint_seq == lease.checkpoint_seq + 2

      assert {:ok, %RunInfo{} = info} = RunStore.inspect_run(context, :system, run.id)
      assert info.claimed_at == nil
      assert %DateTime{} = info.wake_at

      row = TestRepo.get_by!(Run, run_id: run.id)
      assert row.claim_token == nil

      committed =
        Event
        |> TestRepo.all()
        |> Enum.filter(&(&1.run_id == run.id))
        |> Enum.sort_by(& &1.seq)

      metadata = fn event -> Docket.DurableCodec.decode!(event.metadata, :event) end

      yields =
        Enum.filter(committed, fn event ->
          event.type == :checkpoint_committed and
            metadata.(event)["park_reason"] == "drain_budget"
        end)

      assert [yield_event] = yields
      assert yield_event.seq == List.last(committed).seq
      assert metadata.(yield_event)["wake_disposition"] == "immediate"
      assert metadata.(yield_event)["checkpoint_type"] == "step_committed"
      assert metadata.(yield_event)["checkpoint_seq"] == running.checkpoint_seq
    end

    test "event append failure persists no partial yield and sends no notification", %{
      context: context,
      backend: backend
    } do
      rtg = publish!(context, Graphs.endless_cycle())
      run = start_run!(backend, rtg, %{})
      lease = claim!(context, @now)

      listen!()

      assert {:ok, {:discarded, :injected_event_failure}} =
               Vehicle.drain(
                 lease,
                 backend: {FailingEventsBackend, context},
                 graph_cache: false,
                 clock: fn -> @now end,
                 drain_budget: [max_moments: 1]
               )

      refute_receive {:notification, _pid, _ref, "docket_wake", _payload}, 300

      assert {:ok, unchanged} = RunStore.fetch_run(context, :system, run.id)
      assert unchanged.checkpoint_seq == run.checkpoint_seq
      assert unchanged.step == run.step

      events = Event |> TestRepo.all() |> Enum.filter(&(&1.run_id == run.id))

      assert Enum.filter(events, fn event ->
               event.type == :checkpoint_committed and
                 Docket.DurableCodec.decode!(event.metadata, :event)["park_reason"] ==
                   "drain_budget"
             end) == []

      # The discard released the claim with a poll-visible wake.
      assert {:ok, %RunInfo{claimed_at: nil, wake_at: %DateTime{}}} =
               RunStore.inspect_run(context, :system, run.id)
    end

    test "at concurrency one an infinite cycle yields so the older due run completes", %{
      context: context,
      backend: backend
    } do
      cycle_rtg = publish!(context, Graphs.endless_cycle())
      linear_rtg = publish!(context, Graphs.minimal_linear())

      # The cycle is due earlier, so the single slot claims it first.
      cycle = start_run!(backend, cycle_rtg, %{}, DateTime.add(@now, -60, :second))
      short = start_run!(backend, linear_rtg, %{"value" => "x"}, @now)

      lease = claim!(context, @now)
      assert lease.run_id == cycle.id

      assert {:ok, {:parked, :immediate}} =
               Vehicle.drain(
                 lease,
                 vehicle_opts(context, clock: fn -> @now end, drain_budget: [max_moments: 3])
               )

      # The yield stamped the cycle's wake at commit time, so the short run
      # is now the older due candidate and wins the freed slot.
      short_lease = claim!(context, DateTime.utc_now())
      assert short_lease.run_id == short.id

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(short_lease, vehicle_opts(context, clock: fn -> @now end))

      assert {:ok, %{status: :done}} = RunStore.fetch_run(context, :system, short.id)
      assert {:ok, %{status: :running}} = RunStore.fetch_run(context, :system, cycle.id)

      # The cycle keeps its own progress on the next slot.
      cycle_lease = claim!(context, DateTime.utc_now())
      assert cycle_lease.run_id == cycle.id
    end

    test "cache-disabled repeated budget yields keep committing progress", %{
      context: context,
      backend: backend
    } do
      rtg = publish!(context, Graphs.endless_cycle())
      run = start_run!(backend, rtg, %{})

      steps =
        for _ <- 1..3 do
          lease = claim!(context, DateTime.utc_now())

          assert {:ok, {:parked, :immediate}} =
                   Vehicle.drain(
                     lease,
                     vehicle_opts(context,
                       clock: fn -> @now end,
                       drain_budget: [max_moments: 1]
                     )
                   )

          {:ok, current} = RunStore.fetch_run(context, :system, run.id)
          current.step
        end

      assert steps == Enum.sort(steps)
      assert length(Enum.uniq(steps)) == 3
    end

    defp listen! do
      opts =
        TestRepo.config()
        |> Keyword.drop([
          :adapter,
          :log,
          :name,
          :otp_app,
          :pool,
          :pool_count,
          :pool_size,
          :priv,
          :stacktrace,
          :telemetry_prefix
        ])
        |> Keyword.merge(sync_connect: true, auto_reconnect: true)

      {:ok, listener} = Postgrex.Notifications.start_link(opts)
      {:ok, _reference} = Postgrex.Notifications.listen(listener, "docket_wake")
      listener
    end

    defp publish!(context, graph) do
      {:ok, effective, rtg} = Compiler.compile_for_publication(graph, profile: :publish)
      :ok = GraphStore.save_graph(context, rtg.graph_id, rtg.graph_hash, effective)
      rtg
    end

    defp start_run!(backend, rtg, input, now \\ @now) do
      opts = [clock: fn -> now end]
      run = Loop.build_initial_run(rtg, input, opts)
      {:ok, moment} = Loop.propose_init(rtg, run, opts)
      {:ok, moment} = Lifecycle.start(backend, :tenantless, moment)
      moment.run
    end

    defp claim!(context, now, opts \\ []) do
      policy = %{
        now: now,
        limit: 1,
        orphan_ttl_ms: Keyword.get(opts, :orphan_ttl_ms, 60_000),
        max_claim_attempts: Keyword.get(opts, :max_claim_attempts, 3)
      }

      {:ok, %{leases: [lease], poisoned: []}} = RunStore.claim_due(context, :system, policy)
      lease
    end

    defp vehicle_opts(context, extra) do
      Keyword.merge([backend: {Backend, context}, graph_cache: false], extra)
    end

    defp query_counter do
      case Process.whereis(__MODULE__.QueryCounter) do
        nil ->
          {:ok, pid} = Agent.start_link(fn -> %{} end, name: __MODULE__.QueryCounter)
          pid

        pid ->
          pid
      end
    end

    defp attach_query_counter! do
      counter = query_counter()
      Agent.update(counter, fn _counts -> %{} end)
      handler = "vehicle-storage-query-counter-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler,
          [:docket, :postgres, :vehicle_storage_test_repo, :query],
          &Docket.Test.TelemetryRelay.count_query/4,
          __MODULE__.QueryCounter
        )

      handler
    end
  end
end
