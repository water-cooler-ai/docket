if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.VehicleTest do
    use ExUnit.Case, async: false

    alias Docket.Postgres.Vehicle
    alias Docket.Test.Fixtures.Graphs
    alias Docket.Test.MemoryBackend

    @task_sup __MODULE__.TaskSup

    @telemetry_events [
      [:docket, :run, :initialized],
      [:docket, :run, :completed],
      [:docket, :run, :failed],
      [:docket, :checkpoint, :committed],
      [:docket, :node, :completed],
      [:docket, :node, :failed],
      [:docket, :channel, :updated],
      [:docket, :edge, :triggered],
      [:docket, :interrupt, :requested],
      [:docket, :interrupt, :resolved]
    ]

    defmodule Host do
      use Docket, backend: Docket.Test.MemoryBackend
    end

    defmodule RecordingObserver do
      @behaviour Docket.Checkpoint.Observer

      @impl true
      def observe(checkpoint, %{application: %{notify: pid}}) do
        send(pid, {:observed, checkpoint.type})
        :ok
      end

      def observe(_checkpoint, _context), do: :ok
    end

    defmodule StaleCommitBackend do
      def storage, do: Docket.Test.MemoryBackend
      def graphs, do: Docket.Test.MemoryBackend
      def events, do: Docket.Test.MemoryBackend
      def runs, do: __MODULE__.Runs

      defmodule Runs do
        defdelegate fetch_run(context, scope, run_id), to: Docket.Test.MemoryBackend

        defdelegate release_claim(context, scope, run_id, claim_token, now),
          to: Docket.Test.MemoryBackend

        defdelegate abandon_claim(context, scope, run_id, claim_token, policy),
          to: Docket.Test.MemoryBackend

        defdelegate claim_due(context, scope, policy), to: Docket.Test.MemoryBackend

        def commit(_context, _scope, _proposal), do: {:error, :stale_fence}
      end
    end

    defmodule FailingEventsBackend do
      def storage, do: Docket.Test.MemoryBackend
      def graphs, do: Docket.Test.MemoryBackend
      def runs, do: Docket.Test.MemoryBackend
      def events, do: __MODULE__.Events

      defmodule Events do
        def append_events(_context, _scope, _run_id, _events),
          do: {:error, :injected_event_failure}
      end
    end

    defmodule DoneRunBackend do
      def storage, do: Docket.Test.MemoryBackend
      def graphs, do: Docket.Test.MemoryBackend
      def events, do: Docket.Test.MemoryBackend
      def runs, do: __MODULE__.Runs

      defmodule Runs do
        def fetch_run(context, scope, run_id) do
          {:ok, run} = Docket.Test.MemoryBackend.fetch_run(context, scope, run_id)
          {:ok, %{run | status: :done}}
        end
      end
    end

    defmodule MissingRunBackend do
      def storage, do: Docket.Test.MemoryBackend
      def graphs, do: Docket.Test.MemoryBackend
      def events, do: Docket.Test.MemoryBackend
      def runs, do: __MODULE__.Runs

      defmodule Runs do
        def fetch_run(_context, _scope, _run_id), do: {:error, :not_found}
      end
    end

    # Relays every heartbeat refresh (call + result + heartbeat pid) to the
    # process registered as :vehicle_heartbeat_relay, giving tests a
    # deterministic sync point and the heartbeat pid without touching the
    # vehicle's own channels.
    defmodule RelayRunsBackend do
      def storage, do: Docket.Test.MemoryBackend
      def graphs, do: Docket.Test.MemoryBackend
      def events, do: Docket.Test.MemoryBackend
      def runs, do: __MODULE__.Runs

      defmodule Runs do
        defdelegate fetch_run(context, scope, run_id), to: Docket.Test.MemoryBackend

        defdelegate release_claim(context, scope, run_id, claim_token, now),
          to: Docket.Test.MemoryBackend

        defdelegate abandon_claim(context, scope, run_id, claim_token, policy),
          to: Docket.Test.MemoryBackend

        defdelegate claim_due(context, scope, policy), to: Docket.Test.MemoryBackend
        defdelegate commit(context, scope, proposal), to: Docket.Test.MemoryBackend

        def refresh_claim(context, scope, run_id, claim_token, now) do
          result =
            Docket.Test.MemoryBackend.refresh_claim(context, scope, run_id, claim_token, now)

          case Process.whereis(:vehicle_heartbeat_relay) do
            nil ->
              :ok

            pid ->
              # This runs in a refresh worker spawned by the heartbeat, so
              # the parent is the heartbeat itself. Relaying it lets tests
              # synchronize on heartbeat exit - the loss flag is always
              # written before the heartbeat dies.
              {:parent, heartbeat} = Process.info(self(), :parent)
              send(pid, {:refreshed, heartbeat, result})
          end

          result
        end
      end
    end

    defmodule RaisingRefreshBackend do
      def storage, do: Docket.Test.MemoryBackend
      def graphs, do: Docket.Test.MemoryBackend
      def events, do: Docket.Test.MemoryBackend
      def runs, do: __MODULE__.Runs

      defmodule Runs do
        defdelegate fetch_run(context, scope, run_id), to: Docket.Test.MemoryBackend

        defdelegate release_claim(context, scope, run_id, claim_token, now),
          to: Docket.Test.MemoryBackend

        defdelegate abandon_claim(context, scope, run_id, claim_token, policy),
          to: Docket.Test.MemoryBackend

        defdelegate claim_due(context, scope, policy), to: Docket.Test.MemoryBackend
        defdelegate commit(context, scope, proposal), to: Docket.Test.MemoryBackend

        def refresh_claim(_context, _scope, _run_id, _claim_token, _now) do
          case Process.whereis(:vehicle_heartbeat_relay) do
            nil ->
              :ok

            pid ->
              {:parent, heartbeat} = Process.info(self(), :parent)
              send(pid, {:refresh_attempted, heartbeat})
          end

          raise "injected refresh failure"
        end
      end
    end

    # Announces each refresh attempt, then blocks forever: the store call
    # never returns, so only the heartbeat's own staleness bound can end it.
    defmodule StuckRefreshBackend do
      def storage, do: Docket.Test.MemoryBackend
      def graphs, do: Docket.Test.MemoryBackend
      def events, do: Docket.Test.MemoryBackend
      def runs, do: __MODULE__.Runs

      defmodule Runs do
        defdelegate fetch_run(context, scope, run_id), to: Docket.Test.MemoryBackend

        defdelegate release_claim(context, scope, run_id, claim_token, now),
          to: Docket.Test.MemoryBackend

        defdelegate abandon_claim(context, scope, run_id, claim_token, policy),
          to: Docket.Test.MemoryBackend

        defdelegate claim_due(context, scope, policy), to: Docket.Test.MemoryBackend
        defdelegate commit(context, scope, proposal), to: Docket.Test.MemoryBackend

        def refresh_claim(_context, _scope, _run_id, _claim_token, _now) do
          case Process.whereis(:vehicle_heartbeat_relay) do
            nil ->
              :ok

            pid ->
              {:parent, heartbeat} = Process.info(self(), :parent)
              send(pid, {:refresh_started, self(), heartbeat})
          end

          receive do
            :unblock -> {:error, :claim_lost}
          end
        end
      end
    end

    # Holds fetch_run until the test sends :go, then reports the run as
    # already done so claimed_run raises - a controllable raise inside the
    # drain, after the heartbeat has had time to identify itself.
    defmodule BlockingDoneFetchBackend do
      def storage, do: Docket.Test.MemoryBackend
      def graphs, do: Docket.Test.MemoryBackend
      def events, do: Docket.Test.MemoryBackend
      def runs, do: __MODULE__.Runs

      defmodule Runs do
        defdelegate release_claim(context, scope, run_id, claim_token, now),
          to: Docket.Test.MemoryBackend

        defdelegate abandon_claim(context, scope, run_id, claim_token, policy),
          to: Docket.Test.MemoryBackend

        defdelegate claim_due(context, scope, policy), to: Docket.Test.MemoryBackend
        defdelegate commit(context, scope, proposal), to: Docket.Test.MemoryBackend

        defdelegate refresh_claim(context, scope, run_id, claim_token, now),
          to: Docket.Postgres.VehicleTest.RelayRunsBackend.Runs

        def fetch_run(context, scope, run_id) do
          case Process.whereis(:vehicle_heartbeat_relay) do
            nil ->
              :ok

            pid ->
              send(pid, {:fetching, self()})

              receive do
                :go -> :ok
              end
          end

          {:ok, run} = Docket.Test.MemoryBackend.fetch_run(context, scope, run_id)
          {:ok, %{run | status: :done}}
        end
      end
    end

    setup do
      start_supervised!(Host)
      start_supervised!({Task.Supervisor, name: @task_sup})
      {:ok, defaults} = Docket.Runtime.Registry.defaults(Host)

      backend_ref =
        {Keyword.fetch!(defaults, :backend), Keyword.fetch!(defaults, :backend_context)}

      %{backend_ref: backend_ref, context: elem(backend_ref, 1)}
    end

    test "drains a multi-step run to done with one checkpoint event per commit", %{
      backend_ref: backend_ref,
      context: context
    } do
      {run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "hello"})

      assert {:ok, {:parked, :terminal}} = Vehicle.drain(lease, vehicle_opts(backend_ref))

      assert {:ok, done} = Host.fetch_run(run.id)
      assert done.status == :done
      assert done.output == %{"result" => "hello"}

      events = MemoryBackend.events(context, run.id)
      assert events != []

      committed_seqs =
        events
        |> Enum.filter(&(&1.type == :checkpoint_committed))
        |> Enum.map(& &1.metadata["checkpoint_seq"])

      assert committed_seqs == Enum.to_list(1..done.checkpoint_seq)
    end

    test "post-commit observers fire per committed moment via Lifecycle", %{
      backend_ref: backend_ref
    } do
      {_run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "hello"})

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(lease, vehicle_opts(backend_ref, observer_opts()))

      assert_receive {:observed, :step_committed}
      assert_receive {:observed, :run_completed}
      refute_receive {:observed, _}
    end

    test "cyclic graph drains to done without any max_supersteps option", %{
      backend_ref: backend_ref
    } do
      {run, lease} = start_claimed!(backend_ref, Graphs.cycle_counter(), %{})

      assert {:ok, {:parked, :terminal}} = Vehicle.drain(lease, vehicle_opts(backend_ref))

      assert {:ok, done} = Host.fetch_run(run.id)
      assert done.status == :done
      assert done.channels["state:count"].value == 10
    end

    # The cycle_counter fixture declares its own "max_supersteps" graph policy,
    # which always wins over the host budget (Algorithm.max_supersteps/2), so
    # the host budget is exercised on fanout, which declares no policy.
    test "host max_supersteps budget fails the run terminally", %{backend_ref: backend_ref} do
      {run, lease} = start_claimed!(backend_ref, Graphs.fanout(), %{"value" => "x"})

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(lease, vehicle_opts(backend_ref, max_supersteps: 1))

      assert {:ok, failed} = Host.fetch_run(run.id)
      assert failed.status == :failed
      assert failed.failure.code == "max_supersteps_exceeded"
    end

    test "retry parking commits :retry_scheduled, schedules the wake, and exits without sleeping",
         %{backend_ref: backend_ref} do
      t0 = DateTime.add(DateTime.utc_now(), 3600, :second)
      {run, lease} = start_claimed!(backend_ref, Graphs.retry_then_continue(), %{}, t0)

      assert {:ok, {:parked, {:at, deadline}}} =
               Vehicle.drain(lease, vehicle_opts(backend_ref, clock: fn -> t0 end))

      # The fixture's retry backoff_ms is 0, so the deadline is the injected clock.
      assert DateTime.compare(deadline, t0) == :eq

      assert {:ok, parked} = Host.fetch_run(run.id)
      assert parked.status == :running
      assert map_size(parked.active_tasks) == 1
      assert map_size(parked.timers) == 1

      assert {:ok, info} = Host.inspect_run(run.id)
      assert DateTime.compare(info.wake_at, deadline) == :eq
      assert info.claimed_at == nil

      # Attempt 2 fails and parks again; attempt 3 succeeds and completes.
      [lease2] = claim!(backend_ref, deadline)

      assert {:ok, {:parked, {:at, deadline2}}} =
               Vehicle.drain(lease2, vehicle_opts(backend_ref, clock: fn -> deadline end))

      [lease3] = claim!(backend_ref, deadline2)

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(lease3, vehicle_opts(backend_ref, clock: fn -> deadline2 end))

      assert {:ok, done} = Host.fetch_run(run.id)
      assert done.status == :done
      assert done.output == %{"out" => "done"}
    end

    test "fence lost between claim and fetch stops without commit", %{backend_ref: backend_ref} do
      {run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})

      assert {:ok, cancelled} = Host.cancel_run(run.id)
      assert cancelled.status == :cancelled

      assert {:ok, :fence_lost} =
               Vehicle.drain(lease, vehicle_opts(backend_ref, observer_opts()))

      assert {:ok, after_drain} = Host.fetch_run(run.id)
      assert after_drain == cancelled
      assert after_drain.checkpoint_seq == cancelled.checkpoint_seq
      refute_receive {:observed, _}
    end

    test "commit fence loss mid-drain discards the moment and releases the claim", %{
      backend_ref: backend_ref,
      context: context
    } do
      {run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})
      {:ok, pre_drain} = Host.fetch_run(run.id)

      assert {:ok, {:discarded, :stale_fence}} =
               Vehicle.drain(
                 lease,
                 vehicle_opts({StaleCommitBackend, context}, observer_opts())
               )

      assert {:ok, after_drain} = Host.fetch_run(run.id)
      assert after_drain == pre_drain
      assert MemoryBackend.claim(context, run.id) == nil
      assert %DateTime{} = MemoryBackend.wake_at(context, run.id)
      refute_receive {:observed, _}
    end

    test "event append failure rolls back the commit and releases without telemetry", %{
      backend_ref: backend_ref,
      context: context
    } do
      {run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})
      {:ok, pre_drain} = Host.fetch_run(run.id)

      handler_id = "vehicle-test-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach_many(
          handler_id,
          @telemetry_events,
          &__MODULE__.telemetry_relay/4,
          %{pid: self()}
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert {:ok, {:discarded, :injected_event_failure}} =
               Vehicle.drain(
                 lease,
                 vehicle_opts({FailingEventsBackend, context}, observer_opts())
               )

      :telemetry.detach(handler_id)

      assert {:ok, after_drain} = Host.fetch_run(run.id)
      assert after_drain == pre_drain
      assert after_drain.checkpoint_seq == pre_drain.checkpoint_seq
      assert after_drain.status == :running
      assert MemoryBackend.claim(context, run.id) == nil
      assert %DateTime{} = MemoryBackend.wake_at(context, run.id)
      refute_receive {:telemetry, _}
      refute_receive {:observed, _}
    end

    test "compilation failure abandons per the pre-execution disposition", %{
      backend_ref: backend_ref
    } do
      t0 = DateTime.add(DateTime.utc_now(), 3600, :second)
      {run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"}, t0)

      failing_compiler = fn graph, _opts -> {:error, graph} end

      assert {:ok, {:abandoned, :rescheduled, {:graph_compilation_failed, _}}} =
               Vehicle.drain(
                 lease,
                 vehicle_opts(backend_ref,
                   compiler: failing_compiler,
                   clock: fn -> t0 end,
                   jitter: fn _ -> 0 end,
                   abandon_backoff_ms: 1_000
                 )
               )

      assert {:ok, info} = Host.inspect_run(run.id)
      assert info.claim_abandons == 1
      assert info.claim_attempts == 0
      assert info.claimed_at == nil
      assert DateTime.compare(info.wake_at, t0) == :gt
      assert DateTime.compare(info.wake_at, DateTime.add(t0, 1_000, :millisecond)) == :eq

      [lease2] = claim!(backend_ref, info.wake_at)

      assert {:ok, {:abandoned, :poisoned, {:graph_compilation_failed, _}}} =
               Vehicle.drain(
                 lease2,
                 vehicle_opts(backend_ref,
                   compiler: failing_compiler,
                   clock: fn -> info.wake_at end,
                   jitter: fn _ -> 0 end,
                   abandon_backoff_ms: 1_000,
                   max_claim_abandons: 1
                 )
               )

      assert {:ok, poisoned} = Host.inspect_run(run.id)
      assert %DateTime{} = poisoned.poisoned_at
      assert poisoned.poison_reason == "max_claim_abandons_exceeded"
      assert poisoned.claimed_at == nil
      assert poisoned.wake_at == nil
    end

    test "effective-identity mismatch abandons", %{backend_ref: backend_ref} do
      {_run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})

      forging_compiler = fn graph, opts ->
        {:ok, rtg} = Docket.Graph.Compiler.compile_effective_document(graph, opts)
        {:ok, %{rtg | graph_hash: "not-the-lease-hash"}}
      end

      assert {:ok, {:abandoned, :rescheduled, :effective_identity_mismatch}} =
               Vehicle.drain(lease, vehicle_opts(backend_ref, compiler: forging_compiler))
    end

    test "one compilation per drain even across many committed moments", %{
      backend_ref: backend_ref
    } do
      counter = start_supervised!({Agent, fn -> 0 end})
      {run, lease} = start_claimed!(backend_ref, Graphs.fanout(), %{"value" => "x"})

      assert {:ok, {:parked, :terminal}} =
               Vehicle.drain(
                 lease,
                 vehicle_opts(backend_ref, compiler: counting_compiler(counter))
               )

      assert {:ok, done} = Host.fetch_run(run.id)
      assert done.status == :done
      assert done.checkpoint_seq > 2
      assert Agent.get(counter, & &1) == 1
    end

    test "graph cache eliminates compilation on the next drain of the same version", %{
      backend_ref: backend_ref
    } do
      on_exit(&Docket.Postgres.GraphCache.clear/0)
      counter = start_supervised!({Agent, fn -> 0 end})

      {:ok, reference} = Host.save_graph(Graphs.minimal_linear())
      {:ok, run1} = Host.start_run(reference, %{"value" => "a"})
      {:ok, run2} = Host.start_run(reference, %{"value" => "b"})

      [lease1, lease2] = claim!(backend_ref, DateTime.add(DateTime.utc_now(), 1, :second), 2)

      opts = [
        backend: backend_ref,
        graph_cache: Docket.Postgres.GraphCache,
        compiler: counting_compiler(counter)
      ]

      assert {:ok, {:parked, :terminal}} = Vehicle.drain(lease1, opts)
      assert Agent.get(counter, & &1) == 1

      assert {:ok, {:parked, :terminal}} = Vehicle.drain(lease2, opts)
      assert Agent.get(counter, & &1) == 1

      for run_id <- [run1.id, run2.id] do
        assert {:ok, %{status: :done}} = Host.fetch_run(run_id)
      end
    end

    test "deferred hand-back when the active superstep has no due attempt", %{
      backend_ref: backend_ref
    } do
      t0 = DateTime.add(DateTime.utc_now(), 3600, :second)
      {run, lease} = start_claimed!(backend_ref, Graphs.retry_then_continue(), %{}, t0)

      assert {:ok, {:parked, {:at, deadline}}} =
               Vehicle.drain(lease, vehicle_opts(backend_ref, clock: fn -> t0 end))

      [lease2] = claim!(backend_ref, deadline)
      early = DateTime.add(deadline, -5, :second)
      doctored = %{lease2 | claimed_at: early}

      assert {:ok, {:deferred, :rescheduled}} =
               Vehicle.drain(doctored, vehicle_opts(backend_ref, clock: fn -> early end))

      assert {:ok, info} = Host.inspect_run(run.id)
      assert DateTime.compare(info.wake_at, deadline) == :eq
      assert info.claimed_at == nil
      assert info.claim_abandons == 1
    end

    test "claimed non-runnable row raises a claim invariant", %{
      backend_ref: backend_ref,
      context: context
    } do
      {_run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})

      error =
        assert_raise Docket.Error, fn ->
          Vehicle.drain(lease, vehicle_opts({DoneRunBackend, context}))
        end

      assert error.type == :claim_invariant

      error =
        assert_raise Docket.Error, fn ->
          Vehicle.drain(lease, vehicle_opts({MissingRunBackend, context}))
        end

      assert error.type == :claim_invariant
    end

    test "launch/2 runs the drain under the task supervisor", %{backend_ref: backend_ref} do
      {run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})

      assert {:ok, pid} =
               Vehicle.launch(lease, vehicle_opts(backend_ref, task_supervisor: @task_sup))

      monitor = Process.monitor(pid)
      assert_receive {:DOWN, ^monitor, :process, ^pid, reason}
      assert reason in [:normal, :noproc]

      assert {:ok, %{status: :done}} = Host.fetch_run(run.id)
    end

    describe "drain budget" do
      @drain_event [:docket, :postgres, :vehicle, :drain]

      defp watch_drains do
        handler_id = "vehicle-drain-#{System.unique_integer([:positive])}"
        parent = self()

        :ok =
          :telemetry.attach(
            handler_id,
            @drain_event,
            fn _event, measurements, metadata, _config ->
              send(parent, {:drain, measurements, metadata})
            end,
            nil
          )

        on_exit(fn -> :telemetry.detach(handler_id) end)
      end

      defp scripted_monotonic(values) do
        agent = start_supervised!({Agent, fn -> values end}, id: make_ref())

        fn ->
          Agent.get_and_update(agent, fn
            [last] -> {last, [last]}
            [head | tail] -> {head, tail}
          end)
        end
      end

      test "max_moments bounds consecutive commits and yields the Nth continuing moment", %{
        backend_ref: backend_ref,
        context: context
      } do
        watch_drains()
        {run, lease} = start_claimed!(backend_ref, Graphs.endless_cycle(), %{})

        assert {:ok, {:parked, :immediate}} =
                 Vehicle.drain(lease, vehicle_opts(backend_ref, drain_budget: [max_moments: 4]))

        assert {:ok, running} = Host.fetch_run(run.id)
        assert running.status == :running
        # Initialization committed seq 1 outside the drain; the drain adds
        # exactly max_moments commits, the last one the budget yield.
        assert running.checkpoint_seq == 5

        assert MemoryBackend.claim(context, run.id) == nil
        assert %DateTime{} = MemoryBackend.wake_at(context, run.id)

        checkpoints =
          context
          |> MemoryBackend.events(run.id)
          |> Enum.filter(&(&1.type == :checkpoint_committed))

        assert length(checkpoints) == 5

        final = List.last(checkpoints)
        assert final.metadata["checkpoint_type"] == "step_committed"
        assert final.metadata["wake_disposition"] == "immediate"
        assert final.metadata["park_reason"] == "drain_budget"

        yields = Enum.filter(checkpoints, &(&1.metadata["park_reason"] == "drain_budget"))
        assert length(yields) == 1

        assert_receive {:drain, %{committed_moments: 4, elapsed_ms: _},
                        %{outcome: :parked, budget: :max_moments}}

        # The yielded run stays claimable and keeps making progress.
        [lease2] = claim!(backend_ref, DateTime.add(DateTime.utc_now(), 2, :second))

        assert {:ok, {:parked, :immediate}} =
                 Vehicle.drain(lease2, vehicle_opts(backend_ref, drain_budget: [max_moments: 4]))

        assert {:ok, later} = Host.fetch_run(run.id)
        assert later.checkpoint_seq == 9
        assert later.channels["state:count"].value > running.channels["state:count"].value
      end

      test "max_moments of one still commits one moment of progress per drain", %{
        backend_ref: backend_ref
      } do
        {run, lease} = start_claimed!(backend_ref, Graphs.endless_cycle(), %{})

        assert {:ok, {:parked, :immediate}} =
                 Vehicle.drain(lease, vehicle_opts(backend_ref, drain_budget: [max_moments: 1]))

        assert {:ok, after_first} = Host.fetch_run(run.id)
        assert after_first.status == :running
        assert after_first.checkpoint_seq == 2
        assert after_first.step == 1
      end

      test "elapsed budget yields at the first pre-commit boundary observing expiry", %{
        backend_ref: backend_ref
      } do
        watch_drains()
        {run, lease} = start_claimed!(backend_ref, Graphs.endless_cycle(), %{})

        # Wall clock runs backward while the monotonic clock advances; only
        # the monotonic samples (start, then one per pre-commit boundary)
        # gate the yield. The first boundary observes 500ms, commits, the
        # second observes 1500ms and yields: elapsed bounds *starting*
        # supersteps, never preempting the in-flight one.
        t0 = DateTime.utc_now()
        backwards = start_supervised!({Agent, fn -> 0 end}, id: make_ref())

        backward_clock = fn ->
          seconds = Agent.get_and_update(backwards, &{&1, &1 + 1})
          DateTime.add(t0, -seconds, :second)
        end

        assert {:ok, {:parked, :immediate}} =
                 Vehicle.drain(
                   lease,
                   vehicle_opts(backend_ref,
                     clock: backward_clock,
                     monotonic_clock: scripted_monotonic([0, 500, 1_500]),
                     drain_budget: [max_elapsed_ms: 1_000]
                   )
                 )

        assert {:ok, running} = Host.fetch_run(run.id)
        assert running.status == :running
        assert running.checkpoint_seq == 3

        assert_receive {:drain, %{committed_moments: 2, elapsed_ms: 1_500},
                        %{outcome: :parked, budget: :max_elapsed_ms}}
      end

      test "count and elapsed limits firing together produce one yield", %{
        backend_ref: backend_ref,
        context: context
      } do
        watch_drains()
        {run, lease} = start_claimed!(backend_ref, Graphs.endless_cycle(), %{})

        assert {:ok, {:parked, :immediate}} =
                 Vehicle.drain(
                   lease,
                   vehicle_opts(backend_ref,
                     monotonic_clock: scripted_monotonic([0, 500, 1_500]),
                     drain_budget: [max_moments: 2, max_elapsed_ms: 1_000]
                   )
                 )

        yields =
          context
          |> MemoryBackend.events(run.id)
          |> Enum.filter(&(&1.metadata["park_reason"] == "drain_budget"))

        assert length(yields) == 1
        assert_receive {:drain, %{committed_moments: 2}, %{outcome: :parked, budget: :both}}
      end

      test "a terminal moment wins at the exact budget boundary", %{backend_ref: backend_ref} do
        watch_drains()
        {run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})

        # minimal_linear commits exactly two moments; the second is terminal
        # and coincides with the max_moments boundary.
        assert {:ok, {:parked, :terminal}} =
                 Vehicle.drain(lease, vehicle_opts(backend_ref, drain_budget: [max_moments: 2]))

        assert {:ok, %{status: :done}} = Host.fetch_run(run.id)
        assert_receive {:drain, %{committed_moments: 2}, %{outcome: :parked, budget: nil}}
      end

      test "retry and external-wait parks win at the exact budget boundary", %{
        backend_ref: backend_ref
      } do
        t0 = DateTime.add(DateTime.utc_now(), 3600, :second)
        {_run, lease} = start_claimed!(backend_ref, Graphs.retry_then_continue(), %{}, t0)

        assert {:ok, {:parked, {:at, _deadline}}} =
                 Vehicle.drain(
                   lease,
                   vehicle_opts(backend_ref,
                     clock: fn -> t0 end,
                     drain_budget: [max_moments: 1]
                   )
                 )

        {run2, lease2} = start_claimed!(backend_ref, Graphs.interrupt_review(), %{})

        assert {:ok, {:parked, :external}} =
                 Vehicle.drain(lease2, vehicle_opts(backend_ref, drain_budget: [max_moments: 1]))

        assert {:ok, %{status: :waiting}} = Host.fetch_run(run2.id)
      end

      test "max_supersteps terminal failure wins over a coincident drain budget", %{
        backend_ref: backend_ref
      } do
        watch_drains()
        {run, lease} = start_claimed!(backend_ref, Graphs.fanout(), %{"value" => "x"})

        # With max_supersteps 1 the second moment is the terminal failure,
        # exactly where max_moments 2 would otherwise yield.
        assert {:ok, {:parked, :terminal}} =
                 Vehicle.drain(
                   lease,
                   vehicle_opts(backend_ref,
                     max_supersteps: 1,
                     drain_budget: [max_moments: 2]
                   )
                 )

        assert {:ok, failed} = Host.fetch_run(run.id)
        assert failed.status == :failed
        assert failed.failure.code == "max_supersteps_exceeded"
        assert_receive {:drain, _measurements, %{outcome: :parked, budget: nil}}
      end

      test "cancellation wins over the drain budget as fence loss, never a yield", %{
        backend_ref: backend_ref,
        context: context
      } do
        watch_drains()
        {run, lease} = start_claimed!(backend_ref, Graphs.endless_cycle(), %{})

        assert {:ok, cancelled} = Host.cancel_run(run.id)
        assert cancelled.status == :cancelled

        assert {:ok, :fence_lost} =
                 Vehicle.drain(lease, vehicle_opts(backend_ref, drain_budget: [max_moments: 1]))

        assert {:ok, after_drain} = Host.fetch_run(run.id)
        assert after_drain == cancelled

        events = MemoryBackend.events(context, run.id)
        assert Enum.filter(events, &(&1.metadata["park_reason"] == "drain_budget")) == []
        assert_receive {:drain, %{committed_moments: 0}, %{outcome: :fence_lost, budget: nil}}
      end

      test "a failed yield commit emits no committed-yield telemetry", %{
        backend_ref: backend_ref,
        context: context
      } do
        watch_drains()
        {run, lease} = start_claimed!(backend_ref, Graphs.endless_cycle(), %{})
        {:ok, pre_drain} = Host.fetch_run(run.id)

        assert {:ok, {:discarded, :stale_fence}} =
                 Vehicle.drain(
                   lease,
                   vehicle_opts({StaleCommitBackend, context}, drain_budget: [max_moments: 1])
                 )

        assert {:ok, after_drain} = Host.fetch_run(run.id)
        assert after_drain == pre_drain
        assert_receive {:drain, %{committed_moments: 0}, %{outcome: :discarded, budget: nil}}

        # The first run was released with an immediate wake, so claim both
        # and pick the fresh run's lease.
        {:ok, reference} = Host.save_graph(Graphs.endless_cycle())
        {:ok, run2} = Host.start_run(reference, %{})
        leases = claim!(backend_ref, DateTime.add(DateTime.utc_now(), 2, :second), 2)
        lease2 = Enum.find(leases, &(&1.run_id == run2.id))
        {:ok, pre_drain2} = Host.fetch_run(run2.id)

        assert {:ok, {:discarded, :injected_event_failure}} =
                 Vehicle.drain(
                   lease2,
                   vehicle_opts({FailingEventsBackend, context}, drain_budget: [max_moments: 1])
                 )

        assert {:ok, after_drain2} = Host.fetch_run(run2.id)
        assert after_drain2 == pre_drain2
        assert_receive {:drain, %{committed_moments: 0}, %{outcome: :discarded, budget: nil}}
      end

      test "launcher/1 rejects malformed configuration before any vehicle launches", %{
        backend_ref: backend_ref
      } do
        valid = vehicle_opts(backend_ref, task_supervisor: @task_sup)

        assert is_function(Vehicle.launcher(valid), 1)

        assert is_function(
                 Vehicle.launcher(
                   Keyword.put(
                     valid,
                     :drain_budget,
                     max_moments: :infinity,
                     max_elapsed_ms: :infinity
                   )
                 ),
                 1
               )

        for budget <- [
              [max_moments: 0],
              [max_moments: -1],
              [max_moments: 1.5],
              [max_elapsed_ms: 0],
              [max_elapsed_ms: "1000"],
              [bogus: 1],
              %{max_moments: 1},
              :nonsense
            ] do
          assert_raise ArgumentError, fn ->
            Vehicle.launcher(Keyword.put(valid, :drain_budget, budget))
          end
        end

        assert_raise ArgumentError, fn ->
          Vehicle.launcher(Keyword.put(valid, :monotonic_clock, :not_a_fun))
        end

        assert_raise KeyError, fn ->
          Vehicle.launcher(Keyword.delete(valid, :task_supervisor))
        end
      end

      test "drain/2 validates the budget for direct callers", %{backend_ref: backend_ref} do
        {_run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})

        assert_raise ArgumentError, fn ->
          Vehicle.drain(lease, vehicle_opts(backend_ref, drain_budget: [max_moments: 0]))
        end
      end
    end

    describe "claim freshness heartbeat" do
      test "malformed heartbeat configuration raises at the launcher and in drain", %{
        backend_ref: backend_ref
      } do
        for bad <- [
              :strict,
              %{interval_ms: 5},
              [],
              [interval_ms: 0],
              [interval_ms: :fast],
              [interval_ms: 5, bogus: true],
              [bogus: true]
            ] do
          assert_raise ArgumentError, fn ->
            Vehicle.launcher(
              backend: backend_ref,
              task_supervisor: @task_sup,
              heartbeat: bad
            )
          end
        end

        {_run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})

        assert_raise ArgumentError, fn ->
          Vehicle.drain(lease, vehicle_opts(backend_ref, heartbeat: [interval_ms: -1]))
        end
      end

      test "an interval that cannot fit the lease TTL abandons the claim before execution", %{
        backend_ref: backend_ref,
        context: context
      } do
        {run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})

        assert {:ok, {:abandoned, :rescheduled, {:heartbeat_misaligned, details}}} =
                 Vehicle.drain(lease, vehicle_opts(backend_ref, heartbeat: [interval_ms: 30_000]))

        assert details == %{interval_ms: 30_000, orphan_ttl_ms: 60_000}

        assert {:ok, info} = MemoryBackend.inspect_run(context, :system, run.id)
        assert info.claimed_at == nil
        assert info.claim_attempts == 0
        assert info.claim_abandons == 1
        assert %DateTime{} = info.wake_at

        # Nothing executed: the committed run never advanced.
        assert {:ok, after_abandon} = Host.fetch_run(run.id)
        assert after_abandon.status == :running
        assert after_abandon.checkpoint_seq == lease.checkpoint_seq
      end

      test "an interval exactly at the alignment boundary drains normally", %{
        backend_ref: backend_ref
      } do
        {run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "ok"})

        assert {:ok, {:parked, :terminal}} =
                 Vehicle.drain(lease, vehicle_opts(backend_ref, heartbeat: [interval_ms: 20_000]))

        assert {:ok, done} = Host.fetch_run(run.id)
        assert done.status == :done
      end

      test "the heartbeat keeps a long-executing claim fresh", %{
        backend_ref: backend_ref,
        context: context
      } do
        test_pid = self()
        Process.register(test_pid, :vehicle_heartbeat_relay)

        {run, lease} = start_claimed!(backend_ref, Graphs.blocking(), %{})

        opts =
          vehicle_opts({RelayRunsBackend, context},
            heartbeat: [interval_ms: 10],
            context: %{coordinator: test_pid}
          )

        task = Task.Supervisor.async_nolink(@task_sup, fn -> Vehicle.drain(lease, opts) end)

        assert_receive {:blocked, node_pid, "blocker", 1}
        assert_receive {:refreshed, _heartbeat, :ok}
        assert_receive {:refreshed, _heartbeat, :ok}

        # The claim was stamped slightly in the future; keep consuming ticks
        # until one lands past it, proving claimed_at advances while the
        # node is still executing.
        advanced? =
          Enum.reduce_while(1..300, false, fn _tick, _acc ->
            assert_receive {:refreshed, _heartbeat, :ok}
            assert {:ok, info} = MemoryBackend.inspect_run(context, :system, run.id)

            if DateTime.compare(info.claimed_at, lease.claimed_at) == :gt do
              {:halt, true}
            else
              {:cont, false}
            end
          end)

        assert advanced?

        send(node_pid, :release)
        assert {:ok, {:parked, :terminal}} = Task.await(task)

        assert {:ok, done} = Host.fetch_run(run.id)
        assert done.status == :done
      end

      test "a heartbeat that loses its token stops the vehicle accepting the result", %{
        backend_ref: backend_ref,
        context: context
      } do
        test_pid = self()
        Process.register(test_pid, :vehicle_heartbeat_relay)

        {run, lease} =
          start_claimed!(backend_ref, Graphs.blocking(), %{})

        opts =
          vehicle_opts({RelayRunsBackend, context},
            heartbeat: [interval_ms: 10],
            context: %{coordinator: test_pid, notify: test_pid},
            checkpoint_observers: [RecordingObserver],
            task_supervisor: @task_sup
          )

        task = Task.Supervisor.async_nolink(@task_sup, fn -> Vehicle.drain(lease, opts) end)
        assert_receive {:blocked, node_pid, "blocker", 1}

        [stolen] = claim!(backend_ref, DateTime.add(DateTime.utc_now(), 120, :second))
        assert stolen.claim_token != lease.claim_token

        # The loss flag is written before the heartbeat exits, so its DOWN
        # guarantees the vehicle's pre-commit check observes the loss.
        assert_receive {:refreshed, heartbeat, {:error, :claim_lost}}
        monitor = Process.monitor(heartbeat)
        assert_receive {:DOWN, ^monitor, :process, ^heartbeat, _reason}

        send(node_pid, :release)
        assert {:ok, {:discarded, :claim_lost}} = Task.await(task)

        assert {:ok, current} = MemoryBackend.fetch_run(context, :system, run.id)
        assert current.checkpoint_seq == lease.checkpoint_seq
        refute_received {:observed, _type}

        assert {:ok, info} = MemoryBackend.inspect_run(context, :system, run.id)
        assert info.claimed_at == stolen.claimed_at
        assert info.claim_attempts == 2
      end

      test "two workers execute the same run and exactly one commits durable state", %{
        backend_ref: backend_ref,
        context: context
      } do
        test_pid = self()
        Process.register(test_pid, :vehicle_heartbeat_relay)

        {run, lease_a} =
          start_claimed!(backend_ref, Graphs.blocking(), %{})

        opts_a =
          vehicle_opts({RelayRunsBackend, context},
            heartbeat: [interval_ms: 10],
            context: %{coordinator: test_pid}
          )

        task_a = Task.Supervisor.async_nolink(@task_sup, fn -> Vehicle.drain(lease_a, opts_a) end)
        assert_receive {:blocked, node_a, "blocker", 1}

        [lease_b] = claim!(backend_ref, DateTime.add(DateTime.utc_now(), 120, :second))
        assert lease_b.claim_token != lease_a.claim_token

        opts_b = vehicle_opts(backend_ref, context: %{coordinator: test_pid})
        task_b = Task.Supervisor.async_nolink(@task_sup, fn -> Vehicle.drain(lease_b, opts_b) end)

        # The uncommitted superstep re-plans with identical task identity, so
        # the second worker announces the same attempt number.
        assert_receive {:blocked, node_b, "blocker", 1}
        assert node_b != node_a

        send(node_b, :release)
        assert {:ok, {:parked, :terminal}} = Task.await(task_b)

        # Wait for the heartbeat to exit before releasing the node: the loss
        # flag is written before the exit, so DOWN guarantees the vehicle's
        # pre-commit check observes it.
        assert_receive {:refreshed, heartbeat, {:error, :claim_lost}}
        monitor = Process.monitor(heartbeat)
        assert_receive {:DOWN, ^monitor, :process, ^heartbeat, _reason}

        send(node_a, :release)
        assert {:ok, {:discarded, :claim_lost}} = Task.await(task_a)

        assert {:ok, done} = MemoryBackend.fetch_run(context, :system, run.id)
        assert done.status == :done
        assert done.checkpoint_seq > lease_b.checkpoint_seq
      end

      test "persistent refresh failure discards the moment and releases the claim", %{
        backend_ref: backend_ref,
        context: context
      } do
        test_pid = self()
        Process.register(test_pid, :vehicle_heartbeat_relay)

        {run, lease} =
          start_claimed!(backend_ref, Graphs.blocking(), %{})

        opts =
          vehicle_opts({RaisingRefreshBackend, context},
            heartbeat: [interval_ms: 200],
            context: %{coordinator: test_pid},
            # Both racing first draws (heartbeat last_ok, drain budget
            # start) are 0 by design; the tick's staleness_remaining draws
            # 30_000 (a live attempt budget), and the failure check then
            # draws the sticky 100_000 and exhausts the 60s staleness
            # budget. Do not shorten the list - it reintroduces races.
            monotonic_clock: scripted_monotonic([0, 0, 30_000, 100_000])
          )

        task = Task.Supervisor.async_nolink(@task_sup, fn -> Vehicle.drain(lease, opts) end)
        assert_receive {:blocked, node_pid, "blocker", 1}

        assert_receive {:refresh_attempted, heartbeat}
        monitor = Process.monitor(heartbeat)
        assert_receive {:DOWN, ^monitor, :process, ^heartbeat, _reason}

        send(node_pid, :release)
        assert {:ok, {:discarded, :heartbeat_down}} = Task.await(task)

        assert {:ok, info} = MemoryBackend.inspect_run(context, :system, run.id)
        assert info.claimed_at == nil
        assert %DateTime{} = info.wake_at
      end

      test "a refresh call stuck past the staleness budget stops the drain", %{
        backend_ref: backend_ref,
        context: context
      } do
        test_pid = self()
        Process.register(test_pid, :vehicle_heartbeat_relay)

        {run, lease} = start_claimed!(backend_ref, Graphs.blocking(), %{})

        opts =
          vehicle_opts({StuckRefreshBackend, context},
            heartbeat: [interval_ms: 200],
            context: %{coordinator: test_pid},
            # Draw order: heartbeat arm (0), drain-budget start (0), the
            # tick's staleness_remaining (59_990 -> a 10ms attempt budget
            # for the stuck call), then the sticky exhaustion check.
            monotonic_clock: scripted_monotonic([0, 0, 59_990, 100_000])
          )

        task = Task.Supervisor.async_nolink(@task_sup, fn -> Vehicle.drain(lease, opts) end)
        assert_receive {:blocked, node_pid, "blocker", 1}

        assert_receive {:refresh_started, worker, heartbeat}
        worker_monitor = Process.monitor(worker)
        monitor = Process.monitor(heartbeat)

        # The stuck store call is killed at the staleness bound, and the
        # heartbeat gives up loudly (flag written before it exits).
        assert_receive {:DOWN, ^worker_monitor, :process, ^worker, _killed}
        assert_receive {:DOWN, ^monitor, :process, ^heartbeat, _reason}

        send(node_pid, :release)
        assert {:ok, {:discarded, :heartbeat_down}} = Task.await(task)

        assert {:ok, info} = MemoryBackend.inspect_run(context, :system, run.id)
        assert info.claimed_at == nil
        assert %DateTime{} = info.wake_at
      end

      test "the heartbeat dies with a killed vehicle", %{
        backend_ref: backend_ref,
        context: context
      } do
        test_pid = self()
        Process.register(test_pid, :vehicle_heartbeat_relay)

        {_run, lease} =
          start_claimed!(backend_ref, Graphs.blocking(), %{})

        opts =
          vehicle_opts({RelayRunsBackend, context},
            heartbeat: [interval_ms: 10],
            context: %{coordinator: test_pid},
            task_supervisor: @task_sup
          )

        assert {:ok, vehicle} = Vehicle.launch(lease, opts)

        assert_receive {:blocked, _node_pid, "blocker", 1}
        assert_receive {:refreshed, heartbeat, :ok}
        monitor = Process.monitor(heartbeat)

        Process.exit(vehicle, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^heartbeat, _reason}
      end

      test "a drain that raises still stops its heartbeat", %{
        backend_ref: backend_ref,
        context: context
      } do
        test_pid = self()
        Process.register(test_pid, :vehicle_heartbeat_relay)

        {_run, lease} = start_claimed!(backend_ref, Graphs.minimal_linear(), %{"value" => "x"})

        opts =
          vehicle_opts({BlockingDoneFetchBackend, context}, heartbeat: [interval_ms: 10])

        {vehicle, vehicle_monitor} = spawn_monitor(fn -> Vehicle.drain(lease, opts) end)

        assert_receive {:fetching, fetcher}
        assert_receive {:refreshed, heartbeat, :ok}
        heartbeat_monitor = Process.monitor(heartbeat)

        send(fetcher, :go)

        assert_receive {:DOWN, ^vehicle_monitor, :process, ^vehicle, {%Docket.Error{}, _stack}}
        assert_receive {:DOWN, ^heartbeat_monitor, :process, ^heartbeat, :killed}
      end

      test "strict alignment is the default and performs no refreshes", %{
        context: context
      } do
        Process.register(self(), :vehicle_heartbeat_relay)

        {:ok, reference} = Host.save_graph(Graphs.minimal_linear())
        {:ok, run} = Host.start_run(reference, %{"value" => "hello"})

        # A zero-TTL lease can never satisfy the heartbeat alignment rule,
        # so an accidentally armed heartbeat would abandon instead of
        # draining - strict mode must not care about the TTL at all.
        {:ok, %{leases: [lease], poisoned: []}} =
          MemoryBackend.claim_due(context, :system, %{
            now: DateTime.add(DateTime.utc_now(), 1, :second),
            limit: 1,
            orphan_ttl_ms: 0,
            max_claim_attempts: 3
          })

        assert lease.run_id == run.id

        assert {:ok, {:parked, :terminal}} =
                 Vehicle.drain(lease, vehicle_opts({RelayRunsBackend, context}))

        assert {:ok, done} = Host.fetch_run(run.id)
        assert done.status == :done
        refute_received {:refreshed, _heartbeat, _result}
      end
    end

    def telemetry_relay(event, _measurements, _metadata, %{pid: pid}) do
      send(pid, {:telemetry, event})
    end

    defp start_claimed!(backend_ref, graph, input, claim_now \\ nil) do
      {:ok, reference} = Host.save_graph(graph)
      {:ok, run} = Host.start_run(reference, input)

      claim_now = claim_now || DateTime.add(DateTime.utc_now(), 1, :second)
      [lease] = claim!(backend_ref, claim_now)
      assert lease.run_id == run.id

      {run, lease}
    end

    defp claim!({backend, context}, now, limit \\ 1) do
      {:ok, %{leases: leases, poisoned: []}} =
        backend.runs().claim_due(context, :system, %{
          now: now,
          limit: limit,
          orphan_ttl_ms: 60_000,
          max_claim_attempts: 3
        })

      leases
    end

    defp vehicle_opts(backend_ref, extra \\ []) do
      Keyword.merge([backend: backend_ref, graph_cache: false], extra)
    end

    defp counting_compiler(counter) do
      fn graph, opts ->
        Agent.update(counter, &(&1 + 1))
        Docket.Graph.Compiler.compile_effective_document(graph, opts)
      end
    end

    defp observer_opts do
      [
        checkpoint_observers: [RecordingObserver],
        task_supervisor: @task_sup,
        context: %{notify: self()}
      ]
    end
  end
end
