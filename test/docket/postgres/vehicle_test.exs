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
