if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.BackendTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres
    @moduletag capture_log: true

    alias Docket.Postgres.BackendTestRepo, as: TestRepo
    alias Docket.Postgres.BackendSandboxTestRepo, as: SandboxRepo
    alias Docket.Postgres.{AdmissionPhase, Dispatcher}
    alias Docket.Postgres.Schemas.{Event, GraphVersion, Run}
    alias Docket.Test.ConcurrentAdmissionHarness
    alias Docket.Test.Fixtures.Graphs

    @migration_version 20_260_711_000_025
    @v1_migration_version 20_260_719_000_081
    @wrong_shape_migration_version 20_260_719_000_082
    @pruner [
      interval_ms: :timer.hours(1),
      event_retention_ms: :timer.hours(24 * 30),
      run_retention_ms: :timer.hours(24 * 90),
      batch_size: 100
    ]

    defmodule InstallDocket do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule InstallDocketV1 do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(prefix: "docket_v1", version: 1)
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_v1", version: 1)
    end

    defmodule InstallDocketWrongShape do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(prefix: "docket_wrong_shape", version: 1)
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_wrong_shape", version: 1)
    end

    defmodule FixedClock do
      def now, do: ~U[2030-01-02 03:04:05.000000Z]
    end

    defmodule FailingObserver do
      @behaviour Docket.Checkpoint.Observer

      @impl true
      def observe(checkpoint, _context) do
        if pid = Process.whereis(:docket_backend_observer_relay) do
          send(pid, {:observer_called, checkpoint.type})
        end

        {:error, :injected_observer_failure}
      end
    end

    defmodule RelayObserver do
      @behaviour Docket.Checkpoint.Observer

      @impl true
      def observe(checkpoint, _context) do
        if pid = Process.whereis(:docket_testing_observer_relay) do
          send(pid, {:testing_observer_called, checkpoint.type})
        end

        :ok
      end
    end

    defmodule PollHost do
      @pruner [
        interval_ms: :timer.hours(1),
        event_retention_ms: :timer.hours(24 * 30),
        run_retention_ms: :timer.hours(24 * 90),
        batch_size: 100
      ]

      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        notifier: :none,
        dispatcher: [concurrency: 2, poll_interval_ms: 10],
        checkpoint_observers: [FailingObserver],
        pruner: @pruner
    end

    defmodule NotifyHost do
      @pruner [
        interval_ms: :timer.hours(1),
        event_retention_ms: :timer.hours(24 * 30),
        run_retention_ms: :timer.hours(24 * 90),
        batch_size: 100
      ]

      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        context: %{coordinator: :docket_backend_vehicle_relay},
        dispatcher: [concurrency: 1, poll_interval_ms: 60_000],
        pruner: @pruner
    end

    defmodule TenantHost do
      @pruner [
        interval_ms: :timer.hours(1),
        event_retention_ms: :timer.hours(24 * 30),
        run_retention_ms: :timer.hours(24 * 90),
        batch_size: 100
      ]

      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        tenant_mode: :required,
        claim_policy: [
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          default_max_active_runs: 1
        ],
        notifier: :none,
        dispatcher: [concurrency: 1, poll_interval_ms: 60_000],
        pruner: @pruner
    end

    defmodule PoisonHost do
      @pruner [
        interval_ms: :timer.hours(1),
        event_retention_ms: :timer.hours(24 * 30),
        run_retention_ms: :timer.hours(24 * 90),
        batch_size: 100
      ]

      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        notifier: :none,
        dispatcher: [concurrency: 1, poll_interval_ms: 60_000],
        pruner: @pruner
    end

    defmodule InlineHost do
      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        testing: :inline,
        notifier: :none
    end

    defmodule ManualHost do
      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        testing: :manual,
        notifier: :none
    end

    defmodule ConcurrentManualHost do
      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        testing: :manual,
        notifier: :none
    end

    defmodule ClockedManualHost do
      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        testing: :manual,
        clock: &FixedClock.now/0,
        notifier: :none
    end

    defmodule AlternatePolicyManualHost do
      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        testing: :manual,
        notifier: :none,
        claim_policy: [
          implementation: Docket.Test.AlternateClaimPolicy,
          marker: :manual_runtime
        ]
    end

    defmodule TenantFairConfigManualHost do
      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        testing: :manual,
        notifier: :none,
        claim_policy: [
          implementation: Docket.Test.TenantFairConfigClaimPolicy,
          default_max_active_runs: 4
        ]
    end

    defmodule TenantFairConfigSupervisedHost do
      @pruner [
        interval_ms: :timer.hours(1),
        event_retention_ms: :timer.hours(24 * 30),
        run_retention_ms: :timer.hours(24 * 90),
        batch_size: 100
      ]

      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        notifier: :none,
        dispatcher: [concurrency: 1, poll_interval_ms: 10],
        pruner: @pruner,
        claim_policy: [
          implementation: Docket.Test.TenantFairConfigClaimPolicy,
          default_max_active_runs: 4
        ]
    end

    defmodule SandboxInlineHost do
      use Docket,
        backend: Docket.Postgres,
        repo: SandboxRepo,
        prefix: "public",
        testing: :inline,
        notifier: :none
    end

    defmodule ObserverInlineHost do
      use Docket,
        backend: Docket.Postgres,
        repo: TestRepo,
        testing: :inline,
        notifier: :none,
        checkpoint_observers: [RelayObserver]
    end

    setup_all do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      sandbox_config = SandboxRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(sandbox_config)
      :ok = Ecto.Adapters.Postgres.storage_up(sandbox_config)
      start_supervised!(SandboxRepo)
      :ok = Ecto.Migrator.up(SandboxRepo, @migration_version, InstallDocket, log: false)
      :ok
    end

    setup do
      stop_host(PollHost)
      stop_host(NotifyHost)
      stop_host(TenantHost)
      stop_host(PoisonHost)
      stop_host(InlineHost)
      stop_host(ManualHost)
      stop_host(ConcurrentManualHost)
      stop_host(ClockedManualHost)
      stop_host(AlternatePolicyManualHost)
      stop_host(TenantFairConfigManualHost)
      stop_host(TenantFairConfigSupervisedHost)
      stop_host(SandboxInlineHost)
      stop_host(ObserverInlineHost)

      TestRepo.delete_all(Event)
      TestRepo.delete_all(Run)

      TestRepo.query!("""
      UPDATE docket_claim_policy
      SET admission_mode = 'legacy', max_active = NULL, configured_max_active = NULL,
          policy_version = 0, scan_ring_position = 0, initialized_at = NULL,
          updated_at = CURRENT_TIMESTAMP
      WHERE id = 1
      """)

      TestRepo.delete_all(GraphVersion)
      Docket.Postgres.GraphCache.clear()
      on_exit(&Docket.Postgres.GraphCache.clear/0)
      :ok
    end

    test "inline testing drains in the caller without background backend children" do
      start_supervised!(InlineHost)
      backend_name = Module.concat(InlineHost, Backend)

      refute Process.whereis(Docket.Postgres.runner_name(backend_name))
      refute Process.whereis(Docket.Postgres.dispatcher_name(backend_name))
      refute Process.whereis(Docket.Postgres.vehicle_supervisor_name(backend_name))
      refute Process.whereis(Docket.Postgres.notifier_name(backend_name))
      refute Process.whereis(Docket.Postgres.pruner_name(backend_name))

      assert {:ok, reference} = InlineHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %Docket.Run{status: :done}} =
               InlineHost.start_run(reference, %{"value" => "inline"})
    end

    test "inline drained moments deliver configured checkpoint observers" do
      Process.register(self(), :docket_testing_observer_relay)
      start_supervised!(ObserverInlineHost)
      assert {:ok, reference} = ObserverInlineHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %Docket.Run{status: :done}} =
               ObserverInlineHost.start_run(reference, %{"value" => "observed"})

      assert_receive {:testing_observer_called, :run_initialized}
      assert_receive {:testing_observer_called, :run_completed}
    end

    test "inline testing completes a named interrupt through the durable facade" do
      start_supervised!(InlineHost)
      assert {:ok, reference} = InlineHost.save_graph(Graphs.interrupt_review())
      assert {:ok, %Docket.Run{status: :waiting} = waiting} = InlineHost.start_run(reference, %{})
      assert [{interrupt_id, %{status: :open}}] = Map.to_list(waiting.interrupts)

      assert {:ok, %Docket.Run{status: :done} = done} =
               InlineHost.resolve_interrupt(waiting.id, interrupt_id, "approved")

      assert {:ok, %Docket.RunInfo{run: ^done}} = InlineHost.inspect_run(waiting.id)

      assert {:error, %Docket.Error{type: :inactive_run}} =
               InlineHost.resolve_interrupt(waiting.id, interrupt_id, "again")
    end

    test "SQL Sandbox owner completes inline named interrupt flow in the caller" do
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(SandboxRepo)
      start_supervised!(SandboxInlineHost)
      assert {:ok, reference} = SandboxInlineHost.save_graph(Graphs.interrupt_review())

      assert {:ok, %Docket.Run{status: :waiting} = waiting} =
               SandboxInlineHost.start_run(reference, %{})

      assert [{interrupt_id, %{status: :open}}] = Map.to_list(waiting.interrupts)

      assert {:ok, %Docket.Run{status: :done}} =
               SandboxInlineHost.resolve_interrupt(waiting.id, interrupt_id, "sandbox-approved")
    end

    test "SQL Sandbox testing startup fails closed against a missing schema" do
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(SandboxRepo)

      opts = [
        name: __MODULE__.MissingSandboxSchema,
        repo: SandboxRepo,
        prefix: "docket_missing_schema",
        testing: :manual,
        notifier: :none
      ]

      context = Docket.Postgres.context(opts)

      assert_raise ArgumentError, ~r/requires schema version 2, found 0/, fn ->
        Docket.Postgres.init({opts, context})
      end
    end

    test "manual testing advances only through bounded drain_runs" do
      start_supervised!(ManualHost)
      assert {:ok, reference} = ManualHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %Docket.Run{status: :running} = started} =
               ManualHost.start_run(reference, %{"value" => "manual"})

      assert {:ok,
              %{
                drained: 1,
                poisoned: [],
                outcomes: [{:ok, {:parked, :terminal}}],
                limit_reached: true
              }} = ManualHost.drain_runs(max_runs: 1)

      assert {:ok, %Docket.Run{status: :done}} = ManualHost.fetch_run(started.id)

      assert {:ok, %{drained: 0, poisoned: [], outcomes: [], limit_reached: false}} =
               ManualHost.drain_runs(max_runs: 10)
    end

    test "independent manual runtimes drain through distinct checked-out connections" do
      start_supervised!(ManualHost)
      start_supervised!(ConcurrentManualHost)
      assert {:ok, reference} = ManualHost.save_graph(Graphs.minimal_linear())
      assert {:ok, first} = ManualHost.start_run(reference, %{"value" => "first"})
      assert {:ok, second} = ManualHost.start_run(reference, %{"value" => "second"})

      results =
        ConcurrentAdmissionHarness.run_callers!(TestRepo, [
          {:manual_one, fn -> ManualHost.drain_runs(max_runs: 1) end},
          {:manual_two, fn -> ConcurrentManualHost.drain_runs(max_runs: 1) end}
        ])

      assert results |> Enum.map(& &1.backend_pid) |> Enum.uniq() |> length() == 2

      for %{result: result} <- results do
        assert {:ok, %{drained: 1, poisoned: [], limit_reached: true}} = result
      end

      assert {:ok, %{status: :done}} = ManualHost.fetch_run(first.id)
      assert {:ok, %{status: :done}} = ManualHost.fetch_run(second.id)
    end

    test "supervised dispatchers claim through distinct checked-out PostgreSQL connections" do
      start_supervised!(ManualHost)
      assert {:ok, reference} = ManualHost.save_graph(Graphs.minimal_linear())
      assert {:ok, _first} = ManualHost.start_run(reference, %{"value" => "first"})
      assert {:ok, _second} = ManualHost.start_run(reference, %{"value" => "second"})

      parent = self()
      barrier = make_ref()

      first_phase =
        start_supervised!(%{id: make_ref(), start: {AdmissionPhase, :start_link, [[]]}})

      second_phase =
        start_supervised!(%{id: make_ref(), start: {AdmissionPhase, :start_link, [[]]}})

      launch = fn name ->
        fn lease ->
          vehicle = spawn(fn -> receive(do: (:stop -> :ok)) end)
          send(parent, {:launched, name, lease, vehicle})
          {:ok, vehicle}
        end
      end

      dispatchers =
        for {name, phase, registered_name} <- [
              {:first, first_phase, Module.concat(__MODULE__, FirstPinnedDispatcher)},
              {:second, second_phase, Module.concat(__MODULE__, SecondPinnedDispatcher)}
            ] do
          context =
            Docket.Postgres.TestAdmissionContext.resolve(TestRepo, %{
              admission_phase: phase,
              concurrent_admission_probe: %{
                owner: parent,
                ref: barrier,
                name: name,
                timeout: 5_000
              }
            })

          start_supervised!(
            {Dispatcher,
             name: registered_name,
             context: context,
             run_store: ConcurrentAdmissionHarness.PinnedRunStore,
             concurrency: 1,
             poll_interval_ms: 60_000,
             orphan_ttl_ms: 30_000,
             max_claim_attempts: 3,
             drain_timeout_ms: 0,
             launch: launch.(name),
             clock: &DateTime.utc_now/0,
             jitter: fn interval -> interval end}
          )

          registered_name
        end

      checked_out =
        for _ <- 1..2 do
          assert_receive {ConcurrentAdmissionHarness.PinnedRunStore, ^barrier, :checked_out, name,
                          worker, backend_pid},
                         1_000

          %{name: name, worker: worker, backend_pid: backend_pid}
        end

      assert Enum.sort(Enum.map(checked_out, & &1.name)) == [:first, :second]
      assert checked_out |> Enum.map(& &1.worker) |> Enum.uniq() |> length() == 2
      assert checked_out |> Enum.map(& &1.backend_pid) |> Enum.uniq() |> length() == 2

      Enum.each(checked_out, fn participant ->
        send(participant.worker, {ConcurrentAdmissionHarness.PinnedRunStore, barrier, :go})
      end)

      launched =
        for _ <- 1..2 do
          assert_receive {:launched, name, lease, vehicle}, 2_000
          %{name: name, lease: lease, vehicle: vehicle}
        end

      assert Enum.sort(Enum.map(launched, & &1.name)) == [:first, :second]
      assert launched |> Enum.map(& &1.lease.run_id) |> Enum.uniq() |> length() == 2

      Enum.each(dispatchers, fn dispatcher -> assert :ok = stop_supervised(dispatcher) end)
      Enum.each(launched, &send(&1.vehicle, :stop))
    end

    test "one instance clock governs facade timestamps and manual claims" do
      start_supervised!(ClockedManualHost)
      assert {:ok, reference} = ClockedManualHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %Docket.Run{status: :running, started_at: started_at} = started} =
               ClockedManualHost.start_run(reference, %{"value" => "clocked"})

      assert started_at == FixedClock.now()

      per_call_clock = fn -> ~U[2020-01-01 00:00:00.000000Z] end

      assert {:ok, %{drained: 1}} =
               ClockedManualHost.drain_runs(max_runs: 1, clock: per_call_clock)

      assert {:ok, %Docket.Run{status: :done, updated_at: updated_at}} =
               ClockedManualHost.fetch_run(started.id)

      assert updated_at == FixedClock.now()
    end

    test "manual drain uses the instance-selected ClaimPolicy and ignores per-call switches" do
      Process.register(self(), :docket_claim_policy_relay)

      on_exit(fn ->
        if Process.whereis(:docket_claim_policy_relay),
          do: Process.unregister(:docket_claim_policy_relay)
      end)

      start_supervised!(AlternatePolicyManualHost)

      assert_receive {:alternate_claim_policy, :init, :manual_runtime,
                      %{prefix: "public", identifiers: %{runs: ~s("public"."docket_runs")}}}

      assert {:ok, reference} = AlternatePolicyManualHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %{status: :running}} =
               AlternatePolicyManualHost.start_run(reference, %{"value" => "alternate"})

      handler = "alternate-manual-policy-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :claim_policy, :admission],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok, %{drained: 1}} =
               AlternatePolicyManualHost.drain_runs(
                 max_runs: 1,
                 claim_policy: [implementation: Docket.Postgres.ClaimPolicy.Legacy]
               )

      assert_receive {[:docket, :postgres, :claim_policy, :admission], %{demand: 1},
                      %{
                        implementation: Docket.Test.AlternateClaimPolicy,
                        result: :ok
                      }}

      assert_receive {:alternate_claim_policy, :build_plan, :manual_runtime, _pid}
      assert_receive {:alternate_claim_policy, :decode, :manual_runtime, _pid}
      refute_receive {:alternate_claim_policy, :init, :manual_runtime, _context}
    end

    test "manual drain preserves the one normalized TenantFair configuration value" do
      Process.register(self(), :docket_claim_policy_relay)

      on_exit(fn ->
        if Process.whereis(:docket_claim_policy_relay),
          do: Process.unregister(:docket_claim_policy_relay)
      end)

      expected = %Docket.Postgres.ClaimPolicy.TenantFair.Config{
        default_max_active_runs: 4
      }

      start_supervised!(TenantFairConfigManualHost)

      assert_receive {:tenant_fair_config_claim_policy, :init, ^expected,
                      %{prefix: "public", identifiers: %{runs: ~s("public"."docket_runs")}}}

      assert {:ok, reference} = TenantFairConfigManualHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %{status: :running}} =
               TenantFairConfigManualHost.start_run(reference, %{"value" => "config"})

      assert {:ok, %{drained: 1}} =
               TenantFairConfigManualHost.drain_runs(
                 max_runs: 1,
                 claim_policy: [implementation: Docket.Postgres.ClaimPolicy.Legacy]
               )

      assert_receive {:tenant_fair_config_claim_policy, :build_plan, ^expected, _pid}
      assert_receive {:tenant_fair_config_claim_policy, :decode, ^expected, _pid}
      refute_receive {:tenant_fair_config_claim_policy, :init, _, _}
    end

    test "supervised dispatch preserves the one normalized TenantFair configuration value" do
      Process.register(self(), :docket_claim_policy_relay)

      on_exit(fn ->
        if Process.whereis(:docket_claim_policy_relay),
          do: Process.unregister(:docket_claim_policy_relay)
      end)

      expected = %Docket.Postgres.ClaimPolicy.TenantFair.Config{
        default_max_active_runs: 4
      }

      start_supervised!(TenantFairConfigSupervisedHost)

      assert_receive {:tenant_fair_config_claim_policy, :init, ^expected,
                      %{prefix: "public", identifiers: %{runs: ~s("public"."docket_runs")}}}

      assert {:ok, reference} =
               TenantFairConfigSupervisedHost.save_graph(Graphs.minimal_linear())

      assert {:ok, started} =
               TenantFairConfigSupervisedHost.start_run(reference, %{"value" => "config"})

      assert {:ok, %Docket.Run{status: :done}} =
               TenantFairConfigSupervisedHost.await_run(started.id, timeout: 5_000)

      assert_receive {:tenant_fair_config_claim_policy, :build_plan, ^expected, _pid}
      assert_receive {:tenant_fair_config_claim_policy, :decode, ^expected, _pid}
      refute_receive {:tenant_fair_config_claim_policy, :init, _, _}
    end

    test "manual drains preserve retry attempt state across separate claims" do
      start_supervised!(ManualHost)
      assert {:ok, reference} = ManualHost.save_graph(Graphs.retry_then_continue())
      assert {:ok, started} = ManualHost.start_run(reference, %{})

      handler = "manual-admission-phase-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :run_store, :claim],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok, %{drained: 1, limit_reached: true}} = ManualHost.drain_runs(max_runs: 1)

      assert_receive {[:docket, :postgres, :run_store, :claim], _, %{preference: :ready}}

      assert {:ok, %Docket.Run{status: :running, active_tasks: first_tasks}} =
               ManualHost.fetch_run(started.id)

      assert first_tasks != %{}
      assert {:ok, %{drained: 1, limit_reached: true}} = ManualHost.drain_runs(max_runs: 1)

      assert_receive {[:docket, :postgres, :run_store, :claim], _, %{preference: :expired}}

      assert {:ok, %Docket.Run{status: :running, active_tasks: second_tasks}} =
               ManualHost.fetch_run(started.id)

      assert second_tasks != %{}
      assert second_tasks != first_tasks
      assert {:ok, %{drained: 1}} = ManualHost.drain_runs(max_runs: 1)

      assert_receive {[:docket, :postgres, :run_store, :claim], _, %{preference: :ready}}

      assert {:ok, %Docket.Run{status: :done, active_tasks: %{}}} =
               ManualHost.fetch_run(started.id)
    end

    test "poison admission consumes a bounded slot and drain continues to later work" do
      start_supervised!(ManualHost)
      assert {:ok, reference} = ManualHost.save_graph(Graphs.minimal_linear())
      assert {:ok, poison} = ManualHost.start_run(reference, %{"value" => "poison"})
      assert {:ok, normal} = ManualHost.start_run(reference, %{"value" => "normal"})
      poison_at = DateTime.add(DateTime.utc_now(), -1, :second)

      TestRepo.update_all(
        from(row in Run, where: row.run_id == ^poison.id),
        set: [wake_at: poison_at, claim_attempts: 5]
      )

      assert {:ok, %{drained: 1, poisoned: [%{run_id: poison_id}], limit_reached: true}} =
               ManualHost.drain_runs(max_runs: 2)

      assert poison_id == poison.id
      assert {:ok, %Docket.RunInfo{poisoned_at: %DateTime{}}} = ManualHost.inspect_run(poison.id)
      assert {:ok, %Docket.Run{status: :done}} = ManualHost.fetch_run(normal.id)
    end

    test "manual cancel, poison halt, inspection, and recovery are deterministic" do
      start_supervised!(ManualHost)
      assert {:ok, reference} = ManualHost.save_graph(Graphs.minimal_linear())
      assert {:ok, cancellable} = ManualHost.start_run(reference, %{"value" => "cancel"})
      assert {:ok, %Docket.Run{status: :cancelled}} = ManualHost.cancel_run(cancellable.id)

      assert {:ok, %Docket.RunInfo{run: %Docket.Run{status: :cancelled}}} =
               ManualHost.inspect_run(cancellable.id)

      assert {:ok, poisoned} = ManualHost.start_run(reference, %{"value" => "recover"})
      now = DateTime.utc_now()

      TestRepo.update_all(
        from(row in Run, where: row.run_id == ^poisoned.id),
        set: [wake_at: nil, poisoned_at: now, poison_reason: "manual_test"]
      )

      assert {:error, {:poisoned, %Docket.RunInfo{}}} =
               ManualHost.await_run(poisoned.id, timeout: 0)

      assert {:ok, %Docket.Run{status: :running}} = ManualHost.retry_poisoned_run(poisoned.id)
      assert {:ok, %{drained: 1}} = ManualHost.drain_runs(max_runs: 2)
      assert {:ok, %Docket.Run{status: :done}} = ManualHost.fetch_run(poisoned.id)
    end

    test "testing mode is instance-owned and cannot activate draining on production" do
      start_supervised!(ManualHost)
      start_supervised!(PollHost)
      assert {:ok, reference} = ManualHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %Docket.Run{status: :running}} =
               ManualHost.start_run(reference, %{"value" => "manual"}, testing: :inline)

      assert {:error, :testing_mode_required} = PollHost.drain_runs(testing: :manual)
    end

    test "the bundle fixes every capability and assembles poll-only execution plus pruning" do
      Process.register(self(), :docket_backend_observer_relay)

      assert Code.ensure_loaded?(Docket.Postgres)
      assert function_exported?(Docket.Postgres, :transaction, 2)
      refute function_exported?(Docket.Postgres, :storage, 0)
      assert Docket.Postgres.graphs() == Docket.Postgres.GraphStore
      assert Docket.Postgres.runs() == Docket.Postgres.RunStore
      assert Docket.Postgres.events() == Docket.Postgres.EventStore

      start_supervised!(PollHost)

      backend_name = Module.concat(PollHost, Backend)
      assert Process.whereis(Docket.Postgres.runner_name(backend_name))
      assert Process.whereis(Docket.Postgres.admission_phase_name(backend_name))
      assert Process.whereis(Docket.Postgres.vehicle_supervisor_name(backend_name))
      assert Process.whereis(Docket.Postgres.dispatcher_name(backend_name))
      assert Process.whereis(Docket.Postgres.pruner_name(backend_name))
      refute Process.whereis(Docket.Postgres.notifier_name(backend_name))

      assert {:ok, reference} = PollHost.save_graph(Graphs.minimal_linear())

      assert {:ok, started} =
               PollHost.start_run(reference, %{"value" => "postgres"}, context: %{notify: self()})

      assert %Docket.Run{} = started
      assert {:ok, %Docket.Run{} = fetched} = PollHost.fetch_run(started.id)
      assert fetched.id == started.id
      assert {:ok, %Docket.RunInfo{run: %Docket.Run{id: id}}} = PollHost.inspect_run(started.id)
      assert id == started.id

      assert {:ok, %Docket.Run{status: :done} = done} =
               PollHost.await_run(started.id, timeout: 5_000)

      assert {:ok, ^done} = PollHost.fetch_run(started.id)

      assert_receive {:observer_called, :run_initialized}
      assert_receive {:observer_called, :run_completed}
    end

    test "the notifier is an isolated fast path and dispatcher restart replaces its vehicles" do
      Process.register(self(), :docket_backend_vehicle_relay)
      start_supervised!(NotifyHost)
      backend_name = Module.concat(NotifyHost, Backend)

      runner = Process.whereis(Docket.Postgres.runner_name(backend_name))
      notifier = Process.whereis(Docket.Postgres.notifier_name(backend_name))
      dispatcher = Process.whereis(Docket.Postgres.dispatcher_name(backend_name))
      vehicle_supervisor = Process.whereis(Docket.Postgres.vehicle_supervisor_name(backend_name))

      assert is_pid(runner) and is_pid(notifier) and is_pid(dispatcher) and
               is_pid(vehicle_supervisor)

      Process.exit(notifier, :kill)

      replacement_notifier =
        await_replacement(Docket.Postgres.notifier_name(backend_name), notifier)

      assert is_pid(replacement_notifier)
      assert Process.whereis(Docket.Postgres.runner_name(backend_name)) == runner
      assert Process.whereis(Docket.Postgres.dispatcher_name(backend_name)) == dispatcher

      assert {:ok, reference} = NotifyHost.save_graph(Graphs.blocking())
      started_at = System.monotonic_time(:millisecond)
      assert {:ok, _run} = NotifyHost.start_run(reference, %{})
      assert_receive {:blocked, node_pid, "blocker", 1}, 5_000
      assert System.monotonic_time(:millisecond) - started_at < 5_000

      assert [vehicle] = Task.Supervisor.children(vehicle_supervisor) -- [node_pid]
      vehicle_monitor = Process.monitor(vehicle)
      node_monitor = Process.monitor(node_pid)

      Process.exit(dispatcher, :kill)
      assert_receive {:DOWN, ^vehicle_monitor, :process, ^vehicle, _reason}, 5_000
      assert_receive {:DOWN, ^node_monitor, :process, ^node_pid, _reason}, 5_000
      assert is_pid(await_replacement(Docket.Postgres.dispatcher_name(backend_name), dispatcher))

      assert is_pid(
               await_replacement(
                 Docket.Postgres.vehicle_supervisor_name(backend_name),
                 vehicle_supervisor
               )
             )
    end

    test "tenantless, required tenants, and system scope cannot cross through the facade" do
      start_supervised!(PollHost)
      start_supervised!(TenantHost)

      assert {:ok, reference} = PollHost.save_graph(Graphs.minimal_linear())
      assert {:ok, %Docket.Graph{}} = PollHost.fetch_graph(reference)

      assert {:error, %Docket.Error{type: :invalid_tenant}} = TenantHost.fetch_graph(reference)
      assert {:error, :not_found} = TenantHost.fetch_graph(reference, tenant_id: "a")

      assert {:ok, tenantless} = PollHost.start_run(reference, %{"value" => "none"})

      assert {:error, :not_found} = TenantHost.fetch_run(tenantless.id, tenant_id: "a")

      assert {:error, :not_found} =
               TenantHost.start_run(reference, %{"value" => "a"}, tenant_id: "a")

      assert {:ok, ^reference} =
               TenantHost.save_graph(Graphs.minimal_linear(), tenant_id: "a")

      assert {:ok, tenant_a} =
               TenantHost.start_run(reference, %{"value" => "a"}, tenant_id: "a")

      assert {:error, :not_found} = TenantHost.fetch_run(tenant_a.id, tenant_id: "b")
      assert {:error, :not_found} = PollHost.fetch_run(tenant_a.id)

      assert {:ok, tenantless_page} = PollHost.list_runs()
      assert Enum.any?(tenantless_page.runs, &(&1.id == tenantless.id))
      refute Enum.any?(tenantless_page.runs, &(&1.id == tenant_a.id))

      assert {:ok, tenant_page} = TenantHost.list_runs(tenant_id: "a")
      assert Enum.map(tenant_page.runs, & &1.id) == [tenant_a.id]

      assert {:ok, %Docket.RunSummary{id: tenant_a_id}} =
               TenantHost.fetch_latest_run(tenant_id: "a")

      assert tenant_a_id == tenant_a.id
      assert {:ok, %Docket.RunPage{runs: []}} = TenantHost.list_runs(tenant_id: "b")

      assert {:ok, %Docket.Event{seq: 1}} = TenantHost.fetch_event(tenant_a.id, 1, tenant_id: "a")

      assert {:ok, %Docket.Event{}} =
               TenantHost.fetch_latest_event(tenant_a.id, tenant_id: "a")

      assert {:error, :not_found} =
               TenantHost.fetch_latest_event(tenant_a.id, tenant_id: "b")

      assert {:error, %Docket.Error{type: :invalid_tenant}} =
               TenantHost.fetch_run(tenant_a.id,
                 tenant_mode: :none,
                 backend_context: %{repo: TestRepo}
               )

      assert {:ok, %Docket.Run{id: id}} =
               Docket.Postgres.RunStore.fetch_run(TestRepo, :system, tenant_a.id)

      assert id == tenant_a.id
    end

    test "poisoned await returns RunInfo and retry is the separate operational command" do
      start_supervised!(PoisonHost)
      Process.sleep(50)

      assert {:ok, reference} = PoisonHost.save_graph(Graphs.minimal_linear())
      assert {:ok, run} = PoisonHost.start_run(reference, %{"value" => "poison"})
      now = DateTime.utc_now()

      TestRepo.update_all(
        from(row in Run, where: row.run_id == ^run.id),
        set: [wake_at: nil, poisoned_at: now, poison_reason: "test_poison"]
      )

      assert {:error, {:poisoned, %Docket.RunInfo{run: %Docket.Run{id: id}}}} =
               PoisonHost.await_run(run.id, timeout: 1_000)

      assert id == run.id
      assert {:ok, %Docket.Run{id: ^id}} = PoisonHost.retry_poisoned_run(run.id)

      assert {:ok, %Docket.RunInfo{poisoned_at: nil, poison_reason: nil}} =
               PoisonHost.inspect_run(run.id)
    end

    test "configuration is explicit and fails before partial backend operation" do
      opts = [name: __MODULE__.ExplicitContext, repo: TestRepo]
      context = Docket.Postgres.context(opts)

      assert %{start: {Docket.Postgres, :start_link, [^opts, ^context]}} =
               Docket.Postgres.child_spec(opts, context)

      assert_raise ArgumentError, ~r/requires a resolved backend context/, fn ->
        Docket.Postgres.child_spec(opts)
      end

      assert_raise KeyError, fn ->
        Docket.Postgres.context([])
      end

      assert_raise ArgumentError, ~r/Postgres context prefix/, fn ->
        Docket.Postgres.context(repo: TestRepo, prefix: "Bad-Prefix")
      end

      assert_raise ArgumentError, ~r/:pruner must be a keyword list/, fn ->
        postgres_init(name: __MODULE__.MissingPruner, repo: TestRepo)
      end

      assert_raise ArgumentError, ~r/:pruner requires/, fn ->
        postgres_init(name: __MODULE__.PartialPruner, repo: TestRepo, pruner: [])
      end

      assert_raise ArgumentError, ~r/:notifier must be :none or a keyword list/, fn ->
        postgres_init(
          name: __MODULE__.BadNotifier,
          repo: TestRepo,
          notifier: false,
          pruner: @pruner
        )
      end

      assert_raise ArgumentError, ~r/:dispatcher has unknown keys/, fn ->
        postgres_init(
          name: __MODULE__.MixedStore,
          repo: TestRepo,
          dispatcher: [run_store: SomeOtherStore],
          pruner: @pruner
        )
      end

      assert_raise ArgumentError, ~r/:clock is a testing-only option/, fn ->
        postgres_init(
          name: __MODULE__.ProductionClock,
          repo: TestRepo,
          clock: &FixedClock.now/0
        )
      end

      for {key, value} <- [
            dispatcher: [clock: &FixedClock.now/0],
            vehicle: [clock: &FixedClock.now/0],
            pruner: [clock: &FixedClock.now/0]
          ] do
        assert_raise ArgumentError, ~r/:clock is instance-owned/, fn ->
          opts =
            [name: __MODULE__.NestedClock, repo: TestRepo, testing: :manual]
            |> Keyword.put(key, value)

          postgres_init(opts)
        end
      end

      assert_raise ArgumentError, ~r/:vehicle has unknown keys.*executor/, fn ->
        postgres_init(
          name: __MODULE__.NestedVehicleExecution,
          repo: TestRepo,
          vehicle: [executor: Docket.Executor.Local],
          pruner: @pruner
        )
      end

      assert_raise ArgumentError, ~r/must be configured under :vehicle.*drain_budget/, fn ->
        postgres_init(
          name: __MODULE__.TopLevelVehicleMechanic,
          repo: TestRepo,
          drain_budget: [max_moments: 1, max_elapsed_ms: 3_000],
          pruner: @pruner
        )
      end

      assert_raise ArgumentError,
                   ~r/tenant_mode :required requires the TenantFair claim policy/,
                   fn ->
                     postgres_init(
                       name: __MODULE__.TenantLegacyPolicy,
                       repo: TestRepo,
                       tenant_mode: :required,
                       notifier: :none,
                       pruner: @pruner
                     )
                   end
    end

    test "startup fails closed against an older schema" do
      :ok =
        Ecto.Migrator.up(TestRepo, @v1_migration_version, InstallDocketV1,
          log: false,
          migration_lock: false
        )

      opts = [
        name: __MODULE__.OldSchema,
        repo: TestRepo,
        prefix: "docket_v1",
        testing: :manual,
        notifier: :none
      ]

      context = Docket.Postgres.context(opts)

      assert_raise ArgumentError, ~r/requires schema version 2, found 1/, fn ->
        Docket.Postgres.init({opts, context})
      end
    end

    test "startup fails closed when the version marker masks an obsolete schema shape" do
      :ok =
        Ecto.Migrator.up(TestRepo, @wrong_shape_migration_version, InstallDocketWrongShape,
          log: false,
          migration_lock: false
        )

      TestRepo.query!(
        ~s(COMMENT ON TABLE "docket_wrong_shape"."docket_runs" IS '2'),
        [],
        log: false
      )

      opts = [
        name: __MODULE__.WrongShape,
        repo: TestRepo,
        prefix: "docket_wrong_shape",
        testing: :manual,
        notifier: :none
      ]

      context = Docket.Postgres.context(opts)

      assert_raise ArgumentError, ~r/structure does not match the current Docket schema/, fn ->
        Docket.Postgres.init({opts, context})
      end
    end

    test "TenantFair startup idempotently persists configured defaults before children" do
      opts = [
        name: __MODULE__.TenantFairStartup,
        repo: TestRepo,
        tenant_mode: :required,
        claim_policy: [
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          default_max_active_runs: 2
        ],
        testing: :manual,
        notifier: :none
      ]

      TestRepo.query!("UPDATE docket_claim_policy SET configured_max_active = 2 WHERE id = 1")

      assert {:ok, _supervisor_spec} = postgres_init(opts)

      assert [["tenant_fair", 2, 2, 1, initialized_at]] = policy_configuration()
      assert %DateTime{} = initialized_at

      assert {:ok, %{version: 1}} =
               Docket.Postgres.ClaimPolicy.Admin.put_override(
                 Docket.Postgres.context(repo: TestRepo),
                 {:tenant, "preserved"},
                 1,
                 expected_version: 0
               )

      assert {:ok, %{max_active_runs: 3, version: 2}} =
               Docket.Postgres.ClaimPolicy.Admin.put_default(
                 Docket.Postgres.context(repo: TestRepo),
                 3,
                 expected_version: 1
               )

      assert {:ok, _supervisor_spec} = postgres_init(opts)
      assert [["tenant_fair", 3, 2, 2, ^initialized_at]] = policy_configuration()

      changed_opts = put_in(opts, [:claim_policy, :default_max_active_runs], 4)
      assert {:ok, _supervisor_spec} = postgres_init(changed_opts)
      assert [["tenant_fair", 4, 4, 3, ^initialized_at]] = policy_configuration()

      assert [["preserved", 1, 1]] =
               TestRepo.query!(
                 "SELECT scope_key, max_active, partition_version " <>
                   "FROM docket_claim_partitions WHERE scope_key = 'preserved'"
               ).rows
    end

    defp postgres_init(opts) do
      Docket.Postgres.init({opts, Docket.Postgres.context(opts)})
    end

    defp policy_configuration do
      TestRepo.query!(
        "SELECT admission_mode, max_active, configured_max_active, policy_version, " <>
          "initialized_at " <>
          "FROM docket_claim_policy WHERE id = 1"
      ).rows
    end

    test "storage remains exactly the current versioned schema" do
      tables =
        TestRepo.query!(
          "SELECT table_name FROM information_schema.tables " <>
            "WHERE table_schema = current_schema() AND table_name LIKE 'docket_%' " <>
            "ORDER BY table_name",
          [],
          log: false
        ).rows
        |> List.flatten()

      assert tables ==
               ~w(
                 docket_claim_partitions
                 docket_claim_policy
                 docket_claim_schedule
                 docket_events
                 docket_graph_versions
                 docket_runs
               )

      assert %{rows: [[comment]]} =
               TestRepo.query!(
                 "SELECT obj_description('docket_runs'::regclass, 'pg_class')",
                 [],
                 log: false
               )

      assert comment == Integer.to_string(Docket.Postgres.Migration.current_version())
    end

    defp await_replacement(name, old_pid, attempts \\ 100)

    defp await_replacement(_name, _old_pid, 0), do: nil

    defp await_replacement(name, old_pid, attempts) do
      case Process.whereis(name) do
        pid when is_pid(pid) and pid != old_pid ->
          pid

        _ ->
          Process.sleep(10)
          await_replacement(name, old_pid, attempts - 1)
      end
    end

    defp stop_host(host) do
      case Process.whereis(host) do
        nil -> :ok
        pid -> Supervisor.stop(pid)
      end
    end
  end
end
