if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.BackendTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres

    alias Docket.Postgres.BackendTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.{Event, GraphVersion, Run}
    alias Docket.Test.Fixtures.Graphs

    @migration_version 20_260_711_000_025
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

    defmodule LegacyCheckpoint do
      @behaviour Docket.Checkpoint

      @impl true
      def handle(_checkpoint, _context), do: raise("durable path called legacy checkpoint")
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
        checkpoint: LegacyCheckpoint,
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

    setup_all do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      :ok
    end

    setup do
      stop_host(PollHost)
      stop_host(NotifyHost)
      stop_host(TenantHost)
      stop_host(PoisonHost)

      TestRepo.delete_all(Event)
      TestRepo.delete_all(Run)
      TestRepo.delete_all(GraphVersion)
      Docket.Postgres.GraphCache.clear()
      on_exit(&Docket.Postgres.GraphCache.clear/0)
      :ok
    end

    test "the bundle fixes every capability and assembles poll-only execution plus pruning" do
      Process.register(self(), :docket_backend_observer_relay)

      assert Docket.Postgres.storage() == Docket.Postgres.Storage
      assert Docket.Postgres.graphs() == Docket.Postgres.GraphStore
      assert Docket.Postgres.runs() == Docket.Postgres.RunStore
      assert Docket.Postgres.events() == Docket.Postgres.EventStore

      start_supervised!(PollHost)

      backend_name = Module.concat(PollHost, Backend)
      assert Process.whereis(Docket.Postgres.runner_name(backend_name))
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
      assert {:error, %Docket.Error{type: :not_found}} = PollHost.get_run(started.id)

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
      assert_receive {:blocked, _node_pid, "blocker", 1}, 5_000
      assert System.monotonic_time(:millisecond) - started_at < 5_000

      assert [vehicle] = Task.Supervisor.children(vehicle_supervisor)
      vehicle_monitor = Process.monitor(vehicle)

      Process.exit(dispatcher, :kill)
      assert_receive {:DOWN, ^vehicle_monitor, :process, ^vehicle, _reason}, 5_000
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
      assert {:ok, tenantless} = PollHost.start_run(reference, %{"value" => "none"})

      assert {:error, :not_found} = TenantHost.fetch_run(tenantless.id, tenant_id: "a")

      assert {:ok, tenant_a} =
               TenantHost.start_run(reference, %{"value" => "a"}, tenant_id: "a")

      assert {:error, :not_found} = TenantHost.fetch_run(tenant_a.id, tenant_id: "b")
      assert {:error, :not_found} = PollHost.fetch_run(tenant_a.id)

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
      assert_raise KeyError, fn ->
        Docket.Postgres.context([])
      end

      assert_raise ArgumentError, ~r/Postgres context prefix/, fn ->
        Docket.Postgres.context(repo: TestRepo, prefix: "Bad-Prefix")
      end

      assert_raise ArgumentError, ~r/:pruner must be a keyword list/, fn ->
        Docket.Postgres.init(name: __MODULE__.MissingPruner, repo: TestRepo)
      end

      assert_raise ArgumentError, ~r/:pruner requires/, fn ->
        Docket.Postgres.init(name: __MODULE__.PartialPruner, repo: TestRepo, pruner: [])
      end

      assert_raise ArgumentError, ~r/:notifier must be :none or a keyword list/, fn ->
        Docket.Postgres.init(
          name: __MODULE__.BadNotifier,
          repo: TestRepo,
          notifier: false,
          pruner: @pruner
        )
      end

      assert_raise ArgumentError, ~r/:dispatcher has unknown keys/, fn ->
        Docket.Postgres.init(
          name: __MODULE__.MixedStore,
          repo: TestRepo,
          dispatcher: [run_store: SomeOtherStore],
          pruner: @pruner
        )
      end
    end

    test "revision-8 storage remains exactly the version-one three-table schema" do
      tables =
        TestRepo.query!(
          "SELECT table_name FROM information_schema.tables " <>
            "WHERE table_schema = current_schema() AND table_name LIKE 'docket_%' " <>
            "ORDER BY table_name",
          [],
          log: false
        ).rows
        |> List.flatten()

      assert tables == ["docket_events", "docket_graph_versions", "docket_runs"]

      assert %{rows: [[comment]]} =
               TestRepo.query!(
                 "SELECT obj_description('docket_runs'::regclass, 'pg_class')",
                 [],
                 log: false
               )

      assert comment == "1"
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
