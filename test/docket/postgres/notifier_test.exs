if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.NotifierTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres

    alias Docket.Postgres.{Dispatcher, Notifier, RunStore, Storage}
    alias Docket.Postgres.NotifierTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.{GraphVersion, Run}

    @migration_version 20_260_711_000_025
    @prefixed_migration_version 20_260_711_000_026
    @channel "docket_wake"

    defmodule InstallDocket do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule InstallDocketPrefixed do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(prefix: "docket_private")
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_private")
    end

    defmodule StubDispatcher do
      use GenServer

      def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

      @impl true
      def init(test_pid), do: {:ok, test_pid}

      @impl true
      def handle_cast(:request_poll, test_pid) do
        send(test_pid, :request_poll)
        {:noreply, test_pid}
      end
    end

    setup_all do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      :ok =
        Ecto.Migrator.up(TestRepo, @prefixed_migration_version, InstallDocketPrefixed, log: false)

      :ok
    end

    setup do
      TestRepo.delete_all(Run)
      TestRepo.delete_all(GraphVersion)
      TestRepo.delete_all(Run, prefix: "docket_private")
      TestRepo.delete_all(GraphVersion, prefix: "docket_private")
      :ok
    end

    describe "RunStore wake announcements" do
      test "insert announces a due first wake only after the transaction commits" do
        listen!()
        insert_graph!()
        run = initialized_run("insert-due")

        assert {:ok, :committed} =
                 Storage.transaction(TestRepo, fn tx ->
                   {:ok, _run} =
                     RunStore.insert_run(tx, :tenantless, run, :run_initialized, past())

                   refute_receive {:notification, _, _, @channel, _}, 200
                   {:ok, :committed}
                 end)

        assert_receive {:notification, _, _, @channel, ""}
      end

      test "a rolled-back start announces nothing" do
        listen!()
        insert_graph!()
        run = initialized_run("insert-rollback")

        assert {:error, :rolled_back} =
                 Storage.transaction(TestRepo, fn tx ->
                   {:ok, _run} =
                     RunStore.insert_run(tx, :tenantless, run, :run_initialized, past())

                   {:error, :rolled_back}
                 end)

        refute_receive {:notification, _, _, @channel, _}, 300
        assert {:error, :not_found} = RunStore.fetch_run(TestRepo, :system, run.id)
      end

      test "a future first wake announces nothing" do
        listen!()
        insert_graph!()
        run = initialized_run("insert-future")

        assert {:ok, ^run} =
                 RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, future())

        refute_receive {:notification, _, _, @channel, _}, 300
      end

      test "a prefixed context announces its prefix as payload" do
        listen!()
        insert_graph!("docket_private")
        run = initialized_run("insert-prefixed")
        ctx = %{repo: TestRepo, prefix: "docket_private"}

        assert {:ok, ^run} =
                 RunStore.insert_run(ctx, :tenantless, run, :run_initialized, past())

        assert_receive {:notification, _, _, @channel, "docket_private"}
      end

      test "commit announces an immediate release and a due {:at, _} release" do
        insert_graph!()
        listen!()

        for {run_id, schedule} <- [
              {"commit-immediate", {:release_claim, :immediate}},
              {"commit-at-past", {:release_claim, {:at, past()}}}
            ] do
          {run, lease} = insert_and_claim!(run_id)

          assert {:ok, _run} =
                   RunStore.commit(TestRepo, :system, commit_proposal(run, lease, schedule))

          assert_receive {:notification, _, _, @channel, ""}
        end
      end

      test "commit stays silent for future and external schedules" do
        insert_graph!()
        listen!()

        for {run_id, schedule, status} <- [
              {"commit-at-future", {:release_claim, {:at, future()}}, :running},
              {"commit-external", {:release_claim, :external}, :waiting}
            ] do
          {run, lease} = insert_and_claim!(run_id)

          assert {:ok, _run} =
                   RunStore.commit(
                     TestRepo,
                     :system,
                     commit_proposal(run, lease, schedule, status)
                   )

          refute_receive {:notification, _, _, @channel, _}, 300
        end
      end

      test "a signal mutation announces its immediate wake on commit" do
        listen!()
        insert_graph!()
        run = initialized_run("signal-commit")

        assert {:ok, ^run} =
                 RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, future())

        assert {:ok, {:committed, :signaled}} =
                 RunStore.mutate_run(TestRepo, :system, run.id, immediate_mutation(:signaled))

        assert_receive {:notification, _, _, @channel, ""}
      end

      test "a rolled-back signal transaction announces nothing" do
        listen!()
        insert_graph!()
        run = initialized_run("signal-rollback")

        assert {:ok, ^run} =
                 RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, future())

        assert {:error, :event_append_failed} =
                 Storage.transaction(TestRepo, fn tx ->
                   {:ok, {:committed, :signaled}} =
                     RunStore.mutate_run(tx, :system, run.id, immediate_mutation(:signaled))

                   {:error, :event_append_failed}
                 end)

        refute_receive {:notification, _, _, @channel, _}, 300
      end

      test "poison recovery announces its wake; recovering a healthy run stays silent" do
        listen!()
        insert_graph!()
        run = initialized_run("poison-retry")

        assert {:ok, ^run} =
                 RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, future())

        poison!(run.id)

        assert {:ok, _run} =
                 RunStore.retry_poisoned_run(TestRepo, :system, run.id, DateTime.utc_now())

        assert_receive {:notification, _, _, @channel, ""}

        assert {:ok, _run} =
                 RunStore.retry_poisoned_run(TestRepo, :system, run.id, DateTime.utc_now())

        refute_receive {:notification, _, _, @channel, _}, 300
      end

      test "claim release records its wake without an announcement" do
        insert_graph!()
        listen!()
        {run, lease} = insert_and_claim!("release-silent")

        assert :ok =
                 RunStore.release_claim(
                   TestRepo,
                   :system,
                   run.id,
                   lease.claim_token,
                   DateTime.utc_now()
                 )

        refute_receive {:notification, _, _, @channel, _}, 300
      end
    end

    describe "listener" do
      test "a matching notification requests one dispatcher poll; foreign payloads are ignored" do
        stub = start_supervised!({StubDispatcher, self()})

        start_supervised!(
          {Notifier, context: TestRepo, dispatcher: stub, connection: [sync_connect: true]}
        )

        notify!("another_prefix")
        refute_receive :request_poll, 300

        notify!("")
        assert_receive :request_poll
      end

      test "a prefixed listener polls only for its own prefix" do
        stub = start_supervised!({StubDispatcher, self()})

        start_supervised!(
          {Notifier,
           context: %{repo: TestRepo, prefix: "docket_private"},
           dispatcher: stub,
           connection: [sync_connect: true]}
        )

        notify!("")
        refute_receive :request_poll, 300

        notify!("docket_private")
        assert_receive :request_poll
      end

      @tag capture_log: true
      test "supervision restarts a killed listener and polls resume" do
        stub = start_supervised!({StubDispatcher, self()})

        notifier =
          start_supervised!(
            {Notifier, context: TestRepo, dispatcher: stub, connection: [sync_connect: true]}
          )

        notify!("")
        assert_receive :request_poll

        Process.exit(notifier, :kill)

        assert Enum.any?(1..50, fn _attempt ->
                 notify!("")

                 receive do
                   :request_poll -> true
                 after
                   100 -> false
                 end
               end)
      end
    end

    describe "dispatch integration" do
      test "a notify-enabled immediate wake is claimed well below the poll interval" do
        insert_graph!()
        test_pid = self()

        dispatcher =
          start_supervised!(
            {Dispatcher,
             context: TestRepo,
             concurrency: 1,
             poll_interval_ms: 60_000,
             orphan_ttl_ms: 60_000,
             max_claim_attempts: 5,
             drain_timeout_ms: 100,
             jitter: fn interval -> interval end,
             launch: fn lease ->
               vehicle = spawn(fn -> Process.sleep(5_000) end)
               send(test_pid, {:launched, lease.run_id, vehicle})
               {:ok, vehicle}
             end}
          )

        start_supervised!(
          {Notifier, context: TestRepo, dispatcher: dispatcher, connection: [sync_connect: true]}
        )

        Process.sleep(300)

        run = initialized_run("fast-path")

        assert {:ok, :committed} =
                 Storage.transaction(TestRepo, fn tx ->
                   {:ok, _run} =
                     RunStore.insert_run(tx, :tenantless, run, :run_initialized, past())

                   {:ok, :committed}
                 end)

        assert_receive {:launched, run_id, vehicle}, 5_000
        assert run_id == run.id
        Process.exit(vehicle, :kill)
      end

      test "poll-only operation claims without a listener" do
        insert_graph!()
        test_pid = self()

        start_supervised!(
          {Dispatcher,
           context: TestRepo,
           concurrency: 1,
           poll_interval_ms: 100,
           orphan_ttl_ms: 60_000,
           max_claim_attempts: 5,
           drain_timeout_ms: 100,
           launch: fn lease ->
             vehicle = spawn(fn -> Process.sleep(5_000) end)
             send(test_pid, {:launched, lease.run_id, vehicle})
             {:ok, vehicle}
           end}
        )

        run = initialized_run("poll-only")

        assert {:ok, ^run} =
                 RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, past())

        assert_receive {:launched, run_id, vehicle}, 3_000
        assert run_id == run.id
        Process.exit(vehicle, :kill)
      end
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
      {:ok, _reference} = Postgrex.Notifications.listen(listener, @channel)
      listener
    end

    defp notify!(payload) do
      TestRepo.query!("SELECT pg_notify($1, $2)", [@channel, payload], log: false)
    end

    defp past, do: DateTime.add(DateTime.utc_now(), -60, :second)
    defp future, do: DateTime.add(DateTime.utc_now(), 3_600, :second)

    defp insert_graph!(prefix \\ nil) do
      changeset =
        GraphVersion.changeset(%{graph_id: "graph", graph_hash: "hash", graph: <<131, 106>>})

      TestRepo.insert!(changeset, prefix: prefix)
    end

    defp initialized_run(run_id) do
      now = DateTime.utc_now()

      %Docket.Run{
        id: run_id,
        graph_id: "graph",
        graph_hash: "hash",
        status: :running,
        input: %{"prompt" => "hello"},
        metadata: %{"source" => "notifier-test"},
        started_at: now,
        updated_at: now,
        checkpoint_seq: 1
      }
    end

    defp insert_and_claim!(run_id) do
      run = initialized_run(run_id)

      {:ok, ^run} = RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, future())

      {:ok, %{leases: leases, poisoned: []}} =
        RunStore.claim_due(TestRepo, :system, %{
          now: DateTime.add(DateTime.utc_now(), 7_200, :second),
          limit: 10,
          orphan_ttl_ms: 3_600_000,
          max_claim_attempts: 5
        })

      lease = Enum.find(leases, &(&1.run_id == run.id))
      assert lease, "expected a lease for #{run.id}"
      {run, lease}
    end

    defp commit_proposal(run, lease, schedule, status \\ :running) do
      advanced =
        struct(run,
          checkpoint_seq: lease.checkpoint_seq + 1,
          status: status,
          updated_at: DateTime.utc_now()
        )

      %{
        run: advanced,
        expected_checkpoint_seq: lease.checkpoint_seq,
        claim_token: lease.claim_token,
        checkpoint_type: :step_committed,
        schedule: schedule
      }
    end

    defp immediate_mutation(opaque) do
      fn run ->
        advanced =
          struct(run, checkpoint_seq: run.checkpoint_seq + 1, updated_at: DateTime.utc_now())

        {:commit, advanced, :step_committed, {:release_claim, :immediate}, opaque}
      end
    end

    defp poison!(run_id) do
      {1, _} =
        TestRepo.update_all(
          from(run in Run, where: run.run_id == ^run_id),
          set: [
            poisoned_at: DateTime.utc_now(),
            poison_reason: "test_poison",
            claim_token: nil,
            claimed_at: nil,
            wake_at: nil
          ]
        )

      :ok
    end
  end
end
