if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.TransitionDurabilityTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.BackendTests.Fixture
    alias Docket.Postgres.TestRepo

    @migration_version 20_260_724_000_301

    defmodule InstallDocket do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      instance = %{
        backend: Docket.Postgres,
        context: Docket.Postgres.TestAdmissionContext.resolve(%{repo: TestRepo}),
        namespace: "durability-#{System.unique_integer([:positive, :monotonic])}",
        now: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      }

      {:ok, backend_test: instance}
    end

    test "acknowledged transitions and receipts survive a backend restart",
         %{backend_test: instance} do
      transitions = instance.backend.transitions()
      {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "durability")

      run =
        Fixture.run(instance, "durability-run", graph, graph_hash,
          checkpoint_seq: 1,
          event_seq: 1
        )

      event = Fixture.event(run, 1, instance.now, type: :run_initialized)

      init = %{
        transition_id: Fixture.id(instance, "durability-init"),
        run: run,
        checkpoint_type: :run_initialized,
        wake_at: instance.now
      }

      assert {:ok, ^run} =
               transitions.initialize(instance.context, :tenantless, init, [event])

      lease = Fixture.claim(instance)
      assert lease.run_id == run.id

      advanced = %{
        run
        | checkpoint_seq: 2,
          event_seq: 2,
          updated_at: DateTime.add(instance.now, 1, :microsecond)
      }

      claimed_event = Fixture.event(advanced, 2, instance.now)

      claimed = %{
        transition_id: Fixture.id(instance, "durability-claimed"),
        run: advanced,
        expected_checkpoint_seq: 1,
        expected_event_seq: 1,
        claim_token: lease.claim_token,
        checkpoint_type: :step_committed,
        schedule: :retain_claim
      }

      assert {:ok, ^advanced} =
               transitions.commit_claimed(instance.context, :tenantless, claimed, [
                 claimed_event
               ])

      :ok = stop_supervised(TestRepo)
      start_supervised!(TestRepo)

      assert {:ok, ^run} =
               transitions.initialize(instance.context, :tenantless, init, [event])

      assert {:ok, ^advanced} =
               transitions.commit_claimed(instance.context, :tenantless, claimed, [
                 claimed_event
               ])

      assert {:error, :conflict} =
               transitions.initialize(
                 instance.context,
                 :tenantless,
                 %{init | run: %{run | input: %{"different" => true}}},
                 [event]
               )

      assert {:ok, %Docket.Run{checkpoint_seq: 2, event_seq: 2}} =
               instance.backend.runs().fetch_run(instance.context, :tenantless, run.id)

      assert TestRepo.query!("SELECT count(*) FROM docket_transition_receipts").rows == [[2]]
    end
  end
end
