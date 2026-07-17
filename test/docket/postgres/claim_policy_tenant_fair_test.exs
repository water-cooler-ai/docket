if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyTenantFairTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.Admin
    alias Docket.Postgres.RunStore
    alias Docket.Postgres.TestRepo

    @migration_version 20_260_716_000_168
    @now ~U[2026-07-16 12:00:00.000000Z]

    defmodule SecondRepo do
      use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
    end

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

      Application.put_env(:docket, SecondRepo, Keyword.put(config, :pool_size, 4))
      start_supervised!(SecondRepo)

      context = tenant_fair_context(TestRepo, 4)
      second_context = tenant_fair_context(SecondRepo, 4)
      %{context: context, second_context: second_context}
    end

    test "two independent pools cannot overfill the final slot", %{
      context: context,
      second_context: second_context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "cap-race-a", @now)
      insert_ready("tenant", "cap-race-b", @now)

      parent = self()
      gate = make_ref()

      tasks =
        for caller_context <- [context, second_context] do
          Task.async(fn ->
            send(parent, {gate, :ready})
            receive do: ({^gate, :go} -> :ok)
            RunStore.claim_due(caller_context, :system, policy(1))
          end)
        end

      for _ <- tasks, do: assert_receive({^gate, :ready}, 2_000)
      Enum.each(tasks, &send(&1.pid, {gate, :go}))
      results = Enum.map(tasks, &Task.await(&1, 5_000))

      leases =
        Enum.flat_map(results, fn
          {:ok, %{leases: leases}} -> leases
          {:error, {:claim_policy_unavailable, :lock_contention}} -> []
        end)

      assert length(leases) == 1
      assert live_count("tenant") == 1
    end

    test "cap reduction creates debt and release restores capacity only below the cap", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 3, expected_version: 0)

      for id <- ~w(active-a active-b active-c), do: insert_ready("tenant", id, @now)
      assert {:ok, %{leases: leases}} = RunStore.claim_due(context, :system, policy(3))
      assert length(leases) == 3

      assert {:ok, %{version: 2}} = Admin.put_default(context, 1, expected_version: 1)
      insert_ready("tenant", "waiting", @now)
      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(1))

      [first, second, third] = leases
      complete_claim(first)
      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(1))
      complete_claim(second)
      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(1))
      complete_claim(third)

      assert {:ok, %{leases: [%{run_id: "waiting"}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "demand-one discovery rotates from a deep tenant to another tenant", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 4, expected_version: 0)
      insert_ready("a", "a-1", DateTime.add(@now, -3, :second))
      insert_ready("a", "a-2", DateTime.add(@now, -2, :second))
      insert_ready("a", "a-3", DateTime.add(@now, -1, :second))
      insert_ready("b", "b-1", @now)

      assert {:ok, %{leases: [%{owner_scope: {:tenant, "a"}}]}} =
               RunStore.claim_due(context, :system, policy(1))

      assert {:ok, %{leases: [%{owner_scope: {:tenant, "b"}}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "capped heads rotate so a later eligible tenant makes progress", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)

      insert_claimed("a", "a-live", @now)
      insert_ready("a", "a-waiting", DateTime.add(@now, -3, :second))
      insert_claimed("b", "b-live", @now)
      insert_ready("b", "b-waiting", DateTime.add(@now, -2, :second))
      insert_ready("c", "c-ready", DateTime.add(@now, -1, :second))

      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(1))

      assert {:ok, %{leases: [%{run_id: "c-ready"}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "an expired steal is count-neutral and does not admit queued work", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_claimed("tenant", "expired", DateTime.add(@now, -10, :second))
      insert_ready("tenant", "queued", DateTime.add(@now, -5, :second))

      assert {:ok, %{leases: [%{run_id: "expired", claim_attempt: 2}]}} =
               RunStore.claim_due(context, :system, policy(2))

      assert live_count("tenant") == 1

      assert [[nil]] =
               TestRepo.query!("SELECT claim_token FROM docket_runs WHERE run_id = 'queued'").rows
    end

    test "switching to TenantFair makes a Legacy dispatcher fail closed", %{
      context: context,
      second_context: second_context
    } do
      legacy_context = Docket.Postgres.context(repo: SecondRepo)
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "tenant-fair", @now)
      insert_ready("other", "legacy-blocked", @now)

      assert {:ok, %{leases: [_lease]}} = RunStore.claim_due(context, :system, policy(1))

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(legacy_context, :system, policy(1))

      assert second_context.claim_policy.implementation ==
               Docket.Postgres.ClaimPolicy.TenantFair
    end

    defp tenant_fair_context(repo, default_max_active) do
      Docket.Postgres.context(
        repo: repo,
        claim_policy: [
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          default_max_active: default_max_active
        ]
      )
    end

    defp policy(limit) do
      %{
        now: @now,
        limit: limit,
        orphan_ttl_ms: 1_000,
        max_claim_attempts: 5,
        preference: nil
      }
    end

    defp complete_claim(lease) do
      TestRepo.query!(
        """
        UPDATE docket_runs
        SET status = 'done', claim_token = NULL, claimed_at = NULL,
            finished_at = $3, updated_at = $3
        WHERE run_id = $1 AND claim_token = $2
        """,
        [lease.run_id, Ecto.UUID.dump!(lease.claim_token), @now]
      )
    end

    defp insert_ready(scope_key, run_id, wake_at) do
      insert_run(scope_key, run_id, nil, nil, wake_at, 0)
    end

    defp insert_claimed(scope_key, run_id, claimed_at) do
      insert_run(
        scope_key,
        run_id,
        Ecto.UUID.generate(),
        claimed_at,
        nil,
        1
      )
    end

    defp insert_run(scope_key, run_id, claim_token, claimed_at, wake_at, attempts) do
      tenant_id = if scope_key == "", do: nil, else: scope_key
      graph_hash = "hash-#{scope_key}"

      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, 'graph', $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT (scope_key, graph_id, graph_hash) DO NOTHING
        """,
        [tenant_id, graph_hash, <<1>>]
      )

      TestRepo.query!(
        "INSERT INTO docket_claim_partitions (scope_key) VALUES ($1) ON CONFLICT DO NOTHING",
        [scope_key]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, claim_token, claimed_at, wake_at, claim_attempts,
           inserted_at, started_at, updated_at)
        VALUES ($1, $2, 'graph', $3, 'running', $4,
                7, $5, $6, $7, $8,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [
          run_id,
          tenant_id,
          graph_hash,
          <<1>>,
          claim_token && Ecto.UUID.dump!(claim_token),
          claimed_at,
          wake_at,
          attempts
        ]
      )
    end

    defp live_count(scope_key) do
      TestRepo.query!(
        """
        SELECT count(*)::integer
        FROM docket_runs
        WHERE scope_key = $1 AND status = 'running' AND poisoned_at IS NULL
          AND claim_token IS NOT NULL
        """,
        [scope_key]
      ).rows
      |> hd()
      |> hd()
    end
  end
end
