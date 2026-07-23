if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyWindowedTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy
    alias Docket.Postgres.RunStore
    alias Docket.Postgres.TestRepo

    @migration_version 20_260_722_000_170
    @prefixed_migration_version 20_260_722_000_171
    @now ~U[2026-07-22 12:00:00.000000Z]

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

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      %{context: windowed_context(TestRepo)}
    end

    test "the engine interlock fails closed in both directions", %{context: context} do
      legacy_context = Docket.Postgres.context(repo: TestRepo)
      insert_ready("tenant", "due", @now)

      before_claims = policy_authority()

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(context, :system, policy(1))

      assert :ok = configure(context)

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(legacy_context, :system, policy(1))

      assert {:ok, %{leases: [%{run_id: "due"}], poisoned: []}} =
               RunStore.claim_due(context, :system, policy(1))

      assert [["legacy", _at]] = before_claims
      assert [["windowed", _at]] = policy_authority()
    end

    test "windowed admits tenantless work", %{context: context} do
      assert :ok = configure(context)
      insert_ready("", "tenantless", @now)

      assert {:ok, %{leases: [%{run_id: "tenantless", owner_scope: :tenantless}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "windowed admits work from a custom prefix" do
      TestRepo.query!("CREATE SCHEMA docket_private")

      :ok =
        Ecto.Migrator.up(
          TestRepo,
          @prefixed_migration_version,
          InstallDocketPrefixed,
          log: false
        )

      context = windowed_context(TestRepo, prefix: "docket_private")
      assert :ok = configure(context)
      insert_ready("tenant", "prefixed", @now, prefix: "docket_private")

      assert {:ok, %{leases: [%{run_id: "prefixed", owner_scope: {:tenant, "tenant"}}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "transaction-scoped windowed admission commits and rolls back with its caller", %{
      context: context
    } do
      assert :ok = configure(context)
      insert_ready("tenant", "transactional", @now)

      assert {:error, :test_rollback} =
               Docket.Postgres.transaction(context, fn tx ->
                 assert tx.claim_policy === context.claim_policy

                 assert {:ok, %{leases: [%{run_id: "transactional"}]}} =
                          RunStore.claim_due(tx, :system, policy(1))

                 {:error, :test_rollback}
               end)

      assert live_count("tenant") == 0

      assert {:ok, %{leases: [%{run_id: "transactional"}]}} =
               Docket.Postgres.transaction(context, fn tx ->
                 RunStore.claim_due(tx, :system, policy(1))
               end)

      assert live_count("tenant") == 1
    end

    test "a due admitted run is reacquired before an older queued run", %{context: context} do
      assert :ok = configure(context)
      sticky_admitted_at = DateTime.add(@now, -45, :second)
      insert_ready("tenant", "older-queued", DateTime.add(@now, -60, :second))

      insert_ready("tenant", "sticky", DateTime.add(@now, -30, :second),
        admitted_at: sticky_admitted_at
      )

      assert {:ok, %{leases: [%{run_id: "sticky"}]}} =
               RunStore.claim_due(context, :system, policy(1))

      assert [[admitted_at]] =
               TestRepo.query!(
                 "SELECT tenant_admitted_at FROM docket_runs WHERE run_id = 'sticky'"
               ).rows

      assert DateTime.compare(admitted_at, sticky_admitted_at) == :eq
    end

    test "a batch interleaves breadth-first across scopes", %{context: context} do
      assert :ok = configure(context)
      insert_ready("tenant-a", "a-1", DateTime.add(@now, -40, :second))
      insert_ready("tenant-a", "a-2", DateTime.add(@now, -39, :second))
      insert_ready("tenant-b", "b-1", DateTime.add(@now, -2, :second))
      insert_ready("tenant-b", "b-2", DateTime.add(@now, -1, :second))

      assert {:ok, %{leases: leases, poisoned: []}} =
               RunStore.claim_due(context, :system, policy(2))

      assert leases |> Enum.map(& &1.run_id) |> Enum.sort() == ["a-1", "b-1"]
    end

    test "limit-one claims sample only scopes holding due work", %{context: context} do
      assert :ok = configure(context)

      for index <- 1..9 do
        insert_ready(
          "sleeping-#{index}",
          "future-#{index}",
          DateTime.add(@now, 3_600, :second)
        )
      end

      insert_ready("due-tenant", "due", @now)

      for _round <- 1..5 do
        assert {:ok, %{leases: [%{run_id: "due"}], poisoned: []}} =
                 RunStore.claim_due(context, :system, policy(1))

        release_claim("due")
      end
    end

    test "startup configuration normalizes the admission mode only on drift", %{
      context: context
    } do
      assert [["legacy", _seeded_at]] = policy_authority()

      assert :ok = configure(context)
      assert [["windowed", normalized_at]] = policy_authority()

      assert :ok = configure(context)
      assert [["windowed", ^normalized_at]] = policy_authority()

      TestRepo.query!("UPDATE docket_claim_policy SET admission_mode = 'legacy' WHERE id = 1")

      assert :ok = configure(context)
      assert [["windowed", _renormalized_at]] = policy_authority()
    end

    defp windowed_context(repo, opts \\ []) do
      opts
      |> Keyword.put(:repo, repo)
      |> Keyword.put(:claim_policy,
        implementation: Docket.Postgres.ClaimPolicy.WindowedInterleave
      )
      |> Docket.Postgres.context()
    end

    defp configure(context) do
      claim_policy = ClaimPolicy.resolve(context)

      ClaimPolicy.configure(claim_policy, context, fn statement, params ->
        context.repo.query(statement, params, log: false)
      end)
    end

    defp policy_authority do
      TestRepo.query!("SELECT admission_mode, updated_at FROM docket_claim_policy WHERE id = 1").rows
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

    defp release_claim(run_id) do
      TestRepo.query!(
        """
        UPDATE docket_runs
        SET claim_token = NULL, claimed_at = NULL, wake_at = $2
        WHERE run_id = $1
        """,
        [run_id, @now]
      )
    end

    defp insert_ready(scope_key, run_id, wake_at, opts \\ []) do
      prefix = Keyword.get(opts, :prefix)
      admitted_at = Keyword.get(opts, :admitted_at)
      graphs = qualified_table(prefix, "docket_graph_versions")
      partitions = qualified_table(prefix, "docket_claim_partitions")
      runs = qualified_table(prefix, "docket_runs")
      tenant_id = if scope_key == "", do: nil, else: scope_key
      graph_hash = "hash-#{scope_key}"

      TestRepo.query!(
        """
        INSERT INTO #{graphs}
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, 'graph', $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT (scope_key, graph_id, graph_hash) DO NOTHING
        """,
        [tenant_id, graph_hash, <<1>>]
      )

      TestRepo.query!(
        "INSERT INTO #{partitions} (scope_key) VALUES ($1) ON CONFLICT DO NOTHING",
        [scope_key]
      )

      TestRepo.query!(
        """
        INSERT INTO #{runs}
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, tenant_admitted_at, wake_at, claim_attempts,
           inserted_at, started_at, updated_at)
        VALUES ($1, $2, 'graph', $3, 'running', $4,
                7, $5, $6, 0,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [run_id, tenant_id, graph_hash, <<1>>, admitted_at, wake_at]
      )
    end

    defp qualified_table(nil, table), do: ~s("#{table}")
    defp qualified_table(prefix, table), do: ~s("#{prefix}"."#{table}")

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
