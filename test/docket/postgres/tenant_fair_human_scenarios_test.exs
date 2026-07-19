if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.TenantFairHumanScenariosTest do
    use ExUnit.Case, async: false

    @moduletag :postgres
    @moduletag :tenant_fair

    alias Docket.Postgres.RunStore
    alias Docket.Postgres.TestRepo

    @migration_version 20_260_719_000_180
    @now ~U[2026-07-19 12:00:00.000000Z]
    @low_tenants Enum.map(1..12, &"low-#{String.pad_leading(Integer.to_string(&1), 2, "0")}")

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
      :ok
    end

    for hot_backlog <- [100, 5_000] do
      @tag :proof
      test "TenantFair serves every low-volume tenant within one ring despite #{hot_backlog} hot runs" do
        hot_backlog = unquote(hot_backlog)
        seed_hot_and_low_work(hot_backlog)

        served = claim_owners(tenant_fair_context(), length(@low_tenants) + 1)

        assert served == ["hot" | @low_tenants]
        assert Enum.count(served, &(&1 == "hot")) == 1
        assert Enum.sort(served -- ["hot"]) == @low_tenants

        assert claimed_count("hot") == 1

        assert ready_count("hot") == hot_backlog - 1

        assert Enum.map(@low_tenants, &claimed_count/1) ==
                 List.duplicate(1, length(@low_tenants))
      end
    end

    @tag :proof
    test "Legacy drains all 1000 older hot runs before the first low-volume tenant" do
      hot_backlog = 1_000
      seed_hot_and_low_work(hot_backlog)

      context = Docket.Postgres.context(repo: TestRepo)

      hot_service =
        Enum.map(1..hot_backlog, fn expected_ordinal ->
          lease = claim_one!(context)

          assert lease.owner_scope == {:tenant, "hot"}

          assert lease.run_id ==
                   "hot-#{String.pad_leading(Integer.to_string(expected_ordinal), 5, "0")}"

          complete_claim(lease)
          lease
        end)

      first_low = claim_one!(context)

      assert length(hot_service) == hot_backlog
      assert first_low.owner_scope == {:tenant, "low-01"}
      assert first_low.run_id == "low-01-run"
    end

    defp claim_owners(context, count) do
      Enum.map(1..count, fn _ordinal ->
        context
        |> claim_one!()
        |> then(fn %{owner_scope: {:tenant, tenant}} -> tenant end)
      end)
    end

    defp claim_one!(context) do
      assert {:ok, %{leases: [lease], poisoned: []}} =
               RunStore.claim_due(context, :system, policy())

      lease
    end

    defp tenant_fair_context do
      Docket.Postgres.context(
        repo: TestRepo,
        claim_policy: [
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          default_max_active_runs: 10_000
        ]
      )
    end

    defp seed_hot_and_low_work(hot_backlog) do
      tenants = ["hot" | @low_tenants]

      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        SELECT tenant, 'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP
        FROM unnest($1::text[]) AS tenant
        """,
        [tenants]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_claim_partitions (scope_key)
        SELECT tenant
        FROM unnest($1::text[]) WITH ORDINALITY AS seeded(tenant, ordinal)
        ORDER BY ordinal
        """,
        [tenants]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        SELECT 'hot-' || lpad(series::text, 5, '0'),
               'hot', 'graph', 'hash', 'running', decode('01', 'hex'), 1,
               $1::timestamptz - interval '2 hours' + series * interval '1 microsecond',
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, $2) AS series
        """,
        [@now, hot_backlog]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        SELECT tenant || '-run', tenant, 'graph', 'hash', 'running', decode('01', 'hex'), 1,
               $1::timestamptz - interval '1 hour' + ordinal * interval '1 microsecond',
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM unnest($2::text[]) WITH ORDINALITY AS seeded(tenant, ordinal)
        """,
        [@now, @low_tenants]
      )
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

    defp claimed_count(scope_key) do
      [[count]] =
        TestRepo.query!(
          """
          SELECT count(*)::integer
          FROM docket_runs
          WHERE scope_key = $1 AND claim_token IS NOT NULL
          """,
          [scope_key]
        ).rows

      count
    end

    defp ready_count(scope_key) do
      [[count]] =
        TestRepo.query!(
          """
          SELECT count(*)::integer
          FROM docket_runs
          WHERE scope_key = $1 AND status = 'running'
            AND claim_token IS NULL AND poisoned_at IS NULL AND wake_at <= $2
          """,
          [scope_key, @now]
        ).rows

      count
    end

    defp policy do
      %{
        now: @now,
        limit: 1,
        orphan_ttl_ms: 3_600_000,
        max_claim_attempts: 5,
        preference: nil
      }
    end
  end
end
