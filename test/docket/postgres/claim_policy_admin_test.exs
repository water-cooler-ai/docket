if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyAdminTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.Admin
    alias Docket.Postgres.Storage
    alias Docket.Postgres.ClaimPolicyAdminTestRepo, as: TestRepo

    @migration_version 20_260_716_000_068
    @prefixed_migration_version 20_260_716_000_069

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
      %{context: Docket.Postgres.context(repo: TestRepo)}
    end

    test "sets and reads the current default with optional CAS", %{context: context} do
      assert {:ok, %{max_active_runs: nil, version: 0} = initial} = Admin.get_default(context)
      refute Map.has_key?(initial, :max_active)

      assert {:ok, %{max_active_runs: 4, version: 1}} =
               Admin.put_default(context, 4, expected_version: 0)

      assert_no_state_change(context, {:error, :stale}, fn ->
        Admin.put_default(context, 5, expected_version: 0)
      end)

      assert {:ok, %{max_active_runs: 3, version: 2}} = Admin.put_default(context, 3)
      assert {:ok, %{max_active_runs: 3, version: 2}} = Admin.get_default(context)
    end

    test "sets and resets a tenant override without deleting its partition", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 5, expected_version: 0)

      assert {:ok, %{max_active_runs: 2, version: 1}} =
               Admin.put_override(context, {:tenant, "acme"}, 2, expected_version: 0)

      assert {:ok, %{max_active_runs: 2, source: :override, override_version: 1}} =
               Admin.get_effective(context, {:tenant, "acme"})

      assert {:error, :stale} =
               Admin.reset_override(context, {:tenant, "acme"}, expected_version: 0)

      assert {:ok, %{max_active_runs: nil, version: 2}} =
               Admin.reset_override(context, {:tenant, "acme"}, expected_version: 1)

      assert {:ok, %{max_active_runs: 5, source: :default, override_version: 2}} =
               Admin.get_effective(context, {:tenant, "acme"})

      assert [[nil, 2]] =
               TestRepo.query!(
                 "SELECT max_active, partition_version FROM docket_claim_partitions " <>
                   "WHERE scope_key = 'acme'"
               ).rows

      assert [[0]] =
               TestRepo.query!(
                 "SELECT unfinished_count FROM docket_claim_schedule WHERE scope_key = 'acme'"
               ).rows
    end

    test "administers the tenantless partition through override and effective state", %{
      context: context
    } do
      assert {:ok, %{max_active_runs: 5, version: 1}} =
               Admin.put_default(context, 5, expected_version: 0)

      assert {:ok,
              %{
                owner_scope: :tenantless,
                max_active_runs: 5,
                source: :default,
                default_version: 1,
                override_version: 0
              }} = Admin.get_effective(context, :tenantless)

      assert [] = partition_rows(context)

      assert {:ok, %{max_active_runs: 2, version: 1}} =
               Admin.put_override(context, :tenantless, 2, expected_version: 0)

      assert {:ok,
              %{
                owner_scope: :tenantless,
                max_active_runs: 2,
                source: :override,
                default_version: 1,
                override_version: 1
              }} = Admin.get_effective(context, :tenantless)

      assert [["", 2, 1]] = partition_rows(context)

      assert {:ok, %{max_active_runs: nil, version: 2}} =
               Admin.reset_override(context, :tenantless, expected_version: 1)

      assert {:ok,
              %{
                owner_scope: :tenantless,
                max_active_runs: 5,
                source: :default,
                default_version: 1,
                override_version: 2
              }} = Admin.get_effective(context, :tenantless)

      assert [["", nil, 2]] = partition_rows(context)
    end

    test "reports token-free queue, admission, and cap-debt counts", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)

      assert {:ok, %{version: 1}} =
               Admin.put_override(context, {:tenant, "acme"}, 1, expected_version: 0)

      insert_admission_inspection_fixture()

      assert {:ok,
              %{
                owner_scope: {:tenant, "acme"},
                max_active_runs: 1,
                source: :override,
                queued: 1,
                admitted_ready: 1,
                admitted_claimed: 1,
                debt: 1
              } = effective} = Admin.get_effective(context, {:tenant, "acme"})

      assert Enum.sort(Map.keys(effective)) ==
               Enum.sort([
                 :owner_scope,
                 :max_active_runs,
                 :source,
                 :default_version,
                 :override_version,
                 :queued,
                 :admitted_ready,
                 :admitted_claimed,
                 :debt
               ])
    end

    test "routes all administration through an explicit custom prefix", %{context: context} do
      :ok =
        Ecto.Migrator.up(
          TestRepo,
          @prefixed_migration_version,
          InstallDocketPrefixed,
          log: false
        )

      prefixed_context = Docket.Postgres.context(repo: TestRepo, prefix: "docket_private")

      assert {:ok, %{max_active_runs: nil, version: 0}} = Admin.get_default(prefixed_context)

      assert {:ok, %{max_active_runs: 7, version: 1}} =
               Admin.put_default(prefixed_context, 7, expected_version: 0)

      assert {:ok, %{max_active_runs: 3, version: 1}} =
               Admin.put_override(prefixed_context, {:tenant, "private-acme"}, 3,
                 expected_version: 0
               )

      assert {:ok,
              %{
                owner_scope: {:tenant, "private-acme"},
                max_active_runs: 3,
                source: :override,
                default_version: 1,
                override_version: 1
              }} = Admin.get_effective(prefixed_context, {:tenant, "private-acme"})

      assert {:ok, %{max_active_runs: nil, version: 2}} =
               Admin.reset_override(prefixed_context, {:tenant, "private-acme"},
                 expected_version: 1
               )

      assert {:ok, %{max_active_runs: 7, source: :default, override_version: 2}} =
               Admin.get_effective(prefixed_context, {:tenant, "private-acme"})

      assert {:ok, %{max_active_runs: nil, version: 0}} = Admin.get_default(context)
      assert [] = partition_rows(context)
      assert [["private-acme", nil, 2]] = partition_rows(prefixed_context)
    end

    test "stale and invalid operations leave current state exactly unchanged", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 5, expected_version: 0)

      assert {:ok, %{version: 1}} =
               Admin.put_override(context, {:tenant, "acme"}, 2, expected_version: 0)

      assert_no_state_change(context, {:error, :stale}, fn ->
        Admin.put_default(context, 6, expected_version: 0)
      end)

      assert_no_state_change(context, {:error, :invalid_max_active_runs}, fn ->
        Admin.put_default(context, 0)
      end)

      assert_no_state_change(context, {:error, :invalid_expected_version}, fn ->
        Admin.put_default(context, 6, expected_version: -1)
      end)

      assert_no_state_change(context, {:error, :invalid_options}, fn ->
        Admin.put_default(context, 6, actor: "unused")
      end)

      assert_no_state_change(context, {:error, :stale}, fn ->
        Admin.put_override(context, {:tenant, "acme"}, 3, expected_version: 0)
      end)

      assert_no_state_change(context, {:error, :stale}, fn ->
        Admin.reset_override(context, {:tenant, "acme"}, expected_version: 0)
      end)

      assert_no_state_change(context, {:error, :invalid_max_active_runs}, fn ->
        Admin.put_override(context, {:tenant, "acme"}, 0)
      end)

      assert_no_state_change(context, {:error, :invalid_owner_scope}, fn ->
        Admin.put_override(context, :system, 1)
      end)

      assert_no_state_change(context, {:error, :invalid_expected_version}, fn ->
        Admin.reset_override(context, {:tenant, "acme"}, expected_version: -1)
      end)

      assert_no_state_change(context, {:error, :invalid_options}, fn ->
        Admin.reset_override(context, {:tenant, "acme"}, actor: "unused")
      end)

      assert_no_state_change(context, {:error, :stale}, fn ->
        Admin.put_override(context, {:tenant, "missing"}, 3, expected_version: 1)
      end)

      refute Enum.any?(partition_rows(context), fn [scope_key, _maximum, _version] ->
               scope_key == "missing"
             end)
    end

    defp assert_no_state_change(context, expected_error, operation) do
      before = admin_state(context)
      assert expected_error == operation.()
      assert admin_state(context) == before
    end

    defp insert_admission_inspection_fixture do
      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ('acme', 'admin-inspection', 'v1', $1, CURRENT_TIMESTAMP)
        """,
        [<<131, 106>>]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, tenant_admitted_at, claim_token, claimed_at,
           inserted_at, started_at, updated_at)
        VALUES
          ('queued', 'acme', 'admin-inspection', 'v1', 'running', $1,
           1, CURRENT_TIMESTAMP - interval '3 seconds', NULL, NULL, NULL,
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
          ('admitted-ready', 'acme', 'admin-inspection', 'v1', 'running', $1,
           1, CURRENT_TIMESTAMP - interval '2 seconds', CURRENT_TIMESTAMP, NULL, NULL,
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
          ('admitted-claimed', 'acme', 'admin-inspection', 'v1', 'running', $1,
           1, NULL, CURRENT_TIMESTAMP, gen_random_uuid(), CURRENT_TIMESTAMP,
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [<<131, 106>>]
      )
    end

    defp admin_state(context) do
      {repo, prefix} = tables(context)

      %{
        policy:
          repo.query!(
            "SELECT admission_mode, max_active, policy_version, initialized_at, updated_at " <>
              "FROM #{prefix.policy} WHERE id = 1"
          ).rows,
        partitions:
          repo.query!(
            "SELECT scope_key, max_active, partition_version, admission_epoch, " <>
              "inserted_at, updated_at FROM #{prefix.partitions} ORDER BY scope_key"
          ).rows
      }
    end

    defp partition_rows(context) do
      {repo, prefix} = tables(context)

      repo.query!(
        "SELECT scope_key, max_active, partition_version " <>
          "FROM #{prefix.partitions} ORDER BY scope_key"
      ).rows
    end

    defp tables(context) do
      {repo, configured_prefix} = Storage.context!(context)
      prefix = Storage.physical_prefix!(repo, configured_prefix)

      {repo,
       %{
         policy: Storage.qualified_table(prefix, "docket_claim_policy"),
         partitions: Storage.qualified_table(prefix, "docket_claim_partitions")
       }}
    end
  end
end
