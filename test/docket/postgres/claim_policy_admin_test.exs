if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyAdminTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.Admin
    alias Docket.Postgres.ClaimPolicyAdminTestRepo, as: TestRepo

    @migration_version 20_260_716_000_068

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
      %{context: Docket.Postgres.context(repo: TestRepo)}
    end

    test "sets and reads the current default with optional CAS", %{context: context} do
      assert {:ok, %{max_active: nil, version: 0}} = Admin.get_default(context)

      assert {:ok, %{max_active: 4, version: 1}} =
               Admin.put_default(context, 4, expected_version: 0)

      assert {:error, :stale} = Admin.put_default(context, 5, expected_version: 0)
      assert {:ok, %{max_active: 3, version: 2}} = Admin.put_default(context, 3)
      assert {:ok, %{max_active: 3, version: 2}} = Admin.get_default(context)
    end

    test "sets and resets a tenant override without deleting its partition", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 5, expected_version: 0)

      assert {:ok, %{max_active: 2, version: 1}} =
               Admin.put_override(context, {:tenant, "acme"}, 2, expected_version: 0)

      assert {:ok, %{max_active: 2, source: :override, override_version: 1}} =
               Admin.get_effective(context, {:tenant, "acme"})

      assert {:error, :stale} =
               Admin.reset_override(context, {:tenant, "acme"}, expected_version: 0)

      assert {:ok, %{max_active: nil, version: 2}} =
               Admin.reset_override(context, {:tenant, "acme"}, expected_version: 1)

      assert {:ok, %{max_active: 5, source: :default, override_version: 2}} =
               Admin.get_effective(context, {:tenant, "acme"})

      assert [[nil, 2]] =
               TestRepo.query!(
                 "SELECT max_active, partition_version FROM docket_claim_partitions " <>
                   "WHERE scope_key = 'acme'"
               ).rows
    end

    test "validates caps, scopes, and options", %{context: context} do
      assert {:error, :invalid_max_active} = Admin.put_default(context, 0)
      assert {:error, :invalid_owner_scope} = Admin.put_override(context, :system, 1)
      assert {:error, :invalid_options} = Admin.put_default(context, 1, actor: "unused")
    end
  end
end
