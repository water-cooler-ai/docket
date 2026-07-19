if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyTest do
    use ExUnit.Case, async: true

    alias Docket.Postgres.ClaimPolicy
    alias Docket.Postgres.ClaimPolicy.TenantFair
    alias Docket.Postgres.ClaimPolicy.TenantFair.Config

    @context %{repo: Docket.Postgres.TestRepo, prefix: "public"}

    test "defaults to the legacy admission engine" do
      policy = ClaimPolicy.new([], @context)
      assert ClaimPolicy.implementation(policy) == Docket.Postgres.ClaimPolicy.Legacy
    end

    test "TenantFair accepts only one positive default cap" do
      assert {:ok, %Config{default_max_active: 4}} = Config.new(default_max_active: 4)
      assert {:error, {:missing_option, :default_max_active}} = Config.new([])

      assert {:error, {:invalid_option, :default_max_active}} =
               Config.new(default_max_active: 0)

      assert {:error, {:unknown_options, [:weight]}} =
               Config.new(default_max_active: 4, weight: 2)
    end

    test "TenantFair builds one bounded database-authoritative statement" do
      policy =
        ClaimPolicy.new(
          [implementation: TenantFair, default_max_active: 3],
          @context
        )

      runtime =
        ClaimPolicy.effective_policy!(%{
          now: ~U[2026-07-16 00:00:00.000000Z],
          limit: 8,
          orphan_ttl_ms: 1_000,
          max_claim_attempts: 5,
          preference: :ready
        })

      plan = ClaimPolicy.build_plan(policy, @context, runtime)

      assert plan.demand == 8

      assert plan.params == [
               ~U[2026-07-16 00:00:00.000000Z],
               ~U[2026-07-15 23:59:59.000000Z],
               8,
               5,
               "ready",
               3
             ]

      assert plan.statement =~ "docket_tenant_fair_claim"
      assert plan.statement =~ "false"
      assert plan.statement =~ "WHERE claimed.row_kind IN ('outcome', 'error')"
      assert plan.statement =~ "ORDER BY claimed.visit_ordinal"
      refute plan.statement =~ "eligible_partitions"
    end

    test "rejects unknown implementations and invalid runtime input" do
      assert_raise ArgumentError, ~r/does not implement/, fn ->
        ClaimPolicy.new([implementation: String], @context)
      end

      assert_raise ArgumentError, ~r/requires DateTime/, fn ->
        ClaimPolicy.effective_policy!(%{now: :now, limit: 0})
      end
    end
  end
end
