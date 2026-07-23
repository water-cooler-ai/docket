if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyTest do
    use ExUnit.Case, async: true

    alias Docket.Postgres.ClaimPolicy

    @context %{repo: Docket.Postgres.TestRepo, prefix: "public"}

    test "defaults to the legacy admission engine" do
      policy = ClaimPolicy.new([], @context)
      assert ClaimPolicy.implementation(policy) == Docket.Postgres.ClaimPolicy.Legacy
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
