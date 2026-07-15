if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  implementations = [
    {Docket.Postgres.ClaimPolicy.Legacy, []},
    {Docket.Test.AlternateClaimPolicy, marker: :contract}
  ]

  for {implementation, options} <- implementations do
    defmodule Module.concat(implementation, ContractTest) do
      use ExUnit.Case, async: true

      use Docket.Test.ClaimPolicyTests,
        implementation: implementation,
        options: options
    end
  end

  defmodule Docket.Postgres.ClaimPolicyTest do
    use ExUnit.Case, async: true

    alias Docket.Postgres.ClaimPolicy

    defmodule MissingCallbacks do
    end

    defmodule RejectingImplementation do
      def init(_options), do: {:error, :invalid_rollout}
      def claim_due(_run_store, _context, :system, _policy, _state), do: :unreachable
    end

    test "rejects incomplete implementations and invalid implementation configuration" do
      assert_raise ArgumentError, ~r/missing init\/1, claim_due\/5/, fn ->
        ClaimPolicy.new(implementation: MissingCallbacks)
      end

      assert_raise ArgumentError, ~r/rejected its configuration: :invalid_rollout/, fn ->
        ClaimPolicy.new(implementation: RejectingImplementation)
      end

      assert_raise ArgumentError, ~r/:claim_policy must be a keyword list/, fn ->
        ClaimPolicy.new(:legacy)
      end
    end

    test "rejects malformed runtime input before calling the implementation" do
      claim_policy = ClaimPolicy.new()

      assert_raise ArgumentError, ~r/runtime input requires/, fn ->
        ClaimPolicy.claim_due(claim_policy, MissingCallbacks, self(), %{
          now: ~U[2026-07-15 12:00:00Z],
          limit: 0,
          orphan_ttl_ms: 1_000,
          max_claim_attempts: 3
        })
      end
    end
  end
end
