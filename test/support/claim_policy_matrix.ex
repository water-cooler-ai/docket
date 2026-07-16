if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.ClaimPolicyMatrix do
    @moduledoc false

    def implementations do
      [
        %{
          name: "Legacy",
          implementation: Docket.Postgres.ClaimPolicy.Legacy,
          options: [],
          fixture: Docket.Test.LegacyClaimPolicyContract,
          query_marker: "WITH transaction_context AS MATERIALIZED"
        },
        %{
          name: "alternate",
          implementation: Docket.Test.AlternateClaimPolicy,
          options: [marker: :run_store_contract],
          fixture: Docket.Test.AlternateClaimPolicyContract,
          query_marker: "independent alternate claim plan: run_store_contract"
        }
      ]
    end
  end
end
