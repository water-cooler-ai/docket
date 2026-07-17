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
          query_marker: "ready_candidates AS MATERIALIZED"
        },
        %{
          name: "alternate",
          implementation: Docket.Test.AlternateClaimPolicy,
          options: [marker: :run_store_contract],
          fixture: Docket.Test.AlternateClaimPolicyContract,
          query_marker: "independent alternate claim plan: run_store_contract"
        },
        %{
          name: "TenantFair",
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          options: [default_max_active: 2],
          fixture: Docket.Test.TenantFairClaimPolicyContract,
          query_marker: "WITH eligible_partitions AS MATERIALIZED",
          run_store_setup: Docket.Test.TenantFairRunStoreSetup
        }
      ]
    end
  end
end
