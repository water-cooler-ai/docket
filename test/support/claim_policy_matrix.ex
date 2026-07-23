if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.ClaimPolicyMatrix do
    @moduledoc false

    def implementations do
      [
        %{
          name: "Legacy",
          implementation: Docket.Postgres.ClaimPolicy.Legacy,
          options: [],
          query_marker: "ready_candidates AS MATERIALIZED"
        },
        %{
          name: "alternate",
          implementation: Docket.Test.AlternateClaimPolicy,
          options: [marker: :run_store_contract],
          query_marker: "independent alternate claim plan: run_store_contract"
        },
        %{
          name: "Windowed",
          implementation: Docket.Postgres.ClaimPolicy.WindowedInterleave,
          options: [],
          query_marker: "active_scopes AS MATERIALIZED",
          run_store_setup: Docket.Test.WindowedRunStoreSetup
        }
      ]
    end
  end
end
