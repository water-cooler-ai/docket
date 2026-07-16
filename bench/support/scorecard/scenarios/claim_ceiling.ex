defmodule Docket.Bench.Scorecard.Scenarios.ClaimCeiling do
  @moduledoc false
  @behaviour Docket.Bench.Scorecard.Scenario

  @impl true
  def name, do: "claim_ceiling"

  @impl true
  def metric, do: "Claim efficiency"

  @impl true
  def run(_profile, _ctx), do: {:error, :not_implemented}
end
