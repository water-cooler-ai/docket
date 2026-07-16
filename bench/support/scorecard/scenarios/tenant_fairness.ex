defmodule Docket.Bench.Scorecard.Scenarios.TenantFairness do
  @moduledoc false
  @behaviour Docket.Bench.Scorecard.Scenario

  @impl true
  def name, do: "tenant_fairness"

  @impl true
  def metric, do: "Tenant fairness"

  @impl true
  def run(_profile, _ctx), do: {:error, :not_implemented}
end
