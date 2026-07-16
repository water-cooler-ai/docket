defmodule Docket.Bench.Scorecard.Scenarios.FastSlow do
  @moduledoc false
  @behaviour Docket.Bench.Scorecard.Scenario

  @impl true
  def name, do: "fast_slow"

  @impl true
  def metric, do: "Fast/slow fairness"

  @impl true
  def run(_profile, _ctx), do: {:error, :not_implemented}
end
