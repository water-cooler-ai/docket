defmodule Docket.Bench.Scorecard.Scenarios.Surge do
  @moduledoc false
  @behaviour Docket.Bench.Scorecard.Scenario

  @impl true
  def name, do: "surge"

  @impl true
  def metric, do: "Surge resilience"

  @impl true
  def run(_profile, _ctx), do: {:error, :not_implemented}
end
