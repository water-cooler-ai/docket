defmodule Docket.Bench.Scorecard.Scenarios.Concurrency do
  @moduledoc false
  @behaviour Docket.Bench.Scorecard.Scenario

  @impl true
  def name, do: "concurrency"

  @impl true
  def metric, do: "Concurrency scaling"

  @impl true
  def run(_profile, _ctx), do: {:error, :not_implemented}
end
