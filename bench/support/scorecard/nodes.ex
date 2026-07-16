defmodule Docket.Bench.Scorecard.Nodes.NoopNode do
  @moduledoc false
  @behaviour Docket.Node

  @impl true
  def config_schema, do: Docket.Schema.object(%{})

  @impl true
  def call(_state, _config, _context), do: {:ok, %{}}
end

defmodule Docket.Bench.Scorecard.Nodes.SleepNode do
  @moduledoc false
  @behaviour Docket.Node

  def hold_ms, do: Application.get_env(:docket, :scorecard_sleep_node_hold_ms, 0)

  @impl true
  def config_schema, do: Docket.Schema.object(%{})

  @impl true
  def call(_state, _config, _context) do
    Process.sleep(hold_ms())
    {:ok, %{}}
  end
end
