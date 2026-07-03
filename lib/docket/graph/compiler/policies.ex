defmodule Docket.Graph.Compiler.Policies do
  @moduledoc false

  # Graph policy resolution shared by validation and lowering so both passes
  # agree on the effective values.

  alias Docket.Graph

  @max_supersteps_key "max_supersteps"

  @doc """
  Resolves the effective max-supersteps limit.

  A valid graph policy wins over the `opts` runtime default; an explicit nil
  policy counts as unset and falls back to the default. A present policy that
  is not a positive integer is reported as `{:invalid, value}` so validation
  can attach a diagnostic regardless of graph topology.
  """
  @spec max_supersteps(Graph.t(), keyword()) ::
          {:ok, pos_integer() | nil} | {:invalid, term()}
  def max_supersteps(%Graph{} = graph, opts) do
    case Map.get(graph.policies, @max_supersteps_key) do
      nil -> {:ok, Keyword.get(opts, :max_supersteps)}
      limit when is_integer(limit) and limit > 0 -> {:ok, limit}
      invalid -> {:invalid, invalid}
    end
  end

  @spec max_supersteps_key() :: String.t()
  def max_supersteps_key, do: @max_supersteps_key
end
