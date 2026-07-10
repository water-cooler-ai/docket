defmodule Docket.GraphRef do
  @moduledoc """
  Stable reference to one saved, content-addressed graph version.

  `save_graph/2` returns a reference. Durable execution uses its graph content
  address and compiler ABI to load and hydrate the exact published execution
  artifact without invoking the graph compiler.
  """

  @enforce_keys [:graph_id, :graph_hash, :compiler_abi]
  defstruct [:graph_id, :graph_hash, :compiler_abi]

  @type t :: %__MODULE__{
          graph_id: String.t(),
          graph_hash: String.t(),
          compiler_abi: String.t()
        }
end
