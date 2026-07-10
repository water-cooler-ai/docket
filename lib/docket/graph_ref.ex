defmodule Docket.GraphRef do
  @moduledoc """
  Stable reference to one saved, content-addressed graph version.

  `save_graph/2` returns a reference to the effective canonical document after
  node configuration defaults have been materialized. Durable execution loads
  that exact document, validates it against the executing node's installed
  contracts, and compiles it without adding defaults introduced later.
  """

  @enforce_keys [:graph_id, :graph_hash]
  defstruct [:graph_id, :graph_hash]

  @type t :: %__MODULE__{graph_id: String.t(), graph_hash: String.t()}
end
