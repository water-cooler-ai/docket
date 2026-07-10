defmodule Docket.GraphRef do
  @moduledoc """
  Stable reference to one saved, content-addressed graph version.

  `save_graph/2` returns a reference. Durable `start_run/3` accepts that
  reference and loads the canonical graph document from the configured
  backend before compiling it for execution.
  """

  @enforce_keys [:graph_id, :graph_hash]
  defstruct [:graph_id, :graph_hash]

  @type t :: %__MODULE__{graph_id: String.t(), graph_hash: String.t()}
end
