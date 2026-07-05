defmodule Docket.Checkpoint.Context do
  @moduledoc """
  Context passed to `c:Docket.Checkpoint.handle/2` alongside each checkpoint.

  `application` carries the caller-supplied application context from the run
  options; Docket does not interpret it.
  """

  defstruct [:run_id, :graph_id, :graph_hash, application: %{}]

  @type t :: %__MODULE__{
          run_id: String.t(),
          graph_id: String.t(),
          graph_hash: String.t() | nil,
          application: map()
        }
end
