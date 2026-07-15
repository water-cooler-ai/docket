defmodule Docket.Backend.Conformance.Instance do
  @moduledoc """
  One isolated backend instance supplied to the conformance cases.

  `context` is the opaque root context accepted by `backend.transaction/2` and
  by focused-store operations outside a transaction. `namespace` must be
  unique for the case. `now` is a deterministic, microsecond-precision UTC
  timestamp used for portable scheduling and claim assertions.
  """

  @enforce_keys [:backend, :context, :namespace, :now]
  defstruct [:backend, :context, :namespace, :now]

  @type t :: %__MODULE__{
          backend: module(),
          context: Docket.Backend.ctx(),
          namespace: nonempty_binary(),
          now: DateTime.t()
        }
end
