defmodule Docket.Graph.Diagnostic do
  @moduledoc """
  A public graph diagnostic.
  """

  defstruct [
    :severity,
    :code,
    :message,
    path: [],
    public_id: nil,
    runtime_id: nil,
    metadata: %{}
  ]

  @type severity :: :info | :warning | :error

  @type t :: %__MODULE__{
          severity: severity(),
          code: atom(),
          message: String.t(),
          path: [term()],
          public_id: String.t() | nil,
          runtime_id: String.t() | nil,
          metadata: map()
        }
end
