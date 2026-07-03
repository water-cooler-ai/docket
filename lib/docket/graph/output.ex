defmodule Docket.Graph.Output do
  @moduledoc """
  Public output projection from graph fields.
  """

  defstruct [
    :id,
    :source,
    :label,
    :description,
    :schema,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          id: String.t() | nil,
          source: String.t() | nil,
          label: String.t() | nil,
          description: String.t() | nil,
          schema: Docket.Schema.t() | nil,
          metadata: map()
        }
end
