defmodule Docket.Graph.Field do
  @moduledoc """
  Public input or state field in a graph document.
  """

  defstruct [
    :id,
    :kind,
    :label,
    :description,
    :schema,
    :reducer,
    required: false,
    default: nil,
    metadata: %{}
  ]

  @type kind :: :input | :state

  @type t :: %__MODULE__{
          id: String.t() | nil,
          kind: kind() | nil,
          label: String.t() | nil,
          description: String.t() | nil,
          schema: Docket.Schema.t() | nil,
          reducer: Docket.Reducer.t() | nil,
          required: boolean(),
          default: term(),
          metadata: map()
        }
end
