defmodule Docket.Runtime.Graph.Channel do
  @moduledoc """
  Internal runtime channel definition.

  v1 channel types:

  - `:last_value` - input and state channels; stores the last committed value
  - `:ephemeral` - generated edge activation channels; visible for one step
  - `:barrier` - multi-source edge activation channels; fires when every
    source in `sources` has completed since the last firing
  """

  defstruct [
    :id,
    :type,
    :value_schema,
    :reducer,
    :default,
    sources: [],
    metadata: %{}
  ]

  @type type :: :last_value | :ephemeral | :barrier

  @type t :: %__MODULE__{
          id: String.t(),
          type: type(),
          value_schema: Docket.Schema.t() | nil,
          reducer: Docket.Reducer.t() | nil,
          default: term(),
          sources: [String.t()],
          metadata: map()
        }
end
