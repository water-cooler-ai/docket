defmodule Docket.Graph.Edge do
  @moduledoc """
  Editable public edge between graph endpoints.
  """

  defstruct [
    :id,
    :from,
    :to,
    :label,
    :description,
    :source_handle,
    :target_handle,
    :guard,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          id: String.t() | nil,
          from: String.t() | [String.t()] | nil,
          to: String.t() | nil,
          label: String.t() | nil,
          description: String.t() | nil,
          source_handle: String.t() | nil,
          target_handle: String.t() | nil,
          guard: Docket.Guard.t() | nil,
          metadata: map()
        }
end
