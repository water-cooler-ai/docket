defmodule Docket.Reducer do
  @moduledoc """
  Public reducer descriptors for graph state fields.
  """

  defstruct [:type, opts: %{}]

  @type type :: :last_value
  @type t :: %__MODULE__{type: type(), opts: map()}

  @spec last_value(keyword() | map()) :: t()
  def last_value(opts \\ []), do: build(:last_value, opts)

  defp build(type, opts), do: %__MODULE__{type: type, opts: attrs_to_map(opts)}

  defp attrs_to_map(attrs) when is_map(attrs), do: attrs
  defp attrs_to_map(attrs) when is_list(attrs), do: Map.new(attrs)
end
