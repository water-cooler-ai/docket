defmodule Docket.Runtime.StateView do
  @moduledoc false

  # Public-ID view of committed channel state, built in one pass over the
  # graph lowering: `values` and `versions` keyed by public input/field ID,
  # plus the `changed` set guards test against. Never-written channels are
  # absent from `values` unless the graph declares a non-nil default, so
  # nodes and guards see missing state as missing rather than nil; `versions`
  # carries every declared key, 0 when unwritten.

  alias Docket.Run.ChannelState

  defstruct values: %{}, versions: %{}, changed: MapSet.new()

  @type t :: %__MODULE__{
          values: %{optional(String.t()) => term()},
          versions: %{optional(String.t()) => non_neg_integer()},
          changed: MapSet.t(String.t())
        }

  @spec new(Docket.Runtime.Graph.t(), map(), Enumerable.t()) :: t()
  def new(rtg, channels, changed \\ MapSet.new()) do
    {values, versions} =
      Enum.reduce(rtg.lowering.runtime_to_public, {%{}, %{}}, fn
        {channel_id, {kind, public_id}}, {values, versions} when kind in [:input, :field] ->
          case Map.fetch(channels, channel_id) do
            {:ok, %ChannelState{value: value, version: version}} ->
              {Map.put(values, public_id, value), Map.put(versions, public_id, version)}

            :error ->
              versions = Map.put(versions, public_id, 0)

              case Map.fetch!(rtg.channels, channel_id).default do
                nil -> {values, versions}
                default -> {Map.put(values, public_id, default), versions}
              end
          end

        _entry, acc ->
          acc
      end)

    %__MODULE__{values: values, versions: versions, changed: MapSet.new(changed)}
  end
end
