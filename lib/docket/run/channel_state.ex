defmodule Docket.Run.ChannelState do
  @moduledoc """
  Committed state of one runtime channel inside a `Docket.Run`.

  `version` advances once per committed update barrier that wrote the channel
  (write-based change tracking: equal values still bump). Channels that have
  never been written are absent from `run.channels` entirely, so a stored
  version is always at least 1.

  `barrier_seen` is used only by `:barrier` channels: the sorted list of
  source node public IDs that completed since the barrier last fired.
  """

  defstruct [:channel_id, :value, version: 0, barrier_seen: []]

  @type t :: %__MODULE__{
          channel_id: String.t(),
          value: term(),
          version: non_neg_integer(),
          barrier_seen: [String.t()]
        }
end
