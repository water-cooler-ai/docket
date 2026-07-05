defmodule Docket.Run.ChannelState do
  @moduledoc """
  Committed state of one runtime channel inside a `Docket.Run`.

  `version` advances once per committed update barrier that wrote the channel
  (write-based change tracking: equal values still bump). Value channels that
  have never been written are absent from `run.channels` entirely, so their
  stored version is always at least 1; `:barrier` channels may be stored at
  version 0 while accumulating `barrier_seen` before the barrier first fires.

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
