defmodule Docket.Event do
  @moduledoc """
  Append-only fact about run lifecycle, node execution, channel updates,
  edge triggers, and interrupts.

  Events are built at update barriers and delivered inside checkpoints. The
  v1 event types are:

  - `:run_initialized`, `:run_completed`, `:run_failed`
  - `:node_completed`, `:node_failed` (one `:node_failed` per failed attempt)
  - `:channel_updated` (payload carries the new version and writer node IDs,
    not the value)
  - `:edge_triggered`
  - `:interrupt_requested`, `:interrupt_resolved`
  """

  defstruct [
    :run_id,
    :seq,
    :type,
    :step,
    :node_id,
    :channel_id,
    :task_id,
    :timestamp,
    payload: %{},
    metadata: %{}
  ]

  @type type ::
          :run_initialized
          | :run_completed
          | :run_failed
          | :node_completed
          | :node_failed
          | :channel_updated
          | :edge_triggered
          | :interrupt_requested
          | :interrupt_resolved

  @type t :: %__MODULE__{
          run_id: String.t(),
          seq: pos_integer(),
          type: type(),
          step: non_neg_integer(),
          node_id: String.t() | nil,
          channel_id: String.t() | nil,
          task_id: String.t() | nil,
          timestamp: DateTime.t(),
          payload: map(),
          metadata: map()
        }
end
