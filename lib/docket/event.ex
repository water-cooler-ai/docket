defmodule Docket.Event do
  @moduledoc """
  Append-only fact about run lifecycle, node execution, channel updates,
  edge triggers, and interrupts.

  Events are built at update barriers and persisted with committed transitions. The
  v0.1 event types are:

  - `:run_initialized`, `:run_completed`, `:run_failed`, `:run_cancelled`
  - `:checkpoint_committed` (metadata-only durable checkpoint history)
  - `:node_completed`, `:node_failed` (one `:node_failed` per failed attempt)
  - `:channel_updated` (payload carries the new version or the writer node
    IDs depending on the write's origin, never the value)
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
          | :run_cancelled
          | :checkpoint_committed
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

  @types [
    :run_initialized,
    :run_completed,
    :run_failed,
    :run_cancelled,
    :checkpoint_committed,
    :node_completed,
    :node_failed,
    :channel_updated,
    :edge_triggered,
    :interrupt_requested,
    :interrupt_resolved
  ]

  @doc false
  @spec types() :: [type()]
  def types, do: @types
end
