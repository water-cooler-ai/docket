defmodule Docket.Runtime.Activation do
  @moduledoc false

  # One planned node attempt for a superstep. Everything here is derived
  # from the previous committed `Docket.Run` only (execution contract §14):
  # a fresh superstep derives identity from committed channels, and a parked
  # superstep resumes it from `Docket.Run.active_tasks`. Either way, an
  # attempt that never commits re-plans with byte-identical task IDs and
  # idempotency keys, so cooperating integrations can deduplicate external
  # effects across crash-resume re-execution.

  defstruct [
    :task_id,
    :node_id,
    :runtime_node_id,
    :step,
    :attempt,
    :input_hash,
    :idempotency_key,
    :snapshot,
    :source_versions,
    :config,
    :timeout_ms,
    :retry
  ]

  @type retry :: %{max_attempts: pos_integer(), backoff_ms: non_neg_integer()}

  @type t :: %__MODULE__{
          task_id: String.t(),
          node_id: String.t(),
          runtime_node_id: String.t(),
          step: non_neg_integer(),
          attempt: pos_integer(),
          input_hash: String.t(),
          idempotency_key: String.t(),
          snapshot: map(),
          source_versions: %{optional(String.t()) => non_neg_integer()},
          config: map(),
          timeout_ms: pos_integer() | nil,
          retry: retry()
        }
end
