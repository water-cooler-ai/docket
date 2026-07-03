defmodule Docket.Run.TaskState do
  @moduledoc """
  Durable description of one node execution attempt.

  In v1 the superstep is barrier-synchronous, so committed runs never carry
  in-flight tasks; task state appears in event payloads and checkpoint
  metadata. The struct is public so post-v1 async executors can persist
  in-flight work on the run document.
  """

  defstruct [
    :task_id,
    :node_id,
    :step,
    :attempt,
    :status,
    :input_hash,
    :idempotency_key,
    :started_at,
    :deadline_at,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          task_id: String.t(),
          node_id: String.t(),
          step: non_neg_integer(),
          attempt: pos_integer(),
          status: atom() | nil,
          input_hash: String.t() | nil,
          idempotency_key: String.t() | nil,
          started_at: DateTime.t() | nil,
          deadline_at: DateTime.t() | nil,
          metadata: map()
        }
end
