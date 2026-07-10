defmodule Docket.Runtime.TaskResult do
  @moduledoc false

  # Normalized outcome of one node attempt. The dispatcher folds every node
  # return shape, raise, exit, throw, and timeout into exactly one TaskResult
  # per dispatched activation:
  #
  # - `:ok` / `:interrupt` - final; `value` is the update map or interrupt
  # - `:retry` - the attempt failed retryably with budget remaining and
  #   `value` is the failure reason; the loop commits a retry park instead
  #   of a barrier
  # - `:error` - permanent or budget-exhausted failure with the reason in
  #   `value`; fails the superstep
  #
  # `attempt` is the attempt this dispatch executed. Earlier attempts'
  # failures live durably on the parked task state and their events were
  # committed by the retry parks that recorded them.

  defstruct [:task_id, :node_id, :attempt, :status, :value]

  @type t :: %__MODULE__{
          task_id: String.t(),
          node_id: String.t(),
          attempt: pos_integer(),
          status: :ok | :interrupt | :retry | :error,
          value: map() | Docket.Interrupt.t() | term()
        }
end
