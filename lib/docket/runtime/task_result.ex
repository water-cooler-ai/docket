defmodule Docket.Runtime.TaskResult do
  @moduledoc false

  # Normalized outcome of one node attempt. The dispatcher folds every node
  # return shape, raise, exit, throw, and timeout into exactly one TaskResult
  # per dispatched activation:
  #
  # - `:ok` / `:interrupt` - final; `value` is the update map or interrupt
  # - `:retry` - the attempt failed retryably with budget remaining; the loop
  #   commits a retry park instead of a barrier
  # - `:error` - permanent or budget-exhausted failure; fails the superstep
  #
  # `attempt` is the attempt this dispatch executed. `failures` records the
  # failed attempt from this dispatch when there was one (both `:retry` and
  # the final `:error`); earlier attempts' failures were already committed
  # in the retry parks that scheduled them.

  defstruct [:task_id, :node_id, :attempt, :status, :value, failures: []]

  @type failure :: %{attempt: pos_integer(), reason: term()}

  @type t :: %__MODULE__{
          task_id: String.t(),
          node_id: String.t(),
          attempt: pos_integer(),
          status: :ok | :interrupt | :retry | :error,
          value: map() | Docket.Interrupt.t() | term(),
          failures: [failure()]
        }
end
