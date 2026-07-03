defmodule Docket.Runtime.TaskResult do
  @moduledoc false

  # Final normalized outcome of one node activation. The dispatcher folds
  # every node return shape, raise, exit, throw, timeout, and retry sequence
  # into exactly one TaskResult per activation before the update barrier.
  #
  # `attempt` is the attempt that produced the final outcome. `failures`
  # records every failed attempt (including the final one when status is
  # `:error`) for event construction.

  defstruct [:task_id, :node_id, :attempt, :status, :value, failures: []]

  @type failure :: %{attempt: pos_integer(), reason: term()}

  @type t :: %__MODULE__{
          task_id: String.t(),
          node_id: String.t(),
          attempt: pos_integer(),
          status: :ok | :interrupt | :error,
          value: map() | Docket.Interrupt.t() | term(),
          failures: [failure()]
        }
end
