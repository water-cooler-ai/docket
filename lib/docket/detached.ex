defmodule Docket.Detached do
  @moduledoc """
  Completion identity for a detached node attempt.

  Names one attempt of one task in one superstep; the completion mutation is
  fenced on the run still holding exactly that task detached at exactly that
  attempt. `idempotency_key` is the same attempt identity the execution
  contract designates for external-effect deduplication.

  Executing systems learn this identity from the database side — a parked
  task discovered after its detach park committed — never from inside a
  running node. A completion arriving before the park commits receives
  `{:error, %Docket.Error{type: :detach_pending}}` and must retry.
  """

  @enforce_keys [:run_id, :node_id, :step, :attempt, :task_id, :idempotency_key]
  defstruct [:run_id, :node_id, :step, :attempt, :task_id, :idempotency_key]

  @type t :: %__MODULE__{
          run_id: String.t(),
          node_id: String.t(),
          step: non_neg_integer(),
          attempt: pos_integer(),
          task_id: String.t(),
          idempotency_key: String.t()
        }
end
