defmodule Docket.Checkpoint do
  @moduledoc """
  Read-only description of an already committed runtime transition.

  Production backends persist the run, events, and scheduling state directly
  inside their transaction. A checkpoint is constructed only after that
  transaction succeeds and may be delivered to `Docket.Checkpoint.Observer`
  callbacks. Observers cannot veto or participate in persistence.

  `Docket.Test` also returns checkpoint values so processless graph-semantics
  tests can inspect transition order and contents.
  """

  defstruct [:type, :seq, :run, :created_at, events: [], metadata: %{}]

  @type type ::
          :run_initialized
          | :step_committed
          | :retry_scheduled
          | :interrupt_requested
          | :interrupt_resolved
          | :run_completed
          | :run_failed
          | :run_cancelled

  @type t :: %__MODULE__{
          type: type(),
          seq: pos_integer(),
          run: Docket.Run.t(),
          events: [Docket.Event.t()],
          created_at: DateTime.t(),
          metadata: map()
        }

  @types [
    :run_initialized,
    :step_committed,
    :retry_scheduled,
    :interrupt_requested,
    :interrupt_resolved,
    :run_completed,
    :run_failed,
    :run_cancelled
  ]

  @doc false
  @spec types() :: [type()]
  def types, do: @types
end
