defmodule Docket.Run.PendingWrite do
  @moduledoc """
  Completed result of one task in the active superstep, held on
  `Docket.Run.pending_writes` until the superstep's update barrier.

  While sibling tasks are still retrying, completed results are committed
  here so recovery never re-executes them — but they stay invisible to
  channels, guards, and snapshots until the barrier applies the whole
  superstep at once.

  `kind` is `:update` for a validated state-update map or `:interrupt` for
  an interrupt request awaiting barrier registration.
  """

  defstruct [:task_id, :node_id, :attempt, :kind, :value]

  @type t :: %__MODULE__{
          task_id: String.t(),
          node_id: String.t(),
          attempt: pos_integer(),
          kind: :update | :interrupt,
          value: map() | Docket.Interrupt.t()
        }
end
