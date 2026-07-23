defmodule Docket.Interrupt do
  @moduledoc """
  Interrupt request returned by node code to pause a run for external input.

  Nodes may leave `id` nil; the runtime assigns an ID. `resume_channel` must
  name a declared state field: resolving the interrupt writes the resolution
  value to that field through the field's reducer (writing to an
  `append` field accumulates), and the interrupted node re-executes in the
  next superstep with the resolved value visible in its state snapshot.
  """

  defstruct [:id, :node_id, :schema, :resume_channel, metadata: %{}]

  @type t :: %__MODULE__{
          id: String.t() | nil,
          node_id: String.t() | nil,
          schema: Docket.Schema.t() | nil,
          resume_channel: String.t(),
          metadata: map()
        }
end
