defmodule Docket.Run.InterruptState do
  @moduledoc """
  Committed state of one interrupt on a `Docket.Run`.

  An `:open` interrupt keeps its node paused (the node's public ID stays in
  `run.pending_nodes`); resolving it writes the resolution value to
  `resume_channel` and re-executes the node in the next superstep.
  """

  defstruct [
    :id,
    :node_id,
    :status,
    :resume_channel,
    :prompt,
    :schema,
    :created_at,
    :resolved_at,
    metadata: %{}
  ]

  @type status :: :open | :resolved

  @type t :: %__MODULE__{
          id: String.t(),
          node_id: String.t(),
          status: status(),
          resume_channel: String.t(),
          prompt: String.t() | nil,
          schema: Docket.Schema.t() | nil,
          created_at: DateTime.t() | nil,
          resolved_at: DateTime.t() | nil,
          metadata: map()
        }
end
