defmodule Docket.Runtime.Graph do
  @moduledoc """
  Internal executable graph materialization produced by `Docket.Graph.Compiler`.

  A runtime graph is ephemeral node-local derived state, never a durable graph
  format. The planned operational vehicle will fetch the effective canonical
  `Docket.Graph`, compile it once on the executing node, and reuse this
  structure for its claim drain. Every runtime ID maps back to public graph
  intent through `lowering`.
  """

  alias Docket.Runtime.Graph.{Channel, Lowering, Node}

  defstruct [
    :id,
    :graph_id,
    :graph_hash,
    channels: %{},
    nodes: %{},
    edges: %{},
    outputs: %{},
    policies: %{},
    lowering: %Lowering{}
  ]

  @typedoc """
  Runtime edge descriptor.

  Kept as a plain map in v0.1 (compiler design, open decision 4). `from` is a
  list of public node IDs or `["$start"]`; `to` is a public node ID or
  `"$finish"`. `barrier` is true for edges declared with a list-form `from`
  (which may contain a single source).
  """
  @type edge_descriptor :: %{
          id: String.t(),
          channel_id: String.t(),
          from: [String.t()],
          to: String.t(),
          guard: Docket.Guard.t() | nil,
          barrier: boolean()
        }

  @typedoc """
  Output projection over a committed input or state channel.
  """
  @type output_projection :: %{
          id: String.t(),
          runtime_id: String.t(),
          source_channel: String.t(),
          schema: Docket.Schema.t() | nil
        }

  @type t :: %__MODULE__{
          id: String.t(),
          graph_id: String.t(),
          graph_hash: String.t() | nil,
          channels: %{optional(String.t()) => Channel.t()},
          nodes: %{optional(String.t()) => Node.t()},
          edges: %{optional(String.t()) => edge_descriptor()},
          outputs: %{optional(String.t()) => output_projection()},
          policies: map(),
          lowering: Lowering.t()
        }
end
