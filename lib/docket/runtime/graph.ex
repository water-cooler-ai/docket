defmodule Docket.Runtime.Graph do
  @moduledoc """
  Internal executable graph materialization produced by `Docket.Graph.Compiler`.

  A runtime graph is derived, never stored as the canonical graph format. Hosts
  persist `Docket.Graph`; the runtime loop consumes this structure. Every
  runtime ID maps back to public graph intent through `lowering`.
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

  Kept as a plain map in v1 (compiler design, open decision 4). `from` is a
  list of public node IDs or `["$start"]`; `to` is a public node ID or
  `"$finish"`. `barrier` is true for multi-source edges.
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
