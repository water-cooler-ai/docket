defmodule Docket.Runtime.Graph do
  @moduledoc """
  Internal executable graph materialization produced by `Docket.Graph.Compiler`.

  A runtime graph is ephemeral node-local derived state, never a durable graph
  format. Backend execution vehicles fetch the effective canonical
  `Docket.Graph`, compile it on the executing node, and may reuse this structure
  through a generation-scoped cache. Every runtime ID maps back to public graph
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

  `from` is a list of public node IDs or `["$start"]`; `to` is a public node ID
  or `"$finish"`. `barrier` is true for edges declared with a list-form `from`
  (which may contain a single source). The descriptor is a private map so edge
  lowering can evolve without adding a public document type.
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
