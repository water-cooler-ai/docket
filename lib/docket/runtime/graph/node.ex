defmodule Docket.Runtime.Graph.Node do
  @moduledoc """
  Internal runtime node definition.

  `id` is the namespaced runtime ID (`node:<public_id>`); `public_id` is what
  node callbacks receive in their runtime context. `subscribe` lists runtime
  channel IDs that activate this node; `outgoing_edges` lists public edge IDs
  evaluated after this node completes.
  """

  defstruct [
    :id,
    :public_id,
    :module,
    :function,
    config: %{},
    subscribe: [],
    outgoing_edges: [],
    policies: %{},
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          public_id: String.t(),
          module: module(),
          function: atom(),
          config: map(),
          subscribe: [String.t()],
          outgoing_edges: [String.t()],
          policies: map(),
          metadata: map()
        }
end
