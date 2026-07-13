defmodule Docket.SavedGraph do
  @moduledoc """
  One saved graph document together with its stable content address.

  The graph is the effective durable document produced during publication.
  Its `ref` identifies the exact saved version and can be passed to APIs that
  require a content-addressed graph.
  """

  @enforce_keys [:ref, :graph]
  defstruct [:ref, :graph]

  @type t :: %__MODULE__{
          ref: Docket.GraphRef.t(),
          graph: Docket.Graph.t()
        }

  @doc """
  Builds a saved graph projection.

  The reference and document must name the same graph. Storage implementations
  remain responsible for verifying that `graph_hash` addresses the encoded
  durable document before constructing this value.
  """
  @spec new!(Docket.GraphRef.t(), Docket.Graph.t()) :: t()
  def new!(
        %Docket.GraphRef{graph_id: graph_id} = ref,
        %Docket.Graph{id: graph_id} = graph
      )
      when is_binary(graph_id) do
    %__MODULE__{ref: ref, graph: graph}
  end

  def new!(ref, graph) do
    raise ArgumentError,
          "saved graph reference and document must have the same graph ID, got: " <>
            "#{inspect(ref)} and #{inspect(graph)}"
  end
end
