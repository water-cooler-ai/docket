defmodule Docket.Storage.GraphsTest do
  use ExUnit.Case, async: true

  alias Docket.{Graph, GraphRef, SavedGraph}

  test "the graph capability exposes exact and latest reads" do
    assert Docket.Storage.Graphs.behaviour_info(:callbacks) |> Enum.sort() ==
             [fetch_graph: 3, fetch_latest_graph: 2, save_graph: 4]
  end

  test "a saved graph couples the effective document to its exact reference" do
    graph = Graph.new!(id: "workflow")
    ref = %GraphRef{graph_id: graph.id, graph_hash: String.duplicate("a", 64)}

    assert %SavedGraph{ref: ^ref, graph: ^graph} = SavedGraph.new!(ref, graph)

    assert_raise ArgumentError, ~r/must have the same graph ID/, fn ->
      SavedGraph.new!(%{ref | graph_id: "other"}, graph)
    end
  end
end
