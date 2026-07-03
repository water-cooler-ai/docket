defmodule Docket.Graph.Compiler.DeterminismTest do
  use Docket.Test.Case, async: true

  test "compiling the same graph twice yields identical runtime graphs" do
    graph = Graphs.multi_source_edge()

    assert compile!(graph) == compile!(graph)
  end

  test "compile output is independent of graph map insertion order" do
    graph = Graphs.multi_source_edge()

    reordered = %{
      graph
      | nodes: graph.nodes |> Enum.sort_by(fn {id, _node} -> id end, :desc) |> Map.new(),
        edges: graph.edges |> Enum.shuffle() |> Map.new()
    }

    assert compile!(graph) == compile!(reordered)
  end

  test "diagnostic ordering is stable across runs" do
    graph =
      Graphs.invalid_unknown_target()
      |> Graph.put_edge!("edge_ghost_copy", from: "ghost", to: "copy")
      |> Graph.put_node!("stranded", implementation: Nodes.Echo)

    first = verify_error!(graph)
    second = verify_error!(graph)

    assert Enum.map(first, &{&1.code, &1.path}) == Enum.map(second, &{&1.code, &1.path})
  end

  test "compile does not mutate the input graph" do
    graph = Graphs.minimal_linear()
    snapshot = graph

    compile!(graph)
    assert graph == snapshot

    {:ok, verified} = Graph.verify(graph)
    assert %{verified | diagnostics: []} == %{graph | diagnostics: []}
  end

  test "graph hash is independent of diagnostics attached by verify" do
    graph = Graphs.minimal_linear()
    {:ok, verified} = Graph.verify(graph)

    assert Graph.hash(verified) == Graph.hash(graph)
  end
end
