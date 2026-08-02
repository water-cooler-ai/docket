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

  test "compile identity is independent of diagnostics attached by verify" do
    graph = Graphs.minimal_linear()
    {:ok, verified} = Graph.verify(graph)

    assert compile!(verified).graph_hash == compile!(graph).graph_hash
  end

  test "detached graphs compile to identical runtime graphs and hashes" do
    graph = Graphs.detached_linear()

    assert compile!(graph) == compile!(graph)
  end

  test "inline schema defaults materialize into the hash like module schema defaults" do
    implicit = Graphs.detached_linear()

    explicit =
      Graph.update_node!(implicit, "summarize",
        config: %{"endpoint" => "summarize", "style" => "terse"}
      )

    assert compile!(implicit).graph_hash == compile!(explicit).graph_hash
  end

  test "changing a detach policy changes the graph hash" do
    base = Graphs.detached_linear()

    changed =
      Graph.update_node!(base, "summarize",
        policies: %{"detach" => %{"start_to_close_ms" => 120_000}}
      )

    refute compile!(base).graph_hash == compile!(changed).graph_hash
  end

  test "changing a detached node's config changes the graph hash" do
    base = Graphs.detached_linear()
    changed = Graph.update_node!(base, "summarize", config: %{"endpoint" => "translate"})

    refute compile!(base).graph_hash == compile!(changed).graph_hash
  end
end
