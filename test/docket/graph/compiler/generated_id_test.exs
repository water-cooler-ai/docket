defmodule Docket.Graph.Compiler.GeneratedIdTest do
  use Docket.Test.Case, async: true

  alias Docket.Graph.Compiler.RuntimeValidation

  # Internal test file: runtime graph self-validation (phase 9.12) guards
  # compiler invariants that well-formed public graphs cannot violate, because
  # runtime ID namespacing keeps kinds disjoint by construction. These tests
  # doctor a lowered graph to prove the self-checks actually fire.

  test "kind-namespaced runtime IDs cannot collide for valid graphs" do
    graph =
      Graphs.minimal_linear()
      |> Graph.put_node!("result", implementation: Nodes.Echo)
      |> Graph.put_edge!("result", from: "copy", to: "result")

    runtime_graph = compile!(graph)

    runtime_ids =
      Map.keys(runtime_graph.channels) ++
        Map.keys(runtime_graph.nodes) ++
        Enum.map(runtime_graph.outputs, fn {_id, projection} -> projection.runtime_id end)

    assert Enum.uniq(runtime_ids) == runtime_ids
  end

  test "self-validation detects runtime ID collisions" do
    runtime_graph = compile!(Graphs.minimal_linear())

    collided =
      put_in(
        runtime_graph.outputs["result"].runtime_id,
        "state:result"
      )

    diagnostics = RuntimeValidation.run(collided, Graphs.minimal_linear())

    assert_diagnostic(diagnostics, :runtime_id_collision, runtime_id: "state:result")
  end

  test "self-validation detects subscriptions to missing channels" do
    runtime_graph = compile!(Graphs.minimal_linear())

    doctored =
      update_in(
        runtime_graph.nodes["node:copy"].subscribe,
        &["edge:edge_ghost" | &1]
      )

    diagnostics = RuntimeValidation.run(doctored, Graphs.minimal_linear())

    assert_diagnostic(diagnostics, :missing_runtime_channel, runtime_id: "edge:edge_ghost")
  end

  test "self-validation detects runtime nodes with no public counterpart" do
    runtime_graph = compile!(Graphs.minimal_linear())
    ghost = %{runtime_graph.nodes["node:copy"] | id: "node:ghost", public_id: "ghost"}
    doctored = %{runtime_graph | nodes: Map.put(runtime_graph.nodes, "node:ghost", ghost)}

    diagnostics = RuntimeValidation.run(doctored, Graphs.minimal_linear())

    assert_diagnostic(diagnostics, :lowering_invariant_failed, runtime_id: "node:ghost")
  end
end
