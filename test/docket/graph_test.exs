defmodule Docket.GraphTest do
  use ExUnit.Case, async: true

  defmodule Writer do
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  test "builds a canonical graph document with bang pipe helpers" do
    graph =
      Docket.Graph.new!(id: "essay-review", name: "Essay Review")
      |> Docket.Graph.put_input!("topic",
        schema: Docket.Schema.string(),
        required: true
      )
      |> Docket.Graph.put_field!("draft",
        schema: Docket.Schema.string(),
        reducer: Docket.Reducer.last_value()
      )
      |> Docket.Graph.put_node!("writer",
        implementation: Writer,
        config: %{tone: "clear"}
      )
      |> Docket.Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
      |> Docket.Graph.put_edge!("edge_writer_finish", from: "writer", to: "$finish")
      |> Docket.Graph.put_output!("draft", [])

    assert %Docket.Graph{id: "essay-review"} = graph
    refute Map.has_key?(Map.from_struct(graph), :version)
    refute Map.has_key?(Map.from_struct(graph), :joins)
    refute Map.has_key?(Map.from_struct(graph), :branches)
    assert %Docket.Graph.Field{kind: :input, required: true} = graph.inputs["topic"]
    assert %Docket.Graph.Field{kind: :state} = graph.fields["draft"]

    assert %Docket.Graph.Node{implementation: %{module: Writer, function: :call}} =
             graph.nodes["writer"]

    assert graph.nodes["writer"].config == %{tone: "clear"}

    assert %Docket.Graph.Edge{from: "$start", to: "writer"} = graph.edges["edge_start_writer"]
    assert %Docket.Graph.Output{source: "draft"} = graph.outputs["draft"]
    assert Docket.Graph.diagnostics(graph) == []
  end

  test "supports realtime-style node, edge, and field edits" do
    {:ok, graph} = Docket.Graph.new(id: "support-reply")
    {:ok, graph} = Docket.Graph.put_input(graph, "message", schema: Docket.Schema.string())

    {:ok, graph} =
      Docket.Graph.put_node(graph, "draft", %{label: "Draft", config: %{model: "fast"}})

    {:ok, graph} =
      Docket.Graph.put_edge(graph, "edge_start_draft", %{from: "$start", to: "draft"})

    {:ok, graph} = Docket.Graph.update_node(graph, "draft", %{config: %{model: "accurate"}})
    {:ok, graph} = Docket.Graph.put_field(graph, "response", schema: Docket.Schema.string())
    {:ok, graph} = Docket.Graph.delete_edge(graph, "edge_start_draft")

    assert graph.inputs["message"].kind == :input
    assert graph.nodes["draft"].config == %{model: "accurate"}
    refute Map.has_key?(graph.edges, "edge_start_draft")
    assert graph.diagnostics == []
  end

  test "represents joins as multi-source edges and branches as node metadata" do
    approved = Docket.Guard.equals(Docket.Guard.path("review", ["status"]), "approved")
    rejected = Docket.Guard.not(approved)

    graph =
      Docket.Graph.new!(id: "review-flow")
      |> Docket.Graph.put_node!("writer", label: "Writer")
      |> Docket.Graph.put_node!("tester", label: "Tester")
      |> Docket.Graph.put_node!("reviewer",
        label: "Reviewer",
        branches: %{
          "decision" => ["edge_review_approved", "edge_review_rejected"]
        }
      )
      |> Docket.Graph.put_node!("publish", label: "Publish")
      |> Docket.Graph.put_node!("revise", label: "Revise")
      |> Docket.Graph.put_edge!("edge_ready_for_review",
        from: ["writer", "tester"],
        to: "reviewer"
      )
      |> Docket.Graph.put_edge!("edge_review_approved",
        from: "reviewer",
        to: "publish",
        guard: approved
      )
      |> Docket.Graph.put_edge!("edge_review_rejected",
        from: "reviewer",
        to: "revise",
        guard: rejected
      )

    assert %Docket.Graph.Edge{from: ["writer", "tester"], to: "reviewer"} =
             graph.edges["edge_ready_for_review"]

    assert graph.nodes["reviewer"].branches == %{
             "decision" => ["edge_review_approved", "edge_review_rejected"]
           }
  end

  test "keeps public IDs scoped by record kind" do
    graph =
      Docket.Graph.new!(id: "report-flow")
      |> Docket.Graph.put_field!("report", schema: Docket.Schema.map())
      |> Docket.Graph.put_output!("report", source: "report")
      |> Docket.Graph.put_node!("annotate", label: "Annotate")
      |> Docket.Graph.put_edge!("annotate", from: "$start", to: "annotate")

    assert graph.fields["report"].id == "report"
    assert graph.outputs["report"].source == "report"
    assert graph.nodes["annotate"].id == "annotate"
    assert graph.edges["annotate"].id == "annotate"
  end

  test "keeps explicit IDs when update functions return structs" do
    graph =
      Docket.Graph.new!(id: "support-reply")
      |> Docket.Graph.put_node!("draft", label: "Draft")
      |> Docket.Graph.put_edge!("edge_start_draft", from: "$start", to: "draft")
      |> Docket.Graph.update_node!("draft", fn node -> %{node | id: "wrong_node"} end)
      |> Docket.Graph.update_edge!("edge_start_draft", fn edge -> %{edge | id: "wrong_edge"} end)

    assert graph.nodes["draft"].id == "draft"
    assert graph.edges["edge_start_draft"].id == "edge_start_draft"
  end

  test "computes a SHA-256 graph hash from canonical graph content" do
    graph =
      Docket.Graph.new!(id: "support-reply")
      |> Docket.Graph.put_input!("message", schema: Docket.Schema.string())
      |> Docket.Graph.put_node!("draft", %{label: "Draft"})
      |> Docket.Graph.put_edge!("edge_start_draft", %{from: "$start", to: "draft"})

    hash = Docket.Graph.hash(graph)

    assert hash =~ ~r/^[0-9a-f]{64}$/

    graph_with_diagnostics = %{
      graph
      | diagnostics: [
          %Docket.Graph.Diagnostic{
            severity: :warning,
            code: :example,
            message: "ignored"
          }
        ]
    }

    assert Docket.Graph.hash(graph_with_diagnostics) == hash

    renamed_graph = Docket.Graph.update_node!(graph, "draft", %{label: "Draft Reply"})

    assert Docket.Graph.hash(renamed_graph) != hash
  end

  test "keeps guard, schema, and reducer values serializable" do
    guard =
      Docket.Guard.not(Docket.Guard.equals(Docket.Guard.path("review", ["status"]), "approved"))

    assert %Docket.Guard{op: :not} = guard

    assert %Docket.Schema{type: :enum, values: [:low, :medium, :high]} =
             Docket.Schema.enum([:low, :medium, :high], default: :medium)

    assert %Docket.Reducer{type: :last_value} = Docket.Reducer.last_value()
  end

  test "returns tagged errors and bang functions raise for malformed arguments" do
    assert {:error, %Docket.Graph.Error{code: :invalid_public_id}} =
             Docket.Graph.new(id: :not_a_binary)

    assert_raise Docket.Graph.Error, fn ->
      Docket.Graph.new!(id: "has spaces")
    end

    assert {:ok, graph} = Docket.Graph.new(id: "valid")

    assert {:error, %Docket.Graph.Error{code: :reserved_id}} =
             Docket.Graph.put_node(graph, "$start", implementation: Writer)

    assert {:error, %Docket.Graph.Error{code: :invalid_attrs}} =
             Docket.Graph.put_node(graph, "writer", [:not_a_keyword_entry])

    assert {:error, %Docket.Graph.Error{code: :invalid_options}} =
             Docket.Graph.put_node(graph, "writer", [], :not_a_keyword_list)

    assert_raise Docket.Graph.Error, fn ->
      Docket.Graph.put_node!(graph, "$start", implementation: Writer)
    end
  end

  test "verifies through the public graph API" do
    graph = Docket.Graph.new!(id: "stubbed") |> Docket.Graph.put_output!("result", [])

    assert {:error, verified} = Docket.Graph.verify(graph)

    assert %Docket.Graph{id: "stubbed"} = verified
    assert Enum.any?(verified.diagnostics, &(&1.code == :compiler_not_implemented))
  end

  test "graph edits clear stale diagnostics until the graph is verified again" do
    graph =
      Docket.Graph.new!(id: "diagnosed")
      |> Docket.Graph.put_output!("result", [])

    assert {:error, graph} = Docket.Graph.verify(graph)
    assert Enum.any?(graph.diagnostics, &(&1.code == :compiler_not_implemented))

    {:ok, graph} = Docket.Graph.put_output(graph, "result", [])

    assert graph.diagnostics == []
  end
end
