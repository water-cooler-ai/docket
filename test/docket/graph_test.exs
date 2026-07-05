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

  test "accepts schema shorthand across the editing API" do
    graph =
      Docket.Graph.new!(id: "shorthand")
      |> Docket.Graph.put_input!("message", schema: :string, required: true)
      |> Docket.Graph.put_field!("count", schema: {:integer, min: 0})
      |> Docket.Graph.put_field!("tags", schema: {:list, :string})
      |> Docket.Graph.put_output!("count", schema: :integer)
      |> Docket.Graph.update_field!("count", schema: {:integer, min: 1})

    assert %Docket.Schema{type: :string, required: false} = graph.inputs["message"].schema
    assert graph.inputs["message"].required

    assert %Docket.Schema{type: :integer, constraints: %{"min" => 1}} =
             graph.fields["count"].schema

    assert %Docket.Schema{type: :list, item: %Docket.Schema{type: :string}} =
             graph.fields["tags"].schema

    assert %Docket.Schema{type: :integer} = graph.outputs["count"].schema

    reloaded = Docket.Graph.from_map!(Docket.Graph.to_map(graph))
    assert Docket.Graph.hash(reloaded) == Docket.Graph.hash(graph)
  end

  describe "inline field declarations on put_node!" do
    test "materializes inputs and fields as ordinary graph fields" do
      graph =
        Docket.Graph.new!(id: "inline")
        |> Docket.Graph.put_node!("draft",
          implementation: Writer,
          inputs: %{"customer_message" => [schema: :string, required: true]},
          fields: %{
            "draft_response" => :string,
            "llm_usage" => [schema: :map],
            "messages" => [schema: {:list, :map}, reducer: :append]
          }
        )

      assert %Docket.Graph.Field{
               kind: :input,
               required: true,
               schema: %Docket.Schema{type: :string}
             } =
               graph.inputs["customer_message"]

      assert %Docket.Graph.Field{kind: :state, schema: %Docket.Schema{type: :string}} =
               graph.fields["draft_response"]

      assert %Docket.Schema{type: :map} = graph.fields["llm_usage"].schema

      assert %Docket.Graph.Field{
               reducer: %Docket.Reducer{type: :append},
               schema: %Docket.Schema{type: :list, item: %Docket.Schema{type: :map}}
             } = graph.fields["messages"]

      refute Map.has_key?(Map.from_struct(graph.nodes["draft"]), :fields)

      # Materialized fields are ordinary fields: they serialize and hash.
      reloaded = Docket.Graph.from_map!(Docket.Graph.to_map(graph))
      assert Docket.Graph.hash(reloaded) == Docket.Graph.hash(graph)
    end

    test "identical redeclaration is a no-op regardless of order" do
      declare = fn graph, node_id ->
        Docket.Graph.put_node!(graph, node_id,
          implementation: Writer,
          fields: %{"shared" => [schema: {:list, :string}, reducer: :append]}
        )
      end

      graph = Docket.Graph.new!(id: "shared-fields") |> declare.("a") |> declare.("b")

      assert %Docket.Graph.Field{kind: :state} = graph.fields["shared"]
      assert map_size(graph.fields) == 1
    end

    test "conflicting declarations raise instead of overwriting" do
      graph =
        Docket.Graph.new!(id: "conflict")
        |> Docket.Graph.put_field!("shared", schema: :string)

      error =
        assert_raise Docket.Graph.Error, fn ->
          Docket.Graph.put_node!(graph, "n",
            implementation: Writer,
            fields: %{"shared" => :integer}
          )
        end

      assert error.code == :conflicting_field

      assert {:error, %Docket.Graph.Error{code: :conflicting_field}} =
               Docket.Graph.put_node(graph, "n",
                 implementation: Writer,
                 fields: %{"shared" => :integer}
               )
    end

    test "declaring an existing input as a state field conflicts" do
      graph =
        Docket.Graph.new!(id: "kind-conflict")
        |> Docket.Graph.put_input!("message", schema: :string)

      assert_raise Docket.Graph.Error, fn ->
        Docket.Graph.put_node!(graph, "n",
          implementation: Writer,
          fields: %{"message" => :string}
        )
      end
    end

    test "explicit put_field! still updates freely after an inline declaration" do
      graph =
        Docket.Graph.new!(id: "explicit-update")
        |> Docket.Graph.put_node!("n",
          implementation: Writer,
          fields: %{"out" => :string}
        )
        |> Docket.Graph.put_field!("out", schema: :integer)

      assert %Docket.Schema{type: :integer} = graph.fields["out"].schema
    end

    test "deleting the node keeps the materialized fields" do
      graph =
        Docket.Graph.new!(id: "delete-node")
        |> Docket.Graph.put_node!("n",
          implementation: Writer,
          fields: %{"out" => :string}
        )
        |> Docket.Graph.delete_node!("n")

      refute Map.has_key?(graph.nodes, "n")
      assert Map.has_key?(graph.fields, "out")
    end
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

  test "keeps atoms in memory and canonicalizes them to strings at the boundary" do
    atom_schema = Docket.Schema.enum([:low, :medium, :high], default: :medium)

    graph =
      Docket.Graph.new!(id: "sev")
      |> Docket.Graph.put_field!("severity", schema: atom_schema)

    assert graph.fields["severity"].schema.values == [:low, :medium, :high]

    assert %{"values" => ["low", "medium", "high"], "default" => "medium"} =
             Docket.Graph.to_map(graph)["fields"]["severity"]["schema"]

    reloaded = Docket.Graph.from_map!(Docket.Graph.to_map(graph))

    assert reloaded.fields["severity"].schema.values == ["low", "medium", "high"]
    assert Docket.Graph.hash(reloaded) == Docket.Graph.hash(graph)
  end

  defp rich_graph do
    address =
      Docket.Schema.object(%{
        "street" => Docket.Schema.string(),
        "zip" => Docket.Schema.string(required: true)
      })

    profile = Docket.Schema.object(%{"address" => address})
    severity = Docket.Schema.enum(["low", "high"], default: "low")
    explicit_nil = Docket.Schema.string(default: nil)

    guard =
      Docket.Guard.all([
        Docket.Guard.not(
          Docket.Guard.equals(Docket.Guard.path("review", ["status"]), "approved")
        ),
        Docket.Guard.changed("draft")
      ])

    Docket.Graph.new!(
      id: "rich",
      name: "Rich",
      description: "A rich graph",
      metadata: %{"owner" => "team"},
      policies: %{"retries" => 3}
    )
    |> Docket.Graph.put_input!("topic", schema: Docket.Schema.string(), required: true)
    |> Docket.Graph.put_input!("profile", schema: profile)
    |> Docket.Graph.put_field!("draft",
      schema: Docket.Schema.string(),
      reducer: Docket.Reducer.last_value(%{"strategy" => "keep"}),
      metadata: %{"ui" => "hidden"}
    )
    |> Docket.Graph.put_field!("severity", schema: severity)
    |> Docket.Graph.put_field!("note", schema: explicit_nil)
    |> Docket.Graph.put_node!("writer",
      label: "Writer",
      implementation: {Writer, :call},
      config: %{"tone" => "clear"}
    )
    |> Docket.Graph.put_node!("router",
      implementation: %{"model" => "fast", "temperature" => 0.5, type: :llm},
      branches: %{"decision" => ["edge_yes", "edge_no"]}
    )
    |> Docket.Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
    |> Docket.Graph.put_edge!("edge_yes", from: "router", to: "writer", guard: guard)
    |> Docket.Graph.put_edge!("edge_no", from: ["router", "writer"], to: "writer")
    |> Docket.Graph.put_output!("draft", schema: Docket.Schema.string(), label: "Draft")
  end

  test "round-trips a rich graph through to_map/from_map" do
    graph = rich_graph()
    map = Docket.Graph.to_map(graph)

    assert Docket.Graph.from_map!(map) == graph
    assert Docket.Graph.to_map(Docket.Graph.from_map!(map)) == map
    assert map["schema_version"] == 1
    refute Map.has_key?(map, "diagnostics")
  end

  test "round-trips v1.1 schema types and constraints" do
    messages = Docket.Schema.list(Docket.Schema.map(), max_items: 50)
    count = Docket.Schema.integer(min: 0)
    flag = Docket.Schema.boolean(default: false)
    extras = Docket.Schema.object(%{"note" => Docket.Schema.string()}, open: true)

    graph =
      Docket.Graph.new!(id: "typed")
      |> Docket.Graph.put_field!("messages", schema: messages)
      |> Docket.Graph.put_field!("count", schema: count)
      |> Docket.Graph.put_field!("flag", schema: flag)
      |> Docket.Graph.put_field!("extras", schema: extras)

    map = Docket.Graph.to_map(graph)

    assert %{
             "type" => "list",
             "item" => %{"type" => "map"},
             "constraints" => %{"max_items" => 50}
           } =
             map["fields"]["messages"]["schema"]

    assert %{"type" => "integer", "constraints" => %{"min" => 0}} =
             map["fields"]["count"]["schema"]

    assert %{"type" => "boolean", "default" => false} = map["fields"]["flag"]["schema"]

    assert %{"type" => "object", "constraints" => %{"open" => true}} =
             map["fields"]["extras"]["schema"]

    assert Docket.Graph.from_map!(map) == graph
    assert Docket.Graph.to_map(Docket.Graph.from_map!(map)) == map
  end

  test "round-trips v1.1 reducer types and opts" do
    graph =
      Docket.Graph.new!(id: "reduced")
      |> Docket.Graph.put_field!("messages",
        schema: Docket.Schema.list(Docket.Schema.map()),
        reducer: Docket.Reducer.append(unique: true, max_length: 50)
      )
      |> Docket.Graph.put_field!("total",
        schema: Docket.Schema.integer(),
        reducer: Docket.Reducer.sum()
      )
      |> Docket.Graph.put_field!("tags",
        schema: Docket.Schema.list(Docket.Schema.map()),
        reducer: Docket.Reducer.union(by: "id")
      )

    map = Docket.Graph.to_map(graph)

    assert %{"type" => "append", "opts" => %{"unique" => true, "max_length" => 50}} =
             map["fields"]["messages"]["reducer"]

    assert %{"type" => "sum"} = map["fields"]["total"]["reducer"]
    assert %{"type" => "union", "opts" => %{"by" => "id"}} = map["fields"]["tags"]["reducer"]

    assert Docket.Graph.from_map!(map) == graph
    assert Docket.Graph.to_map(Docket.Graph.from_map!(map)) == map
  end

  test "wraps guards nested in plain argument positions with a $guard tag" do
    guard = Docket.Guard.equals(Docket.Guard.path("review", ["status"]), "approved")

    graph =
      Docket.Graph.new!(id: "guarded")
      |> Docket.Graph.put_edge!("e", from: "$start", to: "$finish", guard: guard)

    map = Docket.Graph.to_map(graph)

    assert %{
             "op" => "equals",
             "args" => [
               %{"$guard" => %{"op" => "path", "args" => ["review", ["status"]]}},
               "approved"
             ]
           } = map["edges"]["e"]["guard"]

    assert Docket.Graph.from_map!(map) == graph

    # A plain map value in a guard argument stays a plain map on load.
    plain = put_in(map, ["edges", "e", "guard", "args"], [%{"status" => "x"}, "approved"])

    assert %Docket.Guard{op: :equals, args: [%{"status" => "x"}, "approved"]} =
             Docket.Graph.from_map!(plain).edges["e"].guard
  end

  test "reserves $-prefixed keys in durable content" do
    graph =
      Docket.Graph.new!(id: "reserved")
      |> Docket.Graph.put_node!("n", config: %{"$tag" => 1})

    error = assert_raise Docket.Graph.Error, fn -> Docket.Graph.to_map(graph) end
    assert error.code == :non_durable_value

    doc = %{
      "schema_version" => 1,
      "id" => "reserved",
      "metadata" => %{"$tag" => 1}
    }

    assert {:error, %Docket.Graph.Error{code: :invalid_document}} = Docket.Graph.from_map(doc)
  end

  test "sentinel and explicit-nil schema defaults round-trip distinctly" do
    graph =
      Docket.Graph.new!(id: "defaults")
      |> Docket.Graph.put_field!("a", schema: Docket.Schema.string())
      |> Docket.Graph.put_field!("b", schema: Docket.Schema.string(default: nil))

    map = Docket.Graph.to_map(graph)

    refute Map.has_key?(map["fields"]["a"]["schema"], "default")
    assert Map.fetch(map["fields"]["b"]["schema"], "default") == {:ok, nil}

    reloaded = Docket.Graph.from_map!(map)
    assert reloaded.fields["a"].schema.default == Docket.Schema.no_default()
    assert reloaded.fields["b"].schema.default == nil
    assert reloaded == graph
  end

  test "hash is stable, diagnostic-independent, and content-sensitive" do
    graph = rich_graph()
    hash = Docket.Graph.hash(graph)

    assert hash =~ ~r/^[0-9a-f]{64}$/

    with_diagnostics = %{
      graph
      | diagnostics: [%Docket.Graph.Diagnostic{severity: :warning, code: :x, message: "ignored"}]
    }

    assert Docket.Graph.hash(with_diagnostics) == hash
    assert Docket.Graph.hash(Docket.Graph.from_map!(Docket.Graph.to_map(graph))) == hash

    relabeled = Docket.Graph.update_node!(graph, "writer", %{label: "New Writer"})
    assert Docket.Graph.hash(relabeled) != hash

    guard2 = Docket.Guard.changed("severity")
    reguarded = Docket.Graph.update_edge!(graph, "edge_yes", %{guard: guard2})
    assert Docket.Graph.hash(reguarded) != hash

    reschemad = Docket.Graph.put_field!(graph, "severity", schema: Docket.Schema.string())
    assert Docket.Graph.hash(reschemad) != hash
  end

  test "accepts free-form content in memory and coerces atoms at to_map" do
    graph =
      Docket.Graph.new!(id: "free")
      |> Docket.Graph.metadata!("owner", :team_a)
      |> Docket.Graph.put_node!("n", config: %{model: :fast, tags: [:a, :b]})

    assert graph.metadata == %{"owner" => :team_a}
    assert graph.nodes["n"].config == %{model: :fast, tags: [:a, :b]}

    map = Docket.Graph.to_map(graph)

    assert map["metadata"] == %{"owner" => "team_a"}
    assert map["nodes"]["n"]["config"] == %{"model" => "fast", "tags" => ["a", "b"]}

    # The hash is computed from the canonical document, so it is identical
    # before and after a storage round trip.
    assert Docket.Graph.hash(Docket.Graph.from_map!(map)) == Docket.Graph.hash(graph)
    assert Docket.Graph.to_map(Docket.Graph.from_map!(map)) == map
  end

  test "to_map rejects malformed record content with tagged errors" do
    base = Docket.Graph.new!(id: "nd")

    non_durable = [
      Docket.Graph.metadata!(base, "opts", key: "value"),
      Docket.Graph.put_node!(base, "n", config: %{coords: {1, 2}})
    ]

    for graph <- non_durable do
      error = assert_raise Docket.Graph.Error, fn -> Docket.Graph.to_map(graph) end
      assert error.code == :non_durable_value
    end

    bad_schema = Docket.Graph.put_field!(base, "f", schema: %Docket.Schema{type: :weird})
    error = assert_raise Docket.Graph.Error, fn -> Docket.Graph.to_map(bad_schema) end
    assert error.code == :invalid_schema

    bad_guard =
      Docket.Graph.put_edge!(base, "e",
        from: "$start",
        to: "$finish",
        guard: %Docket.Guard{op: :weird}
      )

    error = assert_raise Docket.Graph.Error, fn -> Docket.Graph.to_map(bad_guard) end
    assert error.code == :invalid_guard
  end

  test "from_map rejects malformed documents" do
    base = Docket.Graph.to_map(Docket.Graph.new!(id: "doc"))

    assert {:error, %Docket.Graph.Error{code: :invalid_document}} =
             Docket.Graph.from_map(Map.delete(base, "schema_version"))

    assert {:error, %Docket.Graph.Error{code: :unsupported_schema_version}} =
             Docket.Graph.from_map(Map.put(base, "schema_version", 99))

    assert {:error, %Docket.Graph.Error{code: :invalid_document}} =
             Docket.Graph.from_map(Map.put(base, "bogus", 1))

    assert {:error, %Docket.Graph.Error{code: :invalid_public_id}} =
             Docket.Graph.from_map(Map.put(base, "id", "has spaces"))

    assert {:error, %Docket.Graph.Error{code: :invalid_document}} =
             Docket.Graph.from_map(%{"schema_version" => 1, "id" => "g", "atom" => %{key: 1}})

    node_doc = %{
      "schema_version" => 1,
      "id" => "g",
      "nodes" => %{"n" => %{"bogus" => 1}}
    }

    assert {:error, %Docket.Graph.Error{code: :invalid_document}} =
             Docket.Graph.from_map(node_doc)
  end

  test "from_map rejects unknown enum-like values" do
    field_doc = fn field ->
      %{"schema_version" => 1, "id" => "g", "fields" => %{"f" => field}}
    end

    assert {:error, %Docket.Graph.Error{code: :invalid_document}} =
             Docket.Graph.from_map(field_doc.(%{"kind" => "unknown"}))

    assert {:error, %Docket.Graph.Error{code: :invalid_document}} =
             Docket.Graph.from_map(
               field_doc.(%{"kind" => "state", "schema" => %{"type" => "unknown"}})
             )

    edge_doc = %{
      "schema_version" => 1,
      "id" => "g",
      "edges" => %{"e" => %{"guard" => %{"op" => "nope", "args" => []}}}
    }

    assert {:error, %Docket.Graph.Error{code: :invalid_document}} =
             Docket.Graph.from_map(edge_doc)
  end

  test "from_map rejects unknown implementation modules without creating atoms" do
    node_doc = %{
      "schema_version" => 1,
      "id" => "g",
      "nodes" => %{
        "n" => %{
          "implementation" => %{
            "type" => "module",
            "module" => "Elixir.Docket.Definitely.Not.A.Real.Module"
          }
        }
      }
    }

    assert {:error, %Docket.Graph.Error{code: :unknown_module}} =
             Docket.Graph.from_map(node_doc)
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
    graph = Docket.Graph.new!(id: "dangling") |> Docket.Graph.put_output!("result", [])

    assert {:error, verified} = Docket.Graph.verify(graph)

    assert %Docket.Graph{id: "dangling"} = verified
    assert Enum.any?(verified.diagnostics, &(&1.code == :unknown_output_source))
  end

  test "graph edits clear stale diagnostics until the graph is verified again" do
    graph =
      Docket.Graph.new!(id: "diagnosed")
      |> Docket.Graph.put_output!("result", [])

    assert {:error, graph} = Docket.Graph.verify(graph)
    assert Enum.any?(graph.diagnostics, &(&1.code == :unknown_output_source))

    {:ok, graph} = Docket.Graph.put_output(graph, "result", [])

    assert graph.diagnostics == []
  end
end
