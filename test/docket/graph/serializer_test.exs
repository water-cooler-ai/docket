defmodule Docket.Graph.SerializerTest do
  use ExUnit.Case, async: true

  alias Docket.Graph
  alias Docket.{Guard, Reducer, Schema}

  defmodule Writer do
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule Tap do
    def run(_state, _config, _context), do: {:ok, %{}}
  end

  defp registry do
    %{"test.writer" => {Writer, :call}, "test.tap" => {Tap, :run}}
  end

  describe "public exports" do
    test "Docket.Graph exports to_map/2, from_map/2, from_map!/2 but not hash" do
      Code.ensure_loaded(Graph)

      for {name, arity} <- [to_map: 2, from_map: 2, from_map!: 2] do
        assert function_exported?(Graph, name, arity)
      end

      refute function_exported?(Graph, :hash, 1)
      refute function_exported?(Graph, :hash, 2)
    end
  end

  describe "full-graph round-trip" do
    defp rich_graph do
      address =
        Schema.object(%{
          "street" => Schema.string(),
          "zip" => Schema.string(required: true)
        })

      profile = Schema.object(%{"address" => address})
      severity = Schema.enum(["low", "high"], default: "low")
      explicit_nil = Schema.string(default: nil)

      guard =
        Guard.all([
          Guard.not(Guard.equals(Guard.path("review", ["status"]), "approved")),
          Guard.changed("draft")
        ])

      Graph.new!(
        id: "rich",
        name: "Rich",
        description: "A rich graph",
        metadata: %{"owner" => "team"},
        policies: %{"retries" => 3}
      )
      |> Graph.put_input!("topic", schema: Schema.string(), required: true)
      |> Graph.put_input!("profile", schema: profile)
      |> Graph.put_field!("draft",
        schema: Schema.string(),
        reducer: Reducer.last_value(%{"strategy" => "keep"}),
        metadata: %{"ui" => "hidden"}
      )
      |> Graph.put_field!("severity", schema: severity)
      |> Graph.put_field!("note", schema: explicit_nil)
      |> Graph.put_node!("writer",
        label: "Writer",
        implementation: {Writer, :call},
        config: %{"tone" => "clear"}
      )
      |> Graph.put_node!("router",
        implementation: %{"type" => "llm", "model" => "fast", "temperature" => 0.5},
        branches: %{"decision" => ["edge_yes", "edge_no"]}
      )
      |> Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
      |> Graph.put_edge!("edge_yes", from: "router", to: "writer", guard: guard)
      |> Graph.put_edge!("edge_no", from: ["router", "writer"], to: "writer")
      |> Graph.put_output!("draft", schema: Schema.string(), label: "Draft")
    end

    test "round-trips a rich graph through to_map/from_map" do
      graph = rich_graph()
      map = Graph.to_map(graph, implementations: registry())

      assert Graph.from_map!(map, implementations: registry()) == graph

      assert Graph.to_map(Graph.from_map!(map, implementations: registry()),
               implementations: registry()
             ) == map

      assert map["schema_version"] == 1
      refute Map.has_key?(map, "diagnostics")
    end
  end

  describe "enum coverage" do
    test "every schema type round-trips" do
      graph =
        Graph.new!(id: "schemas")
        |> Graph.put_field!("string_f", schema: Schema.string())
        |> Graph.put_field!("float_f", schema: Schema.float())
        |> Graph.put_field!("integer_f", schema: Schema.integer())
        |> Graph.put_field!("boolean_f", schema: Schema.boolean())
        |> Graph.put_field!("map_f", schema: Schema.map())
        |> Graph.put_field!("list_f", schema: Schema.list(Schema.string()))
        |> Graph.put_field!("object_f", schema: Schema.object(%{"a" => Schema.string()}))
        |> Graph.put_field!("enum_f", schema: Schema.enum(["a", "b"]))

      map = Graph.to_map(graph)

      dumped_types =
        for id <- ~w(string_f float_f integer_f boolean_f map_f list_f object_f enum_f) do
          map["fields"][id]["schema"]["type"]
        end

      assert dumped_types ==
               ~w(string float integer boolean map list object enum)

      assert Graph.from_map!(map) == graph
    end

    test "every reducer type round-trips" do
      list = Schema.list(Schema.map())

      graph =
        Graph.new!(id: "reducers")
        |> Graph.put_field!("append_f", schema: list, reducer: Reducer.append())
        |> Graph.put_field!("first_f", schema: list, reducer: Reducer.first_value())
        |> Graph.put_field!("last_f", schema: list, reducer: Reducer.last_value())
        |> Graph.put_field!("merge_f", schema: Schema.map(), reducer: Reducer.merge())
        |> Graph.put_field!("sum_f", schema: Schema.integer(), reducer: Reducer.sum())
        |> Graph.put_field!("union_f", schema: list, reducer: Reducer.union())

      map = Graph.to_map(graph)

      dumped_types =
        for id <- ~w(append_f first_f last_f merge_f sum_f union_f) do
          map["fields"][id]["reducer"]["type"]
        end

      assert dumped_types == ~w(append first_value last_value merge sum union)

      assert Graph.from_map!(map) == graph
    end

    test "every guard op round-trips" do
      guards = %{
        "edge_all" => Guard.all([Guard.changed("a"), Guard.changed("b")]),
        "edge_any" => Guard.any([Guard.exists("a"), Guard.exists("b")]),
        "edge_changed" => Guard.changed("draft"),
        "edge_equals" => Guard.equals(Guard.path("review", ["status"]), "approved"),
        "edge_exists" => Guard.exists("draft"),
        "edge_not" => Guard.not(Guard.changed("draft")),
        "edge_path" => Guard.path("review", ["status"]),
        "edge_version" => Guard.version_at_least("draft", 3)
      }

      graph =
        Enum.reduce(guards, Graph.new!(id: "guards"), fn {id, guard}, graph ->
          Graph.put_edge!(graph, id, from: "$start", to: "$finish", guard: guard)
        end)

      map = Graph.to_map(graph)

      dumped_ops =
        for id <- Enum.sort(Map.keys(guards)) do
          map["edges"][id]["guard"]["op"]
        end

      assert Enum.sort(dumped_ops) ==
               ~w(all any changed equals exists not path version_at_least)

      assert Graph.from_map!(map) == graph
    end

    test "wraps guards nested in plain argument positions with a $guard tag" do
      guard = Guard.equals(Guard.path("review", ["status"]), "approved")

      graph =
        Graph.new!(id: "guarded")
        |> Graph.put_edge!("e", from: "$start", to: "$finish", guard: guard)

      map = Graph.to_map(graph)

      assert %{
               "op" => "equals",
               "args" => [
                 %{"$guard" => %{"op" => "path", "args" => ["review", ["status"]]}},
                 "approved"
               ]
             } = map["edges"]["e"]["guard"]

      assert Graph.from_map!(map) == graph

      plain = put_in(map, ["edges", "e", "guard", "args"], [%{"status" => "x"}, "approved"])

      assert %Guard{op: :equals, args: [%{"status" => "x"}, "approved"]} =
               Graph.from_map!(plain).edges["e"].guard
    end
  end

  describe "schema defaults" do
    test "sentinel and explicit-nil defaults round-trip distinctly" do
      graph =
        Graph.new!(id: "defaults")
        |> Graph.put_field!("a", schema: Schema.string())
        |> Graph.put_field!("b", schema: Schema.string(default: nil))

      map = Graph.to_map(graph)

      refute Map.has_key?(map["fields"]["a"]["schema"], "default")
      assert Map.fetch(map["fields"]["b"]["schema"], "default") == {:ok, nil}

      reloaded = Graph.from_map!(map)
      assert reloaded.fields["a"].schema.default == Schema.no_default()
      assert reloaded.fields["b"].schema.default == nil
      assert reloaded == graph
    end
  end

  describe "open content" do
    test "coerces atom keys and values at to_map" do
      graph =
        Graph.new!(id: "free")
        |> Graph.metadata!("owner", :team_a)
        |> Graph.put_node!("n", config: %{model: :fast, tags: [:a, :b]})

      map = Graph.to_map(graph)

      assert map["metadata"] == %{"owner" => "team_a"}
      assert map["nodes"]["n"]["config"] == %{"model" => "fast", "tags" => ["a", "b"]}

      assert Graph.to_map(Graph.from_map!(map)) == map
    end

    test "reserves $-prefixed keys at both boundaries" do
      graph =
        Graph.new!(id: "reserved")
        |> Graph.put_node!("n", config: %{"$tag" => 1})

      error = assert_raise Graph.Error, fn -> Graph.to_map(graph) end
      assert error.code == :non_durable_value

      doc = %{"schema_version" => 1, "id" => "reserved", "metadata" => %{"$tag" => 1}}
      assert {:error, %Graph.Error{code: :invalid_document}} = Graph.from_map(doc)
    end
  end

  describe "to_map rejects malformed record content" do
    test "non-durable, bad schema, and bad guard raise tagged errors" do
      base = Graph.new!(id: "nd")

      non_durable = [
        Graph.metadata!(base, "opts", key: "value"),
        Graph.put_node!(base, "n", config: %{coords: {1, 2}})
      ]

      for graph <- non_durable do
        error = assert_raise Graph.Error, fn -> Graph.to_map(graph) end
        assert error.code == :non_durable_value
      end

      bad_schema = Graph.put_field!(base, "f", schema: %Schema{type: :weird})
      error = assert_raise Graph.Error, fn -> Graph.to_map(bad_schema) end
      assert error.code == :invalid_schema

      bad_guard =
        Graph.put_edge!(base, "e", from: "$start", to: "$finish", guard: %Guard{op: :weird})

      error = assert_raise Graph.Error, fn -> Graph.to_map(bad_guard) end
      assert error.code == :invalid_guard
    end
  end

  describe "from_map rejects malformed documents" do
    test "structural, enum, and version violations" do
      base = Graph.to_map(Graph.new!(id: "doc"))

      assert {:error, %Graph.Error{code: :invalid_document}} =
               Graph.from_map(Map.delete(base, "schema_version"))

      assert {:error, %Graph.Error{code: :unsupported_schema_version}} =
               Graph.from_map(Map.put(base, "schema_version", 99))

      assert {:error, %Graph.Error{code: :invalid_document}} =
               Graph.from_map(Map.put(base, "bogus", 1))

      assert {:error, %Graph.Error{code: :invalid_public_id}} =
               Graph.from_map(Map.put(base, "id", "has spaces"))

      assert {:error, %Graph.Error{code: :invalid_document}} =
               Graph.from_map(%{"schema_version" => 1, "id" => "g", "atom" => %{key: 1}})

      node_doc = %{"schema_version" => 1, "id" => "g", "nodes" => %{"n" => %{"bogus" => 1}}}

      assert {:error, %Graph.Error{code: :invalid_document}} = Graph.from_map(node_doc)
    end

    test "unknown enum-like values" do
      field_doc = fn field ->
        %{"schema_version" => 1, "id" => "g", "fields" => %{"f" => field}}
      end

      assert {:error, %Graph.Error{code: :invalid_document}} =
               Graph.from_map(field_doc.(%{"kind" => "unknown"}))

      assert {:error, %Graph.Error{code: :invalid_document}} =
               Graph.from_map(
                 field_doc.(%{"kind" => "state", "schema" => %{"type" => "unknown"}})
               )

      edge_doc = %{
        "schema_version" => 1,
        "id" => "g",
        "edges" => %{"e" => %{"guard" => %{"op" => "nope", "args" => []}}}
      }

      assert {:error, %Graph.Error{code: :invalid_document}} = Graph.from_map(edge_doc)
    end

    test "malformed tagged guard expression" do
      edge_doc = %{
        "schema_version" => 1,
        "id" => "g",
        "edges" => %{
          "e" => %{
            "guard" => %{
              "op" => "equals",
              "args" => [%{"$guard" => %{"op" => "path", "args" => []}, "extra" => 1}, "x"]
            }
          }
        }
      }

      assert {:error, %Graph.Error{code: :invalid_document}} = Graph.from_map(edge_doc)
    end
  end

  describe "implementation registry" do
    test "round-trips module implementations through the registry" do
      graph =
        Graph.new!(id: "impls")
        |> Graph.put_node!("writer", implementation: Writer)
        |> Graph.put_node!("tap", implementation: {Tap, :run})

      map = Graph.to_map(graph, implementations: registry())

      assert map["nodes"]["writer"]["implementation"] ==
               %{"type" => "module", "implementation" => "test.writer"}

      assert map["nodes"]["tap"]["implementation"] ==
               %{"type" => "module", "implementation" => "test.tap"}

      assert Graph.from_map!(map, implementations: registry()) == graph
    end

    test "round-trips a passthrough (non-module) implementation map" do
      graph =
        Graph.new!(id: "custom")
        |> Graph.put_node!("n", implementation: %{"type" => "llm", "model" => "fast"})

      map = Graph.to_map(graph)
      assert map["nodes"]["n"]["implementation"] == %{"type" => "llm", "model" => "fast"}

      assert Graph.from_map!(map) == graph
    end

    test "rejects a passthrough map that reuses the reserved module tag" do
      for extra <- [%{"handler" => "x"}, %{"implementation" => "foo"}] do
        impl = Map.merge(%{"type" => "module"}, extra)

        graph =
          Graph.new!(id: "reserved")
          |> Graph.put_node!("n", implementation: impl)

        error = assert_raise Graph.Error, fn -> Graph.to_map(graph) end
        assert error.code == :invalid_implementation
      end
    end

    test "agrees with Graph normalization across module implementation shapes" do
      shapes = [Writer, {Writer, :call}, %{type: :module, module: Writer}]

      for impl <- shapes do
        graph =
          Graph.new!(id: "agree")
          |> Graph.put_node!("n", implementation: impl)

        reg = %{"writer" => impl}
        map = Graph.to_map(graph, implementations: reg)

        assert map["nodes"]["n"]["implementation"] ==
                 %{"type" => "module", "implementation" => "writer"}

        assert Graph.from_map!(map, implementations: reg) == graph
      end
    end

    test "nil implementation stays nil" do
      graph = Graph.new!(id: "empty") |> Graph.put_node!("n", label: "N")
      map = Graph.to_map(graph)
      refute Map.has_key?(map["nodes"]["n"], "implementation")
      assert Graph.from_map!(map) == graph
    end

    test "dump of an unregistered module implementation is a typed error" do
      graph =
        Graph.new!(id: "missing")
        |> Graph.put_node!("writer", implementation: Writer)

      error = assert_raise Graph.Error, fn -> Graph.to_map(graph) end
      assert error.code == :unregistered_implementation
    end

    test "load of an unknown identifier is a typed error and creates no atoms" do
      identifier = "totally.unregistered.identifier.xyz"

      doc = %{
        "schema_version" => 1,
        "id" => "g",
        "nodes" => %{
          "n" => %{"implementation" => %{"type" => "module", "implementation" => identifier}}
        }
      }

      assert {:error, %Graph.Error{code: :unknown_implementation}} =
               Graph.from_map(doc, implementations: registry())

      assert_raise ArgumentError, fn -> String.to_existing_atom(identifier) end
    end

    test "passthrough type strings are never converted to atoms" do
      type_string = "unregistered_passthrough_type_qzx"

      doc = %{
        "schema_version" => 1,
        "id" => "g",
        "nodes" => %{"n" => %{"implementation" => %{"type" => type_string}}}
      }

      assert {:ok, graph} = Graph.from_map(doc)
      assert graph.nodes["n"].implementation == %{"type" => type_string}
      assert_raise ArgumentError, fn -> String.to_existing_atom(type_string) end
    end

    test "rejects an ambiguous reverse mapping" do
      ambiguous = %{"a" => Writer, "b" => {Writer, :call}}

      graph = Graph.new!(id: "g")

      error =
        assert_raise Graph.Error, fn -> Graph.to_map(graph, implementations: ambiguous) end

      assert error.code == :invalid_registry
    end

    test "rejects invalid identifiers" do
      graph = Graph.new!(id: "g")

      for bad <- [%{"" => Writer}, %{"$reserved" => Writer}, %{:atom_id => Writer}] do
        error = assert_raise Graph.Error, fn -> Graph.to_map(graph, implementations: bad) end
        assert error.code == :invalid_registry
      end
    end

    test "rejects invalid registry values" do
      graph = Graph.new!(id: "g")

      for bad <- [%{"x" => "not-a-module"}, %{"x" => %{"model" => "fast"}}, %{"x" => nil}] do
        error = assert_raise Graph.Error, fn -> Graph.to_map(graph, implementations: bad) end
        assert error.code == :invalid_registry
      end
    end

    test "rejects a non-map registry" do
      graph = Graph.new!(id: "g")

      error = assert_raise Graph.Error, fn -> Graph.to_map(graph, implementations: [:nope]) end
      assert error.code == :invalid_registry
    end
  end

  describe "from_map error tuple" do
    test "from_map wraps raised errors and from_map! raises" do
      doc = %{"schema_version" => 1, "id" => "has spaces"}

      assert {:error, %Graph.Error{code: :invalid_public_id}} = Graph.from_map(doc)
      assert_raise Graph.Error, fn -> Graph.from_map!(doc) end
    end
  end

  describe "load rejects $-prefixed names dump cannot re-serialize" do
    test "object-schema field named \"$x\" is rejected" do
      doc = %{
        "schema_version" => 1,
        "id" => "g",
        "fields" => %{
          "f" => %{
            "schema" => %{"type" => "object", "fields" => %{"$x" => %{"type" => "string"}}}
          }
        }
      }

      assert {:error, %Graph.Error{code: :invalid_document}} = Graph.from_map(doc)
    end

    test "branch group named \"$b\" is rejected" do
      doc = %{
        "schema_version" => 1,
        "id" => "g",
        "nodes" => %{"n" => %{"branches" => %{"$b" => ["e1"]}}}
      }

      assert {:error, %Graph.Error{code: :invalid_document}} = Graph.from_map(doc)
    end
  end

  describe "improper lists" do
    test "to_map raises on an improper list in node config" do
      graph =
        Graph.new!(id: "improper")
        |> Graph.put_node!("n", config: %{"vals" => [1 | 2]})

      error = assert_raise Graph.Error, fn -> Graph.to_map(graph) end
      assert error.code == :non_durable_value
    end

    test "from_map rejects a hand-built document with an improper list" do
      doc = %{
        "schema_version" => 1,
        "id" => "g",
        "nodes" => %{"n" => %{"config" => %{"vals" => [1 | 2]}}}
      }

      assert {:error, %Graph.Error{code: :non_durable_value}} = Graph.from_map(doc)
    end
  end
end
