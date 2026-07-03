defmodule Docket.Graph.Compiler.ValidationTest do
  use Docket.Test.Case, async: true

  alias Docket.Graph.{Edge, Field, Node, Output}
  alias Docket.{Guard, Reducer, Schema}

  # Compiler validation must not trust edit-time normalization: hosts can load
  # old, manually edited, or externally generated graph documents. Tests that
  # need shapes the editing API rejects build them with direct struct updates.

  describe "public document validation (9.2)" do
    test "rejects unsupported schema versions" do
      graph = %{Graphs.minimal_linear() | schema_version: 99}

      graph
      |> verify_error!()
      |> assert_diagnostic(:unsupported_schema_version, path: [:schema_version])

      assert {:error, _graph} = Compiler.compile(graph)
    end

    test "rejects non-binary graph IDs" do
      %{Graphs.minimal_linear() | id: nil}
      |> verify_error!()
      |> assert_diagnostic(:invalid_public_id, path: [:id])
    end

    test "rejects malformed record IDs on loaded graphs" do
      graph = Graphs.minimal_linear()

      %{
        graph
        | edges: Map.put(graph.edges, "bad id", %Edge{id: "bad id", from: "copy", to: "$finish"})
      }
      |> verify_error!()
      |> assert_diagnostic(:invalid_public_id, path: [:edges, "bad id"], public_id: "bad id")
    end

    test "rejects reserved endpoint IDs used as record IDs" do
      graph = Graphs.minimal_linear()

      %{graph | nodes: Map.put(graph.nodes, "$start", %Node{id: "$start"})}
      |> verify_error!()
      |> assert_diagnostic(:reserved_id, path: [:nodes, "$start"])
    end

    test "rejects input/state field ID collisions" do
      graph = Graphs.minimal_linear()
      shadow = %Field{id: "value", kind: :state, schema: Schema.string()}

      %{graph | fields: Map.put(graph.fields, "value", shadow)}
      |> verify_error!()
      |> assert_diagnostic(:duplicate_state_id, public_id: "value")
    end

    test "rejects non-durable graph content" do
      graph = Graphs.minimal_linear()
      poisoned = %{graph.nodes["copy"] | config: %{"pid" => self()}}

      %{graph | nodes: Map.put(graph.nodes, "copy", poisoned)}
      |> verify_error!()
      |> assert_diagnostic(:non_durable_graph_value)
    end

    test "raises on non-graph arguments instead of returning diagnostics" do
      assert_raise FunctionClauseError, fn -> Compiler.verify(%{not: :a_graph}) end
      assert_raise FunctionClauseError, fn -> Compiler.compile(nil) end
    end
  end

  describe "field, schema, and reducer validation (9.3)" do
    test "requires a schema on state fields" do
      Graphs.minimal_linear()
      |> Graph.put_field!("bare", [])
      |> verify_error!()
      |> assert_diagnostic(:missing_field_schema,
        path: [:fields, "bare", :schema],
        public_id: "bare"
      )
    end

    test "requires a schema on input fields" do
      Graphs.minimal_linear()
      |> Graph.put_input!("bare_input", [])
      |> verify_error!()
      |> assert_diagnostic(:missing_field_schema, path: [:inputs, "bare_input", :schema])
    end

    test "rejects malformed schema values on loaded graphs" do
      graph = Graphs.minimal_linear()
      broken = %{graph.fields["result"] | schema: "not a schema"}

      %{graph | fields: Map.put(graph.fields, "result", broken)}
      |> verify_error!()
      |> assert_diagnostic(:invalid_schema, path: [:fields, "result", :schema])
    end

    test "rejects reducers other than last_value in v1" do
      graph = Graphs.minimal_linear()
      broken = %{graph.fields["result"] | reducer: %Reducer{type: :concat}}

      %{graph | fields: Map.put(graph.fields, "result", broken)}
      |> verify_error!()
      |> assert_diagnostic(:invalid_reducer, path: [:fields, "result", :reducer])
    end

    test "rejects field defaults that fail the field schema" do
      Graphs.minimal_linear()
      |> Graph.put_field!("count", schema: Schema.float(), default: "ten")
      |> verify_error!()
      |> assert_diagnostic(:invalid_field_default, path: [:fields, "count", :default])
    end

    test "accepts fields without reducers by defaulting to last_value" do
      graph =
        Graphs.minimal_linear()
        |> Graph.put_field!("note", schema: Schema.string())

      assert {:ok, _verified} = Graph.verify(graph)
    end
  end

  describe "output validation (9.4)" do
    test "rejects outputs whose source is not an input or state field" do
      Graphs.minimal_linear()
      |> Graph.put_output!("ghost", source: "missing")
      |> verify_error!()
      |> assert_diagnostic(:unknown_output_source,
        path: [:outputs, "ghost", :source],
        public_id: "ghost"
      )
    end

    test "rejects output schemas incompatible with the source field" do
      Graphs.minimal_linear()
      |> Graph.put_output!("result", schema: Schema.float())
      |> verify_error!()
      |> assert_diagnostic(:incompatible_output_schema, path: [:outputs, "result", :schema])
    end

    test "accepts outputs that omit their schema" do
      graph = Graphs.minimal_linear()
      assert graph.outputs["result"].schema == nil
      assert {:ok, _verified} = Graph.verify(graph)
    end
  end

  describe "node validation (9.5)" do
    test "requires an implementation" do
      Graphs.minimal_linear()
      |> Graph.put_node!("bare", [])
      |> Graph.put_edge!("edge_copy_bare", from: "copy", to: "bare")
      |> verify_error!()
      |> assert_diagnostic(:missing_node_implementation,
        path: [:nodes, "bare", :implementation],
        public_id: "bare"
      )
    end

    test "rejects non-module implementation types" do
      Graphs.minimal_linear()
      |> Graph.update_node!("copy", implementation: %{type: :llm, model: "fast"})
      |> verify_error!()
      |> assert_diagnostic(:unsupported_node_implementation,
        path: [:nodes, "copy", :implementation]
      )
    end

    test "rejects callback functions other than call in v1" do
      Graphs.minimal_linear()
      |> Graph.update_node!("copy", implementation: {Nodes.CopyInput, :run})
      |> verify_error!()
      |> assert_diagnostic(:unsupported_node_implementation,
        path: [:nodes, "copy", :implementation]
      )
    end

    test "rejects implementation modules that cannot be loaded" do
      Graphs.minimal_linear()
      |> Graph.update_node!("copy", implementation: Docket.Test.Fixtures.Nodes.DoesNotExist)
      |> verify_error!()
      |> assert_diagnostic(:node_module_not_loaded, path: [:nodes, "copy", :implementation])
    end

    test "rejects modules that do not export config_schema/0 and call/3" do
      Graphs.minimal_linear()
      |> Graph.update_node!("copy", implementation: Nodes.NotANode)
      |> verify_error!()
      |> assert_diagnostic(:invalid_node_implementation, path: [:nodes, "copy", :implementation])
    end

    test "surfaces raising config_schema callbacks as diagnostics, not crashes" do
      Graphs.minimal_linear()
      |> Graph.update_node!("copy", implementation: Nodes.RaisingConfigSchema, config: %{})
      |> verify_error!()
      |> assert_diagnostic(:invalid_node_config_schema, public_id: "copy")
    end

    test "rejects config_schema callbacks that return malformed schemas" do
      Graphs.minimal_linear()
      |> Graph.update_node!("copy", implementation: Nodes.MalformedConfigSchema, config: %{})
      |> verify_error!()
      |> assert_diagnostic(:invalid_node_config_schema, public_id: "copy")
    end

    test "rejects config with keys the config schema does not declare" do
      Graphs.unknown_config_field()
      |> verify_error!()
      |> assert_diagnostic(:invalid_node_config,
        path: [:nodes, "copy", :config],
        public_id: "copy"
      )
    end

    test "rejects config missing required keys" do
      Graphs.minimal_linear()
      |> Graph.update_node!("copy", config: %{from: "value"})
      |> verify_error!()
      |> assert_diagnostic(:invalid_node_config, path: [:nodes, "copy", :config])
    end

    test "rejects config values that fail the config schema" do
      Graphs.minimal_linear()
      |> Graph.put_node!("styled",
        implementation: Nodes.WithDefaults,
        config: %{tone: "warm", temperature: "hot"}
      )
      |> Graph.put_edge!("edge_copy_styled", from: "copy", to: "styled")
      |> verify_error!()
      |> assert_diagnostic(:invalid_node_config, path: [:nodes, "styled", :config])
    end
  end

  describe "edge validation (9.6)" do
    test "rejects unknown edge sources" do
      Graphs.minimal_linear()
      |> Graph.put_edge!("edge_ghost_copy", from: "ghost", to: "copy")
      |> verify_error!()
      |> assert_diagnostic(:unknown_edge_source,
        path: [:edges, "edge_ghost_copy", :from],
        public_id: "edge_ghost_copy"
      )
    end

    test "rejects unknown edge targets" do
      Graphs.invalid_unknown_target()
      |> verify_error!()
      |> assert_diagnostic(:unknown_edge_target, path: [:edges, "edge_copy_ghost", :to])
    end

    test "rejects empty multi-source edges" do
      Graphs.minimal_linear()
      |> Graph.put_edge!("edge_empty", from: [], to: "copy")
      |> verify_error!()
      |> assert_diagnostic(:empty_edge_sources, path: [:edges, "edge_empty", :from])
    end

    test "rejects duplicate sources in multi-source edges" do
      Graphs.multi_source_edge()
      |> Graph.update_edge!("edge_combine_ready", from: ["left", "left"])
      |> verify_error!()
      |> assert_diagnostic(:duplicate_edge_source, path: [:edges, "edge_combine_ready", :from])
    end

    test "rejects $start as an edge target" do
      Graphs.minimal_linear()
      |> Graph.put_edge!("edge_back_to_start", from: "copy", to: "$start")
      |> verify_error!()
      |> assert_diagnostic(:invalid_start_endpoint, path: [:edges, "edge_back_to_start", :to])
    end

    test "rejects $finish as an edge source" do
      Graphs.minimal_linear()
      |> Graph.put_edge!("edge_from_finish", from: "$finish", to: "copy")
      |> verify_error!()
      |> assert_diagnostic(:invalid_finish_endpoint, path: [:edges, "edge_from_finish", :from])
    end

    test "rejects $start inside a multi-source list" do
      Graphs.multi_source_edge()
      |> Graph.update_edge!("edge_combine_ready", from: ["$start", "left"])
      |> verify_error!()
      |> assert_diagnostic(:invalid_start_endpoint, path: [:edges, "edge_combine_ready", :from])
    end

    test "rejects guards that are not Docket.Guard expressions on loaded graphs" do
      graph = Graphs.minimal_linear()
      broken = %{graph.edges["edge_copy_finish"] | guard: %{"op" => "equals"}}

      %{graph | edges: Map.put(graph.edges, "edge_copy_finish", broken)}
      |> verify_error!()
      |> assert_diagnostic(:invalid_guard, path: [:edges, "edge_copy_finish", :guard])
    end

    test "allows self-loops when a max-supersteps policy bounds them" do
      graph =
        Graphs.minimal_linear()
        |> Graph.put_edge!("edge_copy_copy",
          from: "copy",
          to: "copy",
          guard: Guard.changed("result")
        )
        |> Graph.policy!("max_supersteps", 10)

      assert {:ok, _verified} = Graph.verify(graph)
    end
  end

  describe "branch group validation (9.7)" do
    test "rejects branch groups referencing unknown edges" do
      Graphs.branch_group()
      |> Graph.update_node!("reviewer", branches: %{"decision" => ["edge_missing"]})
      |> verify_error!()
      |> assert_diagnostic(:unknown_branch_edge,
        path: [:nodes, "reviewer", :branches, "decision"],
        public_id: "reviewer"
      )
    end

    test "rejects branch groups referencing edges from other nodes" do
      Graphs.branch_group()
      |> Graph.update_node!("reviewer",
        branches: %{"decision" => ["edge_approved", "edge_start_reviewer"]}
      )
      |> verify_error!()
      |> assert_diagnostic(:branch_edge_source_mismatch,
        path: [:nodes, "reviewer", :branches, "decision"]
      )
    end

    test "rejects the same edge in two branch groups" do
      Graphs.branch_group()
      |> Graph.update_node!("reviewer",
        branches: %{
          "decision" => ["edge_approved", "edge_rejected"],
          "second" => ["edge_approved"]
        }
      )
      |> verify_error!()
      |> assert_diagnostic(:duplicate_branch_edge, path: [:nodes, "reviewer", :branches])
    end

    test "warns on unguarded grouped edges instead of failing" do
      {:ok, verified} =
        Graphs.branch_group()
        |> Graph.update_edge!("edge_approved", guard: nil)
        |> Graph.verify()

      assert_diagnostic(verified, :unguarded_branch_edge,
        severity: :warning,
        public_id: "edge_approved"
      )
    end
  end

  describe "guard validation (9.8)" do
    test "rejects unknown guard ops on loaded graphs" do
      graph = Graphs.minimal_linear()
      broken = %{graph.edges["edge_copy_finish"] | guard: %Guard{op: :regex, args: ["value"]}}

      %{graph | edges: Map.put(graph.edges, "edge_copy_finish", broken)}
      |> verify_error!()
      |> assert_diagnostic(:invalid_guard, path: [:edges, "edge_copy_finish", :guard])
    end

    test "rejects guard channel references that do not resolve to fields" do
      Graphs.invalid_guard()
      |> verify_error!()
      |> assert_diagnostic(:unknown_guard_field, path: [:edges, "edge_copy_finish", :guard])
    end

    test "rejects guard references nested inside all/any/not" do
      nested = Guard.all([Guard.changed("value"), Guard.not(Guard.exists("missing"))])

      Graphs.minimal_linear()
      |> Graph.update_edge!("edge_copy_finish", guard: nested)
      |> verify_error!()
      |> assert_diagnostic(:unknown_guard_field, path: [:edges, "edge_copy_finish", :guard])
    end

    test "rejects path segments that are not strings, atoms, or integers" do
      Graphs.minimal_linear()
      |> Graph.update_edge!("edge_copy_finish",
        guard: Guard.equals(Guard.path("value", [1.5]), "x")
      )
      |> verify_error!()
      |> assert_diagnostic(:invalid_guard_path, path: [:edges, "edge_copy_finish", :guard])
    end
  end

  describe "topology validation (9.9)" do
    test "requires at least one edge from $start" do
      Graphs.minimal_linear()
      |> Graph.delete_edge!("edge_start_copy")
      |> verify_error!()
      |> assert_diagnostic(:no_entrypoint, path: [:edges])
    end

    test "rejects nodes unreachable from $start" do
      Graphs.minimal_linear()
      |> Graph.put_node!("stranded", implementation: Nodes.Echo)
      |> verify_error!()
      |> assert_diagnostic(:unreachable_node, path: [:nodes, "stranded"], public_id: "stranded")
    end

    test "a barrier target is unreachable unless all its sources are reachable" do
      diagnostics =
        Graphs.multi_source_edge()
        |> Graph.delete_edge!("edge_source_right")
        |> verify_error!()

      assert_diagnostic(diagnostics, :unreachable_node, public_id: "right")
      assert_diagnostic(diagnostics, :unreachable_node, public_id: "combine")
    end
  end

  describe "cycle analysis (9.10)" do
    test "rejects cycles without a max-supersteps limit" do
      Graphs.cycle_counter()
      |> Map.update!(:policies, &Map.delete(&1, "max_supersteps"))
      |> verify_error!()
      |> assert_diagnostic(:unbounded_cycle)
    end

    test "accepts cycles bounded by graph policy" do
      assert {:ok, _verified} = Graph.verify(Graphs.cycle_counter())
    end

    test "accepts cycles bounded by a runtime default from compile opts" do
      graph =
        Graphs.cycle_counter()
        |> Map.update!(:policies, &Map.delete(&1, "max_supersteps"))

      assert {:ok, _verified} = Graph.verify(graph, max_supersteps: 25)
    end

    test "warns on bounded cycles with no guarded edge" do
      {:ok, verified} =
        Graphs.cycle_counter()
        |> Graph.update_edge!("edge_loop", guard: nil)
        |> Graph.update_edge!("edge_done", guard: nil)
        |> Graph.verify()

      assert_diagnostic(verified, :unguarded_cycle, severity: :warning)
    end
  end

  describe "verify/compile contract" do
    test "all baseline fixtures verify cleanly" do
      for fixture <- [
            Graphs.minimal_linear(),
            Graphs.simple_edge(),
            Graphs.fanout(),
            Graphs.multi_source_edge(),
            Graphs.guarded_edge(),
            Graphs.branch_group(),
            Graphs.cycle_counter()
          ] do
        assert {:ok, verified} = Graph.verify(fixture)
        refute_error_diagnostics(verified.diagnostics)
      end
    end

    test "verify and compile share the same validation rules" do
      graph = Graphs.invalid_unknown_target()

      assert {:error, verified} = Graph.verify(graph)
      assert {:error, compiled} = Compiler.compile(graph)

      assert Enum.map(verified.diagnostics, &{&1.code, &1.path}) ==
               Enum.map(compiled.diagnostics, &{&1.code, &1.path})
    end

    test "verification ignores stale diagnostics and returns fresh ones" do
      stale = %Docket.Graph.Diagnostic{severity: :error, code: :stale, message: "old"}
      graph = %{Graphs.minimal_linear() | diagnostics: [stale]}

      assert {:ok, verified} = Graph.verify(graph)
      refute Enum.any?(verified.diagnostics, &(&1.code == :stale))
    end

    test "verification failure attaches at least one error diagnostic" do
      diagnostics = verify_error!(Graphs.invalid_unknown_target())
      assert Enum.any?(diagnostics, &(&1.severity == :error))
    end
  end
end
