defmodule Docket.Graph.Compiler.LoweringTest do
  use Docket.Test.Case, async: true

  alias Docket.Runtime
  alias Docket.{Graph, Reducer, Schema}

  describe "channel lowering" do
    test "inputs lower to last-value input channels" do
      runtime_graph = compile!(Graphs.minimal_linear())

      assert %Runtime.Graph.Channel{
               id: "input:value",
               type: :last_value,
               value_schema: %Schema{type: :string}
             } = runtime_graph.channels["input:value"]
    end

    test "atom enum values normalize consistently with runtime input values" do
      graph =
        Graph.put_input!(Graphs.minimal_linear(), "value",
          schema: Schema.enum([:low]),
          required: true
        )

      runtime_graph = compile!(graph)
      assert runtime_graph.channels["input:value"].value_schema.values == ["low"]

      assert {:ok, atom_run, _checkpoints} =
               Docket.Test.run_inline(graph, %{"value" => :low})

      assert {:ok, string_run, _checkpoints} =
               Docket.Test.run_inline(graph, %{"value" => "low"})

      assert atom_run.output == %{"result" => "low"}
      assert string_run.output == atom_run.output
    end

    test "state fields lower to last-value state channels with their reducer" do
      runtime_graph = compile!(Graphs.minimal_linear())

      assert %Runtime.Graph.Channel{
               id: "state:result",
               type: :last_value,
               value_schema: %Schema{type: :string},
               reducer: %Reducer{type: :last_value}
             } = runtime_graph.channels["state:result"]
    end

    test "state fields without a reducer default to last_value" do
      graph = Graph.put_field!(Graphs.minimal_linear(), "note", schema: Schema.string())
      runtime_graph = compile!(graph)

      assert %Runtime.Graph.Channel{reducer: %Reducer{type: :last_value}} =
               runtime_graph.channels["state:note"]
    end

    test "field defaults are carried onto their channels" do
      runtime_graph = compile!(Graphs.cycle_counter())

      assert runtime_graph.channels["state:count"].default == 0.0
    end

    test "simple edges lower to generated ephemeral activation channels" do
      runtime_graph = compile!(Graphs.simple_edge())

      assert %Runtime.Graph.Channel{
               id: "edge:edge_writer_reviewer",
               type: :ephemeral
             } = runtime_graph.channels["edge:edge_writer_reviewer"]
    end

    test "multi-source edges lower to barrier channels carrying their sources" do
      runtime_graph = compile!(Graphs.multi_source_edge())
      channel = runtime_graph.channels["edge:edge_combine_ready"]

      assert channel.type == :barrier
      assert channel.sources == ["left", "right"]
    end

    test "no branch-specific channels are generated in v1" do
      runtime_graph = compile!(Graphs.branch_group())

      refute Enum.any?(Map.keys(runtime_graph.channels), &String.starts_with?(&1, "branch:"))
    end
  end

  describe "node lowering" do
    test "nodes lower to runtime nodes with module implementations" do
      runtime_graph = compile!(Graphs.minimal_linear())
      node = runtime_graph.nodes["node:copy"]

      assert %Runtime.Graph.Node{
               id: "node:copy",
               public_id: "copy",
               module: Nodes.CopyInput,
               function: :call
             } = node
    end

    test "node config is normalized with config schema defaults applied" do
      graph =
        Graphs.minimal_linear()
        |> Graph.put_node!("styled", implementation: Nodes.WithDefaults, config: %{tone: "warm"})
        |> Graph.put_edge!("edge_copy_styled", from: "copy", to: "styled")

      runtime_graph = compile!(graph)

      assert runtime_graph.nodes["node:styled"].config == %{
               "tone" => "warm",
               "temperature" => 0.5
             }
    end

    test "atom-keyed config schemas apply defaults under canonical string keys" do
      graph =
        Graphs.minimal_linear()
        |> Graph.put_node!("styled",
          implementation: Nodes.AtomKeyedConfigSchema,
          config: %{tone: "warm"}
        )
        |> Graph.put_edge!("edge_copy_styled", from: "copy", to: "styled")

      runtime_graph = compile!(graph)

      assert runtime_graph.nodes["node:styled"].config == %{
               "tone" => "warm",
               "temperature" => 0.5
             }
    end

    test "config defaults and canonicalization are not written back into the public graph" do
      graph =
        Graphs.minimal_linear()
        |> Graph.put_node!("styled", implementation: Nodes.WithDefaults, config: %{tone: "warm"})
        |> Graph.put_edge!("edge_copy_styled", from: "copy", to: "styled")

      compile!(graph)

      # The public document keeps its free-form in-memory shape; string keys
      # and defaults only appear in the derived runtime graph.
      assert graph.nodes["styled"].config == %{tone: "warm"}
    end

    test "publication returns an effective canonical graph with defaults in its hash" do
      graph =
        Graphs.minimal_linear()
        |> Graph.put_node!("styled", implementation: Nodes.WithDefaults, config: %{tone: "warm"})
        |> Graph.put_edge!("edge_copy_styled", from: "copy", to: "styled")

      assert {:ok, effective, runtime} =
               Docket.Graph.Compiler.compile_for_publication(graph)

      assert graph.nodes["styled"].config == %{tone: "warm"}

      assert effective.nodes["styled"].config == %{
               "tone" => "warm",
               "temperature" => 0.5
             }

      assert runtime.graph_hash == durable_hash(effective)
      assert runtime.nodes["node:styled"].config == effective.nodes["styled"].config
    end

    test "publication canonicalizes atom enum values and defaults from node schemas" do
      graph =
        Graphs.minimal_linear()
        |> Graph.put_node!("classified", implementation: Nodes.AtomEnumDefault, config: %{})
        |> Graph.put_edge!("edge_copy_classified", from: "copy", to: "classified")

      assert {:ok, effective, runtime} =
               Docket.Graph.Compiler.compile_for_publication(graph)

      assert effective.nodes["classified"].config == %{"level" => "low"}
      assert runtime.nodes["node:classified"].config == %{"level" => "low"}
    end

    test "targets subscribe to their incoming activation channels" do
      runtime_graph = compile!(Graphs.simple_edge())

      assert runtime_graph.nodes["node:reviewer"].subscribe == ["edge:edge_writer_reviewer"]
      assert runtime_graph.nodes["node:writer"].subscribe == ["edge:edge_start_writer"]
    end

    test "sources reference their outgoing edges by public edge ID" do
      runtime_graph = compile!(Graphs.fanout())

      assert runtime_graph.nodes["node:source"].outgoing_edges ==
               ["edge_source_left", "edge_source_right"]
    end

    test "fan-out lowers one activation channel per edge" do
      runtime_graph = compile!(Graphs.fanout())

      assert Map.has_key?(runtime_graph.channels, "edge:edge_source_left")
      assert Map.has_key?(runtime_graph.channels, "edge:edge_source_right")
      assert runtime_graph.nodes["node:left"].subscribe == ["edge:edge_source_left"]
      assert runtime_graph.nodes["node:right"].subscribe == ["edge:edge_source_right"]
    end

    test "barrier targets subscribe to the barrier channel" do
      runtime_graph = compile!(Graphs.multi_source_edge())

      assert runtime_graph.nodes["node:combine"].subscribe == ["edge:edge_combine_ready"]

      assert "edge_combine_ready" in runtime_graph.nodes["node:left"].outgoing_edges
      assert "edge_combine_ready" in runtime_graph.nodes["node:right"].outgoing_edges
    end
  end

  describe "edge descriptor lowering" do
    test "start edges lower to initial activation intent" do
      runtime_graph = compile!(Graphs.minimal_linear())
      descriptor = runtime_graph.edges["edge_start_copy"]

      assert descriptor.channel_id == "edge:edge_start_copy"
      assert descriptor.from == ["$start"]
      assert descriptor.to == "copy"
      assert descriptor.guard == nil
      refute descriptor.barrier
    end

    test "finish edges lower to terminal intent with no subscriber" do
      runtime_graph = compile!(Graphs.minimal_linear())
      descriptor = runtime_graph.edges["edge_copy_finish"]

      assert descriptor.to == "$finish"
      assert Map.has_key?(runtime_graph.channels, "edge:edge_copy_finish")

      refute Enum.any?(runtime_graph.nodes, fn {_id, node} ->
               "edge:edge_copy_finish" in node.subscribe
             end)
    end

    test "guarded edges carry their guard expression on the descriptor" do
      runtime_graph = compile!(Graphs.guarded_edge())
      descriptor = runtime_graph.edges["edge_premium"]

      assert %Docket.Guard{op: :equals} = descriptor.guard
    end

    test "guard literals normalize through the runtime value boundary" do
      guard = Docket.Guard.equals(Docket.Guard.path("user", ["tier"]), :premium)

      graph =
        Graphs.guarded_edge()
        |> Graph.update_edge!("edge_premium", guard: guard)

      runtime_graph = compile!(graph)

      assert %Docket.Guard{op: :equals, args: [%Docket.Guard{op: :path}, "premium"]} =
               runtime_graph.edges["edge_premium"].guard

      # Compilation returns an effective normalized graph without mutating
      # the authored public document.
      assert %Docket.Guard{args: [_path, :premium]} = graph.edges["edge_premium"].guard
    end

    test "multi-source edges lower to barrier descriptors" do
      runtime_graph = compile!(Graphs.multi_source_edge())
      descriptor = runtime_graph.edges["edge_combine_ready"]

      assert descriptor.barrier
      assert descriptor.from == ["left", "right"]
      assert descriptor.to == "combine"
    end
  end

  describe "output lowering" do
    test "outputs lower to projections over source channels" do
      runtime_graph = compile!(Graphs.minimal_linear())
      projection = runtime_graph.outputs["result"]

      assert projection.runtime_id == "output:result"
      assert projection.source_channel == "state:result"
    end

    test "outputs inherit the source field schema when omitted" do
      runtime_graph = compile!(Graphs.minimal_linear())

      assert %Schema{type: :string} = runtime_graph.outputs["result"].schema
    end

    test "outputs keep an explicit compatible schema" do
      graph =
        Graphs.minimal_linear()
        |> Graph.put_output!("result", schema: Schema.string(metadata: %{"display" => "wide"}))

      runtime_graph = compile!(graph)

      assert runtime_graph.outputs["result"].schema.metadata == %{"display" => "wide"}
    end

    test "outputs can project input channels" do
      graph = Graph.put_output!(Graphs.minimal_linear(), "echoed", source: "value")
      runtime_graph = compile!(graph)

      assert runtime_graph.outputs["echoed"].source_channel == "input:value"
    end
  end

  describe "runtime graph document" do
    test "identifies its source graph and content hash" do
      graph = Graphs.minimal_linear()

      assert {:ok, effective, runtime_graph} =
               Docket.Graph.Compiler.compile_for_publication(graph)

      assert runtime_graph.graph_id == "minimal-linear"
      assert runtime_graph.graph_hash == durable_hash(effective)

      assert runtime_graph.id ==
               "minimal-linear@" <> String.slice(runtime_graph.graph_hash, 0, 12)
    end

    test "normalizes graph policies for the runtime" do
      runtime_graph = compile!(Graphs.cycle_counter())

      assert runtime_graph.policies["max_supersteps"] == 50
    end

    test "compile opts provide the max-supersteps runtime default" do
      graph =
        Graphs.cycle_counter()
        |> Map.update!(:policies, &Map.delete(&1, "max_supersteps"))

      runtime_graph = compile!(graph, max_supersteps: 25)

      assert runtime_graph.policies["max_supersteps"] == 25
    end

    test "publication permits an unbounded cyclic graph" do
      graph =
        Graphs.cycle_counter()
        |> Map.update!(:policies, &Map.delete(&1, "max_supersteps"))

      assert {:ok, effective, runtime_graph} =
               Docket.Graph.Compiler.compile_for_publication(graph)

      refute Map.has_key?(effective.policies, "max_supersteps")
      refute Map.has_key?(runtime_graph.policies, "max_supersteps")
      assert runtime_graph.graph_hash == durable_hash(effective)
    end

    test "an explicit nil policy is replaced by the opts runtime default" do
      graph =
        Graphs.cycle_counter()
        |> Map.update!(:policies, &Map.put(&1, "max_supersteps", nil))

      runtime_graph = compile!(graph, max_supersteps: 25)

      assert runtime_graph.policies["max_supersteps"] == 25
    end

    test "an explicit nil policy normalizes away when no default is given" do
      graph =
        Graphs.minimal_linear()
        |> Map.update!(:policies, &Map.put(&1, "max_supersteps", nil))

      runtime_graph = compile!(graph)

      refute Map.has_key?(runtime_graph.policies, "max_supersteps")
    end
  end

  defp durable_hash(graph) do
    graph
    |> then(&Docket.DurableCodec.encode!(:graph, &1))
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
