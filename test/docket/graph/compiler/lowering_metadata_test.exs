defmodule Docket.Graph.Compiler.LoweringMetadataTest do
  use Docket.Test.Case, async: true

  alias Docket.Runtime

  describe "public_to_runtime" do
    test "maps every public record kind to its runtime ID" do
      runtime_graph = compile!(Graphs.minimal_linear())
      mapping = runtime_graph.lowering.public_to_runtime

      assert mapping.inputs == %{"value" => "input:value"}
      assert mapping.fields == %{"result" => "state:result"}
      assert mapping.nodes == %{"copy" => "node:copy"}

      assert mapping.edges == %{
               "edge_start_copy" => "edge:edge_start_copy",
               "edge_copy_finish" => "edge:edge_copy_finish"
             }

      assert mapping.outputs == %{"result" => "output:result"}
    end
  end

  describe "runtime_to_public" do
    test "maps every runtime ID back to tagged public intent" do
      runtime_graph = compile!(Graphs.minimal_linear())
      mapping = runtime_graph.lowering.runtime_to_public

      assert mapping["input:value"] == {:input, "value"}
      assert mapping["state:result"] == {:field, "result"}
      assert mapping["node:copy"] == {:node, "copy"}
      assert mapping["edge:edge_start_copy"] == {:edge, "edge_start_copy"}
      assert mapping["output:result"] == {:output, "result"}
    end

    test "covers every runtime channel and node" do
      runtime_graph = compile!(Graphs.multi_source_edge())
      mapping = runtime_graph.lowering.runtime_to_public

      for runtime_id <- Map.keys(runtime_graph.channels) do
        assert Map.has_key?(mapping, runtime_id),
               "channel #{runtime_id} has no public mapping"
      end

      for runtime_id <- Map.keys(runtime_graph.nodes) do
        assert Map.has_key?(mapping, runtime_id),
               "node #{runtime_id} has no public mapping"
      end
    end
  end

  describe "generated" do
    test "explains generated activation channels" do
      runtime_graph = compile!(Graphs.simple_edge())

      assert runtime_graph.lowering.generated["edge:edge_writer_reviewer"] == %{
               kind: :activation_channel,
               public_edge_id: "edge_writer_reviewer"
             }
    end

    test "explains generated barrier channels" do
      runtime_graph = compile!(Graphs.multi_source_edge())
      entry = runtime_graph.lowering.generated["edge:edge_combine_ready"]

      assert entry.kind == :activation_channel
      assert entry.public_edge_id == "edge_combine_ready"
    end
  end

  describe "branch groups" do
    test "branch groups survive as lowering metadata only" do
      runtime_graph = compile!(Graphs.branch_group())

      assert %Runtime.Graph.Lowering{} = runtime_graph.lowering

      assert runtime_graph.lowering.branches == %{
               "reviewer" => %{"decision" => ["edge_approved", "edge_rejected"]}
             }
    end
  end
end
