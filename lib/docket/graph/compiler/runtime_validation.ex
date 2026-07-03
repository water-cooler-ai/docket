defmodule Docket.Graph.Compiler.RuntimeValidation do
  @moduledoc false

  # Phase 9.12: internal invariant checks over the lowered runtime graph.
  # Failures here are compiler bugs or unsupported graph shapes, but they
  # still surface as diagnostics rather than exceptions.

  alias Docket.Graph
  alias Docket.Runtime

  import Docket.Graph.Compiler.Diagnostics, only: [error: 3]

  @start_id "$start"
  @finish_id "$finish"

  @spec run(Runtime.Graph.t(), Graph.t()) :: [Docket.Graph.Diagnostic.t()]
  def run(%Runtime.Graph{} = runtime_graph, %Graph{} = graph) do
    List.flatten([
      check_runtime_id_collisions(runtime_graph),
      check_subscriptions(runtime_graph),
      check_outgoing_edges(runtime_graph),
      check_public_mapping(runtime_graph, graph),
      check_output_projections(runtime_graph),
      check_start_and_finish(runtime_graph)
    ])
  end

  defp check_runtime_id_collisions(runtime_graph) do
    runtime_ids =
      Map.keys(runtime_graph.channels) ++
        Map.keys(runtime_graph.nodes) ++
        Enum.map(runtime_graph.outputs, fn {_id, projection} -> projection.runtime_id end)

    runtime_ids
    |> Enum.frequencies()
    |> Enum.filter(fn {_runtime_id, count} -> count > 1 end)
    |> Enum.sort()
    |> Enum.map(fn {runtime_id, _count} ->
      error(:runtime_id_collision, "generated runtime ID #{inspect(runtime_id)} is not unique",
        runtime_id: runtime_id
      )
    end)
  end

  defp check_subscriptions(runtime_graph) do
    for {runtime_id, node} <- sorted(runtime_graph.nodes),
        channel_id <- node.subscribe,
        not Map.has_key?(runtime_graph.channels, channel_id) do
      error(
        :missing_runtime_channel,
        "runtime node #{inspect(runtime_id)} subscribes to missing channel #{inspect(channel_id)}",
        runtime_id: channel_id,
        public_id: node.public_id
      )
    end
  end

  defp check_outgoing_edges(runtime_graph) do
    for {runtime_id, node} <- sorted(runtime_graph.nodes),
        edge_id <- node.outgoing_edges,
        dangling_outgoing_edge?(runtime_graph, edge_id) do
      error(
        :lowering_invariant_failed,
        "runtime node #{inspect(runtime_id)} references outgoing edge #{inspect(edge_id)} with no runtime channel",
        runtime_id: runtime_id,
        public_id: edge_id
      )
    end
  end

  defp dangling_outgoing_edge?(runtime_graph, edge_id) do
    case Map.get(runtime_graph.edges, edge_id) do
      nil -> true
      descriptor -> not Map.has_key?(runtime_graph.channels, descriptor.channel_id)
    end
  end

  defp check_public_mapping(runtime_graph, graph) do
    mapping = runtime_graph.lowering.runtime_to_public

    node_diagnostics =
      for {runtime_id, _node} <- sorted(runtime_graph.nodes),
          not public_record?(mapping, runtime_id, graph) do
        error(
          :lowering_invariant_failed,
          "runtime node #{inspect(runtime_id)} does not map back to a public node",
          runtime_id: runtime_id
        )
      end

    channel_diagnostics =
      for {runtime_id, _channel} <- sorted(runtime_graph.channels),
          not public_record?(mapping, runtime_id, graph) do
        error(
          :lowering_invariant_failed,
          "runtime channel #{inspect(runtime_id)} does not map back to public intent",
          runtime_id: runtime_id
        )
      end

    node_diagnostics ++ channel_diagnostics
  end

  defp public_record?(mapping, runtime_id, graph) do
    case Map.get(mapping, runtime_id) do
      {:input, id} -> Map.has_key?(graph.inputs, id)
      {:field, id} -> Map.has_key?(graph.fields, id)
      {:node, id} -> Map.has_key?(graph.nodes, id)
      {:edge, id} -> Map.has_key?(graph.edges, id)
      {:output, id} -> Map.has_key?(graph.outputs, id)
      nil -> false
    end
  end

  defp check_output_projections(runtime_graph) do
    for {output_id, projection} <- sorted(runtime_graph.outputs),
        not Map.has_key?(runtime_graph.channels, projection.source_channel) do
      error(
        :missing_runtime_channel,
        "output #{inspect(output_id)} projects missing channel #{inspect(projection.source_channel)}",
        runtime_id: projection.runtime_id,
        public_id: output_id
      )
    end
  end

  defp check_start_and_finish(runtime_graph) do
    subscribed_channels =
      runtime_graph.nodes
      |> Enum.flat_map(fn {_id, node} -> node.subscribe end)
      |> MapSet.new()

    start_diagnostics =
      for {edge_id, descriptor} <- sorted(runtime_graph.edges),
          descriptor.from == [@start_id],
          descriptor.to != @finish_id,
          not Map.has_key?(runtime_graph.nodes, "node:" <> descriptor.to) do
        error(
          :lowering_invariant_failed,
          "start edge #{inspect(edge_id)} activates missing runtime node #{inspect(descriptor.to)}",
          public_id: edge_id
        )
      end

    finish_diagnostics =
      for {edge_id, descriptor} <- sorted(runtime_graph.edges),
          descriptor.to == @finish_id,
          MapSet.member?(subscribed_channels, descriptor.channel_id) do
        error(
          :lowering_invariant_failed,
          "finish edge #{inspect(edge_id)} must not have node subscriptions",
          public_id: edge_id,
          runtime_id: descriptor.channel_id
        )
      end

    start_diagnostics ++ finish_diagnostics
  end

  defp sorted(map) when is_map(map), do: Enum.sort_by(map, fn {id, _value} -> id end)
end
