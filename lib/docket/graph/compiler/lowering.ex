defmodule Docket.Graph.Compiler.Lowering do
  @moduledoc false

  # Phase 9.11: lowers a validated public graph into the internal runtime
  # graph. Only runs when validation produced no blocking diagnostics, so it
  # may assume record shapes are sound. All iteration is sorted by public ID
  # and generated content is derived only from graph content plus compile
  # opts, keeping compilation deterministic.

  alias Docket.Graph
  alias Docket.Graph.Compiler.{NodeContracts, Policies}
  alias Docket.Graph.Edge
  alias Docket.Reducer
  alias Docket.Runtime

  @hash_prefix_length 12

  @spec run(Graph.t(), %{optional(String.t()) => NodeContracts.fetch_result()}, keyword()) ::
          Runtime.Graph.t()
  def run(%Graph{} = graph, config_schemas, opts) do
    graph_hash = Graph.hash(graph)

    %Runtime.Graph{
      id: "#{graph.id}@#{String.slice(graph_hash, 0, @hash_prefix_length)}",
      graph_id: graph.id,
      graph_hash: graph_hash,
      channels: channels(graph),
      nodes: nodes(graph, config_schemas),
      edges: edge_descriptors(graph),
      outputs: output_projections(graph),
      policies: normalize_policies(graph, opts),
      lowering: lowering_metadata(graph)
    }
  end

  # ---------------------------------------------------------------------------
  # Channels
  # ---------------------------------------------------------------------------

  defp channels(graph) do
    input_channels =
      for {id, field} <- graph.inputs, into: %{} do
        {input_channel_id(id),
         %Runtime.Graph.Channel{
           id: input_channel_id(id),
           type: :last_value,
           value_schema: field.schema,
           default: field.default
         }}
      end

    state_channels =
      for {id, field} <- graph.fields, into: %{} do
        {state_channel_id(id),
         %Runtime.Graph.Channel{
           id: state_channel_id(id),
           type: :last_value,
           value_schema: field.schema,
           reducer: field.reducer || Reducer.last_value(),
           default: field.default
         }}
      end

    edge_channels =
      for {id, edge} <- graph.edges, into: %{} do
        {edge_channel_id(id),
         %Runtime.Graph.Channel{
           id: edge_channel_id(id),
           type: if(barrier?(edge), do: :barrier, else: :ephemeral),
           sources: if(barrier?(edge), do: Enum.sort(edge.from), else: [])
         }}
      end

    input_channels
    |> Map.merge(state_channels)
    |> Map.merge(edge_channels)
  end

  # ---------------------------------------------------------------------------
  # Nodes
  # ---------------------------------------------------------------------------

  defp nodes(graph, config_schemas) do
    for {id, node} <- graph.nodes, into: %{} do
      %{module: module, function: function} = node.implementation

      {node_runtime_id(id),
       %Runtime.Graph.Node{
         id: node_runtime_id(id),
         public_id: id,
         module: module,
         function: function,
         config: config_with_defaults(config_schemas, id, node.config),
         subscribe: subscriptions(graph, id),
         outgoing_edges: outgoing_edges(graph, id),
         policies: node.policies,
         metadata: node.metadata
       }}
    end
  end

  # Config schema defaults are applied here during lowering; they are never
  # written back into the public graph document. The schema comes from the
  # per-compile fetch shared with validation, so user config_schema/0 code is
  # never re-entered here.
  defp config_with_defaults(config_schemas, public_id, config) do
    case Map.get(config_schemas, public_id) do
      {:ok, schema} ->
        Enum.reduce(schema.fields, config, fn {key, field_schema}, acc ->
          if field_schema.default == Docket.Schema.no_default() do
            acc
          else
            Map.put_new(acc, key, field_schema.default)
          end
        end)

      _missing_or_invalid ->
        config
    end
  end

  defp subscriptions(graph, node_id) do
    for {edge_id, %Edge{to: ^node_id}} <- graph.edges do
      edge_channel_id(edge_id)
    end
    |> Enum.sort()
  end

  defp outgoing_edges(graph, node_id) do
    for {edge_id, edge} <- graph.edges, source?(edge, node_id) do
      edge_id
    end
    |> Enum.sort()
  end

  defp source?(%Edge{from: from}, node_id) when is_list(from), do: node_id in from
  defp source?(%Edge{from: from}, node_id), do: from == node_id

  # ---------------------------------------------------------------------------
  # Edge descriptors and output projections
  # ---------------------------------------------------------------------------

  defp edge_descriptors(graph) do
    for {id, edge} <- graph.edges, into: %{} do
      {id,
       %{
         id: id,
         channel_id: edge_channel_id(id),
         from: List.wrap(edge.from),
         to: edge.to,
         guard: edge.guard,
         barrier: barrier?(edge)
       }}
    end
  end

  defp barrier?(%Edge{from: from}), do: is_list(from)

  defp output_projections(graph) do
    for {id, output} <- graph.outputs, into: %{} do
      source_channel =
        if Map.has_key?(graph.inputs, output.source) do
          input_channel_id(output.source)
        else
          state_channel_id(output.source)
        end

      source_field =
        Map.get(graph.inputs, output.source) || Map.fetch!(graph.fields, output.source)

      {id,
       %{
         id: id,
         runtime_id: output_runtime_id(id),
         source_channel: source_channel,
         schema: output.schema || source_field.schema
       }}
    end
  end

  # ---------------------------------------------------------------------------
  # Policies and lowering metadata
  # ---------------------------------------------------------------------------

  # Lowering only runs on graphs that passed validation, so the policy is
  # either valid or unset here; an explicit nil policy normalizes away and
  # the opts runtime default fills the gap when present.
  defp normalize_policies(graph, opts) do
    case Policies.max_supersteps(graph, opts) do
      {:ok, nil} -> Map.delete(graph.policies, Policies.max_supersteps_key())
      {:ok, limit} -> Map.put(graph.policies, Policies.max_supersteps_key(), limit)
      {:invalid, _value} -> graph.policies
    end
  end

  defp lowering_metadata(graph) do
    public_to_runtime = %{
      inputs: id_map(graph.inputs, &input_channel_id/1),
      fields: id_map(graph.fields, &state_channel_id/1),
      nodes: id_map(graph.nodes, &node_runtime_id/1),
      edges: id_map(graph.edges, &edge_channel_id/1),
      outputs: id_map(graph.outputs, &output_runtime_id/1)
    }

    runtime_to_public =
      [
        Enum.map(graph.inputs, fn {id, _record} -> {input_channel_id(id), {:input, id}} end),
        Enum.map(graph.fields, fn {id, _record} -> {state_channel_id(id), {:field, id}} end),
        Enum.map(graph.nodes, fn {id, _record} -> {node_runtime_id(id), {:node, id}} end),
        Enum.map(graph.edges, fn {id, _record} -> {edge_channel_id(id), {:edge, id}} end),
        Enum.map(graph.outputs, fn {id, _record} -> {output_runtime_id(id), {:output, id}} end)
      ]
      |> List.flatten()
      |> Map.new()

    generated =
      Map.new(graph.edges, fn {id, _edge} ->
        {edge_channel_id(id), %{kind: :activation_channel, public_edge_id: id}}
      end)

    branches =
      for {node_id, node} <- graph.nodes, map_size(node.branches) > 0, into: %{} do
        {node_id, node.branches}
      end

    %Runtime.Graph.Lowering{
      public_to_runtime: public_to_runtime,
      runtime_to_public: runtime_to_public,
      generated: generated,
      branches: branches
    }
  end

  defp id_map(collection, id_fun) do
    Map.new(collection, fn {id, _record} -> {id, id_fun.(id)} end)
  end

  # ---------------------------------------------------------------------------
  # Runtime ID policy (compiler design section 11)
  # ---------------------------------------------------------------------------

  defp input_channel_id(id), do: "input:" <> id
  defp state_channel_id(id), do: "state:" <> id
  defp edge_channel_id(id), do: "edge:" <> id
  defp node_runtime_id(id), do: "node:" <> id
  defp output_runtime_id(id), do: "output:" <> id
end
