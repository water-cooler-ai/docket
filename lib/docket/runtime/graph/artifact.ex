defmodule Docket.Runtime.Graph.Artifact do
  @moduledoc """
  Versioned, JSON-safe execution artifact for a compiled runtime graph.

  Publication encodes this artifact once. Execution paths hydrate it without
  invoking `Docket.Graph.Compiler`. The canonical `Docket.Graph` document
  remains the editable source of truth; this envelope is immutable derived
  execution data selected by `compiler_abi`.
  """

  alias Docket.Graph.Serializer
  alias Docket.Runtime.Graph
  alias Docket.Runtime.Graph.{Channel, Lowering, Node}

  @format_version 1
  @compiler_abi "docket-runtime-graph/v1"

  @channel_types %{last_value: "last_value", ephemeral: "ephemeral", barrier: "barrier"}
  @channel_types_in Map.new(@channel_types, fn {key, value} -> {value, key} end)

  @public_kinds %{input: "input", field: "field", node: "node", edge: "edge", output: "output"}
  @public_kinds_in Map.new(@public_kinds, fn {key, value} -> {value, key} end)

  @spec compiler_abi() :: String.t()
  def compiler_abi, do: @compiler_abi

  @spec dump(Graph.t()) :: map()
  def dump(%Graph{} = graph) do
    runtime = dump_runtime(graph)

    %{
      "format_version" => @format_version,
      "compiler_abi" => @compiler_abi,
      "graph_id" => graph.graph_id,
      "graph_hash" => graph.graph_hash,
      "artifact_hash" => hash(runtime),
      "runtime" => runtime
    }
  end

  @spec load(map(), String.t(), String.t()) :: {:ok, Graph.t()} | {:error, Docket.Error.t()}
  def load(artifact, graph_id, graph_hash) do
    try do
      {:ok, load!(artifact, graph_id, graph_hash)}
    rescue
      error ->
        {:error,
         Docket.Error.new(:invalid_graph_artifact, "saved graph artifact is invalid",
           reason: error
         )}
    end
  end

  defp load!(artifact, graph_id, graph_hash) when is_map(artifact) do
    assert_keys!(
      artifact,
      ~w(format_version compiler_abi graph_id graph_hash artifact_hash runtime)
    )

    assert!(artifact["format_version"] == @format_version, "unsupported artifact format")
    assert!(artifact["compiler_abi"] == @compiler_abi, "unsupported compiler ABI")
    assert!(artifact["graph_id"] == graph_id, "artifact graph id mismatch")
    assert!(artifact["graph_hash"] == graph_hash, "artifact graph hash mismatch")

    runtime = Map.fetch!(artifact, "runtime")
    assert!(artifact["artifact_hash"] == hash(runtime), "artifact content hash mismatch")

    graph = load_runtime(runtime)
    assert!(graph.graph_id == graph_id, "runtime graph id mismatch")
    assert!(graph.graph_hash == graph_hash, "runtime graph hash mismatch")
    graph
  end

  defp load!(_artifact, _graph_id, _graph_hash),
    do: raise(ArgumentError, "artifact must be a map")

  defp dump_runtime(graph) do
    %{
      "id" => graph.id,
      "graph_id" => graph.graph_id,
      "graph_hash" => graph.graph_hash,
      "channels" => Map.new(graph.channels, fn {id, channel} -> {id, dump_channel(channel)} end),
      "nodes" => Map.new(graph.nodes, fn {id, node} -> {id, dump_node(node)} end),
      "edges" => Map.new(graph.edges, fn {id, edge} -> {id, dump_edge(edge)} end),
      "outputs" => Map.new(graph.outputs, fn {id, output} -> {id, dump_output(output)} end),
      "policies" => durable!(graph.policies),
      "lowering" => dump_lowering(graph.lowering)
    }
  end

  defp dump_channel(%Channel{} = channel) do
    %{
      "id" => channel.id,
      "type" => Map.fetch!(@channel_types, channel.type),
      "value_schema" => Serializer.dump_schema(channel.value_schema),
      "reducer" => Serializer.dump_reducer(channel.reducer),
      "default" => durable!(channel.default),
      "required" => channel.required,
      "sources" => channel.sources,
      "metadata" => durable!(channel.metadata)
    }
  end

  defp dump_node(%Node{} = node) do
    %{
      "id" => node.id,
      "public_id" => node.public_id,
      "module" => Atom.to_string(node.module),
      "function" => Atom.to_string(node.function),
      "config" => durable!(node.config),
      "subscribe" => node.subscribe,
      "outgoing_edges" => node.outgoing_edges,
      "policies" => durable!(node.policies),
      "metadata" => durable!(node.metadata)
    }
  end

  defp dump_edge(edge) do
    %{
      "id" => edge.id,
      "channel_id" => edge.channel_id,
      "from" => edge.from,
      "to" => edge.to,
      "guard" => Serializer.dump_guard(edge.guard),
      "barrier" => edge.barrier
    }
  end

  defp dump_output(output) do
    %{
      "id" => output.id,
      "runtime_id" => output.runtime_id,
      "source_channel" => output.source_channel,
      "schema" => Serializer.dump_schema(output.schema)
    }
  end

  defp dump_lowering(%Lowering{} = lowering) do
    %{
      "public_to_runtime" => %{
        "inputs" => lowering.public_to_runtime.inputs,
        "fields" => lowering.public_to_runtime.fields,
        "nodes" => lowering.public_to_runtime.nodes,
        "edges" => lowering.public_to_runtime.edges,
        "outputs" => lowering.public_to_runtime.outputs
      },
      "runtime_to_public" =>
        Map.new(lowering.runtime_to_public, fn {runtime_id, {kind, public_id}} ->
          {runtime_id, %{"kind" => Map.fetch!(@public_kinds, kind), "id" => public_id}}
        end),
      "generated" =>
        Map.new(lowering.generated, fn {runtime_id, generated} ->
          {runtime_id,
           %{
             "kind" => Atom.to_string(generated.kind),
             "public_edge_id" => generated.public_edge_id
           }}
        end),
      "branches" => durable!(lowering.branches)
    }
  end

  defp load_runtime(runtime) do
    assert_keys!(
      runtime,
      ~w(id graph_id graph_hash channels nodes edges outputs policies lowering)
    )

    %Graph{
      id: fetch_string!(runtime, "id"),
      graph_id: fetch_string!(runtime, "graph_id"),
      graph_hash: fetch_string!(runtime, "graph_hash"),
      channels: load_collection(runtime, "channels", &load_channel/1),
      nodes: load_collection(runtime, "nodes", &load_node/1),
      edges: load_collection(runtime, "edges", &load_edge/1),
      outputs: load_collection(runtime, "outputs", &load_output/1),
      policies: fetch_map!(runtime, "policies"),
      lowering: load_lowering(Map.fetch!(runtime, "lowering"))
    }
  end

  defp load_channel(map) do
    assert_keys!(map, ~w(id type value_schema reducer default required sources metadata))

    %Channel{
      id: fetch_string!(map, "id"),
      type: fetch_enum!(map, "type", @channel_types_in),
      value_schema: Serializer.load_schema!(map["value_schema"], "compiled channel"),
      reducer: Serializer.load_reducer!(map["reducer"], "compiled channel"),
      default: Map.get(map, "default"),
      required: fetch_boolean!(map, "required"),
      sources: fetch_string_list!(map, "sources"),
      metadata: fetch_map!(map, "metadata")
    }
  end

  defp load_node(map) do
    assert_keys!(
      map,
      ~w(id public_id module function config subscribe outgoing_edges policies metadata)
    )

    module = available_module!(fetch_string!(map, "module"))
    assert!(Code.ensure_loaded?(module), "compiled node module is unavailable")
    function = existing_atom!(fetch_string!(map, "function"))
    assert!(function_exported?(module, function, 3), "compiled node function is unavailable")

    %Node{
      id: fetch_string!(map, "id"),
      public_id: fetch_string!(map, "public_id"),
      module: module,
      function: function,
      config: fetch_map!(map, "config"),
      subscribe: fetch_string_list!(map, "subscribe"),
      outgoing_edges: fetch_string_list!(map, "outgoing_edges"),
      policies: fetch_map!(map, "policies"),
      metadata: fetch_map!(map, "metadata")
    }
  end

  defp load_edge(map) do
    assert_keys!(map, ~w(id channel_id from to guard barrier))

    %{
      id: fetch_string!(map, "id"),
      channel_id: fetch_string!(map, "channel_id"),
      from: fetch_string_list!(map, "from"),
      to: fetch_string!(map, "to"),
      guard: Serializer.load_guard!(map["guard"], "compiled edge"),
      barrier: fetch_boolean!(map, "barrier")
    }
  end

  defp load_output(map) do
    assert_keys!(map, ~w(id runtime_id source_channel schema))

    %{
      id: fetch_string!(map, "id"),
      runtime_id: fetch_string!(map, "runtime_id"),
      source_channel: fetch_string!(map, "source_channel"),
      schema: Serializer.load_schema!(map["schema"], "compiled output")
    }
  end

  defp load_lowering(map) do
    assert_keys!(map, ~w(public_to_runtime runtime_to_public generated branches))
    public = Map.fetch!(map, "public_to_runtime")
    assert_keys!(public, ~w(inputs fields nodes edges outputs))

    %Lowering{
      public_to_runtime: %{
        inputs: fetch_map!(public, :inputs, "inputs"),
        fields: fetch_map!(public, :fields, "fields"),
        nodes: fetch_map!(public, :nodes, "nodes"),
        edges: fetch_map!(public, :edges, "edges"),
        outputs: fetch_map!(public, :outputs, "outputs")
      },
      runtime_to_public:
        Map.new(Map.fetch!(map, "runtime_to_public"), fn {runtime_id, value} ->
          assert_keys!(value, ~w(kind id))

          {runtime_id, {fetch_enum!(value, "kind", @public_kinds_in), fetch_string!(value, "id")}}
        end),
      generated:
        Map.new(Map.fetch!(map, "generated"), fn {runtime_id, value} ->
          assert_keys!(value, ~w(kind public_edge_id))
          assert!(value["kind"] == "activation_channel", "unknown generated runtime kind")

          {runtime_id,
           %{kind: :activation_channel, public_edge_id: fetch_string!(value, "public_edge_id")}}
        end),
      branches: fetch_map!(map, "branches")
    }
  end

  defp load_collection(map, key, loader) do
    map
    |> Map.fetch!(key)
    |> Map.new(fn {id, value} ->
      loaded = loader.(value)
      assert!(Map.fetch!(loaded, :id) == id, "compiled collection id mismatch")
      {id, loaded}
    end)
  end

  defp durable!(value) do
    case Docket.Wire.dump_value(value) do
      {:ok, durable} -> durable
      {:error, reason} -> raise ArgumentError, "compiled artifact is not durable: #{reason}"
    end
  end

  defp hash(runtime) do
    runtime
    |> Serializer.canonical_json_encode()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp existing_atom!(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError ->
      raise ArgumentError, "compiled artifact references unknown atom #{inspect(value)}"
  end

  defp available_module!(name) do
    case Enum.find(:code.all_available(), fn {module, _path, _loaded?} ->
           available_module_name(module) == name
         end) do
      {module, _path, _loaded?} when is_atom(module) ->
        module

      {module, _path, _loaded?} when is_list(module) ->
        List.to_atom(module)

      nil ->
        raise ArgumentError, "compiled artifact references unavailable module #{inspect(name)}"
    end
  end

  defp available_module_name(module) when is_atom(module), do: Atom.to_string(module)
  defp available_module_name(module) when is_list(module), do: List.to_string(module)

  defp fetch_string!(map, key) do
    case Map.fetch!(map, key) do
      value when is_binary(value) -> value
      value -> raise ArgumentError, "#{key} must be a string, got: #{inspect(value)}"
    end
  end

  defp fetch_boolean!(map, key) do
    case Map.fetch!(map, key) do
      value when is_boolean(value) -> value
      value -> raise ArgumentError, "#{key} must be a boolean, got: #{inspect(value)}"
    end
  end

  defp fetch_string_list!(map, key) do
    case Map.fetch!(map, key) do
      values when is_list(values) ->
        assert!(Enum.all?(values, &is_binary/1), "#{key} must contain only strings")
        values

      value ->
        raise ArgumentError, "#{key} must be a list, got: #{inspect(value)}"
    end
  end

  defp fetch_map!(map, key) do
    case Map.fetch!(map, key) do
      value when is_map(value) and not is_struct(value) -> value
      value -> raise ArgumentError, "#{key} must be a map, got: #{inspect(value)}"
    end
  end

  defp fetch_map!(map, atom_key, string_key) do
    value = Map.get(map, atom_key, Map.get(map, string_key))
    assert!(is_map(value) and not is_struct(value), "#{string_key} must be a map")
    value
  end

  defp fetch_enum!(map, key, values) do
    case Map.fetch(values, Map.fetch!(map, key)) do
      {:ok, value} -> value
      :error -> raise ArgumentError, "#{key} has an unknown value"
    end
  end

  defp assert_keys!(map, allowed) do
    keys = Map.keys(map)
    unknown = keys -- allowed
    missing = allowed -- keys

    assert!(unknown == [], "compiled artifact has unknown keys: #{inspect(unknown)}")
    assert!(missing == [], "compiled artifact is missing keys: #{inspect(missing)}")
  end

  defp assert!(true, _message), do: :ok
  defp assert!(false, message), do: raise(ArgumentError, message)
end
