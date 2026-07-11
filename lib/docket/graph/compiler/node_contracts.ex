defmodule Docket.Graph.Compiler.NodeContracts do
  @moduledoc false

  # Fetches node config schemas exactly once per compile. The facade builds
  # this map after ingest and hands it to both validation and lowering, so
  # every pass sees the same result even when a config_schema/0 callback is
  # stateful or nondeterministic, and lowering never re-invokes user code.
  #
  # Config schemas cross the same wire boundary as graph schemas, so field
  # keys, defaults, enum values, constraints, and metadata match canonical
  # graph configuration before validation and materialization.

  alias Docket.Graph
  alias Docket.Graph.Serializer
  alias Docket.Schema

  @type fetch_result :: {:ok, Schema.t()} | {:error, map()}

  @spec config_schemas(Graph.t()) :: %{optional(String.t()) => fetch_result()}
  def config_schemas(%Graph{} = graph) do
    implementations =
      for {_id, node} <- graph.nodes,
          is_struct(node, Graph.Node),
          match?(
            %{type: :module, module: module, function: :call} when is_atom(module),
            node.implementation
          ),
          do: node.implementation.module

    schemas_by_module =
      implementations
      |> Enum.uniq()
      |> Enum.sort_by(&Atom.to_string/1)
      |> Map.new(&{&1, fetch_config_schema(&1)})

    for {id, node} <- graph.nodes,
        is_struct(node, Graph.Node),
        match?(
          %{type: :module, module: module, function: :call} when is_atom(module),
          node.implementation
        ),
        Map.has_key?(schemas_by_module, node.implementation.module),
        into: %{} do
      {id, Map.fetch!(schemas_by_module, node.implementation.module)}
    end
  end

  @spec materialize_defaults(Graph.t(), %{optional(String.t()) => fetch_result()}) :: Graph.t()
  def materialize_defaults(%Graph{} = graph, config_schemas) do
    nodes =
      Map.new(graph.nodes, fn
        {id, %Graph.Node{} = node} ->
          config =
            case Map.get(config_schemas, id) do
              {:ok, schema} -> apply_defaults(node.config, schema)
              _missing_or_invalid -> node.config
            end

          {id, %{node | config: config}}

        entry ->
          entry
      end)

    %{graph | nodes: nodes}
  end

  @spec fetch_config_schema(module()) :: fetch_result()
  def fetch_config_schema(module) do
    schema = module.config_schema()

    case schema
         |> Serializer.dump_schema()
         |> Serializer.load_schema!("node config schema") do
      %Schema{} = canonical -> {:ok, canonical}
      _other -> {:error, %{returned: inspect(schema)}}
    end
  rescue
    exception -> {:error, %{error: Exception.message(exception)}}
  catch
    kind, reason -> {:error, %{error: "#{kind}: #{inspect(reason)}"}}
  end

  defp apply_defaults(config, schema) when is_map(config) and not is_struct(config) do
    Enum.reduce(schema.fields, config, fn {key, field_schema}, acc ->
      if field_schema.default == Schema.no_default() do
        acc
      else
        Map.put_new(acc, key, field_schema.default)
      end
    end)
  end

  defp apply_defaults(config, _schema), do: config
end
