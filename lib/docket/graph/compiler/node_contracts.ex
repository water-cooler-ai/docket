defmodule Docket.Graph.Compiler.NodeContracts do
  @moduledoc false

  # Fetches node config schemas exactly once per compile. The facade builds
  # this map after ingest and hands it to both validation and lowering, so
  # every pass sees the same result even when a config_schema/0 callback is
  # stateful or nondeterministic, and lowering never re-invokes user code.
  #
  # Config schemas are canonicalized on fetch: object field keys become
  # strings, matching the canonical (wire-format) shape of node config. A
  # schema whose shape cannot be canonicalized (non-Schema field values,
  # unknown types, non atom/binary keys) is reported as invalid rather than
  # crashing later passes.

  alias Docket.Graph
  alias Docket.Schema

  @schema_types [:string, :float, :map, :object, :enum]

  @type fetch_result :: {:ok, Schema.t()} | {:error, map()}

  @spec config_schemas(Graph.t()) :: %{optional(String.t()) => fetch_result()}
  def config_schemas(%Graph{} = graph) do
    for {id, node} <- graph.nodes,
        is_struct(node, Graph.Node),
        match?(
          %{type: :module, module: module, function: :call} when is_atom(module),
          node.implementation
        ),
        into: %{} do
      {id, fetch_config_schema(node.implementation.module)}
    end
  end

  @spec fetch_config_schema(module()) :: fetch_result()
  def fetch_config_schema(module) do
    returned = module.config_schema()

    case canonicalize_schema(returned) do
      {:ok, schema} -> {:ok, schema}
      :error -> {:error, %{returned: inspect(returned)}}
    end
  rescue
    exception -> {:error, %{error: Exception.message(exception)}}
  catch
    kind, reason -> {:error, %{error: "#{kind}: #{inspect(reason)}"}}
  end

  defp canonicalize_schema(%Schema{type: type} = schema) when type in @schema_types do
    with {:ok, fields} <- canonicalize_fields(schema.fields),
         {:ok, item} <- canonicalize_item(schema.item) do
      {:ok, %{schema | fields: fields, item: item}}
    end
  end

  defp canonicalize_schema(_other), do: :error

  defp canonicalize_fields(fields) when is_map(fields) and not is_struct(fields) do
    Enum.reduce_while(fields, {:ok, %{}}, fn
      {key, %Schema{} = child}, {:ok, acc} when is_atom(key) or is_binary(key) ->
        case canonicalize_schema(child) do
          {:ok, child} -> {:cont, {:ok, Map.put(acc, to_string(key), child)}}
          :error -> {:halt, :error}
        end

      _entry, _acc ->
        {:halt, :error}
    end)
  end

  defp canonicalize_fields(_other), do: :error

  defp canonicalize_item(nil), do: {:ok, nil}
  defp canonicalize_item(%Schema{} = item), do: canonicalize_schema(item)
  defp canonicalize_item(_other), do: :error
end
