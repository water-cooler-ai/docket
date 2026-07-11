defmodule Docket.Graph.Compiler.Canonical do
  @moduledoc false

  alias Docket.Graph
  alias Docket.Graph.{Edge, Field, Node, Output}
  alias Docket.{Guard, Reducer, Schema, Wire}

  @guard_ops [:all, :any, :changed, :equals, :exists, :not, :path, :version_at_least]

  @spec normalize!(Graph.t()) :: Graph.t()
  def normalize!(%Graph{} = graph) do
    normalize_graph(graph)
  rescue
    error in Docket.Error -> reraise error, __STACKTRACE__
    _error -> invalid!("graph durable root")
  catch
    _kind, _reason -> invalid!("graph durable root")
  end

  @spec validate!(Graph.t()) :: :ok
  def validate!(%Graph{} = graph) do
    if normalize!(graph) === graph,
      do: :ok,
      else: invalid_state!("graph durable root is not canonical")
  end

  defp normalize_graph(graph) do
    %{
      graph
      | inputs: collection!(graph.inputs, Field, &field/1, "graph inputs"),
        fields: collection!(graph.fields, Field, &field/1, "graph fields"),
        outputs: collection!(graph.outputs, Output, &output/1, "graph outputs"),
        nodes: collection!(graph.nodes, Node, &normalize_node/1, "graph nodes"),
        edges: collection!(graph.edges, Edge, &edge/1, "graph edges"),
        policies: open_map!(graph.policies, "graph policies"),
        metadata: open_map!(graph.metadata, "graph metadata"),
        diagnostics: []
    }
  end

  defp collection!(collection, module, normalize, _location)
       when is_map(collection) and not is_struct(collection) do
    Map.new(collection, fn {id, record} ->
      unless is_binary(id), do: invalid_state!("graph collection keys must be strings")

      unless is_struct(record, module) do
        invalid_state!("graph collection entries must be #{inspect(module)} structs")
      end

      {id, normalize.(record)}
    end)
  end

  defp collection!(_collection, _module, _normalize, location), do: invalid!(location)

  defp field(%Field{} = field) do
    %{
      field
      | schema: schema(field.schema, "field #{inspect(field.id)} schema"),
        reducer: reducer(field.reducer, "field #{inspect(field.id)} reducer"),
        default: Wire.dump_value!(field.default, "field #{inspect(field.id)} default"),
        metadata: open_map!(field.metadata, "field metadata")
    }
  end

  defp output(%Output{} = output) do
    %{
      output
      | schema: schema(output.schema, "output #{inspect(output.id)} schema"),
        metadata: open_map!(output.metadata, "output metadata")
    }
  end

  defp normalize_node(%Node{} = node) do
    %{
      node
      | implementation: implementation(node.implementation, node.id),
        branches: Wire.dump_value!(node.branches, "node #{inspect(node.id)} branches"),
        config: open_map!(node.config, "node config"),
        policies: open_map!(node.policies, "node policies"),
        metadata: open_map!(node.metadata, "node metadata")
    }
  end

  defp edge(%Edge{} = edge) do
    %{
      edge
      | guard: guard(edge.guard, edge.id),
        metadata: open_map!(edge.metadata, "edge metadata")
    }
  end

  defp implementation(nil, _id), do: nil

  defp implementation(%{type: :module, module: module, function: :call} = value, _id)
       when is_atom(module) and map_size(value) == 3,
       do: value

  defp implementation(value, id),
    do: Wire.dump_value!(value, "node #{inspect(id)} implementation")

  defp guard(nil, _id), do: nil

  defp guard(%Guard{args: args} = guard, id) when is_list(args) do
    args =
      Enum.map(args, fn
        %Guard{} = nested -> guard(nested, id)
        value -> Wire.dump_value!(value, "edge #{inspect(id)} guard")
      end)

    %{guard | op: structural_atom(guard.op, @guard_ops), args: args}
  end

  defp guard(%Guard{} = guard, id) do
    %{
      guard
      | op: structural_atom(guard.op, @guard_ops),
        args: Wire.dump_value!(guard.args, "edge #{inspect(id)} guard arguments")
    }
  end

  defp guard(other, _id), do: other

  defp schema(nil, _location), do: nil
  defp schema(%Schema{} = schema, location), do: Schema.normalize_durable!(schema, location)
  defp schema(other, _location), do: other

  defp reducer(nil, _location), do: nil

  defp reducer(%Reducer{} = reducer, location) do
    %{
      reducer
      | type: structural_atom(reducer.type, Reducer.types()),
        opts: open_map!(reducer.opts, "#{location} options")
    }
  end

  defp reducer(other, _location), do: other

  defp open_map!(value, location) when is_map(value) and not is_struct(value),
    do: normalize_open(value, location)

  defp open_map!(_value, location), do: invalid!(location)

  defp normalize_open(value, _location)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: value

  defp normalize_open(value, _location) when is_atom(value), do: Atom.to_string(value)
  defp normalize_open([], _location), do: []

  defp normalize_open([head | tail], location),
    do: [normalize_open(head, location) | normalize_open(tail, location)]

  defp normalize_open(value, location) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.map(&normalize_open(&1, location)) |> List.to_tuple()
  end

  defp normalize_open(%MapSet{} = set, location),
    do: set |> Enum.map(&normalize_open(&1, location)) |> MapSet.new()

  defp normalize_open(%DateTime{} = datetime, _location), do: datetime

  defp normalize_open(%_struct{} = struct, location),
    do:
      raise(Docket.Error,
        type: :invalid_durable_state,
        message: "#{location} contains unsupported struct #{inspect(struct.__struct__)}"
      )

  defp normalize_open(map, location) when is_map(map) do
    Enum.reduce(map, %{}, fn {key, value}, normalized ->
      key = normalize_open(key, location)

      if Map.has_key?(normalized, key) do
        raise Docket.Error,
          type: :invalid_durable_state,
          message: "#{location} has colliding key #{inspect(key)} after atom normalization"
      end

      Map.put(normalized, key, normalize_open(value, location))
    end)
  end

  defp normalize_open(value, _location), do: value

  defp structural_atom(value, allowed) when is_atom(value) do
    if value in allowed, do: value, else: Atom.to_string(value)
  end

  defp structural_atom(value, _allowed), do: value

  defp invalid!(location) do
    invalid_state!("#{location} must be a plain map")
  end

  defp invalid_state!(message) do
    raise Docket.Error, type: :invalid_durable_state, message: message
  end
end
