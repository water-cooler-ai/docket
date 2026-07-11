defmodule Docket.DurableCodec do
  @moduledoc false

  alias Docket.Graph.{Edge, Field, Node, Output}
  alias Docket.{Graph, Guard, Reducer, Schema, Wire}

  @version 1
  @kinds [:graph, :run]
  @guard_ops [:all, :any, :changed, :equals, :exists, :not, :path, :version_at_least]
  @known_structs [
    DateTime,
    MapSet,
    Docket.Graph,
    Docket.Graph.Edge,
    Docket.Graph.Field,
    Docket.Graph.Node,
    Docket.Graph.Output,
    Docket.Guard,
    Docket.Interrupt,
    Docket.Reducer,
    Docket.Run.ChannelState,
    Docket.Run.Failure,
    Docket.Run.InterruptState,
    Docket.Run.PendingWrite,
    Docket.Run.TaskState,
    Docket.Run.TimerState,
    Docket.Schema
  ]
  @atom_modules [Docket.Run, Docket.Runtime.Loop]

  @doc false
  @spec encode!(:graph, Docket.Graph.t()) :: binary()
  @spec encode!(:run, map()) :: binary()
  def encode!(:graph, %Graph{} = graph) do
    {_graph, bytes} = encode_graph!(graph)
    bytes
  end

  def encode!(:run, map) when is_map(map) and not is_struct(map), do: encode_term!(:run, map)
  def encode!(kind, _term), do: invalid!("invalid #{kind} durable root")

  @doc false
  @spec encode_graph!(Graph.t()) :: {Graph.t(), binary()}
  def encode_graph!(%Graph{} = graph) do
    graph = normalize_graph_root!(graph)
    {graph, encode_term!(:graph, graph)}
  end

  def encode_graph!(_graph), do: invalid!("invalid graph durable root")

  @doc false
  @spec valid_datetime?(term()) :: boolean()
  def valid_datetime?(%DateTime{} = datetime) do
    exact_struct?(datetime, DateTime) and valid_datetime_representation?(datetime)
  end

  def valid_datetime?(_datetime), do: false

  @doc false
  @spec decode(binary(), :graph | :run) ::
          {:ok, Docket.Graph.t() | map()} | {:error, Docket.Error.t()}
  def decode(binary, kind) do
    {:ok, decode!(binary, kind)}
  rescue
    error in Docket.Error -> {:error, error}
  end

  @doc false
  @spec decode!(binary(), :graph | :run) :: Docket.Graph.t() | map()
  def decode!(<<131, 80, _::binary>>, _kind), do: invalid!("compressed ETF is not accepted")

  def decode!(binary, kind) when is_binary(binary) and kind in @kinds do
    Enum.each(@known_structs ++ @atom_modules, &Code.ensure_loaded?/1)

    {envelope, used} =
      try do
        :erlang.binary_to_term(binary, [:safe, :used])
      rescue
        ArgumentError -> invalid!("invalid or unsafe ETF payload")
      end

    if used != byte_size(binary), do: invalid!("ETF payload has trailing bytes")

    case envelope do
      {:docket, @version, ^kind, term} ->
        validate_decoded_root!(kind, term)
        durable!(term)
        term

      _other ->
        invalid!("ETF envelope does not match codec version and kind")
    end
  end

  def decode!(_binary, _kind), do: invalid!("invalid ETF payload or kind")

  defp validate_decoded_root!(:graph, %Graph{diagnostics: []} = graph) do
    if normalize_graph_root!(graph) === graph,
      do: :ok,
      else: invalid!("graph durable root is not normalized")
  end

  defp validate_decoded_root!(:run, map) when is_map(map) and not is_struct(map), do: :ok
  defp validate_decoded_root!(kind, _term), do: invalid!("invalid #{kind} durable root")

  defp encode_term!(kind, term) do
    durable!(term)
    :erlang.term_to_binary({:docket, @version, kind, term}, [:deterministic, minor_version: 2])
  end

  defp normalize_graph_root!(graph) do
    normalize_graph!(graph)
  rescue
    error in Docket.Error -> reraise error, __STACKTRACE__
    _error -> invalid!("invalid graph durable root")
  catch
    _kind, _reason -> invalid!("invalid graph durable root")
  end

  defp normalize_graph!(%Graph{} = graph) do
    assert_plain_map!(graph.policies, "graph policies")
    assert_plain_map!(graph.metadata, "graph metadata")

    %{
      graph
      | inputs: normalize_collection(graph.inputs, Field, &normalize_field!/1, "graph inputs"),
        fields: normalize_collection(graph.fields, Field, &normalize_field!/1, "graph fields"),
        outputs:
          normalize_collection(graph.outputs, Output, &normalize_output!/1, "graph outputs"),
        nodes: normalize_collection(graph.nodes, Node, &normalize_node!/1, "graph nodes"),
        edges: normalize_collection(graph.edges, Edge, &normalize_edge!/1, "graph edges"),
        policies: normalize_open!(graph.policies, "graph policies"),
        metadata: normalize_open!(graph.metadata, "graph metadata"),
        diagnostics: []
    }
  end

  defp normalize_collection(collection, module, normalize, _location)
       when is_map(collection) and not is_struct(collection) do
    Map.new(collection, fn {id, record} ->
      {id, if(is_struct(record, module), do: normalize.(record), else: record)}
    end)
  end

  defp normalize_collection(_other, _module, _normalize, location),
    do: invalid!("#{location} must be a plain map")

  defp normalize_field!(%Field{} = field) do
    assert_plain_map!(field.metadata, "field metadata")

    %{
      field
      | schema: normalize_schema!(field.schema, "field #{inspect(field.id)} schema"),
        reducer: normalize_reducer!(field.reducer, "field #{inspect(field.id)} reducer"),
        default: Wire.dump_value!(field.default, "field #{inspect(field.id)} default"),
        metadata: normalize_open!(field.metadata, "field #{inspect(field.id)} metadata")
    }
  end

  defp normalize_field!(other), do: other

  defp normalize_output!(%Output{} = output) do
    assert_plain_map!(output.metadata, "output metadata")

    %{
      output
      | schema: normalize_schema!(output.schema, "output #{inspect(output.id)} schema"),
        metadata: normalize_open!(output.metadata, "output #{inspect(output.id)} metadata")
    }
  end

  defp normalize_output!(other), do: other

  defp normalize_node!(%Node{} = node) do
    assert_plain_map!(node.branches, "node branches")
    assert_plain_map!(node.config, "node config")
    assert_plain_map!(node.policies, "node policies")
    assert_plain_map!(node.metadata, "node metadata")

    %{
      node
      | implementation: normalize_implementation!(node.implementation, node.id),
        branches: Wire.dump_value!(node.branches, "node #{inspect(node.id)} branches"),
        config: normalize_open!(node.config, "node #{inspect(node.id)} config"),
        policies: normalize_open!(node.policies, "node #{inspect(node.id)} policies"),
        metadata: normalize_open!(node.metadata, "node #{inspect(node.id)} metadata")
    }
  end

  defp normalize_node!(other), do: other

  defp normalize_implementation!(nil, _node_id), do: nil

  defp normalize_implementation!(
         %{type: :module, module: module, function: :call} = implementation,
         _node_id
       )
       when is_atom(module) and map_size(implementation) == 3,
       do: implementation

  defp normalize_implementation!(implementation, node_id),
    do: Wire.dump_value!(implementation, "node #{inspect(node_id)} implementation")

  defp normalize_edge!(%Edge{} = edge) do
    assert_proper_list_if_list!(edge.from, "edge from")
    assert_plain_map!(edge.metadata, "edge metadata")

    %{
      edge
      | guard: normalize_guard!(edge.guard, edge.id),
        metadata: normalize_open!(edge.metadata, "edge #{inspect(edge.id)} metadata")
    }
  end

  defp normalize_edge!(other), do: other

  defp normalize_guard!(nil, _edge_id), do: nil

  defp normalize_guard!(%Guard{args: args} = guard, edge_id) when is_list(args) do
    args =
      Enum.map(args, fn
        %Guard{} = nested -> normalize_guard!(nested, edge_id)
        value -> Wire.dump_value!(value, "edge #{inspect(edge_id)} guard")
      end)

    %{guard | op: normalize_structural_atom(guard.op, @guard_ops), args: args}
  end

  defp normalize_guard!(%Guard{} = guard, edge_id) do
    %{
      guard
      | op: normalize_structural_atom(guard.op, @guard_ops),
        args: Wire.dump_value!(guard.args, "edge #{inspect(edge_id)} guard arguments")
    }
  end

  defp normalize_guard!(other, _edge_id), do: other

  defp normalize_schema!(nil, _location), do: nil

  defp normalize_schema!(%Schema{} = schema, location),
    do: Schema.normalize_durable!(schema, location)

  defp normalize_schema!(other, _location), do: other

  defp normalize_reducer!(nil, _location), do: nil

  defp normalize_reducer!(%Reducer{} = reducer, location) do
    assert_plain_map!(reducer.opts, "#{location} options")

    %{
      reducer
      | type: normalize_structural_atom(reducer.type, Reducer.types()),
        opts: Wire.dump_value!(reducer.opts, "#{location} options")
    }
  end

  defp normalize_reducer!(other, _location), do: other

  defp normalize_open!(value, _location)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: value

  defp normalize_open!(value, _location) when is_atom(value), do: Atom.to_string(value)
  defp normalize_open!([], _location), do: []

  defp normalize_open!([head | tail], location),
    do: [normalize_open!(head, location) | normalize_open!(tail, location)]

  defp normalize_open!(value, location) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&normalize_open!(&1, location))
    |> List.to_tuple()
  end

  defp normalize_open!(%MapSet{} = set, location) do
    set |> Enum.map(&normalize_open!(&1, location)) |> MapSet.new()
  end

  defp normalize_open!(%DateTime{calendar: Calendar.ISO} = datetime, _location), do: datetime

  defp normalize_open!(%DateTime{} = datetime, location) do
    invalid!("#{location} contains unsupported DateTime calendar #{inspect(datetime.calendar)}")
  end

  defp normalize_open!(%_struct{} = struct, location) do
    invalid!("#{location} contains unsupported struct #{inspect(struct.__struct__)}")
  end

  defp normalize_open!(map, location) when is_map(map) do
    Enum.reduce(map, %{}, fn {key, value}, normalized ->
      key = normalize_open!(key, location)

      if Map.has_key?(normalized, key) do
        invalid!("#{location} has colliding key #{inspect(key)} after atom normalization")
      end

      Map.put(normalized, key, normalize_open!(value, location))
    end)
  end

  defp normalize_open!(value, _location), do: value

  defp assert!(true, _message), do: :ok
  defp assert!(false, message), do: invalid!(message)

  defp assert_plain_map!(value, location),
    do: assert!(is_map(value) and not is_struct(value), "#{location} must be a plain map")

  defp assert_proper_list_if_list!(value, location) when is_list(value),
    do: assert!(proper_list?(value), "#{location} must be a proper list")

  defp assert_proper_list_if_list!(_value, _location), do: :ok

  defp proper_list?([]), do: true
  defp proper_list?([_value | rest]), do: proper_list?(rest)
  defp proper_list?(_tail), do: false

  defp normalize_structural_atom(value, allowed) when is_atom(value) do
    if value in allowed, do: value, else: Atom.to_string(value)
  end

  defp normalize_structural_atom(value, _allowed), do: value

  defp durable!(term)
       when is_atom(term) or is_number(term) or is_binary(term),
       do: :ok

  defp durable!(term) when is_list(term), do: durable_list!(term)
  defp durable!(term) when is_tuple(term), do: term |> Tuple.to_list() |> durable!()

  defp durable!(%MapSet{map: map} = set) do
    assert_exact_struct!(set, MapSet)

    unless is_map(map) and
             Enum.all?(map, fn {value, marker} -> marker == [] and open_atom_safe?(value) end) do
      invalid!("malformed or atom-unsafe MapSet")
    end

    Enum.each(Map.keys(map), &durable!/1)
  end

  defp durable!(%DateTime{} = datetime) do
    unless valid_datetime?(datetime) do
      invalid!("malformed or atom-unsafe DateTime")
    end
  end

  defp durable!(%{__struct__: module} = struct) do
    if module not in @known_structs,
      do: invalid!("foreign struct #{inspect(module)} is not durable")

    assert_exact_struct!(struct, module)

    struct |> Map.from_struct() |> durable!()
  end

  defp durable!(map) when is_map(map) do
    Enum.each(map, fn {key, value} ->
      durable!(key)
      durable!(value)
    end)
  end

  defp durable!(term), do: invalid!("non-durable term #{inspect(term)}")

  defp durable_list!([]), do: :ok

  defp durable_list!([head | tail]) do
    durable!(head)
    durable_list!(tail)
  end

  defp durable_list!(tail), do: invalid!("improper list tail #{inspect(tail)}")

  defp assert_exact_struct!(struct, module) do
    unless exact_struct?(struct, module) do
      invalid!("malformed #{inspect(module)} struct")
    end
  end

  defp exact_struct?(struct, module),
    do: MapSet.new(Map.keys(struct)) == MapSet.new(Map.keys(module.__struct__()))

  defp valid_datetime_representation?(
         %DateTime{
           calendar: Calendar.ISO,
           time_zone: time_zone,
           zone_abbr: zone_abbr,
           utc_offset: utc_offset,
           std_offset: std_offset,
           microsecond: {microsecond, precision},
           year: year,
           month: month,
           day: day,
           hour: hour,
           minute: minute,
           second: second
         } = datetime
       ) do
    is_binary(time_zone) and is_binary(zone_abbr) and is_integer(utc_offset) and
      is_integer(std_offset) and is_integer(microsecond) and microsecond in 0..999_999 and
      is_integer(precision) and precision in 0..6 and valid_date?(year, month, day) and
      valid_time?(hour, minute, second, datetime.microsecond)
  end

  defp valid_datetime_representation?(_datetime), do: false

  defp valid_date?(year, month, day)
       when is_integer(year) and is_integer(month) and is_integer(day),
       do: match?({:ok, _date}, Date.new(year, month, day))

  defp valid_date?(_year, _month, _day), do: false

  defp valid_time?(hour, minute, second, microsecond)
       when is_integer(hour) and is_integer(minute) and is_integer(second),
       do: match?({:ok, _time}, Time.new(hour, minute, second, microsecond))

  defp valid_time?(_hour, _minute, _second, _microsecond), do: false

  defp open_atom_safe?(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: true

  defp open_atom_safe?(value) when is_atom(value), do: false
  defp open_atom_safe?([]), do: true
  defp open_atom_safe?([head | tail]), do: open_atom_safe?(head) and open_atom_safe?(tail)

  defp open_atom_safe?(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.all?(&open_atom_safe?/1)

  defp open_atom_safe?(%DateTime{} = datetime), do: valid_datetime?(datetime)

  defp open_atom_safe?(%MapSet{map: map} = set) when is_map(map) do
    exact_struct?(set, MapSet) and
      Enum.all?(map, fn {value, marker} -> marker == [] and open_atom_safe?(value) end)
  end

  defp open_atom_safe?(%MapSet{}), do: false
  defp open_atom_safe?(%_struct{}), do: false

  defp open_atom_safe?(map) when is_map(map),
    do: Enum.all?(map, fn {key, value} -> open_atom_safe?(key) and open_atom_safe?(value) end)

  defp open_atom_safe?(_value), do: true

  defp invalid!(message) do
    raise Docket.Error, type: :invalid_durable_state, message: message
  end
end
