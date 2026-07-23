defmodule Docket.DurableCodec do
  @moduledoc false

  alias Docket.Graph
  alias Docket.Graph.Compiler.Canonical

  @version 1
  @kinds [:event, :graph, :run]
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
  @spec encode!(:event, map()) :: binary()
  def encode!(:graph, %Graph{diagnostics: []} = graph), do: encode_term!(:graph, graph)

  def encode!(:run, map) when is_map(map) and not is_struct(map), do: encode_term!(:run, map)
  def encode!(:event, map) when is_map(map) and not is_struct(map), do: encode_term!(:event, map)
  def encode!(kind, _term), do: invalid!("invalid #{kind} durable root")

  @doc false
  @spec valid_datetime?(term()) :: boolean()
  def valid_datetime?(%DateTime{} = datetime) do
    exact_struct?(datetime, DateTime) and valid_datetime_representation?(datetime)
  end

  def valid_datetime?(_datetime), do: false

  @doc false
  @spec decode(binary(), :event | :graph | :run) ::
          {:ok, Docket.Graph.t() | map()} | {:error, Docket.Error.t()}
  def decode(binary, kind) do
    {:ok, decode!(binary, kind)}
  rescue
    error in Docket.Error -> {:error, error}
  end

  @doc false
  @spec decode!(binary(), :event | :graph | :run) :: Docket.Graph.t() | map()
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

  defp validate_decoded_root!(:graph, %Graph{diagnostics: []} = graph),
    do: Canonical.validate!(graph)

  defp validate_decoded_root!(:run, map) when is_map(map) and not is_struct(map), do: :ok
  defp validate_decoded_root!(:event, map) when is_map(map) and not is_struct(map), do: :ok
  defp validate_decoded_root!(kind, _term), do: invalid!("invalid #{kind} durable root")

  defp encode_term!(kind, term) do
    durable!(term)
    :erlang.term_to_binary({:docket, @version, kind, term}, [:deterministic, minor_version: 2])
  end

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
