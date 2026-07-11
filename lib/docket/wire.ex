defmodule Docket.Wire do
  @moduledoc false

  # Durable-value coercion for open runtime and graph content: atoms coerce to
  # strings (map keys and values), terms outside Docket's portable open-value
  # contract are rejected, and "$"-prefixed map keys are reserved.
  #
  # `dump_value/1` is non-raising so the update barrier can turn durability
  # failures into typed node errors instead of exceptions.

  @spec dump_value(term()) :: {:ok, term()} | {:error, String.t()}
  def dump_value(value) do
    {:ok, durable!(value)}
  catch
    {:non_durable, reason} -> {:error, reason}
  end

  @spec dump_value!(term(), String.t()) :: term()
  def dump_value!(value, location) do
    case dump_value(value) do
      {:ok, durable} ->
        durable

      {:error, reason} ->
        raise Docket.Error,
          type: :non_durable_value,
          message: "#{location} contains a non-durable value: #{reason}",
          details: %{location: location}
    end
  end

  @doc false
  @spec dump_key!(term(), String.t()) :: String.t()
  def dump_key!(key, location) do
    case dump_value(%{key => nil}) do
      {:ok, normalized} ->
        normalized |> Map.keys() |> List.first()

      {:error, reason} ->
        raise Docket.Error,
          type: :non_durable_value,
          message: "#{location} contains a non-durable key: #{reason}",
          details: %{location: location}
    end
  end

  @spec load_value!(term(), String.t()) :: term()
  def load_value!(value, _location)
      when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
      do: value

  def load_value!(list, location) when is_list(list) do
    Enum.map(list, &load_value!(&1, location))
  end

  def load_value!(map, location) when is_map(map) and not is_struct(map) do
    Map.new(map, fn {key, value} ->
      {load_key!(key, location), load_value!(value, location)}
    end)
  end

  def load_value!(other, location) do
    raise Docket.Error,
      type: :invalid_document,
      message: "#{location} contains a non-durable value #{inspect(other)}",
      details: %{location: location, value: inspect(other)}
  end

  defp load_key!(key, location) when is_binary(key) do
    if String.starts_with?(key, "$") do
      raise Docket.Error,
        type: :invalid_document,
        message: "#{location} map keys starting with \"$\" are reserved, got #{inspect(key)}",
        details: %{location: location, key: key}
    else
      key
    end
  end

  defp load_key!(key, location) do
    raise Docket.Error,
      type: :invalid_document,
      message: "#{location} map keys must be strings, got #{inspect(key)}",
      details: %{location: location, key: inspect(key)}
  end

  defp durable!(value)
       when is_binary(value) or is_integer(value) or is_float(value) or is_boolean(value) or
              is_nil(value),
       do: value

  defp durable!(atom) when is_atom(atom), do: Atom.to_string(atom)

  defp durable!(list) when is_list(list), do: durable_list!(list)

  defp durable!(%_struct{} = struct) do
    throw({:non_durable, "structs are not durable values, got #{inspect(struct)}"})
  end

  defp durable!(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {durable_key!(key), durable!(value)} end)
  end

  defp durable!(value) when is_tuple(value) do
    throw(
      {:non_durable,
       "tuples and keyword lists are not durable, got #{inspect(value)}; " <>
         "keyword lists should become string-keyed maps"}
    )
  end

  defp durable!(value) do
    throw({:non_durable, "#{inspect(value)} has no durable representation"})
  end

  defp durable_key!(key) when is_binary(key) do
    if String.starts_with?(key, "$") do
      throw({:non_durable, "map keys starting with \"$\" are reserved, got #{inspect(key)}"})
    else
      key
    end
  end

  defp durable_key!(key) when is_atom(key), do: durable_key!(Atom.to_string(key))

  defp durable_key!(key) do
    throw({:non_durable, "map keys must be strings or atoms, got #{inspect(key)}"})
  end

  defp durable_list!([]), do: []
  defp durable_list!([head | tail]), do: [durable!(head) | durable_list!(tail)]

  defp durable_list!(tail) do
    throw({:non_durable, "improper lists are not durable, got tail #{inspect(tail)}"})
  end
end
