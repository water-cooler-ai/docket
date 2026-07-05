defmodule Docket.Reducer do
  @moduledoc """
  Public reducer descriptors for graph state fields, plus the pure reduction
  engine the runtime applies at every commit.

  A reducer folds the field's prior committed value with the step's writes
  (deterministically sorted by writer node ID):

      new_value = reduce(reducer, current_committed_value, sorted_writes)

  ## Built-in reducers

  | Reducer        | Semantics                                                       |
  | -------------- | --------------------------------------------------------------- |
  | `:last_value`  | last write wins; the prior value is ignored                     |
  | `:first_value` | keep the prior value once set; otherwise the first write        |
  | `:append`      | list accumulation: `current ++ writes`                          |
  | `:merge`       | map merge: writes fold into the prior map                       |
  | `:sum`         | numeric accumulation: prior value plus every write              |
  | `:union`       | append + dedupe (first occurrence wins)                         |

  For `:append` and `:union`, a **list write concatenates** its elements and
  any other write appends as a single element; to append one element that is
  itself a list, wrap it in an outer list.

  ## Options

    * `:append` — `unique: true` drops duplicate elements (first occurrence
      wins); `max_length: n` keeps only the last `n` elements (sliding
      window), applied after `unique`.
    * `:union` — `by: key` dedupes elements by the value at `key` (a string
      or a list of strings forming a path); elements missing the key dedupe
      by their whole value.
    * `:merge` — `deep: true` merges nested maps recursively instead of
      replacing them.

  Option keys are stored as strings (constructors normalize atom keys),
  matching the wire format. Unknown option keys are stored but ignored.

  ## Initial values

  Accumulating reducers have a natural zero used as the field's effective
  default when it declares none: `[]` for `:append`/`:union`, `%{}` for
  `:merge`, `0` for `:sum`. A field's explicit default acts as the base the
  first commit folds into — including `:first_value`, where an explicit
  default counts as the value already being set.

  ## Write validation

  Node writes to a field validate against a reducer-dependent shape rather
  than the committed field schema (which stays the truth for the committed
  value): `:append`/`:union` writes validate against the list schema's
  `item` (a list write validates each element), `:merge` writes validate as
  a map fragment of the field schema with top-level required fields
  relaxed, and `:sum` writes must be a number of the schema's type.
  """

  alias Docket.Schema

  defstruct [:type, opts: %{}]

  @type type :: :append | :first_value | :last_value | :merge | :sum | :union
  @type t :: %__MODULE__{type: type(), opts: map()}

  @types [:append, :first_value, :last_value, :merge, :sum, :union]

  @doc "All built-in reducer types."
  @spec types() :: [type()]
  def types, do: @types

  @spec last_value(keyword() | map()) :: t()
  def last_value(opts \\ []), do: build(:last_value, opts)

  @spec first_value(keyword() | map()) :: t()
  def first_value(opts \\ []), do: build(:first_value, opts)

  @spec append(keyword() | map()) :: t()
  def append(opts \\ []), do: build(:append, opts)

  @spec merge(keyword() | map()) :: t()
  def merge(opts \\ []), do: build(:merge, opts)

  @spec sum(keyword() | map()) :: t()
  def sum(opts \\ []), do: build(:sum, opts)

  @spec union(keyword() | map()) :: t()
  def union(opts \\ []), do: build(:union, opts)

  defp build(type, opts), do: %__MODULE__{type: type, opts: attrs_to_map(opts)}

  defp attrs_to_map(attrs) do
    Map.new(attrs, fn {key, value} -> {to_string(key), value} end)
  end

  @doc """
  Folds the prior committed value with one step's writes.

  `current` is `{:ok, value}` when the field has a committed value or an
  effective default, `:unset` otherwise. `values` are the step's writes in
  sorted writer order and must be non-empty. A `nil` reducer reduces as
  `:last_value`.
  """
  @spec reduce(t() | nil, {:ok, term()} | :unset, [term()]) :: term()
  def reduce(nil, current, values), do: reduce(%__MODULE__{type: :last_value}, current, values)

  def reduce(%__MODULE__{type: :last_value}, _current, values), do: List.last(values)

  def reduce(%__MODULE__{type: :first_value}, {:ok, current}, _values), do: current
  def reduce(%__MODULE__{type: :first_value}, :unset, values), do: List.first(values)

  def reduce(%__MODULE__{type: :append, opts: opts}, current, values) do
    appended = base_list(current) ++ Enum.flat_map(values, &wrap_write/1)

    appended
    |> maybe_unique(opt(opts, "unique") == true)
    |> maybe_window(opt(opts, "max_length"))
  end

  def reduce(%__MODULE__{type: :union, opts: opts}, current, values) do
    key_fun = union_key_fun(opt(opts, "by"))

    (base_list(current) ++ Enum.flat_map(values, &wrap_write/1))
    |> Enum.uniq_by(key_fun)
  end

  def reduce(%__MODULE__{type: :merge, opts: opts}, current, values) do
    deep = opt(opts, "deep") == true

    Enum.reduce(values, base_map(current), fn write, acc ->
      merge_maps(acc, write, deep)
    end)
  end

  def reduce(%__MODULE__{type: :sum}, current, values) do
    base =
      case current do
        {:ok, value} when is_number(value) -> value
        _unset -> 0
      end

    base + Enum.sum(values)
  end

  @doc """
  The natural zero for accumulating reducers, used as a field's effective
  default when it declares none. `nil` for non-accumulating reducers.
  """
  @spec zero(t() | nil) :: term()
  def zero(%__MODULE__{type: type}) when type in [:append, :union], do: []
  def zero(%__MODULE__{type: :merge}), do: %{}
  def zero(%__MODULE__{type: :sum}), do: 0
  def zero(_reducer), do: nil

  @doc """
  The schema a single node write validates against, given the field schema
  and the write itself. `nil` means the write is not schema-validated.
  """
  @spec write_schema(t() | nil, Schema.t() | nil, term()) :: Schema.t() | nil
  def write_schema(_reducer, nil, _write), do: nil

  def write_schema(%__MODULE__{type: type}, %Schema{} = schema, write)
      when type in [:append, :union] do
    cond do
      is_list(write) -> %Schema{type: :list, item: schema.item}
      true -> schema.item
    end
  end

  def write_schema(%__MODULE__{type: :merge}, %Schema{type: :object} = schema, _write) do
    fields =
      Map.new(schema.fields, fn {name, field_schema} ->
        {name, %{field_schema | required: false}}
      end)

    %{schema | fields: fields, required: true, default: Schema.no_default()}
  end

  def write_schema(%__MODULE__{type: :merge}, %Schema{} = schema, _write) do
    %Schema{type: schema.type, required: true}
  end

  def write_schema(%__MODULE__{type: :sum}, %Schema{} = schema, _write) do
    %Schema{type: schema.type, required: true}
  end

  def write_schema(_last_or_first_or_nil, %Schema{} = schema, _write), do: schema

  # ---------------------------------------------------------------------------
  # Reduction helpers
  # ---------------------------------------------------------------------------

  # A list write concatenates; any other write (including nil) appends as
  # one element.
  defp wrap_write(write) when is_list(write), do: write
  defp wrap_write(write), do: [write]

  defp base_list({:ok, value}) when is_list(value), do: value
  defp base_list(_unset), do: []

  defp base_map({:ok, value}) when is_map(value) and not is_struct(value), do: value
  defp base_map(_unset), do: %{}

  defp maybe_unique(list, true), do: Enum.uniq(list)
  defp maybe_unique(list, false), do: list

  defp maybe_window(list, max) when is_integer(max) and max >= 0 do
    Enum.take(list, -max)
  end

  defp maybe_window(list, _max), do: list

  defp union_key_fun(nil), do: & &1

  defp union_key_fun(by) when is_binary(by), do: union_key_fun([by])

  defp union_key_fun(path) when is_list(path) do
    fn element ->
      case fetch_path(element, path) do
        {:ok, key} -> {:key, key}
        :missing -> {:value, element}
      end
    end
  end

  defp union_key_fun(_other), do: & &1

  defp fetch_path(value, []), do: {:ok, value}

  defp fetch_path(value, [segment | rest]) when is_map(value) and not is_struct(value) do
    case Map.fetch(value, segment) do
      {:ok, next} -> fetch_path(next, rest)
      :error -> :missing
    end
  end

  defp fetch_path(_value, _segments), do: :missing

  defp merge_maps(base, write, false) when is_map(write), do: Map.merge(base, write)

  defp merge_maps(base, write, true) when is_map(write) do
    Map.merge(base, write, fn _key, left, right ->
      if is_map(left) and not is_struct(left) and is_map(right) and not is_struct(right) do
        merge_maps(left, right, true)
      else
        right
      end
    end)
  end

  defp merge_maps(base, _write, _deep), do: base

  # Reducer opts are stored string-keyed (wire format); the atom fallback
  # keeps hand-built structs from silently changing semantics.
  defp opt(opts, key) do
    case Map.fetch(opts, key) do
      {:ok, value} -> value
      :error -> Map.get(opts, String.to_atom(key))
    end
  end
end
