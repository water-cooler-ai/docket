defmodule Docket.Schema do
  @moduledoc """
  Serializable schema IR used by graph fields and node contracts.

  `validate/2` implements the validation engine used by the compiler for
  field defaults and node config, and later by the runtime for input payloads
  and state writes.

  ## Types

  `:string`, `:float` (accepts any number), `:integer`, `:boolean`, `:map`
  (any string-keyed map), `:object` (declared fields), `:enum` (closed value
  set), and `:list` (per-item schema in `item`).

  ## Constraints

  Constraints are stored as a string-keyed map (the constructors normalize
  atom option keys) and enforced by `validate/2`:

    * numbers (`:float`, `:integer`) — `min`, `max` (inclusive)
    * strings — `min_length`, `max_length`, `pattern` (Elixir/PCRE regex
      source string)
    * lists — `min_items`, `max_items`

  Unknown constraint keys are stored but ignored. Objects accept an
  `open: true` option (stored in `constraints`) that permits unknown keys;
  by default unknown object keys are rejected.

  ## Shorthand

  Everywhere the constructors and the graph editing API expect a schema,
  terse literals normalize (via `normalize/1`) into real schemas:

      :string                          # bare primitive type
      {:integer, min: 0}               # type + options
      {:list, :string}                 # list + item shorthand
      {:list, :string, max_items: 10}
      {:enum, ["low", "high"]}
      {:object, %{"name" => :string}, open: true}

  Object field values and list items normalize recursively, so
  `Docket.Schema.object(%{age: {:integer, min: 0}, tags: {:list, :string}})`
  is fully shorthand. Values that match no shorthand pass through unchanged
  and are reported by the compiler.
  """

  @no_default :__docket_no_default__
  @types [:boolean, :enum, :float, :integer, :list, :map, :object, :string]

  defstruct [
    :type,
    fields: %{},
    item: nil,
    values: [],
    required: false,
    default: @no_default,
    constraints: %{},
    metadata: %{}
  ]

  @type primitive_type :: :boolean | :float | :integer | :map | :string
  @type type :: primitive_type() | :enum | :list | :object

  @type t :: %__MODULE__{
          type: type(),
          fields: %{optional(String.t() | atom()) => t()},
          item: t() | nil,
          values: [term()],
          required: boolean(),
          default: term(),
          constraints: map(),
          metadata: map()
        }

  @doc false
  @spec no_default() :: :__docket_no_default__
  def no_default, do: @no_default

  @spec string(keyword()) :: t()
  def string(opts \\ []), do: build(:string, opts)

  @spec float(keyword()) :: t()
  def float(opts \\ []), do: build(:float, opts)

  @spec integer(keyword()) :: t()
  def integer(opts \\ []), do: build(:integer, opts)

  @spec boolean(keyword()) :: t()
  def boolean(opts \\ []), do: build(:boolean, opts)

  @spec map(keyword()) :: t()
  def map(opts \\ []), do: build(:map, opts)

  @spec list(t() | term(), keyword()) :: t()
  def list(item, opts \\ []) do
    :list
    |> build(opts)
    |> Map.put(:item, normalize(item))
  end

  @spec object(map(), keyword()) :: t()
  def object(fields, opts \\ []) when is_map(fields) do
    :object
    |> build(opts)
    |> Map.put(:fields, Map.new(fields, fn {name, value} -> {name, normalize(value)} end))
  end

  @spec enum([term()], keyword()) :: t()
  def enum(values, opts \\ []) when is_list(values) do
    :enum
    |> build(opts)
    |> Map.put(:values, values)
  end

  @doc """
  Normalizes schema shorthand (see the module documentation) into a
  `Docket.Schema` struct.

  Real schemas and values matching no shorthand pass through unchanged; the
  compiler reports the latter as invalid schemas.
  """
  @spec normalize(term()) :: term()
  def normalize(%__MODULE__{} = schema), do: schema

  def normalize(type) when type in [:boolean, :float, :integer, :map, :string] do
    build(type, [])
  end

  def normalize({:list, item}), do: list(item)
  def normalize({:list, item, opts}) when is_list(opts), do: list(item, opts)

  def normalize({:enum, values}) when is_list(values), do: enum(values)

  def normalize({:enum, values, opts}) when is_list(values) and is_list(opts) do
    enum(values, opts)
  end

  def normalize({:object, fields}) when is_map(fields), do: object(fields)

  def normalize({:object, fields, opts}) when is_map(fields) and is_list(opts) do
    object(fields, opts)
  end

  def normalize({type, opts})
      when type in [:boolean, :float, :integer, :map, :string] and is_list(opts) do
    build(type, opts)
  end

  def normalize(other), do: other

  defp build(type, opts) do
    {known, constraints} =
      Keyword.split(opts, [:required, :default, :metadata])

    %__MODULE__{
      type: type,
      required: Keyword.get(known, :required, false),
      default: Keyword.get(known, :default, @no_default),
      metadata: Keyword.get(known, :metadata, %{}),
      constraints: Map.new(constraints, fn {key, value} -> {to_string(key), value} end)
    }
  end

  @doc """
  Validates a value against a schema.

  Checks primitive types, enum membership, required object fields, unknown
  object keys (unless the object is `open`), list items against `item`, and
  the stored constraints listed in the module documentation. `nil` is valid
  unless the schema is `required`.

  Returns `:ok` or `{:error, reasons}` with human-readable reason strings.
  """
  @spec validate(t(), term()) :: :ok | {:error, [String.t()]}
  def validate(%__MODULE__{} = schema, value) do
    case validate_value(schema, value, "value") do
      [] -> :ok
      reasons -> {:error, reasons}
    end
  end

  @doc false
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{} = schema) do
    schema.type in @types and is_boolean(schema.required) and plain_map?(schema.fields) and
      is_list(schema.values) and plain_map?(schema.constraints) and plain_map?(schema.metadata) and
      Enum.all?(schema.fields, fn {key, child} ->
        is_binary(key) and valid?(child)
      end) and valid_item?(schema) and valid_default?(schema) and portable?(schema.values) and
      portable?(schema.constraints) and portable?(schema.metadata)
  rescue
    _error -> false
  end

  def valid?(_schema), do: false

  @doc false
  @spec normalize_durable!(t(), String.t()) :: t()
  def normalize_durable!(%__MODULE__{} = schema, location \\ "schema") do
    fields =
      Enum.reduce(schema.fields, %{}, fn {key, child}, normalized ->
        key = Docket.Wire.dump_key!(key, "#{location} fields")

        if Map.has_key?(normalized, key) do
          raise Docket.Error,
            type: :non_durable_value,
            message: "#{location} has colliding field key #{inspect(key)} after normalization"
        end

        Map.put(normalized, key, normalize_durable!(child, "#{location}.#{key}"))
      end)

    %{
      schema
      | type: normalize_type(schema.type),
        fields: fields,
        item: normalize_item!(schema.item, location),
        values: Docket.Wire.dump_value!(schema.values, "#{location} values"),
        required: normalize_atom(schema.required),
        default: normalize_default!(schema.default, location),
        constraints: Docket.Wire.dump_value!(schema.constraints, "#{location} constraints"),
        metadata: Docket.Wire.dump_value!(schema.metadata, "#{location} metadata")
    }
  rescue
    error in Docket.Error ->
      raise error

    _error ->
      raise Docket.Error,
        type: :non_durable_value,
        message: "#{location} is not a durable Docket.Schema"
  end

  defp valid_item?(%__MODULE__{type: :list, item: item}), do: valid?(item)
  defp valid_item?(%__MODULE__{item: nil}), do: true
  defp valid_item?(_schema), do: false

  defp valid_default?(%__MODULE__{default: @no_default}), do: true

  defp valid_default?(%__MODULE__{} = schema),
    do: portable?(schema.default) and validate(schema, schema.default) == :ok

  defp normalize_item!(nil, _location), do: nil

  defp normalize_item!(%__MODULE__{} = item, location),
    do: normalize_durable!(item, "#{location} item")

  defp normalize_item!(other, _location), do: normalize_atom(other)

  defp normalize_default!(@no_default, _location), do: @no_default

  defp normalize_default!(default, location),
    do: Docket.Wire.dump_value!(default, "#{location} default")

  defp normalize_type(type) when type in @types, do: type
  defp normalize_type(type) when is_atom(type), do: Atom.to_string(type)
  defp normalize_type(type), do: type

  defp normalize_atom(value) when value in [nil, true, false], do: value
  defp normalize_atom(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_atom(value), do: value

  defp portable?(value) do
    case Docket.Wire.dump_value(value) do
      {:ok, normalized} -> normalized === value
      {:error, _reason} -> false
    end
  end

  defp plain_map?(value), do: is_map(value) and not is_struct(value)

  defp validate_value(%__MODULE__{required: true}, nil, location) do
    ["#{location} is required but was nil"]
  end

  defp validate_value(%__MODULE__{}, nil, _location), do: []

  defp validate_value(%__MODULE__{type: :string} = schema, value, location) do
    if is_binary(value) do
      string_constraints(schema.constraints, value, location)
    else
      ["#{location} must be a string, got #{inspect(value)}"]
    end
  end

  defp validate_value(%__MODULE__{type: :float} = schema, value, location) do
    if is_number(value) do
      number_constraints(schema.constraints, value, location)
    else
      ["#{location} must be a number, got #{inspect(value)}"]
    end
  end

  defp validate_value(%__MODULE__{type: :integer} = schema, value, location) do
    if is_integer(value) do
      number_constraints(schema.constraints, value, location)
    else
      ["#{location} must be an integer, got #{inspect(value)}"]
    end
  end

  defp validate_value(%__MODULE__{type: :boolean}, value, location) do
    if is_boolean(value), do: [], else: ["#{location} must be a boolean, got #{inspect(value)}"]
  end

  defp validate_value(%__MODULE__{type: :map}, value, location) do
    if is_map(value) and not is_struct(value) do
      []
    else
      ["#{location} must be a map, got #{inspect(value)}"]
    end
  end

  defp validate_value(%__MODULE__{type: :enum, values: values}, value, location) do
    if value in values do
      []
    else
      ["#{location} must be one of #{inspect(values)}, got #{inspect(value)}"]
    end
  end

  defp validate_value(%__MODULE__{type: :list} = schema, value, location) do
    if is_list(value) do
      items =
        case schema.item do
          %__MODULE__{} = item ->
            value
            |> Enum.with_index()
            |> Enum.flat_map(fn {element, index} ->
              validate_value(item, element, "#{location}[#{index}]")
            end)

          nil ->
            []
        end

      items ++ list_constraints(schema.constraints, value, location)
    else
      ["#{location} must be a list, got #{inspect(value)}"]
    end
  end

  defp validate_value(%__MODULE__{type: :object} = schema, value, location) do
    if is_map(value) and not is_struct(value) do
      fields = schema.fields

      unknown =
        if constraint(schema.constraints, "open") == true do
          []
        else
          for key <- Map.keys(value), not Map.has_key?(fields, key) do
            "#{location} has unknown field #{inspect(key)}"
          end
        end

      declared =
        Enum.flat_map(fields, fn {name, field_schema} ->
          case Map.fetch(value, name) do
            {:ok, field_value} ->
              validate_value(field_schema, field_value, "#{location}.#{name}")

            :error ->
              if field_schema.required do
                ["#{location}.#{name} is required but missing"]
              else
                []
              end
          end
        end)

      Enum.sort(unknown ++ declared)
    else
      ["#{location} must be a map, got #{inspect(value)}"]
    end
  end

  # ---------------------------------------------------------------------------
  # Constraint enforcement
  # ---------------------------------------------------------------------------

  defp number_constraints(constraints, value, location) do
    bound_constraints(constraints, value, location, "min", "max", "")
  end

  defp string_constraints(constraints, value, location) do
    length_reasons =
      bound_constraints(
        constraints,
        String.length(value),
        location,
        "min_length",
        "max_length",
        " characters"
      )

    length_reasons ++ pattern_constraint(constraints, value, location)
  end

  defp list_constraints(constraints, value, location) do
    bound_constraints(constraints, length(value), location, "min_items", "max_items", " items")
  end

  defp bound_constraints(constraints, measured, location, min_key, max_key, unit) do
    min = constraint(constraints, min_key)
    max = constraint(constraints, max_key)

    List.flatten([
      if is_number(min) and measured < min do
        ["#{location} must be at least #{min}#{unit}, got #{measured}#{unit}"]
      else
        []
      end,
      if is_number(max) and measured > max do
        ["#{location} must be at most #{max}#{unit}, got #{measured}#{unit}"]
      else
        []
      end
    ])
  end

  defp pattern_constraint(constraints, value, location) do
    case constraint(constraints, "pattern") do
      nil ->
        []

      pattern when is_binary(pattern) ->
        case Regex.compile(pattern) do
          {:ok, regex} ->
            if Regex.match?(regex, value) do
              []
            else
              ["#{location} must match pattern #{inspect(pattern)}, got #{inspect(value)}"]
            end

          {:error, _reason} ->
            ["#{location} has an invalid pattern constraint #{inspect(pattern)}"]
        end

      other ->
        ["#{location} has an invalid pattern constraint #{inspect(other)}"]
    end
  end

  # Constructors and the wire format store string constraint keys; the atom
  # fallback keeps hand-built structs from silently skipping enforcement.
  defp constraint(constraints, key) do
    case Map.fetch(constraints, key) do
      {:ok, value} -> value
      :error -> Map.get(constraints, String.to_atom(key))
    end
  end
end
