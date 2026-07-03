defmodule Docket.Schema do
  @moduledoc """
  Serializable schema IR used by graph fields and node contracts.

  `validate/2` implements the minimal v1 validation engine used by the
  compiler for field defaults and node config, and later by the runtime for
  input payloads and state writes.
  """

  @no_default :__docket_no_default__

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

  @type primitive_type :: :float | :map | :string
  @type type :: primitive_type() | :enum | :object

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

  @spec map(keyword()) :: t()
  def map(opts \\ []), do: build(:map, opts)

  @spec object(map(), keyword()) :: t()
  def object(fields, opts \\ []) when is_map(fields) do
    :object
    |> build(opts)
    |> Map.put(:fields, fields)
  end

  @spec enum([term()], keyword()) :: t()
  def enum(values, opts \\ []) when is_list(values) do
    :enum
    |> build(opts)
    |> Map.put(:values, values)
  end

  defp build(type, opts) do
    {known, constraints} =
      Keyword.split(opts, [:required, :default, :metadata])

    %__MODULE__{
      type: type,
      required: Keyword.get(known, :required, false),
      default: Keyword.get(known, :default, @no_default),
      metadata: Keyword.get(known, :metadata, %{}),
      constraints: Map.new(constraints)
    }
  end

  @doc """
  Validates a value against a schema with the minimal v1 engine.

  Checks primitive types, enum membership, required object fields, and unknown
  object keys. `nil` is valid unless the schema is `required`. Constraints
  beyond these are ignored in v1.

  Returns `:ok` or `{:error, reasons}` with human-readable reason strings.
  """
  @spec validate(t(), term()) :: :ok | {:error, [String.t()]}
  def validate(%__MODULE__{} = schema, value) do
    case validate_value(schema, value, "value") do
      [] -> :ok
      reasons -> {:error, reasons}
    end
  end

  defp validate_value(%__MODULE__{required: true}, nil, location) do
    ["#{location} is required but was nil"]
  end

  defp validate_value(%__MODULE__{}, nil, _location), do: []

  defp validate_value(%__MODULE__{type: :string}, value, location) do
    if is_binary(value), do: [], else: ["#{location} must be a string, got #{inspect(value)}"]
  end

  defp validate_value(%__MODULE__{type: :float}, value, location) do
    if is_number(value), do: [], else: ["#{location} must be a number, got #{inspect(value)}"]
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

  defp validate_value(%__MODULE__{type: :object, fields: fields}, value, location) do
    if is_map(value) and not is_struct(value) do
      unknown =
        for key <- Map.keys(value), not Map.has_key?(fields, key) do
          "#{location} has unknown field #{inspect(key)}"
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
end
