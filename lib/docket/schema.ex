defmodule Docket.Schema do
  @moduledoc """
  Serializable schema IR used by graph fields and node contracts.

  The validation engine is intentionally not implemented in this outline; these
  constructors establish the public shape that later compiler and runtime
  validation will consume.
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
end
