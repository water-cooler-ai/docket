if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.Config do
    @moduledoc """
    Validated, data-only configuration for the future TenantFair claim engine.

    This value deliberately does not make TenantFair selectable. The engine,
    admission behavior, and schema support ship in later slices.
    """

    @maximum_integer 2_147_483_647
    @option_keys [
      :partition_by,
      :default_preferred_active,
      :default_max_active,
      :default_weight,
      :borrowing
    ]
    @required_keys [
      :partition_by,
      :default_preferred_active,
      :default_max_active,
      :default_weight
    ]

    @enforce_keys [
      :partition_by,
      :default_preferred_active,
      :default_max_active,
      :default_weight,
      :borrowing
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            partition_by: :tenant_id,
            default_preferred_active: 0..2_147_483_647,
            default_max_active: 0..2_147_483_647,
            default_weight: 1..2_147_483_647,
            borrowing: boolean()
          }

    @type error_reason ::
            {:expected_keyword_list, term()}
            | {:duplicate_options, [atom()]}
            | {:unknown_options, [atom()]}
            | {:missing_options, [atom()]}
            | {:invalid_option, atom(), term(), term()}
            | {:invalid_relationship, atom(), term(), atom(), term()}

    @doc false
    @spec new(term()) :: {:ok, t()} | {:error, error_reason()}
    def new(options) do
      with :ok <- validate_keyword(options),
           :ok <- validate_duplicate_keys(options),
           :ok <- validate_known_keys(options),
           :ok <- validate_required_keys(options),
           partition_by = Keyword.fetch!(options, :partition_by),
           preferred = Keyword.fetch!(options, :default_preferred_active),
           maximum = Keyword.fetch!(options, :default_max_active),
           weight = Keyword.fetch!(options, :default_weight),
           borrowing = Keyword.get(options, :borrowing, false),
           :ok <- validate_partition_by(partition_by),
           :ok <- validate_non_negative(:default_preferred_active, preferred),
           :ok <- validate_non_negative(:default_max_active, maximum),
           :ok <- validate_positive(:default_weight, weight),
           :ok <- validate_boolean(:borrowing, borrowing),
           :ok <- validate_relationship(preferred, maximum) do
        {:ok,
         %__MODULE__{
           partition_by: partition_by,
           default_preferred_active: preferred,
           default_max_active: maximum,
           default_weight: weight,
           borrowing: borrowing
         }}
      end
    end

    defp validate_keyword(options) do
      if Keyword.keyword?(options), do: :ok, else: {:error, {:expected_keyword_list, options}}
    end

    defp validate_duplicate_keys(options) do
      case duplicate_keys(options) do
        [] -> :ok
        duplicates -> {:error, {:duplicate_options, duplicates}}
      end
    end

    defp validate_known_keys(options) do
      unknown = options |> Keyword.keys() |> Enum.reject(&(&1 in @option_keys)) |> Enum.uniq()

      if unknown == [], do: :ok, else: {:error, {:unknown_options, unknown}}
    end

    defp validate_required_keys(options) do
      missing = Enum.reject(@required_keys, &Keyword.has_key?(options, &1))
      if missing == [], do: :ok, else: {:error, {:missing_options, missing}}
    end

    defp validate_partition_by(:tenant_id), do: :ok

    defp validate_partition_by(value),
      do: {:error, {:invalid_option, :partition_by, value, {:expected, :tenant_id}}}

    defp validate_non_negative(_key, value)
         when is_integer(value) and value >= 0 and value <= @maximum_integer,
         do: :ok

    defp validate_non_negative(key, value),
      do: {:error, {:invalid_option, key, value, {:integer_range, 0, @maximum_integer}}}

    defp validate_positive(_key, value)
         when is_integer(value) and value >= 1 and value <= @maximum_integer,
         do: :ok

    defp validate_positive(key, value),
      do: {:error, {:invalid_option, key, value, {:integer_range, 1, @maximum_integer}}}

    defp validate_boolean(_key, value) when is_boolean(value), do: :ok

    defp validate_boolean(key, value),
      do: {:error, {:invalid_option, key, value, :boolean}}

    defp validate_relationship(preferred, maximum) when preferred <= maximum, do: :ok

    defp validate_relationship(preferred, maximum) do
      {:error,
       {:invalid_relationship, :default_preferred_active, preferred, :default_max_active, maximum}}
    end

    defp duplicate_keys(options) do
      options
      |> Keyword.keys()
      |> Enum.frequencies()
      |> Enum.flat_map(fn
        {key, count} when count > 1 -> [key]
        {_key, _count} -> []
      end)
      |> Enum.sort()
    end
  end
end
