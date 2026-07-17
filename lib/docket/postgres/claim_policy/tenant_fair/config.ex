if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.Config do
    @moduledoc false

    @enforce_keys [:default_max_active]
    defstruct @enforce_keys

    @type t :: %__MODULE__{default_max_active: pos_integer()}

    @spec new(keyword()) :: {:ok, t()} | {:error, term()}
    def new(options) when is_list(options) do
      with true <- Keyword.keyword?(options) || {:error, :not_keyword},
           [] <- Keyword.keys(options) -- [:default_max_active],
           {:ok, maximum} <- Keyword.fetch(options, :default_max_active),
           true <- is_integer(maximum) and maximum > 0 and maximum <= 2_147_483_647 do
        {:ok, %__MODULE__{default_max_active: maximum}}
      else
        {:error, :not_keyword} -> {:error, :not_keyword}
        :error -> {:error, {:missing_option, :default_max_active}}
        unknown when is_list(unknown) -> {:error, {:unknown_options, unknown}}
        false -> {:error, {:invalid_option, :default_max_active}}
      end
    end

    def new(_options), do: {:error, :not_keyword}
  end
end
