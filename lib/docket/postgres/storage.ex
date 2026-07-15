if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Storage do
    @moduledoc false

    @type ctx :: module() | %{required(:repo) => module(), optional(:prefix) => String.t() | nil}
    @type normalized_ctx :: %{required(:repo) => module(), required(:prefix) => String.t() | nil}

    @prefix_pattern ~r/^[a-z_][a-z0-9_]*$/

    def transaction(ctx, fun) when is_function(fun, 1) do
      {repo, prefix} = context!(ctx)
      transaction_ctx = %{repo: repo, prefix: prefix}
      invalid_result = make_ref()

      case repo.transaction(fn ->
             case fun.(transaction_ctx) do
               {:ok, value} ->
                 value

               {:error, reason} ->
                 repo.rollback(reason)

               other ->
                 repo.rollback({invalid_result, other})
             end
           end) do
        {:ok, value} ->
          {:ok, value}

        {:error, {^invalid_result, other}} ->
          raise ArgumentError,
                "transaction callback must return {:ok, value} or {:error, reason}, got: " <>
                  inspect(other)

        {:error, reason} ->
          {:error, reason}
      end
    end

    def transaction(_ctx, fun) do
      raise ArgumentError,
            "transaction callback must be a one-argument function, got: #{inspect(fun)}"
    end

    @doc false
    @spec context!(ctx()) :: {module(), String.t() | nil}
    def context!(repo) when is_atom(repo), do: {repo, nil}

    def context!(%{repo: repo} = ctx) when is_atom(repo) do
      case Map.get(ctx, :prefix) do
        nil ->
          {repo, nil}

        prefix when is_binary(prefix) ->
          unless valid_prefix?(prefix) do
            raise ArgumentError,
                  "Postgres context prefix must be a lowercase identifier up to 63 bytes, got: " <>
                    inspect(prefix)
          end

          {repo, prefix}

        prefix ->
          raise ArgumentError,
                "Postgres context prefix must be a string or nil, got: #{inspect(prefix)}"
      end
    end

    def context!(ctx) do
      raise ArgumentError,
            "Postgres context must be a Repo or contain :repo and optional :prefix, got: " <>
              inspect(ctx)
    end

    @doc false
    @spec valid_prefix?(term()) :: boolean()
    def valid_prefix?(prefix) do
      is_binary(prefix) and byte_size(prefix) in 1..63 and Regex.match?(@prefix_pattern, prefix)
    end

    @doc false
    @spec qualified_table(String.t() | nil, String.t()) :: String.t()
    def qualified_table(nil, table), do: quote_identifier(table)

    def qualified_table(prefix, table) when is_binary(prefix) do
      quote_identifier(prefix) <> "." <> quote_identifier(table)
    end

    defp quote_identifier(identifier) do
      ~s("#{String.replace(identifier, "\"", "\"\"")}")
    end
  end
end
