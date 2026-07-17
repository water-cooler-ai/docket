if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.Admin do
    @moduledoc """
    Minimal current-state administration for exact-cap admission.

    Updates are compare-and-set when `:expected_version` is supplied. Policy
    history, actors, receipts, legal holds, and export workflows are outside
    the v0.1.0 contract.
    """

    alias Docket.Postgres.Storage

    @type policy :: %{
            required(:max_active) => pos_integer() | nil,
            required(:version) => non_neg_integer(),
            required(:updated_at) => DateTime.t()
          }

    @spec get_default(Docket.Backend.ctx()) :: {:ok, policy()} | {:error, term()}
    def get_default(context) do
      with {:ok, control} <- control(context),
           {:ok, %{rows: [[mode, maximum, version, updated_at]]}} <-
             control.repo.query(
               "SELECT admission_mode, max_active, policy_version, updated_at " <>
                 "FROM #{control.policy} WHERE id = 1",
               []
             ) do
        {:ok,
         %{
           admission_mode: String.to_existing_atom(mode),
           max_active: maximum,
           version: version,
           updated_at: updated_at
         }}
      end
    end

    @spec put_default(Docket.Backend.ctx(), pos_integer(), keyword()) ::
            {:ok, policy()} | {:error, :stale | term()}
    def put_default(context, max_active, opts \\ []) do
      with :ok <- validate_cap(max_active),
           {:ok, expected_version} <- expected_version(opts),
           {:ok, control} <- control(context) do
        Storage.transaction(control.context, fn transaction_context ->
          {repo, _prefix} = Storage.context!(transaction_context)
          {where, params} = cas_clause(expected_version, [max_active])

          case repo.query(
                 """
                 UPDATE #{control.policy}
                 SET max_active = $1,
                     policy_version = policy_version + 1,
                     initialized_at = COALESCE(initialized_at, CURRENT_TIMESTAMP),
                     updated_at = CURRENT_TIMESTAMP
                 WHERE id = 1#{where}
                 RETURNING max_active, policy_version, updated_at
                 """,
                 params
               ) do
            {:ok, %{rows: [[maximum, version, updated_at]]}} ->
              {:ok, %{max_active: maximum, version: version, updated_at: updated_at}}

            {:ok, %{rows: []}} ->
              {:error, :stale}

            {:error, reason} ->
              {:error, reason}
          end
        end)
      end
    end

    @spec put_override(
            Docket.Backend.ctx(),
            Docket.Backend.owner_scope(),
            pos_integer(),
            keyword()
          ) ::
            {:ok, policy()} | {:error, :stale | term()}
    def put_override(context, owner_scope, max_active, opts \\ []) do
      with {:ok, scope_key} <- scope_key(owner_scope),
           :ok <- validate_cap(max_active),
           {:ok, expected_version} <- expected_version(opts),
           {:ok, control} <- control(context) do
        Storage.transaction(control.context, fn transaction_context ->
          {repo, _prefix} = Storage.context!(transaction_context)

          with {:ok, _} <-
                 repo.query(
                   "INSERT INTO #{control.partitions} (scope_key) VALUES ($1) " <>
                     "ON CONFLICT (scope_key) DO NOTHING",
                   [scope_key]
                 ) do
            {where, params} = cas_clause(expected_version, [max_active, scope_key], 3)

            case repo.query(
                   """
                   UPDATE #{control.partitions}
                   SET max_active = $1,
                       partition_version = partition_version + 1,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE scope_key = $2#{where}
                   RETURNING max_active, partition_version, updated_at
                   """,
                   params
                 ) do
              {:ok, %{rows: [[maximum, version, updated_at]]}} ->
                {:ok, %{max_active: maximum, version: version, updated_at: updated_at}}

              {:ok, %{rows: []}} ->
                {:error, :stale}

              {:error, reason} ->
                {:error, reason}
            end
          end
        end)
      end
    end

    @spec reset_override(Docket.Backend.ctx(), Docket.Backend.owner_scope(), keyword()) ::
            {:ok, policy()} | {:error, :stale | term()}
    def reset_override(context, owner_scope, opts \\ []) do
      with {:ok, scope_key} <- scope_key(owner_scope),
           {:ok, expected_version} <- expected_version(opts),
           {:ok, control} <- control(context) do
        Storage.transaction(control.context, fn transaction_context ->
          {repo, _prefix} = Storage.context!(transaction_context)
          {where, params} = cas_clause(expected_version, [scope_key], 2)

          case repo.query(
                 """
                 UPDATE #{control.partitions}
                 SET max_active = NULL,
                     partition_version = partition_version + 1,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE scope_key = $1#{where}
                 RETURNING max_active, partition_version, updated_at
                 """,
                 params
               ) do
            {:ok, %{rows: [[maximum, version, updated_at]]}} ->
              {:ok, %{max_active: maximum, version: version, updated_at: updated_at}}

            {:ok, %{rows: []}} ->
              {:error, :stale}

            {:error, reason} ->
              {:error, reason}
          end
        end)
      end
    end

    @spec get_effective(Docket.Backend.ctx(), Docket.Backend.owner_scope()) ::
            {:ok, map()} | {:error, :not_initialized | term()}
    def get_effective(context, owner_scope) do
      with {:ok, scope_key} <- scope_key(owner_scope),
           {:ok, control} <- control(context),
           {:ok, %{rows: [[default_max, default_version, override_max, override_version]]}} <-
             control.repo.query(
               """
               SELECT policy.max_active, policy.policy_version,
                      partitions.max_active, COALESCE(partitions.partition_version, 0)
               FROM #{control.policy} AS policy
               LEFT JOIN #{control.partitions} AS partitions ON partitions.scope_key = $1
               WHERE policy.id = 1
               """,
               [scope_key]
             ) do
        if is_nil(default_max) do
          {:error, :not_initialized}
        else
          {:ok,
           %{
             owner_scope: owner_scope,
             max_active: override_max || default_max,
             source: if(is_nil(override_max), do: :default, else: :override),
             default_version: default_version,
             override_version: override_version
           }}
        end
      end
    end

    defp control(context) do
      try do
        {repo, configured_prefix} = Storage.context!(context)
        prefix = Storage.physical_prefix!(repo, configured_prefix)

        {:ok,
         %{
           repo: repo,
           context: Map.put(%{repo: repo}, :prefix, prefix),
           policy: Storage.qualified_table(prefix, "docket_claim_policy"),
           partitions: Storage.qualified_table(prefix, "docket_claim_partitions")
         }}
      rescue
        exception -> {:error, exception}
      end
    end

    defp expected_version(opts) when is_list(opts) do
      cond do
        not Keyword.keyword?(opts) -> {:error, :invalid_options}
        Keyword.keys(opts) -- [:expected_version] != [] -> {:error, :invalid_options}
        true -> validate_expected_version(Keyword.get(opts, :expected_version))
      end
    end

    defp expected_version(_opts), do: {:error, :invalid_options}

    defp validate_expected_version(nil), do: {:ok, nil}

    defp validate_expected_version(version)
         when is_integer(version) and version >= 0 and version <= 9_223_372_036_854_775_807,
         do: {:ok, version}

    defp validate_expected_version(_version), do: {:error, :invalid_expected_version}

    defp cas_clause(version, params, position \\ nil)

    defp cas_clause(nil, params, _position), do: {"", params}

    defp cas_clause(version, params, nil) do
      position = length(params) + 1
      {" AND policy_version = $#{position}", params ++ [version]}
    end

    defp cas_clause(version, params, position) do
      {" AND partition_version = $#{position}", params ++ [version]}
    end

    defp validate_cap(value) when is_integer(value) and value > 0 and value <= 2_147_483_647,
      do: :ok

    defp validate_cap(_value), do: {:error, :invalid_max_active}

    defp scope_key(:tenantless), do: {:ok, ""}

    defp scope_key({:tenant, tenant_id}) when is_binary(tenant_id) and byte_size(tenant_id) > 0,
      do: {:ok, tenant_id}

    defp scope_key(_scope), do: {:error, :invalid_owner_scope}
  end
end
