if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.TransitionReceipt do
    @moduledoc false

    alias Docket.Postgres.Storage

    @type operation :: :initialize | :claimed | :unclaimed

    @spec reserve(
            Docket.Backend.ctx(),
            Docket.Backend.scope(),
            operation(),
            String.t(),
            binary(),
            binary()
          ) :: :fresh | :replay | {:error, :not_found | :conflict}
    def reserve(ctx, scope, operation, transition_id, digest, result) do
      {repo, prefix} = Storage.context!(ctx)
      attempt_id = Ecto.UUID.generate()

      params = [
        transition_id,
        scope_key(scope),
        Atom.to_string(operation),
        digest,
        result,
        dump_uuid!(attempt_id)
      ]

      case repo.query(reserve_statement(prefix), params, log: false) do
        {:ok, %{rows: [[true]]}} -> :fresh
        {:ok, %{rows: [[false]]}} -> :replay
        {:ok, %{rows: []}} -> classify(ctx, scope, operation, transition_id, digest)
        {:error, reason} -> {:error, reason}
      end
    end

    @spec classify(
            Docket.Backend.ctx(),
            Docket.Backend.scope(),
            operation(),
            String.t(),
            binary()
          ) :: :replay | {:error, :not_found | :conflict}
    def classify(ctx, scope, operation, transition_id, digest) do
      {repo, prefix} = Storage.context!(ctx)
      receipts = Storage.qualified_table(prefix, "docket_transition_receipts")

      case repo.query(
             "SELECT scope_key, operation, digest FROM #{receipts} WHERE transition_id = $1",
             [transition_id],
             log: false
           ) do
        {:ok, %{rows: [[stored_scope, stored_operation, stored_digest]]}} ->
          cond do
            stored_scope != scope_key(scope) -> {:error, :not_found}
            stored_operation != Atom.to_string(operation) -> {:error, :conflict}
            stored_digest != digest -> {:error, :conflict}
            true -> :replay
          end

        {:ok, %{rows: []}} ->
          {:error, :conflict}

        {:error, reason} ->
          {:error, reason}
      end
    end

    @doc false
    def scope_key(:system), do: "system"
    def scope_key(:tenantless), do: "owner:tenantless"

    def scope_key({:tenant, tenant_id}) when is_binary(tenant_id) and byte_size(tenant_id) > 0,
      do: "owner:tenant:" <> tenant_id

    def scope_key(scope) do
      raise ArgumentError,
            "scope must be :system, :tenantless, or {:tenant, tenant_id}, got: " <>
              inspect(scope)
    end

    @doc false
    def reserve_statement(prefix) do
      receipts = Storage.qualified_table(prefix, "docket_transition_receipts")

      """
      INSERT INTO #{receipts} AS stored (
        transition_id, scope_key, operation, digest, result, attempt_id, inserted_at
      )
      VALUES ($1, $2, $3, $4, $5, $6::uuid, clock_timestamp())
      ON CONFLICT (transition_id) DO UPDATE
      SET transition_id = stored.transition_id
      WHERE stored.scope_key = EXCLUDED.scope_key
        AND stored.operation = EXCLUDED.operation
        AND stored.digest = EXCLUDED.digest
      RETURNING attempt_id = $6::uuid
      """
    end

    defp dump_uuid!(uuid) do
      case Ecto.UUID.dump(uuid) do
        {:ok, dumped} -> dumped
        :error -> raise ArgumentError, "receipt attempt ID must be a valid UUID"
      end
    end
  end
end
