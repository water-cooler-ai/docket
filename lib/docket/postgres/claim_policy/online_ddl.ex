if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.OnlineDDL do
    @moduledoc """
    Canonical online index and foreign-key contract for exact-cap admission.

    Runtime migration, readiness verification, tests, and the PostgreSQL
    benchmark all consume these definitions. Keeping the predicates here makes
    a predicate or DDL fingerprint change an explicit contract change.
    """

    alias Docket.Postgres.Storage

    @ready_name "docket_runs_scope_ready_claim_index"
    @live_name "docket_runs_scope_live_claim_index"
    @foreign_key_name "docket_runs_scope_key_claim_partition_fkey"

    @ready_predicate "status = 'running' AND poisoned_at IS NULL AND claim_token IS NULL AND wake_at IS NOT NULL"
    @live_predicate "status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL"

    @type index_kind :: :ready | :live

    def index_name(:ready), do: @ready_name
    def index_name(:live), do: @live_name
    def foreign_key_name, do: @foreign_key_name

    def predicate(:ready), do: @ready_predicate
    def predicate(:live), do: @live_predicate

    def catalog_predicate(:ready) do
      "((status = 'running'::text) AND (poisoned_at IS NULL) AND " <>
        "(claim_token IS NULL) AND (wake_at IS NOT NULL))"
    end

    def catalog_predicate(:live) do
      "((status = 'running'::text) AND (poisoned_at IS NULL) AND " <>
        "(claim_token IS NOT NULL))"
    end

    def columns(:ready), do: ["scope_key", "wake_at", "id"]
    def columns(:live), do: ["scope_key", "claimed_at", "id"]

    @spec create_index_sql(String.t(), index_kind(), keyword()) :: String.t()
    def create_index_sql(prefix, kind, opts \\ []) when kind in [:ready, :live] do
      concurrently = if Keyword.get(opts, :concurrently, true), do: " CONCURRENTLY", else: ""

      "CREATE INDEX#{concurrently} #{quote_identifier(index_name(kind))} " <>
        "ON #{qualified(prefix, "docket_runs")} " <>
        "(#{Enum.join(columns(kind), ", ")}) WHERE #{predicate(kind)}"
    end

    @spec drop_index_sql(String.t(), index_kind()) :: String.t()
    def drop_index_sql(prefix, kind) when kind in [:ready, :live] do
      "DROP INDEX CONCURRENTLY #{qualified(prefix, index_name(kind))}"
    end

    @spec add_foreign_key_sql(String.t()) :: String.t()
    def add_foreign_key_sql(prefix) do
      "ALTER TABLE #{qualified(prefix, "docket_runs")} " <>
        "ADD CONSTRAINT #{@foreign_key_name} FOREIGN KEY (scope_key) " <>
        "REFERENCES #{qualified(prefix, "docket_claim_partitions")} (scope_key) " <>
        "ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID"
    end

    @spec validate_foreign_key_sql(String.t()) :: String.t()
    def validate_foreign_key_sql(prefix) do
      "ALTER TABLE #{qualified(prefix, "docket_runs")} " <>
        "VALIDATE CONSTRAINT #{@foreign_key_name}"
    end

    @spec index_fingerprint(String.t(), index_kind()) :: binary()
    def index_fingerprint(_prefix, kind) do
      :crypto.hash(:sha256, canonical_index_contract(kind))
    end

    @spec index_fingerprints(String.t()) :: %{ready: binary(), live: binary()}
    def index_fingerprints(prefix) do
      %{ready: index_fingerprint(prefix, :ready), live: index_fingerprint(prefix, :live)}
    end

    @spec index_fingerprint_hex(String.t(), index_kind()) :: String.t()
    def index_fingerprint_hex(prefix, kind) do
      prefix |> index_fingerprint(kind) |> Base.encode16(case: :lower)
    end

    defp qualified(prefix, name), do: Storage.qualified_table(prefix, name)

    defp quote_identifier(name), do: ~s("#{name}")

    defp canonical_index_contract(kind) do
      [
        "docket-online-index/v1",
        "role=#{kind}",
        "name=#{index_name(kind)}",
        "method=btree",
        "unique=false",
        "keys=#{Enum.join(columns(kind), ",")}",
        "order=asc-default-nulls",
        "opclass=column-default",
        "collation=column-default",
        "include=",
        "predicate=#{predicate(kind)}"
      ]
      |> Enum.join("\n")
    end
  end
end
