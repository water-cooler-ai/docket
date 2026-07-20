if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migration do
    @moduledoc """
    Creates and versions the tables owned by Docket's PostgreSQL backend.

    Schema version 1 contains durable graphs, runs, and events. Version 2 adds exact-cap
    policy and partition authority, the authoritative unfinished-tenant ring,
    sticky logical-run admission, the domain cursor, the sole TenantFair claim
    function, and supporting indexes. Every step is an ordinary transactional
    migration.
    """

    use Ecto.Migration

    alias Docket.Postgres.ClaimPolicy.TenantFair.RingFunction
    alias Docket.Postgres.Storage

    @initial_version 1
    @current_version 2
    @default_prefix "public"

    @spec up(keyword()) :: :ok
    def up(opts \\ []) when is_list(opts) do
      opts = with_defaults(opts, @current_version)
      initial = migrated_version(opts)

      if opts.create_schema and opts.prefix != @default_prefix do
        execute(~s(CREATE SCHEMA IF NOT EXISTS "#{opts.prefix}"))
      end

      cond do
        initial == 0 -> change(@initial_version..opts.version, :up, opts)
        initial < opts.version -> change((initial + 1)..opts.version, :up, opts)
        true -> :ok
      end
    end

    @spec down(keyword()) :: :ok
    def down(opts \\ []) when is_list(opts) do
      opts = with_defaults(opts, @initial_version)
      initial = max(migrated_version(opts), @initial_version)

      if initial >= opts.version do
        change(initial..opts.version//-1, :down, opts)
      end

      :ok
    end

    @spec migrated_version(keyword() | map()) :: non_neg_integer()
    def migrated_version(opts \\ [])

    def migrated_version(opts) when is_list(opts) do
      migrated_version(with_defaults(opts, @initial_version))
    end

    def migrated_version(%{prefix: prefix} = opts) do
      repo = Map.get_lazy(opts, :repo, fn -> repo() end)

      query = """
      SELECT obj_description(pg_class.oid, 'pg_class')
      FROM pg_class
      LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
      WHERE pg_class.relname = 'docket_runs' AND pg_namespace.nspname = $1
      """

      case repo.query(query, [prefix], log: false) do
        {:ok, %{rows: [[version]]}} when is_binary(version) -> String.to_integer(version)
        _missing_or_uncommented -> 0
      end
    end

    def initial_version, do: @initial_version
    def current_version, do: @current_version

    @doc false
    @spec current_shape_query() :: String.t()
    def current_shape_query do
      identity_arguments = RingFunction.identity_arguments()

      """
      SELECT
        EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = $1
            AND table_name = 'docket_runs'
            AND column_name = 'tenant_admitted_at'
        )
        AND (
          SELECT
            count(*) = 1
            AND count(*) FILTER (
              WHERE pg_get_function_identity_arguments(pg_proc.oid) = '#{identity_arguments}'
            ) = 1
          FROM pg_proc
          JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
          WHERE pg_namespace.nspname = $1
            AND pg_proc.proname = '#{RingFunction.name()}'
            AND pg_proc.prokind = 'f'
        )
      """
    end

    @doc false
    @spec current_shape?(module(), String.t()) :: boolean()
    def current_shape?(repo, prefix) do
      case repo.query(current_shape_query(), [prefix], log: false) do
        {:ok, %{rows: [[true]]}} -> true
        _missing_or_unexpected -> false
      end
    end

    defp change(range, direction, opts) do
      for version <- range do
        padded = String.pad_leading(Integer.to_string(version), 2, "0")
        module = Module.concat([Docket.Postgres.Migrations, "V#{padded}"])
        apply(module, direction, [opts])
      end

      case {direction, range} do
        {:up, _first..last//_step} ->
          record_version(opts, last)

        {:down, _first..last//_step} when last > @initial_version ->
          record_version(opts, last - 1)

        {:down, _range} ->
          :ok
      end

      :ok
    end

    defp record_version(%{prefix: prefix}, version) do
      execute(~s(COMMENT ON TABLE "#{prefix}"."docket_runs" IS '#{version}'))
    end

    defp with_defaults(opts, default_version) do
      opts = Enum.into(opts, %{prefix: @default_prefix, version: default_version})

      unless Storage.valid_prefix?(opts.prefix) do
        raise ArgumentError,
              "expected :prefix to be a lowercase identifier up to 63 bytes, got: " <>
                inspect(opts.prefix)
      end

      unless is_integer(opts.version) and opts.version in @initial_version..@current_version do
        raise ArgumentError,
              "expected :version to be an integer between #{@initial_version} and " <>
                "#{@current_version}, got: #{inspect(opts.version)}"
      end

      Map.put_new(opts, :create_schema, opts.prefix != @default_prefix)
    end
  end
end
