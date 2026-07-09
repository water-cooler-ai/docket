if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migration do
    @moduledoc """
    Creates and versions the tables Docket's Postgres backend owns.

    The host application owns a single migration file that delegates here;
    bumping `version:` (or omitting it to take the latest) applies any
    steps the database is missing.

    ## Usage

    Generate the host migration:

        $ mix docket.gen.migration

    Or write it by hand:

        defmodule MyApp.Repo.Migrations.AddDocketTables do
          use Ecto.Migration

          def up, do: Docket.Postgres.Migration.up(version: 1)
          def down, do: Docket.Postgres.Migration.down(version: 1)
        end

    Then run it as usual:

        $ mix ecto.migrate

    ## Options

      * `:version` — the target schema version. `up/1` defaults to the newest
        version; `down/1` defaults to rolling everything back.
      * `:prefix` — the Postgres schema (namespace) to install the tables
        into. Defaults to `"public"`.
      * `:create_schema` — whether `up/1` should `CREATE SCHEMA IF NOT
        EXISTS` for a non-default `:prefix`. Defaults to `true` whenever
        `:prefix` is set.

    ## Tables

    Version 1 installs `docket_graph_versions`, `docket_runs`, and
    `docket_events`.

    The migrated version is recorded as a `COMMENT` on the `docket_runs`
    table, so `up/1` and `down/1` are idempotent and only apply the steps
    the database is actually missing.
    """

    use Ecto.Migration

    @initial_version 1
    @current_version 1
    @default_prefix "public"

    @doc """
    Migrates the Docket tables up to `:version` (defaults to the newest).

    Must be called from within a host `Ecto.Migration`.
    """
    @spec up(keyword()) :: :ok
    def up(opts \\ []) when is_list(opts) do
      opts = with_defaults(opts, @current_version)
      initial = migrated_version(opts)

      if opts.create_schema and opts.prefix != @default_prefix do
        execute("CREATE SCHEMA IF NOT EXISTS #{opts.prefix}")
      end

      cond do
        initial == 0 -> change(@initial_version..opts.version, :up, opts)
        initial < opts.version -> change((initial + 1)..opts.version, :up, opts)
        true -> :ok
      end
    end

    @doc """
    Rolls the Docket tables back down to (and including) `:version`.

    `down(version: 1)` — the default — removes everything.
    Must be called from within a host `Ecto.Migration`.
    """
    @spec down(keyword()) :: :ok
    def down(opts \\ []) when is_list(opts) do
      opts = with_defaults(opts, @initial_version)
      initial = max(migrated_version(opts), @initial_version)

      if initial >= opts.version do
        change(initial..opts.version//-1, :down, opts)
      end

      :ok
    end

    @doc """
    The schema version currently installed in the database, or `0` when the
    Docket tables are absent.

    Reads through the migration runner's repo by default; pass `:repo` to
    query outside of a migration.
    """
    @spec migrated_version(keyword() | map()) :: non_neg_integer()
    def migrated_version(opts \\ [])

    def migrated_version(opts) when is_list(opts) do
      migrated_version(with_defaults(opts, @initial_version))
    end

    def migrated_version(%{prefix: prefix} = opts) do
      repo = Map.get_lazy(opts, :repo, fn -> repo() end)

      query = """
      SELECT description
      FROM pg_class
      LEFT JOIN pg_description ON pg_description.objoid = pg_class.oid
      LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
      WHERE pg_class.relname = 'docket_runs' AND pg_namespace.nspname = $1
      """

      case repo.query(query, [prefix], log: false) do
        {:ok, %{rows: [[version]]}} when is_binary(version) -> String.to_integer(version)
        _missing_or_uncommented -> 0
      end
    end

    @doc false
    @spec initial_version() :: pos_integer()
    def initial_version, do: @initial_version

    @doc false
    @spec current_version() :: pos_integer()
    def current_version, do: @current_version

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
          # V01's down drops docket_runs, and the version comment goes with it.
          :ok
      end

      :ok
    end

    defp record_version(%{prefix: prefix}, version) do
      execute("COMMENT ON TABLE #{prefix}.docket_runs IS '#{version}'")
    end

    defp with_defaults(opts, default_version) do
      opts = Enum.into(opts, %{prefix: @default_prefix, version: default_version})

      unless opts.prefix =~ ~r/^[a-zA-Z0-9_]+$/ do
        raise ArgumentError,
              "expected :prefix to be a simple identifier, got: #{inspect(opts.prefix)}"
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
