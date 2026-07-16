if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Mix.Tasks.Docket.Gen.Migration do
    @shortdoc "Generates a fresh install or v1-to-v2 Docket migration"

    @moduledoc """
    Generates a host-application migration that either installs Docket's
    complete Postgres schema or upgrades an existing version-1 installation.

        $ mix docket.gen.migration
        $ mix docket.gen.migration -r MyApp.Repo
        $ mix docket.gen.migration --upgrade-from-v1
        $ mix docket.gen.migration --online --prefix public

    The default fresh-install artifact migrates from no Docket tables through
    the current schema. `--upgrade-from-v1` instead emits the explicit v1-to-v2
    step. Both generated `down/0` callbacks use the routine migration surface,
    which deliberately refuses once v2 is installed. Destructive v2 teardown
    is a separate operator action through
    `Docket.Postgres.Migration.destructive_down/1`; it requires stopped-fleet,
    audit-export, receipt-loss, and partition-loss acknowledgements plus live
    database guards. Commit the chosen artifact with the host application and
    never edit it after it has been applied.

    ## Options

      * `-r`, `--repo` — the repo to generate the migration for. Defaults to
        the repos configured under `:ecto_repos`.
      * `--migrations-path` — the directory to write the migration into.
        Defaults to the repo's `priv/.../migrations` directory.
      * `--upgrade-from-v1` — generate the pinned v1-to-v2 upgrade artifact
        instead of a fresh installation.
      * `--online` — generate the separate resumable online DDL migration.
        The artifact always sets `@disable_ddl_transaction true` and retains
        Ecto's migration lock in addition to Docket's prefix advisory runner.
      * `--prefix` — explicit lowercase PostgreSQL schema for every generated
        migration. Defaults to `"public"`.
    """

    use Mix.Task

    import Mix.Ecto, only: [parse_repo: 1, ensure_repo: 2]
    import Mix.EctoSQL, only: [source_repo_priv: 1]
    import Mix.Generator, only: [create_directory: 1, create_file: 2]

    @switches [
      migrations_path: :string,
      repo: [:string, :keep],
      upgrade_from_v1: :boolean,
      online: :boolean,
      prefix: :string
    ]
    @aliases [r: :repo]

    @impl Mix.Task
    def run(args) do
      {opts, _args} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)

      for repo <- parse_repo(args) do
        ensure_repo(repo, args)

        path =
          opts[:migrations_path] || Path.join(source_repo_priv(repo), "migrations")

        prefix = Keyword.get(opts, :prefix, "public")

        unless Docket.Postgres.Storage.valid_prefix?(prefix) do
          Mix.raise("--prefix must be a lowercase identifier up to 63 bytes")
        end

        {basename, migration_name, content} = generation(opts, prefix, repo)
        file = Path.join(path, "#{timestamp()}_#{basename}.exs")
        module = Module.concat([repo, Migrations, migration_name])

        create_directory(path)

        create_file(file, String.replace(content, "__MODULE__", inspect(module)))

        file
      end
    end

    defp generation(opts, prefix, _repo) do
      online? = Keyword.get(opts, :online, false)
      upgrade? = Keyword.get(opts, :upgrade_from_v1, false)

      if online? and upgrade? do
        Mix.raise("--online and --upgrade-from-v1 are mutually exclusive")
      end

      cond do
        online? ->
          {"complete_docket_v2_online", CompleteDocketV2Online,
           """
           defmodule __MODULE__ do
             use Ecto.Migration

             @disable_ddl_transaction true

             def up,
               do: Docket.Postgres.OnlineMigration.up(repo: repo(), prefix: #{inspect(prefix)})

             def down,
               do: Docket.Postgres.OnlineMigration.down(repo: repo(), prefix: #{inspect(prefix)})
           end
           """}

        upgrade? ->
          schema_migration(
            "upgrade_docket_to_v2",
            UpgradeDocketToV2,
            2,
            2,
            prefix
          )

        true ->
          schema_migration(
            "add_docket_tables",
            AddDocketTables,
            Docket.Postgres.Migration.current_version(),
            Docket.Postgres.Migration.initial_version(),
            prefix
          )
      end
    end

    defp schema_migration(basename, name, version, down_version, prefix) do
      {basename, name,
       """
       defmodule __MODULE__ do
         use Ecto.Migration

         def up,
           do: Docket.Postgres.Migration.up(version: #{version}, prefix: #{inspect(prefix)})

         def down,
           do: Docket.Postgres.Migration.down(version: #{down_version}, prefix: #{inspect(prefix)})
       end
       """}
    end

    defp timestamp do
      %{year: y, month: mo, day: d, hour: h, minute: mi, second: s} = DateTime.utc_now()
      [y, pad(mo), pad(d), pad(h), pad(mi), pad(s)] |> Enum.join()
    end

    defp pad(part), do: String.pad_leading(Integer.to_string(part), 2, "0")
  end
end
