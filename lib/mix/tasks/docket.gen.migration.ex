if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Mix.Tasks.Docket.Gen.Migration do
    @shortdoc "Generates a fresh install or v1-to-v2 Docket migration"

    @moduledoc """
    Generates a host-application migration that either installs Docket's
    complete Postgres schema or upgrades an existing version-1 installation.

        $ mix docket.gen.migration
        $ mix docket.gen.migration -r MyApp.Repo
        $ mix docket.gen.migration --upgrade-from-v1

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
    """

    use Mix.Task

    import Mix.Ecto, only: [parse_repo: 1, ensure_repo: 2]
    import Mix.EctoSQL, only: [source_repo_priv: 1]
    import Mix.Generator, only: [create_directory: 1, create_file: 2]

    @switches [migrations_path: :string, repo: [:string, :keep], upgrade_from_v1: :boolean]
    @aliases [r: :repo]

    @impl Mix.Task
    def run(args) do
      {opts, _args} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)

      for repo <- parse_repo(args) do
        ensure_repo(repo, args)

        path =
          opts[:migrations_path] || Path.join(source_repo_priv(repo), "migrations")

        {basename, migration_name, version, down_version} = generation(opts)
        file = Path.join(path, "#{timestamp()}_#{basename}.exs")
        module = Module.concat([repo, Migrations, migration_name])

        create_directory(path)

        create_file(file, """
        defmodule #{inspect(module)} do
          use Ecto.Migration

          def up, do: Docket.Postgres.Migration.up(version: #{version})

          def down, do: Docket.Postgres.Migration.down(version: #{down_version})
        end
        """)

        file
      end
    end

    defp generation(opts) do
      if Keyword.get(opts, :upgrade_from_v1, false) do
        {"upgrade_docket_to_v2", UpgradeDocketToV2, 2, 2}
      else
        {"add_docket_tables", AddDocketTables, Docket.Postgres.Migration.current_version(),
         Docket.Postgres.Migration.initial_version()}
      end
    end

    defp timestamp do
      %{year: y, month: mo, day: d, hour: h, minute: mi, second: s} = DateTime.utc_now()
      [y, pad(mo), pad(d), pad(h), pad(mi), pad(s)] |> Enum.join()
    end

    defp pad(part), do: String.pad_leading(Integer.to_string(part), 2, "0")
  end
end
