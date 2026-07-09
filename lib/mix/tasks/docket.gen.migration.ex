if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Mix.Tasks.Docket.Gen.Migration do
    @shortdoc "Generates the migration that installs Docket's Postgres tables"

    @moduledoc """
    Generates the host-application migration that installs (and can roll
    back) Docket's Postgres tables via `Docket.Postgres.Migration`.

        $ mix docket.gen.migration
        $ mix docket.gen.migration -r MyApp.Repo

    The generated file pins the current schema version; upgrading Docket
    later means generating a new migration (or editing `version:`), never
    editing this one.

    ## Options

      * `-r`, `--repo` — the repo to generate the migration for. Defaults to
        the repos configured under `:ecto_repos`.
      * `--migrations-path` — the directory to write the migration into.
        Defaults to the repo's `priv/.../migrations` directory.
    """

    use Mix.Task

    import Mix.Ecto, only: [parse_repo: 1, ensure_repo: 2]
    import Mix.EctoSQL, only: [source_repo_priv: 1]
    import Mix.Generator, only: [create_directory: 1, create_file: 2]

    @switches [migrations_path: :string, repo: [:string, :keep]]
    @aliases [r: :repo]

    @impl Mix.Task
    def run(args) do
      {opts, _args} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)

      for repo <- parse_repo(args) do
        ensure_repo(repo, args)

        path =
          opts[:migrations_path] || Path.join(source_repo_priv(repo), "migrations")

        file = Path.join(path, "#{timestamp()}_add_docket_tables.exs")
        module = Module.concat([repo, Migrations, AddDocketTables])
        version = Docket.Postgres.Migration.current_version()

        create_directory(path)

        create_file(file, """
        defmodule #{inspect(module)} do
          use Ecto.Migration

          def up, do: Docket.Postgres.Migration.up(version: #{version})

          def down, do: Docket.Postgres.Migration.down(version: #{version})
        end
        """)

        file
      end
    end

    defp timestamp do
      %{year: y, month: mo, day: d, hour: h, minute: mi, second: s} = DateTime.utc_now()
      [y, pad(mo), pad(d), pad(h), pad(mi), pad(s)] |> Enum.join()
    end

    defp pad(part), do: String.pad_leading(Integer.to_string(part), 2, "0")
  end
end
