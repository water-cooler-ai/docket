if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Mix.Tasks.Docket.Gen.Migration do
    @shortdoc "Generates a Docket PostgreSQL migration"

    @moduledoc """
    Generates the fresh-install Docket schema migration.

    The generated `up/0` installs the current schema version and `down/0`
    removes the Docket schema entirely. Hosts upgrading an existing
    installation do not use this task: each release's upgrade guide documents
    a hand-written migration pinning `Docket.Postgres.Migration.up/1` and
    `down/1` to the versions that release adds, so a rollback returns the
    host to its pre-upgrade schema.
    """

    use Mix.Task

    import Mix.Ecto, only: [parse_repo: 1, ensure_repo: 2]
    import Mix.EctoSQL, only: [source_repo_priv: 1]
    import Mix.Generator, only: [create_directory: 1, create_file: 2]

    @switches [
      migrations_path: :string,
      repo: [:string, :keep],
      prefix: :string
    ]
    @aliases [r: :repo]

    @impl Mix.Task
    def run(args) do
      {opts, _args} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)

      for repo <- parse_repo(args) do
        ensure_repo(repo, args)
        path = opts[:migrations_path] || Path.join(source_repo_priv(repo), "migrations")
        prefix = Keyword.get(opts, :prefix, "public")

        unless Docket.Postgres.Storage.valid_prefix?(prefix) do
          Mix.raise("--prefix must be a lowercase identifier up to 63 bytes")
        end

        version = Docket.Postgres.Migration.current_version()
        down_version = Docket.Postgres.Migration.initial_version()

        file = Path.join(path, "#{timestamp()}_add_docket_tables.exs")
        module = Module.concat([repo, Migrations, AddDocketTables])

        content = """
        defmodule #{inspect(module)} do
          use Ecto.Migration

          def up,
            do: Docket.Postgres.Migration.up(version: #{version}, prefix: #{inspect(prefix)})

          def down,
            do: Docket.Postgres.Migration.down(version: #{down_version}, prefix: #{inspect(prefix)})
        end
        """

        create_directory(path)
        create_file(file, content)
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
