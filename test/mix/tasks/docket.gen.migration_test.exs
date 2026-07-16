if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Mix.Tasks.Docket.Gen.MigrationTest do
    use ExUnit.Case, async: false

    @tmp_path Path.join(System.tmp_dir!(), "docket_gen_migration_test")

    setup do
      File.rm_rf!(@tmp_path)
      File.mkdir_p!(@tmp_path)

      Mix.shell(Mix.Shell.Process)

      on_exit(fn ->
        Mix.shell(Mix.Shell.IO)
        File.rm_rf!(@tmp_path)
      end)

      :ok
    end

    test "generates a migration that delegates to Docket.Postgres.Migration" do
      [file] =
        Mix.Tasks.Docket.Gen.Migration.run([
          "-r",
          "Docket.Postgres.TestRepo",
          "--migrations-path",
          @tmp_path
        ])

      assert Path.dirname(file) == @tmp_path
      assert Path.basename(file) =~ ~r/^\d{14}_add_docket_tables\.exs$/

      content = File.read!(file)

      assert content =~
               "defmodule Docket.Postgres.TestRepo.Migrations.AddDocketTables do"

      assert content =~ "use Ecto.Migration"
      assert content =~ "Docket.Postgres.Migration.up(version: 2, prefix: \"public\")"
      assert content =~ "Docket.Postgres.Migration.down(version: 1, prefix: \"public\")"

      # The generated file must at least parse.
      assert {:ok, _ast} = Code.string_to_quoted(content)
    end

    test "generates an explicit v1-to-v2 upgrade with routine-down refusal" do
      [file] =
        Mix.Tasks.Docket.Gen.Migration.run([
          "-r",
          "Docket.Postgres.TestRepo",
          "--migrations-path",
          @tmp_path,
          "--upgrade-from-v1"
        ])

      assert Path.dirname(file) == @tmp_path
      assert Path.basename(file) =~ ~r/^\d{14}_upgrade_docket_to_v2\.exs$/

      content = File.read!(file)

      assert content =~
               "defmodule Docket.Postgres.TestRepo.Migrations.UpgradeDocketToV2 do"

      assert content =~ "Docket.Postgres.Migration.up(version: 2, prefix: \"public\")"
      assert content =~ "Docket.Postgres.Migration.down(version: 2, prefix: \"public\")"
      refute content =~ "down(version: 1,"
      assert {:ok, _ast} = Code.string_to_quoted(content)
    end

    test "generates separate nontransactional online migration with explicit custom prefix" do
      [file] =
        Mix.Tasks.Docket.Gen.Migration.run([
          "-r",
          "Docket.Postgres.TestRepo",
          "--migrations-path",
          @tmp_path,
          "--online",
          "--prefix",
          "docket_private"
        ])

      assert Path.basename(file) =~ ~r/^\d{14}_complete_docket_v2_online\.exs$/
      content = File.read!(file)

      assert content =~
               "defmodule Docket.Postgres.TestRepo.Migrations.CompleteDocketV2Online do"

      assert content =~ "@disable_ddl_transaction true"

      assert content =~
               "Docket.Postgres.OnlineMigration.up(repo: repo(), prefix: \"docket_private\")"

      assert content =~
               "Docket.Postgres.OnlineMigration.down(repo: repo(), prefix: \"docket_private\")"

      assert {:ok, _ast} = Code.string_to_quoted(content)
    end

    test "rejects ambiguous online upgrade generation and unsafe prefixes" do
      assert_raise Mix.Error, fn ->
        Mix.Tasks.Docket.Gen.Migration.run([
          "-r",
          "Docket.Postgres.TestRepo",
          "--migrations-path",
          @tmp_path,
          "--online",
          "--upgrade-from-v1"
        ])
      end

      assert_raise Mix.Error, fn ->
        Mix.Tasks.Docket.Gen.Migration.run([
          "-r",
          "Docket.Postgres.TestRepo",
          "--migrations-path",
          @tmp_path,
          "--prefix",
          "unsafe-prefix"
        ])
      end
    end
  end
end
