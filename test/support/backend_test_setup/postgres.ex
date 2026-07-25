if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.BackendTestSetup.Postgres do
    @moduledoc false

    alias Docket.Postgres.SharedBackendTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.{Event, GraphVersion, Run}

    @migration_version 20_260_715_000_052

    defmodule InstallDocket do
      @moduledoc false
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    def setup_suite do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      ExUnit.Callbacks.start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      {:ok, backend_test_suite: TestRepo}
    end

    def setup(%{backend_test_suite: TestRepo}) do
      TestRepo.delete_all(Event)
      TestRepo.delete_all(Run)
      TestRepo.delete_all(GraphVersion)
      Docket.Postgres.GraphCache.clear()
      ExUnit.Callbacks.on_exit(&Docket.Postgres.GraphCache.clear/0)
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      subject = %{
        backend: Docket.Postgres,
        context: Docket.Postgres.TestAdmissionContext.resolve(%{repo: TestRepo}),
        namespace: "postgres-#{System.unique_integer([:positive, :monotonic])}",
        now: now
      }

      {:ok, backend_test: subject}
    end
  end
end
