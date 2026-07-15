if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.BackendConformance.PostgresHarness do
    @moduledoc false
    @behaviour Docket.Backend.Conformance.Harness

    alias Docket.Backend.Conformance.Instance
    alias Docket.Postgres.ConformanceTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.{Event, GraphVersion, Run}

    @migration_version 20_260_715_000_052

    defmodule InstallDocket do
      @moduledoc false
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    @impl true
    def setup_suite do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      ExUnit.Callbacks.start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      {:ok, TestRepo}
    end

    @impl true
    def setup_case(TestRepo, _context) do
      TestRepo.delete_all(Event)
      TestRepo.delete_all(Run)
      TestRepo.delete_all(GraphVersion)
      Docket.Postgres.GraphCache.clear()
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      {:ok,
       %Instance{
         backend: Docket.Postgres,
         context: %{repo: TestRepo},
         namespace: "postgres-#{System.unique_integer([:positive, :monotonic])}",
         now: now
       }}
    end

    @impl true
    def teardown_case(_instance), do: Docket.Postgres.GraphCache.clear()
  end
end
