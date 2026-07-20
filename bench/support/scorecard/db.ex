Postgrex.Types.define(Docket.Bench.Scorecard.Types, [], json: JSON)

defmodule Docket.Bench.Scorecard.Repo do
  @moduledoc false
  use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
end

defmodule Docket.Bench.Scorecard.Migration do
  @moduledoc false
  use Ecto.Migration

  def up do
    prefix = Application.fetch_env!(:docket, :scorecard_bench_prefix)
    Docket.Postgres.Migration.up(prefix: prefix, create_schema: false)
  end

  def down do
    prefix = Application.fetch_env!(:docket, :scorecard_bench_prefix)
    Docket.Postgres.Migration.down(prefix: prefix)
  end
end

defmodule Docket.Bench.Scorecard.Db do
  @moduledoc "Repo lifecycle, scratch schema management, reset, and environment metadata."

  alias Docket.Bench.Scorecard.{Migration, Repo, Types}

  @migration_version 20_260_715_000_038

  def repo, do: Repo

  def database_url! do
    System.get_env("DOCKET_BENCH_DATABASE_URL") ||
      raise "DOCKET_BENCH_DATABASE_URL must name a dedicated benchmark-capable database"
  end

  def start_repo!(url, pool_size) do
    {:ok, _pid} =
      Repo.start_link(
        url: url,
        pool_size: pool_size,
        queue_target: 5_000,
        queue_interval: 5_000,
        types: Types,
        log: false
      )

    :ok
  end

  def stop_repo do
    Supervisor.stop(Repo)
  end

  def scratch_prefix do
    suffix = System.unique_integer([:positive, :monotonic])
    "docket_bench_#{System.pid()}_#{suffix}" |> String.replace(~r/[^a-z0-9_]/, "_")
  end

  def create_schema!(prefix) do
    validate_owned_prefix!(prefix)
    Repo.query!("CREATE SCHEMA #{quote_identifier(prefix)}")
    Application.put_env(:docket, :scorecard_bench_prefix, prefix)

    :ok =
      Ecto.Migrator.up(Repo, @migration_version, Migration,
        log: false,
        prefix: prefix
      )

    :ok
  end

  def drop_schema_if_exists!(prefix) do
    validate_owned_prefix!(prefix)
    Repo.query!("DROP SCHEMA IF EXISTS #{quote_identifier(prefix)} CASCADE")
    :ok
  end

  def reset(ctx) do
    runs = table(ctx.prefix, "docket_runs")
    graphs = table(ctx.prefix, "docket_graph_versions")
    partitions = table(ctx.prefix, "docket_claim_partitions")
    policy = table(ctx.prefix, "docket_claim_policy")

    {:ok, _result} =
      Repo.transaction(fn ->
        # V2 protects trigger-maintained unfinished counts by rejecting TRUNCATE.
        # Deleting runs first lets the activity trigger bring every schedule row
        # to zero before its owning partition is removed.
        Repo.query!("DELETE FROM #{runs}")
        Repo.query!("DELETE FROM #{graphs}")
        Repo.query!("DELETE FROM #{partitions}")

        # Policy variants share this isolated scratch schema. With no runs or
        # partitions left, restore its pre-admission state so Legacy and
        # TenantFair trials remain independent.
        Repo.query!("""
        UPDATE #{policy}
        SET admission_mode = 'legacy',
            max_active = NULL,
            policy_version = 0,
            scan_ring_position = 0,
            initialized_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """)
      end)

    :ok
  end

  def finished_runs(ctx) do
    runs = table(ctx.prefix, "docket_runs")

    Repo.query!("SELECT run_id, finished_at, status FROM #{runs} WHERE finished_at IS NOT NULL").rows
    |> Enum.map(fn [run_id, finished_at, status] ->
      %{run_id: run_id, finished_at: finished_at, status: status}
    end)
  end

  def unfinished_count(ctx) do
    runs = table(ctx.prefix, "docket_runs")
    [[count]] = Repo.query!("SELECT count(*) FROM #{runs} WHERE finished_at IS NULL").rows
    count
  end

  def table(prefix, name), do: quote_identifier(prefix) <> "." <> quote_identifier(name)

  def quote_identifier(value), do: ~s("#{String.replace(value, "\"", "\"\"")}")

  def require_postgres_13!(version) when version >= 130_000, do: :ok

  def require_postgres_13!(version) do
    raise "scorecard benchmark requires PostgreSQL 13+, got server_version_num=#{version}"
  end

  def postgres_metadata! do
    [[server_version_num]] = Repo.query!("SHOW server_version_num").rows
    [[server_version]] = Repo.query!("SHOW server_version").rows
    [[version]] = Repo.query!("SELECT version()").rows

    [[database, user]] = Repo.query!("SELECT current_database(), current_user").rows

    %{
      server_version_num: String.to_integer(server_version_num),
      server_version: server_version,
      version: version,
      database: database,
      user: user
    }
  end

  def git_metadata do
    {sha, 0} = System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true)
    {status, 0} = System.cmd("git", ["status", "--porcelain"], stderr_to_stdout: true)
    %{sha: String.trim(sha), dirty: String.trim(status) != ""}
  end

  def runtime_metadata do
    %{
      elixir: System.version(),
      otp_release: System.otp_release(),
      ecto_sql: app_version(:ecto_sql),
      postgrex: app_version(:postgrex),
      os: :os.type() |> inspect(),
      schedulers_online: System.schedulers_online()
    }
  end

  defp app_version(app) do
    case Application.spec(app, :vsn) do
      nil -> nil
      version -> to_string(version)
    end
  end

  defp validate_owned_prefix!("docket_bench_" <> suffix = prefix) when byte_size(suffix) > 0 do
    if String.match?(prefix, ~r/\Adocket_bench_[a-z0-9_]+\z/) and byte_size(prefix) <= 63 do
      prefix
    else
      raise ArgumentError, "unsafe benchmark schema name: #{inspect(prefix)}"
    end
  end

  defp validate_owned_prefix!(prefix) do
    raise ArgumentError,
          "refusing to manage non-benchmark schema #{inspect(prefix)}; expected docket_bench_*"
  end
end
