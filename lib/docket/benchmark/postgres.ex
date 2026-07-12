if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Benchmark.Repo do
    @moduledoc false
    use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
  end

  defmodule Docket.Benchmark.NoopNode do
    @moduledoc false
    @behaviour Docket.Node
    @impl true
    def config_schema, do: Docket.Schema.object(%{})
    @impl true
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule Docket.Benchmark.Postgres do
    @moduledoc false
    alias Docket.Benchmark.Repo

    @migration_version 20_260_711_000_038
    @runtime Docket.Benchmark.Runtime
    @pruner [
      interval_ms: 86_400_000,
      event_retention_ms: 86_400_000,
      run_retention_ms: 86_400_000,
      batch_size: 100
    ]

    defmodule Migration do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    def run(config) do
      database = isolated_database(config)
      repo_config = [url: database.url, pool_size: config.pool_size, log: false]
      Application.put_env(:docket, Repo, repo_config)

      primary = primary_config(config.database_url)

      result =
        try do
          create_database!(primary, database.name)
          {:ok, repo} = Repo.start_link()

          try do
            :ok = Ecto.Migrator.up(Repo, @migration_version, Migration, log: false)
            execute(config, database, repo)
          after
            if Process.alive?(repo), do: GenServer.stop(repo, :normal, 5_000)
          end
        rescue
          error -> {:error, Exception.message(error)}
        after
          drop_database(primary, database.name)
          Application.delete_env(:docket, Repo)
        end

      result
    end

    defp execute(config, database, _repo) do
      graph = graph()
      manual_opts = runtime_opts(config, testing: :manual)
      {:ok, manual} = Docket.Runtime.Supervisor.start_link(manual_opts)
      {:ok, ref} = Docket.save_graph(@runtime, graph)
      run_ids = seed_runs(ref, config.runs + config.warmup)
      Supervisor.stop(manual, :normal, 5_000)

      started_at = DateTime.utc_now()
      t0 = System.monotonic_time()
      {:ok, runtime} = Docket.Runtime.Supervisor.start_link(runtime_opts(config))

      try do
        wait_for_terminal(run_ids, config.timeout_ms)
        duration_native = System.monotonic_time() - t0
        finished_at = DateTime.utc_now()
        invariants = invariants(config)
        passed = Enum.all?(invariants, & &1.pass)
        duration_ms = System.convert_time_unit(duration_native, :native, :millisecond)

        artifact = %{
          schema_version: 1,
          classification: "exploratory",
          success: passed,
          scenario: if(config.scenario == "smoke", do: "empty_one_step", else: config.scenario),
          parameters: Map.drop(config, [:database_url, :output]),
          started_at: DateTime.to_iso8601(started_at),
          finished_at: DateTime.to_iso8601(finished_at),
          duration_ms: duration_ms,
          measurements: %{
            completed_runs: config.runs + config.warmup,
            measured_runs: config.runs,
            observed_runs_per_second: rate(config.runs, duration_ms)
          },
          environment: environment(config, database),
          invariants: invariants,
          warnings: ["Observed throughput is environment-specific and is not a capacity maximum."]
        }

        write_artifact!(config.output, artifact)
        {:ok, %{output: Path.expand(config.output), artifact: artifact}}
      after
        Supervisor.stop(runtime, :normal, 5_000)
      end
    end

    defp runtime_opts(config, extra \\ []) do
      [
        name: @runtime,
        backend: Docket.Postgres,
        repo: Repo,
        notifier: :none,
        dispatcher: [concurrency: config.concurrency, poll_interval_ms: config.poll_interval_ms],
        pruner: @pruner
      ] ++ extra
    end

    defp seed_runs(ref, count) do
      Enum.map(1..count, fn _ ->
        {:ok, run} = Docket.start_run(@runtime, ref, %{})
        run.id
      end)
    end

    defp wait_for_terminal(run_ids, timeout_ms) do
      deadline = System.monotonic_time(:millisecond) + timeout_ms
      wait(run_ids, deadline)
    end

    defp wait(run_ids, deadline) do
      {remaining, failures} =
        Enum.reduce(run_ids, {[], []}, fn id, {remaining, failures} ->
          case Docket.fetch_run(@runtime, id) do
            {:ok, %{status: :done}} ->
              {remaining, failures}

            {:ok, %{status: status}} when status in [:failed, :cancelled] ->
              {remaining, [{id, status} | failures]}

            {:ok, _run} ->
              {[id | remaining], failures}

            {:error, reason} ->
              {remaining, [{id, reason} | failures]}
          end
        end)

      cond do
        failures != [] ->
          raise "benchmark runs failed: #{inspect(failures)}"

        remaining == [] ->
          :ok

        System.monotonic_time(:millisecond) >= deadline ->
          raise "benchmark timed out with #{length(remaining)} runs incomplete"

        true ->
          Process.sleep(5)
          wait(remaining, deadline)
      end
    end

    defp invariants(config) do
      queries = [
        {"no duplicate current claim tokens",
         "SELECT count(*) FROM (SELECT claim_token FROM docket_runs WHERE claim_token IS NOT NULL GROUP BY claim_token HAVING count(*) > 1) q",
         0},
        {"no active claims remain",
         "SELECT count(*) FROM docket_runs WHERE claim_token IS NOT NULL", 0},
        {"all seeded runs completed", "SELECT count(*) FROM docket_runs WHERE status = 'done'",
         config.runs + config.warmup},
        {"no stranded running rows", "SELECT count(*) FROM docket_runs WHERE status = 'running'",
         0},
        {"event sequence is unique",
         "SELECT count(*) FROM (SELECT run_id, seq FROM docket_events GROUP BY run_id, seq HAVING count(*) > 1) q",
         0}
      ]

      Enum.map(queries, fn {name, sql, expected} ->
        %{rows: [[actual]]} = Ecto.Adapters.SQL.query!(Repo, sql, [])
        %{name: name, pass: actual == expected, expected: expected, actual: actual}
      end)
    end

    defp graph do
      Docket.Graph.new!(id: "docket-bench-empty-one-step")
      |> Docket.Graph.put_node!("noop", implementation: Docket.Benchmark.NoopNode)
      |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
      |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
    end

    defp environment(config, database) do
      %{
        docket: git_metadata(),
        elixir: System.version(),
        otp_release: List.to_string(:erlang.system_info(:otp_release)),
        erts: List.to_string(:erlang.system_info(:version)),
        os: inspect(:os.type()),
        cpu_count: System.schedulers_online(),
        cpu_model: cpu_model(),
        postgres_version: scalar("SHOW server_version"),
        postgres_settings: settings(),
        repo_pool_size: Repo.config()[:pool_size],
        repo_pool_count: Repo.config()[:pool_count] || 1,
        dispatcher_nodes: config.nodes,
        database: database.name,
        storage_class: "unreported",
        ram_bytes: total_memory()
      }
    end

    defp settings do
      names =
        ~w(synchronous_commit fsync full_page_writes wal_level max_connections shared_buffers)

      Enum.into(names, %{}, fn name -> {name, scalar("SHOW #{name}")} end)
    end

    defp scalar(sql) do
      %{rows: [[value]]} = Ecto.Adapters.SQL.query!(Repo, sql, [])
      value
    end

    defp git_metadata do
      {commit, 0} = System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true)
      {branch, _} = System.cmd("git", ["branch", "--show-current"], stderr_to_stdout: true)

      {status, _} =
        System.cmd("git", ["status", "--porcelain", "--untracked-files=normal"],
          stderr_to_stdout: true
        )

      %{
        commit: String.trim(commit),
        branch: String.trim(branch),
        dirty: String.trim(status) != ""
      }
    end

    defp cpu_model do
      case System.cmd(
             "sh",
             [
               "-c",
               "sysctl -n machdep.cpu.brand_string 2>/dev/null || awk -F: '/model name/{print $2; exit}' /proc/cpuinfo"
             ],
             stderr_to_stdout: true
           ) do
        {value, 0} -> String.trim(value)
        _ -> "unreported"
      end
    end

    defp total_memory do
      case System.cmd(
             "sh",
             [
               "-c",
               "sysctl -n hw.memsize 2>/dev/null || awk '/MemTotal/{print $2 * 1024}' /proc/meminfo"
             ],
             stderr_to_stdout: true
           ) do
        {value, 0} ->
          case Integer.parse(String.trim(value)) do
            {number, _} -> number
            _ -> nil
          end

        _ ->
          nil
      end
    end

    defp rate(_runs, 0), do: nil
    defp rate(runs, duration_ms), do: Float.round(runs * 1_000 / duration_ms, 3)

    defp write_artifact!(path, artifact) do
      path = Path.expand(path)
      File.mkdir_p!(Path.dirname(path))
      temp = path <> ".tmp-#{System.unique_integer([:positive])}"
      File.write!(temp, JSON.encode_to_iodata!(artifact))
      File.rename!(temp, path)
    end

    defp isolated_database(config) do
      uri = URI.parse(config.database_url)

      name =
        "docket_bench_#{System.system_time(:millisecond)}_#{System.unique_integer([:positive])}"

      %{name: name, url: %{uri | path: "/" <> name} |> URI.to_string()}
    end

    defp primary_config(url) do
      uri = URI.parse(url)

      [username, password] =
        case String.split(uri.userinfo || System.get_env("USER") || "postgres", ":", parts: 2) do
          [username, password] -> [URI.decode(username), URI.decode(password)]
          [username] -> [URI.decode(username), nil]
        end

      [
        hostname: uri.host || "localhost",
        port: uri.port || 5432,
        username: username,
        password: password,
        database: "postgres",
        pool_size: 1
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    end

    defp create_database!(config, name) do
      {:ok, pid} = Postgrex.start_link(config)

      try do
        Postgrex.query!(pid, "CREATE DATABASE \"#{name}\"", [])
      after
        GenServer.stop(pid)
      end
    end

    defp drop_database(config, name) do
      case Postgrex.start_link(config) do
        {:ok, pid} ->
          try do
            Postgrex.query(pid, "DROP DATABASE IF EXISTS \"#{name}\" WITH (FORCE)", [])
          after
            GenServer.stop(pid)
          end

        _ ->
          :ok
      end
    end
  end
end
