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

      repo_config = [
        url: database.url,
        pool_size: config.pool_size,
        log: false,
        telemetry_prefix: [:docket, :benchmark, :repo]
      ]

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

    defp execute(config, _database, _repo) do
      graph = graph()
      manual_opts = runtime_opts(config, testing: :manual)
      {:ok, manual} = Docket.Runtime.Supervisor.start_link(manual_opts)
      {:ok, ref} = Docket.save_graph(@runtime, graph)
      _run_ids = seed_runs(ref, config.runs + config.warmup)
      Supervisor.stop(manual, :normal, 5_000)

      activation_at = DateTime.add(DateTime.utc_now(), 250, :millisecond)
      stage_activation(activation_at)
      Docket.Postgres.GraphCache.clear()
      physical_before = physical_snapshot()
      collector = Docket.Benchmark.Collector.start()
      {:ok, runtime} = Docket.Runtime.Supervisor.start_link(runtime_opts(config))
      sleep_until(activation_at)
      started_at = DateTime.utc_now()
      t0 = System.monotonic_time()

      try do
        wait_for_completion(collector, config.runs, config.timeout_ms)
        duration_native = System.monotonic_time() - t0
        finished_at = DateTime.utc_now()
        Supervisor.stop(runtime, :normal, 5_000)
        events = Docket.Benchmark.Collector.stop(collector)
        physical_after = physical_snapshot()
        invariants = invariants(config)

        measurements =
          measurements(events, t0, duration_native, config, physical_before, physical_after)

        passed =
          Enum.all?(invariants, & &1.pass) and measurements.collection.complete_sample_set

        duration_us = System.convert_time_unit(duration_native, :native, :microsecond)

        artifact = %{
          schema_version: 2,
          classification: "exploratory",
          success: passed,
          scenario: if(config.scenario == "smoke", do: "empty_one_step", else: config.scenario),
          parameters: Map.drop(config, [:database_url, :output]),
          started_at: DateTime.to_iso8601(started_at),
          finished_at: DateTime.to_iso8601(finished_at),
          timing_scope: "common-due-time staged burst through dispatch and terminal commit",
          duration_us: duration_us,
          measurements: measurements,
          environment: environment(config),
          invariants: invariants,
          warnings: warnings(config)
        }

        write_artifact!(config.output, artifact)
        {:ok, %{output: Path.expand(config.output), artifact: artifact}}
      after
        if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)

        if :ets.info(collector.table) != :undefined do
          Docket.Benchmark.Collector.stop(collector)
        end

        Docket.Postgres.GraphCache.clear()
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

    defp wait_for_completion(collector, expected, timeout_ms) do
      deadline = System.monotonic_time(:millisecond) + timeout_ms
      wait(collector, expected, deadline)
    end

    defp wait(collector, expected, deadline) do
      completed = Docket.Benchmark.Collector.count(collector, [:docket, :run, :completed])

      drained =
        Docket.Benchmark.Collector.count(collector, [:docket, :postgres, :vehicle, :drain])

      cond do
        completed >= expected and drained >= expected ->
          :ok

        System.monotonic_time(:millisecond) >= deadline ->
          raise "benchmark timed out with #{completed}/#{expected} completions and #{drained}/#{expected} drains"

        true ->
          Process.sleep(1)
          wait(collector, expected, deadline)
      end
    end

    defp stage_activation(activation_at) do
      Ecto.Adapters.SQL.query!(
        Repo,
        "UPDATE docket_runs SET wake_at = $1 WHERE status = 'running' AND claim_token IS NULL",
        [activation_at]
      )
    end

    defp sleep_until(activation_at) do
      remaining = DateTime.diff(activation_at, DateTime.utc_now(), :millisecond)
      if remaining > 0, do: Process.sleep(remaining)
    end

    defp measurements(events, t0, duration_native, config, physical_before, physical_after) do
      events =
        Enum.filter(events, fn {_event, _measurements, _metadata, observed_at} ->
          observed_at >= t0
        end)

      claim_scans = event_measurements(events, [:docket, :postgres, :run_store, :claim])
      claim_queries = event_measurements(events, [:docket, :postgres, :run_store, :claim_query])
      attempts = event_records(events, [:docket, :postgres, :claim, :attempt])
      ready_attempts = Enum.filter(attempts, fn {_m, meta, _at} -> meta.class == :ready end)
      expired_attempts = Enum.filter(attempts, fn {_m, meta, _at} -> meta.class == :expired end)
      polls = event_measurements(events, [:docket, :postgres, :dispatcher, :poll])
      states = event_measurements(events, [:docket, :postgres, :dispatcher, :state])
      drains = event_measurements(events, [:docket, :postgres, :vehicle, :drain])
      completions = event_records(events, [:docket, :run, :completed])
      committed = event_measurements(events, [:docket, :lifecycle, :committed])
      repo_queries = event_measurements(events, [:docket, :benchmark, :repo, :query])
      store = event_measurements(events, [:docket, :postgres, :store])

      completion_offsets =
        Enum.map(completions, fn {_measurements, _metadata, observed_at} -> observed_at - t0 end)

      ready_lags = Enum.map(ready_attempts, fn {m, _meta, _at} -> m.eligible_age_ms end)
      invalid_ready_lags = Enum.count(ready_lags, &(&1 < 0))

      %{
        completed_runs: length(completions),
        measured_runs: config.runs,
        observed_runs_per_second: rate(config.runs, duration_native),
        latency: %{
          activation_to_terminal_commit_us:
            Docket.Benchmark.Stats.native_distribution(completion_offsets),
          claim_scan_total_us: native_metric_distribution(claim_scans, :duration),
          claim_query_time_us: native_metric_distribution(claim_queries, :query_time),
          claim_queue_time_us: native_metric_distribution(claim_queries, :queue_time),
          claim_decode_time_us: native_metric_distribution(claim_queries, :decode_time),
          selected_ready_due_to_claim_lag_ms:
            Docket.Benchmark.Stats.millisecond_distribution(Enum.reject(ready_lags, &(&1 < 0))),
          expired_claim_age_ms:
            Docket.Benchmark.Stats.millisecond_distribution(
              Enum.map(expired_attempts, fn {m, _meta, _at} -> m.eligible_age_ms end)
            ),
          expired_overdue_after_ttl_ms:
            Docket.Benchmark.Stats.millisecond_distribution(
              Enum.map(expired_attempts, fn {m, _meta, _at} -> m.overdue_after_ttl_ms end)
            ),
          dispatcher_poll_us:
            native_event_distribution(events, [:docket, :postgres, :dispatcher, :poll]),
          dispatcher_launch_us:
            native_event_distribution(events, [:docket, :postgres, :dispatcher, :launch]),
          vehicle_total_us:
            native_event_distribution(events, [:docket, :postgres, :vehicle, :stop]),
          vehicle_claim_held_ms:
            Docket.Benchmark.Stats.millisecond_distribution(Enum.map(drains, & &1.claim_held_ms)),
          vehicle_moment_loop_ms:
            Docket.Benchmark.Stats.millisecond_distribution(Enum.map(drains, & &1.elapsed_ms)),
          lifecycle_transaction_us:
            native_event_distribution(events, [:docket, :lifecycle, :transaction, :stop]),
          node_execution_us: native_event_distribution(events, [:docket, :node, :execution]),
          graph_fetch_us:
            native_event_distribution(events, [:docket, :postgres, :graph, :fetch, :stop]),
          graph_compile_us:
            native_event_distribution(events, [:docket, :postgres, :graph, :compile, :stop]),
          repo_query_time_us: native_metric_distribution(repo_queries, :query_time),
          repo_queue_time_us: native_metric_distribution(repo_queries, :queue_time)
        },
        counts: %{
          claim_scans: length(claim_scans),
          claim_query_samples: length(claim_queries),
          claim_leases: sum(claim_scans, :leases),
          claim_attempts: length(attempts),
          reacquired_claims:
            Enum.count(attempts, fn {_m, meta, _at} -> meta.result == :reacquired end),
          steals: sum(claim_scans, :steals),
          poisoned: sum(claim_scans, :poisoned),
          dispatcher_polls: length(polls),
          empty_polls: Enum.count(polls, &(&1.leases == 0 and &1.poisoned == 0)),
          maximum_in_flight_vehicles: max_value(states, :in_flight),
          committed_moments: sum(committed, :count),
          repo_queries: length(repo_queries)
        },
        amplification:
          amplification(config, committed, repo_queries, store, physical_before, physical_after),
        collection: %{
          percentile_method: "nearest-rank, no interpolation",
          expected_ready_claim_samples: config.runs,
          observed_ready_claim_samples: length(ready_attempts),
          observed_completion_samples: length(completions),
          invalid_negative_ready_lag_samples: invalid_ready_lags,
          complete_sample_set:
            length(ready_attempts) == config.runs and length(completions) == config.runs and
              invalid_ready_lags == 0,
          telemetry_events: Enum.map(Docket.Benchmark.Collector.events(), &Enum.join(&1, "."))
        }
      }
    end

    defp amplification(config, committed, repo_queries, store, before, after_snapshot) do
      event_rows = scalar("SELECT count(*) FROM docket_events")
      run_rows = scalar("SELECT count(*) FROM docket_runs")
      committed_count = sum(committed, :count)

      %{
        durable_run_rows: run_rows,
        durable_event_rows: event_rows,
        events_per_completed_run: ratio(event_rows, config.runs),
        committed_moments_per_run: ratio(committed_count, config.runs),
        repo_queries_per_run: ratio(length(repo_queries), config.runs),
        store_attempted_rows: sum(store, :attempted_rows),
        store_encoded_bytes: sum(store, :encoded_bytes),
        wal_bytes: after_snapshot.wal_bytes_position - before.wal_bytes_position,
        database_size_bytes_delta:
          after_snapshot.database_size_bytes - before.database_size_bytes,
        postgres_database_counters_delta:
          map_delta(after_snapshot.database_counters, before.database_counters),
        caveat:
          "WAL and pg_stat_database deltas can include concurrent server activity and stats lag."
      }
    end

    defp event_records(events, event) do
      for {^event, measurements, metadata, observed_at} <- events,
          do: {measurements, metadata, observed_at}
    end

    defp event_measurements(events, event) do
      Enum.map(event_records(events, event), fn {measurements, _metadata, _observed_at} ->
        measurements
      end)
    end

    defp native_event_distribution(events, event),
      do: native_metric_distribution(event_measurements(events, event), :duration)

    defp native_metric_distribution(measurements, key) do
      measurements
      |> Enum.flat_map(fn measurement ->
        if is_number(measurement[key]), do: [measurement[key]], else: []
      end)
      |> Docket.Benchmark.Stats.native_distribution()
    end

    defp sum(measurements, key),
      do:
        Enum.reduce(measurements, 0, fn measurement, total -> total + (measurement[key] || 0) end)

    defp max_value([], _key), do: 0
    defp max_value(measurements, key), do: measurements |> Enum.map(&(&1[key] || 0)) |> Enum.max()
    defp ratio(_value, 0), do: nil
    defp ratio(value, denominator), do: Float.round(value / denominator, 3)

    defp physical_snapshot do
      %{rows: [[wal_bytes_position, database_size_bytes]]} =
        Ecto.Adapters.SQL.query!(
          Repo,
          "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, pg_database_size(current_database())",
          []
        )

      columns =
        ~w(xact_commit xact_rollback blks_read blks_hit tup_returned tup_fetched tup_inserted tup_updated tup_deleted)a

      %{rows: [values]} =
        Ecto.Adapters.SQL.query!(
          Repo,
          "SELECT xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted FROM pg_stat_database WHERE datname = current_database()",
          []
        )

      %{
        wal_bytes_position: wal_bytes_position,
        database_size_bytes: database_size_bytes,
        database_counters: Enum.zip(columns, values) |> Map.new()
      }
    end

    defp map_delta(after_map, before_map),
      do: Map.new(after_map, fn {key, value} -> {key, value - before_map[key]} end)

    defp warnings(config) do
      [
        "Observed throughput is environment-specific and is not a capacity maximum.",
        "The staged burst is one repetition without warmup or a steady-state saturation sweep.",
        "p95/p99 values from #{config.runs} smoke samples are descriptive only.",
        "Claim query timing is client-observed Ecto timing, not server-exclusive execution time."
      ]
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

    defp environment(config) do
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

      {status, _} =
        System.cmd("git", ["status", "--porcelain", "--untracked-files=normal"],
          stderr_to_stdout: true
        )

      %{
        commit: String.trim(commit),
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

    defp rate(runs, duration_native) do
      duration_us = System.convert_time_unit(duration_native, :native, :microsecond)
      if duration_us == 0, do: nil, else: Float.round(runs * 1_000_000 / duration_us, 3)
    end

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
