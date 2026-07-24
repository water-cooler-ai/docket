for file <- ["db.ex", "nodes.ex", "stats.ex", "invariants.ex"] do
  Code.require_file(Path.expand("../../support/scorecard/#{file}", __DIR__))
end

defmodule Docket.Bench.Compare.Postgres.DocketRunner do
  alias Docket.Bench.Scorecard.{Db, Invariants, Nodes, Stats}

  @runtime Docket.Bench.Compare.Postgres.Instance
  @pruner [
    interval_ms: 86_400_000,
    event_retention_ms: 86_400_000,
    run_retention_ms: 86_400_000,
    batch_size: 100
  ]

  def main(argv) do
    {opts, positional} =
      OptionParser.parse!(Enum.drop_while(argv, &(&1 == "--")),
        strict: [
          database_url: :string,
          levels: :string,
          pool_size: :integer,
          scenarios: :string,
          repeats: :integer,
          warmup: :integer,
          single_runs: :integer,
          chain_runs: :integer,
          fanout_runs: :integer
        ]
      )

    if positional != [] do
      raise ArgumentError, "unexpected positional arguments: #{inspect(positional)}"
    end

    database_url =
      Keyword.get(opts, :database_url) ||
        raise ArgumentError, "--database-url is required"

    levels = opts |> Keyword.get(:levels, "1,8,32") |> parse_levels!()
    pool_size = Keyword.get(opts, :pool_size, 2 * Enum.max(levels) + 8)
    selected_scenarios = opts |> Keyword.get(:scenarios) |> select_scenarios!()
    repeats = Keyword.get(opts, :repeats, 3)
    warmup = Keyword.get(opts, :warmup, 5)

    counts = %{
      "single_node" => Keyword.get(opts, :single_runs, 300),
      "chain_10" => Keyword.get(opts, :chain_runs, 300),
      "fanout_8" => Keyword.get(opts, :fanout_runs, 300)
    }

    validate!(levels, pool_size, repeats, warmup, counts)
    notifier_connection = notifier_connection!(database_url)
    prefix = Db.scratch_prefix()
    Db.start_repo!(database_url, pool_size, ssl_options(database_url))
    {telemetry_handler, telemetry_table} = install_telemetry_counter!()

    try do
      postgres = Db.postgres_metadata!()
      Db.require_postgres_13!(postgres.server_version_num)
      Db.create_schema!(prefix)

      IO.puts(
        "META,docket_postgres,#{System.version()},#{System.otp_release()}," <>
          "#{postgres.server_version_num},#{postgres.database},#{prefix}"
      )

      for {scenario, shape} <- selected_scenarios,
          concurrency <- levels do
        run_configuration(
          %{repo: Db.repo(), prefix: prefix},
          scenario,
          shape,
          concurrency,
          counts[scenario],
          repeats,
          warmup,
          notifier_connection,
          telemetry_table
        )
      end
    after
      try do
        Db.drop_schema_if_exists!(prefix)
      after
        :telemetry.detach(telemetry_handler)
        :ets.delete(telemetry_table)
        Db.stop_repo()
      end
    end
  end

  defp scenarios do
    [
      {"single_node", {:chain, 1}},
      {"chain_10", {:chain, 10}},
      {"fanout_8", {:fanout, 8}}
    ]
  end

  defp run_configuration(
         ctx,
         scenario,
         shape,
         concurrency,
         count,
         repeats,
         warmup,
         notifier_connection,
         telemetry_table
       ) do
    Db.reset(ctx)
    graph = graph(scenario, shape)
    runtime = start_runtime(ctx, concurrency, notifier_connection)

    try do
      {:ok, graph_ref} = Docket.save_graph(@runtime, graph)

      if warmup > 0 do
        _warmup =
          run_batch(
            ctx,
            graph_ref,
            "#{scenario}-c#{concurrency}-warm",
            warmup,
            concurrency,
            telemetry_table
          )

        delete_runs(ctx)
      end

      Enum.each(1..repeats, fn repeat ->
        delete_runs(ctx)
        reset_telemetry_counter(telemetry_table)

        result =
          run_batch(
            ctx,
            graph_ref,
            "#{scenario}-c#{concurrency}-r#{repeat}",
            count,
            concurrency,
            telemetry_table
          )

        telemetry = telemetry_snapshot(telemetry_table)
        invariant_pass = Enum.all?(result.invariants, & &1.pass)

        IO.puts(
          Enum.join(
            [
              "RESULT",
              "docket_postgres",
              scenario,
              concurrency,
              repeat,
              count,
              format_number(result.elapsed_ms),
              format_number(result.runs_per_second),
              format_number(result.latency.p50),
              format_number(result.latency.p95),
              format_number(result.latency.p99),
              telemetry.notifications_received,
              telemetry.notification_polls,
              telemetry.scheduled_polls,
              result.run_rows,
              result.event_rows,
              result.logical_bytes,
              invariant_pass
            ],
            ","
          )
        )

        unless invariant_pass do
          raise "Docket invariant failure: #{inspect(result.invariants)}"
        end
      end)
    after
      if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)
    end
  end

  defp run_batch(ctx, graph_ref, id_prefix, count, concurrency, completion_table) do
    started_at = DateTime.utc_now()

    1..count
    |> Task.async_stream(
      fn index ->
        run_id = "#{id_prefix}-#{index}"

        case Docket.start_run(@runtime, graph_ref, %{"token" => 0}, run_id: run_id) do
          {:ok, _run} ->
            case await_local_terminal(completion_table, run_id, 120_000) do
              :ok -> :ok
              other -> raise "local completion wait failed for #{run_id}: #{inspect(other)}"
            end

          other ->
            raise "start_run failed for #{run_id}: #{inspect(other)}"
        end
      end,
      max_concurrency: concurrency,
      ordered: false,
      timeout: 120_000
    )
    |> Enum.each(fn
      {:ok, :ok} -> :ok
      other -> raise "run submission failed: #{inspect(other)}"
    end)

    finished = Db.finished_runs(ctx)

    latencies_ms =
      finished
      |> Enum.map(&(DateTime.diff(&1.finished_at, started_at, :microsecond) / 1_000))

    elapsed_ms = Enum.max(latencies_ms)
    latency = Stats.percentiles(latencies_ms)

    invariants = Invariants.check(ctx, count)
    runs = Db.table(ctx.prefix, "docket_runs")
    events = Db.table(ctx.prefix, "docket_events")

    %{
      elapsed_ms: elapsed_ms,
      runs_per_second: count / max(elapsed_ms / 1_000, 0.000_001),
      latency: latency,
      invariants: invariants,
      run_rows: scalar!("SELECT count(*) FROM #{runs}"),
      event_rows: scalar!("SELECT count(*) FROM #{events}"),
      logical_bytes: logical_bytes!([runs, events])
    }
  end

  defp delete_runs(ctx) do
    runs = Db.table(ctx.prefix, "docket_runs")
    Db.repo().query!("DELETE FROM #{runs}")
    :ok
  end

  defp scalar!(sql) do
    [[value]] = Db.repo().query!(sql).rows
    value
  end

  defp logical_bytes!(tables) do
    Enum.sum(
      Enum.map(tables, fn table ->
        scalar!("SELECT COALESCE(sum(pg_column_size(row_data)), 0) FROM #{table} AS row_data")
      end)
    )
  end

  defp start_runtime(ctx, concurrency, notifier_connection) do
    {:ok, runtime} =
      Docket.Runtime.Supervisor.start_link(
        name: @runtime,
        tenant_mode: :none,
        max_attempt_elapsed_ms: 2_000,
        backend:
          {Docket.Postgres,
           repo: ctx.repo,
           prefix: ctx.prefix,
           notifier: [connection: notifier_connection],
           dispatcher: [
             concurrency: concurrency,
             poll_interval_ms: 1_000,
             orphan_ttl_ms: 60_000,
             max_claim_attempts: 5,
             drain_timeout_ms: 30_000
           ],
           vehicle: [drain_budget: [max_moments: 100, max_elapsed_ms: 3_000]],
           pruner: @pruner}
      )

    runtime
  end

  defp notifier_connection!(database_url) do
    uri = URI.parse(database_url)
    database = uri.path && String.trim_leading(uri.path, "/")

    unless uri.scheme in ["postgres", "postgresql"] and is_binary(uri.host) and
             is_binary(database) and database != "" do
      raise ArgumentError,
            "--database-url must be a postgres URL with a host and database path"
    end

    credentials =
      case uri.userinfo && String.split(uri.userinfo, ":", parts: 2) do
        nil -> []
        [username] -> [username: URI.decode(username)]
        [username, password] -> [username: URI.decode(username), password: URI.decode(password)]
      end

    [
      hostname: uri.host,
      port: uri.port || 5432,
      database: database,
      sync_connect: true
    ] ++ credentials ++ ssl_options(database_url)
  end

  defp ssl_options(database_url) do
    query = database_url |> URI.parse() |> Map.get(:query)
    sslmode = query && URI.decode_query(query)["sslmode"]

    case sslmode do
      "require" ->
        [ssl: [verify: :verify_none]]

      mode when mode in ["verify-ca", "verify-full"] ->
        raise ArgumentError,
              "sslmode=#{mode} requires explicit Postgrex CA options; refusing to downgrade verification"

      _other ->
        []
    end
  end

  defp install_telemetry_counter! do
    table = :ets.new(__MODULE__, [:set, :public])
    handler = {__MODULE__, self(), System.unique_integer([:positive])}

    :ok =
      :telemetry.attach_many(
        handler,
        [
          [:docket, :postgres, :notification],
          [:docket, :postgres, :dispatcher, :poll],
          [:docket, :run, :completed],
          [:docket, :run, :failed]
        ],
        &__MODULE__.handle_benchmark_telemetry/4,
        table
      )

    reset_telemetry_counter(table)
    {handler, table}
  end

  @doc false
  def handle_benchmark_telemetry(
        [:docket, :postgres, :notification],
        measurements,
        %{result: :received},
        table
      ) do
    increment_telemetry(table, :notifications_received, Map.get(measurements, :count, 1))
  end

  def handle_benchmark_telemetry(
        [:docket, :postgres, :dispatcher, :poll],
        _measurements,
        %{source: :notification},
        table
      ) do
    increment_telemetry(table, :notification_polls, 1)
  end

  def handle_benchmark_telemetry(
        [:docket, :postgres, :dispatcher, :poll],
        _measurements,
        %{source: :scheduled},
        table
      ) do
    increment_telemetry(table, :scheduled_polls, 1)
  end

  def handle_benchmark_telemetry(
        [:docket, :run, :completed],
        _measurements,
        %{run_id: run_id},
        table
      ) do
    record_terminal(table, run_id, :done)
  end

  def handle_benchmark_telemetry(
        [:docket, :run, :failed],
        _measurements,
        %{run_id: run_id},
        table
      ) do
    record_terminal(table, run_id, :failed)
  end

  def handle_benchmark_telemetry(_event, _measurements, _metadata, _table), do: :ok

  defp await_local_terminal(table, run_id, timeout_ms) do
    waiter_key = {:waiter, run_id}
    :ets.insert(table, {waiter_key, self()})

    try do
      case terminal_status(table, run_id) do
        nil ->
          receive do
            {:docket_benchmark_terminal, ^run_id, :done} -> :ok
            {:docket_benchmark_terminal, ^run_id, status} -> {:error, status}
          after
            timeout_ms -> {:error, :timeout}
          end

        :done ->
          :ok

        status ->
          {:error, status}
      end
    after
      :ets.delete(table, waiter_key)
    end
  end

  defp record_terminal(table, run_id, status) do
    :ets.insert(table, {{:terminal, run_id}, status})

    case :ets.lookup(table, {:waiter, run_id}) do
      [{{:waiter, ^run_id}, pid}] when is_pid(pid) ->
        send(pid, {:docket_benchmark_terminal, run_id, status})

      [] ->
        :ok
    end

    :ok
  end

  defp terminal_status(table, run_id) do
    case :ets.lookup(table, {:terminal, run_id}) do
      [{{:terminal, ^run_id}, status}] -> status
      [] -> nil
    end
  end

  defp reset_telemetry_counter(table) do
    :ets.insert(table,
      notifications_received: 0,
      notification_polls: 0,
      scheduled_polls: 0
    )

    :ok
  end

  defp increment_telemetry(table, key, count) do
    :ets.update_counter(table, key, {2, count}, {key, 0})
    :ok
  end

  defp telemetry_snapshot(table) do
    %{
      notifications_received: :ets.lookup_element(table, :notifications_received, 2),
      notification_polls: :ets.lookup_element(table, :notification_polls, 2),
      scheduled_polls: :ets.lookup_element(table, :scheduled_polls, 2)
    }
  end

  defp graph(scenario, {:chain, count}) do
    graph =
      Docket.Graph.new!(id: "compare-postgres-#{scenario}")
      |> Docket.Graph.put_input!("token", schema: :integer, required: true)

    node_ids = for index <- 1..count, do: "node_#{index}"

    graph =
      Enum.reduce(node_ids, graph, fn node_id, acc ->
        Docket.Graph.put_node!(acc, node_id, implementation: Nodes.NoopNode)
      end)

    graph =
      Docket.Graph.put_edge!(graph, "start-node-1", from: "$start", to: hd(node_ids))

    graph =
      node_ids
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.with_index(1)
      |> Enum.reduce(graph, fn {[from, to], index}, acc ->
        Docket.Graph.put_edge!(acc, "chain-#{index}", from: from, to: to)
      end)

    Docket.Graph.put_edge!(graph, "last-finish", from: List.last(node_ids), to: "$finish")
  end

  defp graph(scenario, {:fanout, count}) do
    graph =
      Docket.Graph.new!(id: "compare-postgres-#{scenario}")
      |> Docket.Graph.put_input!("token", schema: :integer, required: true)

    Enum.reduce(1..count, graph, fn index, acc ->
      node_id = "node_#{index}"

      acc
      |> Docket.Graph.put_node!(node_id, implementation: Nodes.NoopNode)
      |> Docket.Graph.put_edge!("start-#{index}", from: "$start", to: node_id)
      |> Docket.Graph.put_edge!("finish-#{index}", from: node_id, to: "$finish")
    end)
  end

  defp parse_levels!(value) do
    levels =
      value
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_integer(String.trim(&1)))

    if levels == [] or Enum.any?(levels, &(&1 <= 0)) or Enum.uniq(levels) != levels do
      raise ArgumentError, "--levels must be unique positive integers"
    end

    levels
  rescue
    ArgumentError -> raise ArgumentError, "--levels must be comma-separated positive integers"
  end

  defp select_scenarios!(nil), do: scenarios()

  defp select_scenarios!(value) do
    requested =
      value
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)

    available = Map.new(scenarios())

    if requested == [] or Enum.uniq(requested) != requested or
         Enum.any?(requested, &(not Map.has_key?(available, &1))) do
      raise ArgumentError,
            "--scenarios must be unique names from: #{Enum.join(Map.keys(available), ",")}"
    end

    Enum.map(requested, &{&1, Map.fetch!(available, &1)})
  end

  defp validate!(levels, pool_size, repeats, warmup, counts) do
    unless pool_size >= 2, do: raise(ArgumentError, "--pool-size must be at least 2")
    unless repeats >= 1, do: raise(ArgumentError, "--repeats must be at least 1")
    unless warmup >= 0, do: raise(ArgumentError, "--warmup must be non-negative")

    unless Enum.all?(levels, &is_integer/1),
      do: raise(ArgumentError, "invalid concurrency levels")

    unless Enum.all?(counts, fn {_scenario, count} -> is_integer(count) and count > 0 end) do
      raise ArgumentError, "all scenario run counts must be positive"
    end
  end

  defp format_number(value) when is_integer(value), do: Integer.to_string(value)
  defp format_number(value), do: :erlang.float_to_binary(value * 1.0, decimals: 3)
end

Docket.Bench.Compare.Postgres.DocketRunner.main(System.argv())
