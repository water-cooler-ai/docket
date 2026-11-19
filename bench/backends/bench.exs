# Runs the same workloads against Docket.SQLite and Docket.Postgres through the
# real supervised runtime, and writes one JSON result file per invocation.
#
#     mix run bench/backends/bench.exs -- --backend sqlite
#     DOCKET_BENCH_DATABASE_URL=postgres://postgres:postgres@localhost:55432/docket_bench \
#       mix run bench/backends/bench.exs -- --backend both
#
# See bench/backends/README.md for options and how to compare runs.

defmodule Docket.Bench.Backends.Nodes.Noop do
  @moduledoc false
  @behaviour Docket.Node
  @impl true
  def config_schema, do: Docket.Schema.object(%{})
  @impl true
  def call(_state, _config, _context), do: {:ok, %{}}
end

defmodule Docket.Bench.Backends.Nodes.Sleep do
  @moduledoc false
  @behaviour Docket.Node
  @impl true
  def config_schema, do: Docket.Schema.object(%{"ms" => Docket.Schema.float(required: true)})
  @impl true
  def call(_state, config, _context) do
    Process.sleep(trunc(config["ms"]))
    {:ok, %{}}
  end
end

defmodule Docket.Bench.Backends.Nodes.Gate do
  @moduledoc false
  @behaviour Docket.Node
  @impl true
  def config_schema, do: Docket.Schema.object(%{})
  @impl true
  def call(state, _config, _context) do
    case state["decision"] do
      nil -> {:interrupt, %Docket.Interrupt{resume_channel: "decision"}}
      _decision -> {:ok, %{}}
    end
  end
end

defmodule Docket.Bench.Backends.Observer do
  @moduledoc false
  # Reports terminal and parked checkpoints to the collector. Observers are
  # best effort; the harness verifies final counts against storage.
  @collector :docket_bench_collector

  def observe(%Docket.Checkpoint{type: type, run: run}, _context)
      when type in [:run_completed, :run_failed, :interrupt_requested] do
    case Process.whereis(@collector) do
      nil -> :ok
      pid -> send(pid, {:checkpoint, type, run.id, System.monotonic_time(:microsecond)})
    end

    :ok
  end

  def observe(_checkpoint, _context), do: :ok
end

defmodule Docket.Bench.Backends.PgRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
end

defmodule Docket.Bench.Backends.InstallDocket do
  @moduledoc false
  use Ecto.Migration
  def up, do: Docket.Postgres.Migration.up()
  def down, do: Docket.Postgres.Migration.down()
end

defmodule Docket.Bench.Backends do
  @moduledoc false

  alias Docket.{Graph, Reducer, Schema}
  alias Docket.Bench.Backends.{InstallDocket, Nodes, Observer, PgRepo}

  @runtime Docket.Bench.Backends.Runtime
  @switches [
    backend: :string,
    scenarios: :string,
    n: :integer,
    concurrency: :integer,
    chain: :integer,
    fanout: :integer,
    sleep_ms: :integer,
    synchronous: :string,
    output: :string,
    label: :string,
    timeout_ms: :integer
  ]
  # Drain scenarios seed a backlog with a manual runtime, then time the real
  # runtime draining it. Live scenarios drive the running runtime directly.
  @drain_scenarios ~w(throughput chain fanout io_bound)
  @all_scenarios @drain_scenarios ++ ~w(enqueue interrupt serial_latency)

  def main(argv) do
    {opts, _rest, invalid} =
      OptionParser.parse(Enum.drop_while(argv, &(&1 == "--")), strict: @switches)

    if invalid != [], do: raise(ArgumentError, "unknown options: #{inspect(invalid)}")

    config = %{
      backends:
        case Keyword.get(opts, :backend, "sqlite") do
          "both" -> [:sqlite, :postgres]
          name -> [String.to_existing_atom(name)]
        end,
      scenarios:
        opts |> Keyword.get(:scenarios, Enum.join(@all_scenarios, ",")) |> String.split(","),
      n: Keyword.get(opts, :n, 2_000),
      concurrency: Keyword.get(opts, :concurrency, 10),
      chain: Keyword.get(opts, :chain, 10),
      fanout: Keyword.get(opts, :fanout, 8),
      sleep_ms: Keyword.get(opts, :sleep_ms, 20),
      synchronous: opts |> Keyword.get(:synchronous, "normal") |> String.to_existing_atom(),
      timeout_ms: Keyword.get(opts, :timeout_ms, 300_000),
      label: Keyword.get(opts, :label, git_branch())
    }

    Logger.configure(level: :warning)
    Process.register(self(), :docket_bench_collector)

    results =
      for backend <- config.backends, scenario <- config.scenarios do
        IO.write("#{backend} #{scenario} ... ")
        result = run_scenario(backend, scenario, config)
        IO.puts(summary_line(result))
        result
      end

    report(results)

    output =
      Keyword.get_lazy(opts, :output, fn ->
        stamp = DateTime.utc_now() |> DateTime.to_unix()

        Path.join([
          "tmp",
          "bench",
          "backends",
          "#{config.label |> String.replace("/", "-")}-#{stamp}.json"
        ])
      end)

    File.mkdir_p!(Path.dirname(output))

    File.write!(
      output,
      JSON.encode!(%{
        label: config.label,
        git: git_sha(),
        recorded_at: DateTime.utc_now() |> DateTime.to_iso8601(),
        machine: machine(),
        config: Map.drop(config, [:backends]) |> Map.put(:backends, config.backends),
        results: results
      })
    )

    IO.puts("\nwrote #{output}")
  end

  # ---------------------------------------------------------------------
  # Scenarios
  # ---------------------------------------------------------------------

  defp run_scenario(backend, scenario, config) do
    with_storage(backend, config, fn storage ->
      measured =
        if scenario in @drain_scenarios do
          {graph, n} = drain_workload(scenario, config)
          drain(storage, graph, n, config)
        else
          with_runtime(storage, :production, fn -> measure(scenario, config) end)
        end

      Map.merge(%{backend: backend, scenario: scenario}, measured)
    end)
  rescue
    error -> %{backend: backend, scenario: scenario, error: Exception.message(error)}
  end

  defp drain_workload("throughput", config), do: {chain_graph("bench-throughput", 1), config.n}

  defp drain_workload("chain", config),
    do: {chain_graph("bench-chain", config.chain), div(config.n, 4)}

  defp drain_workload("fanout", config), do: {fanout_graph(config.fanout), div(config.n, 4)}

  defp drain_workload("io_bound", config) do
    graph =
      Graph.new!(id: "bench-io")
      |> Graph.put_node!("work",
        implementation: Nodes.Sleep,
        config: %{ms: config.sleep_ms * 1.0}
      )
      |> Graph.put_edge!("s", from: "$start", to: "work")
      |> Graph.put_edge!("f", from: "work", to: "$finish")

    {graph, div(config.n, 4)}
  end

  # Seeds n runs while no runner is executing, then starts the production
  # runtime and times it until every run has completed.
  defp drain(storage, graph, n, config) do
    ids =
      with_runtime(storage, :manual, fn ->
        {:ok, ref} = Docket.save_graph(@runtime, graph)

        for _ <- 1..n do
          {:ok, run} = Docket.start_run(@runtime, ref, %{})
          run.id
        end
      end)

    flush_checkpoints()

    commits = :counters.new(1, [:write_concurrency])
    handler = "docket-bench-commits-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:docket, :lifecycle, :committed],
      fn _event, _measurements, _metadata, counter -> :counters.add(counter, 1, 1) end,
      commits
    )

    with_runtime(storage, :production, fn ->
      started = System.monotonic_time(:microsecond)
      done = collect(MapSet.new(ids), :run_completed, config.timeout_ms)
      elapsed_s = (Enum.max(Map.values(done)) - started) / 1_000_000
      committed = :counters.get(commits, 1)
      verify_done!(n)

      %{
        runs: n,
        elapsed_s: elapsed_s,
        runs_per_sec: n / elapsed_s,
        commits: committed,
        commits_per_run: committed / n,
        commits_per_sec: committed / elapsed_s,
        completion_ms: percentiles(Enum.map(ids, &((done[&1] - started) / 1_000)))
      }
    end)
    |> tap(fn _ -> :telemetry.detach(handler) end)
    |> then(fn result ->
      if graph.id == "bench-io",
        do: Map.put(result, :ideal_runs_per_sec, config.concurrency * 1_000 / config.sleep_ms),
        else: result
    end)
  end

  defp flush_checkpoints do
    receive do
      {:checkpoint, _type, _id, _at} -> flush_checkpoints()
    after
      0 -> :ok
    end
  end

  # Runs started one at a time from one caller while the runtime executes
  # them: start_run commit rate and start-to-completion latency under load.
  defp measure("enqueue", config) do
    {:ok, ref} = Docket.save_graph(@runtime, chain_graph("bench-enqueue", 1))
    n = config.n
    started = System.monotonic_time(:microsecond)

    started_at =
      Map.new(1..n, fn _ ->
        t = System.monotonic_time(:microsecond)
        {:ok, run} = Docket.start_run(@runtime, ref, %{})
        {run.id, t}
      end)

    enqueued = System.monotonic_time(:microsecond)
    done = collect(MapSet.new(Map.keys(started_at)), :run_completed, config.timeout_ms)
    elapsed_s = (Enum.max(Map.values(done)) - started) / 1_000_000
    verify_done!(n)

    %{
      runs: n,
      runs_per_sec: n / elapsed_s,
      start_run_per_sec: n / ((enqueued - started) / 1_000_000),
      latency_ms: percentiles(Enum.map(started_at, fn {id, t} -> (done[id] - t) / 1_000 end))
    }
  end

  defp measure("interrupt", config) do
    n = div(config.n, 2)
    {:ok, ref} = Docket.save_graph(@runtime, interrupt_graph())

    ids =
      for _ <- 1..n do
        {:ok, run} = Docket.start_run(@runtime, ref, %{})
        run.id
      end

    parked = collect(MapSet.new(ids), :interrupt_requested, config.timeout_ms)

    resolve_started = System.monotonic_time(:microsecond)

    resolved_at =
      Map.new(ids, fn id ->
        {:ok, run} = Docket.fetch_run(@runtime, id)
        [{interrupt_id, _}] = Map.to_list(run.interrupts)
        {:ok, _} = Docket.resolve_interrupt(@runtime, id, interrupt_id, "ok")
        {id, System.monotonic_time(:microsecond)}
      end)

    resolve_elapsed = System.monotonic_time(:microsecond) - resolve_started
    done = collect(MapSet.new(ids), :run_completed, config.timeout_ms)
    finished = Enum.max(Map.values(done))
    verify_done!(n)

    %{
      runs: n,
      parked: map_size(parked),
      resolve_calls_per_sec: n / (resolve_elapsed / 1_000_000),
      runs_per_sec: n / ((finished - resolve_started) / 1_000_000),
      latency_ms: percentiles(Enum.map(ids, &((done[&1] - resolved_at[&1]) / 1_000)))
    }
  end

  # One run at a time: measures the wake path (start_run commit -> claim ->
  # completion) with no queueing behind other runs.
  defp measure("serial_latency", config) do
    {:ok, ref} = Docket.save_graph(@runtime, chain_graph("bench-serial", 1))
    n = min(config.n, 500)

    latencies =
      for _ <- 1..n do
        started = System.monotonic_time(:microsecond)
        {:ok, run} = Docket.start_run(@runtime, ref, %{})
        done = collect(MapSet.new([run.id]), :run_completed, config.timeout_ms)
        (done[run.id] - started) / 1_000
      end

    verify_done!(n)
    %{runs: n, latency_ms: percentiles(latencies)}
  end

  defp measure(other, _config), do: raise(ArgumentError, "unknown scenario #{other}")

  defp collect(pending, type, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_collect(pending, type, deadline, %{})
  end

  defp do_collect(pending, type, deadline, acc) do
    if MapSet.size(pending) == 0 do
      acc
    else
      wait = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:checkpoint, ^type, id, at} ->
          if MapSet.member?(pending, id),
            do: do_collect(MapSet.delete(pending, id), type, deadline, Map.put(acc, id, at)),
            else: do_collect(pending, type, deadline, acc)

        {:checkpoint, :run_failed, id, _at} ->
          raise "run #{id} failed"

        {:checkpoint, _other, _id, _at} ->
          do_collect(pending, type, deadline, acc)
      after
        wait -> raise "timed out with #{MapSet.size(pending)} runs not at #{type}"
      end
    end
  end

  # Observers are best effort, so completion is confirmed against storage.
  defp verify_done!(expected) do
    done = count_done(nil, 0)

    if done < expected,
      do: raise("storage has #{done} done runs, expected #{expected}")
  end

  defp count_done(before, acc) do
    opts = [limit: 1_000, statuses: [:done]] ++ if(before, do: [before: before], else: [])
    {:ok, page} = Docket.list_runs(@runtime, opts)
    acc = acc + length(page.runs)
    if page.has_more?, do: count_done(page.next_before, acc), else: acc
  end

  # ---------------------------------------------------------------------
  # Graphs
  # ---------------------------------------------------------------------

  defp chain_graph(id, steps) do
    names = for i <- 1..steps, do: "n#{i}"

    graph =
      Enum.reduce(names, Graph.new!(id: id), fn name, g ->
        Graph.put_node!(g, name, implementation: Nodes.Noop)
      end)

    [{"$start", hd(names)} | Enum.zip(names, tl(names) ++ ["$finish"])]
    |> Enum.with_index()
    |> Enum.reduce(graph, fn {{from, to}, i}, g ->
      Graph.put_edge!(g, "e#{i}", from: from, to: to)
    end)
  end

  defp fanout_graph(width) do
    workers = for i <- 1..width, do: "w#{i}"

    graph =
      Graph.new!(id: "bench-fanout")
      |> Graph.put_node!("join", implementation: Nodes.Noop)
      |> Graph.put_edge!("join-finish", from: "join", to: "$finish")

    graph =
      Enum.reduce(workers, graph, fn w, g ->
        g
        |> Graph.put_node!(w, implementation: Nodes.Noop)
        |> Graph.put_edge!("start-#{w}", from: "$start", to: w)
      end)

    Graph.put_edge!(graph, "fan-in", from: workers, to: "join")
  end

  defp interrupt_graph do
    Graph.new!(id: "bench-interrupt")
    |> Graph.put_field!("decision", schema: Schema.string(), reducer: Reducer.last_value())
    |> Graph.put_node!("gate", implementation: Nodes.Gate)
    |> Graph.put_edge!("s", from: "$start", to: "gate")
    |> Graph.put_edge!("f", from: "gate", to: "$finish")
  end

  # ---------------------------------------------------------------------
  # Runtimes. Every scenario gets fresh storage and a fresh runtime.
  # ---------------------------------------------------------------------

  defp with_storage(:sqlite, config, fun) do
    dir = Path.join(System.tmp_dir!(), "docket-bench-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    storage = %{
      backend:
        {Docket.SQLite,
         database: Path.join(dir, "docket.db"),
         synchronous: config.synchronous,
         runner: [concurrency: config.concurrency, poll_interval_ms: 1_000]}
    }

    try do
      storage |> fun.() |> Map.put(:db_bytes, dir_bytes(dir))
    after
      File.rm_rf!(dir)
    end
  end

  defp with_storage(:postgres, config, fun) do
    url =
      System.get_env("DOCKET_BENCH_DATABASE_URL") ||
        raise "set DOCKET_BENCH_DATABASE_URL to benchmark Postgres"

    repo_config =
      Ecto.Repo.Supervisor.parse_url(url) ++ [pool_size: config.concurrency + 10, log: false]

    _ = Ecto.Adapters.Postgres.storage_down(repo_config)
    :ok = Ecto.Adapters.Postgres.storage_up(repo_config)
    # The LISTEN/NOTIFY connection reads repo.config(), which comes from the
    # application environment rather than start_link options.
    Application.put_env(:docket, PgRepo, repo_config)
    {:ok, repo} = PgRepo.start_link()
    Ecto.Migrator.up(PgRepo, 1, InstallDocket, log: false)

    storage = %{
      backend:
        {Docket.Postgres,
         repo: PgRepo,
         dispatcher: [concurrency: config.concurrency, poll_interval_ms: 1_000],
         pruner: [
           interval_ms: 86_400_000,
           event_retention_ms: 86_400_000,
           run_retention_ms: 86_400_000,
           batch_size: 100
         ]}
    }

    try do
      fun.(storage)
    after
      Supervisor.stop(repo, :normal, 10_000)
      _ = Ecto.Adapters.Postgres.storage_down(repo_config)
    end
  end

  defp with_runtime(storage, mode, fun) do
    {module, options} = storage.backend

    options =
      if mode == :manual,
        do: Keyword.drop(options, [:runner, :dispatcher, :pruner, :notifier]),
        else: options

    opts =
      [name: @runtime, checkpoint_observers: [Observer], backend: {module, options}] ++
        if(mode == :manual, do: [testing: :manual], else: [])

    {:ok, sup} = Docket.Runtime.Supervisor.start_link(opts)

    try do
      fun.()
    after
      Supervisor.stop(sup, :normal, 10_000)
    end
  end

  # ---------------------------------------------------------------------
  # Reporting
  # ---------------------------------------------------------------------

  defp summary_line(%{error: error}), do: "ERROR #{error}"

  defp summary_line(result) do
    [
      result[:runs_per_sec] && "#{round(result.runs_per_sec)} runs/s",
      result[:commits_per_sec] && "#{round(result.commits_per_sec)} commits/s",
      result[:latency_ms] && "p50 #{result.latency_ms.p50}ms p99 #{result.latency_ms.p99}ms",
      result[:start_run_per_sec] && "#{round(result.start_run_per_sec)} start_run/s"
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(", ")
  end

  defp report(results) do
    IO.puts(
      "\n" <>
        String.pad_trailing("scenario", 16) <>
        Enum.map_join(backends(results), "", &String.pad_leading(to_string(&1), 34))
    )

    for scenario <- results |> Enum.map(& &1.scenario) |> Enum.uniq() do
      cells =
        for backend <- backends(results) do
          result = Enum.find(results, &(&1.backend == backend and &1.scenario == scenario))

          String.pad_leading(
            if(result, do: summary_line(result) |> String.slice(0, 32), else: "-"),
            34
          )
        end

      IO.puts(String.pad_trailing(scenario, 16) <> Enum.join(cells))
    end
  end

  defp backends(results), do: results |> Enum.map(& &1.backend) |> Enum.uniq()

  defp percentiles(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    at = fn p -> sorted |> Enum.at(min(count - 1, trunc(p * count))) |> Float.round(2) end
    %{p50: at.(0.50), p90: at.(0.90), p99: at.(0.99), max: Float.round(List.last(sorted), 2)}
  end

  defp dir_bytes(dir) do
    dir |> File.ls!() |> Enum.map(&File.stat!(Path.join(dir, &1)).size) |> Enum.sum()
  end

  defp machine do
    %{
      otp: System.otp_release(),
      elixir: System.version(),
      schedulers: System.schedulers_online(),
      os: :os.type() |> Tuple.to_list() |> Enum.join("/")
    }
  end

  defp git_branch, do: git(["rev-parse", "--abbrev-ref", "HEAD"])
  defp git_sha, do: git(["rev-parse", "--short", "HEAD"])

  defp git(args) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {out, 0} -> String.trim(out)
      _ -> "unknown"
    end
  end
end

Docket.Bench.Backends.main(System.argv())
