defmodule Docket.Benchmark do
  @moduledoc """
  Validation and entry point for reproducible Postgres benchmark runs.

  Benchmarks are observational. A result describes one environment and is not
  a universal capacity claim. Long saturation and soak scenarios intentionally
  remain explicit, manual work rather than ordinary test-suite work.
  """

  @scenarios ~w(empty_one_step smoke)
  @all_scenarios ~w(claim_only empty_one_step cyclic_drain blocked_vehicles mixed_fairness graph_cache freshness_split_brain multinode notify_poll amplification soak smoke)

  @defaults %{
    scenario: "smoke",
    runs: 10,
    concurrency: 2,
    pool_size: 5,
    nodes: 1,
    event_policy: "all",
    output: "results/docket-bench.json",
    seed: 1,
    repetitions: 1,
    warmup: 0,
    poll_interval_ms: 10,
    timeout_ms: 30_000,
    database_url: "postgres://localhost:5432/docket_bench"
  }

  @switches [
    scenario: :string,
    runs: :integer,
    concurrency: :integer,
    pool_size: :integer,
    nodes: :integer,
    duration: :string,
    event_policy: :string,
    output: :string,
    seed: :integer,
    repetitions: :integer,
    warmup: :integer,
    poll_interval_ms: :integer,
    timeout_ms: :integer,
    database_url: :string,
    authoritative: :boolean
  ]

  @doc false
  def parse(argv) do
    case OptionParser.parse(argv, strict: @switches) do
      {opts, [], []} -> validate(Map.merge(@defaults, Map.new(opts)))
      {_opts, args, invalid} -> {:error, "unexpected arguments: #{inspect(args ++ invalid)}"}
    end
  end

  @doc false
  def validate(config) do
    with :ok <- member(config.scenario, @all_scenarios, "scenario"),
         :ok <- supported(config.scenario),
         :ok <- positive(config.runs, "runs"),
         :ok <- positive(config.concurrency, "concurrency"),
         :ok <- positive(config.pool_size, "pool-size"),
         :ok <- positive(config.nodes, "nodes"),
         :ok <- positive(config.repetitions, "repetitions"),
         :ok <- non_negative(config.warmup, "warmup"),
         :ok <- current_run_shape(config),
         :ok <- positive(config.poll_interval_ms, "poll-interval-ms"),
         :ok <- positive(config.timeout_ms, "timeout-ms"),
         :ok <- event_policy(config.event_policy),
         :ok <- one_node(config.nodes),
         :ok <- output(config.output),
         :ok <- authority(config) do
      {:ok, config}
    end
  end

  @doc "Runs a validated benchmark configuration."
  def run(config) do
    if Code.ensure_loaded?(Docket.Benchmark.Postgres) do
      apply(Docket.Benchmark.Postgres, :run, [config])
    else
      {:error, "Postgres benchmarks require the optional ecto_sql and postgrex dependencies"}
    end
  end

  defp supported(scenario) when scenario in @scenarios, do: :ok

  defp supported(scenario),
    do:
      {:error,
       "scenario #{inspect(scenario)} is not implemented; refusing to substitute a different workload"}

  defp event_policy("all"), do: :ok

  defp event_policy("none"),
    do: {:error, "event-policy none is not supported by the v0.1 production lifecycle"}

  defp event_policy(value), do: {:error, "event-policy must be all, got: #{inspect(value)}"}
  defp one_node(1), do: :ok

  defp one_node(value),
    do: {:error, "nodes=#{value} requires independent BEAM nodes and is not implemented"}

  defp authority(%{authoritative: true}),
    do:
      {:error,
       "this harness slice is exploratory; authoritative saturation suites require dedicated hardware and at least three repetitions"}

  defp authority(_), do: :ok

  defp current_run_shape(%{warmup: warmup}) when warmup != 0,
    do: {:error, "warmup is not implemented with a separate supervised interval; use 0"}

  defp current_run_shape(%{repetitions: repetitions}) when repetitions != 1,
    do: {:error, "repetitions are not implemented as separate artifacts; use 1"}

  defp current_run_shape(%{duration: duration}),
    do:
      {:error,
       "duration=#{inspect(duration)} is only valid for steady-state scenarios that are not implemented"}

  defp current_run_shape(_), do: :ok
  defp positive(value, _name) when is_integer(value) and value > 0, do: :ok

  defp positive(value, name),
    do: {:error, "#{name} must be a positive integer, got: #{inspect(value)}"}

  defp non_negative(value, _name) when is_integer(value) and value >= 0, do: :ok

  defp non_negative(value, name),
    do: {:error, "#{name} must be a non-negative integer, got: #{inspect(value)}"}

  defp member(value, values, name) do
    if value in values do
      :ok
    else
      {:error, "#{name} must be one of #{Enum.join(values, ", ")}, got: #{inspect(value)}"}
    end
  end

  defp output(value) when is_binary(value) and byte_size(value) > 0, do: :ok
  defp output(value), do: {:error, "output must be a non-empty path, got: #{inspect(value)}"}
end
