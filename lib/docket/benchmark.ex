defmodule Docket.Benchmark do
  @moduledoc """
  Validation and entry point for reproducible Postgres benchmark runs.

  Benchmarks are observational. A result describes one environment and is not
  a universal capacity claim. Long saturation and soak scenarios intentionally
  remain explicit, manual work rather than ordinary test-suite work.
  """

  @scenarios ~w(blocked_vehicles claim_only cyclic_vs_one_step empty_one_step mixed_service_times parked_wait_vs_blocking_wait smoke steady_arrival)
  @all_scenarios ~w(claim_only empty_one_step cyclic_drain cyclic_vs_one_step blocked_vehicles mixed_fairness mixed_service_times parked_wait_vs_blocking_wait graph_cache freshness_split_brain multinode notify_poll amplification soak smoke steady_arrival)
  @maximum_steady_arrival_runs 1_000_000
  @observer_abba_sequence [
    {"bounded_instrumented", 1, 1},
    {"counters_only_control", 2, 1},
    {"counters_only_control", 3, 2},
    {"bounded_instrumented", 4, 2}
  ]

  @defaults %{
    scenario: "smoke",
    runs: 10,
    concurrency: 2,
    pool_size: 5,
    nodes: 1,
    event_policy: "all",
    output: "results/docket-bench.json",
    format: "json",
    concurrency_matrix: nil,
    pool_size_matrix: nil,
    batch_size: nil,
    ready_ratio: nil,
    hold_ms: nil,
    slow_percent: nil,
    cycle_moments: nil,
    drain_max_moments: nil,
    drain_max_elapsed_ms: nil,
    sample_interval_ms: nil,
    max_samples: nil,
    probe_count: nil,
    duration: nil,
    arrival_rate: nil,
    orphan_ttl_ms: 60_000,
    seed: 1,
    repetitions: 1,
    warmup: 0,
    poll_interval_ms: 10,
    timeout_ms: 30_000,
    database_url: "postgres://localhost:5432/docket_bench",
    observer_abba: false
  }

  @switches [
    scenario: :string,
    runs: :integer,
    concurrency: :integer,
    pool_size: :integer,
    concurrency_matrix: :string,
    pool_size_matrix: :string,
    batch_size: :integer,
    ready_ratio: :string,
    hold_ms: :integer,
    slow_percent: :integer,
    cycle_moments: :integer,
    drain_max_moments: :integer,
    drain_max_elapsed_ms: :integer,
    sample_interval_ms: :integer,
    max_samples: :integer,
    probe_count: :integer,
    orphan_ttl_ms: :integer,
    nodes: :integer,
    duration: :string,
    arrival_rate: :string,
    event_policy: :string,
    output: :string,
    format: :string,
    seed: :integer,
    repetitions: :integer,
    warmup: :integer,
    poll_interval_ms: :integer,
    timeout_ms: :integer,
    database_url: :string,
    authoritative: :boolean,
    observer_abba: :boolean
  ]

  @doc false
  def parse(argv) do
    case OptionParser.parse(argv, strict: @switches) do
      {opts, [], []} ->
        with :ok <- reject_matrix_conflicts(opts),
             {:ok, config} <- @defaults |> Map.merge(Map.new(opts)) |> normalize_matrix(),
             {:ok, config} <- normalize_scenario(config) do
          validate(config)
        end

      {_opts, args, invalid} ->
        {:error, "unexpected arguments: #{inspect(args ++ invalid)}"}
    end
  end

  @doc false
  def validate(config) do
    with :ok <- member(config.scenario, @all_scenarios, "scenario"),
         :ok <- supported(config.scenario),
         :ok <- observer_abba_scenario(config),
         :ok <- positive(config.runs, "runs"),
         :ok <- positive(config.concurrency, "concurrency"),
         :ok <- positive(config.pool_size, "pool-size"),
         :ok <- positive_list(config.concurrencies, "concurrency-matrix"),
         :ok <- positive_list(config.pool_sizes, "pool-size-matrix"),
         :ok <- positive(config.nodes, "nodes"),
         :ok <- non_negative(config.seed, "seed"),
         :ok <- positive(config.repetitions, "repetitions"),
         :ok <- non_negative(config.warmup, "warmup"),
         :ok <- cyclic_controls_scope(config),
         :ok <- mixed_service_controls_scope(config),
         :ok <- scenario_options(config),
         :ok <- current_run_shape(config),
         :ok <- positive(config.poll_interval_ms, "poll-interval-ms"),
         :ok <- positive(config.orphan_ttl_ms, "orphan-ttl-ms"),
         :ok <- positive(config.timeout_ms, "timeout-ms"),
         :ok <- event_policy(config.event_policy),
         :ok <- one_node(config.nodes),
         :ok <- output(config.output),
         :ok <- member(config.format, ~w(json ndjson), "format"),
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

  @doc false
  def run_for_cli(config, opts \\ []) do
    if Code.ensure_loaded?(Docket.Benchmark.Postgres) do
      apply(Docket.Benchmark.Postgres, :run_for_cli, [config, opts])
    else
      {:error, "Postgres benchmarks require the optional ecto_sql and postgrex dependencies"}
    end
  end

  @doc false
  def plan(config) do
    cells =
      for concurrency <- config.concurrencies,
          pool_size <- config.pool_sizes,
          do: {concurrency, pool_size}

    base =
      for repetition <- 1..config.repetitions,
          {concurrency, pool_size} <- rotate(cells, config.seed + repetition - 1) do
        config
        |> Map.put(:concurrency, concurrency)
        |> Map.put(:pool_size, pool_size)
        |> Map.put(:repetition, repetition)
      end

    if config.observer_abba do
      Enum.flat_map(base, &observer_abba_points/1)
    else
      base
    end
  end

  defp supported(scenario) when scenario in @scenarios, do: :ok

  defp supported(scenario),
    do:
      {:error,
       "scenario #{inspect(scenario)} is not implemented; refusing to substitute a different workload"}

  defp observer_abba_scenario(%{observer_abba: true, scenario: scenario})
       when scenario in ["smoke", "empty_one_step"],
       do: :ok

  defp observer_abba_scenario(%{observer_abba: true, scenario: scenario}),
    do:
      {:error,
       "observer-abba is implemented only for smoke/empty_one_step, got: #{inspect(scenario)}"}

  defp observer_abba_scenario(_config), do: :ok

  defp cyclic_controls_scope(%{scenario: "cyclic_vs_one_step"}), do: :ok

  defp cyclic_controls_scope(%{
         cycle_moments: nil,
         drain_max_moments: nil,
         drain_max_elapsed_ms: nil
       }),
       do: :ok

  defp cyclic_controls_scope(_config),
    do: {:error, "cycle/drain controls are only valid for cyclic_vs_one_step"}

  defp mixed_service_controls_scope(%{scenario: "mixed_service_times"}), do: :ok
  defp mixed_service_controls_scope(%{slow_percent: nil}), do: :ok

  defp mixed_service_controls_scope(_config),
    do: {:error, "slow-percent is only valid for mixed_service_times"}

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

  defp current_run_shape(%{scenario: "steady_arrival"}), do: :ok

  defp current_run_shape(%{duration: duration}) when not is_nil(duration),
    do:
      {:error,
       "duration=#{inspect(duration)} is only valid for steady-state scenarios that are not implemented"}

  defp current_run_shape(_), do: :ok

  defp scenario_options(%{scenario: "claim_only"} = config) do
    with :ok <- positive(config.batch_size, "batch-size") do
      if config.warmup == 0,
        do: :ok,
        else: {:error, "claim_only does not support warmup runs; use repetitions"}
    end
  end

  defp scenario_options(%{scenario: "blocked_vehicles"} = config) do
    with :ok <- positive(config.hold_ms, "hold-ms"),
         :ok <- at_least(config.sample_interval_ms, 5, "sample-interval-ms"),
         :ok <- positive(config.max_samples, "max-samples"),
         :ok <- positive(config.probe_count, "probe-count"),
         :ok <- blocked_backlog(config),
         :ok <- blocked_ttl(config) do
      if config.warmup == 0,
        do: :ok,
        else: {:error, "blocked_vehicles does not support warmup runs; use repetitions"}
    end
  end

  defp scenario_options(%{scenario: "steady_arrival"} = config) do
    with :ok <- at_least(config.duration_ms, 10, "duration"),
         :ok <- at_least(config.sample_interval_ms, 5, "sample-interval-ms"),
         :ok <- positive(config.max_samples, "max-samples"),
         :ok <- steady_arrival_options(config) do
      if config.warmup == 0,
        do: :ok,
        else: {:error, "steady_arrival does not support warmup runs; use repetitions"}
    end
  end

  defp scenario_options(%{scenario: "mixed_service_times"} = config) do
    with :ok <- positive(config.hold_ms, "hold-ms"),
         :ok <- at_least(config.runs, 2, "runs"),
         :ok <- percentage(config.slow_percent, "slow-percent"),
         :ok <- blocking_hold_below_ttl(config) do
      if config.warmup == 0,
        do: :ok,
        else: {:error, "mixed_service_times does not support warmup runs; use repetitions"}
    end
  end

  defp scenario_options(%{scenario: "parked_wait_vs_blocking_wait"} = config) do
    with :ok <- positive(config.hold_ms, "hold-ms"),
         :ok <- at_least(config.runs, 2, "runs"),
         :ok <- blocking_hold_below_ttl(config) do
      if config.warmup == 0,
        do: :ok,
        else:
          {:error, "parked_wait_vs_blocking_wait does not support warmup runs; use repetitions"}
    end
  end

  defp scenario_options(%{scenario: "cyclic_vs_one_step"} = config) do
    with :ok <- at_least(config.runs, 2, "runs"),
         :ok <- positive(config.cycle_moments, "cycle-moments"),
         :ok <- positive(config.drain_max_moments, "drain-max-moments"),
         :ok <- optional_positive(config.drain_max_elapsed_ms, "drain-max-elapsed-ms"),
         :ok <- cycle_exceeds_drain_budget(config) do
      if config.warmup == 0,
        do: :ok,
        else: {:error, "cyclic_vs_one_step does not support warmup runs; use repetitions"}
    end
  end

  defp scenario_options(%{
         batch_size: nil,
         ready_ratio: nil,
         hold_ms: nil,
         slow_percent: nil,
         cycle_moments: nil,
         drain_max_moments: nil,
         drain_max_elapsed_ms: nil,
         arrival_rate: nil,
         sample_interval_ms: nil,
         max_samples: nil,
         probe_count: nil
       }),
       do: :ok

  defp scenario_options(_config),
    do:
      {:error,
       "batch-size/ready-ratio are only valid for claim_only; hold-ms is valid for blocked_vehicles and wait-comparison scenarios; slow-percent is only valid for mixed_service_times; cycle/drain controls are only valid for cyclic_vs_one_step; duration/arrival-rate are only valid for steady_arrival; sample/max options are valid for blocked_vehicles and steady_arrival; probe-count is only valid for blocked_vehicles"}

  defp steady_arrival_options(config) do
    unsupported =
      [
        batch_size: config.batch_size,
        ready_ratio: config.ready_ratio,
        hold_ms: config.hold_ms,
        slow_percent: config.slow_percent,
        cycle_moments: config.cycle_moments,
        drain_max_moments: config.drain_max_moments,
        drain_max_elapsed_ms: config.drain_max_elapsed_ms,
        probe_count: config.probe_count
      ]
      |> Enum.filter(fn {_name, value} -> not is_nil(value) end)

    if unsupported == [],
      do: :ok,
      else: {:error, "unsupported steady_arrival options: #{inspect(Keyword.keys(unsupported))}"}
  end

  defp cycle_exceeds_drain_budget(config) do
    if config.cycle_moments > config.drain_max_moments do
      :ok
    else
      {:error,
       "cycle-moments must be greater than drain-max-moments so the benchmark exercises a bounded drain yield"}
    end
  end

  defp blocked_backlog(config) do
    maximum_concurrency = Enum.max(config.concurrencies)

    if config.runs >= maximum_concurrency do
      :ok
    else
      {:error,
       "blocked_vehicles requires runs >= maximum configured concurrency (#{maximum_concurrency})"}
    end
  end

  defp blocked_ttl(config) do
    if config.hold_ms <= div(config.orphan_ttl_ms, 2) do
      :ok
    else
      {:error, "blocked_vehicles hold-ms must be at most half of orphan-ttl-ms"}
    end
  end

  defp blocking_hold_below_ttl(config) do
    if config.hold_ms < config.orphan_ttl_ms - 1 do
      :ok
    else
      {:error, "blocking hold-ms must leave deadline headroom below orphan-ttl-ms"}
    end
  end

  defp positive(value, _name) when is_integer(value) and value > 0, do: :ok

  defp positive(value, name),
    do: {:error, "#{name} must be a positive integer, got: #{inspect(value)}"}

  defp optional_positive(nil, _name), do: :ok
  defp optional_positive(value, name), do: positive(value, name)

  defp percentage(value, _name) when is_integer(value) and value in 1..99, do: :ok

  defp percentage(value, name),
    do: {:error, "#{name} must be an integer from 1 through 99, got: #{inspect(value)}"}

  defp at_least(value, minimum, _name) when is_integer(value) and value >= minimum, do: :ok

  defp at_least(value, minimum, name),
    do: {:error, "#{name} must be an integer >= #{minimum}, got: #{inspect(value)}"}

  defp non_negative(value, _name) when is_integer(value) and value >= 0, do: :ok

  defp non_negative(value, name),
    do: {:error, "#{name} must be a non-negative integer, got: #{inspect(value)}"}

  defp positive_list(values, name) when is_list(values) do
    if values != [] and Enum.all?(values, &(is_integer(&1) and &1 > 0)) do
      :ok
    else
      {:error, "#{name} must contain positive integers"}
    end
  end

  defp member(value, values, name) do
    if value in values do
      :ok
    else
      {:error, "#{name} must be one of #{Enum.join(values, ", ")}, got: #{inspect(value)}"}
    end
  end

  defp output(value) when is_binary(value) and byte_size(value) > 0, do: :ok
  defp output(value), do: {:error, "output must be a non-empty path, got: #{inspect(value)}"}

  defp normalize_matrix(config) do
    with {:ok, concurrencies} <- matrix_values(config.concurrency_matrix, config.concurrency),
         {:ok, pool_sizes} <- matrix_values(config.pool_size_matrix, config.pool_size) do
      {:ok, Map.merge(config, %{concurrencies: concurrencies, pool_sizes: pool_sizes})}
    end
  end

  defp matrix_values(nil, fallback), do: {:ok, [fallback]}

  defp matrix_values(value, _fallback) when is_binary(value) do
    values =
      value
      |> String.split(",", trim: false)
      |> Enum.map(&String.trim/1)

    parsed = Enum.map(values, &Integer.parse/1)

    cond do
      parsed == [] or Enum.any?(values, &(&1 == "")) ->
        {:error, "matrix values must not be empty"}

      Enum.all?(parsed, fn
        {number, ""} when number > 0 -> true
        _ -> false
      end) ->
        numbers = Enum.map(parsed, &elem(&1, 0))

        if Enum.uniq(numbers) == numbers do
          {:ok, numbers}
        else
          {:error, "matrix values must not contain duplicates, got: #{inspect(value)}"}
        end

      true ->
        {:error,
         "matrix values must be comma-separated positive integers, got: #{inspect(value)}"}
    end
  end

  defp reject_matrix_conflicts(opts) do
    conflicts =
      [
        {:concurrency, :concurrency_matrix},
        {:pool_size, :pool_size_matrix}
      ]
      |> Enum.filter(fn {scalar, matrix} ->
        Keyword.has_key?(opts, scalar) and Keyword.has_key?(opts, matrix)
      end)

    case conflicts do
      [] ->
        :ok

      [{scalar, matrix} | _] ->
        {:error, "--#{dash(scalar)} and --#{dash(matrix)} are mutually exclusive"}
    end
  end

  defp dash(value), do: value |> Atom.to_string() |> String.replace("_", "-")

  defp normalize_scenario(%{scenario: "claim_only"} = config) do
    with {:ok, ratio} <- parse_ratio(config.ready_ratio || "1:1") do
      {:ok, %{config | batch_size: config.batch_size || 50, ready_ratio: ratio}}
    end
  end

  defp normalize_scenario(%{scenario: "blocked_vehicles"} = config) do
    {:ok,
     %{
       config
       | hold_ms: config.hold_ms || 250,
         sample_interval_ms: config.sample_interval_ms || 20,
         max_samples: config.max_samples || 256,
         probe_count: config.probe_count || 3
     }}
  end

  defp normalize_scenario(%{scenario: "steady_arrival"} = config) do
    with {:ok, duration_ms} <- parse_duration(config.duration),
         {:ok, arrival_rate} <- parse_arrival_rate(config.arrival_rate),
         {:ok, runs} <- steady_arrival_runs(config.runs, duration_ms, arrival_rate) do
      {:ok,
       %{
         config
         | runs: runs,
           arrival_rate: arrival_rate,
           sample_interval_ms: config.sample_interval_ms || 20,
           max_samples: config.max_samples || 256
       }
       |> Map.put(:duration_ms, duration_ms)}
    end
  end

  defp normalize_scenario(%{scenario: "mixed_service_times"} = config) do
    {:ok, %{config | hold_ms: config.hold_ms || 250, slow_percent: config.slow_percent || 10}}
  end

  defp normalize_scenario(%{scenario: "parked_wait_vs_blocking_wait"} = config) do
    {:ok, %{config | hold_ms: config.hold_ms || 250}}
  end

  defp normalize_scenario(%{scenario: "cyclic_vs_one_step"} = config) do
    {:ok,
     %{
       config
       | cycle_moments: config.cycle_moments || 12,
         drain_max_moments: config.drain_max_moments || 4,
         drain_max_elapsed_ms: config.drain_max_elapsed_ms || 3_000
     }}
  end

  defp normalize_scenario(config), do: {:ok, config}

  defp parse_ratio(value) when is_binary(value) do
    case String.split(value, ":", parts: 2) do
      [ready, expired] ->
        with {ready, ""} <- Integer.parse(ready),
             {expired, ""} <- Integer.parse(expired),
             true <- ready >= 0 and expired >= 0 and ready + expired > 0 do
          {:ok, %{ready_weight: ready, expired_weight: expired}}
        else
          _ ->
            {:error,
             "ready-ratio must be READY:EXPIRED non-negative weights, got: #{inspect(value)}"}
        end

      _ ->
        {:error, "ready-ratio must be READY:EXPIRED non-negative weights, got: #{inspect(value)}"}
    end
  end

  defp parse_duration(nil),
    do: {:error, "steady_arrival requires --duration with an ms, s, or m suffix"}

  defp parse_duration(value) when is_binary(value) do
    case Regex.run(~r/\A([1-9][0-9]*)(ms|s|m)\z/, value) do
      [_, count, "ms"] -> {:ok, String.to_integer(count)}
      [_, count, "s"] -> {:ok, String.to_integer(count) * 1_000}
      [_, count, "m"] -> {:ok, String.to_integer(count) * 60_000}
      _ -> {:error, "duration must be a positive integer with an ms, s, or m suffix"}
    end
  end

  defp parse_arrival_rate(nil), do: {:ok, nil}

  defp parse_arrival_rate(value) when is_binary(value) do
    case Float.parse(value) do
      {rate, ""} when rate > 0 -> {:ok, rate}
      _ -> {:error, "arrival-rate must be a positive number of runs per second"}
    end
  end

  defp steady_arrival_runs(runs, _duration_ms, nil), do: {:ok, runs}

  defp steady_arrival_runs(_runs, duration_ms, arrival_rate) do
    derived = ceil(arrival_rate * duration_ms / 1_000)

    cond do
      derived < 1 ->
        {:error, "arrival-rate and duration must schedule at least one run"}

      derived > @maximum_steady_arrival_runs ->
        {:error,
         "arrival-rate and duration exceed the #{@maximum_steady_arrival_runs} run safety limit"}

      true ->
        {:ok, derived}
    end
  end

  defp rotate([], _seed), do: []

  defp rotate(values, seed) do
    offset = Integer.mod(seed, length(values))
    {left, right} = Enum.split(values, offset)
    right ++ left
  end

  defp observer_abba_points(config) do
    Enum.map(@observer_abba_sequence, fn {mode, position, pair} ->
      config
      |> Map.put(:observer_mode, mode)
      |> Map.put(:observer_position, position)
      |> Map.put(:observer_pair, pair)
    end)
  end
end
