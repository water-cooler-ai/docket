defmodule Docket.Benchmark.Console do
  @moduledoc false

  @missing "—"
  @comparative_scenarios ~w(cyclic_vs_one_step mixed_service_times parked_wait_vs_blocking_wait)

  def lines([]), do: ["No benchmark trials were produced."]
  def lines([point]), do: point_lines(point)

  def lines(points) when is_list(points) do
    points =
      Enum.sort_by(points, fn point ->
        {
          point.point.concurrency,
          point.point.pool_size,
          point.point.repetition,
          Map.get(point.point, :observer_position, 0)
        }
      end)

    scenario = hd(points).scenario
    valid = Enum.count(points, & &1.success)
    observer? = observer_abba?(points)

    reported_points =
      if observer?,
        do: Enum.filter(points, &(observer_mode(&1) == "bounded_instrumented")),
        else: points

    cells = Enum.group_by(reported_points, &{&1.point.concurrency, &1.point.pool_size})

    summary_note =
      if observer? do
        "Ordinary medians use bounded-instrumented A trials only; distribution columns use p95 only when every reported trial has n>=20, otherwise max."
      else
        "Medians across repetitions; distribution columns use p95 only when every trial has n>=20, otherwise max."
      end

    trial_label =
      if observer?, do: "observer trials", else: plural(length(points), "trial", "trials")

    [
      "#{status(valid == length(points))} exploratory · #{scenario} · #{map_size(cells)} #{plural(map_size(cells), "cell", "cells")} · #{valid}/#{length(points)} #{trial_label} valid",
      summary_note
    ] ++
      (cells
       |> Enum.sort_by(fn {{concurrency, pool_size}, _points} -> {concurrency, pool_size} end)
       |> Enum.flat_map(fn {{concurrency, pool_size}, cell_points} ->
         cell_lines(scenario, concurrency, pool_size, cell_points)
       end)) ++ observer_lines(points, scenario, observer?) ++ cohort_note(scenario)
  end

  defp point_lines(point) do
    scenario = point.scenario
    runs = get_in(point, [:parameters, :runs])
    concurrency = get_in(point, [:point, :concurrency])
    pool_size = get_in(point, [:point, :pool_size])

    details = if is_number(point.duration_us), do: scenario_lines(scenario, point), else: []

    [
      "#{status(point.success)} exploratory · #{scenario} · #{format_count(runs)} #{work_items(scenario, runs)} · concurrency #{format_count(concurrency)} · pool #{format_count(pool_size)}",
      point_rate_line(point)
    ] ++ details ++ check_lines(point)
  end

  defp point_rate_line(point) do
    label =
      case point.scenario do
        "claim_only" -> "Claim drain"
        "steady_arrival" -> "Arrival + drain"
        _other -> "Burst"
      end

    throughput = get_in(point, [:measurements, :throughput_per_second])

    "#{label} #{format_duration(point.duration_us)} · #{format_rate(throughput, point.scenario)}"
  end

  defp scenario_lines("empty_one_step", point) do
    latency = get_in(point, [:measurements, :latency]) || %{}

    [
      "",
      "Cohort offsets from common activation (queue-inclusive)",
      distribution_line(
        "first durable commit",
        latency[:burst_activation_to_first_commit_offset_us]
      ),
      distribution_line(
        "terminal durable commit",
        latency[:burst_activation_to_terminal_commit_offset_us]
      ),
      "After first durable commit",
      distribution_line("first commit -> terminal", latency[:first_commit_to_terminal_us]),
      "Postgres / lease",
      distribution_line("claim scan", latency[:claim_scan_total_us]),
      distribution_line("ready age at scan", latency[:selected_ready_age_at_scan_start_ms]),
      distribution_line("claim held", latency[:vehicle_claim_held_ms])
    ]
  end

  defp scenario_lines("claim_only", point) do
    latency = get_in(point, [:measurements, :latency]) || %{}
    counts = get_in(point, [:measurements, :counts]) || %{}
    batches = get_in(point, [:measurements, :batches]) || %{}

    [
      "",
      "Claim offsets from burst start (backlog-inclusive)",
      distribution_line("all claims", latency[:burst_start_to_claim_offset_us]),
      distribution_line("ready claims", latency[:ready_burst_start_to_claim_offset_us]),
      distribution_line("expired claims", latency[:expired_burst_start_to_claim_offset_us]),
      "Postgres claim path",
      distribution_line("claim scan", latency[:claim_scan_total_us]),
      distribution_line("claim query", latency[:claim_query_time_us]),
      distribution_line("pool queue", latency[:claim_queue_time_us]),
      "Claims ready #{format_count(counts[:ready_claims])} · expired #{format_count(counts[:expired_claims])} · mean rows/nonempty scan #{format_decimal_or_missing(batches[:mean_rows_per_nonempty_scan], 1)}"
    ]
  end

  defp scenario_lines("blocked_vehicles", point) do
    latency = get_in(point, [:measurements, :latency]) || %{}
    blocked = get_in(point, [:measurements, :blocked_vehicles]) || %{}
    blocked_latency = blocked[:latency] || %{}
    freshness = blocked[:claim_freshness] || %{}
    timeline = blocked[:timeline] || %{}
    diagnostics = timeline[:observer_diagnostics] || %{}
    ttl = get_in(point, [:parameters, :orphan_ttl_ms])

    [
      "",
      "Cohort offsets from common activation (queue-inclusive)",
      distribution_line(
        "first durable commit",
        latency[:burst_activation_to_first_commit_offset_us]
      ),
      distribution_line(
        "terminal durable commit",
        latency[:burst_activation_to_terminal_commit_offset_us]
      ),
      "Blocked plateau / release",
      "  fill #{format_duration(blocked[:plateau_fill_duration_us])} · stable hold #{format_duration(blocked[:stable_hold_duration_us])}",
      distribution_line(
        "release -> terminal",
        blocked_latency[:gate_release_to_terminal_commit_us]
      ),
      distribution_line(
        "unrelated short query",
        blocked_latency[:unrelated_short_query_round_trip_us]
      ),
      "Freshness / sampling",
      "  claim age at release #{format_milliseconds(freshness[:maximum_claim_age_ms_at_release])} / TTL #{format_milliseconds(ttl)} · derived wake age max #{format_milliseconds(timeline_max(timeline, :derived_oldest_unclaimed_wake_at_age_ms))}",
      "  sampler missed #{format_count(timeline[:missed_ticks])} ticks · duty #{format_percent(diagnostics[:serial_sampler_duty_cycle_percent])}"
    ]
  end

  defp scenario_lines("steady_arrival", point) do
    steady = get_in(point, [:measurements, :steady_arrival]) || %{}

    [
      "",
      "Open-loop arrival window",
      "  offered #{format_rate(steady[:offered_rate_per_second], "steady_arrival")} · achieved in window #{format_rate(steady[:achieved_arrival_window_rate_per_second], "steady_arrival")} · exact terminal-drain #{format_rate(steady[:achieved_terminal_drain_rate_per_second], "steady_arrival")}",
      distribution_line("completion lag", steady[:retained_completion_lag_us]),
      "Backlog at window end",
      "  state #{safe_label(steady[:backlog_state_at_window_end] || @missing)} · outstanding #{format_count(steady[:due_outstanding_at_arrival_window_end])} · finite-window net accumulation #{format_decimal_or_missing(steady[:backlog_growth_runs_per_second], 1)} runs/s",
      "  oldest-due growth #{format_decimal_or_missing(steady[:oldest_due_lag_growth_ms_per_second], 1)} ms/s · terminal drain after window #{format_duration(steady[:terminal_drain_after_arrival_window_us])}",
      "  terminal drain duration #{format_duration(steady[:terminal_drain_duration_us])} · polling detection delay #{format_duration(steady[:completion_poll_detection_delay_us])}"
    ]
  end

  defp scenario_lines(scenario, point) when scenario in @comparative_scenarios do
    cohorts = sorted_cohorts(point)

    if cohorts == [] do
      []
    else
      terminal_lines =
        Enum.map(cohorts, fn {label, cohort} ->
          distribution_line(
            "#{label} terminal",
            cohort[:activation_to_terminal_commit_offset_us]
          )
        end)

      first_claim_lines =
        Enum.map(cohorts, fn {label, cohort} ->
          distribution_line(
            "#{label} first claim",
            cohort[:activation_to_first_claim_offset_us]
          )
        end)

      service_lines =
        Enum.map(cohorts, fn {label, cohort} ->
          distribution_line(
            "#{label} claim -> terminal",
            cohort[:first_claim_to_terminal_commit_us]
          )
        end)

      fairness_lines =
        Enum.map(cohorts, fn {label, cohort} ->
          normalized = get_in(cohort, [:normalized_slowdown, :p50])
          terminal_rank = get_in(cohort, [:terminal_rank_in_retained_sample, :p50])
          subsequent = get_in(cohort, [:claims, :retained_subsequent_observations])

          "  #{String.pad_trailing("#{label} fairness", 25)} normalized slowdown p50 #{format_decimal_or_missing(normalized, 2)}x · retained terminal rank p50 #{format_count(terminal_rank)} · retained subsequent claims #{format_count(subsequent)}"
        end)

      queue_shares =
        Enum.map_join(cohorts, " · ", fn {label, cohort} ->
          "#{label} #{format_percent(cohort[:queue_share_of_median_percent])}"
        end)

      ["", "Cohort offsets from common activation (queue-inclusive)"] ++
        terminal_lines ++
        ["Wait before first claim"] ++
        first_claim_lines ++
        ["Per-run service after first claim"] ++
        service_lines ++
        ["Normalized wait and completion order"] ++
        fairness_lines ++
        ["  #{String.pad_trailing("queue share of p50", 25)} #{queue_shares}"]
    end
  end

  defp scenario_lines(_scenario, _point), do: []

  defp sorted_cohorts(point) do
    point
    |> get_in([:measurements, :cohorts])
    |> Kernel.||(%{})
    |> Enum.sort_by(fn {label, _cohort} -> to_string(label) end)
  end

  defp check_lines(point) do
    invariants = point[:invariants] || []
    passed = Enum.count(invariants, & &1.pass)
    cleanup = get_in(point, [:cleanup, :isolated_database_removed])

    base = ["", "Checks #{passed}/#{length(invariants)} · cleanup #{cleanup_status(cleanup)}"]

    failed =
      invariants
      |> Enum.reject(& &1.pass)
      |> Enum.take(3)
      |> Enum.map(fn invariant -> "  FAIL #{safe_label(invariant.name)}" end)

    failure_stage =
      case point[:failure_stage] do
        stage when is_binary(stage) -> ["  failure stage #{safe_label(stage)}"]
        _other -> []
      end

    base ++ failed ++ failure_stage
  end

  defp cell_lines(scenario, concurrency, pool_size, points) do
    valid = Enum.filter(points, & &1.success)
    throughput = repetition(valid, [:measurements, :throughput_per_second])
    duration = repetition(valid, [:duration_us])

    headline =
      "  c=#{concurrency} pool=#{pool_size} · #{length(valid)}/#{length(points)} valid · #{format_rate(throughput.median, scenario)} median · spread #{format_percent(throughput.spread_percent)} · duration #{format_duration(duration.median)}"

    [headline, cell_metric_line(scenario, valid)]
  end

  defp cell_metric_line("empty_one_step", points) do
    first =
      cell_distribution(points, [
        :measurements,
        :latency,
        :burst_activation_to_first_commit_offset_us
      ])

    terminal =
      cell_distribution(points, [
        :measurements,
        :latency,
        :burst_activation_to_terminal_commit_offset_us
      ])

    terminalization =
      cell_distribution(points, [:measurements, :latency, :first_commit_to_terminal_us])

    "    cohort first #{first.statistic}* #{format_duration(first.median)} · terminal #{terminal.statistic}* #{format_duration(terminal.median)} · first commit -> terminal #{terminalization.statistic} #{format_duration(terminalization.median)}"
  end

  defp cell_metric_line("claim_only", points) do
    claim =
      cell_distribution(points, [:measurements, :latency, :burst_start_to_claim_offset_us])

    scan = cell_distribution(points, [:measurements, :latency, :claim_scan_total_us])
    queue = cell_distribution(points, [:measurements, :latency, :claim_queue_time_us])

    "    claim offset #{claim.statistic}* #{format_duration(claim.median)} · scan #{scan.statistic} #{format_duration(scan.median)} · queue #{queue.statistic} #{format_duration(queue.median)}"
  end

  defp cell_metric_line("blocked_vehicles", points) do
    fill =
      median_at(points, [:measurements, :blocked_vehicles, :plateau_fill_duration_us])

    release =
      cell_distribution(points, [
        :measurements,
        :blocked_vehicles,
        :latency,
        :gate_release_to_terminal_commit_us
      ])

    query =
      cell_distribution(points, [
        :measurements,
        :blocked_vehicles,
        :latency,
        :unrelated_short_query_round_trip_us
      ])

    "    plateau fill #{format_duration(fill)} · release -> terminal #{release.statistic} #{format_duration(release.median)} · short query #{query.statistic} #{format_duration(query.median)}"
  end

  defp cell_metric_line("steady_arrival", points) do
    achieved =
      median_at(points, [
        :measurements,
        :steady_arrival,
        :achieved_arrival_window_rate_per_second
      ])

    lag =
      cell_distribution(points, [
        :measurements,
        :steady_arrival,
        :retained_completion_lag_us
      ])

    growth =
      median_at(points, [:measurements, :steady_arrival, :backlog_growth_runs_per_second])

    "    window achieved #{format_rate(achieved, "steady_arrival")} · completion lag #{lag.statistic} #{format_duration(lag.median)} · backlog growth #{format_decimal_or_missing(growth, 1)} runs/s"
  end

  defp cell_metric_line(scenario, points) when scenario in @comparative_scenarios do
    labels =
      points
      |> Enum.flat_map(fn point ->
        point |> get_in([:measurements, :cohorts]) |> Kernel.||(%{}) |> Map.keys()
      end)
      |> Enum.uniq()
      |> Enum.sort_by(&to_string/1)

    parts =
      Enum.map(labels, fn label ->
        terminal =
          cell_distribution(points, [
            :measurements,
            :cohorts,
            label,
            :activation_to_terminal_commit_offset_us
          ])

        "#{label} terminal #{terminal.statistic}* #{format_duration(terminal.median)}"
      end)

    if parts == [], do: "", else: "    " <> Enum.join(parts, " · ")
  end

  defp cell_metric_line(_scenario, _points), do: ""

  defp cohort_note(scenario)
       when scenario in ["empty_one_step", "claim_only"] or scenario in @comparative_scenarios,
       do: ["* Cohort offsets include backlog waiting."]

  defp cohort_note(_scenario), do: []

  defp observer_abba?(points) do
    Enum.any?(points, fn point ->
      get_in(point, [:observer_control, :enabled]) == true
    end)
  end

  defp observer_mode(point) do
    get_in(point, [:observer_control, :mode]) || get_in(point, [:point, :observer_mode])
  end

  defp observer_lines(_points, _scenario, false), do: []

  defp observer_lines(points, scenario, true) do
    cells =
      points
      |> Enum.group_by(&{&1.point.concurrency, &1.point.pool_size})
      |> Enum.sort_by(fn {{concurrency, pool_size}, _trials} -> {concurrency, pool_size} end)
      |> Enum.map(fn {{concurrency, pool_size}, trials} ->
        pairs =
          trials
          |> Enum.group_by(fn trial ->
            {trial.point.repetition, Map.get(trial.point, :observer_pair)}
          end)
          |> Enum.map(fn {_pair, pair_trials} -> observer_pair_delta(pair_trials) end)

        valid = Enum.filter(pairs, & &1.valid)

        throughput = observer_median(valid, :throughput)
        throughput_percent = observer_median(valid, :throughput_percent)
        duration = observer_median(valid, :duration)
        duration_percent = observer_median(valid, :duration_percent)

        "  c=#{concurrency} pool=#{pool_size} · #{length(valid)}/#{length(pairs)} pairs valid · throughput delta median #{format_rate(throughput, scenario)} (#{format_percent(throughput_percent)}) · duration delta median #{format_duration(duration)} (#{format_percent(duration_percent)})"
      end)

    [
      "",
      "Observer ABBA raw deltas (bounded instrumented - counters-only control)",
      "Diagnostic sensitivity only; these deltas are not a causal latency correction."
    ] ++ cells
  end

  defp observer_pair_delta(trials) do
    instrumented = Enum.find(trials, &(observer_mode(&1) == "bounded_instrumented"))
    control = Enum.find(trials, &(observer_mode(&1) == "counters_only_control"))

    valid =
      instrumented != nil and control != nil and instrumented.success and control.success and
        is_number(instrumented.duration_us) and is_number(control.duration_us) and
        is_number(get_in(instrumented, [:measurements, :throughput_per_second])) and
        is_number(get_in(control, [:measurements, :throughput_per_second]))

    if valid do
      instrumented_throughput = get_in(instrumented, [:measurements, :throughput_per_second])
      control_throughput = get_in(control, [:measurements, :throughput_per_second])

      %{
        valid: true,
        throughput: instrumented_throughput - control_throughput,
        throughput_percent: percent_delta(instrumented_throughput, control_throughput),
        duration: instrumented.duration_us - control.duration_us,
        duration_percent: percent_delta(instrumented.duration_us, control.duration_us)
      }
    else
      %{valid: false}
    end
  end

  defp percent_delta(_observed, 0), do: nil
  defp percent_delta(observed, baseline), do: (observed - baseline) * 100 / baseline

  defp observer_median(values, key) do
    values
    |> Enum.flat_map(fn value -> if is_number(value[key]), do: [value[key]], else: [] end)
    |> Enum.sort()
    |> median()
  end

  defp repetition(points, path) do
    values = numeric_values(points, path)

    case values do
      [] ->
        %{median: nil, spread_percent: nil}

      values ->
        sorted = Enum.sort(values)
        median = median(sorted)
        spread = List.last(sorted) - hd(sorted)

        %{
          median: median,
          spread_percent: if(median == 0, do: nil, else: spread * 100 / median)
        }
    end
  end

  defp median_at(points, path), do: points |> numeric_values(path) |> Enum.sort() |> median()

  defp cell_distribution(points, path) do
    distributions =
      Enum.flat_map(points, fn point ->
        case get_in(point, path) do
          %{sample_count: count} = distribution when is_integer(count) and count > 0 ->
            [distribution]

          _other ->
            []
        end
      end)

    statistic =
      if distributions != [] and length(distributions) == length(points) and
           Enum.all?(distributions, &(&1.sample_count >= 20)),
         do: :p95,
         else: :max

    values =
      Enum.flat_map(distributions, fn distribution ->
        case distribution[statistic] do
          value when is_number(value) -> [value]
          _other -> []
        end
      end)

    %{statistic: Atom.to_string(statistic), median: values |> Enum.sort() |> median()}
  end

  defp numeric_values(points, path) do
    Enum.flat_map(points, fn point ->
      case get_in(point, path) do
        value when is_number(value) -> [value]
        _other -> []
      end
    end)
  end

  defp median([]), do: nil

  defp median(sorted) do
    count = length(sorted)
    middle = div(count, 2)

    if rem(count, 2) == 1,
      do: Enum.at(sorted, middle),
      else: (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
  end

  defp distribution_line(label, %{sample_count: count} = distribution)
       when is_integer(count) and count > 0 do
    values =
      if count < 20 do
        "p50 #{format_distribution_value(distribution[:p50], distribution[:unit])} · max #{format_distribution_value(distribution[:max], distribution[:unit])} · n=#{count}"
      else
        "p50 #{format_distribution_value(distribution[:p50], distribution[:unit])} · p95 #{format_distribution_value(distribution[:p95], distribution[:unit])}"
      end

    "  #{String.pad_trailing(label, 25)} #{values}"
  end

  defp distribution_line(label, _distribution),
    do: "  #{String.pad_trailing(label, 25)} #{@missing}"

  defp timeline_max(timeline, metric), do: get_in(timeline, [:summary, metric, :max])

  defp format_distribution_value(value, "ms") when is_number(value),
    do: format_duration(value * 1_000)

  defp format_distribution_value(value, _unit), do: format_duration(value)

  defp format_duration(value) when is_number(value) do
    magnitude = abs(value)

    cond do
      magnitude < 1_000 -> "#{format_decimal(value, 0)} us"
      magnitude < 1_000_000 -> "#{format_decimal(value / 1_000, 1)} ms"
      true -> "#{format_decimal(value / 1_000_000, 2)} s"
    end
  end

  defp format_duration(_value), do: @missing

  defp format_milliseconds(value) when is_number(value), do: format_duration(value * 1_000)
  defp format_milliseconds(_value), do: @missing

  defp format_rate(value, scenario) when is_number(value) do
    unit = if scenario == "claim_only", do: "claims/s", else: "runs/s"
    "#{format_decimal(value, 1)} #{unit}"
  end

  defp format_rate(_value, _scenario), do: @missing

  defp format_percent(value) when is_number(value), do: "#{format_decimal(value, 1)}%"
  defp format_percent(_value), do: @missing

  defp format_decimal_or_missing(value, places) when is_number(value),
    do: format_decimal(value, places)

  defp format_decimal_or_missing(_value, _places), do: @missing

  defp format_decimal(value, places) do
    value
    |> Kernel.*(1.0)
    |> :erlang.float_to_binary(decimals: places)
    |> group_decimal()
  end

  defp group_decimal(value) do
    case String.split(value, ".", parts: 2) do
      [whole, fractional] -> "#{group_whole(whole)}.#{fractional}"
      [whole] -> group_whole(whole)
    end
  end

  defp group_whole("-" <> digits), do: "-" <> group_digits(digits)
  defp group_whole(digits), do: group_digits(digits)

  defp group_digits(digits) do
    digits
    |> String.reverse()
    |> String.graphemes()
    |> Enum.chunk_every(3)
    |> Enum.map_join(",", &Enum.join/1)
    |> String.reverse()
  end

  defp format_count(value) when is_integer(value),
    do: value |> Integer.to_string() |> group_whole()

  defp format_count(value) when is_number(value), do: format_decimal(value, 0)
  defp format_count(_value), do: @missing

  defp cleanup_status(true), do: "passed"
  defp cleanup_status(false), do: "failed"
  defp cleanup_status(_value), do: "unknown"

  defp status(true), do: "PASS"
  defp status(false), do: "FAIL"

  defp work_items("claim_only", 1), do: "claim"
  defp work_items("claim_only", _count), do: "claims"
  defp work_items(_scenario, 1), do: "run"
  defp work_items(_scenario, _count), do: "runs"

  defp plural(1, singular, _plural), do: singular
  defp plural(_count, _singular, plural), do: plural

  defp safe_label(value) do
    value
    |> to_string()
    |> String.replace(~r/[\x00-\x1F\x7F]/u, " ")
    |> String.slice(0, 160)
  end
end
