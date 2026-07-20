defmodule Docket.Bench.Scorecard.Scenarios.Surge do
  @moduledoc "Burst-recovery resilience measured against the supervised production runtime with a periodic SQL backlog probe."
  @behaviour Docket.Bench.Scorecard.Scenario

  alias Docket.Bench.Scorecard.{Db, Invariants, Probe, Runtime, Scenario, Seed, Stats}
  alias Docket.Bench.Scorecard.Nodes.NoopNode

  @steady_fraction 0.4
  @burst_multiplier 15
  @burst_min 10
  @calibration_multiplier 20
  @lead_ms 3_000
  @probe_interval_ms 250
  @arrival_bucket_ms 100
  @recovery_backlog_margin 2
  @recovery_sustain_samples 3

  @impl true
  def name, do: "surge"

  @impl true
  def metric, do: "Surge resilience"

  @impl true
  def run(profile, ctx) do
    concurrency = profile.concurrency
    window_ms = profile.window_ms

    calibration = calibrate(ctx, concurrency)
    t_cal = calibration.runs_per_sec

    r = @steady_fraction * t_cal
    arrival_count = max(round(r * window_ms / 1_000), 1)
    burst = max(round(@burst_multiplier * r), @burst_min)
    expected = arrival_count + burst

    seed_main(ctx, expected)

    t0 = DateTime.add(DateTime.utc_now(), @lead_ms, :millisecond)
    burst_at = DateTime.add(t0, div(window_ms, 2), :millisecond)
    stage(ctx, schedule(arrival_count, burst, t0, burst_at, window_ms))

    timeout_ms = @lead_ms + window_ms + ctx.config.drain_timeout_ms
    runtime = Runtime.start(ctx, concurrency: concurrency)
    probe_started_wall = DateTime.utc_now()
    probe = Probe.start(ctx, @probe_interval_ms)

    samples =
      try do
        Runtime.drain_wait(ctx, timeout_ms)
        Process.sleep((@recovery_sustain_samples + 1) * @probe_interval_ms)
        Probe.stop(probe)
      after
        Probe.shutdown(probe)
        Runtime.stop(runtime)
      end

    main_invariants = Invariants.check(ctx, expected)

    window_start_ms = DateTime.diff(t0, probe_started_wall, :millisecond)
    burst_at_ms = DateTime.diff(burst_at, probe_started_wall, :millisecond)
    quarter_ms = window_start_ms + div(window_ms, 4)

    pre_samples =
      Enum.filter(samples, fn sample ->
        sample.t_ms >= quarter_ms and sample.t_ms < burst_at_ms
      end)

    lpre = Stats.percentiles(Enum.map(pre_samples, & &1.ready_backlog)).p50 || 0
    threshold = lpre + @recovery_backlog_margin

    post_samples = Enum.filter(samples, fn sample -> sample.t_ms > burst_at_ms end)
    recovery_sample = find_recovery(post_samples, threshold)

    ideal_recovery_s = burst / t_cal

    {score, recovery_time_s, recovered, evidence} =
      case recovery_sample do
        nil ->
          {0, nil, false, "did not recover in window"}

        %{t_ms: t_ms} ->
          recovery_time_s = (t_ms - burst_at_ms) / 1_000
          value = round(100 * min(1.0, ideal_recovery_s / recovery_time_s))

          {value, recovery_time_s, true,
           "recovered #{format_s(recovery_time_s)}s (ideal #{format_s(ideal_recovery_s)}s)"}
      end

    measurements = %{
      window_ms: window_ms,
      concurrency: concurrency,
      t_cal_runs_per_sec: t_cal,
      arrival_rate_per_sec: r,
      arrival_count: arrival_count,
      burst: burst,
      lead_ms: @lead_ms,
      probe_interval_ms: @probe_interval_ms,
      window_start_ms: window_start_ms,
      burst_at_ms: burst_at_ms,
      pre_backlog_median: lpre,
      recovery_time_s: recovery_time_s,
      ideal_recovery_s: ideal_recovery_s,
      recovered: recovered,
      calibration: Map.drop(calibration, [:invariants]),
      probe_samples: samples
    }

    invariants =
      prefix_invariants("calibration_", calibration.invariants) ++
        prefix_invariants("main_", main_invariants)

    {:ok,
     %{
       scenario: name(),
       metric: metric(),
       label: "#{@burst_multiplier}x burst @#{round(@steady_fraction * 100)}% load",
       score: score,
       passed: true,
       evidence: evidence,
       measurements: measurements,
       invariants: invariants
     }}
  end

  defp calibrate(ctx, concurrency) do
    n = concurrency * @calibration_multiplier
    due_at = DateTime.add(DateTime.utc_now(), -1, :second)

    runs =
      for idx <- 1..n do
        %{idx: idx, due_at: due_at, cohort: "calibration", tenant: nil}
      end

    plan = %{scenario: name(), tenant_mode: :none, graph: graph(), runs: runs}
    trial = Scenario.run_trial(ctx, plan, concurrency: concurrency)

    max_finished = max_datetime(Enum.map(trial.finished, & &1.finished_at))

    elapsed_s =
      max(DateTime.diff(max_finished, trial.started_at, :microsecond) / 1_000_000, 0.000_001)

    %{
      n: n,
      completed: length(trial.finished),
      elapsed_s: elapsed_s,
      runs_per_sec: n / elapsed_s,
      invariants: trial.invariants
    }
  end

  defp seed_main(ctx, expected) do
    placeholder = DateTime.add(DateTime.utc_now(), 3_600, :second)

    runs =
      for idx <- 1..expected do
        %{idx: idx, due_at: placeholder, cohort: "surge", tenant: nil}
      end

    plan = %{scenario: name(), tenant_mode: :none, graph: graph(), runs: runs}
    Db.reset(ctx)
    Docket.Postgres.GraphCache.clear()
    Seed.seed(ctx, plan)
    :ok
  end

  defp schedule(arrival_count, burst, t0, burst_at, window_ms) do
    num_buckets = max(div(window_ms, @arrival_bucket_ms), 1)

    arrivals =
      for i <- 0..(arrival_count - 1) do
        offset_ms = div(i * num_buckets, arrival_count) * @arrival_bucket_ms
        {Seed.run_id(name(), i + 1), DateTime.add(t0, offset_ms, :millisecond)}
      end

    burst_runs =
      for j <- 1..burst do
        {Seed.run_id(name(), arrival_count + j), burst_at}
      end

    arrivals ++ burst_runs
  end

  defp stage(ctx, run_specs) do
    runs = Db.table(ctx.prefix, "docket_runs")

    run_specs
    |> Enum.group_by(fn {_run_id, due_at} -> due_at end, fn {run_id, _due_at} -> run_id end)
    |> Enum.each(fn {due_at, ids} ->
      Db.repo().query!(
        "UPDATE #{runs} SET wake_at = $1 WHERE run_id = ANY($2) AND status = 'running' AND claim_token IS NULL AND poisoned_at IS NULL",
        [due_at, ids]
      )
    end)

    :ok
  end

  defp find_recovery([], _threshold), do: nil

  defp find_recovery([sample | rest] = samples, threshold) do
    window = Enum.take(samples, @recovery_sustain_samples)

    if length(window) == @recovery_sustain_samples and
         Enum.all?(window, &(&1.ready_backlog <= threshold)) do
      sample
    else
      find_recovery(rest, threshold)
    end
  end

  defp prefix_invariants(prefix, invariants) do
    Enum.map(invariants, fn invariant -> %{invariant | name: prefix <> invariant.name} end)
  end

  defp graph do
    Docket.Graph.new!(id: "docket-scorecard-surge")
    |> Docket.Graph.put_node!("noop", implementation: NoopNode)
    |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
    |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
  end

  defp max_datetime([]), do: raise("surge calibration completed no runs")

  defp max_datetime([first | rest]) do
    Enum.reduce(rest, first, fn candidate, acc ->
      if DateTime.compare(candidate, acc) == :gt, do: candidate, else: acc
    end)
  end

  defp format_s(value), do: :erlang.float_to_binary(value * 1.0, decimals: 1)
end
