defmodule Docket.Bench.Scorecard.Scenarios.Concurrency do
  @moduledoc "Throughput scaling across dispatcher concurrency levels on the supervised production runtime."
  @behaviour Docket.Bench.Scorecard.Scenario

  alias Docket.Bench.Scorecard.{Scenario, Stats}
  alias Docket.Bench.Scorecard.Nodes.NoopNode

  @impl true
  def name, do: "concurrency"

  @impl true
  def metric, do: "Concurrency scaling"

  @impl true
  def run(profile, ctx) do
    levels = profile.levels
    per_slot = profile.per_slot

    trials = Enum.map(levels, fn level -> run_level(ctx, level, per_slot) end)

    c_min = Enum.min(levels)
    c_max = Enum.max(levels)
    t_min = throughput_at(trials, c_min)
    t_max = throughput_at(trials, c_max)

    efficiency = t_max / t_min / (c_max / c_min)
    scaled = Stats.clamp(efficiency, 0.0, 1.0)
    score = round(100 * scaled)

    curve =
      Enum.map(trials, fn trial ->
        %{
          level: trial.level,
          runs: trial.runs,
          runs_per_sec: trial.runs_per_sec,
          p95_ms: trial.p95_ms
        }
      end)

    measurements = %{
      levels: levels,
      per_slot: per_slot,
      c_min: c_min,
      c_max: c_max,
      throughput_min: t_min,
      throughput_max: t_max,
      efficiency: efficiency,
      curve: curve,
      per_level_invariants:
        Map.new(trials, fn trial -> {"level_#{trial.level}", trial.invariants} end)
    }

    evidence = "efficiency #{format_ratio(scaled)}, #{round(t_min)}→#{round(t_max)} r/s"

    {:ok,
     %{
       scenario: name(),
       metric: metric(),
       label: "#{c_min}→#{c_max} workers",
       score: score,
       passed: true,
       evidence: evidence,
       measurements: measurements,
       invariants: Enum.flat_map(trials, &level_invariants/1)
     }}
  end

  defp run_level(ctx, level, per_slot) do
    n = level * per_slot
    due_at = DateTime.add(DateTime.utc_now(), -1, :second)

    runs =
      for idx <- 1..n do
        %{idx: idx, due_at: due_at, cohort: "level-#{level}", tenant: nil}
      end

    plan = %{scenario: name(), tenant_mode: :none, graph: graph(), runs: runs}
    trial = Scenario.run_trial(ctx, plan, concurrency: level)

    max_finished = max_datetime(Enum.map(trial.finished, & &1.finished_at))
    elapsed_us = DateTime.diff(max_finished, trial.started_at, :microsecond)
    elapsed_s = max(elapsed_us / 1_000_000, 0.000_001)
    runs_per_sec = trial.expected / elapsed_s

    latencies_ms =
      Enum.map(trial.finished, fn %{run_id: run_id, finished_at: finished_at} ->
        Stats.wait_ms(finished_at, trial.seed[run_id].due_at, trial.started_at)
      end)

    latency = Stats.percentiles(latencies_ms)

    %{
      level: level,
      runs: trial.expected,
      runs_per_sec: runs_per_sec,
      p95_ms: latency.p95,
      invariants: trial.invariants
    }
  end

  defp throughput_at(trials, level) do
    Enum.find(trials, &(&1.level == level)).runs_per_sec
  end

  defp level_invariants(trial) do
    prefixed =
      Enum.map(trial.invariants, fn invariant ->
        %{invariant | name: "level_#{trial.level}/#{invariant.name}"}
      end)

    all_passed = Enum.all?(trial.invariants, & &1.pass)

    prefixed ++
      [
        %{
          name: "level_#{trial.level}/all_passed",
          pass: all_passed,
          expected: true,
          actual: all_passed
        }
      ]
  end

  defp graph do
    Docket.Graph.new!(id: "docket-scorecard-concurrency")
    |> Docket.Graph.put_node!("noop", implementation: NoopNode)
    |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
    |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
  end

  defp max_datetime([]), do: raise("concurrency trial completed no runs")

  defp max_datetime([first | rest]) do
    Enum.reduce(rest, first, fn candidate, acc ->
      if DateTime.compare(candidate, acc) == :gt, do: candidate, else: acc
    end)
  end

  defp format_ratio(value), do: :erlang.float_to_binary(value * 1.0, decimals: 2)
end
