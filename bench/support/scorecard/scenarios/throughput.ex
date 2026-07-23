defmodule Docket.Bench.Scorecard.Scenarios.Throughput do
  @moduledoc "One-step Noop drain throughput measured against the supervised production runtime."
  @behaviour Docket.Bench.Scorecard.Scenario

  alias Docket.Bench.Scorecard.{Scenario, Stats}
  alias Docket.Bench.Scorecard.Nodes.NoopNode

  @impl true
  def name, do: "throughput"

  @impl true
  def metric, do: "Throughput"

  @impl true
  def run(profile, ctx) do
    n = profile.n
    concurrency = profile.concurrency
    target = profile.target_runs_per_sec
    due_at = DateTime.add(DateTime.utc_now(), -1, :second)

    runs =
      for idx <- 1..n do
        %{idx: idx, due_at: due_at, cohort: "default", tenant: nil}
      end

    plan = %{scenario: name(), tenant_mode: :none, graph: graph(), runs: runs}
    trial = Scenario.run_trial(ctx, plan, concurrency: concurrency)

    max_finished = max_datetime(Enum.map(trial.finished, & &1.finished_at))
    elapsed_us = DateTime.diff(max_finished, trial.started_at, :microsecond)
    elapsed_s = max(elapsed_us / 1_000_000, 0.000_001)
    runs_per_sec = trial.expected / elapsed_s

    latencies_ms =
      Enum.map(trial.finished, fn %{run_id: run_id, finished_at: finished_at} ->
        Stats.wait_ms(finished_at, trial.seed[run_id].due_at, trial.started_at)
      end)

    latency = Stats.percentiles(latencies_ms)
    score = round(100 * min(1.0, runs_per_sec / target))

    measurements = %{
      n: n,
      concurrency: concurrency,
      target_runs_per_sec: target,
      runs_per_sec: runs_per_sec,
      elapsed_s: elapsed_s,
      completed: length(trial.finished),
      latency_ms: latency,
      timing_scope: "max(due-time, runtime start) to terminal finished_at (queue plus service)"
    }

    evidence = "#{round(runs_per_sec)} r/s (target #{target}), p95 #{format_ms(latency.p95)}"

    {:ok,
     %{
       scenario: name(),
       metric: metric(),
       label: "#{n} one-step drain @#{concurrency}",
       score: score,
       passed: true,
       evidence: evidence,
       measurements: measurements,
       invariants: trial.invariants
     }}
  end

  defp graph do
    Docket.Graph.new!(id: "docket-scorecard-throughput")
    |> Docket.Graph.put_node!("noop", implementation: NoopNode)
    |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
    |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
  end

  defp max_datetime([]), do: raise("throughput trial completed no runs")

  defp max_datetime([first | rest]) do
    Enum.reduce(rest, first, fn candidate, acc ->
      if DateTime.compare(candidate, acc) == :gt, do: candidate, else: acc
    end)
  end

  defp format_ms(nil), do: "n/a"
  defp format_ms(ms), do: "#{ms}ms"
end
