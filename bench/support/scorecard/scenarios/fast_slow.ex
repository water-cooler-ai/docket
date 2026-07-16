defmodule Docket.Bench.Scorecard.Scenarios.FastSlow do
  @moduledoc "Fast-run slowdown under a slow-run convoy, measured against the supervised production runtime."
  @behaviour Docket.Bench.Scorecard.Scenario

  alias Docket.Bench.Scorecard.{Db, Invariants, Runtime, Scenario, Seed, Stats}
  alias Docket.Bench.Scorecard.Nodes.{NoopNode, SleepNode}

  @impl true
  def name, do: "fast_slow"

  @impl true
  def metric, do: "Fast/slow fairness"

  @impl true
  def run(profile, ctx) do
    concurrency = profile.concurrency
    n_fast = profile.n_fast
    hold_ms = profile.hold_ms
    good = profile.slowdown_good
    bad = profile.slowdown_bad
    n_slow = 5 * concurrency

    runtime_opts = [
      max_attempt_elapsed_ms: hold_ms + 1_500,
      drain_max_elapsed_ms: hold_ms + 2_000,
      orphan_ttl_ms: hold_ms + 4_500
    ]

    previous_hold = Application.get_env(:docket, :scorecard_sleep_node_hold_ms)
    Application.put_env(:docket, :scorecard_sleep_node_hold_ms, hold_ms)

    try do
      control_trial = run_control(ctx, concurrency, n_fast, runtime_opts)
      mixed_trial = run_mixed(ctx, concurrency, n_fast, n_slow, runtime_opts)

      control_dist = cohort_distributions(control_trial)
      mixed_dist = cohort_distributions(mixed_trial)

      control_fast_p95 = fast_p95(control_dist)
      mixed_fast_p95 = fast_p95(mixed_dist)
      slowdown = mixed_fast_p95 / max(control_fast_p95, 1)

      score = round(100 * Stats.clamp((bad - slowdown) / (bad - good), 0.0, 1.0))

      measurements = %{
        concurrency: concurrency,
        n_fast: n_fast,
        n_slow: n_slow,
        hold_ms: hold_ms,
        slowdown: slowdown,
        slowdown_good: good,
        slowdown_bad: bad,
        control_fast_p95_ms: control_fast_p95,
        mixed_fast_p95_ms: mixed_fast_p95,
        control: control_dist,
        mixed: mixed_dist,
        timing_scope: "staged due-time to terminal finished_at per cohort (queue plus service)"
      }

      invariants =
        prefix_invariants("control", control_trial.invariants) ++
          prefix_invariants("mixed", mixed_trial.invariants)

      {:ok,
       %{
         scenario: name(),
         metric: metric(),
         label: "+#{n_slow} slow @#{hold_ms}ms",
         score: score,
         passed: true,
         evidence: "fast p95 slowdown #{Float.round(slowdown, 1)}x",
         measurements: measurements,
         invariants: invariants
       }}
    after
      restore_hold(previous_hold)
    end
  end

  defp run_control(ctx, concurrency, n_fast, runtime_opts) do
    due_at = DateTime.add(DateTime.utc_now(), -1, :second)

    runs =
      for idx <- 1..n_fast do
        %{idx: idx, due_at: due_at, cohort: "fast", tenant: nil}
      end

    plan = %{scenario: name(), tenant_mode: :none, graph: fast_graph(), runs: runs}
    Scenario.run_trial(ctx, plan, concurrency: concurrency, runtime: runtime_opts)
  end

  defp run_mixed(ctx, concurrency, n_fast, n_slow, runtime_opts) do
    due_at = DateTime.add(DateTime.utc_now(), -1, :second)

    slow_runs =
      for idx <- 1..n_slow do
        %{idx: idx, due_at: due_at, cohort: "slow", tenant: nil}
      end

    fast_runs =
      for offset <- 1..n_fast do
        %{idx: n_slow + offset, due_at: due_at, cohort: "fast", tenant: nil}
      end

    slow_plan = %{scenario: name(), tenant_mode: :none, graph: slow_graph(), runs: slow_runs}
    fast_plan = %{scenario: name(), tenant_mode: :none, graph: fast_graph(), runs: fast_runs}

    multi_seed_trial(ctx, [slow_plan, fast_plan],
      concurrency: concurrency,
      runtime: runtime_opts
    )
  end

  defp multi_seed_trial(ctx, plans, opts) do
    Db.truncate(ctx)
    Docket.Postgres.GraphCache.clear()

    seed =
      plans
      |> Enum.map(&Seed.seed(ctx, &1))
      |> Enum.reduce(%{}, fn map, acc -> Map.merge(acc, map) end)

    overrides =
      [concurrency: Keyword.fetch!(opts, :concurrency), tenant_mode: :none] ++
        Keyword.get(opts, :runtime, [])

    timeout_ms = Keyword.get(opts, :drain_timeout_ms, ctx.config.drain_timeout_ms)
    expected = plans |> Enum.map(&length(&1.runs)) |> Enum.sum()
    started_at = DateTime.utc_now()
    runtime = Runtime.start(ctx, overrides)

    try do
      drained!(Runtime.drain_wait(ctx, timeout_ms), ctx, expected)
    after
      Runtime.stop(runtime)
    end

    %{
      seed: seed,
      expected: expected,
      started_at: started_at,
      finished: Db.finished_runs(ctx),
      invariants: Invariants.check(ctx, expected)
    }
  end

  defp drained!(:ok, _ctx, _expected), do: :ok

  defp drained!({:timeout, remaining}, ctx, expected) do
    summary =
      ctx
      |> Invariants.check(expected)
      |> Enum.reject(& &1.pass)
      |> case do
        [] ->
          "no invariant violations detected"

        failing ->
          Enum.map_join(failing, ", ", fn invariant ->
            "#{invariant.name} expected=#{invariant.expected} actual=#{invariant.actual}"
          end)
      end

    raise "scorecard drain timed out with #{remaining} runs not finished; " <> summary
  end

  defp cohort_distributions(trial) do
    trial.finished
    |> Enum.map(fn %{run_id: run_id, finished_at: finished_at} ->
      seed = trial.seed[run_id]
      %{cohort: seed.cohort, wait_ms: Stats.wait_ms(finished_at, seed.due_at, trial.started_at)}
    end)
    |> Enum.group_by(& &1.cohort)
    |> Map.new(fn {cohort, group} ->
      {cohort, Stats.percentiles(Enum.map(group, & &1.wait_ms))}
    end)
  end

  defp fast_p95(distribution) do
    case Map.get(distribution, "fast") do
      %{p95: p95} when is_integer(p95) -> p95
      _ -> 0
    end
  end

  defp prefix_invariants(prefix, invariants) do
    Enum.map(invariants, fn invariant -> %{invariant | name: "#{prefix}.#{invariant.name}"} end)
  end

  defp restore_hold(nil), do: Application.delete_env(:docket, :scorecard_sleep_node_hold_ms)
  defp restore_hold(value), do: Application.put_env(:docket, :scorecard_sleep_node_hold_ms, value)

  defp fast_graph do
    Docket.Graph.new!(id: "docket-scorecard-fast-slow-fast")
    |> Docket.Graph.put_node!("noop", implementation: NoopNode)
    |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
    |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
  end

  defp slow_graph do
    Docket.Graph.new!(id: "docket-scorecard-fast-slow-slow")
    |> Docket.Graph.put_node!("sleep", implementation: SleepNode)
    |> Docket.Graph.put_edge!("start-sleep", from: "$start", to: "sleep")
    |> Docket.Graph.put_edge!("sleep-finish", from: "sleep", to: "$finish")
  end
end
