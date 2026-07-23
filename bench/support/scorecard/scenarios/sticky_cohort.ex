defmodule Docket.Bench.Scorecard.Scenarios.StickyCohort do
  @moduledoc "Work-in-progress boundedness and time-to-first-completion for multi-slice runs in a single scope."
  @behaviour Docket.Bench.Scorecard.Scenario

  alias Docket.Bench.Scorecard.{Db, Invariants, Runtime, Seed, Stats}
  alias Docket.Bench.Scorecard.Nodes.NoopNode

  @sample_interval_ms 25

  @impl true
  def name, do: "sticky_cohort"

  @impl true
  def metric, do: "Cohort stickiness"

  @impl true
  def run(profile, ctx) do
    n = profile.n
    chain = profile.chain
    drain_moments = profile.drain_moments
    concurrency = profile.concurrency

    due_at = DateTime.add(DateTime.utc_now(), -1, :second)

    runs =
      for idx <- 1..n do
        %{idx: idx, due_at: due_at, cohort: "cohort", tenant: nil}
      end

    plan = %{scenario: name(), tenant_mode: :none, graph: graph(chain), runs: runs}

    trial =
      sampled_trial(ctx, plan,
        concurrency: concurrency,
        runtime: [drain_max_moments: drain_moments]
      )

    finished_times =
      trial.finished
      |> Enum.map(& &1.finished_at)
      |> Enum.sort({:asc, DateTime})

    drain_ms = max(DateTime.diff(List.last(finished_times), trial.started_at, :millisecond), 1)
    ttfc_ms = DateTime.diff(List.first(finished_times), trial.started_at, :millisecond)
    ttfc_ratio = ttfc_ms / drain_ms
    peak_wip = Enum.max(trial.samples, fn -> 0 end)

    score = round(100 * Stats.clamp((n - peak_wip) / max(n - concurrency, 1), 0.0, 1.0))

    measurements = %{
      n: n,
      chain: chain,
      drain_moments: drain_moments,
      slices_per_run: ceil(chain / drain_moments),
      concurrency: concurrency,
      peak_wip: peak_wip,
      ttfc_ms: ttfc_ms,
      drain_ms: drain_ms,
      ttfc_ratio: ttfc_ratio,
      wip_samples: trial.samples,
      timing_scope:
        "runtime start to terminal finished_at; WIP sampled every #{@sample_interval_ms}ms"
    }

    {:ok,
     %{
       scenario: name(),
       metric: metric(),
       label: "#{n}x#{chain}-step @#{concurrency}",
       score: score,
       passed: true,
       evidence:
         "peak WIP #{peak_wip}/#{n}, first completion at #{round(ttfc_ratio * 100)}% of drain",
       measurements: measurements,
       invariants: trial.invariants
     }}
  end

  defp sampled_trial(ctx, plan, opts) do
    Db.reset(ctx)
    Docket.Postgres.GraphCache.clear()
    seed = Seed.seed(ctx, plan)

    overrides =
      [concurrency: Keyword.fetch!(opts, :concurrency), tenant_mode: plan.tenant_mode] ++
        Keyword.get(opts, :runtime, [])

    timeout_ms = Keyword.get(opts, :drain_timeout_ms, ctx.config.drain_timeout_ms)
    checkpoint_floor = max_checkpoint_seq(ctx)
    started_at = DateTime.utc_now()
    runtime = Runtime.start(ctx, overrides)
    sampler = Task.async(fn -> sample_wip(ctx, checkpoint_floor, []) end)

    try do
      Runtime.drain_wait(ctx, timeout_ms)
    after
      send(sampler.pid, :stop)
      Runtime.stop(runtime)
    end

    %{
      seed: seed,
      expected: length(plan.runs),
      started_at: started_at,
      finished: Db.finished_runs(ctx),
      samples: Task.await(sampler, 5_000),
      invariants: Invariants.check(ctx, length(plan.runs))
    }
  end

  defp sample_wip(ctx, checkpoint_floor, acc) do
    receive do
      :stop -> Enum.reverse(acc)
    after
      @sample_interval_ms ->
        sample_wip(ctx, checkpoint_floor, [wip_count(ctx, checkpoint_floor) | acc])
    end
  end

  defp wip_count(ctx, checkpoint_floor) do
    runs = Db.table(ctx.prefix, "docket_runs")

    [[count]] =
      Db.repo().query!(
        "SELECT count(*) FROM #{runs} " <>
          "WHERE checkpoint_seq > #{checkpoint_floor} AND finished_at IS NULL"
      ).rows

    count
  end

  defp max_checkpoint_seq(ctx) do
    runs = Db.table(ctx.prefix, "docket_runs")
    [[floor]] = Db.repo().query!("SELECT COALESCE(MAX(checkpoint_seq), 0) FROM #{runs}").rows
    floor
  end

  defp graph(chain) do
    base = Docket.Graph.new!(id: "docket-scorecard-sticky-cohort")

    with_nodes =
      Enum.reduce(1..chain, base, fn i, acc ->
        Docket.Graph.put_node!(acc, "step-#{i}", implementation: NoopNode)
      end)

    with_start =
      Docket.Graph.put_edge!(with_nodes, "start-step-1", from: "$start", to: "step-1")

    with_chain =
      Enum.reduce(1..(chain - 1), with_start, fn i, acc ->
        Docket.Graph.put_edge!(acc, "step-#{i}-#{i + 1}",
          from: "step-#{i}",
          to: "step-#{i + 1}"
        )
      end)

    Docket.Graph.put_edge!(with_chain, "step-#{chain}-finish",
      from: "step-#{chain}",
      to: "$finish"
    )
  end
end
