defmodule Docket.Bench.Scorecard.Scenarios.TenantFairness do
  @moduledoc "Hot-tenant convoy fairness measured against the supervised production runtime with tenant_mode required."
  @behaviour Docket.Bench.Scorecard.Scenario

  alias Docket.Bench.Scorecard.{Scenario, Stats}
  alias Docket.Bench.Scorecard.Nodes.NoopNode

  @impl true
  def name, do: "tenant_fairness"

  @impl true
  def metric, do: "Tenant fairness"

  @impl true
  def run(profile, ctx) do
    k = profile.tenants
    n = profile.n
    concurrency = profile.concurrency
    hot_fraction = profile.hot_fraction
    hot_n = round(n * hot_fraction)
    due_at = DateTime.add(DateTime.utc_now(), -1, :second)

    runs = build_runs(k, n, hot_n, due_at)
    plan = %{scenario: name(), tenant_mode: :required, graph: graph(), runs: runs}
    trial = Scenario.run_trial(ctx, plan, concurrency: concurrency)

    waits =
      Enum.map(trial.finished, fn %{run_id: run_id, finished_at: finished_at} ->
        seed = trial.seed[run_id]

        %{
          tenant: seed.tenant,
          cohort: seed.cohort,
          wait_ms: Stats.wait_ms(finished_at, seed.due_at, trial.started_at)
        }
      end)

    max_finished = max_datetime(Enum.map(trial.finished, & &1.finished_at))
    drain_time_ms = max(DateTime.diff(max_finished, trial.started_at, :millisecond), 1)

    hot_waits = for w <- waits, w.cohort == "hot", do: w.wait_ms
    light_waits = for w <- waits, w.cohort == "light", do: w.wait_ms

    light_p95 = Stats.percentiles(light_waits).p95 || 0
    light_p95_norm = light_p95 / drain_time_ms

    per_tenant = per_tenant_stats(waits)
    jain = Stats.jain(Enum.map(per_tenant, & &1.mean_wait_ms))

    score = round(100 * Stats.clamp(1 - light_p95_norm, 0.0, 1.0))

    measurements = %{
      tenants: k,
      light_tenant_count: k - 1,
      hot_fraction: hot_fraction,
      n: n,
      concurrency: concurrency,
      hot_run_count: hot_n,
      light_run_count: n - hot_n,
      drain_time_ms: drain_time_ms,
      light_p95_wait_ms: light_p95,
      light_p95_norm: light_p95_norm,
      jain_mean_wait: jain,
      hot_wait_ms: Stats.percentiles(hot_waits),
      light_wait_ms: Stats.percentiles(light_waits),
      per_tenant: per_tenant,
      timing_scope: "staged due-time to terminal finished_at per tenant (queue plus service)"
    }

    evidence = evidence(light_p95_norm, score)

    {:ok,
     %{
       scenario: name(),
       metric: metric(),
       label: "#{round(hot_fraction * 100)}% hot tenant @#{concurrency}",
       score: score,
       passed: true,
       evidence: evidence,
       measurements: measurements,
       invariants: trial.invariants
     }}
  end

  defp build_runs(k, n, hot_n, due_at) do
    hot_tenant = tenant_name(0)
    light_tenants = for i <- 1..(k - 1), do: tenant_name(i)

    hot_runs =
      for idx <- 1..hot_n do
        %{idx: idx, due_at: due_at, cohort: "hot", tenant: hot_tenant}
      end

    light_count = n - hot_n

    light_runs =
      for j <- 0..(light_count - 1) do
        %{
          idx: hot_n + 1 + j,
          due_at: due_at,
          cohort: "light",
          tenant: Enum.at(light_tenants, rem(j, k - 1))
        }
      end

    hot_runs ++ light_runs
  end

  defp per_tenant_stats(waits) do
    waits
    |> Enum.group_by(& &1.tenant)
    |> Enum.map(fn {tenant, tenant_waits} ->
      values = Enum.map(tenant_waits, & &1.wait_ms)

      %{
        tenant: tenant,
        cohort: hd(tenant_waits).cohort,
        count: length(values),
        mean_wait_ms: Stats.mean(values),
        p95_wait_ms: Stats.percentiles(values).p95
      }
    end)
    |> Enum.sort_by(& &1.tenant)
  end

  defp evidence(light_p95_norm, score) do
    base = "light p95 = #{round(light_p95_norm * 100)}% of drain"
    if score < 30, do: base <> " (legacy tenant-blind policy)", else: base
  end

  defp tenant_name(i), do: "tenant-" <> String.pad_leading(Integer.to_string(i), 2, "0")

  defp graph do
    Docket.Graph.new!(id: "docket-scorecard-tenant-fairness")
    |> Docket.Graph.put_node!("noop", implementation: NoopNode)
    |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
    |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
  end

  defp max_datetime([]), do: raise("tenant_fairness trial completed no runs")

  defp max_datetime([first | rest]) do
    Enum.reduce(rest, first, fn candidate, acc ->
      if DateTime.compare(candidate, acc) == :gt, do: candidate, else: acc
    end)
  end
end
