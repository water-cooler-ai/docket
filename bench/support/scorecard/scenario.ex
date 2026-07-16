defmodule Docket.Bench.Scorecard.Scenario do
  @moduledoc "Scenario behaviour, registry, shared runtime-trial plumbing, and result normalization."

  alias Docket.Bench.Scorecard.{Db, Invariants, Runtime, Seed}

  @callback name() :: String.t()
  @callback metric() :: String.t()
  @callback run(profile :: map(), ctx :: map()) :: {:ok, map()} | {:error, term()}

  @registry [
    {"throughput", Docket.Bench.Scorecard.Scenarios.Throughput},
    {"concurrency", Docket.Bench.Scorecard.Scenarios.Concurrency},
    {"claim_ceiling", Docket.Bench.Scorecard.Scenarios.ClaimCeiling},
    {"tenant_fairness", Docket.Bench.Scorecard.Scenarios.TenantFairness},
    {"fast_slow", Docket.Bench.Scorecard.Scenarios.FastSlow},
    {"surge", Docket.Bench.Scorecard.Scenarios.Surge}
  ]

  def registry, do: @registry

  def names, do: Enum.map(@registry, fn {name, _module} -> name end)

  def module(name) do
    case List.keyfind(@registry, name, 0) do
      {^name, module} -> module
      nil -> raise ArgumentError, "unknown scenario #{inspect(name)}"
    end
  end

  def run_one(name, ctx) do
    module = module(name)
    profile = ctx.config.scenarios[name]

    try do
      case module.run(profile, ctx) do
        {:ok, result} -> normalize(module, result)
        {:error, :not_implemented} -> not_implemented_result(module)
        {:error, reason} -> error_result(module, reason)
      end
    rescue
      error -> error_result(module, error)
    catch
      kind, reason -> error_result(module, {kind, reason})
    end
  end

  def run_trial(ctx, plan, opts \\ []) do
    Db.truncate(ctx)
    Docket.Postgres.GraphCache.clear()
    seed = Seed.seed(ctx, plan)

    overrides =
      [concurrency: Keyword.fetch!(opts, :concurrency), tenant_mode: plan.tenant_mode] ++
        Keyword.get(opts, :runtime, [])

    timeout_ms = Keyword.get(opts, :drain_timeout_ms, ctx.config.drain_timeout_ms)
    started_at = DateTime.utc_now()
    runtime = Runtime.start(ctx, overrides)

    try do
      Runtime.drain_wait(ctx, timeout_ms)
    after
      Runtime.stop(runtime)
    end

    expected = length(plan.runs)

    %{
      seed: seed,
      expected: expected,
      started_at: started_at,
      finished: Db.finished_runs(ctx),
      invariants: Invariants.check(ctx, expected)
    }
  end

  def not_implemented_result(module) do
    module
    |> base_result()
    |> Map.merge(%{score: nil, passed: false, evidence: "not implemented"})
  end

  def error_result(module, reason) do
    module
    |> base_result()
    |> Map.merge(%{score: nil, passed: false, evidence: "error: #{describe(reason)}"})
  end

  defp base_result(module) do
    %{
      scenario: module.name(),
      metric: module.metric(),
      label: nil,
      score: nil,
      passed: false,
      evidence: "",
      measurements: %{},
      invariants: []
    }
  end

  defp normalize(module, result) do
    invariants = Map.get(result, :invariants, [])
    invariant_failed = Enum.any?(invariants, &(not &1.pass))

    %{
      scenario: Map.get(result, :scenario, module.name()),
      metric: Map.get(result, :metric, module.metric()),
      label: Map.get(result, :label),
      score: if(invariant_failed, do: nil, else: Map.get(result, :score)),
      passed: not invariant_failed and Map.get(result, :passed, false),
      evidence: Map.get(result, :evidence, ""),
      measurements: Map.get(result, :measurements, %{}),
      invariants: invariants
    }
  end

  defp describe(reason) do
    if is_exception(reason), do: Exception.message(reason), else: inspect(reason)
  end
end
