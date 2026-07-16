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

  @policy_sensitive ["claim_ceiling", "tenant_fairness", "fast_slow"]

  def registry, do: @registry

  def names, do: Enum.map(@registry, fn {name, _module} -> name end)

  def policy_sensitive?(name), do: name in @policy_sensitive

  def run_variants(name, ctx) do
    if policy_sensitive?(name) do
      Enum.map(ctx.config.claim_policies, fn policy ->
        run_one(name, Map.put(ctx, :claim_policy, policy))
      end)
    else
      [run_one(name, ctx)]
    end
  end

  def module(name) do
    case List.keyfind(@registry, name, 0) do
      {^name, module} -> module
      nil -> raise ArgumentError, "unknown scenario #{inspect(name)}"
    end
  end

  def run_one(name, ctx) do
    module = module(name)
    profile = ctx.config.scenarios[name]
    policy = policy_name(ctx)

    {pid, ref} =
      spawn_monitor(fn ->
        marker =
          try do
            {:run, module.run(profile, ctx)}
          rescue
            error -> {:rescue, error}
          catch
            kind, reason -> {:catch, kind, reason}
          end

        exit({:scorecard_result, marker})
      end)

    receive do
      {:DOWN, ^ref, :process, ^pid, {:scorecard_result, marker}} ->
        dispatch_marker(module, marker, policy)

      {:DOWN, ^ref, :process, ^pid, reason} ->
        error_result(module, {:exit, reason}, policy)
    end
  end

  defp dispatch_marker(module, {:run, {:ok, result}}, policy),
    do: normalize(module, result, policy)

  defp dispatch_marker(module, {:run, {:error, :not_implemented}}, policy),
    do: not_implemented_result(module, policy)

  defp dispatch_marker(module, {:run, {:error, reason}}, policy),
    do: error_result(module, reason, policy)

  defp dispatch_marker(module, {:rescue, error}, policy),
    do: error_result(module, error, policy)

  defp dispatch_marker(module, {:catch, kind, reason}, policy),
    do: error_result(module, {kind, reason}, policy)

  defp policy_name(ctx) do
    case Map.get(ctx, :claim_policy) do
      %{name: name} -> name
      nil -> nil
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
    expected = length(plan.runs)
    started_at = DateTime.utc_now()
    runtime = Runtime.start(ctx, overrides)

    drain =
      try do
        Runtime.drain_wait(ctx, timeout_ms)
      after
        Runtime.stop(runtime)
      end

    case drain do
      :ok -> :ok
      {:timeout, remaining} -> raise drain_timeout_message(ctx, expected, remaining)
    end

    %{
      seed: seed,
      expected: expected,
      started_at: started_at,
      finished: Db.finished_runs(ctx),
      invariants: Invariants.check(ctx, expected)
    }
  end

  defp drain_timeout_message(ctx, expected, remaining) do
    summary = drain_invariant_summary(Invariants.check(ctx, expected))
    "scorecard drain timed out with #{remaining} runs not finished; " <> summary
  end

  defp drain_invariant_summary(invariants) do
    case Enum.filter(invariants, &(not &1.pass)) do
      [] ->
        "no invariant violations detected"

      failing ->
        Enum.map_join(failing, ", ", fn %{name: name, expected: expected, actual: actual} ->
          "#{name} expected=#{expected} actual=#{actual}"
        end)
    end
  end

  def not_implemented_result(module, policy \\ nil) do
    module
    |> base_result(policy)
    |> Map.merge(%{score: nil, passed: false, evidence: "not implemented"})
  end

  def error_result(module, reason, policy \\ nil) do
    module
    |> base_result(policy)
    |> Map.merge(%{score: nil, passed: false, evidence: "error: #{describe(reason)}"})
  end

  defp base_result(module, policy) do
    %{
      scenario: module.name(),
      metric: module.metric(),
      label: nil,
      policy: policy,
      score: nil,
      passed: false,
      evidence: "",
      measurements: %{},
      invariants: []
    }
  end

  defp normalize(module, result, policy) do
    invariants = Map.get(result, :invariants, [])
    invariant_failed = Enum.any?(invariants, &(not &1.pass))

    %{
      scenario: Map.get(result, :scenario, module.name()),
      metric: Map.get(result, :metric, module.metric()),
      label: Map.get(result, :label),
      policy: policy,
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
