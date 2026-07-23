defmodule Docket.Bench.Scorecard.Runtime do
  @moduledoc "Supervised production runtime lifecycle and durable drain-wait polling."

  alias Docket.Bench.Scorecard.Db

  @runtime_name Docket.Bench.Scorecard.Instance
  @pruner [
    interval_ms: 86_400_000,
    event_retention_ms: 86_400_000,
    run_retention_ms: 86_400_000,
    batch_size: 100
  ]
  @default_orphan_ttl_ms 60_000
  @default_drain_max_elapsed_ms 3_000
  @default_max_attempt_elapsed_ms 2_000

  def name, do: @runtime_name

  def start(ctx, overrides) do
    {:ok, runtime} = Docket.Runtime.Supervisor.start_link(production_opts(ctx, overrides))
    runtime
  end

  def stop(runtime) do
    if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)
  end

  def drain_wait(ctx, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll_drain(ctx, deadline)
  end

  defp poll_drain(ctx, deadline) do
    remaining = Db.unfinished_count(ctx)

    cond do
      remaining == 0 ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise "scorecard drain timed out with #{remaining} runs not finished"

      true ->
        Process.sleep(150)
        poll_drain(ctx, deadline)
    end
  end

  defp production_opts(ctx, overrides) do
    concurrency = Keyword.fetch!(overrides, :concurrency)
    tenant_mode = Keyword.get(overrides, :tenant_mode, :none)
    orphan_ttl_ms = Keyword.get(overrides, :orphan_ttl_ms, @default_orphan_ttl_ms)

    drain_max_elapsed_ms =
      Keyword.get(overrides, :drain_max_elapsed_ms, @default_drain_max_elapsed_ms)

    drain_max_moments = Keyword.get(overrides, :drain_max_moments, 100)

    max_attempt_elapsed_ms =
      Keyword.get(overrides, :max_attempt_elapsed_ms, @default_max_attempt_elapsed_ms)

    [
      name: @runtime_name,
      tenant_mode: tenant_mode,
      max_attempt_elapsed_ms: max_attempt_elapsed_ms,
      backend:
        {Docket.Postgres,
         repo: ctx.repo,
         prefix: ctx.prefix,
         notifier: :none,
         claim_policy: Docket.Bench.Scorecard.Config.claim_policy_config(ctx),
         dispatcher: [
           concurrency: concurrency,
           poll_interval_ms: ctx.config.poll_interval_ms,
           orphan_ttl_ms: orphan_ttl_ms,
           max_claim_attempts: 5,
           drain_timeout_ms: 30_000
         ],
         vehicle: [
           drain_budget: [max_moments: drain_max_moments, max_elapsed_ms: drain_max_elapsed_ms]
         ],
         pruner: @pruner}
    ]
  end
end
