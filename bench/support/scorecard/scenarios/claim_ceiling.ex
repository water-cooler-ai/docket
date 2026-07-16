defmodule Docket.Bench.Scorecard.Scenarios.ClaimCeiling do
  @moduledoc "Direct claim throughput against a frozen backlog, bypassing the dispatcher to measure raw SQL claim contention."
  @behaviour Docket.Bench.Scorecard.Scenario

  alias Docket.Bench.Scorecard.{Config, Db, Seed, Stats}
  alias Docket.Bench.Scorecard.Nodes.NoopNode

  @batch 50
  @empty_streak_limit 3
  @orphan_ttl_ms 3_600_000
  @max_claim_attempts 5
  @note "concurrent direct claimers measure raw SQL claim contention; production serializes claims through one dispatcher per instance"

  @impl true
  def name, do: "claim_ceiling"

  @impl true
  def metric, do: "Claim efficiency"

  @impl true
  def run(profile, ctx) do
    n = profile.n
    workers = profile.workers
    target = profile.target_claims_per_sec
    batch = Map.get(profile, :batch, @batch)

    seed_backlog(ctx, n)

    ctx_pg =
      Docket.Postgres.context(
        repo: Db.repo(),
        prefix: ctx.prefix,
        claim_policy: Config.claim_policy_config(ctx)
      )

    started = System.monotonic_time(:microsecond)

    tasks =
      Enum.map(1..workers, fn _worker -> Task.async(fn -> drain_worker(ctx_pg, batch) end) end)

    results = await_workers(tasks, workers, ctx.config.drain_timeout_ms)

    full_elapsed_us = System.monotonic_time(:microsecond) - started

    elapsed_us =
      case results |> Enum.map(& &1.last_lease_at) |> Enum.reject(&is_nil/1) do
        [] -> full_elapsed_us
        lease_ends -> Enum.max(lease_ends) - started
      end

    elapsed_s = max(elapsed_us / 1_000_000, 0.000_001)

    total_leases = Enum.sum(Enum.map(results, & &1.leases))
    total_poisoned = Enum.sum(Enum.map(results, & &1.poisoned))

    call_latency =
      results
      |> Enum.flat_map(& &1.call_times_us)
      |> Enum.map(&(&1 / 1000))
      |> Stats.percentiles()

    claims_per_sec = total_leases / elapsed_s
    score = round(100 * min(1.0, claims_per_sec / target))

    measurements = %{
      n: n,
      workers: workers,
      batch_size: batch,
      target_claims_per_sec: target,
      claims_per_sec: claims_per_sec,
      elapsed_s: elapsed_s,
      total_leases: total_leases,
      total_poisoned: total_poisoned,
      call_latency_ms: call_latency,
      note: @note
    }

    evidence =
      "#{format_rate(claims_per_sec)} claims/s (target #{target}), p99 #{format_ms(call_latency.p99)}"

    {:ok,
     %{
       scenario: name(),
       metric: metric(),
       label: "#{n} frozen backlog @#{workers}",
       score: score,
       passed: true,
       evidence: evidence,
       measurements: measurements,
       invariants: invariants(ctx, n, total_leases)
     }}
  end

  defp seed_backlog(ctx, n) do
    Db.truncate(ctx)
    Docket.Postgres.GraphCache.clear()

    due_at = DateTime.add(DateTime.utc_now(), -1, :second)

    runs =
      for idx <- 1..n do
        %{idx: idx, due_at: due_at, cohort: "backlog", tenant: nil}
      end

    plan = %{scenario: name(), tenant_mode: :none, graph: graph(), runs: runs}
    Seed.seed(ctx, plan)
  end

  defp await_workers(tasks, workers, timeout_ms) do
    outcomes =
      tasks
      |> Task.yield_many(timeout_ms)
      |> Enum.map(fn {task, res} ->
        case res || Task.shutdown(task, :brutal_kill) do
          {:ok, value} -> {:ok, value}
          _ -> :timeout
        end
      end)

    timed_out = Enum.count(outcomes, &(&1 == :timeout))

    if timed_out > 0 do
      raise "claim_ceiling drain timed out with #{timed_out} of #{workers} workers unfinished"
    end

    Enum.map(outcomes, fn {:ok, value} -> value end)
  end

  defp drain_worker(ctx_pg, batch), do: drain_loop(ctx_pg, batch, 0, [], 0, 0, nil)

  defp drain_loop(_ctx_pg, _batch, streak, times, leases, poisoned, last_lease_at)
       when streak >= @empty_streak_limit do
    %{
      leases: leases,
      poisoned: poisoned,
      call_times_us: Enum.reverse(times),
      last_lease_at: last_lease_at
    }
  end

  defp drain_loop(ctx_pg, batch, streak, times, leases, poisoned, last_lease_at) do
    policy = %{
      now: DateTime.utc_now(),
      limit: batch,
      orphan_ttl_ms: @orphan_ttl_ms,
      max_claim_attempts: @max_claim_attempts,
      preference: nil
    }

    t0 = System.monotonic_time(:microsecond)

    {claimed, poisoned_batch} =
      case Docket.Postgres.RunStore.claim_due(ctx_pg, :system, policy) do
        {:ok, %{leases: claimed, poisoned: poisoned_batch}} -> {claimed, poisoned_batch}
        {:error, reason} -> raise "claim_due failed: #{inspect(reason)}"
      end

    t1 = System.monotonic_time(:microsecond)

    count = length(claimed)

    if count == 0 do
      drain_loop(
        ctx_pg,
        batch,
        streak + 1,
        times,
        leases,
        poisoned + length(poisoned_batch),
        last_lease_at
      )
    else
      drain_loop(
        ctx_pg,
        batch,
        0,
        [t1 - t0 | times],
        leases + count,
        poisoned + length(poisoned_batch),
        t1
      )
    end
  end

  defp invariants(ctx, n, total_leases) do
    runs = Db.table(ctx.prefix, "docket_runs")

    [
      sql_zero("all_claimed", "SELECT count(*) FROM #{runs} WHERE claim_token IS NULL"),
      sql_zero(
        "dup_claim_tokens",
        "SELECT count(*) FROM (SELECT claim_token FROM #{runs} WHERE claim_token IS NOT NULL GROUP BY claim_token HAVING count(*) > 1) AS duplicates"
      ),
      sql_zero("no_poisoned", "SELECT count(*) FROM #{runs} WHERE poisoned_at IS NOT NULL"),
      %{name: "leases_match_backlog", pass: total_leases == n, expected: n, actual: total_leases}
    ]
  end

  defp sql_zero(name, sql) do
    [[actual]] = Db.repo().query!(sql).rows
    %{name: name, pass: actual == 0, expected: 0, actual: actual}
  end

  defp graph do
    Docket.Graph.new!(id: "docket-scorecard-claim-ceiling")
    |> Docket.Graph.put_node!("noop", implementation: NoopNode)
    |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
    |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
  end

  defp format_rate(value) when value >= 1000, do: "#{Float.round(value / 1000, 1)}k"
  defp format_rate(value), do: "#{round(value)}"

  defp format_ms(nil), do: "n/a"
  defp format_ms(ms), do: "#{Float.round(ms * 1.0, 2)}ms"
end
