defmodule Docket.Benchmark.Headline do
  @moduledoc """
  Flat per-trial headline metrics embedded in benchmark artifacts.

  Each trial artifact carries a `headline` block: a single-level map of the
  trial's most decision-relevant values under stable, unit-suffixed keys, so
  cross-run comparison and scripted analysis never need scenario-specific
  nested paths. Values that a trial did not produce are omitted. The nested
  `measurements` tree remains the complete record; distribution sample counts
  and measurement caveats live there.
  """

  @doc "Builds the headline block for one trial artifact."
  def build(artifact) do
    artifact
    |> common()
    |> Map.merge(scenario(artifact[:scenario], artifact))
    |> Map.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp common(artifact) do
    %{
      throughput_per_second: get_in(artifact, [:measurements, :throughput_per_second]),
      duration_us: artifact[:duration_us],
      completed_runs: get_in(artifact, [:measurements, :completed_runs])
    }
  end

  defp scenario("empty_one_step", artifact) do
    %{
      activation_to_first_commit_p50_us:
        latency(artifact, :burst_activation_to_first_commit_offset_us, :p50),
      activation_to_first_commit_p95_us:
        latency(artifact, :burst_activation_to_first_commit_offset_us, :p95),
      first_commit_to_terminal_p50_us: latency(artifact, :first_commit_to_terminal_us, :p50),
      first_commit_to_terminal_p95_us: latency(artifact, :first_commit_to_terminal_us, :p95),
      activation_to_terminal_p50_us:
        latency(artifact, :burst_activation_to_terminal_commit_offset_us, :p50),
      activation_to_terminal_p95_us:
        latency(artifact, :burst_activation_to_terminal_commit_offset_us, :p95),
      claim_scan_p50_us: latency(artifact, :claim_scan_total_us, :p50),
      claim_scan_p95_us: latency(artifact, :claim_scan_total_us, :p95)
    }
  end

  defp scenario("claim_only", artifact) do
    %{
      burst_start_to_claim_p50_us: latency(artifact, :burst_start_to_claim_offset_us, :p50),
      burst_start_to_claim_p95_us: latency(artifact, :burst_start_to_claim_offset_us, :p95),
      claim_query_p50_us: latency(artifact, :claim_query_time_us, :p50),
      claim_query_p95_us: latency(artifact, :claim_query_time_us, :p95),
      pool_queue_p50_us: latency(artifact, :claim_queue_time_us, :p50),
      pool_queue_p95_us: latency(artifact, :claim_queue_time_us, :p95),
      mean_rows_per_nonempty_scan:
        get_in(artifact, [:measurements, :batches, :mean_rows_per_nonempty_scan]),
      ready_claims: get_in(artifact, [:measurements, :counts, :ready_claims]),
      expired_claims: get_in(artifact, [:measurements, :counts, :expired_claims])
    }
  end

  defp scenario("blocked_vehicles", artifact) do
    blocked = [:measurements, :blocked_vehicles]

    %{
      plateau_fill_duration_us: get_in(artifact, blocked ++ [:plateau_fill_duration_us]),
      stable_hold_duration_us: get_in(artifact, blocked ++ [:stable_hold_duration_us]),
      short_query_p50_us:
        get_in(artifact, blocked ++ [:latency, :unrelated_short_query_round_trip_us, :p50]),
      short_query_p95_us:
        get_in(artifact, blocked ++ [:latency, :unrelated_short_query_round_trip_us, :p95]),
      gate_release_to_terminal_p95_us:
        get_in(artifact, blocked ++ [:latency, :gate_release_to_terminal_commit_us, :p95]),
      max_claim_age_at_release_ms:
        get_in(artifact, blocked ++ [:claim_freshness, :maximum_claim_age_ms_at_release]),
      orphan_ttl_ms: get_in(artifact, [:parameters, :orphan_ttl_ms]),
      sampler_missed_ticks: get_in(artifact, blocked ++ [:timeline, :missed_ticks])
    }
  end

  defp scenario(scenario, artifact)
       when scenario in [
              "cyclic_vs_one_step",
              "mixed_service_times",
              "parked_wait_vs_blocking_wait"
            ] do
    cohorts = get_in(artifact, [:measurements, :cohorts]) || %{}

    aggregate = %{
      activation_to_terminal_p50_us:
        latency(artifact, :burst_activation_to_terminal_commit_offset_us, :p50),
      activation_to_terminal_p95_us:
        latency(artifact, :burst_activation_to_terminal_commit_offset_us, :p95)
    }

    Enum.reduce(cohorts, aggregate, fn {label, cohort}, headline ->
      Map.merge(headline, %{
        :"cohort_#{label}_activation_to_terminal_p50_us" =>
          get_in(cohort, [:activation_to_terminal_commit_offset_us, :p50]),
        :"cohort_#{label}_activation_to_terminal_p95_us" =>
          get_in(cohort, [:activation_to_terminal_commit_offset_us, :p95]),
        :"cohort_#{label}_first_claim_to_terminal_p50_us" =>
          get_in(cohort, [:first_claim_to_terminal_commit_us, :p50]),
        :"cohort_#{label}_queue_share_of_median_percent" => cohort[:queue_share_of_median_percent]
      })
    end)
  end

  defp scenario(_scenario, _artifact), do: %{}

  defp latency(artifact, metric, statistic),
    do: get_in(artifact, [:measurements, :latency, metric, statistic])
end
