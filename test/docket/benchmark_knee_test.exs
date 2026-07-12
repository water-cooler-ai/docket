if Code.ensure_loaded?(Docket.Benchmark.Postgres) do
  defmodule Docket.BenchmarkKneeTest do
    use ExUnit.Case, async: true

    alias Docket.Benchmark.Postgres

    test "suite payload detects the first concurrency knee per pool deterministically" do
      artifacts =
        [
          {4, 5, 188.0, 145},
          {1, 5, 100.0, 100},
          {2, 5, 180.0, 110}
        ]
        |> Enum.flat_map(fn {concurrency, pool_size, throughput, p95} ->
          artifacts(concurrency, pool_size, throughput, p95)
        end)

      payload = Postgres.suite_summary_payload(artifacts)

      assert payload.concurrency_knee.data_status == "sufficient"
      assert payload.concurrency_knee.method.minimum_valid_cells_per_pool == 3
      assert payload.concurrency_knee.method.minimum_successful_repetitions_per_cell == 3

      assert payload.concurrency_knee.method.thresholds == %{
               minimum_throughput_gain_percent: 10.0,
               tail_latency_increase_percent: 20.0
             }

      assert [pool] = payload.concurrency_knee.pools
      assert pool.pool_size == 5
      assert pool.status == "knee_detected"
      assert pool.tail_latency_metric == "burst_activation_to_terminal_commit_p95_us"

      assert pool.baseline == %{
               concurrency: 1,
               throughput_per_second: 100.0,
               tail_latency_p95_us: 100
             }

      assert pool.peak_throughput == %{
               concurrency: 4,
               throughput_per_second: 188.0,
               tail_latency_p95_us: 145
             }

      assert pool.knee_reason == "throughput_plateau_and_tail_latency_increase"
      assert pool.bottleneck_attribution.status == "inconclusive"
      assert pool.recommended_safe_concurrency == 2
      assert pool.knee_point.concurrency == 4

      assert pool.knee_point.changes_from_previous_percent == %{
               throughput: 4.444,
               tail_latency_p95: 31.818
             }
    end

    test "reports throughput regression as a knee even when tail latency does not rise" do
      analysis =
        Postgres.concurrency_knee_analysis(
          [
            cell(1, 2, 100.0, 100),
            cell(2, 2, 180.0, 90),
            cell(4, 2, 160.0, 85)
          ],
          "empty_one_step"
        )

      assert [pool] = analysis.pools
      assert pool.status == "knee_detected"
      assert pool.knee_reason == "throughput_regression"
      assert pool.knee_point.concurrency == 4
      assert pool.knee_point.changes_from_previous_percent.throughput == -11.111
      assert pool.recommended_safe_concurrency == 2
      assert pool.peak_throughput.concurrency == 2
    end

    test "uses the highest tested concurrency when no knee is observed" do
      analysis =
        Postgres.concurrency_knee_analysis(
          [
            cell(4, 2, 210.0, 85),
            cell(1, 2, 100.0, 100),
            cell(2, 2, 150.0, 90)
          ],
          "empty_one_step"
        )

      assert [pool] = analysis.pools
      assert pool.status == "knee_not_observed"
      assert pool.knee_point == nil
      assert pool.knee_reason == nil
      assert pool.recommended_safe_concurrency == 4
      assert pool.peak_throughput.concurrency == 4
      assert pool.recommendation_basis =~ "highest successful tested concurrency"
    end

    test "attributes a detected knee only when stage evidence crosses conservative thresholds" do
      analysis =
        Postgres.concurrency_knee_analysis(
          [
            cell_with_bottleneck(1, 5, 100.0, 1_000, 20),
            cell_with_bottleneck(2, 5, 180.0, 900, 100),
            cell_with_bottleneck(4, 5, 185.0, 1_500, 600)
          ],
          "empty_one_step"
        )

      assert [pool] = analysis.pools
      assert pool.status == "knee_detected"
      assert pool.bottleneck_attribution.status == "evidence_supported"
      assert pool.bottleneck_attribution.primary == "repo_pool_queue"
      assert "repo_pool_queue" in pool.bottleneck_attribution.contributors

      repo = pool.bottleneck_attribution.evidence.repo_pool_queue
      assert repo.knee_p95_us == 600
      assert repo.growth_percent == 500.0
      assert repo.share_of_knee_tail_percent == 40.0
      assert repo.supported
    end

    test "marks pools with fewer than three valid cells as insufficient" do
      analysis =
        Postgres.concurrency_knee_analysis(
          [
            cell(1, 2, 100.0, 100),
            cell(2, 2, 180.0, 90, false),
            cell(1, 5, 100.0, 100),
            cell(2, 5, 150.0, 90),
            cell(4, 5, 210.0, 85)
          ],
          "empty_one_step"
        )

      assert analysis.data_status == "partial"
      assert [insufficient, sufficient] = analysis.pools

      assert insufficient.pool_size == 2
      assert insufficient.status == "insufficient_data"
      assert insufficient.tested_cell_count == 2
      assert insufficient.valid_cell_count == 1
      assert insufficient.baseline.concurrency == 1
      assert insufficient.knee_point == nil
      assert insufficient.recommended_safe_concurrency == nil
      assert insufficient.insufficient_data_reason =~ "at least 3 successful concurrency cells"

      assert sufficient.pool_size == 5
      assert sufficient.status == "knee_not_observed"
      assert sufficient.insufficient_data_reason == nil
    end

    test "withholds an evidence-grade knee and safe recommendation below three repetitions" do
      analysis =
        Postgres.concurrency_knee_analysis(
          [
            cell(1, 2, 100.0, 100, true, 1),
            cell(2, 2, 180.0, 110, true, 1),
            cell(4, 2, 188.0, 145, true, 1)
          ],
          "empty_one_step"
        )

      assert analysis.data_status == "insufficient"
      assert analysis.method.minimum_successful_repetitions_per_cell == 3
      assert [pool] = analysis.pools
      assert pool.status == "insufficient_data"
      assert pool.valid_cell_count == 0
      assert pool.recommended_safe_concurrency == nil
      assert pool.insufficient_data_reason =~ "at least 3 successful repetitions"
    end

    test "aggregate active time alone does not support database-pressure attribution" do
      analysis =
        Postgres.concurrency_knee_analysis(
          [
            cell_with_database_context(1, 5, 100.0, 1_000, 20.0),
            cell_with_database_context(2, 5, 180.0, 900, 40.0),
            cell_with_database_context(4, 5, 185.0, 1_500, 500.0)
          ],
          "empty_one_step"
        )

      assert [pool] = analysis.pools
      assert pool.status == "knee_detected"
      assert pool.bottleneck_attribution.status == "inconclusive"

      database = pool.bottleneck_attribution.evidence.database_pressure
      refute database.supported
      refute database.active_time_supports_attribution
      assert database.active_time_percent_of_measured_wall == 500.0
      assert database.support_basis =~ "requires a positive boundary wait/lock signal"
    end

    test "steady-arrival disables the generic drain-inclusive safe-capacity recommendation" do
      analysis =
        Postgres.concurrency_knee_analysis(
          [
            steady_cell(1, 2, 40.0, 40.0, 0, 0.0, 1_000),
            steady_cell(2, 2, 48.0, 45.0, 2, 10.0, 2_000),
            steady_cell(4, 2, 50.0, 35.0, 12, 100.0, 10_000)
          ],
          "steady_arrival"
        )

      assert analysis.data_status == "exploratory"
      assert analysis.method.generic_safe_capacity_recommendation == "disabled"
      assert [pool] = analysis.pools
      assert pool.status == "exploratory_only"
      assert pool.recommended_safe_concurrency == nil
      assert pool.knee_point == nil
      assert length(pool.sustainability_observations) == 3

      overloaded = List.last(pool.sustainability_observations)
      assert overloaded.achieved_percent_of_offered == 70.0
      assert overloaded.due_outstanding_at_arrival_window_end == 12
      assert overloaded.oldest_due_lag_growth_ms_per_second == 100.0
    end

    defp artifacts(concurrency, pool_size, throughput, p95) do
      for repetition <- 1..3 do
        artifact(concurrency, pool_size, throughput, p95, repetition)
      end
    end

    defp artifact(concurrency, pool_size, throughput, p95, repetition) do
      %{
        scenario: "empty_one_step",
        success: true,
        point: %{concurrency: concurrency, pool_size: pool_size, repetition: repetition},
        parameters: %{repetitions: 3},
        duration_us: p95,
        measurements: %{
          throughput_per_second: throughput,
          latency: %{
            burst_activation_to_first_commit_offset_us: %{p50: p95, p95: p95},
            first_commit_to_terminal_us: %{p50: p95, p95: p95},
            burst_activation_to_terminal_commit_offset_us: %{p50: p95, p95: p95}
          }
        }
      }
    end

    defp cell(
           concurrency,
           pool_size,
           throughput,
           p95,
           success \\ true,
           successful_repetitions \\ 3
         ) do
      %{
        concurrency: concurrency,
        pool_size: pool_size,
        successful_repetitions: successful_repetitions,
        success: success,
        throughput_per_second: %{median: throughput, sample_count: successful_repetitions},
        latency_across_repetitions: %{
          burst_activation_to_terminal_commit_p95_us: %{median: p95}
        }
      }
    end

    defp cell_with_bottleneck(concurrency, pool_size, throughput, p95, repo_queue) do
      cell(concurrency, pool_size, throughput, p95)
      |> Map.put(:bottleneck_evidence, %{
        latency: %{
          repo_queue_p95_us: %{median: repo_queue},
          claim_scan_p95_us: %{median: 20},
          lifecycle_transaction_p95_us: %{median: 40},
          node_execution_p95_us: %{median: 10},
          dispatcher_poll_p95_us: %{median: 20}
        },
        database: %{
          active_time_percent_of_measured_wall: %{median: 20.0},
          active_waiting_backends_max_boundary: %{median: 0},
          lock_waiting_backends_max_boundary: %{median: 0},
          ungranted_lock_rows_max_boundary: %{median: 0}
        }
      })
    end

    defp cell_with_database_context(concurrency, pool_size, throughput, p95, active_time) do
      cell(concurrency, pool_size, throughput, p95)
      |> Map.put(:bottleneck_evidence, %{
        latency: %{
          repo_queue_p95_us: %{median: 10},
          claim_scan_p95_us: %{median: 10},
          lifecycle_transaction_p95_us: %{median: 10},
          node_execution_p95_us: %{median: 10},
          dispatcher_poll_p95_us: %{median: 10}
        },
        database: %{
          active_time_percent_of_measured_wall: %{median: active_time},
          active_waiting_backends_max_boundary: %{median: 0},
          lock_waiting_backends_max_boundary: %{median: 0},
          ungranted_lock_rows_max_boundary: %{median: 0}
        }
      })
    end

    defp steady_cell(
           concurrency,
           pool_size,
           offered,
           achieved,
           outstanding,
           oldest_lag_growth,
           completion_lag_p95
         ) do
      %{
        concurrency: concurrency,
        pool_size: pool_size,
        successful_repetitions: 3,
        success: true,
        throughput_per_second: %{median: achieved, sample_count: 3},
        latency_across_repetitions: %{
          offered_rate_per_second: %{median: offered},
          achieved_arrival_window_rate_per_second: %{median: achieved},
          due_outstanding_at_arrival_window_end: %{median: outstanding},
          backlog_growth_runs_per_second: %{median: outstanding * 1.0},
          oldest_due_lag_growth_ms_per_second: %{median: oldest_lag_growth},
          retained_completion_lag_p95_us: %{median: completion_lag_p95}
        }
      }
    end
  end
end
