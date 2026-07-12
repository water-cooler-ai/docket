if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.BenchmarkPostgresTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    test "claim-only mixed backlog preserves exact samples and invariants" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-claim-bench-#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario claim_only --runs 12 --concurrency 4 --pool-size 2 --batch-size 3 --ready-ratio 7:5 --output #{output})
               )

      assert {:ok, %{artifact: suite, artifacts: [artifact]}} = Docket.Benchmark.run(config)
      assert suite.kind == "benchmark_suite"
      assert suite.trial_count == 1
      assert artifact.success
      assert artifact.workload.ready_count == 7
      assert artifact.workload.expired_count == 5
      assert artifact.measurements.collection.complete_sample_set
      assert artifact.measurements.counts.ready_claims == 7
      assert artifact.measurements.counts.expired_claims == 5
      assert artifact.headline.ready_claims == 7
      assert is_number(artifact.headline.burst_start_to_claim_p50_us)
      assert is_number(artifact.headline.claim_query_p95_us)
      assert Enum.all?(artifact.invariants, & &1.pass)
      assert artifact.cleanup.isolated_database_removed

      encoded = File.read!(output)
      decoded = JSON.decode!(encoded)
      assert is_map(decoded)
      assert decoded["kind"] == "benchmark_suite"
      assert decoded["schema_version"] == 4
      assert decoded["scenario"] == "claim_only"
      assert [point] = decoded["points"]
      assert point["schema_version"] == 4
      assert point["headline"]["ready_claims"] == 7
      refute encoded =~ config.database_url
      refute encoded =~ output
      refute encoded =~ "docket_bench_"

      refute encoded =~
               ~r/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i
    end

    test "claim-only workers stop after the backlog is leased" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-claim-quiescence-#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario claim_only --runs 1000 --concurrency 8 --pool-size 8 --batch-size 1000 --ready-ratio 1:0 --output #{output})
               )

      assert {:ok, %{artifacts: [artifact]}} = Docket.Benchmark.run(config)
      assert artifact.success
      assert artifact.workload.ready_count == 1000
      assert artifact.workload.expired_count == 0
      assert artifact.measurements.batches.nonempty_scans >= 1
      assert artifact.measurements.batches.nonempty_scans <= config.concurrency
      assert artifact.measurements.batches.empty_scans <= config.concurrency
      assert artifact.measurements.batches.total_scans <= config.concurrency * 2

      assert artifact.measurements.counts.claim_query_samples ==
               artifact.measurements.batches.total_scans

      assert artifact.measurements.collection.observed_claim_samples == config.runs
      assert artifact.measurements.collection.complete_sample_set
    end

    test "claim-only supports an entirely expired backlog" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-claim-expired-#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario claim_only --runs 12 --concurrency 4 --pool-size 4 --batch-size 12 --ready-ratio 0:1 --output #{output})
               )

      assert {:ok, %{artifacts: [artifact]}} = Docket.Benchmark.run(config)
      assert artifact.success
      assert artifact.workload.ready_count == 0
      assert artifact.workload.expired_count == 12
      assert artifact.measurements.counts.ready_claims == 0
      assert artifact.measurements.counts.expired_claims == 12
      assert artifact.measurements.counts.reacquired_claims == 12
      assert artifact.measurements.counts.steals == 12
      assert artifact.measurements.counts.poisoned == 0

      assert artifact.measurements.latency.ready_burst_start_to_claim_offset_us.sample_count ==
               0

      assert artifact.measurements.latency.ready_age_at_frozen_claim_clock_ms.sample_count == 0

      assert artifact.measurements.latency.expired_burst_start_to_claim_offset_us.sample_count ==
               12

      assert artifact.measurements.latency.expired_overdue_after_ttl_ms.sample_count == 12
      assert artifact.measurements.collection.expected_ready_samples == 0
      assert artifact.measurements.collection.observed_ready_samples == 0
      assert artifact.measurements.collection.expected_expired_samples == 12
      assert artifact.measurements.collection.observed_expired_samples == 12
      assert artifact.measurements.collection.complete_sample_set
      assert Enum.all?(artifact.invariants, & &1.pass)
    end

    test "blocked vehicles expose a bounded saturated plateau without holding the Repo" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-blocked-bench-#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario blocked_vehicles --runs 4 --concurrency 2 --pool-size 1 --hold-ms 30 --sample-interval-ms 5 --max-samples 8 --probe-count 2 --output #{output})
               )

      assert {:ok, %{artifacts: [artifact]}} = Docket.Benchmark.run(config)
      assert artifact.success
      assert artifact.scenario == "blocked_vehicles"
      assert is_number(artifact.headline.plateau_fill_duration_us)
      assert is_number(artifact.headline.short_query_p50_us)
      assert artifact.headline.orphan_ttl_ms == config.orphan_ttl_ms
      assert artifact.cleanup.isolated_database_removed
      assert artifact.measurements.blocked_vehicles.collection.complete_sample_set
      assert artifact.measurements.blocked_vehicles.plateau.active_claims == 2
      assert artifact.measurements.blocked_vehicles.plateau.ready_unclaimed_runs == 2
      assert artifact.measurements.blocked_vehicles.plateau.repo_pool.capacity == 1

      assert artifact.measurements.blocked_vehicles.plateau.repo_pool.busy_or_unavailable_connections ==
               0

      assert artifact.measurements.blocked_vehicles.plateau.repo_pool.checkout_queue_length == 0

      assert artifact.measurements.blocked_vehicles.latency.unrelated_short_query_round_trip_us.sample_count ==
               2

      assert artifact.measurements.blocked_vehicles.latency.unrelated_short_query_queue_time_us.sample_count ==
               2

      assert artifact.measurements.blocked_vehicles.stable_hold_duration_us >= 30_000

      assert artifact.measurements.blocked_vehicles.claim_freshness.maximum_claim_age_ms_at_release <
               config.orphan_ttl_ms

      phase = artifact.measurements.blocked_vehicles.phase_boundaries_us
      assert phase.activation <= phase.plateau_reached
      assert phase.plateau_reached <= phase.stable_hold_started
      assert phase.stable_hold_started <= phase.gate_release_started
      assert phase.gate_release_started <= phase.gate_release_completed
      assert phase.gate_release_completed <= phase.vehicle_quiescence

      timeline = artifact.measurements.blocked_vehicles.timeline
      assert timeline.raw_sample_count > 0
      assert timeline.retained_bucket_count <= 8
      assert timeline.max_retained_buckets == 8
      assert timeline.represented_sample_count == timeline.raw_sample_count
      assert timeline.forced_phase_sample_count == 2
      assert timeline.forced_final_sample_count == 1
      assert timeline.summary.dispatcher_in_flight_vehicles.max == 2
      assert timeline.summary.blocked_node_calls.max == 2
      assert timeline.summary.dispatcher_maximum_in_flight_vehicles.max == 2
      assert timeline.summary.maximum_blocked_node_calls.max == 2
      assert timeline.failed_samples == 0
      assert artifact.measurements.counts.benchmark_probe_queries_excluded == 2
      assert artifact.measurements.counts.benchmark_control_queries_excluded == 10
      assert Enum.all?(artifact.invariants, & &1.pass)

      encoded = File.read!(output)
      refute encoded =~ config.database_url
      refute encoded =~ output
      refute encoded =~ "docket_bench_"

      refute encoded =~
               ~r/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i
    end

    test "blocked timeout artifacts preserve verified database cleanup" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-blocked-timeout-#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario blocked_vehicles --runs 4 --concurrency 2 --pool-size 1 --hold-ms 10 --sample-interval-ms 5 --max-samples 8 --probe-count 1 --timeout-ms 1 --output #{output})
               )

      assert {:error, message} = Docket.Benchmark.run(config)
      assert message =~ "results were written"

      suite = output |> File.read!() |> JSON.decode!()
      refute suite["success"]
      assert suite["kind"] == "benchmark_suite"
      assert [artifact] = suite["points"]
      refute artifact["success"]
      assert artifact["duration_us"] == nil
      assert artifact["cleanup"]["isolated_database_removed"]
      assert artifact["failure_stage"] == "setup_or_execution"
      assert artifact["error"] =~ "timed out before reaching its plateau"
    end

    test "comparative fairness scenarios preserve cohort samples" do
      scenarios = [
        {"mixed_service_times", ["--hold-ms", "10"], [:slow, :fast], 4},
        {"parked_wait_vs_blocking_wait", ["--hold-ms", "10"], [:blocking, :parked], 6},
        {"cyclic_vs_one_step", [], [:cyclic, :one_step], 4}
      ]

      for {scenario, extra, cohorts, expected_claims} <- scenarios do
        output =
          Path.join(
            System.tmp_dir!(),
            "docket-#{scenario}-#{System.unique_integer([:positive])}.json"
          )

        on_exit(fn -> File.rm(output) end)

        argv =
          ~w(--scenario #{scenario} --runs 4 --concurrency 2 --pool-size 2 --timeout-ms 10000 --output #{output}) ++
            extra

        assert {:ok, config} = Docket.Benchmark.parse(argv)
        assert {:ok, %{artifacts: [artifact]}} = Docket.Benchmark.run(config)
        assert artifact.success
        assert artifact.scenario == scenario
        assert artifact.measurements.collection.complete_sample_set

        assert artifact.measurements.collection.expected_ready_claim_samples ==
                 expected_claims

        assert Map.keys(artifact.measurements.cohorts) |> Enum.sort() == Enum.sort(cohorts)

        assert Enum.all?(artifact.measurements.cohorts, fn {_name, cohort} ->
                 cohort.runs == cohort.completed_runs and
                   cohort.activation_to_terminal_commit_offset_us.sample_count == cohort.runs and
                   cohort.activation_to_first_claim_offset_us.sample_count == cohort.runs and
                   cohort.first_claim_to_terminal_commit_us.sample_count == cohort.runs and
                   is_number(cohort.queue_share_of_median_percent)
               end)

        for label <- cohorts do
          assert is_number(artifact.headline[:"cohort_#{label}_activation_to_terminal_p50_us"])
          assert is_number(artifact.headline[:"cohort_#{label}_first_claim_to_terminal_p50_us"])
          assert is_number(artifact.headline[:"cohort_#{label}_queue_share_of_median_percent"])
        end

        assert Enum.all?(artifact.invariants, & &1.pass)
        assert artifact.cleanup.isolated_database_removed
      end
    end

    test "comparative fairness repetitions produce suite latency summaries" do
      points = [
        %{
          measurements: %{
            latency: comparative_suite_latency(10),
            cohorts: comparative_suite_cohorts(10)
          }
        },
        %{
          measurements: %{
            latency: comparative_suite_latency(20),
            cohorts: comparative_suite_cohorts(20)
          }
        }
      ]

      for scenario <- ~w(mixed_service_times parked_wait_vs_blocking_wait cyclic_vs_one_step) do
        summary = Docket.Benchmark.Postgres.suite_latency_summary(points, scenario)

        assert summary.burst_activation_to_first_commit_p50_us.sample_count == 2
        assert summary.burst_activation_to_first_commit_p50_us.median == 15.0
        assert summary.first_commit_to_terminal_p95_us.sample_count == 2
        assert summary.burst_activation_to_terminal_commit_p95_us.sample_count == 2

        assert summary.cohorts.slow.activation_to_terminal_p50_us.sample_count == 2
        assert summary.cohorts.slow.activation_to_terminal_p50_us.median == 150.0
        assert summary.cohorts.slow.first_claim_to_terminal_p50_us.sample_count == 2
        assert summary.cohorts.slow.queue_share_of_median_percent.median == 15.0
        assert summary.cohorts.fast.activation_to_terminal_p95_us.sample_count == 2
      end
    end

    test "warm repeated matrix writes raw NDJSON trials and one summary" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-matrix-bench-#{System.unique_integer([:positive])}.ndjson"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario smoke --runs 3 --warmup 1 --repetitions 2 --concurrency-matrix 1,2 --pool-size 2 --format ndjson --output #{output})
               )

      assert {:ok, %{artifact: suite, artifacts: trials}} = Docket.Benchmark.run(config)
      assert suite.success
      assert suite.matrix_point_count == 2
      assert suite.trial_count == 4
      assert length(trials) == 4
      assert Enum.all?(trials, & &1.success)
      assert Enum.all?(trials, &(&1.warmup.completed_runs == 1))

      Enum.each(trials, fn trial ->
        collection = trial.measurements.collection
        latency = trial.measurements.latency
        first = latency.burst_activation_to_first_commit_offset_us
        span = latency.first_commit_to_terminal_us
        terminal = latency.burst_activation_to_terminal_commit_offset_us

        assert collection.observed_first_commit_samples == 3
        assert collection.observed_first_to_terminal_pairs == 3
        assert collection.observed_terminal_commit_samples == 3
        assert collection.observed_completion_event_samples == 3
        assert collection.invalid_checkpoint_shapes == 0
        assert collection.invalid_terminal_shapes == 0
        assert collection.unknown_correlation_events == 0
        assert collection.pre_activation_work_events == 0
        assert first.sample_count == 3
        assert span.sample_count == 3
        assert terminal.sample_count == 3
        assert first.min >= 0
        assert span.min >= 0
        assert terminal.min >= 0
        assert first.max <= terminal.max
        assert span.max <= terminal.max
        assert trial.duration_us == terminal.max
        assert trial.schema_version == 4
        assert trial.scenario == "empty_one_step"
        assert is_number(trial.headline.throughput_per_second)
        assert trial.headline.activation_to_terminal_p95_us == terminal.p95
        assert trial.measurements.amplification.durable_run_rows == 3
        assert trial.measurements.latency.vehicle_claim_held_ms.sample_count == 3
        assert trial.cleanup.isolated_database_removed
      end)

      assert Enum.all?(suite.summary, fn cell ->
               cell.throughput_per_second.sample_count == 2 and
                 is_number(cell.throughput_per_second.median) and
                 not Map.has_key?(cell.throughput_per_second, :p95) and
                 cell.latency_across_repetitions.first_commit_to_terminal_p95_us.sample_count ==
                   2
             end)

      records =
        output
        |> File.stream!()
        |> Enum.map(fn line -> line |> JSON.decode!() end)

      assert length(records) == 5
      assert Enum.count(records, &(&1["record_type"] == "point")) == 4
      summary = List.last(records)
      assert summary["record_type"] == "suite_summary"
      assert summary["scenario"] == "empty_one_step"
      refute Map.has_key?(summary, "points")

      assert Enum.all?(summary["summary"], fn cell ->
               cell["throughput_per_second"]["sample_count"] == 2 and
                 cell["measured_duration_us"]["sample_count"] == 2
             end)

      encoded = File.read!(output)
      refute encoded =~ config.database_url
      refute encoded =~ output
      refute encoded =~ "docket_bench_"

      refute encoded =~
               ~r/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i
    end

    defp comparative_suite_latency(value) do
      %{
        burst_activation_to_first_commit_offset_us: %{p50: value, p95: value + 1},
        first_commit_to_terminal_us: %{p50: value + 2, p95: value + 3},
        burst_activation_to_terminal_commit_offset_us: %{p50: value + 4, p95: value + 5}
      }
    end

    defp comparative_suite_cohorts(value) do
      %{
        slow: %{
          activation_to_terminal_commit_offset_us: %{p50: value * 10, p95: value * 11},
          first_claim_to_terminal_commit_us: %{p50: value * 2},
          queue_share_of_median_percent: value * 1.0
        },
        fast: %{
          activation_to_terminal_commit_offset_us: %{p50: value * 20, p95: value * 21},
          first_claim_to_terminal_commit_us: %{p50: value},
          queue_share_of_median_percent: value * 2.0
        }
      }
    end
  end
end
