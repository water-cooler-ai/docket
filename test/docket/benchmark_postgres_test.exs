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
      assert artifact.measurements.collection.telemetry_checks_pass
      assert artifact.measurements.counts.ready_claims == 7
      assert artifact.measurements.counts.expired_claims == 5
      assert artifact.headline.ready_claims == 7
      assert is_number(artifact.headline.burst_start_to_claim_p50_us)
      assert is_number(artifact.headline.claim_query_p95_us)
      assert Enum.all?(artifact.invariants, & &1.pass)
      assert artifact.cleanup.isolated_database_removed

      environment = artifact.environment
      assert environment.postgres_settings["checkpoint_timeout"] != nil
      assert environment.postgres_settings["autovacuum"] != nil
      assert environment.postgres_settings["work_mem"] != nil
      assert environment.postgres_settings["shared_preload_libraries"] != nil
      assert environment.postgres_setting_details["checkpoint_timeout"].status == "available"
      assert is_list(environment.unavailable_postgres_settings)
      assert environment.pg_stat_statements.status in ["available", "unavailable"]
      refute environment.pg_stat_statements.query_statistics_captured
      assert environment.postgres_connection.transport in ["tcp", "unix_socket"]
      assert is_binary(environment.host.os.family)
      assert is_binary(environment.runtime.architecture)
      assert is_boolean(environment.container.detected)
      assert is_binary(environment.storage.class)
      assert is_binary(environment.storage.filesystem)
      assert is_binary(environment.storage.postgres_data_directory)

      amplification = artifact.measurements.amplification
      counters = amplification.postgres_database_counters_delta
      assert is_number(counters.xact_commit)
      assert Map.has_key?(counters, :deadlocks)
      assert Map.has_key?(counters, :temp_bytes)
      assert Map.has_key?(counters, :blk_read_time)
      assert Map.has_key?(counters, :active_time)

      contention = amplification.postgres_contention
      assert is_number(contention.before.activity.other_backends)
      assert is_number(contention.after.activity.active_waiting_backends)
      assert is_number(contention.before.locks.ungranted_lock_rows)
      assert is_number(contention.after.locks.backends_with_ungranted_locks)
      assert is_number(contention.gauge_delta.activity.active_backends)
      assert is_number(contention.gauge_delta.locks.lock_rows)
      assert contention.caveat =~ "miss transient"

      encoded = File.read!(output)
      decoded = JSON.decode!(encoded)
      assert is_map(decoded)
      assert decoded["kind"] == "benchmark_suite"
      assert decoded["schema_version"] == 5
      assert decoded["scenario"] == "claim_only"
      assert [point] = decoded["points"]
      assert point["schema_version"] == 5
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
      batches = artifact.measurements.batches.exact_global_counts
      assert batches.nonempty_scan_events >= 1
      assert batches.nonempty_scan_events <= config.concurrency
      assert batches.empty_scan_events <= config.concurrency
      assert batches.total_scan_events <= config.concurrency * 2

      assert artifact.measurements.counts.claim_query_samples ==
               batches.total_scan_events

      exact = artifact.measurements.collection.exact_global_counts
      assert exact.claim_attempt_events == config.runs
      assert exact.claimed_rows_from_scan_events == config.runs
      assert artifact.measurements.collection.telemetry_checks_pass
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
      exact = artifact.measurements.collection.exact_global_counts
      assert exact.expected_ready_claim_attempt_events == 0
      assert exact.ready_claim_attempt_events == 0
      assert exact.expected_expired_claim_attempt_events == 12
      assert exact.expired_claim_attempt_events == 12
      assert artifact.measurements.collection.telemetry_checks_pass
      assert Enum.all?(artifact.invariants, & &1.pass)
    end

    test "steady arrivals preserve the open-loop schedule and drain exact backlog" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-steady-arrival-#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario steady_arrival --duration 80ms --arrival-rate 50 --concurrency 2 --pool-size 2 --sample-interval-ms 5 --max-samples 8 --timeout-ms 10000 --output #{output})
               )

      assert config.runs == 4
      assert {:ok, %{artifact: suite, artifacts: [artifact]}} = Docket.Benchmark.run(config)
      assert artifact.success
      assert artifact.scenario == "steady_arrival"
      assert artifact.measurements.collection.telemetry_checks_pass

      steady = artifact.measurements.steady_arrival
      assert steady.offered_rate_per_second == 50.0
      assert is_number(steady.achieved_arrival_window_rate_per_second)
      assert is_number(steady.achieved_terminal_drain_rate_per_second)
      assert steady.retained_completion_lag_us.sample_count == 4

      exact = steady.exact_terminal_telemetry
      assert exact.boundary_accounting_exact
      assert exact.completed_in_arrival_window == steady.completed_by_arrival_window_end

      assert exact.completed_in_arrival_window + exact.due_outstanding_at_arrival_window_end ==
               4

      assert steady.due_outstanding_at_arrival_window_end ==
               4 - exact.completed_in_arrival_window

      assert steady.achieved_arrival_window_rate_per_second ==
               Float.round(exact.completed_in_arrival_window / 0.08, 3)

      window_sample = steady.boundary_samples.arrival_window_end
      assert window_sample.requested_boundary_offset_us == 80_000
      assert window_sample.observed_sample_start_offset_us == window_sample.offset_us
      assert window_sample.observed_sample_completion_offset_us >= window_sample.offset_us
      assert window_sample.sample_start_delay_us >= 0
      assert window_sample.sample_callback_duration_us >= 0

      assert window_sample.requested_to_sample_completion_us >=
               window_sample.sample_start_delay_us

      refute window_sample.exact_boundary_state
      assert window_sample.snapshot_scope =~ "later within the callback interval"
      assert steady.oldest_due_lag_growth_sample_span_us > 0
      assert steady.oldest_due_lag_growth_scope =~ "observed SQL sample starts"
      assert steady.backlog_growth_scope =~ "not instantaneous growth"
      assert steady.terminal_drain_duration_us == exact.last_terminal_checkpoint_offset_us
      assert steady.completion_poll_detection_delay_us >= 0

      assert abs(
               steady.completion_poll_detected_offset_us - steady.terminal_drain_duration_us -
                 steady.completion_poll_detection_delay_us
             ) <= 1

      assert steady.achieved_terminal_drain_rate_per_second ==
               Float.round(4 * 1_000_000 / steady.terminal_drain_duration_us, 3)

      post_detection = steady.boundary_samples.post_completion_detection
      assert post_detection.phase == "post_completion_detection"
      assert post_detection.metrics.completed_runs == 4
      assert post_detection.metrics.due_outstanding_runs == 0
      assert steady.timeline.retained_bucket_count <= 8
      assert steady.timeline.represented_sample_count == steady.timeline.raw_sample_count
      assert steady.timeline.forced_phase_sample_count == 3

      assert steady.backlog_state_at_window_end in [
               "drained_by_window_end",
               "outstanding_at_window_end"
             ]

      assert artifact.headline.offered_rate_per_second == 50.0
      assert artifact.headline.terminal_drain_duration_us == steady.terminal_drain_duration_us

      assert artifact.headline.completion_poll_detection_delay_us ==
               steady.completion_poll_detection_delay_us

      assert is_number(artifact.headline.retained_completion_lag_p95_us)
      assert Enum.all?(artifact.invariants, & &1.pass)
      assert artifact.cleanup.isolated_database_removed

      assert suite.concurrency_knee.data_status == "exploratory"
      assert suite.concurrency_knee.method.generic_safe_capacity_recommendation == "disabled"
      assert [pool] = suite.concurrency_knee.pools
      assert pool.status == "exploratory_only"
      assert pool.recommended_safe_concurrency == nil

      assert suite.summary
             |> hd()
             |> get_in([
               :latency_across_repetitions,
               :retained_completion_lag_p95_us,
               :sample_count
             ]) == 1
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
      assert artifact.measurements.blocked_vehicles.collection.telemetry_checks_pass
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
        {"cyclic_vs_one_step", [], [:cyclic, :one_step], :from_budget_yields}
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
        assert artifact.measurements.collection.telemetry_checks_pass

        expected_claims =
          if expected_claims == :from_budget_yields do
            config.runs + artifact.measurements.drain_fairness.budget_yields.total
          else
            expected_claims
          end

        assert artifact.measurements.collection.exact_global_counts.expected_ready_claim_attempt_events ==
                 expected_claims

        assert Map.keys(artifact.measurements.cohorts) |> Enum.sort() == Enum.sort(cohorts)

        assert Enum.all?(artifact.measurements.cohorts, fn {_name, cohort} ->
                 cohort.runs == cohort.completed_runs and
                   cohort.activation_to_terminal_commit_offset_us.sample_count == cohort.runs and
                   cohort.activation_to_first_claim_offset_us.sample_count == cohort.runs and
                   cohort.first_claim_to_terminal_commit_us.sample_count == cohort.runs and
                   cohort.normalized_slowdown.sample_count == cohort.runs and
                   cohort.terminal_rank_in_retained_sample.sample_count == cohort.runs and
                   cohort.terminal_rank_minus_staged_ordinal.sample_count == cohort.runs and
                   cohort.claims.claims_per_run.sample_count == cohort.runs and
                   cohort.sampling.complete_population and
                   is_number(cohort.activation_to_first_claim_offset_us.p95) and
                   is_number(cohort.activation_to_first_claim_offset_us.p99) and
                   is_number(cohort.activation_to_first_claim_offset_us.max) and
                   is_number(cohort.queue_share_of_median_percent)
               end)

        for label <- cohorts do
          assert is_number(artifact.headline[:"cohort_#{label}_activation_to_terminal_p50_us"])
          assert is_number(artifact.headline[:"cohort_#{label}_activation_to_first_claim_p95_us"])
          assert is_number(artifact.headline[:"cohort_#{label}_activation_to_first_claim_p99_us"])
          assert is_number(artifact.headline[:"cohort_#{label}_activation_to_first_claim_max_us"])
          assert is_number(artifact.headline[:"cohort_#{label}_first_claim_to_terminal_p50_us"])
          assert is_number(artifact.headline[:"cohort_#{label}_normalized_slowdown_p50_ratio"])

          assert is_number(
                   artifact.headline[:"cohort_#{label}_terminal_rank_in_retained_sample_p50"]
                 )

          assert is_number(artifact.headline[:"cohort_#{label}_queue_share_of_median_percent"])
        end

        total_terminal_rank_samples =
          artifact.measurements.cohorts
          |> Enum.reduce(0, fn {_label, cohort}, total ->
            total + cohort.terminal_rank_in_retained_sample.sample_count
          end)

        assert total_terminal_rank_samples == config.runs
        assert artifact.measurements.fairness.sampling.complete_population
        assert artifact.measurements.fairness.sampling.retained_correlation_samples == config.runs

        cond do
          scenario == "parked_wait_vs_blocking_wait" ->
            assert artifact.measurements.cohorts.parked.claims.retained_observations == 4

            assert artifact.measurements.cohorts.parked.claims.retained_subsequent_observations ==
                     2

            assert artifact.measurements.cohorts.parked.claims.retained_runs_with_subsequent_claims ==
                     2

            assert artifact.measurements.cohorts.parked.claims.activation_to_subsequent_claim_offset_us.sample_count ==
                     2

            assert artifact.measurements.cohorts.parked.claims.subsequent_ready_age_at_scan_start_ms.sample_count ==
                     2

            assert artifact.headline.cohort_parked_retained_subsequent_claims == 2
            assert is_number(artifact.headline.cohort_parked_subsequent_ready_age_p95_ms)

            assert artifact.measurements.cohorts.blocking.claims.retained_subsequent_observations ==
                     0

          scenario == "cyclic_vs_one_step" ->
            assert artifact.measurements.cohorts.cyclic.claims.retained_subsequent_observations >
                     0

            assert artifact.measurements.cohorts.one_step.claims.retained_subsequent_observations ==
                     0

          true ->
            assert Enum.all?(artifact.measurements.cohorts, fn {_label, cohort} ->
                     cohort.claims.retained_subsequent_observations == 0
                   end)
        end

        if scenario == "mixed_service_times" do
          assert is_number(
                   artifact.measurements.fairness.fast_to_slow_normalized_slowdown_p50_ratio
                 )

          assert is_number(
                   artifact.measurements.fairness.fast_to_slow_normalized_slowdown_p95_ratio
                 )

          assert is_number(artifact.headline.fast_to_slow_normalized_slowdown_p50_ratio)
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

        exact = collection.exact_global_counts
        retained = collection.retained_per_run_shape_evidence
        assert exact.terminal_checkpoint_events == 3
        assert exact.completion_events == 3
        assert exact.pre_activation_work_events == 0
        assert retained.runs_with_first_checkpoint == 3
        assert retained.first_to_terminal_pairs == 3
        assert retained.runs_with_terminal_checkpoint == 3
        assert retained.runs_with_completion_event == 3
        assert retained.invalid_checkpoint_shapes == 0
        assert retained.invalid_terminal_checkpoint_shapes == 0
        assert retained.invalid_completion_event_shapes == 0
        assert retained.unindexed_or_unknown_correlation_events == 0
        assert collection.full_population_uniqueness.status == "available"
        assert collection.full_population_uniqueness.unique_run_count == 3
        assert first.sample_count == 3
        assert span.sample_count == 3
        assert terminal.sample_count == 3
        assert first.min >= 0
        assert span.min >= 0
        assert terminal.min >= 0
        assert first.max <= terminal.max
        assert span.max <= terminal.max
        assert trial.duration_us == terminal.max
        assert trial.schema_version == 5
        assert trial.scenario == "empty_one_step"
        assert is_number(trial.headline.throughput_per_second)
        assert trial.headline.completion_event_count == 3
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
