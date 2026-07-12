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

      assert {:ok, %{artifact: artifact}} = Docket.Benchmark.run(config)
      assert artifact.success
      assert artifact.workload.ready_count == 7
      assert artifact.workload.expired_count == 5
      assert artifact.measurements.collection.complete_sample_set
      assert artifact.measurements.counts.ready_claims == 7
      assert artifact.measurements.counts.expired_claims == 5
      assert Enum.all?(artifact.invariants, & &1.pass)
      assert artifact.cleanup.isolated_database_removed

      encoded = File.read!(output)
      decoded = JSON.decode!(encoded)
      assert is_map(decoded)
      assert decoded["schema_version"] == 3
      assert decoded["scenario"] == "claim_only"
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

      assert {:ok, %{artifact: artifact}} = Docket.Benchmark.run(config)
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

      assert {:ok, %{artifact: artifact}} = Docket.Benchmark.run(config)
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
        assert trial.schema_version == 3
        assert trial.scenario == "empty_one_step"
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
  end
end
