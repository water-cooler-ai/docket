defmodule Docket.BenchmarkObserverTest do
  use ExUnit.Case, async: false

  test "observer ABBA is opt-in, smoke-only, and expands every base point in order" do
    assert {:ok, config} =
             Docket.Benchmark.parse(
               ~w(--scenario smoke --observer-abba --repetitions 2 --concurrency-matrix 1,2 --pool-size 3)
             )

    assert config.observer_abba
    points = Docket.Benchmark.plan(config)
    assert length(points) == 16

    Enum.each(Enum.chunk_every(points, 4), fn abba ->
      assert Enum.map(abba, & &1.observer_mode) == [
               "bounded_instrumented",
               "counters_only_control",
               "counters_only_control",
               "bounded_instrumented"
             ]

      assert Enum.map(abba, & &1.observer_position) == [1, 2, 3, 4]
      assert Enum.map(abba, & &1.observer_pair) == [1, 1, 2, 2]

      assert abba
             |> Enum.map(&{&1.concurrency, &1.pool_size, &1.repetition})
             |> Enum.uniq()
             |> length() == 1
    end)

    assert {:error, message} =
             Docket.Benchmark.parse(~w(--scenario claim_only --observer-abba))

    assert message =~ "only for smoke/empty_one_step"
  end

  test "counters-only control retains exact global counts and no per-run distributions" do
    collector =
      Docket.Benchmark.Collector.start(["run-a"], mode: :counters_only_control)

    now = DateTime.utc_now()

    checkpoint = %Docket.Event{
      run_id: "run-a",
      seq: 2,
      type: :checkpoint_committed,
      step: 1,
      timestamp: now,
      metadata: %{"checkpoint_type" => "run_completed"}
    }

    completed = %Docket.Event{
      run_id: "run-a",
      seq: 3,
      type: :run_completed,
      step: 1,
      timestamp: now
    }

    run = %Docket.Run{id: "run-a"}
    Docket.Telemetry.emit_events(run, [checkpoint, completed])
    Docket.Telemetry.emit_events(run, [checkpoint, completed])

    assert Docket.Benchmark.Collector.count(collector, [:docket, :run, :completed]) == 2

    assert Docket.Benchmark.Collector.count(
             collector,
             [:docket, :checkpoint, :committed],
             %{checkpoint_type: "run_completed"}
           ) == 2

    assert %{
             capture_mode: "counters_only_control",
             observed_events: 4,
             retained_event_samples: 0,
             exact_counters: true,
             distribution_sketch: "none",
             histogram_scope: "none",
             max_samples_per_event: 0,
             sampled_correlations: 0,
             correlation_correctness_scope: "exact_global_counts_without_per_run_shape_proof"
           } = Docket.Benchmark.Collector.stats(collector)

    snapshot = Docket.Benchmark.Collector.stop(collector)
    assert Docket.Benchmark.Collector.sampled_events(snapshot) == []

    assert Docket.Benchmark.Collector.observation_count(
             snapshot,
             [:docket, :run, :completed]
           ) == 2

    assert Docket.Benchmark.Collector.unique_count(snapshot, [:docket, :run, :completed]) ==
             :unavailable

    assert Docket.Benchmark.Collector.full_population_unique_count(
             snapshot,
             [:docket, :run, :completed]
           ) == {:unavailable, :bounded_correlation_sample}

    assert Docket.Benchmark.Collector.uniqueness_scope(snapshot) == :bounded_correlation_sample

    summary = Docket.Benchmark.Collector.correlation_summary(snapshot)
    assert summary.completion_count_frequencies == %{}
    assert summary.sampled_expected == 0
    refute summary.full_population_shape_coverage
  end

  if Code.ensure_loaded?(Docket.Benchmark.Postgres) do
    test "ordinary bounded collector points are not presented as ABBA trials" do
      points = [
        trial("bounded_instrumented", 1, 1, 110, 90.0),
        trial("bounded_instrumented", 1, 1, 100, 100.0)
      ]

      points =
        Enum.with_index(points, 1)
        |> Enum.map(fn {point, repetition} ->
          point
          |> put_in([:point, :repetition], repetition)
          |> Map.put(:observer_control, %{
            enabled: false,
            design: "not_requested",
            mode: "bounded_instrumented"
          })
        end)

      output = points |> Docket.Benchmark.Console.lines() |> Enum.join("\n")

      assert output =~ "2/2 trials valid"
      assert output =~ "95.0 runs/s median"
      refute output =~ "observer trials"
      refute output =~ "Observer ABBA raw deltas"
    end

    test "suite reports raw paired observer deltas without mixing controls into normal summaries" do
      artifacts = [
        trial("bounded_instrumented", 1, 1, 110, 90.0),
        trial("counters_only_control", 2, 1, 100, 100.0),
        trial("counters_only_control", 3, 2, 102, 100.0),
        trial("bounded_instrumented", 4, 2, 108, 95.0)
      ]

      suite = Docket.Benchmark.Postgres.suite_summary_payload(artifacts)

      assert suite.trial_count == 4
      assert suite.expected_repetitions == 1
      assert [cell] = suite.summary
      assert cell.repetitions == 1
      assert cell.observer_trial_count == 4
      assert cell.reported_instrumented_trial_count == 2
      assert cell.throughput_per_second.sample_count == 2
      assert cell.throughput_per_second.median == 92.5

      control = suite.observer_effect_control
      assert control.enabled
      assert control.status == "complete"
      assert control.design == "ABBA"
      assert control.delta_direction == "instrumented_minus_control"
      assert control.pair_count == 2
      assert control.valid_pair_count == 2
      assert control.interpretation =~ "not a causal correction"
      assert control.sampler_scope =~ "Not controlled"

      assert [observer_cell] = control.cells
      assert observer_cell.status == "complete"
      assert observer_cell.duration_instrumented_minus_control_us.median == 8.0

      assert observer_cell.duration_instrumented_minus_control_percent.min == 5.882
      assert observer_cell.duration_instrumented_minus_control_percent.max == 10.0

      assert observer_cell.throughput_instrumented_minus_control_per_second.median == -7.5
      assert observer_cell.throughput_instrumented_minus_control_percent.min == -10.0
      assert observer_cell.throughput_instrumented_minus_control_percent.max == -5.0

      assert Enum.map(observer_cell.pairs, & &1.order) == [
               ["bounded_instrumented", "counters_only_control"],
               ["counters_only_control", "bounded_instrumented"]
             ]
    end

    test "console reports A trials normally and renders ABBA deltas separately" do
      artifacts = [
        trial("bounded_instrumented", 1, 1, 110, 90.0),
        trial("counters_only_control", 2, 1, 100, 100.0),
        trial("counters_only_control", 3, 2, 102, 100.0),
        trial("bounded_instrumented", 4, 2, 108, 95.0)
      ]

      text = artifacts |> Docket.Benchmark.Console.lines() |> Enum.join("\n")

      assert text =~ "4/4 observer trials valid"
      assert text =~ "Ordinary medians use bounded-instrumented A trials only"
      assert text =~ "2/2 valid · 92.5 runs/s median"
      assert text =~ "Observer ABBA raw deltas (bounded instrumented - counters-only control)"
      assert text =~ "2/2 pairs valid · throughput delta median -7.5 runs/s (-7.5%)"
      assert text =~ "duration delta median 8 us (7.9%)"
      refute text =~ "97.5 runs/s median"
    end

    @tag :postgres
    test "live smoke ABBA preserves correctness while controls retain no distributions" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-observer-abba-#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario smoke --observer-abba --runs 4 --concurrency 2 --pool-size 2 --output #{output})
               )

      assert {:ok, %{artifact: suite, artifacts: artifacts}} = Docket.Benchmark.run(config)
      assert length(artifacts) == 4
      assert Enum.all?(artifacts, & &1.success)
      assert Enum.all?(artifacts, & &1.cleanup.isolated_database_removed)

      assert Enum.map(artifacts, & &1.point.observer_mode) == [
               "bounded_instrumented",
               "counters_only_control",
               "counters_only_control",
               "bounded_instrumented"
             ]

      {controls, instrumented} =
        Enum.split_with(artifacts, fn artifact ->
          artifact.point.observer_mode == "counters_only_control"
        end)

      assert length(controls) == 2
      assert length(instrumented) == 2

      assert Enum.all?(controls, fn artifact ->
               observer = artifact.measurements.collection.observer

               observer.capture_mode == "counters_only_control" and
                 observer.retained_event_samples == 0 and
                 observer.distribution_sketch == "none" and
                 artifact.measurements.collection.telemetry_checks_pass and
                 artifact.measurements.collection.exact_global_counts.completion_events == 4 and
                 artifact.measurements.collection.full_population_uniqueness.status ==
                   "unavailable" and
                 artifact.measurements.collection.retained_distribution_samples.completion_event_offsets ==
                   0 and
                 artifact.measurements.latency.burst_activation_to_terminal_commit_offset_us.sample_count ==
                   0
             end)

      assert Enum.all?(instrumented, fn artifact ->
               artifact.measurements.collection.observer.capture_mode ==
                 "bounded_streaming_reservoir" and
                 artifact.measurements.collection.full_population_uniqueness.status ==
                   "available" and
                 artifact.measurements.collection.retained_distribution_samples.completion_event_offsets ==
                   4
             end)

      assert suite.observer_effect_control.enabled
      assert suite.observer_effect_control.status == "complete"
      assert suite.observer_effect_control.pair_count == 2
      assert suite.observer_effect_control.valid_pair_count == 2
      assert [cell] = suite.summary
      assert cell.observer_trial_count == 4
      assert cell.reported_instrumented_trial_count == 2
      assert cell.throughput_per_second.sample_count == 2
    end

    defp trial(mode, position, pair, duration_us, throughput) do
      latency = %{
        burst_activation_to_first_commit_offset_us: distribution(10),
        first_commit_to_terminal_us: distribution(5),
        burst_activation_to_terminal_commit_offset_us: distribution(duration_us)
      }

      capture_mode =
        if mode == "bounded_instrumented",
          do: "bounded_streaming_reservoir",
          else: "counters_only_control"

      %{
        success: true,
        scenario: "empty_one_step",
        point: %{
          concurrency: 2,
          pool_size: 3,
          repetition: 1,
          observer_mode: mode,
          observer_position: position,
          observer_pair: pair
        },
        parameters: %{repetitions: 1},
        duration_us: duration_us,
        measurements: %{
          throughput_per_second: throughput,
          latency: latency,
          collection: %{observer: %{capture_mode: capture_mode}}
        },
        observer_control: %{
          enabled: true,
          design: "ABBA",
          mode: mode,
          position: position,
          pair: pair,
          collector_capture_mode: capture_mode
        }
      }
    end

    defp distribution(value),
      do: %{
        unit: "us",
        sample_count: 10,
        min: value,
        p50: value,
        p95: value,
        p99: value,
        max: value,
        mean: value * 1.0
      }
  end
end
