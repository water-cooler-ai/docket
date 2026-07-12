if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.BenchmarkMixedSlotFairnessTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    test "fifty-fifty mixed work can fill every slot and reports fast tail slowdown" do
      artifact = run_point(50, 2, "balanced")
      evidence = artifact.measurements.fairness.slow_slot_occupancy
      fast_tail = artifact.measurements.fairness.fast_cohort_tail_slowdown

      assert artifact.workload.graph_shape.requested_slow_percent == 50
      assert artifact.workload.graph_shape.actual_slow_percent == 50.0
      assert artifact.workload.graph_shape.slow_runs == 2
      assert artifact.workload.graph_shape.fast_runs == 2
      assert artifact.workload.graph_shape.slow_cohort_has_enough_runs_to_fill_all_slots

      assert evidence.slow_runs == 2
      assert evidence.fast_runs == 2
      assert evidence.configured_slots == 2
      assert evidence.maximum_simultaneous_slow_runs_from_cohort_size == 2
      assert evidence.maximum_slow_slot_occupancy_percent_from_cohort_size == 100.0
      assert evidence.slow_cohort_has_enough_runs_to_fill_all_slots
      assert evidence.observed_simultaneous_slow_slot_occupancy == "not_instrumented"

      assert is_number(fast_tail.normalized_slowdown_p50_ratio)
      assert is_number(fast_tail.normalized_slowdown_p95_ratio)
      assert is_number(fast_tail.activation_to_first_claim_p95_us)
      assert is_number(fast_tail.activation_to_terminal_p95_us)
      assert is_number(fast_tail.first_claim_to_terminal_p95_us)

      refute Enum.any?(artifact.warnings, &String.contains?(&1, "cannot occupy every slot"))
      refute artifact.measurements.fairness.paired_all_fast_control.executed
    end

    test "undersized slow cohorts emit explicit all-slot limitation evidence" do
      artifact = run_point(25, 4, "undersized")
      evidence = artifact.measurements.fairness.slow_slot_occupancy

      refute evidence.slow_cohort_has_enough_runs_to_fill_all_slots
      assert evidence.slow_runs == 1
      assert evidence.configured_slots == 4
      assert evidence.maximum_simultaneous_slow_runs_from_cohort_size == 1
      assert evidence.maximum_slow_slot_occupancy_percent_from_cohort_size == 25.0

      assert Enum.any?(artifact.warnings, fn warning ->
               warning =~ "cannot occupy every slot" and warning =~ "--slow-percent"
             end)
    end

    defp run_point(slow_percent, concurrency, suffix) do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-mixed-slots-#{suffix}-#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario mixed_service_times --runs 4 --concurrency #{concurrency} --pool-size 2 --hold-ms 10 --slow-percent #{slow_percent} --timeout-ms 30000 --output #{output})
               )

      assert {:ok, %{artifacts: [artifact]}} = Docket.Benchmark.run(config)
      assert artifact.success
      assert Enum.all?(artifact.invariants, & &1.pass)
      assert artifact.cleanup.isolated_database_removed
      artifact
    end
  end
end
