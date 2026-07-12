if Code.ensure_loaded?(Docket.Benchmark.Postgres.Scenarios.SteadyArrival) do
  defmodule Docket.BenchmarkSteadyArrivalTest do
    use ExUnit.Case, async: true

    alias Docket.Benchmark.Postgres.Scenarios.SteadyArrival

    test "labels delayed SQL boundary snapshots without claiming exact boundary state" do
      sample = %{
        offset_us: 82_500,
        kind: :phase,
        metrics: %{completed_runs: 4, sampler_probe_callback_duration_us: 1_250}
      }

      annotated =
        SteadyArrival.annotate_boundary_sample(sample, 80_000, "arrival_window_end")

      assert annotated.requested_boundary_offset_us == 80_000
      assert annotated.observed_sample_start_offset_us == 82_500
      assert annotated.observed_sample_completion_offset_us == 83_750
      assert annotated.sample_start_delay_us == 2_500
      assert annotated.sample_callback_duration_us == 1_250
      assert annotated.requested_to_sample_completion_us == 3_750

      assert annotated.observation_interval_us == %{
               start_offset_us: 82_500,
               end_offset_us: 83_750
             }

      assert annotated.delay_status == "delayed"
      refute annotated.exact_boundary_state
      assert annotated.snapshot_scope =~ "later within the callback interval"
      assert annotated.snapshot_scope =~ "not the exact requested boundary"
      assert annotated.metrics.completed_runs == 4
    end

    test "oldest-due slope uses the actual observed sample span" do
      assert SteadyArrival.observed_sample_slope(1, 11, 50_000) == 200.0
      assert SteadyArrival.observed_sample_slope(1, 11, 0) == nil
    end
  end
end
