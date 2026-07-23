if Code.ensure_loaded?(Docket.Postgres.Telemetry) do
  defmodule Docket.Postgres.TelemetryTest do
    use ExUnit.Case, async: true

    test "metric metadata retains only bounded ClaimPolicy implementation labels" do
      metadata = %{
        implementation: Docket.Postgres.ClaimPolicy.Legacy,
        claim_policy: Docket.Postgres.ClaimPolicy.Legacy,
        result: :ok,
        contention_phase: :none,
        source: :scheduled,
        run_id: "run-1",
        tenant_id: "tenant-1",
        implementation_state: %{unbounded: "state"}
      }

      assert Docket.Postgres.Telemetry.metric_metadata(
               [:docket, :postgres, :claim_policy, :admission],
               metadata
             ) == %{
               implementation: Docket.Postgres.ClaimPolicy.Legacy,
               result: :ok,
               contention_phase: :none
             }

      assert Docket.Postgres.Telemetry.metric_metadata(
               [:docket, :postgres, :dispatcher, :poll],
               metadata
             ) == %{
               claim_policy: Docket.Postgres.ClaimPolicy.Legacy,
               result: :ok,
               source: :scheduled
             }

      assert Docket.Postgres.Telemetry.metric_metadata(
               [:docket, :postgres, :admission, :release],
               Map.put(metadata, :reason, :terminal)
             ) == %{reason: :terminal}
    end

    test "delegates core events to the core projection" do
      assert Docket.Postgres.Telemetry.metric_metadata(
               [:docket, :node, :execution],
               %{result: :ok, run_id: "run-1"}
             ) == %{result: :ok}
    end
  end
end
