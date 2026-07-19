if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.FairRotationOracleTest do
    use ExUnit.Case, async: true

    alias Docket.Test.ConcurrentAdmissionHarness.FairRotationOracle

    test "uses the demand-aware scan-call bound" do
      bounds =
        FairRotationOracle.bounds!(
          target: "low",
          cohort: ["hot", "low"],
          ring: [{10, "hot"}, {40, "low"}],
          scan_budget: 2,
          quantum: 1,
          lock_failures: 0
        )

      assert bounds.other_grants == 1
      assert bounds.other_outcomes == 1
      assert bounds.scan_calls == 2
    end

    test "accepts a trace at every frozen bound" do
      trace = [
        event(1, 1, 0, 10, 2, "hot", :grant, 2, 1),
        event(2, 1, 10, 40, 1, "dormant", :stale, 0, 0),
        event(2, 2, 40, 90, 1, "low", :lock_skip, 0, 0),
        event(3, 1, 90, 10, 2, "hot", :grant, 2, 1),
        event(4, 1, 10, 40, 1, "dormant", :empty, 0, 0),
        event(4, 2, 40, 90, 1, "low", :grant, 1, 1)
      ]

      result =
        FairRotationOracle.assert_trace!(trace,
          target: "low",
          cohort: ["hot", "low"],
          ring: [{10, "hot"}, {40, "dormant"}, {90, "low"}],
          scan_budget: 2,
          quantum: 2,
          lock_failures: 1
        )

      assert result.observed_other_grants == 2
      assert result.other_grants == 2
      assert result.observed_other_outcomes == 4
      assert result.other_outcomes == 4
      assert result.observed_scan_calls == 4
      assert result.scan_calls == 4
    end

    test "rejects synthetic service credit for an unsuccessful inspection" do
      trace = [
        event(1, 1, 0, 10, 1, "dormant", :stale, 0, 1),
        event(1, 2, 10, 90, 1, "low", :grant, 1, 1)
      ]

      assert_raise ArgumentError, ~r/cannot return outcomes or advance admission_epoch/, fn ->
        FairRotationOracle.assert_trace!(trace,
          target: "low",
          cohort: ["low"],
          ring: [{10, "dormant"}, {90, "low"}],
          scan_budget: 2,
          quantum: 1,
          lock_failures: 0
        )
      end
    end

    test "accepts sparse absolute positions and repeated wrap when H is less than S" do
      trace = [
        event(1, 1, 0, 50, 1, "low", :lock_skip, 0, 0),
        event(1, 2, 50, 50, 1, "low", :grant, 1, 1)
      ]

      result =
        FairRotationOracle.assert_trace!(trace,
          target: "low",
          cohort: ["low"],
          ring: [{50, "low"}],
          scan_budget: 2,
          quantum: 1,
          lock_failures: 1
        )

      assert result.observed_scan_calls == 1
      assert result.observed_other_grants == 0
    end

    test "rejects skipped or repeated cursor positions" do
      trace = [
        event(1, 1, 0, 10, 1, "hot", :grant, 1, 1),
        event(2, 1, 0, 10, 1, "hot", :grant, 1, 1),
        event(3, 1, 10, 40, 1, "low", :grant, 1, 1)
      ]

      assert_raise ArgumentError, ~r/scan cursor is not contiguous/, fn ->
        FairRotationOracle.assert_trace!(trace,
          target: "low",
          cohort: ["hot", "low"],
          ring: [{10, "hot"}, {40, "low"}],
          scan_budget: 1,
          quantum: 1,
          lock_failures: 0
        )
      end
    end

    test "rejects an early stop when a grant did not fill demand" do
      trace = [
        event(1, 1, 0, 10, 2, "hot", :grant, 1, 1),
        event(2, 1, 10, 40, 1, "dormant", :stale, 0, 0),
        event(2, 2, 40, 90, 1, "low", :grant, 1, 1)
      ]

      assert_raise ArgumentError, ~r/unfilled scan call 1/, fn ->
        FairRotationOracle.assert_trace!(trace,
          target: "low",
          cohort: ["hot", "low"],
          ring: [{10, "hot"}, {40, "dormant"}, {90, "low"}],
          scan_budget: 2,
          quantum: 1,
          lock_failures: 0
        )
      end
    end

    test "database windows reject churn, rollback, and instrumentation gaps" do
      window = database_window()
      opts = database_opts()

      churned =
        Map.put(window, :ring_snapshots, [
          [{10, "low"}],
          [{20, "low"}]
        ])

      assert_raise ArgumentError, ~r/changed its frozen ring/, fn ->
        FairRotationOracle.assert_database_trace!(churned, opts)
      end

      rolled_back = put_in(window, [:calls, Access.at(0), :committed], false)

      assert_raise ArgumentError, ~r/rolled-back call/, fn ->
        FairRotationOracle.assert_database_trace!(rolled_back, opts)
      end

      instrumentation_gap =
        update_in(window, [:calls, Access.at(0), :rows, Access.at(0)], fn inspection ->
          %{inspection | outcome_count: 0}
        end)

      assert_raise ArgumentError, ~r/outcomes without inspection evidence/, fn ->
        FairRotationOracle.assert_database_trace!(instrumentation_gap, opts)
      end
    end

    defp database_window do
      identity = %{
        call_token: "call-1",
        transaction_id: 1,
        demand: 1,
        visit_ordinal: 1
      }

      inspection =
        Map.merge(identity, %{
          row_kind: "inspection",
          cursor_before: 0,
          cursor_after: 10,
          ring_position: 10,
          scope_key: "low",
          disposition: "grant",
          outcome_count: 1,
          epoch_delta: 1
        })

      outcome =
        Map.merge(identity, %{
          row_kind: "outcome",
          outcome_ordinal: 1
        })

      %{
        calls: [%{rows: [inspection, outcome], committed: true}],
        ring_snapshots: [[{10, "low"}], [{10, "low"}]],
        policy_snapshots: [%{engine: :tenant_fair}, %{engine: :tenant_fair}],
        epoch_snapshots: [%{"low" => 0}, %{"low" => 1}],
        target_admissible_before_calls: [true],
        repair_active: false,
        instrumentation_complete: true
      }
    end

    defp database_opts do
      [
        target: "low",
        cohort: ["low"],
        ring: [{10, "low"}],
        scan_budget: 1,
        quantum: 1,
        lock_failures: 0
      ]
    end

    defp event(
           call,
           ordinal,
           cursor_before,
           cursor_after,
           demand,
           partition,
           disposition,
           outcomes,
           epoch_delta
         ) do
      %{
        call: call,
        ordinal: ordinal,
        cursor_before: cursor_before,
        cursor_after: cursor_after,
        demand: demand,
        partition: partition,
        disposition: disposition,
        outcomes: outcomes,
        epoch_delta: epoch_delta,
        committed: true
      }
    end
  end
end
