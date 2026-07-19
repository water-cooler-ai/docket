if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.FairRotationProofMatrixTest do
    use ExUnit.Case, async: true

    alias Docket.Test.ConcurrentAdmissionHarness.FairRotationOracle

    test "small fixed-ring target-only traces satisfy the theorem across cursor phases" do
      for ring_count <- 1..5,
          scan_budget <- 1..4,
          lock_failures <- 0..2,
          target_index <- 1..ring_count do
        ring =
          for index <- 1..ring_count do
            partition = if index == target_index, do: "target", else: "dormant-#{index}"
            {index * 10, partition}
          end

        initial_cursors = [0 | Enum.map(ring, &elem(&1, 0))]

        for initial_cursor <- initial_cursors do
          trace =
            target_only_trace(
              ring,
              initial_cursor,
              scan_budget,
              lock_failures,
              "target"
            )

          result =
            FairRotationOracle.assert_trace!(trace,
              target: "target",
              cohort: ["target"],
              ring: ring,
              scan_budget: scan_budget,
              quantum: 1,
              lock_failures: lock_failures
            )

          assert result.observed_other_grants == 0
          assert result.observed_other_outcomes == 0
          assert result.observed_scan_calls <= result.scan_calls
        end
      end
    end

    test "L=2 can reach the grant, outcome, and demand-aware call bounds exactly" do
      ring = [{10, "a"}, {20, "b"}, {30, "target"}]

      trace = [
        event(1, 0, 10, "a", :grant, 2, 1),
        event(2, 10, 20, "b", :grant, 2, 1),
        event(3, 20, 30, "target", :lock_skip, 0, 0),
        event(4, 30, 10, "a", :grant, 2, 1),
        event(5, 10, 20, "b", :grant, 2, 1),
        event(6, 20, 30, "target", :stale, 0, 0),
        event(7, 30, 10, "a", :grant, 2, 1),
        event(8, 10, 20, "b", :grant, 2, 1),
        event(9, 20, 30, "target", :grant, 2, 1)
      ]

      opts = [
        target: "target",
        cohort: ["a", "b", "target"],
        ring: ring,
        scan_budget: 1,
        quantum: 2,
        lock_failures: 2
      ]

      result = FairRotationOracle.assert_trace!(trace, opts)

      assert result.observed_other_grants == 6
      assert result.observed_other_grants == result.other_grants
      assert result.observed_other_outcomes == 12
      assert result.observed_other_outcomes == result.other_outcomes
      assert result.observed_scan_calls == 9
      assert result.observed_scan_calls == result.scan_calls

      assert_raise ArgumentError, ~r/target failed 2 inspections; L allows 1/, fn ->
        FairRotationOracle.assert_trace!(trace, Keyword.put(opts, :lock_failures, 1))
      end
    end

    defp target_only_trace(ring, initial_cursor, scan_budget, lock_failures, target) do
      Stream.unfold({initial_cursor, 0, 1}, fn
        {_cursor, target_failures, _visit} when target_failures > lock_failures ->
          nil

        {cursor, target_failures, visit} ->
          {cursor_after, partition} = next_position(ring, cursor)

          {disposition, outcomes, epoch_delta, next_target_failures} =
            cond do
              partition != target ->
                {:empty, 0, 0, target_failures}

              target_failures < lock_failures ->
                {:lock_skip, 0, 0, target_failures + 1}

              true ->
                {:grant, 1, 1, target_failures + 1}
            end

          call = div(visit - 1, scan_budget) + 1
          ordinal = rem(visit - 1, scan_budget) + 1

          event = %{
            call: call,
            ordinal: ordinal,
            cursor_before: cursor,
            cursor_after: cursor_after,
            demand: 1,
            partition: partition,
            disposition: disposition,
            outcomes: outcomes,
            epoch_delta: epoch_delta,
            committed: true
          }

          {event, {cursor_after, next_target_failures, visit + 1}}
      end)
      |> Enum.to_list()
    end

    defp next_position(ring, cursor) do
      Enum.find(ring, fn {position, _partition} -> position > cursor end) || hd(ring)
    end

    defp event(call, cursor_before, cursor_after, partition, disposition, outcomes, epoch_delta) do
      %{
        call: call,
        ordinal: 1,
        cursor_before: cursor_before,
        cursor_after: cursor_after,
        demand: 2,
        partition: partition,
        disposition: disposition,
        outcomes: outcomes,
        epoch_delta: epoch_delta,
        committed: true
      }
    end
  end
end
