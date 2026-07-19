if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.FairRotationAdversarialFalsificationTest do
    use ExUnit.Case, async: true

    alias Docket.Test.FairRotationAdversarialVerifier

    test "accepts a complete database-sequenced trace with independent commit and outcome evidence" do
      trace = [
        event(40, 1, 50, 50, "target", :lock_skip, [], 0),
        event(41, 1, 50, 50, "target", :grant, ["target-run"], 1)
      ]

      result =
        FairRotationAdversarialVerifier.assert_trace!(
          trace,
          opts(lock_failures: 1, committed_call_sequences: [40, 41])
        )

      assert result.observed_scan_calls == 2
      assert result.observed_other_grants == 0
    end

    test "rejects caller-reordered calls even when H=1 makes every cursor edge identical" do
      trace = [
        event(41, 1, 50, 50, "target", :grant, ["target-run"], 1),
        event(40, 1, 50, 50, "target", :lock_skip, [], 0)
      ]

      assert_raise ArgumentError, ~r/call sequence is reordered or incomplete/, fn ->
        FairRotationAdversarialVerifier.assert_trace!(trace, opts(lock_failures: 1))
      end
    end

    test "rejects an omitted full-wrap unsuccessful call that cursor and epoch snapshots cannot see" do
      trace = [
        event(40, 1, 50, 50, "target", :lock_skip, [], 0),
        event(42, 1, 50, 50, "target", :grant, ["target-run"], 1)
      ]

      assert_raise ArgumentError, ~r/call sequence is reordered or incomplete/, fn ->
        FairRotationAdversarialVerifier.assert_trace!(
          trace,
          opts(
            lock_failures: 2,
            committed_call_sequences: [40, 41, 42]
          )
        )
      end
    end

    test "rejects a fabricated outcome absent from independent durable evidence" do
      trace = [event(40, 1, 50, 50, "target", :grant, ["fabricated-run"], 1)]

      assert_raise ArgumentError,
                   ~r/do not exactly match independent durable outcome evidence/,
                   fn ->
                     FairRotationAdversarialVerifier.assert_trace!(
                       trace,
                       opts(durable_outcome_ids: [])
                     )
                   end
    end

    test "rejects an inflated P that would weaken all three bounds" do
      trace = [
        event(40, 1, 0, 10, "hot", :grant, ["hot-run"], 1),
        event(41, 1, 10, 90, "target", :grant, ["target-run"], 1)
      ]

      assert_raise ArgumentError, ~r/declared cohort inflates or omits/, fn ->
        FairRotationAdversarialVerifier.assert_trace!(
          trace,
          opts(
            cohort: ["hot", "dormant", "target"],
            ring: [{10, "hot"}, {40, "dormant"}, {90, "target"}],
            committed_call_sequences: [40, 41],
            durable_outcome_ids: ["hot-run", "target-run"]
          )
        )
      end
    end

    test "delegates the L+1 falsification to the numeric oracle" do
      trace = [
        event(40, 1, 50, 50, "target", :lock_skip, [], 0),
        event(41, 1, 50, 50, "target", :stale, [], 0),
        event(42, 1, 50, 50, "target", :grant, ["target-run"], 1)
      ]

      assert_raise ArgumentError, ~r/target failed 2 inspections; L allows 1/, fn ->
        FairRotationAdversarialVerifier.assert_trace!(
          trace,
          opts(
            lock_failures: 1,
            committed_call_sequences: [40, 41, 42]
          )
        )
      end
    end

    test "rejects caller-declared commit without matching independent commit evidence" do
      trace = [event(40, 1, 50, 50, "target", :grant, ["target-run"], 1)]

      assert_raise ArgumentError, ~r/commit evidence does not exactly cover/, fn ->
        FairRotationAdversarialVerifier.assert_trace!(trace, opts(committed_call_sequences: []))
      end
    end

    defp event(
           database_call_sequence,
           ordinal,
           cursor_before,
           cursor_after,
           partition,
           disposition,
           outcome_ids,
           epoch_delta
         ) do
      %{
        database_call_sequence: database_call_sequence,
        ordinal: ordinal,
        cursor_before: cursor_before,
        cursor_after: cursor_after,
        demand: 1,
        partition: partition,
        disposition: disposition,
        outcomes: length(outcome_ids),
        outcome_ids: outcome_ids,
        epoch_delta: epoch_delta,
        committed: true
      }
    end

    defp opts(overrides) do
      Keyword.merge(
        [
          target: "target",
          cohort: ["target"],
          ring: [{50, "target"}],
          scan_budget: 1,
          quantum: 1,
          lock_failures: 0,
          first_database_call_sequence: 40,
          committed_call_sequences: [40],
          durable_outcome_ids: ["target-run"]
        ],
        overrides
      )
    end
  end
end
