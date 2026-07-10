defmodule Docket.RunInfoTest do
  use Docket.Test.Case, async: true

  alias Docket.{Run, RunInfo}

  defp run, do: %Run{id: "run_1", graph_id: "g", status: :running, input: %{}}

  describe "new!/1" do
    test "builds a healthy projection from keyword or map fields" do
      wake_at = ~U[2026-07-09 10:00:00.000000Z]

      info = RunInfo.new!(run: run(), wake_at: wake_at, claim_attempts: 2)

      assert %RunInfo{run: %Run{id: "run_1"}, wake_at: ^wake_at, claim_attempts: 2} = info
      assert info == RunInfo.new!(%{run: run(), wake_at: wake_at, claim_attempts: 2})
      refute RunInfo.poisoned?(info)
    end

    test "builds a poisoned projection with paired facts" do
      info =
        RunInfo.new!(
          run: run(),
          claim_attempts: 3,
          poisoned_at: ~U[2026-07-09 10:00:00.000000Z],
          poison_reason: %{"type" => "max_claim_attempts_exceeded"}
        )

      assert RunInfo.poisoned?(info)
    end

    test "requires a Docket.Run" do
      assert_raise ArgumentError, ~r/must be a Docket.Run/, fn ->
        RunInfo.new!(run: %{id: "run_1"})
      end
    end

    test "rejects unpaired poison facts" do
      assert_raise ArgumentError, ~r/must be paired/, fn ->
        RunInfo.new!(run: run(), poisoned_at: ~U[2026-07-09 10:00:00.000000Z])
      end

      assert_raise ArgumentError, ~r/must be paired/, fn ->
        RunInfo.new!(run: run(), poison_reason: %{"type" => "test"})
      end
    end

    test "rejects malformed timestamps and claim attempts" do
      assert_raise ArgumentError, fn -> RunInfo.new!(run: run(), wake_at: "soon") end
      assert_raise ArgumentError, fn -> RunInfo.new!(run: run(), claimed_at: 1) end
      assert_raise ArgumentError, fn -> RunInfo.new!(run: run(), claim_attempts: -1) end
    end

    test "has no claim token field" do
      refute :claim_token in Map.keys(RunInfo.new!(run: run()))
    end
  end
end
