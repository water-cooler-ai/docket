defmodule Docket.FairRotationContractOracleTest do
  use ExUnit.Case, async: true

  alias Docket.Test.FairRotationContractOracle

  test "the frozen bounds are defined for every small valid contract domain" do
    for h <- 1..8,
        a <- 1..h,
        s <- 1..4,
        q <- 1..3,
        l <- 0..2 do
      bounds = FairRotationContractOracle.bounds!(a, h, s, q, l)

      assert bounds.competing_grants == (l + 1) * (a - 1)
      assert bounds.competing_outcomes == q * bounds.competing_grants

      assert bounds.qualifying_calls ==
               (l + 1) * (a - 1 + div(h - a + 1 + s - 1, s))
    end
  end

  test "the demand-aware call bound keeps the load-bearing demand-one term" do
    bounds = FairRotationContractOracle.bounds!(2, 2, 2, 8, 0)

    assert bounds.qualifying_calls == 2
    refute bounds.qualifying_calls == div(2 + 2 - 1, 2)
  end

  test "invalid theorem populations and budgets fail closed" do
    for args <- [
          {0, 1, 1, 1, 0},
          {2, 1, 1, 1, 0},
          {1, 1, 0, 1, 0},
          {1, 1, 1, 0, 0},
          {1, 1, 1, 1, -1}
        ] do
      assert_raise ArgumentError, fn ->
        apply(FairRotationContractOracle, :bounds!, Tuple.to_list(args))
      end
    end
  end
end
