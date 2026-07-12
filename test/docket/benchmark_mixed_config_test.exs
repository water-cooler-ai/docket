defmodule Docket.BenchmarkMixedConfigTest do
  use ExUnit.Case, async: true

  test "mixed service times defaults to ten percent slow and supports fifty-fifty" do
    assert {:ok, default} =
             Docket.Benchmark.parse(~w(--scenario mixed_service_times --runs 10))

    assert default.slow_percent == 10

    assert {:ok, balanced} =
             Docket.Benchmark.parse(
               ~w(--scenario mixed_service_times --runs 10 --slow-percent 50)
             )

    assert balanced.slow_percent == 50
  end

  test "slow percentage must preserve both cohorts" do
    for percent <- [0, 100] do
      assert {:error, message} =
               Docket.Benchmark.parse(
                 ~w(--scenario mixed_service_times --runs 10 --slow-percent #{percent})
               )

      assert message =~ "slow-percent must be an integer from 1 through 99"
    end
  end

  test "slow percentage is rejected outside mixed service times" do
    for scenario <- ~w(smoke parked_wait_vs_blocking_wait cyclic_vs_one_step) do
      args = ~w(--scenario #{scenario} --runs 10 --slow-percent 50)
      assert {:error, message} = Docket.Benchmark.parse(args)
      assert message =~ "slow-percent is only valid for mixed_service_times"
    end
  end
end
