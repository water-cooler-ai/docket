defmodule Docket.BenchmarkTest do
  use ExUnit.Case, async: true

  test "parses the bounded smoke contract" do
    assert {:ok, config} =
             Docket.Benchmark.parse(
               ~w(--scenario smoke --runs 12 --concurrency 3 --pool-size 4 --output tmp/result.json)
             )

    assert config.runs == 12
    assert config.concurrency == 3
    assert config.pool_size == 4
  end

  test "rejects production-incompatible event suppression" do
    assert {:error, message} = Docket.Benchmark.parse(~w(--event-policy none))
    assert message =~ "not supported"
  end

  test "rejects unsupported scenarios rather than substituting work" do
    assert {:error, message} = Docket.Benchmark.parse(~w(--scenario soak))
    assert message =~ "not implemented"
  end

  test "does not mislabel exploratory results as authoritative" do
    assert {:error, message} = Docket.Benchmark.parse(~w(--authoritative))
    assert message =~ "exploratory"
  end

  test "rejects fake local multi-node measurements" do
    assert {:error, message} = Docket.Benchmark.parse(~w(--nodes 2))
    assert message =~ "independent BEAM nodes"
  end

  test "rejects accepted-looking options that are not measured yet" do
    assert {:error, warmup} = Docket.Benchmark.parse(~w(--warmup 1))
    assert warmup =~ "separate supervised interval"

    assert {:error, repetitions} = Docket.Benchmark.parse(~w(--repetitions 3))
    assert repetitions =~ "separate artifacts"

    assert {:error, duration} = Docket.Benchmark.parse(~w(--duration 10m))
    assert duration =~ "steady-state scenarios"
  end
end
