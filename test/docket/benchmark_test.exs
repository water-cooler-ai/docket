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

  test "nearest-rank distributions retain sample counts and units" do
    assert %{
             unit: "ms",
             sample_count: 5,
             min: 1,
             p50: 3,
             p95: 100,
             p99: 100,
             max: 100,
             mean: 22.0
           } = Docket.Benchmark.Stats.millisecond_distribution([1, 2, 3, 4, 100])

    assert %{unit: "us", sample_count: 0} =
             Docket.Benchmark.Stats.native_distribution([])
  end

  test "collector keeps bounded metric metadata and drops query details" do
    collector = Docket.Benchmark.Collector.start()

    :telemetry.execute(
      [:docket, :postgres, :claim, :attempt],
      %{count: 1, eligible_age_ms: 7, overdue_after_ttl_ms: 0},
      %{class: :ready, result: :acquired, run_id: "secret", claim_token: "secret"}
    )

    :telemetry.execute(
      [:docket, :benchmark, :repo, :query],
      %{query_time: 10, queue_time: 2},
      %{query: "SELECT secret", params: ["secret"]}
    )

    events = Docket.Benchmark.Collector.stop(collector)

    assert {[:docket, :postgres, :claim, :attempt], _, %{class: :ready, result: :acquired}, _} =
             Enum.find(events, fn {event, _, _, _} ->
               event == [:docket, :postgres, :claim, :attempt]
             end)

    assert {[:docket, :benchmark, :repo, :query], _, %{}, _} =
             Enum.find(events, fn {event, _, _, _} ->
               event == [:docket, :benchmark, :repo, :query]
             end)
  end
end
