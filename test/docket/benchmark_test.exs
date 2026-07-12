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

  test "accepts warmup, repetitions, and matrix dimensions" do
    assert {:ok, config} =
             Docket.Benchmark.parse(
               ~w(--warmup 10 --repetitions 3 --concurrency-matrix 1,5,10 --pool-size-matrix 2,8 --format ndjson)
             )

    assert config.warmup == 10
    assert config.repetitions == 3
    assert config.concurrencies == [1, 5, 10]
    assert config.pool_sizes == [2, 8]
    assert config.format == "ndjson"
  end

  test "rejects malformed matrix and steady-state duration options" do
    assert {:error, matrix} = Docket.Benchmark.parse(~w(--concurrency-matrix 1,nope))
    assert matrix =~ "comma-separated positive integers"

    assert {:error, empty} = Docket.Benchmark.parse(~w(--concurrency-matrix 1,,2))
    assert empty =~ "must not be empty"

    assert {:error, duplicate} = Docket.Benchmark.parse(~w(--pool-size-matrix 2,2))
    assert duplicate =~ "must not contain duplicates"

    assert {:error, conflict} =
             Docket.Benchmark.parse(~w(--concurrency 2 --concurrency-matrix 1,2))

    assert conflict =~ "mutually exclusive"

    assert {:error, duration} = Docket.Benchmark.parse(~w(--duration 10m))
    assert duration =~ "steady-state scenarios"
  end

  test "normalizes claim-only options and rejects them for vehicle scenarios" do
    assert {:ok, config} =
             Docket.Benchmark.parse(
               ~w(--scenario claim_only --runs 11 --batch-size 3 --ready-ratio 2:1 --concurrency 4)
             )

    assert config.batch_size == 3
    assert config.ready_ratio == %{ready_weight: 2, expired_weight: 1}

    assert {:error, message} = Docket.Benchmark.parse(~w(--scenario smoke --batch-size 3))
    assert message =~ "only valid for claim_only"

    assert {:error, ratio} =
             Docket.Benchmark.parse(~w(--scenario claim_only --ready-ratio 0:0))

    assert ratio =~ "non-negative weights"
  end

  test "matrix plans cover every cell and rotate deterministically by seed" do
    assert {:ok, config} =
             Docket.Benchmark.parse(
               ~w(--repetitions 2 --concurrency-matrix 1,5 --pool-size-matrix 2,8 --seed 7)
             )

    first = Docket.Benchmark.plan(config)
    assert first == Docket.Benchmark.plan(config)
    assert length(first) == 8

    assert Enum.frequencies_by(first, &{&1.concurrency, &1.pool_size}) == %{
             {1, 2} => 2,
             {1, 8} => 2,
             {5, 2} => 2,
             {5, 8} => 2
           }

    refute Enum.map(first, &{&1.concurrency, &1.pool_size}) ==
             Enum.map(Docket.Benchmark.plan(%{config | seed: 8}), &{&1.concurrency, &1.pool_size})
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

  test "repetition summaries use min median max and spread instead of tail labels" do
    assert %{
             unit: "us",
             sample_count: 4,
             min: 10,
             median: 25.0,
             max: 50,
             mean: 27.5,
             spread: 40,
             spread_percent_of_median: 160.0
           } = Docket.Benchmark.Stats.repetition_summary([50, 10, 20, 30], "us")

    refute Map.has_key?(Docket.Benchmark.Stats.repetition_summary([1, 2, 3], "us"), :p95)
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

    :telemetry.execute(
      [:docket, :benchmark, :repo, :query],
      %{query_time: 10, queue_time: 2},
      %{query: "SELECT secret", params: ["secret"]}
    )

    assert Docket.Benchmark.Collector.count(
             collector,
             [:docket, :benchmark, :repo, :query]
           ) == 2

    assert %{
             capture_mode: "full_event_capture",
             captured_events: 3,
             observer_effect: "not_quantified"
           } =
             Docket.Benchmark.Collector.stats(collector)

    events = Docket.Benchmark.Collector.stop(collector)

    assert {[:docket, :postgres, :claim, :attempt], _, %{class: :ready, result: :acquired}, _} =
             Enum.find(events, fn {event, _, _, _} ->
               event == [:docket, :postgres, :claim, :attempt]
             end)

    assert {[:docket, :benchmark, :repo, :query], _, %{}, _} =
             Enum.find(events, fn {event, _, _, _} ->
               event == [:docket, :benchmark, :repo, :query]
             end)

    assert 2 ==
             Enum.count(events, fn {event, _, _, _} ->
               event == [:docket, :benchmark, :repo, :query]
             end)
  end

  test "collector retains run identity only as an internal correlation key" do
    collector = Docket.Benchmark.Collector.start(["run-secret"])

    handler =
      :telemetry.list_handlers([:docket, :run, :completed])
      |> Enum.find(&(&1.id == collector.handler_id))

    assert Map.keys(handler.config) |> Enum.sort() == [:correlate?, :counters, :table]
    refute inspect(handler.config) =~ "run-secret"

    event = %Docket.Event{
      run_id: "run-secret",
      seq: 2,
      type: :checkpoint_committed,
      step: 1,
      timestamp: DateTime.utc_now(),
      metadata: %{"checkpoint_type" => "step_committed"}
    }

    Docket.Telemetry.emit_events(%Docket.Run{id: "run-secret"}, [event])
    events = Docket.Benchmark.Collector.stop(collector)

    assert {[:docket, :checkpoint, :committed], _,
            %{correlation_id: 1, checkpoint_type: "step_committed"},
            _} =
             Enum.find(events, fn {name, _, _, _} ->
               name == [:docket, :checkpoint, :committed]
             end)
  end

  test "collector wait counters count unique correlated terminal runs" do
    collector = Docket.Benchmark.Collector.start(["run-a", "run-b"])
    now = DateTime.utc_now()

    checkpoint = %Docket.Event{
      run_id: "run-a",
      seq: 2,
      type: :checkpoint_committed,
      step: 1,
      timestamp: now,
      metadata: %{"checkpoint_type" => "run_completed"}
    }

    completed = %Docket.Event{
      run_id: "run-a",
      seq: 3,
      type: :run_completed,
      step: 1,
      timestamp: now
    }

    run = %Docket.Run{id: "run-a"}
    Docket.Telemetry.emit_events(run, [checkpoint, completed])
    Docket.Telemetry.emit_events(run, [checkpoint, completed])

    assert Docket.Benchmark.Collector.count(collector, [:docket, :run, :completed]) == 1

    assert Docket.Benchmark.Collector.count(
             collector,
             [:docket, :checkpoint, :committed],
             %{checkpoint_type: "run_completed"}
           ) == 1

    assert Docket.Benchmark.Collector.stats(collector).captured_events == 4
    Docket.Benchmark.Collector.stop(collector)
  end
end
