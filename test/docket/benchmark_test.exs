defmodule Docket.BenchmarkTest do
  use ExUnit.Case, async: false

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

  test "normalizes bounded blocked-vehicle sampling options" do
    assert {:ok, config} =
             Docket.Benchmark.parse(
               ~w(--scenario blocked_vehicles --runs 20 --concurrency-matrix 5,10 --pool-size 2 --hold-ms 100 --sample-interval-ms 5 --max-samples 32 --probe-count 2)
             )

    assert config.hold_ms == 100
    assert config.sample_interval_ms == 5
    assert config.max_samples == 32
    assert config.probe_count == 2

    assert {:error, backlog} =
             Docket.Benchmark.parse(
               ~w(--scenario blocked_vehicles --runs 4 --concurrency-matrix 2,5)
             )

    assert backlog =~ "runs >= maximum"

    assert {:error, ttl} =
             Docket.Benchmark.parse(
               ~w(--scenario blocked_vehicles --runs 5 --concurrency 5 --hold-ms 1000 --orphan-ttl-ms 1000)
             )

    assert ttl =~ "at most half of orphan-ttl-ms"

    assert {:error, interval} =
             Docket.Benchmark.parse(
               ~w(--scenario blocked_vehicles --runs 5 --concurrency 5 --sample-interval-ms 1)
             )

    assert interval =~ "integer >= 5"

    assert {:error, warmup} =
             Docket.Benchmark.parse(
               ~w(--scenario blocked_vehicles --runs 5 --concurrency 5 --warmup 1)
             )

    assert warmup =~ "does not support warmup"

    assert {:error, misplaced} = Docket.Benchmark.parse(~w(--scenario smoke --hold-ms 10))
    assert misplaced =~ "only valid for blocked_vehicles"
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

  test "bounded timelines compact samples without losing extrema or weighted means" do
    timeline =
      Enum.reduce(1..20, Docket.Benchmark.Timeline.new(4), fn value, timeline ->
        observed = if value == 7, do: 100, else: value
        Docket.Benchmark.Timeline.add(timeline, value * 10, %{load: observed})
      end)

    artifact = Docket.Benchmark.Timeline.artifact(timeline)
    assert artifact.raw_sample_count == 20
    assert artifact.retained_bucket_count <= 4
    assert artifact.compactions > 0
    assert artifact.maximum_samples_represented_by_one_bucket > 1
    assert artifact.summary.load.sample_count == 20
    assert artifact.summary.load.min == 1
    assert artifact.summary.load.max == 100
    assert artifact.summary.load.sample_mean == 15.15
    assert artifact.summary.load.last == 20
    assert artifact.represented_sample_count == 20
    assert Enum.sum(Enum.map(artifact.buckets, & &1.represented_samples)) == 20
    assert hd(artifact.buckets).start_offset_us == 10
    assert List.last(artifact.buckets).end_offset_us == 200
  end

  test "sampler forces a final bounded sample and retains event-derived gauges" do
    sampler =
      Docket.Benchmark.Sampler.start(
        start_at: System.monotonic_time() + System.convert_time_unit(1, :second, :native),
        interval_ms: 1_000,
        max_buckets: 4,
        sample: fn gauges ->
          %{custom_value: 7, observed_in_flight: gauges.dispatcher_in_flight_vehicles}
        end
      )

    :telemetry.execute(
      [:docket, :postgres, :dispatcher, :state],
      %{in_flight: 3, demand: 2, poll_active: 1, poll_pending: 0},
      %{}
    )

    :telemetry.execute(
      [:docket, :postgres, :dispatcher, :state],
      %{in_flight: 2, demand: 1, poll_active: 0, poll_pending: 1},
      %{}
    )

    :telemetry.execute(
      [:docket, :postgres, :run_store, :claim],
      %{leases: 5},
      %{}
    )

    phase = Docket.Benchmark.Sampler.force_sample(sampler)
    assert phase.metrics.dispatcher_in_flight_vehicles == 2
    assert phase.metrics.dispatcher_maximum_in_flight_vehicles == 3

    artifact = Docket.Benchmark.Sampler.stop(sampler)
    assert artifact.raw_sample_count == 2
    assert artifact.forced_phase_sample_count == 1
    assert artifact.forced_final_sample_count == 1
    assert artifact.failed_samples == 0
    assert artifact.summary.custom_value.last == 7
    assert artifact.summary.dispatcher_in_flight_vehicles.last == 2
    assert artifact.summary.dispatcher_maximum_in_flight_vehicles.last == 3
    assert artifact.summary.cumulative_claim_leases.last == 5
    assert artifact.summary.observed_in_flight.last == 2
    assert artifact.summary.sampler_probe_callback_duration_us.last >= 0
    assert artifact.observer_diagnostics.summed_sampler_self_time_us >= 0
  end

  test "sampler skips missed deadlines instead of issuing catch-up storms" do
    sampler =
      Docket.Benchmark.Sampler.start(
        start_at: System.monotonic_time(),
        interval_ms: 5,
        max_buckets: 16,
        sample: fn _gauges ->
          Process.sleep(20)
          %{slow_probe: 1}
        end
      )

    Process.sleep(55)
    artifact = Docket.Benchmark.Sampler.stop(sampler)

    assert artifact.missed_ticks >= 3
    assert artifact.raw_sample_count <= 5
    assert artifact.summary.slow_probe.sample_count == artifact.raw_sample_count
  end

  test "balanced timeline compaction preserves broad temporal resolution" do
    timeline =
      Enum.reduce(1..1_000, Docket.Benchmark.Timeline.new(8), fn value, timeline ->
        Docket.Benchmark.Timeline.add(timeline, value, %{value: value})
      end)

    artifact = Docket.Benchmark.Timeline.artifact(timeline)
    weights = Enum.map(artifact.buckets, & &1.represented_samples)

    assert artifact.retained_bucket_count == 8
    assert artifact.represented_sample_count == 1_000
    assert Enum.sum(weights) == 1_000
    assert Enum.max(weights) <= 256
    assert artifact.summary.value.first == 1
    assert artifact.summary.value.last == 1_000
    assert artifact.summary.value.delta == 999

    one_bucket =
      Enum.reduce(1..10, Docket.Benchmark.Timeline.new(1), fn value, timeline ->
        Docket.Benchmark.Timeline.add(timeline, value, %{value: value})
      end)
      |> Docket.Benchmark.Timeline.artifact()

    assert one_bucket.retained_bucket_count == 1
    assert one_bucket.represented_sample_count == 10
  end

  test "timeline buckets stay chronological with and without compaction" do
    for {samples, max_buckets} <- [{5, 8}, {300, 16}, {1_000, 7}] do
      artifact =
        Enum.reduce(1..samples, Docket.Benchmark.Timeline.new(max_buckets), fn value, timeline ->
          Docket.Benchmark.Timeline.add(timeline, value, %{value: value})
        end)
        |> Docket.Benchmark.Timeline.artifact()

      starts = Enum.map(artifact.buckets, & &1.start_offset_us)
      ends = Enum.map(artifact.buckets, & &1.end_offset_us)

      assert starts == Enum.sort(starts)
      assert ends == Enum.sort(ends)
      assert hd(starts) == 1
      assert List.last(ends) == samples
      assert Enum.all?(artifact.buckets, &(&1.start_offset_us <= &1.end_offset_us))
      assert Enum.all?(artifact.buckets, &(&1.metrics.value.first <= &1.metrics.value.last))
    end
  end

  test "sampler validates before attaching and reports unavailable metrics" do
    before_handlers =
      :telemetry.list_handlers([:docket, :postgres, :dispatcher, :state]) |> length()

    assert_raise ArgumentError, fn ->
      Docket.Benchmark.Sampler.start(
        start_at: System.monotonic_time(),
        interval_ms: 10,
        max_buckets: 0,
        sample: fn _ -> %{} end
      )
    end

    assert length(:telemetry.list_handlers([:docket, :postgres, :dispatcher, :state])) ==
             before_handlers

    sampler =
      Docket.Benchmark.Sampler.start(
        start_at: System.monotonic_time(),
        interval_ms: 1_000,
        max_buckets: 2,
        sample: fn _ -> %{available: 1, missing: nil, invalid: "value"} end
      )

    artifact = Docket.Benchmark.Sampler.stop(sampler)
    assert artifact.unavailable_metric_observations == %{invalid: 1, missing: 1}
    assert artifact.summary.available.last == 1
    refute Map.has_key?(artifact.summary, :missing)
  end

  test "sampler detaches when its owner exits and kills a wedged callback on timeout" do
    parent = self()

    owner =
      spawn(fn ->
        sampler =
          Docket.Benchmark.Sampler.start(
            start_at: System.monotonic_time() + System.convert_time_unit(1, :second, :native),
            interval_ms: 1_000,
            max_buckets: 2,
            sample: fn _ -> %{} end
          )

        send(parent, {:owned_sampler, sampler})
      end)

    owner_monitor = Process.monitor(owner)
    assert_receive {:owned_sampler, owned_sampler}, 1_000
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :normal}, 1_000
    wait_until_dead(owned_sampler.pid)

    refute Enum.any?(
             :telemetry.list_handlers([:docket, :postgres, :dispatcher, :state]),
             &(&1.id == owned_sampler.handler_id)
           )

    crashing_owner =
      spawn(fn ->
        sampler =
          Docket.Benchmark.Sampler.start(
            start_at: System.monotonic_time() + System.convert_time_unit(1, :second, :native),
            interval_ms: 1_000,
            max_buckets: 2,
            sample: fn _ -> %{} end
          )

        send(parent, {:crashing_owned_sampler, sampler})
        receive do: (:crash -> exit(:boom))
      end)

    crashing_monitor = Process.monitor(crashing_owner)
    assert_receive {:crashing_owned_sampler, crashing_sampler}, 1_000
    send(crashing_owner, :crash)
    assert_receive {:DOWN, ^crashing_monitor, :process, ^crashing_owner, :boom}, 1_000
    wait_until_dead(crashing_sampler.pid)

    refute Enum.any?(
             :telemetry.list_handlers([:docket, :postgres, :dispatcher, :state]),
             &(&1.id == crashing_sampler.handler_id)
           )

    wedged_owner =
      spawn(fn ->
        sampler =
          Docket.Benchmark.Sampler.start(
            start_at: System.monotonic_time(),
            interval_ms: 1,
            max_buckets: 2,
            sample: fn _ ->
              send(parent, :owned_sampler_callback_entered)
              receive do: (:never -> %{})
            end
          )

        send(parent, {:wedged_owned_sampler, sampler})
        receive do: (:owner_exit -> :ok)
      end)

    wedged_owner_monitor = Process.monitor(wedged_owner)
    assert_receive {:wedged_owned_sampler, wedged_owned_sampler}, 1_000
    assert_receive :owned_sampler_callback_entered, 1_000
    send(wedged_owner, :owner_exit)
    assert_receive {:DOWN, ^wedged_owner_monitor, :process, ^wedged_owner, :normal}, 1_000
    wait_until_dead(wedged_owned_sampler.pid)

    refute Enum.any?(
             :telemetry.list_handlers([:docket, :postgres, :dispatcher, :state]),
             &(&1.id == wedged_owned_sampler.handler_id)
           )

    wedged =
      Docket.Benchmark.Sampler.start(
        start_at: System.monotonic_time(),
        interval_ms: 1,
        max_buckets: 2,
        stop_timeout_ms: 50,
        sample: fn _ -> receive do: (:never -> %{}) end
      )

    Process.sleep(5)

    assert_raise RuntimeError, "benchmark sampler did not stop", fn ->
      Docket.Benchmark.Sampler.stop(wedged)
    end

    refute Process.alive?(wedged.pid)

    refute Enum.any?(
             :telemetry.list_handlers([:docket, :postgres, :dispatcher, :state]),
             &(&1.id == wedged.handler_id)
           )
  end

  test "blocking gate releases a saturated plateau and passes later backlog work" do
    gate =
      Docket.Benchmark.BlockingGate.start(
        owner: self(),
        allowed_run_ids: ["run-a", "run-b", "run-c"],
        target: 2
      )

    on_exit(fn -> Docket.Benchmark.BlockingGate.stop(gate) end)

    first =
      Task.async(fn ->
        Docket.Benchmark.BlockingGate.await(gate.pid, gate.token, "run-a", "blocker", 1)
      end)

    second =
      Task.async(fn ->
        Docket.Benchmark.BlockingGate.await(gate.pid, gate.token, "run-b", "blocker", 1)
      end)

    assert_receive {:docket_benchmark_blocking_plateau, gate_pid, 2, plateau_at}, 1_000
    assert gate_pid == gate.pid
    assert is_integer(plateau_at)

    assert %{
             currently_blocked: 2,
             maximum_blocked: 2,
             observed_runs: 2,
             duplicate_runs: 0,
             unknown_runs: 0,
             plateau_reached: true
           } = Docket.Benchmark.BlockingGate.snapshot(gate)

    assert length(Docket.Benchmark.BlockingGate.blocked_pids(gate)) == 2
    release = Docket.Benchmark.BlockingGate.open(gate)
    assert release.blocked_count == 2
    assert length(release.blocked_arrival_times) == 2
    assert Task.await(first) == :ok
    assert Task.await(second) == :ok

    assert Docket.Benchmark.BlockingGate.await(
             gate.pid,
             gate.token,
             "run-c",
             "blocker",
             1
           ) == :ok

    assert %{currently_blocked: 0, observed_runs: 3, open: true} =
             Docket.Benchmark.BlockingGate.snapshot(gate)
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

    assert {[:docket, :benchmark, :repo, :query], _, %{benchmark_query: :workload}, _} =
             Enum.find(events, fn {event, _, _, _} ->
               event == [:docket, :benchmark, :repo, :query]
             end)

    assert 2 ==
             Enum.count(events, fn {event, _, _, _} ->
               event == [:docket, :benchmark, :repo, :query]
             end)
  end

  test "collector reports its own ETS memory so samplers can subtract the observer" do
    collector = Docket.Benchmark.Collector.start()

    baseline = Docket.Benchmark.Collector.observer_memory_bytes(collector)
    assert baseline > 0

    for step <- 1..500 do
      :telemetry.execute(
        [:docket, :postgres, :dispatcher, :poll],
        %{duration: step, leases: 1, poisoned: 0},
        %{result: :ok, source: :interval}
      )
    end

    grown = Docket.Benchmark.Collector.observer_memory_bytes(collector)
    assert grown > baseline

    events = Docket.Benchmark.Collector.stop(collector)
    assert length(events) == 500

    assert Docket.Benchmark.Collector.observer_memory_bytes(collector) == 0
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

  test "console summary labels smoke cohort and post-first-commit metrics" do
    text =
      "empty_one_step"
      |> console_point()
      |> then(&Docket.Benchmark.Console.lines([&1]))
      |> Enum.join("\n")

    assert text =~ "PASS exploratory · empty_one_step · 1,000 runs · concurrency 5 · pool 8"
    assert text =~ "Burst 753.4 ms · 1,327.3 runs/s"
    assert text =~ "Cohort offsets from common activation (queue-inclusive)"
    assert text =~ "first durable commit"
    assert text =~ "p50 441.1 ms · p95 722.2 ms"
    assert text =~ "After first durable commit"
    assert text =~ "first commit -> terminal"
    assert text =~ "p50 872 us · p95 1.2 ms"
    assert text =~ "ready age at scan"
    assert text =~ "Checks 2/2 · cleanup passed"
    refute text =~ "activation_to_terminal_commit_us"
  end

  test "console summary uses p50 and max for small blocked probe samples" do
    text =
      "blocked_vehicles"
      |> console_point()
      |> then(&Docket.Benchmark.Console.lines([&1]))
      |> Enum.join("\n")

    assert text =~ "Blocked plateau / release"
    assert text =~ "fill 80.0 ms · stable hold 30.0 ms"
    assert text =~ "unrelated short query"
    assert text =~ "p50 420 us · max 610 us · n=3"
    assert text =~ "claim age at release 111.0 ms / TTL 60.00 s"
    assert text =~ "sampler missed 1 ticks · duty 9.0%"
  end

  test "console summary reports claim drain, database path, and class counts" do
    text =
      "claim_only"
      |> console_point()
      |> then(&Docket.Benchmark.Console.lines([&1]))
      |> Enum.join("\n")

    assert text =~ "1,000 claims"
    assert text =~ "Claim drain 753.4 ms · 1,327.3 claims/s"
    assert text =~ "Claim offsets from burst start (backlog-inclusive)"
    assert text =~ "all claims"
    assert text =~ "Postgres claim path"
    assert text =~ "claim query"
    assert text =~ "pool queue"
    assert text =~ "Claims ready 600 · expired 400 · mean rows/nonempty scan 50.0"
  end

  test "console suite reports valid repetitions and deterministic cell medians" do
    first = console_point("empty_one_step")

    second =
      first
      |> put_in([:point, :repetition], 2)
      |> put_in([:measurements, :throughput_per_second], 1_200.0)
      |> Map.put(:duration_us, 800_000)
      |> Map.put(:success, false)
      |> Map.put(:invariants, [%{name: "durable rows match", pass: false}])

    other_cell =
      first
      |> put_in([:point, :concurrency], 1)
      |> put_in([:point, :pool_size], 2)
      |> put_in([:measurements, :throughput_per_second], 240.0)

    text =
      Docket.Benchmark.Console.lines([first, second, other_cell])
      |> Enum.join("\n")

    assert text =~ "FAIL exploratory · empty_one_step · 2 cells · 2/3 trials valid"
    assert text =~ "c=1 pool=2 · 1/1 valid · 240.0 runs/s median"
    assert text =~ "c=5 pool=8 · 1/2 valid · 1,327.3 runs/s median"
    assert text =~ "distribution columns use p95 only when every trial has n>=20, otherwise max"
    assert text =~ "* Cohort offsets include backlog waiting."

    assert :binary.match(text, "c=1 pool=2") < :binary.match(text, "c=5 pool=8")
  end

  test "console matrix uses max rather than p95 for small distributions" do
    first = console_point("blocked_vehicles")
    second = put_in(first, [:point, :repetition], 2)

    text = Docket.Benchmark.Console.lines([first, second]) |> Enum.join("\n")

    assert text =~ "release -> terminal max 2.1 ms"
    assert text =~ "short query max 610 us"
    refute text =~ "release -> terminal p95"
  end

  test "console failure summary tolerates skeletal artifacts without exposing error details" do
    point = %{
      success: false,
      classification: "exploratory",
      scenario: "blocked_vehicles",
      point: %{concurrency: 2, pool_size: 1, repetition: 1},
      parameters: %{runs: 4, orphan_ttl_ms: 60_000},
      duration_us: nil,
      measurements: %{throughput_per_second: nil},
      invariants: [],
      cleanup: %{isolated_database_removed: true},
      failure_stage: "setup_or_execution",
      error: "postgres://secret@localhost/private\nSELECT secret"
    }

    text = Docket.Benchmark.Console.lines([point]) |> Enum.join("\n")
    assert text =~ "FAIL exploratory"
    assert text =~ "Burst — · —"
    assert text =~ "failure stage setup_or_execution"
    refute text =~ "secret"
    refute text =~ "SELECT"
  end

  defp console_point(scenario) do
    distribution = fn p50, p95, max, unit, count ->
      %{p50: p50, p95: p95, max: max, unit: unit, sample_count: count}
    end

    base = %{
      success: true,
      classification: "exploratory",
      scenario: scenario,
      point: %{concurrency: 5, pool_size: 8, repetition: 1},
      parameters: %{runs: 1_000, orphan_ttl_ms: 60_000},
      duration_us: 753_383,
      measurements: %{
        throughput_per_second: 1_327.346,
        latency: %{
          burst_activation_to_first_commit_offset_us:
            distribution.(441_088, 722_231, 752_719, "us", 1_000),
          burst_activation_to_terminal_commit_offset_us:
            distribution.(441_900, 723_008, 753_383, "us", 1_000),
          first_commit_to_terminal_us: distribution.(872, 1_210, 4_026, "us", 1_000),
          claim_scan_total_us: distribution.(641, 864, 49_640, "us", 832),
          selected_ready_age_at_scan_start_ms: distribution.(407, 754, 790, "ms", 1_000),
          vehicle_claim_held_ms: distribution.(5, 13, 20, "ms", 1_000)
        }
      },
      invariants: [
        %{name: "durable rows match", pass: true},
        %{name: "terminal rows match", pass: true}
      ],
      cleanup: %{isolated_database_removed: true}
    }

    case scenario do
      "claim_only" ->
        put_in(base, [:measurements], %{
          throughput_per_second: 1_327.346,
          latency: %{
            burst_start_to_claim_offset_us: distribution.(400_000, 700_000, 753_000, "us", 1_000),
            ready_burst_start_to_claim_offset_us:
              distribution.(390_000, 690_000, 750_000, "us", 600),
            expired_burst_start_to_claim_offset_us:
              distribution.(410_000, 710_000, 753_000, "us", 400),
            claim_scan_total_us: distribution.(600, 900, 1_400, "us", 20),
            claim_query_time_us: distribution.(400, 700, 1_100, "us", 20),
            claim_queue_time_us: distribution.(100, 200, 400, "us", 20)
          },
          counts: %{ready_claims: 600, expired_claims: 400},
          batches: %{mean_rows_per_nonempty_scan: 50.0}
        })

      "blocked_vehicles" ->
        put_in(base, [:measurements, :blocked_vehicles], %{
          plateau_fill_duration_us: 80_000,
          stable_hold_duration_us: 30_000,
          latency: %{
            gate_release_to_terminal_commit_us: distribution.(1_100, 1_900, 2_100, "us", 5),
            unrelated_short_query_round_trip_us: distribution.(420, 610, 610, "us", 3)
          },
          claim_freshness: %{maximum_claim_age_ms_at_release: 111},
          timeline: %{
            missed_ticks: 1,
            observer_diagnostics: %{serial_sampler_duty_cycle_percent: 9.0},
            summary: %{
              derived_oldest_unclaimed_wake_at_age_ms: %{max: 112}
            }
          }
        })

      _other ->
        base
    end
  end

  defp wait_until_dead(pid, attempts \\ 100)
  defp wait_until_dead(pid, 0), do: refute(Process.alive?(pid))

  defp wait_until_dead(pid, attempts) do
    if Process.alive?(pid) do
      Process.sleep(1)
      wait_until_dead(pid, attempts - 1)
    else
      :ok
    end
  end
end
