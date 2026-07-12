defmodule Docket.BenchmarkTest do
  use ExUnit.Case, async: false

  if Code.ensure_loaded?(Docket.Benchmark.Postgres.Scenario) do
    test "Postgres scenarios have focused modules behind one registry" do
      assert Docket.Benchmark.Postgres.Scenario.fetch!("smoke") ==
               Docket.Benchmark.Postgres.Scenarios.EmptyOneStep

      assert Docket.Benchmark.Postgres.Scenario.fetch!("claim_only") ==
               Docket.Benchmark.Postgres.Scenarios.ClaimOnly

      assert Docket.Benchmark.Postgres.Scenario.fetch!("blocked_vehicles") ==
               Docket.Benchmark.Postgres.Scenarios.BlockedVehicles

      assert Docket.Benchmark.Postgres.Scenario.fetch!("steady_arrival") ==
               Docket.Benchmark.Postgres.Scenarios.SteadyArrival

      assert Docket.Benchmark.Postgres.Scenario.fetch!("mixed_service_times") ==
               Docket.Benchmark.Postgres.Scenarios.MixedServiceTimes

      assert Docket.Benchmark.Postgres.Scenario.fetch!("parked_wait_vs_blocking_wait") ==
               Docket.Benchmark.Postgres.Scenarios.ParkedWaitVsBlockingWait

      assert Docket.Benchmark.Postgres.Scenario.fetch!("cyclic_vs_one_step") ==
               Docket.Benchmark.Postgres.Scenarios.CyclicVsOneStep

      assert Docket.Benchmark.Postgres.Scenario.canonical_name("smoke") == "empty_one_step"
    end

    test "steady-arrival schedules uniform offsets inside the configured window" do
      assert Docket.Benchmark.Postgres.Scenarios.SteadyArrival.schedule_offsets(4, 100) ==
               [0, 25_000, 50_000, 75_000]
    end

    test "Postgres physical deltas preserve explicit unavailable values" do
      assert Docket.Benchmark.Postgres.map_delta(
               %{commits: 12, newer_counter: "unavailable"},
               %{commits: 5, newer_counter: "unavailable"}
             ) == %{commits: 7, newer_counter: "unavailable"}

      before = %{
        activity: %{active_backends: 1, active_waiting_backends: 0},
        locks: %{lock_rows: 2, ungranted_lock_rows: 0}
      }

      after_snapshot = %{
        activity: %{active_backends: 3, active_waiting_backends: 1},
        locks: %{lock_rows: 5, ungranted_lock_rows: 1}
      }

      change = Docket.Benchmark.Postgres.contention_change(before, after_snapshot)
      assert change.before == before
      assert change.after == after_snapshot
      assert change.gauge_delta.activity.active_backends == 2
      assert change.gauge_delta.activity.active_waiting_backends == 1
      assert change.gauge_delta.locks.lock_rows == 3
      assert change.gauge_delta.locks.ungranted_lock_rows == 1
      assert change.caveat =~ "miss transient"
    end

    test "host and runtime fingerprints always expose stable fields" do
      host = Docket.Benchmark.Postgres.host_fingerprint()
      runtime = Docket.Benchmark.Postgres.runtime_fingerprint()
      container = Docket.Benchmark.Postgres.container_fingerprint()

      assert is_binary(host.os.family)
      assert is_binary(host.os.version)
      assert is_binary(host.os.kernel_release)
      assert is_binary(host.cpu.model)
      assert Map.has_key?(host.memory, :host_total_bytes)
      assert is_binary(runtime.architecture)
      assert runtime.schedulers_online == System.schedulers_online()
      assert Map.has_key?(runtime, :logical_processors_available)
      assert is_boolean(container.detected)
      assert is_binary(container.runtime)
      assert Map.has_key?(container, :cpu_quota_cores)
      assert Map.has_key?(container, :memory_limit_bytes)
    end

    test "comparative measurements expose claim tails, retry visibility, and terminal rank" do
      native_ms = System.convert_time_unit(1, :millisecond, :native)
      t0 = 10 * native_ms

      claim = fn id, offset_ms, eligible_age_ms ->
        {
          [:docket, :postgres, :claim, :attempt],
          %{eligible_age_ms: eligible_age_ms},
          %{correlation_id: id, class: :ready, result: :acquired},
          t0 + offset_ms * native_ms
        }
      end

      terminal = fn id, offset_ms ->
        {
          [:docket, :checkpoint, :committed],
          %{},
          %{correlation_id: id, checkpoint_type: "run_completed"},
          t0 + offset_ms * native_ms
        }
      end

      events = [
        claim.(1, 1, 1),
        claim.(2, 2, 2),
        claim.(3, 3, 3),
        claim.(4, 4, 4),
        claim.(2, 6, 7),
        terminal.(4, 7),
        terminal.(2, 8),
        terminal.(1, 11),
        terminal.(3, 12)
      ]

      result =
        Docket.Benchmark.Postgres.Scenarios.ComparativeBurst.comparative_measurements(
          events,
          t0,
          [:slow, :fast, :slow, :fast],
          :mixed_service_times
        )

      fast = result.cohorts.fast
      slow = result.cohorts.slow

      assert fast.activation_to_first_claim_offset_us.p95 == 4_000
      assert fast.activation_to_first_claim_offset_us.p99 == 4_000
      assert fast.activation_to_first_claim_offset_us.max == 4_000

      assert fast.terminal_rank_in_retained_sample == %{
               unit: "rank",
               sample_count: 2,
               min: 1,
               p50: 1,
               p95: 2,
               p99: 2,
               max: 2,
               mean: 1.5
             }

      assert fast.terminal_rank_minus_staged_ordinal.min == -3
      assert fast.terminal_order.finished_ahead_of_staged_ordinal == 1
      assert fast.terminal_order.finished_at_staged_ordinal == 1
      assert slow.terminal_order.finished_behind_staged_ordinal == 2

      assert fast.claims.retained_observations == 3
      assert fast.claims.retained_subsequent_observations == 1
      assert fast.claims.retained_runs_with_subsequent_claims == 1
      assert fast.claims.activation_to_subsequent_claim_offset_us.p50 == 6_000
      assert fast.claims.first_to_subsequent_claim_us.p50 == 4_000
      assert fast.claims.subsequent_ready_age_at_scan_start_ms.p50 == 7
      assert slow.claims.retained_subsequent_observations == 0

      assert result.fairness.fast_to_slow_normalized_slowdown_p50_ratio == 1.212
      assert result.fairness.fast_to_slow_normalized_slowdown_p95_ratio == 1.75

      sampled_events =
        Enum.filter(events, fn {_event, _measurements, metadata, _at} ->
          metadata.correlation_id in [2, 3]
        end)

      sampled =
        Docket.Benchmark.Postgres.Scenarios.ComparativeBurst.comparative_measurements(
          sampled_events,
          t0,
          [:slow, :fast, :slow, :fast, :slow, :fast],
          :mixed_service_times
        )

      refute sampled.fairness.sampling.complete_population
      assert sampled.fairness.sampling.population_runs == 6
      assert sampled.fairness.sampling.retained_correlation_samples == 2
      assert sampled.cohorts.fast.sampling.population_runs == 3
      assert sampled.cohorts.fast.sampling.retained_correlation_samples == 1
      assert sampled.cohorts.fast.terminal_rank_in_retained_sample.p50 == 1
      refute Map.has_key?(sampled.cohorts.fast, :terminal_rank_minus_staged_ordinal)
      refute Map.has_key?(sampled.cohorts.fast, :terminal_order)
    end
  end

  test "normalizes comparative fairness scenarios" do
    for scenario <- ~w(mixed_service_times parked_wait_vs_blocking_wait) do
      assert {:ok, config} =
               Docket.Benchmark.parse(~w(--scenario #{scenario} --runs 10 --hold-ms 75))

      assert config.hold_ms == 75
      assert config.warmup == 0

      if Code.ensure_loaded?(Docket.Benchmark.Postgres) do
        budget = apply(Docket.Benchmark.Postgres, :comparative_attempt_budget, [config])
        assert budget[:max_attempt_elapsed_ms] == 2_000
        assert budget[:drain_budget][:max_elapsed_ms] == 2_000
      end
    end

    assert {:ok, config} =
             Docket.Benchmark.parse(~w(--scenario cyclic_vs_one_step --runs 10))

    assert config.scenario == "cyclic_vs_one_step"

    assert {:error, too_small} =
             Docket.Benchmark.parse(~w(--scenario mixed_service_times --runs 1))

    assert too_small =~ "runs must be an integer >= 2"

    assert {:error, warmup} =
             Docket.Benchmark.parse(
               ~w(--scenario parked_wait_vs_blocking_wait --runs 10 --warmup 1)
             )

    assert warmup =~ "does not support warmup"

    assert {:error, deadline} =
             Docket.Benchmark.parse(
               ~w(--scenario mixed_service_times --runs 10 --hold-ms 1000 --orphan-ttl-ms 1001)
             )

    assert deadline =~ "deadline headroom"
  end

  test "normalizes bounded steady-arrival options" do
    assert {:ok, derived} =
             Docket.Benchmark.parse(
               ~w(--scenario steady_arrival --duration 200ms --arrival-rate 20 --sample-interval-ms 5 --max-samples 16)
             )

    assert derived.duration_ms == 200
    assert derived.arrival_rate == 20.0
    assert derived.runs == 4
    assert derived.sample_interval_ms == 5
    assert derived.max_samples == 16

    assert {:ok, by_runs} =
             Docket.Benchmark.parse(~w(--scenario steady_arrival --duration 1s --runs 12))

    assert by_runs.runs == 12
    assert by_runs.arrival_rate == nil

    assert {:error, missing} = Docket.Benchmark.parse(~w(--scenario steady_arrival))
    assert missing =~ "requires --duration"

    assert {:error, warmup} =
             Docket.Benchmark.parse(~w(--scenario steady_arrival --duration 100ms --warmup 1))

    assert warmup =~ "does not support warmup"

    assert {:error, unsupported} =
             Docket.Benchmark.parse(~w(--scenario steady_arrival --duration 100ms --hold-ms 10))

    assert unsupported =~ "unsupported steady_arrival options"
  end

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
    assert misplaced =~ "hold-ms is valid for blocked_vehicles"
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
             capture_mode: "bounded_streaming_reservoir",
             captured_events: 3,
             retained_event_samples: 3,
             exact_counters: true,
             max_samples_per_event: 4_096
           } = Docket.Benchmark.Collector.stats(collector)

    snapshot = Docket.Benchmark.Collector.stop(collector)
    events = Docket.Benchmark.Collector.sampled_events(snapshot)

    assert {[:docket, :postgres, :claim, :attempt], _, attempt_metadata, _} =
             Enum.find(events, fn {event, _, _, _} ->
               event == [:docket, :postgres, :claim, :attempt]
             end)

    assert attempt_metadata == %{class: :ready, result: :acquired, correlation_id: nil}

    assert {[:docket, :benchmark, :repo, :query], _, %{benchmark_query: :workload}, _} =
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

    assert Map.keys(handler.config) |> Enum.sort() == [
             :activation_at,
             :correlate?,
             :counters,
             :max_samples,
             :measurement_end_at,
             :mode,
             :table
           ]

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

    :telemetry.execute(
      [:docket, :postgres, :claim, :attempt],
      %{count: 1, eligible_age_ms: 7, overdue_after_ttl_ms: 0},
      %{class: :ready, result: :acquired, run_id: "run-secret"}
    )

    snapshot = Docket.Benchmark.Collector.stop(collector)
    events = Docket.Benchmark.Collector.sampled_events(snapshot)

    assert {[:docket, :checkpoint, :committed], _,
            %{correlation_id: 1, checkpoint_type: "step_committed"},
            _} =
             Enum.find(events, fn {name, _, _, _} ->
               name == [:docket, :checkpoint, :committed]
             end)

    assert {[:docket, :postgres, :claim, :attempt], _,
            %{class: :ready, result: :acquired, correlation_id: 1},
            _} =
             Enum.find(events, fn {name, _, _, _} ->
               name == [:docket, :postgres, :claim, :attempt]
             end)

    refute inspect(events) =~ "run-secret"
  end

  test "collector wait counters stay raw while full correlation checks detect duplicates" do
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

    assert Docket.Benchmark.Collector.count(collector, [:docket, :run, :completed]) == 2

    assert Docket.Benchmark.Collector.count(
             collector,
             [:docket, :checkpoint, :committed],
             %{checkpoint_type: "run_completed"}
           ) == 2

    assert Docket.Benchmark.Collector.stats(collector).captured_events == 4
    snapshot = Docket.Benchmark.Collector.stop(collector)

    assert Docket.Benchmark.Collector.unique_count(snapshot, [:docket, :run, :completed]) == 1

    assert Docket.Benchmark.Collector.full_population_unique_count(
             snapshot,
             [:docket, :run, :completed]
           ) == {:ok, 1}

    assert Docket.Benchmark.Collector.unique_count(
             snapshot,
             [:docket, :postgres, :claim, :attempt]
           ) == :unsupported

    assert Docket.Benchmark.Collector.correlation_summary(snapshot).completion_count_frequencies ==
             %{0 => 1, 2 => 1}

    terminal_max =
      Docket.Benchmark.Collector.observed_at_max(
        snapshot,
        [:docket, :checkpoint, :committed],
        %{checkpoint_type: "run_completed"}
      )

    assert is_integer(terminal_max)

    assert terminal_max ==
             Docket.Benchmark.Collector.phase_observed_at_max(
               snapshot,
               :measured,
               [:docket, :checkpoint, :committed],
               %{checkpoint_type: "run_completed"}
             )
  end

  test "collector bounds retained observations while preserving exact aggregates" do
    collector = Docket.Benchmark.Collector.start([], max_samples_per_event: 8)

    Enum.each(1..1_000, fn leases ->
      :telemetry.execute(
        [:docket, :postgres, :run_store, :claim],
        %{duration: leases, leases: leases, steals: 1, poisoned: 0},
        %{}
      )
    end)

    stats = Docket.Benchmark.Collector.stats(collector)
    assert stats.observed_events == 1_000
    assert stats.retained_event_samples == 8
    assert stats.aggregated_events == 992

    snapshot = Docket.Benchmark.Collector.stop(collector)

    assert Docket.Benchmark.Collector.observation_count(
             snapshot,
             [:docket, :postgres, :run_store, :claim]
           ) == 1_000

    assert Docket.Benchmark.Collector.numeric_sum(
             snapshot,
             [:docket, :postgres, :run_store, :claim],
             :leases
           ) == 500_500

    assert Docket.Benchmark.Collector.numeric_max(
             snapshot,
             [:docket, :postgres, :run_store, :claim],
             :leases
           ) == 1_000

    assert length(Docket.Benchmark.Collector.sampled_events(snapshot)) == 8

    assert Docket.Benchmark.Collector.uniqueness_scope(snapshot) ==
             :correlation_population_not_configured

    assert snapshot
           |> Docket.Benchmark.Collector.histogram(
             [:docket, :postgres, :run_store, :claim],
             :leases
           )
           |> Map.values()
           |> Enum.sum() == 8
  end

  test "collector bounds lease-value distributions with ten thousand distinct values" do
    event = [:docket, :postgres, :run_store, :claim]
    collector = Docket.Benchmark.Collector.start([], max_samples_per_event: 32)

    Enum.each(0..9_999, &emit_claim_measurement/1)

    stats = Docket.Benchmark.Collector.stats(collector)
    assert stats.histogram_scope == "retained_bounded_event_sample"
    assert stats.retained_event_samples == 32
    refute stats.full_population_shape_coverage
    assert stats.uniqueness_scope == "correlation_population_not_configured"
    assert :ets.info(collector.counters, :size) < 50

    snapshot = Docket.Benchmark.Collector.stop(collector)
    histogram = Docket.Benchmark.Collector.histogram(snapshot, event, :leases)

    assert histogram |> Map.values() |> Enum.sum() == 32
    assert map_size(histogram) <= 32
    assert Docket.Benchmark.Collector.observation_count(snapshot, event) == 10_000
    assert Docket.Benchmark.Collector.numeric_sum(snapshot, event, :leases) == 49_995_000
    assert Docket.Benchmark.Collector.numeric_max(snapshot, event, :leases) == 9_999
    assert Docket.Benchmark.Collector.predicate_count(snapshot, event, :leases_zero) == 1
  end

  test "collector exact aggregates exclude pre-activation and expose window phases" do
    activation_at =
      System.monotonic_time() + System.convert_time_unit(50, :millisecond, :native)

    measurement_end_at =
      activation_at + System.convert_time_unit(250, :millisecond, :native)

    collector =
      Docket.Benchmark.Collector.start([],
        activation_at: activation_at,
        measurement_end_at: measurement_end_at,
        max_samples_per_event: 8
      )

    emit_claim_measurement(1)
    sleep_until_monotonic(activation_at)
    emit_claim_measurement(2)
    sleep_until_monotonic(measurement_end_at)
    emit_claim_measurement(4)

    assert Docket.Benchmark.Collector.count(
             collector,
             [:docket, :postgres, :run_store, :claim]
           ) == 2

    snapshot = Docket.Benchmark.Collector.stop(collector)
    event = [:docket, :postgres, :run_store, :claim]

    assert Docket.Benchmark.Collector.phase_observation_count(
             snapshot,
             :pre_activation,
             event
           ) == 1

    assert Docket.Benchmark.Collector.phase_observation_count(snapshot, :measured, event) == 1

    assert Docket.Benchmark.Collector.phase_observation_count(snapshot, :post_measurement, event) ==
             1

    assert Docket.Benchmark.Collector.observation_count(snapshot, event) == 2

    assert Docket.Benchmark.Collector.phase_numeric_sum(snapshot, :pre_activation, event, :leases) ==
             1

    assert Docket.Benchmark.Collector.phase_numeric_sum(snapshot, :measured, event, :leases) == 2

    assert Docket.Benchmark.Collector.phase_numeric_sum(
             snapshot,
             :post_measurement,
             event,
             :leases
           ) == 4

    assert Docket.Benchmark.Collector.numeric_sum(snapshot, event, :leases) == 6
    assert Docket.Benchmark.Collector.numeric_max(snapshot, event, :leases) == 4

    assert Docket.Benchmark.Collector.phase_histogram(snapshot, :pre_activation, event, :leases) ==
             %{1 => 1}

    assert Docket.Benchmark.Collector.phase_histogram(snapshot, :measured, event, :leases) == %{
             2 => 1
           }

    assert Docket.Benchmark.Collector.phase_histogram(snapshot, :post_measurement, event, :leases) ==
             %{4 => 1}

    assert Docket.Benchmark.Collector.histogram(snapshot, event, :leases) == %{2 => 1, 4 => 1}
  end

  test "collector correlation state remains hard-bounded for large populations" do
    run_ids = Enum.map(1..50_000, &"run-#{&1}")
    collector = Docket.Benchmark.Collector.start(run_ids, max_samples_per_event: 32)

    Enum.each(run_ids, fn run_id ->
      :telemetry.execute([:docket, :run, :completed], %{duration: 1}, %{run_id: run_id})
    end)

    stats = Docket.Benchmark.Collector.stats(collector)
    assert stats.correlation_population == 50_000
    assert stats.indexed_correlations == 32
    assert stats.peak_correlation_cardinality == 32

    assert stats.correlation_correctness_scope ==
             "exact_global_counts_with_bounded_per_run_sample"

    assert :ets.info(collector.counters, :size) < 300
    assert Docket.Benchmark.Collector.count(collector, [:docket, :run, :completed]) == 50_000

    snapshot = Docket.Benchmark.Collector.stop(collector)
    summary = Docket.Benchmark.Collector.correlation_summary(snapshot)

    assert summary.population_expected == 50_000
    assert summary.sampled_expected == 32
    assert summary.completion_count_frequencies == %{1 => 32}
    refute summary.full_population_shape_coverage

    assert Docket.Benchmark.Collector.unique_count(snapshot, [:docket, :run, :completed]) ==
             :unavailable

    assert Docket.Benchmark.Collector.full_population_unique_count(
             snapshot,
             [:docket, :run, :completed]
           ) == {:unavailable, :bounded_correlation_sample}
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

  test "console summary compares cohorts for comparative scenarios" do
    text =
      "mixed_service_times"
      |> console_point()
      |> then(&Docket.Benchmark.Console.lines([&1]))
      |> Enum.join("\n")

    assert text =~ "Cohort offsets from common activation (queue-inclusive)"
    assert text =~ "fast terminal"
    assert text =~ "slow terminal"
    assert text =~ "p50 5.69 s · p95 6.10 s"
    assert text =~ "Wait before first claim"
    assert text =~ "fast first claim"
    assert text =~ "Per-run service after first claim"
    assert text =~ "fast claim -> terminal"
    assert text =~ "Normalized wait and completion order"

    assert text =~
             "normalized slowdown p50 120.00x · retained terminal rank p50 550 · retained subsequent claims 0"

    assert text =~ "queue share of p50"
    assert text =~ "fast 99.9% · slow 90.7%"
  end

  test "console suite compares cohort terminal medians per cell" do
    first = console_point("mixed_service_times")
    second = put_in(first, [:point, :repetition], 2)

    text = Docket.Benchmark.Console.lines([first, second]) |> Enum.join("\n")

    assert text =~ "fast terminal p95* 6.10 s · slow terminal p95* 5.14 s"
    assert text =~ "* Cohort offsets include backlog waiting."
  end

  test "headline projects scenario measurements into flat stable keys" do
    headline =
      "empty_one_step"
      |> console_point()
      |> Docket.Benchmark.Headline.build()

    assert headline.throughput_per_second == 1_327.346
    assert headline.duration_us == 753_383
    assert headline.activation_to_first_commit_p50_us == 441_088
    assert headline.first_commit_to_terminal_p95_us == 1_210
    assert headline.activation_to_terminal_p95_us == 723_008
    assert headline.claim_scan_p95_us == 864
    refute Map.has_key?(headline, :completed_runs)
  end

  test "headline flattens cohort comparisons and omits unproduced values" do
    headline =
      "mixed_service_times"
      |> console_point()
      |> Docket.Benchmark.Headline.build()

    assert headline.cohort_fast_activation_to_terminal_p50_us == 5_685_113
    assert headline.cohort_fast_activation_to_first_claim_p95_us == 6_090_000
    assert headline.cohort_fast_activation_to_first_claim_p99_us == 6_120_000
    assert headline.cohort_fast_activation_to_first_claim_max_us == 6_120_000
    assert headline.cohort_fast_first_claim_to_terminal_p50_us == 1_200
    assert headline.cohort_fast_normalized_slowdown_p50_ratio == 120.0
    assert headline.cohort_fast_terminal_rank_in_retained_sample_p95 == 950
    assert headline.cohort_fast_retained_subsequent_claims == 0
    assert headline.fast_to_slow_normalized_slowdown_p50_ratio == 12.4
    assert headline.cohort_slow_queue_share_of_median_percent == 90.7
    refute Map.has_key?(headline, :activation_to_terminal_p50_us)

    failure = %{
      scenario: "blocked_vehicles",
      duration_us: nil,
      measurements: %{throughput_per_second: nil},
      parameters: %{orphan_ttl_ms: 60_000}
    }

    assert Docket.Benchmark.Headline.build(failure) == %{orphan_ttl_ms: 60_000}
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
      %{p50: p50, p95: p95, p99: max, max: max, unit: unit, sample_count: count}
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

      "mixed_service_times" ->
        put_in(base, [:measurements], %{
          throughput_per_second: 162.906,
          latency: %{},
          fairness: %{
            fast_to_slow_normalized_slowdown_p50_ratio: 12.4,
            fast_to_slow_normalized_slowdown_p95_ratio: 19.2
          },
          cohorts: %{
            fast: %{
              activation_to_terminal_commit_offset_us:
                distribution.(5_685_113, 6_100_585, 6_138_508, "us", 900),
              activation_to_first_claim_offset_us:
                distribution.(5_680_000, 6_090_000, 6_120_000, "us", 900),
              first_claim_to_terminal_commit_us: distribution.(1_200, 3_400, 9_000, "us", 900),
              normalized_slowdown: distribution.(120.0, 300.0, 500.0, "ratio", 900),
              terminal_rank_in_retained_sample: distribution.(550, 950, 1_000, "rank", 900),
              claims: %{
                retained_subsequent_observations: 0,
                subsequent_ready_age_at_scan_start_ms: %{unit: "ms", sample_count: 0}
              },
              queue_share_of_median_percent: 99.9
            },
            slow: %{
              activation_to_terminal_commit_offset_us:
                distribution.(2_711_165, 5_136_728, 5_410_657, "us", 100),
              activation_to_first_claim_offset_us:
                distribution.(2_460_000, 4_880_000, 5_120_000, "us", 100),
              first_claim_to_terminal_commit_us:
                distribution.(251_000, 260_000, 270_000, "us", 100),
              normalized_slowdown: distribution.(9.7, 15.6, 18.0, "ratio", 100),
              terminal_rank_in_retained_sample: distribution.(100, 700, 900, "rank", 100),
              claims: %{
                retained_subsequent_observations: 0,
                subsequent_ready_age_at_scan_start_ms: %{unit: "ms", sample_count: 0}
              },
              queue_share_of_median_percent: 90.7
            }
          }
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

  defp emit_claim_measurement(leases) do
    :telemetry.execute(
      [:docket, :postgres, :run_store, :claim],
      %{duration: leases, leases: leases, steals: 0, poisoned: 0},
      %{}
    )
  end

  defp sleep_until_monotonic(target) do
    remaining = target - System.monotonic_time()

    if remaining > 0 do
      milliseconds = max(System.convert_time_unit(remaining, :native, :millisecond), 1)
      Process.sleep(milliseconds)
      sleep_until_monotonic(target)
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
