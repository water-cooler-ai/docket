if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Benchmark.Postgres.Scenarios.BlockedVehicles do
    @moduledoc "The saturated blocked-vehicle and pool-contention benchmark."
    @behaviour Docket.Benchmark.Postgres.Scenario

    import Docket.Benchmark.Postgres

    @runtime Docket.Benchmark.Runtime
    @minimum_runtime_ready_lead_ms 250

    @impl true
    def name, do: "blocked_vehicles"

    @impl true
    def run(config, _context) do
      manual_opts = runtime_opts(config, testing: :manual)

      run_ids =
        with_manual_runtime(manual_opts, fn ->
          {:ok, ref} = Docket.save_graph(@runtime, blocked_graph())
          seed_runs(ref, config.runs)
        end)

      Docket.Postgres.GraphCache.clear()
      initial_event_rows = scalar("SELECT count(*) FROM docket_events")

      initial_checkpoint_sum =
        scalar("SELECT coalesce(sum(checkpoint_seq), 0)::bigint FROM docket_runs")

      {activation_at, t0, physical_before} = prepare_measured_activation(1_000)

      gate =
        Docket.Benchmark.BlockingGate.start(
          owner: self(),
          allowed_run_ids: run_ids,
          target: config.concurrency
        )

      collector = Docket.Benchmark.Collector.start(run_ids, activation_at: t0)

      try do
        sampler =
          Docket.Benchmark.Sampler.start(
            start_at: t0,
            interval_ms: config.sample_interval_ms,
            max_buckets: config.max_samples,
            sample: fn gauges -> blocked_system_sample(config, gate, gauges, t0) end
          )

        try do
          runtime_opts =
            runtime_opts(config,
              context: %{blocking_benchmark: %{gate: gate.pid, token: gate.token}},
              executor: Docket.Executor.Task
            )

          runtime = start_runtime!(runtime_opts, collector)

          try do
            runtime_ready_lead_us =
              ensure_activation_lead!(t0, @minimum_runtime_ready_lead_ms)

            sleep_until(t0)
            plateau_at = wait_for_blocking_plateau(gate, config.concurrency, config.timeout_ms)
            topology = wait_for_blocked_topology(gate, config.concurrency, config.timeout_ms)

            plateau =
              blocked_plateau_snapshot(
                config,
                collector,
                topology,
                initial_event_rows,
                initial_checkpoint_sum
              )

            stable_hold_started_at = System.monotonic_time()
            stable_start_sample = Docket.Benchmark.Sampler.force_sample(sampler)
            probes = run_short_work_probes(config)

            sleep_until_strict(
              stable_hold_started_at +
                System.convert_time_unit(config.hold_ms, :millisecond, :native)
            )

            pre_release_sample = Docket.Benchmark.Sampler.force_sample(sampler)
            maximum_claim_age_ms_at_release = benchmark_maximum_claim_age_ms()
            release = Docket.Benchmark.BlockingGate.open(gate)
            wait_for_completion(collector, config.runs, config.timeout_ms)
            wait_for_vehicle_quiescence(collector, sampler, config.runs, config.timeout_ms)
            quiescence_at = System.monotonic_time()
            control_duration = quiescence_at - t0
            timeline = Docket.Benchmark.Sampler.stop(sampler)
            finished_at = DateTime.utc_now()
            Supervisor.stop(runtime, :normal, 5_000)
            collector_stats = Docket.Benchmark.Collector.stats(collector)
            events = Docket.Benchmark.Collector.stop(collector)
            physical_after = physical_snapshot()
            gate_final = Docket.Benchmark.BlockingGate.snapshot(gate)

            measurements =
              measurements(
                events,
                t0,
                control_duration,
                config,
                physical_before,
                physical_after,
                collector_stats
              )

            blocked =
              blocked_measurements(
                events,
                t0,
                plateau_at,
                stable_hold_started_at,
                release,
                run_ids,
                probes,
                plateau,
                gate_final,
                timeline,
                %{
                  stable_start: stable_start_sample,
                  pre_release: pre_release_sample
                },
                maximum_claim_age_ms_at_release,
                quiescence_at,
                config
              )

            measurements = Map.put(measurements, :blocked_vehicles, blocked)

            invariants =
              invariants(config) ++
                blocked_invariants(config, plateau, gate_final, blocked, measurements)

            passed =
              Enum.all?(invariants, & &1.pass) and
                measurements.collection.telemetry_checks_pass and
                blocked.collection.telemetry_checks_pass

            {:ok,
             %{
               schema_version: schema_version(),
               classification: "exploratory",
               success: passed,
               scenario: "blocked_vehicles",
               point: %{
                 concurrency: config.concurrency,
                 pool_size: config.pool_size,
                 repetition: config.repetition
               },
               parameters: artifact_parameters(config),
               started_at: DateTime.to_iso8601(activation_at),
               finished_at: DateTime.to_iso8601(finished_at),
               timing_scope:
                 "common-due-time burst, saturated blocked-vehicle plateau, gate release, and terminal commit",
               warmup: %{
                 requested_runs: 0,
                 completed_runs: 0,
                 graph_cache_state: "cold"
               },
               workload: %{
                 backlog: config.runs,
                 blocked_plateau_target: config.concurrency,
                 stable_hold_ms: config.hold_ms,
                 short_work_probe_count: config.probe_count,
                 post_release_gate: "open_for_remaining_backlog",
                 graph_shape: %{nodes: 1, edges: 2, kind: "blocking_one_step"},
                 input_state_bytes: 0,
                 event_policy: config.event_policy,
                 initial_graph_cache_state: "cold"
               },
               runtime_configuration: %{
                 notifier: "poll_only",
                 poll_interval_ms: config.poll_interval_ms,
                 orphan_ttl_ms: config.orphan_ttl_ms,
                 max_claim_attempts: 5,
                 drain_budget: %{max_moments: 100, max_elapsed_ms: 1_000},
                 heartbeat: "disabled",
                 node_executor: "Docket.Executor.Task",
                 staged_activation_target_lead_ms: 1_000,
                 minimum_runtime_ready_lead_ms: @minimum_runtime_ready_lead_ms,
                 observed_runtime_ready_lead_us: runtime_ready_lead_us,
                 dispatcher_concurrency: config.concurrency,
                 repo_pool_size: config.pool_size,
                 node_count: config.nodes
               },
               duration_us: measurements.burst_duration_us,
               measurements: measurements,
               environment: environment(config),
               invariants: invariants,
               warnings: blocked_warnings(config)
             }}
          after
            _release = Docket.Benchmark.BlockingGate.open(gate)
            if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)
          end
        after
          cleanup_sampler(sampler)
        end
      after
        cleanup_collector(collector)
        Docket.Benchmark.BlockingGate.stop(gate)
        Docket.Postgres.GraphCache.clear()
      end
    end
  end
end
