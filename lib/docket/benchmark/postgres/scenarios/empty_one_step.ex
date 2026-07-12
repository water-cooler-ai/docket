if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Benchmark.Postgres.Scenarios.EmptyOneStep do
    @moduledoc "The end-to-end one-step graph throughput benchmark."
    @behaviour Docket.Benchmark.Postgres.Scenario

    alias Docket.Benchmark.Repo
    import Docket.Benchmark.Postgres

    @runtime Docket.Benchmark.Runtime
    @minimum_runtime_ready_lead_ms 250

    @impl true
    def name, do: "empty_one_step"

    @impl true
    def run(config, _context) do
      graph = graph()
      manual_opts = runtime_opts(config, testing: :manual)
      seed_count = if config.warmup > 0, do: config.warmup, else: config.runs

      {ref, run_ids} =
        with_manual_runtime(manual_opts, fn ->
          {:ok, ref} = Docket.save_graph(@runtime, graph)
          {ref, seed_runs(ref, seed_count)}
        end)

      {warmup, measured_run_ids} =
        if config.warmup > 0 do
          result = run_warmup(config, run_ids)

          Ecto.Adapters.SQL.query!(
            Repo,
            "TRUNCATE TABLE docket_events, docket_runs RESTART IDENTITY",
            []
          )

          measured_run_ids =
            with_manual_runtime(manual_opts, fn -> seed_runs(ref, config.runs) end)

          {result, measured_run_ids}
        else
          {%{requested_runs: 0, completed_runs: 0, graph_cache_state: "cold"}, run_ids}
        end

      if config.warmup == 0, do: Docket.Postgres.GraphCache.clear()
      {activation_at, t0, physical_before} = prepare_measured_activation()
      collector = Docket.Benchmark.Collector.start(measured_run_ids)
      runtime = start_runtime!(runtime_opts(config), collector)
      started_at = activation_at

      try do
        runtime_ready_lead_us =
          ensure_activation_lead!(t0, @minimum_runtime_ready_lead_ms)

        sleep_until(t0)
        wait_for_completion(collector, config.runs, config.timeout_ms)
        duration_native = System.monotonic_time() - t0
        finished_at = DateTime.utc_now()
        Supervisor.stop(runtime, :normal, 5_000)
        collector_stats = Docket.Benchmark.Collector.stats(collector)
        events = Docket.Benchmark.Collector.stop(collector)
        physical_after = physical_snapshot()
        invariants = invariants(config)

        measurements =
          measurements(
            events,
            t0,
            duration_native,
            config,
            physical_before,
            physical_after,
            collector_stats
          )

        passed =
          Enum.all?(invariants, & &1.pass) and measurements.collection.complete_sample_set

        duration_us = measurements.burst_duration_us

        artifact = %{
          schema_version: schema_version(),
          classification: "exploratory",
          success: passed,
          scenario: canonical_scenario(config.scenario),
          point: %{
            concurrency: config.concurrency,
            pool_size: config.pool_size,
            repetition: config.repetition
          },
          parameters: artifact_parameters(config),
          started_at: DateTime.to_iso8601(started_at),
          finished_at: DateTime.to_iso8601(finished_at),
          timing_scope: "common-due-time staged burst through dispatch and terminal commit",
          warmup: warmup,
          workload: %{
            backlog: config.runs,
            graph_shape: %{nodes: 1, edges: 2, kind: "empty_one_step"},
            input_state_bytes: 0,
            event_policy: config.event_policy,
            initial_graph_cache_state: warmup.graph_cache_state
          },
          runtime_configuration: %{
            notifier: "poll_only",
            poll_interval_ms: config.poll_interval_ms,
            orphan_ttl_ms: config.orphan_ttl_ms,
            max_claim_attempts: 5,
            drain_budget: %{max_moments: 100, max_elapsed_ms: 1_000},
            heartbeat: "disabled",
            staged_activation_target_lead_ms: 500,
            minimum_runtime_ready_lead_ms: @minimum_runtime_ready_lead_ms,
            observed_runtime_ready_lead_us: runtime_ready_lead_us,
            dispatcher_concurrency: config.concurrency,
            repo_pool_size: config.pool_size,
            node_count: config.nodes
          },
          duration_us: duration_us,
          measurements: measurements,
          environment: environment(config),
          invariants: invariants,
          warnings: warnings(config)
        }

        {:ok, artifact}
      after
        if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)

        if :ets.info(collector.table) != :undefined do
          Docket.Benchmark.Collector.stop(collector)
        end

        Docket.Postgres.GraphCache.clear()
      end
    end
  end
end
