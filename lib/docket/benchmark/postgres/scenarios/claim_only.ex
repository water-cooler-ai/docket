if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Benchmark.Postgres.Scenarios.ClaimOnly do
    @moduledoc "The direct concurrent claim-scan benchmark."
    @behaviour Docket.Benchmark.Postgres.Scenario

    import Docket.Benchmark.Postgres

    @runtime Docket.Benchmark.Runtime

    @impl true
    def name, do: "claim_only"

    @impl true
    def run(config, _context) do
      manual_opts = runtime_opts(config, testing: :manual)

      run_ids =
        with_manual_runtime(manual_opts, fn ->
          {:ok, ref} = Docket.save_graph(@runtime, graph())
          seed_runs(ref, config.runs)
        end)

      ratio = config.ready_ratio

      ready_count =
        div(config.runs * ratio.ready_weight, ratio.ready_weight + ratio.expired_weight)

      expired_count = config.runs - ready_count
      {expired_ids, ready_ids} = Enum.split(run_ids, expired_count)
      claim_now = DateTime.utc_now()
      expired_claimed_at = DateTime.add(claim_now, -config.orphan_ttl_ms - 1, :millisecond)
      stage_claim_only(expired_ids, claim_now, expired_claimed_at)
      initial_event_rows = scalar("SELECT count(*) FROM docket_events")

      initial_checkpoint_sum =
        scalar("SELECT coalesce(sum(checkpoint_seq), 0)::bigint FROM docket_runs")

      physical_before = physical_snapshot()
      collector = Docket.Benchmark.Collector.start()
      leases = :ets.new(__MODULE__.ClaimLeases, [:set, :public, write_concurrency: true])
      counters = :atomics.new(3, signed: false)
      t0 = System.monotonic_time()
      started_at = DateTime.utc_now()

      try do
        run_claimers!(config, claim_now, leases, counters)
        control_duration = System.monotonic_time() - t0
        finished_at = DateTime.utc_now()
        collector_stats = Docket.Benchmark.Collector.stats(collector)
        events = Docket.Benchmark.Collector.stop(collector)
        physical_after = physical_snapshot()

        setup = %{
          claim_now: claim_now,
          expired_claimed_at: expired_claimed_at,
          ready_ids: ready_ids,
          expired_ids: expired_ids,
          ready_count: ready_count,
          expired_count: expired_count,
          initial_event_rows: initial_event_rows,
          initial_checkpoint_sum: initial_checkpoint_sum
        }

        invariants = claim_only_invariants(config, setup, counters)

        measurements =
          claim_only_measurements(
            events,
            t0,
            control_duration,
            config,
            setup,
            physical_before,
            physical_after,
            collector_stats
          )

        passed =
          Enum.all?(invariants, & &1.pass) and measurements.collection.complete_sample_set

        {:ok,
         %{
           schema_version: schema_version(),
           classification: "exploratory",
           success: passed,
           scenario: "claim_only",
           point: %{
             concurrency: config.concurrency,
             pool_size: config.pool_size,
             repetition: config.repetition
           },
           parameters: artifact_parameters(config),
           workload: %{
             backlog: config.runs,
             batch_size: config.batch_size,
             ready_count: ready_count,
             expired_count: expired_count,
             ready_ratio: ratio,
             frozen_claim_clock: true
           },
           runtime_configuration: %{
             execution: "direct_run_store",
             orphan_ttl_ms: config.orphan_ttl_ms,
             concurrent_claimers: config.concurrency,
             repo_pool_size: config.pool_size,
             node_count: config.nodes
           },
           started_at: DateTime.to_iso8601(started_at),
           finished_at: DateTime.to_iso8601(finished_at),
           timing_scope: "concurrent direct claim scans until the frozen backlog is fully leased",
           warmup: %{requested_runs: 0, completed_runs: 0, graph_cache_state: "not_applicable"},
           duration_us: measurements.claim_window_duration_us,
           measurements: measurements,
           environment: environment(config),
           invariants: invariants,
           warnings: claim_only_warnings(config)
         }}
      after
        if :ets.info(collector.table) != :undefined do
          Docket.Benchmark.Collector.stop(collector)
        end

        if :ets.info(leases) != :undefined, do: :ets.delete(leases)
      end
    end
  end
end
