if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Benchmark.SleepNode do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema,
      do: Docket.Schema.object(%{"hold_ms" => Docket.Schema.float(required: true)})

    @impl true
    def call(_state, config, _context) do
      Process.sleep(trunc(config["hold_ms"]))
      {:ok, %{}}
    end
  end

  defmodule Docket.Benchmark.ParkOnceNode do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(_state, _config, %{attempt: 1}), do: {:error, :benchmark_park_once}
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule Docket.Benchmark.IncrementNode do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema,
      do: Docket.Schema.object(%{"field" => Docket.Schema.string(required: true)})

    @impl true
    def call(state, config, _context) do
      field = config["field"]
      {:ok, %{field => Map.get(state, field, 0.0) + 1.0}}
    end
  end

  defmodule Docket.Benchmark.Postgres.Scenarios.ComparativeBurst do
    @moduledoc false

    import Docket.Benchmark.Postgres

    @runtime Docket.Benchmark.Runtime
    @minimum_runtime_ready_lead_ms 250

    def run(config, kind) do
      {specs, labels, expected_claims, shape} = workload(config, kind)
      manual_opts = runtime_opts(config, testing: :manual)

      run_ids =
        with_manual_runtime(manual_opts, fn ->
          refs =
            Map.new(specs, fn {label, graph} ->
              {:ok, ref} = Docket.save_graph(@runtime, graph)
              {label, ref}
            end)

          Enum.map(labels, fn label ->
            {:ok, run} = Docket.start_run(@runtime, Map.fetch!(refs, label), %{})
            run.id
          end)
        end)

      Docket.Postgres.GraphCache.clear()
      {activation_at, t0, physical_before} = prepare_measured_activation()
      collector = Docket.Benchmark.Collector.start(run_ids)

      measurement_config =
        config
        |> Map.put(:expected_ready_claim_samples, expected_claims)
        |> Map.put(:flexible_checkpoint_shapes, kind != :mixed_service_times)

      runtime = start_runtime!(runtime_opts(config, executor: Docket.Executor.Task), collector)

      try do
        runtime_ready_lead_us = ensure_activation_lead!(t0, @minimum_runtime_ready_lead_ms)
        sleep_until(t0)
        wait_for_completion(collector, config.runs, config.timeout_ms)
        control_duration = System.monotonic_time() - t0
        finished_at = DateTime.utc_now()
        Supervisor.stop(runtime, :normal, 5_000)
        collector_stats = Docket.Benchmark.Collector.stats(collector)
        events = Docket.Benchmark.Collector.stop(collector)
        physical_after = physical_snapshot()

        measurements =
          measurements(
            events,
            t0,
            control_duration,
            measurement_config,
            physical_before,
            physical_after,
            collector_stats
          )
          |> Map.put(:cohorts, cohort_measurements(events, t0, labels))

        invariants = invariants(config)
        passed = Enum.all?(invariants, & &1.pass) and measurements.collection.complete_sample_set

        {:ok,
         %{
           schema_version: schema_version(),
           classification: "exploratory",
           success: passed,
           scenario: Atom.to_string(kind),
           point: %{
             concurrency: config.concurrency,
             pool_size: config.pool_size,
             repetition: config.repetition
           },
           parameters: artifact_parameters(config),
           started_at: DateTime.to_iso8601(activation_at),
           finished_at: DateTime.to_iso8601(finished_at),
           timing_scope: "interleaved common-due-time comparative cohort burst",
           warmup: %{requested_runs: 0, completed_runs: 0, graph_cache_state: "cold"},
           workload: %{
             backlog: config.runs,
             cohort_order: "alternating, first cohort first",
             cohorts: Enum.frequencies(labels),
             graph_shape: shape,
             hold_ms: Map.get(config, :hold_ms),
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
             staged_activation_target_lead_ms: 500,
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
           warnings:
             warnings(config) ++
               [
                 "Per-cohort first-claim offsets use the claim-scan telemetry emit time, so leases from one batch share one observation timestamp."
               ]
         }}
      after
        if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)

        if :ets.info(collector.table) != :undefined do
          Docket.Benchmark.Collector.stop(collector)
        end

        Docket.Postgres.GraphCache.clear()
      end
    end

    defp workload(config, :mixed_service_times) do
      slow = max(1, div(config.runs, 10))
      labels = interleave(:slow, slow, :fast, config.runs - slow)

      specs = [slow: sleep_graph(config.hold_ms), fast: noop_graph("mixed-fast")]
      shape = %{kind: "interleaved_slow_and_fast", slow_fraction: slow / config.runs}
      {specs, labels, config.runs, shape}
    end

    defp workload(config, :parked_wait_vs_blocking_wait) do
      blocking = div(config.runs + 1, 2)
      parked = config.runs - blocking
      labels = interleave(:blocking, blocking, :parked, parked)

      specs = [
        blocking: sleep_graph(config.hold_ms),
        parked: park_graph(config.hold_ms)
      ]

      shape = %{kind: "blocking_execution_vs_retry_park", retry_backoff_ms: config.hold_ms}
      {specs, labels, config.runs + parked, shape}
    end

    defp workload(config, :cyclic_vs_one_step) do
      cyclic = div(config.runs + 1, 2)
      one_step = config.runs - cyclic
      labels = interleave(:cyclic, cyclic, :one_step, one_step)
      specs = [cyclic: cycle_graph(), one_step: noop_graph("cyclic-control")]
      shape = %{kind: "ten_iteration_cycle_vs_one_step", cycle_iterations: 10}
      {specs, labels, config.runs, shape}
    end

    defp interleave(a, a_count, b, b_count) do
      pairs = min(a_count, b_count)

      List.duplicate([a, b], pairs)
      |> List.flatten()
      |> Kernel.++(List.duplicate(a, a_count - pairs))
      |> Kernel.++(List.duplicate(b, b_count - pairs))
    end

    defp cohort_measurements(events, t0, labels) do
      terminal_at =
        events
        |> event_records([:docket, :checkpoint, :committed])
        |> Enum.filter(fn {_m, metadata, _at} ->
          metadata.checkpoint_type == "run_completed" and is_integer(metadata.correlation_id)
        end)
        |> Map.new(fn {_m, metadata, at} -> {metadata.correlation_id, at} end)

      first_claim_at =
        events
        |> event_records([:docket, :postgres, :claim, :attempt])
        |> Enum.filter(fn {_m, metadata, _at} -> is_integer(metadata[:correlation_id]) end)
        |> Enum.group_by(fn {_m, metadata, _at} -> metadata.correlation_id end)
        |> Map.new(fn {correlation_id, records} ->
          {correlation_id, records |> Enum.map(fn {_m, _metadata, at} -> at end) |> Enum.min()}
        end)

      labels
      |> Enum.uniq()
      |> Map.new(fn label ->
        correlation_ids =
          for {cohort, index} <- Enum.with_index(labels, 1), cohort == label, do: index

        terminal_offsets =
          for id <- correlation_ids, at = terminal_at[id], is_integer(at), do: at - t0

        first_claim_offsets =
          for id <- correlation_ids, at = first_claim_at[id], is_integer(at), do: at - t0

        service_times =
          for id <- correlation_ids,
              claim = first_claim_at[id],
              terminal = terminal_at[id],
              is_integer(claim) and is_integer(terminal),
              do: terminal - claim

        terminal_distribution = Docket.Benchmark.Stats.native_distribution(terminal_offsets)

        first_claim_distribution =
          Docket.Benchmark.Stats.native_distribution(first_claim_offsets)

        {label,
         %{
           runs: length(correlation_ids),
           completed_runs: length(terminal_offsets),
           activation_to_terminal_commit_offset_us: terminal_distribution,
           activation_to_first_claim_offset_us: first_claim_distribution,
           first_claim_to_terminal_commit_us:
             Docket.Benchmark.Stats.native_distribution(service_times),
           queue_share_of_median_percent:
             queue_share(first_claim_distribution, terminal_distribution)
         }}
      end)
    end

    defp queue_share(%{p50: claim_p50}, %{p50: terminal_p50})
         when is_number(claim_p50) and is_number(terminal_p50) and terminal_p50 > 0 do
      Float.round(claim_p50 * 100 / terminal_p50, 1)
    end

    defp queue_share(_first_claim_distribution, _terminal_distribution), do: nil

    defp noop_graph(id) do
      Docket.Graph.new!(id: "docket-bench-#{id}")
      |> Docket.Graph.put_node!("noop", implementation: Docket.Benchmark.NoopNode)
      |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
      |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
    end

    defp sleep_graph(hold_ms) do
      Docket.Graph.new!(id: "docket-bench-sleep-#{hold_ms}")
      |> Docket.Graph.put_node!("sleep",
        implementation: Docket.Benchmark.SleepNode,
        config: %{hold_ms: hold_ms * 1.0}
      )
      |> Docket.Graph.put_edge!("start-sleep", from: "$start", to: "sleep")
      |> Docket.Graph.put_edge!("sleep-finish", from: "sleep", to: "$finish")
    end

    defp park_graph(backoff_ms) do
      Docket.Graph.new!(id: "docket-bench-park-#{backoff_ms}")
      |> Docket.Graph.put_node!("park",
        implementation: Docket.Benchmark.ParkOnceNode,
        policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => backoff_ms}}
      )
      |> Docket.Graph.put_edge!("start-park", from: "$start", to: "park")
      |> Docket.Graph.put_edge!("park-finish", from: "park", to: "$finish")
    end

    defp cycle_graph do
      alias Docket.Guard

      Docket.Graph.new!(id: "docket-bench-cycle-ten")
      |> Docket.Graph.put_field!("count", schema: Docket.Schema.float(), default: 0.0)
      |> Docket.Graph.put_node!("increment",
        implementation: Docket.Benchmark.IncrementNode,
        config: %{field: "count"}
      )
      |> Docket.Graph.put_node!("decide", implementation: Docket.Benchmark.NoopNode)
      |> Docket.Graph.put_edge!("start-increment", from: "$start", to: "increment")
      |> Docket.Graph.put_edge!("increment-decide", from: "increment", to: "decide")
      |> Docket.Graph.put_edge!("loop",
        from: "decide",
        to: "increment",
        guard: Guard.not(Guard.equals(Guard.path("count", []), 10.0))
      )
      |> Docket.Graph.put_edge!("finish",
        from: "decide",
        to: "$finish",
        guard: Guard.equals(Guard.path("count", []), 10.0)
      )
      |> Docket.Graph.policy!("max_supersteps", 50)
    end
  end

  defmodule Docket.Benchmark.Postgres.Scenarios.MixedServiceTimes do
    @moduledoc "Interleaved slow and fast run fairness benchmark."
    @behaviour Docket.Benchmark.Postgres.Scenario
    def name, do: "mixed_service_times"

    def run(config, _context),
      do: Docket.Benchmark.Postgres.Scenarios.ComparativeBurst.run(config, :mixed_service_times)
  end

  defmodule Docket.Benchmark.Postgres.Scenarios.ParkedWaitVsBlockingWait do
    @moduledoc "Retry-park versus resident blocking execution benchmark."
    @behaviour Docket.Benchmark.Postgres.Scenario
    def name, do: "parked_wait_vs_blocking_wait"

    def run(config, _context),
      do:
        Docket.Benchmark.Postgres.Scenarios.ComparativeBurst.run(
          config,
          :parked_wait_vs_blocking_wait
        )
  end

  defmodule Docket.Benchmark.Postgres.Scenarios.CyclicVsOneStep do
    @moduledoc "Continuously multi-step cyclic runs versus one-step runs."
    @behaviour Docket.Benchmark.Postgres.Scenario
    def name, do: "cyclic_vs_one_step"

    def run(config, _context),
      do: Docket.Benchmark.Postgres.Scenarios.ComparativeBurst.run(config, :cyclic_vs_one_step)
  end
end
