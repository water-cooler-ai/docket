if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Benchmark.Postgres.Scenarios.SteadyArrival do
    @moduledoc "Uniform open-loop arrivals through the production Postgres runtime."
    @behaviour Docket.Benchmark.Postgres.Scenario

    alias Docket.Benchmark.Repo
    import Docket.Benchmark.Postgres

    @runtime Docket.Benchmark.Runtime
    @minimum_runtime_ready_lead_ms 250

    @impl true
    def name, do: "steady_arrival"

    @impl true
    def run(config, _context) do
      manual_opts = runtime_opts(config, testing: :manual)

      run_ids =
        with_manual_runtime(manual_opts, fn ->
          {:ok, ref} = Docket.save_graph(@runtime, graph())
          seed_runs(ref, config.runs)
        end)

      Docket.Postgres.GraphCache.clear()

      {activation_at, t0, physical_before, schedule} =
        prepare_schedule(run_ids, config.duration_ms)

      window_end =
        t0 + System.convert_time_unit(config.duration_ms, :millisecond, :native)

      collector =
        Docket.Benchmark.Collector.start(run_ids,
          activation_at: t0,
          measurement_end_at: window_end
        )

      try do
        sampler =
          Docket.Benchmark.Sampler.start(
            start_at: t0,
            interval_ms: config.sample_interval_ms,
            max_buckets: config.max_samples,
            sample: fn _gauges -> backlog_sample() end
          )

        try do
          runtime = start_runtime!(runtime_opts(config), collector)

          try do
            runtime_ready_lead_us = ensure_activation_lead!(t0, @minimum_runtime_ready_lead_ms)
            sleep_until(t0)
            activation_sample = Docket.Benchmark.Sampler.force_sample(sampler)

            sleep_until_strict(window_end)
            window_end_sample = Docket.Benchmark.Sampler.force_sample(sampler)
            wait_for_completion(collector, config.runs, config.timeout_ms)
            completion_detected_at = System.monotonic_time()
            post_detection_sample = Docket.Benchmark.Sampler.force_sample(sampler)
            timeline = Docket.Benchmark.Sampler.stop(sampler)
            finished_at = DateTime.utc_now()
            Supervisor.stop(runtime, :normal, 5_000)
            collector_stats = Docket.Benchmark.Collector.stats(collector)
            snapshot = Docket.Benchmark.Collector.stop(collector)
            physical_after = physical_snapshot()

            measurements =
              measurements(
                snapshot,
                t0,
                completion_detected_at - t0,
                config,
                physical_before,
                physical_after,
                collector_stats
              )

            steady =
              steady_measurements(
                snapshot,
                t0,
                window_end,
                completion_detected_at,
                schedule,
                activation_sample,
                window_end_sample,
                post_detection_sample,
                timeline,
                config
              )

            measurements =
              measurements
              |> Map.put(:steady_arrival, steady)
              |> Map.put(:throughput_per_second, steady.achieved_terminal_drain_rate_per_second)
              |> Map.put(
                :observed_runs_per_second,
                steady.achieved_terminal_drain_rate_per_second
              )

            invariants =
              invariants(config) ++ steady_invariants(config, schedule, steady)

            passed =
              Enum.all?(invariants, & &1.pass) and
                measurements.collection.telemetry_checks_pass

            {:ok,
             %{
               schema_version: schema_version(),
               classification: "exploratory",
               success: passed,
               scenario: "steady_arrival",
               point: %{
                 concurrency: config.concurrency,
                 pool_size: config.pool_size,
                 repetition: config.repetition
               },
               parameters: artifact_parameters(config),
               started_at: DateTime.to_iso8601(activation_at),
               finished_at: DateTime.to_iso8601(finished_at),
               timing_scope: "uniform pre-staged open-loop due times through final backlog drain",
               warmup: %{requested_runs: 0, completed_runs: 0, graph_cache_state: "cold"},
               workload: %{
                 scheduled_runs: config.runs,
                 arrival_window_ms: config.duration_ms,
                 configured_arrival_rate_per_second: config.arrival_rate,
                 schedule: "uniform due offsets fixed before measurement",
                 graph_shape: %{nodes: 1, edges: 2, kind: "empty_one_step"},
                 event_policy: config.event_policy,
                 initial_graph_cache_state: "cold"
               },
               runtime_configuration: %{
                 notifier: "poll_only",
                 poll_interval_ms: config.poll_interval_ms,
                 orphan_ttl_ms: config.orphan_ttl_ms,
                 max_claim_attempts: 5,
                 max_attempt_elapsed_ms: 2_000,
                 drain_budget: %{max_moments: 100, max_elapsed_ms: 3_000},
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
               warnings: steady_warnings(config)
             }}
          after
            if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)
          end
        after
          cleanup_sampler(sampler)
        end
      after
        cleanup_collector(collector)
        Docket.Postgres.GraphCache.clear()
      end
    end

    @doc false
    def schedule_offsets(run_count, duration_ms) do
      duration_us = duration_ms * 1_000
      for index <- 0..(run_count - 1), do: div(index * duration_us, run_count)
    end

    defp prepare_schedule(run_ids, duration_ms, lead_ms \\ 1_000) do
      activation_at = DateTime.add(DateTime.utc_now(), lead_ms, :millisecond)
      offsets_us = schedule_offsets(length(run_ids), duration_ms)

      due_times =
        Enum.map(offsets_us, &DateTime.add(activation_at, &1, :microsecond))

      %{num_rows: staged} =
        Ecto.Adapters.SQL.query!(
          Repo,
          "UPDATE docket_runs AS runs SET wake_at = schedule.wake_at FROM unnest($1::text[], $2::timestamptz[]) AS schedule(run_id, wake_at) WHERE runs.run_id = schedule.run_id",
          [run_ids, due_times]
        )

      if staged != length(run_ids), do: raise("steady-arrival schedule did not stage every run")

      physical_before = physical_snapshot()
      remaining_us = DateTime.diff(activation_at, DateTime.utc_now(), :microsecond)

      if remaining_us >= @minimum_runtime_ready_lead_ms * 1_000 do
        t0 =
          System.monotonic_time() +
            System.convert_time_unit(remaining_us, :microsecond, :native)

        {activation_at, t0, physical_before, %{offsets_us: offsets_us, staged_runs: staged}}
      else
        prepare_schedule(run_ids, duration_ms, lead_ms * 2)
      end
    end

    defp backlog_sample do
      now = DateTime.utc_now()

      %{rows: [[ready, claimed, completed, future, oldest_lag_ms]]} =
        Ecto.Adapters.SQL.query!(
          Repo,
          """
          SELECT
            count(*) FILTER (WHERE status = 'running' AND claim_token IS NULL AND wake_at <= $1)::bigint,
            count(*) FILTER (WHERE status = 'running' AND claim_token IS NOT NULL)::bigint,
            count(*) FILTER (WHERE status = 'done')::bigint,
            count(*) FILTER (WHERE status = 'running' AND claim_token IS NULL AND wake_at > $1)::bigint,
            coalesce(max(floor(greatest(extract(epoch FROM ($1::timestamptz - wake_at)), 0) * 1000)) FILTER (WHERE status = 'running' AND claim_token IS NULL AND wake_at <= $1), 0)::bigint
          FROM docket_runs
          """,
          [now],
          telemetry_options: [benchmark_query: :control]
        )

      %{
        ready_unclaimed_backlog: ready,
        active_claimed_runs: claimed,
        due_outstanding_runs: ready + claimed,
        completed_runs: completed,
        future_scheduled_runs: future,
        oldest_due_lag_ms: oldest_lag_ms
      }
    end

    defp steady_measurements(
           snapshot,
           t0,
           window_end,
           completion_detected_at,
           schedule,
           activation_sample,
           window_end_sample,
           post_detection_sample,
           timeline,
           config
         ) do
      terminal_lags = completion_lags(snapshot, t0, schedule.offsets_us)

      completed_in_window =
        Docket.Benchmark.Collector.phase_observation_count(
          snapshot,
          :measured,
          [:docket, :checkpoint, :committed],
          %{checkpoint_type: "run_completed"}
        )

      completed_total =
        Docket.Benchmark.Collector.observation_count(
          snapshot,
          [:docket, :checkpoint, :committed],
          %{checkpoint_type: "run_completed"}
        )

      terminal_checkpoint_at =
        Docket.Benchmark.Collector.observed_at_max(
          snapshot,
          [:docket, :checkpoint, :committed],
          %{checkpoint_type: "run_completed"}
        )

      unless is_integer(terminal_checkpoint_at) do
        raise "steady-arrival terminal checkpoint timestamp is unavailable"
      end

      terminal_drain_duration = max(terminal_checkpoint_at - t0, 0)
      completion_poll_detection_delay = max(completion_detected_at - terminal_checkpoint_at, 0)

      # All offsets are strictly inside the arrival window, so exact terminal
      # telemetry gives exact aggregate outstanding work at the boundary even
      # when the control SQL query waits behind saturated Repo work.
      end_backlog = max(schedule.staged_runs - completed_in_window, 0)
      start_backlog = 0
      duration_seconds = config.duration_ms / 1_000

      activation_sample = annotate_boundary_sample(activation_sample, 0, "activation")

      window_end_sample =
        annotate_boundary_sample(
          window_end_sample,
          config.duration_ms * 1_000,
          "arrival_window_end"
        )

      completion_detection_offset_us =
        System.convert_time_unit(max(completion_detected_at - t0, 0), :native, :microsecond)

      post_detection_sample =
        annotate_boundary_sample(
          post_detection_sample,
          completion_detection_offset_us,
          "post_completion_detection"
        )

      observed_sql_span_us = max(window_end_sample.offset_us - activation_sample.offset_us, 0)

      oldest_due_lag_growth =
        observed_sample_slope(
          activation_sample.metrics.oldest_due_lag_ms,
          window_end_sample.metrics.oldest_due_lag_ms,
          observed_sql_span_us
        )

      %{
        configured_arrival_rate_per_second: config.arrival_rate,
        offered_rate_per_second: Float.round(config.runs / duration_seconds, 3),
        achieved_arrival_window_rate_per_second:
          Float.round(completed_in_window / duration_seconds, 3),
        achieved_terminal_drain_rate_per_second: rate(config.runs, terminal_drain_duration),
        terminal_drain_duration_us:
          System.convert_time_unit(terminal_drain_duration, :native, :microsecond),
        completion_poll_detection_delay_us:
          System.convert_time_unit(
            completion_poll_detection_delay,
            :native,
            :microsecond
          ),
        completion_poll_detected_offset_us: completion_detection_offset_us,
        completed_by_arrival_window_end: completed_in_window,
        due_outstanding_at_arrival_window_end: end_backlog,
        terminal_drain_after_arrival_window_us:
          System.convert_time_unit(
            max(terminal_checkpoint_at - window_end, 0),
            :native,
            :microsecond
          ),
        retained_completion_lag_us: Docket.Benchmark.Stats.native_distribution(terminal_lags),
        backlog_growth_runs_per_second:
          Float.round((end_backlog - start_backlog) / duration_seconds, 3),
        backlog_growth_scope:
          "net due-not-terminal accumulation from the empty pre-arrival boundary to exact window end; not instantaneous growth or sustainability proof",
        oldest_due_lag_growth_ms_per_second: oldest_due_lag_growth,
        oldest_due_lag_growth_sample_span_us: observed_sql_span_us,
        oldest_due_lag_growth_scope:
          "between observed SQL sample starts, not the requested boundary instants",
        backlog_state_at_window_end: backlog_state(start_backlog, end_backlog),
        backlog_drained_after_window: end_backlog > 0 and completed_total == schedule.staged_runs,
        exact_terminal_telemetry: %{
          source: "checkpoint committed run_completed telemetry observed_at",
          interval: "activation inclusive, arrival-window end exclusive",
          completed_in_arrival_window: completed_in_window,
          completed_through_final_drain: completed_total,
          last_terminal_checkpoint_offset_us:
            System.convert_time_unit(terminal_checkpoint_at - t0, :native, :microsecond),
          completion_poll_detection_delay_us:
            System.convert_time_unit(
              completion_poll_detection_delay,
              :native,
              :microsecond
            ),
          scheduled_due_by_arrival_window_end: schedule.staged_runs,
          due_outstanding_at_arrival_window_end: end_backlog,
          boundary_accounting_exact: true
        },
        boundary_samples: %{
          activation: activation_sample,
          arrival_window_end: window_end_sample,
          post_completion_detection: post_detection_sample
        },
        timeline:
          Map.merge(timeline, %{
            sampling_scope: "activation through final backlog drain",
            whole_run_summary: true
          })
      }
    end

    @doc false
    def annotate_boundary_sample(sample, requested_offset_us, phase)
        when is_map(sample) and is_integer(requested_offset_us) do
      delay_us = max(sample.offset_us - requested_offset_us, 0)
      callback_duration_us = Map.get(sample.metrics, :sampler_probe_callback_duration_us, 0)
      completion_offset_us = sample.offset_us + callback_duration_us

      Map.merge(sample, %{
        phase: phase,
        requested_boundary_offset_us: requested_offset_us,
        observed_sample_start_offset_us: sample.offset_us,
        observed_sample_completion_offset_us: completion_offset_us,
        sample_start_delay_us: delay_us,
        sample_callback_duration_us: callback_duration_us,
        requested_to_sample_completion_us: max(completion_offset_us - requested_offset_us, 0),
        observation_interval_us: %{
          start_offset_us: sample.offset_us,
          end_offset_us: completion_offset_us
        },
        delay_status: if(delay_us > 0, do: "delayed", else: "same_microsecond"),
        exact_boundary_state: false,
        snapshot_scope:
          "SQL status state may be observed later within the callback interval; due cutoff and oldest-due lag use the callback-start wall clock, not the exact requested boundary"
      })
    end

    @doc false
    def observed_sample_slope(start_value, end_value, span_us)
        when is_number(start_value) and is_number(end_value) and is_integer(span_us) and
               span_us > 0 do
      Float.round((end_value - start_value) * 1_000_000 / span_us, 3)
    end

    def observed_sample_slope(_start_value, _end_value, _span_us), do: nil

    defp completion_lags(snapshot, t0, offsets_us) do
      due_offsets =
        offsets_us |> Enum.with_index(1) |> Map.new(fn {offset, id} -> {id, offset} end)

      snapshot
      |> event_records([:docket, :checkpoint, :committed])
      |> Enum.flat_map(fn {_measurements, metadata, observed_at} ->
        case {metadata[:checkpoint_type], due_offsets[metadata[:correlation_id]]} do
          {"run_completed", offset_us} when is_integer(offset_us) ->
            due_at = t0 + System.convert_time_unit(offset_us, :microsecond, :native)
            [max(observed_at - due_at, 0)]

          _other ->
            []
        end
      end)
    end

    defp backlog_state(_start, 0), do: "drained_by_window_end"
    defp backlog_state(_start, _finish), do: "outstanding_at_window_end"

    defp steady_invariants(config, schedule, steady) do
      final = steady.boundary_samples.post_completion_detection.metrics
      terminal = steady.exact_terminal_telemetry
      timeline = steady.timeline

      [
        invariant("every steady-arrival run is scheduled", schedule.staged_runs, config.runs),
        invariant("steady-arrival schedule starts at activation", hd(schedule.offsets_us), 0),
        invariant(
          "steady-arrival schedule ends inside the arrival window",
          List.last(schedule.offsets_us) < config.duration_ms * 1_000,
          true
        ),
        invariant(
          "exact terminal window accounting covers every scheduled arrival",
          terminal.completed_in_arrival_window +
            terminal.due_outstanding_at_arrival_window_end,
          config.runs
        ),
        invariant(
          "exact terminal telemetry sees every completion through final drain",
          terminal.completed_through_final_drain,
          config.runs
        ),
        invariant(
          "steady-arrival post-detection SQL sample sees every completion",
          final.completed_runs,
          config.runs
        ),
        invariant(
          "steady-arrival post-detection SQL sample has no ready backlog",
          final.ready_unclaimed_backlog,
          0
        ),
        invariant(
          "steady-arrival post-detection SQL sample has no active claims",
          final.active_claimed_runs,
          0
        ),
        invariant("steady-arrival sampler records no failed samples", timeline.failed_samples, 0),
        invariant(
          "steady-arrival sampler buckets represent every sample",
          timeline.represented_sample_count,
          timeline.raw_sample_count
        ),
        invariant(
          "steady-arrival sampler records three phase boundaries",
          timeline.forced_phase_sample_count,
          3
        )
      ]
    end

    defp steady_warnings(config) do
      [
        "Observed steady-arrival behavior is environment-specific and exploratory.",
        "Due times are fixed before measurement; no closed-loop producer adapts to completion rate.",
        "Exact arrival-window completion and aggregate outstanding counts come from phase-scoped terminal telemetry; SQL state splits are delayed observed snapshots.",
        "Terminal drain duration and rate use the exact last run_completed checkpoint telemetry timestamp; polling detection delay is reported separately.",
        "Backlog sampling issues tagged control queries through the measured Repo, can be delayed by saturation, and can perturb very small trials.",
        "Generic throughput/latency safe-capacity recommendations are disabled for steady_arrival; interpret offered/achieved rate, exact outstanding work, and backlog/lag trends together.",
        "This point is repetition #{config.repetition} of #{config.repetitions}; use dedicated hardware and repeated cells for capacity claims."
      ]
    end
  end
end
