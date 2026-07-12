if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Benchmark.Repo do
    @moduledoc false
    use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
  end

  defmodule Docket.Benchmark.NoopNode do
    @moduledoc false
    @behaviour Docket.Node
    @impl true
    def config_schema, do: Docket.Schema.object(%{})
    @impl true
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule Docket.Benchmark.Postgres do
    @moduledoc false
    alias Docket.Benchmark.{Progress, Repo}
    alias Docket.Benchmark.Postgres.{Database, Scenario}

    @migration_version 20_260_711_000_038
    @runtime Docket.Benchmark.Runtime
    @artifact_schema_version 5
    @minimum_runtime_ready_lead_ms 250
    @unavailable "unavailable"
    @postgres_setting_names ~w(
      archive_mode
      autovacuum
      autovacuum_analyze_scale_factor
      autovacuum_analyze_threshold
      autovacuum_max_workers
      autovacuum_naptime
      autovacuum_vacuum_scale_factor
      autovacuum_vacuum_threshold
      checkpoint_completion_target
      checkpoint_timeout
      compute_query_id
      effective_cache_size
      effective_io_concurrency
      fsync
      full_page_writes
      huge_pages
      jit
      maintenance_work_mem
      max_connections
      max_parallel_workers
      max_parallel_workers_per_gather
      max_wal_size
      max_worker_processes
      min_wal_size
      random_page_cost
      seq_page_cost
      shared_buffers
      shared_preload_libraries
      ssl
      superuser_reserved_connections
      synchronous_commit
      temp_buffers
      track_io_timing
      track_wal_io_timing
      wal_buffers
      wal_compression
      wal_level
      work_mem
    )
    @database_counter_names ~w(
      xact_commit
      xact_rollback
      blks_read
      blks_hit
      tup_returned
      tup_fetched
      tup_inserted
      tup_updated
      tup_deleted
      conflicts
      temp_files
      temp_bytes
      deadlocks
      checksum_failures
      blk_read_time
      blk_write_time
      session_time
      active_time
      idle_in_transaction_time
      sessions
      sessions_abandoned
      sessions_fatal
      sessions_killed
    )a
    @knee_minimum_valid_cells 3
    @knee_minimum_successful_repetitions 3
    @knee_minimum_throughput_gain_percent 10.0
    @knee_tail_latency_increase_percent 20.0
    @bottleneck_growth_threshold_percent 20.0
    @bottleneck_minimum_latency_us 100
    @bottleneck_queue_share_threshold_percent 25.0
    @bottleneck_node_share_threshold_percent 50.0
    @bottleneck_lifecycle_share_threshold_percent 30.0
    @pruner [
      interval_ms: 86_400_000,
      event_retention_ms: 86_400_000,
      run_retention_ms: 86_400_000,
      batch_size: 100
    ]

    defmodule Migration do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    # Runner entry point: plans matrix points, isolates failures, and writes one suite result.

    def run(config) do
      case run_for_cli(config) do
        {:ok, result} -> {:ok, result}
        {:invalid, _result, reason} -> {:error, reason}
        {:error, reason} -> {:error, reason}
      end
    end

    def run_for_cli(config, opts \\ []) do
      points = Docket.Benchmark.plan(config)
      progress = Progress.start(length(points), mode: Keyword.get(opts, :progress, :off))

      execution =
        try do
          run_points(points, progress)
        after
          Progress.stop(progress)
        end

      with {:ok, artifacts} <- execution do
        payload = suite_summary_payload(artifacts)
        write_results!(config.output, config.format, artifacts, payload)

        result = %{output: Path.expand(config.output), artifact: payload, artifacts: artifacts}

        if payload.success do
          {:ok, result}
        else
          {:invalid, result,
           "one or more benchmark trials failed; results were written to #{result.output}"}
        end
      end
    end

    @doc false
    def run_points(points, progress \\ :off) do
      artifacts =
        points
        |> Enum.with_index(1)
        |> Enum.map(fn {point, index} ->
          Progress.point_started(progress, index, point)

          artifact =
            case run_point(point) do
              {:ok, artifact} -> artifact
              {:error, reason} -> failure_artifact(point, reason)
            end

          artifact = Map.put(artifact, :headline, Docket.Benchmark.Headline.build(artifact))
          Progress.point_finished(progress, index, artifact.success)
          artifact
        end)

      {:ok, artifacts}
    end

    @doc false
    def failure_artifact(config, reason) do
      {reason, cleanup, failure_stage} =
        case reason do
          %{message: message, cleanup: cleanup, stage: stage} -> {message, cleanup, stage}
          other -> {other, %{isolated_database_removed: nil}, "setup_or_execution"}
        end

      %{
        schema_version: schema_version(),
        classification: "exploratory",
        success: false,
        scenario: canonical_scenario(config.scenario),
        point: artifact_point(config),
        parameters: artifact_parameters(config),
        duration_us: nil,
        measurements: %{throughput_per_second: nil},
        invariants: [],
        warnings: [],
        started_at: nil,
        finished_at: nil,
        timing_scope: nil,
        warmup: %{requested_runs: config.warmup, completed_runs: nil},
        workload: nil,
        runtime_configuration: nil,
        environment: nil,
        observer_control: observer_trial_metadata(config),
        cleanup: cleanup,
        failure_stage: failure_stage,
        error: if(is_binary(reason), do: reason, else: inspect(reason))
      }
    end

    @doc false
    def run_point(config) do
      try do
        run_point!(config)
      rescue
        error -> {:error, Exception.message(error)}
      catch
        kind, reason -> {:error, Exception.format_banner(kind, reason)}
      end
    end

    @doc false
    def run_point!(config) do
      database = Database.isolated(config.database_url)
      previous_repo_config = Application.fetch_env(:docket, Repo)

      repo_config = [
        url: database.url,
        pool_size: config.pool_size,
        log: false,
        telemetry_prefix: [:docket, :benchmark, :repo]
      ]

      Application.put_env(:docket, Repo, repo_config)

      primary = Database.primary_config(config.database_url)

      try do
        Database.create!(primary, database.name)
        run_created_database(config, database, primary)
      after
        try do
          Docket.Postgres.GraphCache.clear()
        after
          restore_repo_config(previous_repo_config)
        end
      end
    end

    @doc false
    def run_created_database(config, database, primary) do
      execution =
        try do
          {:ok, migration_repo} = Repo.start_link(pool_size: max(config.pool_size, 2))

          try do
            :ok = Ecto.Migrator.up(Repo, @migration_version, Migration, log: false)
          after
            if Process.alive?(migration_repo),
              do: GenServer.stop(migration_repo, :normal, 5_000)
          end

          {:ok, repo} = Repo.start_link()

          try do
            execute(config, database, repo)
          after
            if Process.alive?(repo), do: GenServer.stop(repo, :normal, 5_000)
          end
        rescue
          error -> {:error, Exception.message(error)}
        catch
          kind, reason -> {:error, Exception.format_banner(kind, reason)}
        end

      merge_database_cleanup(execution, Database.drop(primary, database.name))
    end

    @doc false
    def merge_database_cleanup({:ok, artifact}, :ok) do
      {:ok, Map.put(artifact, :cleanup, %{isolated_database_removed: true})}
    end

    @doc false
    def merge_database_cleanup({:ok, artifact}, {:error, reason}) do
      cleanup_invariant = %{
        name: "isolated benchmark database removed",
        pass: false,
        expected: true,
        actual: false
      }

      {:ok,
       artifact
       |> Map.put(:success, false)
       |> Map.put(:cleanup, %{isolated_database_removed: false, error: reason})
       |> Map.update!(:invariants, &(&1 ++ [cleanup_invariant]))}
    end

    @doc false
    def merge_database_cleanup({:error, reason}, :ok) do
      {:error,
       %{
         message: reason,
         cleanup: %{isolated_database_removed: true},
         stage: "setup_or_execution"
       }}
    end

    @doc false
    def merge_database_cleanup({:error, reason}, {:error, cleanup_reason}) do
      {:error,
       %{
         message: "#{reason}; isolated database cleanup also failed: #{cleanup_reason}",
         cleanup: %{isolated_database_removed: false, error: cleanup_reason},
         stage: "setup_execution_and_cleanup"
       }}
    end

    @doc false
    def restore_repo_config({:ok, config}), do: Application.put_env(:docket, Repo, config)
    @doc false
    def restore_repo_config(:error), do: Application.delete_env(:docket, Repo)

    @doc false
    def execute(config, database, repo) do
      scenario = Scenario.fetch!(config.scenario)
      scenario.run(config, %{database: database, repo: repo})
    end

    # Scenario implementations. Selection belongs to Postgres.Scenario; these functions keep
    # the execution mechanics close to the shared measurement helpers they depend on.

    @doc false
    def stage_claim_only(expired_ids, ready_at, expired_claimed_at) do
      Ecto.Adapters.SQL.query!(
        Repo,
        "UPDATE docket_runs SET claim_token = NULL, claimed_at = NULL, wake_at = $1, claim_attempts = 0, poisoned_at = NULL, poison_reason = NULL",
        [ready_at]
      )

      Repo.transaction(fn ->
        Enum.each(expired_ids, fn run_id ->
          Ecto.Adapters.SQL.query!(
            Repo,
            "UPDATE docket_runs SET claim_token = $1, claimed_at = $2, wake_at = NULL, claim_attempts = 1 WHERE run_id = $3",
            [Ecto.UUID.generate() |> Ecto.UUID.dump!(), expired_claimed_at, run_id]
          )
        end)
      end)
    end

    # Blocked-vehicle observations and invariants.

    @doc false
    def run_claimers!(config, claim_now, leases, counters) do
      policy = %{
        now: claim_now,
        limit: config.batch_size,
        orphan_ttl_ms: config.orphan_ttl_ms,
        max_claim_attempts: 2,
        preference: nil
      }

      1..config.concurrency
      |> Task.async_stream(
        fn _worker -> claim_worker(config.runs, policy, leases, counters) end,
        max_concurrency: config.concurrency,
        ordered: false,
        timeout: config.timeout_ms,
        on_timeout: :kill_task
      )
      |> Enum.each(fn
        {:ok, :ok} -> :ok
        {:exit, reason} -> raise "claim worker failed: #{inspect(reason)}"
      end)
    end

    @doc false
    def claim_worker(backlog, policy, leases, counters) do
      if :atomics.get(counters, 1) >= backlog do
        :ok
      else
        case Docket.Postgres.RunStore.claim_due(Repo, :system, policy) do
          {:ok, %{leases: claimed, poisoned: []}} ->
            Enum.each(claimed, &record_claim!(&1, leases, counters))

            if claimed == [] do
              :ok
            else
              claim_worker(backlog, policy, leases, counters)
            end

          {:ok, %{poisoned: poisoned}} ->
            :atomics.add(counters, 3, length(poisoned))
            raise "claim-only scenario unexpectedly poisoned #{length(poisoned)} rows"

          {:error, reason} ->
            raise "claim-only scan failed: #{inspect(reason)}"
        end
      end
    end

    @doc false
    def record_claim!(lease, leases, counters) do
      unique_id? = :ets.insert_new(leases, {{:run, lease.run_id}, true})
      unique_token? = :ets.insert_new(leases, {{:token, lease.claim_token}, true})

      if unique_id? and unique_token? do
        :atomics.add(counters, 1, 1)
      else
        :atomics.add(counters, 2, 1)
        raise "claim-only scenario observed a duplicate lease identity"
      end
    end

    @doc false
    def wait_for_blocking_plateau(%{pid: gate}, expected, timeout_ms) do
      receive do
        {:docket_benchmark_blocking_plateau, ^gate, ^expected, observed_at} -> observed_at
      after
        timeout_ms -> raise "blocked-vehicle benchmark timed out before reaching its plateau"
      end
    end

    @doc false
    def wait_for_blocked_topology(gate, expected, timeout_ms) do
      deadline = System.monotonic_time(:millisecond) + timeout_ms
      do_wait_for_blocked_topology(gate, expected, deadline)
    end

    @doc false
    def do_wait_for_blocked_topology(gate, expected, deadline) do
      blocked = Docket.Benchmark.BlockingGate.blocked_pids(gate) |> MapSet.new()
      children = Task.Supervisor.children(vehicle_supervisor_name()) |> MapSet.new()
      vehicles = MapSet.difference(children, blocked)

      snapshot = %{
        blocked_node_processes: MapSet.size(blocked),
        vehicle_processes: MapSet.size(vehicles),
        supervised_dynamic_children: MapSet.size(children)
      }

      cond do
        snapshot.blocked_node_processes == expected and snapshot.vehicle_processes == expected ->
          snapshot

        System.monotonic_time(:millisecond) >= deadline ->
          raise "blocked-vehicle benchmark topology did not stabilize: #{inspect(snapshot)}"

        true ->
          Process.sleep(1)
          do_wait_for_blocked_topology(gate, expected, deadline)
      end
    end

    @doc false
    def blocked_plateau_snapshot(
          config,
          collector,
          topology,
          initial_event_rows,
          initial_checkpoint_sum
        ) do
      %{
        active_claims:
          benchmark_scalar("SELECT count(*) FROM docket_runs WHERE claim_token IS NOT NULL"),
        distinct_active_claim_tokens:
          benchmark_scalar(
            "SELECT count(DISTINCT claim_token) FROM docket_runs WHERE claim_token IS NOT NULL"
          ),
        ready_unclaimed_runs:
          benchmark_scalar(
            "SELECT count(*) FROM docket_runs WHERE status = 'running' AND claim_token IS NULL AND wake_at IS NOT NULL"
          ),
        summed_claim_attempts:
          benchmark_scalar("SELECT coalesce(sum(claim_attempts), 0)::bigint FROM docket_runs"),
        maximum_claim_attempt:
          benchmark_scalar("SELECT coalesce(max(claim_attempts), 0) FROM docket_runs"),
        maximum_claim_age_ms: benchmark_maximum_claim_age_ms(),
        poisoned_runs:
          benchmark_scalar("SELECT count(*) FROM docket_runs WHERE poisoned_at IS NOT NULL"),
        durable_event_rows: benchmark_scalar("SELECT count(*) FROM docket_events"),
        expected_initial_event_rows: initial_event_rows,
        checkpoint_sequence_sum:
          benchmark_scalar("SELECT coalesce(sum(checkpoint_seq), 0)::bigint FROM docket_runs"),
        expected_initial_checkpoint_sequence_sum: initial_checkpoint_sum,
        correlated_terminal_commits:
          Docket.Benchmark.Collector.count(
            collector,
            [:docket, :checkpoint, :committed],
            %{checkpoint_type: "run_completed"}
          ),
        correlated_completion_events:
          Docket.Benchmark.Collector.count(collector, [:docket, :run, :completed]),
        repo_pool: repo_pool_snapshot(config),
        topology: topology
      }
    end

    @doc false
    def run_short_work_probes(config) do
      Enum.map(1..config.probe_count, fn _probe ->
        before = repo_pool_snapshot(config)
        started = System.monotonic_time()

        %{rows: [[1]]} =
          Ecto.Adapters.SQL.query!(Repo, "SELECT 1", [],
            telemetry_options: [benchmark_query: :probe]
          )

        duration = System.monotonic_time() - started
        after_snapshot = repo_pool_snapshot(config)

        %{
          success: true,
          duration: duration,
          busy_or_unavailable_before: before.busy_or_unavailable_connections,
          queue_before: before.checkout_queue_length,
          busy_or_unavailable_after: after_snapshot.busy_or_unavailable_connections,
          queue_after: after_snapshot.checkout_queue_length
        }
      end)
    end

    @doc false
    def blocked_system_sample(config, gate, event_gauges, activation_at) do
      gate_gauges = Docket.Benchmark.BlockingGate.gauges(gate)
      pool = repo_pool_snapshot(config)
      memory = :erlang.memory()
      scheduler_queues = :erlang.statistics(:run_queue_lengths)
      unclaimed = max(config.runs - event_gauges.cumulative_claim_leases, 0)

      wake_age_ms =
        if unclaimed > 0 do
          max(System.monotonic_time() - activation_at, 0)
          |> System.convert_time_unit(:native, :millisecond)
        else
          0
        end

      %{
        blocked_node_calls: gate_gauges.currently_blocked,
        maximum_blocked_node_calls: gate_gauges.maximum_blocked,
        derived_unclaimed_common_due_backlog: unclaimed,
        derived_oldest_unclaimed_wake_at_age_ms: wake_age_ms,
        repo_ready_connections: pool.ready_connections,
        repo_checkout_queue_length: pool.checkout_queue_length,
        repo_busy_or_unavailable_connections: pool.busy_or_unavailable_connections,
        beam_run_queue_total: :erlang.statistics(:run_queue),
        beam_normal_scheduler_run_queue_max:
          scheduler_queues
          |> Enum.take(System.schedulers_online())
          |> Enum.max(fn -> 0 end),
        beam_process_count: :erlang.system_info(:process_count),
        beam_memory_total_bytes: memory[:total],
        beam_memory_processes_bytes: memory[:processes],
        beam_memory_binary_bytes: memory[:binary],
        beam_memory_ets_bytes: memory[:ets],
        runtime_mailbox_length: mailbox_length(@runtime),
        dispatcher_mailbox_length: mailbox_length(dispatcher_name()),
        vehicle_supervisor_mailbox_length: mailbox_length(vehicle_supervisor_name()),
        repo_pool_mailbox_length: mailbox_length(repo_pool_pid())
      }
    end

    @doc false
    def blocked_measurements(
          snapshot,
          t0,
          plateau_at,
          stable_hold_started_at,
          release,
          run_ids,
          probes,
          plateau,
          gate_final,
          timeline,
          phase_samples,
          maximum_claim_age_ms_at_release,
          quiescence_at,
          config
        ) do
      events =
        snapshot
        |> event_list()
        |> Enum.filter(fn {_event, _measurements, _metadata, observed_at} ->
          observed_at >= t0
        end)

      checkpoints = event_records(events, [:docket, :checkpoint, :committed])
      first_commit_times = correlation_times(checkpoints, :first)

      terminal_commit_times =
        checkpoints
        |> Enum.filter(fn {_measurements, metadata, _observed_at} ->
          metadata.checkpoint_type == "run_completed"
        end)
        |> correlation_times(:last)

      ordinals = run_ids |> Enum.with_index(1) |> Map.new()
      blocked_ordinals = release.blocked_run_ids |> Enum.map(&ordinals[&1]) |> MapSet.new()

      release_to_first =
        for {ordinal, observed_at} <- first_commit_times,
            MapSet.member?(blocked_ordinals, ordinal),
            do: observed_at - release.release_started_at

      release_to_terminal =
        for {ordinal, observed_at} <- terminal_commit_times,
            MapSet.member?(blocked_ordinals, ordinal),
            do: observed_at - release.release_started_at

      node_executions = event_measurements(events, [:docket, :node, :execution])
      drains = event_measurements(events, [:docket, :postgres, :vehicle, :drain])
      probe_durations = Enum.map(probes, & &1.duration)

      probe_query_measurements =
        events
        |> event_records([:docket, :benchmark, :repo, :query])
        |> Enum.filter(fn {_measurement, metadata, _at} -> metadata.benchmark_query == :probe end)
        |> Enum.map(fn {measurement, _metadata, _at} -> measurement end)

      invalid_negative_release_offsets =
        Enum.count(release_to_first ++ release_to_terminal, &(&1 < 0))

      maximum_probe_busy =
        probes
        |> Enum.flat_map(&[&1.busy_or_unavailable_before, &1.busy_or_unavailable_after])
        |> Enum.max(fn -> 0 end)

      maximum_probe_queue =
        probes
        |> Enum.flat_map(&[&1.queue_before, &1.queue_after])
        |> Enum.max(fn -> 0 end)

      node_execution_count =
        exact_observation_count(
          snapshot,
          [:docket, :node, :execution],
          %{},
          length(node_executions)
        )

      drain_count =
        exact_observation_count(
          snapshot,
          [:docket, :postgres, :vehicle, :drain],
          %{},
          length(drains)
        )

      probe_query_count =
        exact_observation_count(
          snapshot,
          [:docket, :benchmark, :repo, :query],
          %{benchmark_query: :probe},
          length(probe_query_measurements)
        )

      correlation_capture = correlation_summary(snapshot, checkpoints, [])
      full_correlation_capture = correlation_capture.full_population_shape_coverage

      release_pairs_complete =
        length(release_to_first) == length(release_to_terminal) and
          if full_correlation_capture,
            do: length(release_to_first) == config.concurrency,
            else: release_to_first != []

      exact_global_counts_pass =
        gate_final.observed_runs == config.runs and gate_final.duplicate_runs == 0 and
          gate_final.unknown_runs == 0 and gate_final.invalid_attempts == 0 and
          gate_final.invalid_nodes == 0 and node_execution_count == config.runs and
          drain_count == config.runs and
          length(release.blocked_arrival_times) == config.concurrency and
          Enum.count(probes, & &1.success) == config.probe_count and
          probe_query_count == config.probe_count

      retained_shape_checks_pass =
        if correlation_capture.sampled_expected == 0,
          do: nil,
          else: release_pairs_complete and invalid_negative_release_offsets == 0

      telemetry_checks_pass =
        exact_global_counts_pass and retained_shape_checks_pass != false

      %{
        plateau_fill_duration_us:
          System.convert_time_unit(plateau_at - t0, :native, :microsecond),
        stable_hold_duration_us:
          System.convert_time_unit(
            release.release_started_at - stable_hold_started_at,
            :native,
            :microsecond
          ),
        release_fanout_duration_us:
          System.convert_time_unit(release.fanout_duration, :native, :microsecond),
        latency: %{
          activation_to_blocked_node_offset_us:
            Docket.Benchmark.Stats.native_distribution(
              Enum.map(release.blocked_arrival_times, &(&1 - t0))
            ),
          gate_release_to_first_commit_us:
            Docket.Benchmark.Stats.native_distribution(release_to_first),
          gate_release_to_terminal_commit_us:
            Docket.Benchmark.Stats.native_distribution(release_to_terminal),
          unrelated_short_query_round_trip_us:
            Docket.Benchmark.Stats.native_distribution(probe_durations),
          unrelated_short_query_query_time_us:
            native_metric_distribution(probe_query_measurements, :query_time),
          unrelated_short_query_queue_time_us:
            native_metric_distribution(probe_query_measurements, :queue_time),
          unrelated_short_query_decode_time_us:
            native_metric_distribution(probe_query_measurements, :decode_time)
        },
        phase_boundaries_us: %{
          activation: 0,
          plateau_reached: System.convert_time_unit(plateau_at - t0, :native, :microsecond),
          stable_hold_started:
            System.convert_time_unit(stable_hold_started_at - t0, :native, :microsecond),
          gate_release_started:
            System.convert_time_unit(release.release_started_at - t0, :native, :microsecond),
          gate_release_completed:
            System.convert_time_unit(release.release_completed_at - t0, :native, :microsecond),
          vehicle_quiescence: System.convert_time_unit(quiescence_at - t0, :native, :microsecond)
        },
        plateau_resource_samples: phase_samples,
        claim_freshness: %{
          maximum_claim_age_ms_at_release: maximum_claim_age_ms_at_release,
          maximum_vehicle_reported_claim_held_ms:
            exact_numeric_max(
              snapshot,
              [:docket, :postgres, :vehicle, :drain],
              :claim_held_ms,
              max_value(drains, :claim_held_ms)
            )
        },
        probes: %{
          requested: config.probe_count,
          successful: Enum.count(probes, & &1.success),
          maximum_busy_or_unavailable_connections_before_or_after: maximum_probe_busy,
          maximum_checkout_queue_length_before_or_after: maximum_probe_queue,
          caveat:
            "Pool capacity minus ready connections is busy-or-unavailable capacity, not an exact checkout count."
        },
        plateau: plateau,
        gate: Map.drop(gate_final, [:open]),
        timeline:
          Map.merge(timeline, %{
            sampling_scope: "activation through vehicle-drain quiescence before runtime shutdown",
            whole_run_summary: true,
            derived_common_due_metrics: [
              :derived_unclaimed_common_due_backlog,
              :derived_oldest_unclaimed_wake_at_age_ms
            ],
            derived_common_due_source: "common_due_activation_boundary_and_cumulative_leases"
          }),
        collection: %{
          exact_global_counts: %{
            expected_blocked_arrivals: config.concurrency,
            blocked_arrivals: length(release.blocked_arrival_times),
            expected_gate_events: config.runs,
            gate_events: gate_final.observed_runs,
            expected_node_execution_events: config.runs,
            node_execution_events: node_execution_count,
            expected_vehicle_drain_events: config.runs,
            vehicle_drain_events: drain_count,
            expected_probe_query_events: config.probe_count,
            probe_query_events: probe_query_count,
            checks_pass: exact_global_counts_pass
          },
          retained_per_run_shape_evidence: %{
            status:
              if(correlation_capture.sampled_expected == 0,
                do: "unavailable",
                else: "available"
              ),
            scope: correlation_capture.per_run_shape_scope,
            sampled_runs: correlation_capture.sampled_expected,
            population_runs: correlation_capture.population_expected,
            covers_full_population: full_correlation_capture,
            release_to_first_commit_pairs: length(release_to_first),
            release_to_terminal_commit_pairs: length(release_to_terminal),
            negative_release_offsets: invalid_negative_release_offsets,
            checks_pass: retained_shape_checks_pass
          },
          retained_distribution_samples: %{
            node_execution_durations: length(node_executions),
            vehicle_drain_durations: length(drains),
            probe_query_durations: length(probe_query_measurements)
          },
          telemetry_checks_pass: telemetry_checks_pass
        }
      }
    end

    @doc false
    def blocked_invariants(config, plateau, gate, blocked, measurements) do
      sampled_in_flight_max =
        get_in(blocked, [
          :timeline,
          :summary,
          :dispatcher_in_flight_vehicles,
          :max
        ])

      sampled_blocked_max = get_in(blocked, [:timeline, :summary, :blocked_node_calls, :max])
      scheduled_samples = blocked.timeline.scheduled_sample_count

      sampling_quality? =
        blocked.timeline.missed_ticks <= max(scheduled_samples, 1) and
          blocked.timeline.observer_diagnostics.serial_sampler_duty_cycle_percent <= 50.0

      [
        invariant(
          "blocked plateau reaches configured concurrency",
          plateau.active_claims,
          config.concurrency
        ),
        invariant(
          "blocked plateau claim tokens are distinct",
          plateau.distinct_active_claim_tokens,
          config.concurrency
        ),
        invariant(
          "unclaimed backlog remains ready during the plateau",
          plateau.ready_unclaimed_runs,
          config.runs - config.concurrency
        ),
        invariant(
          "plateau claims are first attempts",
          plateau.summed_claim_attempts,
          config.concurrency
        ),
        invariant("plateau maximum claim attempt is one", plateau.maximum_claim_attempt, 1),
        invariant(
          "claims remain younger than the orphan TTL at the plateau snapshot",
          plateau.maximum_claim_age_ms < config.orphan_ttl_ms,
          true
        ),
        invariant("plateau has no poisoned runs", plateau.poisoned_runs, 0),
        invariant(
          "blocked node work appends no durable events before release",
          plateau.durable_event_rows,
          plateau.expected_initial_event_rows
        ),
        invariant(
          "blocked node work advances no checkpoint before release",
          plateau.checkpoint_sequence_sum,
          plateau.expected_initial_checkpoint_sequence_sum
        ),
        invariant(
          "no terminal commit occurs before release",
          plateau.correlated_terminal_commits,
          0
        ),
        invariant(
          "no completion event occurs before release",
          plateau.correlated_completion_events,
          0
        ),
        invariant(
          "blocked node process count matches concurrency",
          plateau.topology.blocked_node_processes,
          config.concurrency
        ),
        invariant(
          "resident vehicle process count matches concurrency",
          plateau.topology.vehicle_processes,
          config.concurrency
        ),
        invariant(
          "vehicle supervisor contains one vehicle and node task per slot",
          plateau.topology.supervised_dynamic_children,
          config.concurrency * 2
        ),
        invariant(
          "Repo has no busy-or-unavailable capacity at the stable blocked snapshot",
          plateau.repo_pool.busy_or_unavailable_connections,
          0
        ),
        invariant(
          "Repo checkout queue is empty at the stable blocked snapshot",
          plateau.repo_pool.checkout_queue_length,
          0
        ),
        invariant("gate observes every run exactly once", gate.observed_runs, config.runs),
        invariant("gate sees no duplicate runs", gate.duplicate_runs, 0),
        invariant("gate sees no unknown runs", gate.unknown_runs, 0),
        invariant("gate sees only first attempts", gate.invalid_attempts, 0),
        invariant("gate sees only the blocking node", gate.invalid_nodes, 0),
        invariant(
          "gate reaches the configured blocked maximum",
          gate.maximum_blocked,
          config.concurrency
        ),
        invariant(
          "stable hold lasts at least the requested duration",
          blocked.stable_hold_duration_us >= config.hold_ms * 1_000,
          true
        ),
        invariant(
          "claims remain younger than the orphan TTL at release",
          blocked.claim_freshness.maximum_claim_age_ms_at_release < config.orphan_ttl_ms,
          true
        ),
        invariant(
          "vehicle-reported claim hold stays below the orphan TTL",
          blocked.claim_freshness.maximum_vehicle_reported_claim_held_ms <
            config.orphan_ttl_ms,
          true
        ),
        invariant("all short-work probes succeed", blocked.probes.successful, config.probe_count),
        invariant(
          "short-work probes observe no pre/post queue",
          blocked.probes.maximum_checkout_queue_length_before_or_after,
          0
        ),
        invariant(
          "short-work probes observe no pre/post busy-or-unavailable capacity",
          blocked.probes.maximum_busy_or_unavailable_connections_before_or_after,
          0
        ),
        invariant(
          "sampler observes the current in-flight plateau",
          sampled_in_flight_max,
          config.concurrency
        ),
        invariant(
          "sampler observes the current blocked-node plateau",
          sampled_blocked_max,
          config.concurrency
        ),
        invariant(
          "stable-start milestone observes the in-flight plateau",
          get_in(blocked, [
            :plateau_resource_samples,
            :stable_start,
            :metrics,
            :dispatcher_in_flight_vehicles
          ]),
          config.concurrency
        ),
        invariant(
          "pre-release milestone observes the blocked plateau",
          get_in(blocked, [
            :plateau_resource_samples,
            :pre_release,
            :metrics,
            :blocked_node_calls
          ]),
          config.concurrency
        ),
        invariant(
          "sampler records both plateau milestones",
          blocked.timeline.forced_phase_sample_count,
          2
        ),
        invariant(
          "sampler buckets represent every captured sample",
          blocked.timeline.represented_sample_count,
          blocked.timeline.raw_sample_count
        ),
        invariant("sampler cadence remains usable", sampling_quality?, true),
        invariant("sampler records no failed samples", blocked.timeline.failed_samples, 0),
        invariant(
          "release-to-commit offsets remain non-negative",
          blocked.collection.retained_per_run_shape_evidence.negative_release_offsets,
          0
        ),
        invariant(
          "blocked workload telemetry checks pass",
          blocked.collection.telemetry_checks_pass,
          true
        ),
        invariant(
          "benchmark probes are excluded from workload Repo query counts",
          measurements.counts.benchmark_probe_queries_excluded,
          config.probe_count
        ),
        invariant(
          "plateau control queries are excluded from workload Repo query counts",
          measurements.counts.benchmark_control_queries_excluded,
          10
        )
      ]
    end

    @doc false
    def repo_pool_snapshot(config) do
      metrics = DBConnection.get_connection_metrics(repo_pool_pid())
      ready = Enum.reduce(metrics, 0, &(&1.ready_conn_count + &2))
      queued = Enum.reduce(metrics, 0, &(&1.checkout_queue_length + &2))
      capacity = config.pool_size * (Repo.config()[:pool_count] || 1)

      %{
        capacity: capacity,
        ready_connections: ready,
        checkout_queue_length: queued,
        busy_or_unavailable_connections: max(capacity - ready, 0),
        source: "DBConnection.get_connection_metrics"
      }
    end

    # Shared runtime lifecycle and activation synchronization.

    @doc false
    def repo_pool_pid do
      %{pid: pool} = Ecto.Adapter.lookup_meta(Repo.get_dynamic_repo())
      pool
    end

    @doc false
    def mailbox_length(name_or_pid) do
      pid = if is_pid(name_or_pid), do: name_or_pid, else: Process.whereis(name_or_pid)

      case pid && Process.info(pid, :message_queue_len) do
        {:message_queue_len, length} -> length
        _ -> nil
      end
    end

    @doc false
    def backend_name, do: Module.concat(@runtime, "Backend")
    @doc false
    def dispatcher_name, do: Docket.Postgres.dispatcher_name(backend_name())
    @doc false
    def vehicle_supervisor_name, do: Docket.Postgres.vehicle_supervisor_name(backend_name())

    @doc false
    def cleanup_sampler(sampler) do
      if Process.alive?(sampler.pid) do
        try do
          Docket.Benchmark.Sampler.stop(sampler)
        rescue
          _error -> :ok
        catch
          _kind, _reason -> :ok
        end
      else
        :telemetry.detach(sampler.handler_id)
      end
    end

    @doc false
    def runtime_opts(config, extra \\ []) do
      opts =
        [
          name: @runtime,
          backend: Docket.Postgres,
          repo: Repo,
          notifier: :none,
          dispatcher: [
            concurrency: config.concurrency,
            poll_interval_ms: config.poll_interval_ms,
            orphan_ttl_ms: config.orphan_ttl_ms
          ],
          pruner: @pruner
        ]

      opts =
        if config.scenario == "cyclic_vs_one_step" do
          Keyword.put(opts, :vehicle, drain_budget: cyclic_drain_budget(config))
        else
          opts
        end

      opts ++ extra
    end

    @doc false
    def cyclic_drain_budget(config) do
      [max_moments: config.drain_max_moments]
      |> then(fn budget ->
        if config.drain_max_elapsed_ms,
          do: Keyword.put(budget, :max_elapsed_ms, config.drain_max_elapsed_ms),
          else: budget
      end)
    end

    @doc false
    def seed_runs(ref, count) do
      Enum.map(1..count, fn _ ->
        {:ok, run} = Docket.start_run(@runtime, ref, %{})
        run.id
      end)
    end

    @doc false
    def wait_for_completion(collector, expected, timeout_ms) do
      deadline = System.monotonic_time(:millisecond) + timeout_ms
      wait(collector, expected, deadline)
    end

    @doc false
    def wait_for_vehicle_quiescence(collector, sampler, expected, timeout_ms) do
      deadline = System.monotonic_time(:millisecond) + timeout_ms
      do_wait_for_vehicle_quiescence(collector, sampler, expected, deadline)
    end

    @doc false
    def do_wait_for_vehicle_quiescence(collector, sampler, expected, deadline) do
      drains =
        Docket.Benchmark.Collector.count(collector, [:docket, :postgres, :vehicle, :drain])

      in_flight = Docket.Benchmark.Sampler.snapshot(sampler).dispatcher_in_flight_vehicles

      cond do
        drains >= expected and in_flight == 0 ->
          :ok

        System.monotonic_time(:millisecond) >= deadline ->
          raise "benchmark timed out with #{drains}/#{expected} vehicle drains and #{in_flight} vehicles still in flight"

        true ->
          Process.sleep(1)
          do_wait_for_vehicle_quiescence(collector, sampler, expected, deadline)
      end
    end

    @doc false
    def wait(collector, expected, deadline) do
      completed = Docket.Benchmark.Collector.count(collector, [:docket, :run, :completed])

      terminal =
        Docket.Benchmark.Collector.count(
          collector,
          [:docket, :checkpoint, :committed],
          %{checkpoint_type: "run_completed"}
        )

      cond do
        completed >= expected and terminal >= expected ->
          :ok

        System.monotonic_time(:millisecond) >= deadline ->
          raise "benchmark timed out with #{completed}/#{expected} completion events and #{terminal}/#{expected} terminal commits"

        true ->
          Process.sleep(1)
          wait(collector, expected, deadline)
      end
    end

    @doc false
    def stage_activation(activation_at) do
      Ecto.Adapters.SQL.query!(
        Repo,
        "UPDATE docket_runs SET wake_at = $1 WHERE status = 'running' AND claim_token IS NULL",
        [activation_at]
      )
    end

    @doc false
    def run_warmup(config, run_ids) do
      Docket.Postgres.GraphCache.clear()
      {activation_at, activation_monotonic} = activation_boundary()
      stage_activation(activation_at)
      collector = Docket.Benchmark.Collector.start(run_ids)
      runtime = start_runtime!(runtime_opts(config), collector)

      try do
        sleep_until(activation_monotonic)
        wait_for_completion(collector, config.warmup, config.timeout_ms)

        %{
          requested_runs: config.warmup,
          completed_runs:
            Docket.Benchmark.Collector.count(collector, [:docket, :run, :completed]),
          graph_cache_state: "warm"
        }
      after
        if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)

        if :ets.info(collector.table) != :undefined do
          Docket.Benchmark.Collector.stop(collector)
        end
      end
    end

    @doc false
    def activation_boundary do
      lead_ms = 250

      {
        DateTime.add(DateTime.utc_now(), lead_ms, :millisecond),
        System.monotonic_time() + System.convert_time_unit(lead_ms, :millisecond, :native)
      }
    end

    @doc false
    def prepare_measured_activation(lead_ms \\ 500) do
      activation_at = DateTime.add(DateTime.utc_now(), lead_ms, :millisecond)
      stage_activation(activation_at)
      physical_before = physical_snapshot()
      remaining_ms = DateTime.diff(activation_at, DateTime.utc_now(), :millisecond)

      if remaining_ms >= @minimum_runtime_ready_lead_ms do
        t0 =
          System.monotonic_time() +
            System.convert_time_unit(remaining_ms, :millisecond, :native)

        {activation_at, t0, physical_before}
      else
        prepare_measured_activation(lead_ms * 2)
      end
    end

    @doc false
    def ensure_activation_lead!(activation_monotonic, minimum_ms) do
      remaining = activation_monotonic - System.monotonic_time()
      minimum = System.convert_time_unit(minimum_ms, :millisecond, :native)

      if remaining < minimum do
        raise "benchmark runtime setup left less than #{minimum_ms} ms before activation"
      end

      System.convert_time_unit(remaining, :native, :microsecond)
    end

    @doc false
    def sleep_until(activation_monotonic) do
      remaining = activation_monotonic - System.monotonic_time()

      if remaining > 0 do
        Process.sleep(System.convert_time_unit(remaining, :native, :millisecond))
      end
    end

    @doc false
    def sleep_until_strict(deadline) do
      remaining = deadline - System.monotonic_time()

      if remaining > 0 do
        remaining_us = System.convert_time_unit(remaining, :native, :microsecond)
        Process.sleep(div(remaining_us + 999, 1_000))
        sleep_until_strict(deadline)
      end
    end

    @doc false
    def start_runtime!(opts, collector) do
      try do
        case Docket.Runtime.Supervisor.start_link(opts) do
          {:ok, runtime} -> runtime
          {:error, reason} -> raise "benchmark runtime failed to start: #{inspect(reason)}"
        end
      rescue
        error ->
          cleanup_collector(collector)
          reraise error, __STACKTRACE__
      catch
        kind, reason ->
          cleanup_collector(collector)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end

    @doc false
    def cleanup_collector(collector) do
      if :ets.info(collector.table) != :undefined do
        Docket.Benchmark.Collector.stop(collector)
      end
    end

    @doc false
    def with_manual_runtime(opts, fun) do
      {:ok, runtime} = Docket.Runtime.Supervisor.start_link(opts)

      try do
        fun.()
      after
        if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)
      end
    end

    @doc false
    def measurements(
          snapshot,
          t0,
          duration_native,
          config,
          physical_before,
          physical_after,
          collector_stats
        ) do
      work_events = [
        [:docket, :checkpoint, :committed],
        [:docket, :run, :completed],
        [:docket, :postgres, :claim, :attempt]
      ]

      pre_activation_events =
        Enum.sum(
          Enum.map(work_events, fn event ->
            Docket.Benchmark.Collector.phase_count(snapshot, :pre_activation, event)
          end)
        )

      pre_activation_polls =
        Docket.Benchmark.Collector.phase_count(
          snapshot,
          :pre_activation,
          [:docket, :postgres, :dispatcher, :poll]
        )

      events =
        snapshot
        |> event_list()
        |> Enum.filter(fn {_event, _measurements, _metadata, observed_at} ->
          observed_at >= t0
        end)

      claim_scans = event_measurements(events, [:docket, :postgres, :run_store, :claim])
      claim_queries = event_measurements(events, [:docket, :postgres, :run_store, :claim_query])
      attempts = event_records(events, [:docket, :postgres, :claim, :attempt])
      ready_attempts = Enum.filter(attempts, fn {_m, meta, _at} -> meta.class == :ready end)
      expired_attempts = Enum.filter(attempts, fn {_m, meta, _at} -> meta.class == :expired end)
      polls = event_measurements(events, [:docket, :postgres, :dispatcher, :poll])
      states = event_measurements(events, [:docket, :postgres, :dispatcher, :state])
      drains = event_measurements(events, [:docket, :postgres, :vehicle, :drain])
      completions = event_records(events, [:docket, :run, :completed])
      checkpoints = event_records(events, [:docket, :checkpoint, :committed])
      committed = event_measurements(events, [:docket, :lifecycle, :committed])
      repo_query_records = event_records(events, [:docket, :benchmark, :repo, :query])

      repo_queries =
        repo_query_records
        |> Enum.filter(fn {_measurement, metadata, _at} ->
          metadata.benchmark_query == :workload
        end)
        |> Enum.map(fn {measurement, _metadata, _at} -> measurement end)

      benchmark_probe_queries =
        exact_observation_count(
          snapshot,
          [:docket, :benchmark, :repo, :query],
          %{benchmark_query: :probe},
          Enum.count(repo_query_records, fn {_measurement, metadata, _at} ->
            metadata.benchmark_query == :probe
          end)
        )

      benchmark_control_queries =
        exact_observation_count(
          snapshot,
          [:docket, :benchmark, :repo, :query],
          %{benchmark_query: :control},
          Enum.count(repo_query_records, fn {_measurement, metadata, _at} ->
            metadata.benchmark_query == :control
          end)
        )

      store = event_measurements(events, [:docket, :postgres, :store])

      completion_event_times = correlation_times(completions, :last)
      first_commit_times = correlation_times(checkpoints, :first)

      terminal_commit_times =
        checkpoints
        |> Enum.filter(fn {_measurements, metadata, _observed_at} ->
          metadata.checkpoint_type == "run_completed"
        end)
        |> correlation_times(:last)

      correlations = correlation_summary(snapshot, checkpoints, completions)

      completion_offsets =
        Enum.map(terminal_commit_times, fn {_id, observed_at} -> observed_at - t0 end)

      first_commit_offsets =
        Enum.map(first_commit_times, fn {_id, observed_at} -> observed_at - t0 end)

      first_to_terminal =
        for {id, first_at} <- first_commit_times,
            terminal_at = terminal_commit_times[id],
            is_integer(terminal_at),
            do: terminal_at - first_at

      flexible_checkpoint_shapes = Map.get(config, :flexible_checkpoint_shapes, false)

      invalid_checkpoint_shapes =
        invalid_frequency_count(
          correlations.checkpoint_count_frequencies,
          fn count -> if flexible_checkpoint_shapes, do: count < 2, else: count != 2 end
        )

      invalid_terminal_shapes =
        invalid_frequency_count(
          correlations.terminal_checkpoint_count_frequencies,
          &(&1 != 1)
        )

      invalid_completion_shapes =
        invalid_frequency_count(correlations.completion_count_frequencies, &(&1 != 1))

      unknown_correlation_events = Enum.sum(Map.values(correlations.unknown_events))

      terminal_checkpoint_observed_at =
        exact_observed_at_max(
          snapshot,
          [:docket, :checkpoint, :committed],
          %{checkpoint_type: "run_completed"},
          nil
        )

      burst_duration_native =
        if is_integer(terminal_checkpoint_observed_at),
          do: max(terminal_checkpoint_observed_at - t0, 0),
          else: Enum.max(completion_offsets, fn -> duration_native end)

      ready_lags = Enum.map(ready_attempts, fn {m, _meta, _at} -> m.eligible_age_ms end)

      invalid_ready_lags =
        exact_negative_count(
          snapshot,
          [:docket, :postgres, :claim, :attempt],
          :eligible_age_ms,
          Enum.count(ready_lags, &(&1 < 0))
        )

      expected_ready_claim_samples = Map.get(config, :expected_ready_claim_samples, config.runs)

      population_completion_events =
        exact_observation_count(
          snapshot,
          [:docket, :run, :completed],
          %{},
          length(completions)
        )

      population_terminal_commit_events =
        exact_observation_count(
          snapshot,
          [:docket, :checkpoint, :committed],
          %{checkpoint_type: "run_completed"},
          map_size(terminal_commit_times)
        )

      ready_attempt_count =
        exact_observation_count(
          snapshot,
          [:docket, :postgres, :claim, :attempt],
          %{class: :ready},
          length(ready_attempts)
        )

      retained_first_commit_shapes =
        correlations.sampled_expected -
          Map.get(correlations.checkpoint_count_frequencies, 0, 0)

      retained_terminal_commit_shapes =
        correlations.sampled_expected -
          Map.get(correlations.terminal_checkpoint_count_frequencies, 0, 0)

      retained_completion_shapes =
        correlations.sampled_expected -
          Map.get(correlations.completion_count_frequencies, 0, 0)

      unknown_correlation_check_passes =
        not correlations.full_population_shape_coverage or unknown_correlation_events == 0

      exact_global_counts_pass =
        ready_attempt_count == expected_ready_claim_samples and
          population_terminal_commit_events == config.runs and
          population_completion_events == config.runs and invalid_ready_lags == 0 and
          pre_activation_events == 0

      retained_shape_checks_pass =
        if correlations.sampled_expected == 0 do
          nil
        else
          invalid_checkpoint_shapes == 0 and invalid_terminal_shapes == 0 and
            invalid_completion_shapes == 0 and unknown_correlation_check_passes
        end

      full_population_uniqueness =
        full_population_uniqueness_evidence(
          snapshot,
          [:docket, :run, :completed],
          config.runs
        )

      telemetry_checks_pass =
        exact_global_counts_pass and retained_shape_checks_pass != false and
          full_population_uniqueness[:checks_pass] != false

      %{
        completion_event_count: population_completion_events,
        measured_runs: config.runs,
        throughput_per_second: rate(config.runs, burst_duration_native),
        observed_runs_per_second: rate(config.runs, burst_duration_native),
        burst_duration_us: System.convert_time_unit(burst_duration_native, :native, :microsecond),
        latency: %{
          burst_activation_to_first_commit_offset_us:
            Docket.Benchmark.Stats.native_distribution(first_commit_offsets),
          first_commit_to_terminal_us:
            Docket.Benchmark.Stats.native_distribution(first_to_terminal),
          burst_activation_to_terminal_commit_offset_us:
            Docket.Benchmark.Stats.native_distribution(completion_offsets),
          claim_scan_total_us: native_metric_distribution(claim_scans, :duration),
          claim_query_time_us: native_metric_distribution(claim_queries, :query_time),
          claim_queue_time_us: native_metric_distribution(claim_queries, :queue_time),
          claim_decode_time_us: native_metric_distribution(claim_queries, :decode_time),
          selected_ready_age_at_scan_start_ms:
            Docket.Benchmark.Stats.millisecond_distribution(Enum.reject(ready_lags, &(&1 < 0))),
          expired_claim_age_ms:
            Docket.Benchmark.Stats.millisecond_distribution(
              Enum.map(expired_attempts, fn {m, _meta, _at} -> m.eligible_age_ms end)
            ),
          expired_overdue_after_ttl_ms:
            Docket.Benchmark.Stats.millisecond_distribution(
              Enum.map(expired_attempts, fn {m, _meta, _at} -> m.overdue_after_ttl_ms end)
            ),
          dispatcher_poll_us:
            native_event_distribution(events, [:docket, :postgres, :dispatcher, :poll]),
          dispatcher_launch_us:
            native_event_distribution(events, [:docket, :postgres, :dispatcher, :launch]),
          vehicle_total_us:
            native_event_distribution(events, [:docket, :postgres, :vehicle, :stop]),
          vehicle_claim_held_ms:
            Docket.Benchmark.Stats.millisecond_distribution(Enum.map(drains, & &1.claim_held_ms)),
          vehicle_moment_loop_ms:
            Docket.Benchmark.Stats.millisecond_distribution(Enum.map(drains, & &1.elapsed_ms)),
          lifecycle_transaction_us:
            native_event_distribution(events, [:docket, :lifecycle, :transaction, :stop]),
          node_execution_us: native_event_distribution(events, [:docket, :node, :execution]),
          graph_fetch_us:
            native_event_distribution(events, [:docket, :postgres, :graph, :fetch, :stop]),
          graph_compile_us:
            native_event_distribution(events, [:docket, :postgres, :graph, :compile, :stop]),
          repo_query_time_us: native_metric_distribution(repo_queries, :query_time),
          repo_queue_time_us: native_metric_distribution(repo_queries, :queue_time)
        },
        counts: %{
          claim_scans:
            exact_observation_count(
              snapshot,
              [:docket, :postgres, :run_store, :claim],
              %{},
              length(claim_scans)
            ),
          claim_query_samples:
            exact_observation_count(
              snapshot,
              [:docket, :postgres, :run_store, :claim_query],
              %{},
              length(claim_queries)
            ),
          claim_leases:
            exact_numeric_sum(
              snapshot,
              [:docket, :postgres, :run_store, :claim],
              :leases,
              sum(claim_scans, :leases)
            ),
          claim_attempts:
            exact_observation_count(
              snapshot,
              [:docket, :postgres, :claim, :attempt],
              %{},
              length(attempts)
            ),
          reacquired_claims:
            exact_observation_count(
              snapshot,
              [:docket, :postgres, :claim, :attempt],
              %{result: :reacquired},
              Enum.count(attempts, fn {_m, meta, _at} -> meta.result == :reacquired end)
            ),
          steals:
            exact_numeric_sum(
              snapshot,
              [:docket, :postgres, :run_store, :claim],
              :steals,
              sum(claim_scans, :steals)
            ),
          poisoned:
            exact_numeric_sum(
              snapshot,
              [:docket, :postgres, :run_store, :claim],
              :poisoned,
              sum(claim_scans, :poisoned)
            ),
          dispatcher_polls:
            exact_observation_count(
              snapshot,
              [:docket, :postgres, :dispatcher, :poll],
              %{},
              length(polls)
            ),
          empty_polls:
            exact_predicate_count(
              snapshot,
              [:docket, :postgres, :dispatcher, :poll],
              :empty,
              Enum.count(polls, &(&1.leases == 0 and &1.poisoned == 0))
            ),
          maximum_in_flight_vehicles:
            exact_numeric_max(
              snapshot,
              [:docket, :postgres, :dispatcher, :state],
              :in_flight,
              max_value(states, :in_flight)
            ),
          committed_moments:
            exact_numeric_sum(
              snapshot,
              [:docket, :lifecycle, :committed],
              :count,
              sum(committed, :count)
            ),
          repo_queries:
            exact_observation_count(
              snapshot,
              [:docket, :benchmark, :repo, :query],
              %{benchmark_query: :workload},
              length(repo_queries)
            ),
          benchmark_probe_queries_excluded: benchmark_probe_queries,
          benchmark_control_queries_excluded: benchmark_control_queries
        },
        amplification:
          amplification(
            config,
            snapshot,
            committed,
            repo_queries,
            store,
            physical_before,
            physical_after,
            pre_activation_polls
          ),
        collection: %{
          percentile_method: "nearest-rank, no interpolation",
          exact_global_counts: %{
            scope: "activation inclusive through collector stop",
            expected_ready_claim_attempt_events: expected_ready_claim_samples,
            ready_claim_attempt_events: ready_attempt_count,
            expected_terminal_checkpoint_events: config.runs,
            terminal_checkpoint_events: population_terminal_commit_events,
            expected_completion_events: config.runs,
            completion_events: population_completion_events,
            negative_ready_age_events: invalid_ready_lags,
            pre_activation_work_events: pre_activation_events,
            checks_pass: exact_global_counts_pass
          },
          full_population_uniqueness: full_population_uniqueness,
          retained_per_run_shape_evidence: %{
            status:
              if(correlations.sampled_expected == 0,
                do: "unavailable",
                else: "available"
              ),
            scope: correlations.per_run_shape_scope,
            sampled_runs: correlations.sampled_expected,
            population_runs: correlations.population_expected,
            covers_full_population: correlations.full_population_shape_coverage,
            runs_with_first_checkpoint: retained_first_commit_shapes,
            runs_with_terminal_checkpoint: retained_terminal_commit_shapes,
            runs_with_completion_event: retained_completion_shapes,
            first_to_terminal_pairs: length(first_to_terminal),
            invalid_checkpoint_shapes: invalid_checkpoint_shapes,
            invalid_terminal_checkpoint_shapes: invalid_terminal_shapes,
            invalid_completion_event_shapes: invalid_completion_shapes,
            unindexed_or_unknown_correlation_events: unknown_correlation_events,
            unindexed_or_unknown_scope: correlations.unknown_correlation_scope,
            checks_pass: retained_shape_checks_pass
          },
          retained_distribution_samples: %{
            ready_claim_attempts: length(ready_attempts),
            first_commit_offsets: map_size(first_commit_times),
            first_to_terminal_pairs: length(first_to_terminal),
            terminal_commit_offsets: map_size(terminal_commit_times),
            completion_event_offsets: map_size(completion_event_times)
          },
          pre_activation_dispatcher_polls_in_physical_scope: pre_activation_polls,
          control_wait_duration_us:
            System.convert_time_unit(duration_native, :native, :microsecond),
          observer: collector_stats,
          telemetry_checks_pass: telemetry_checks_pass,
          telemetry_events: Enum.map(Docket.Benchmark.Collector.events(), &Enum.join(&1, "."))
        }
      }
    end

    # Shared event-to-artifact measurement derivations.

    @doc false
    def event_list(%Docket.Benchmark.Collector.Snapshot{} = snapshot),
      do: Docket.Benchmark.Collector.sampled_events(snapshot)

    def event_list(events) when is_list(events), do: events

    @doc false
    def exact_observation_count(
          %Docket.Benchmark.Collector.Snapshot{} = snapshot,
          event,
          metadata,
          _fallback
        ),
        do: Docket.Benchmark.Collector.observation_count(snapshot, event, metadata)

    def exact_observation_count(_events, _event, _metadata, fallback), do: fallback

    @doc false
    def full_population_uniqueness_evidence(
          %Docket.Benchmark.Collector.Snapshot{} = snapshot,
          event,
          expected
        ) do
      case Docket.Benchmark.Collector.full_population_unique_count(snapshot, event) do
        {:ok, count} ->
          %{
            status: "available",
            scope: "exact_full_population",
            expected_runs: expected,
            unique_run_count: count,
            checks_pass: count == expected
          }

        {:unavailable, scope} ->
          %{
            status: "unavailable",
            scope: to_string(scope),
            expected_runs: expected,
            reason:
              "collector did not index the full correlation population; exact raw event counts are available but full-population uniqueness is not"
          }

        {:unsupported, unsupported_event} ->
          %{
            status: "unsupported",
            scope: "unsupported_event",
            expected_runs: expected,
            event: Enum.join(unsupported_event, ".")
          }
      end
    end

    def full_population_uniqueness_evidence(_events, event, expected) do
      %{
        status: "unavailable",
        scope: "legacy_event_list_without_population_index",
        expected_runs: expected,
        event: Enum.join(event, ".")
      }
    end

    @doc false
    def exact_numeric_sum(
          %Docket.Benchmark.Collector.Snapshot{} = snapshot,
          event,
          key,
          _fallback
        ),
        do: Docket.Benchmark.Collector.numeric_sum(snapshot, event, key)

    def exact_numeric_sum(_events, _event, _key, fallback), do: fallback

    @doc false
    def exact_numeric_max(
          %Docket.Benchmark.Collector.Snapshot{} = snapshot,
          event,
          key,
          _fallback
        ),
        do: Docket.Benchmark.Collector.numeric_max(snapshot, event, key)

    def exact_numeric_max(_events, _event, _key, fallback), do: fallback

    @doc false
    def exact_observed_at_max(
          %Docket.Benchmark.Collector.Snapshot{} = snapshot,
          event,
          _fallback
        ),
        do: Docket.Benchmark.Collector.observed_at_max(snapshot, event)

    def exact_observed_at_max(_events, _event, fallback), do: fallback

    @doc false
    def exact_observed_at_max(
          %Docket.Benchmark.Collector.Snapshot{} = snapshot,
          event,
          metadata,
          _fallback
        ),
        do: Docket.Benchmark.Collector.observed_at_max(snapshot, event, metadata)

    def exact_observed_at_max(_events, _event, _metadata, fallback), do: fallback

    @doc false
    def exact_negative_count(
          %Docket.Benchmark.Collector.Snapshot{} = snapshot,
          event,
          key,
          _fallback
        ),
        do: Docket.Benchmark.Collector.negative_count(snapshot, event, key)

    def exact_negative_count(_events, _event, _key, fallback), do: fallback

    @doc false
    def exact_predicate_count(
          %Docket.Benchmark.Collector.Snapshot{} = snapshot,
          event,
          predicate,
          _fallback
        ),
        do: Docket.Benchmark.Collector.predicate_count(snapshot, event, predicate)

    def exact_predicate_count(_events, _event, _predicate, fallback), do: fallback

    @doc false
    def correlation_summary(
          %Docket.Benchmark.Collector.Snapshot{} = snapshot,
          _checkpoints,
          _completions
        ),
        do: Docket.Benchmark.Collector.correlation_summary(snapshot)

    def correlation_summary(_events, checkpoints, completions) do
      checkpoint_counts =
        checkpoints
        |> Enum.reject(fn {_measurements, metadata, _observed_at} ->
          is_nil(metadata.correlation_id)
        end)
        |> Enum.frequencies_by(fn {_measurements, metadata, _observed_at} ->
          metadata.correlation_id
        end)

      terminal_counts =
        checkpoints
        |> Enum.filter(fn {_measurements, metadata, _observed_at} ->
          metadata.checkpoint_type == "run_completed" and not is_nil(metadata.correlation_id)
        end)
        |> Enum.frequencies_by(fn {_measurements, metadata, _observed_at} ->
          metadata.correlation_id
        end)

      completion_counts =
        completions
        |> Enum.reject(fn {_measurements, metadata, _observed_at} ->
          is_nil(metadata.correlation_id)
        end)
        |> Enum.frequencies_by(fn {_measurements, metadata, _observed_at} ->
          metadata.correlation_id
        end)

      sampled = map_size(checkpoint_counts)

      %{
        expected: sampled,
        population_expected: sampled,
        sampled_expected: sampled,
        sampled: sampled,
        per_run_shape_scope: "legacy_full_event_list",
        full_population_shape_coverage: true,
        checkpoint_count_frequencies: Enum.frequencies(Map.values(checkpoint_counts)),
        terminal_checkpoint_count_frequencies: Enum.frequencies(Map.values(terminal_counts)),
        completion_count_frequencies: Enum.frequencies(Map.values(completion_counts)),
        claim_count_frequencies: %{},
        unknown_events: %{
          [:docket, :checkpoint, :committed] =>
            Enum.count(checkpoints, fn {_m, metadata, _at} ->
              is_nil(metadata.correlation_id)
            end),
          [:docket, :run, :completed] =>
            Enum.count(completions, fn {_m, metadata, _at} ->
              is_nil(metadata.correlation_id)
            end)
        },
        unknown_correlation_scope: "legacy_full_event_list"
      }
    end

    @doc false
    def invalid_frequency_count(frequencies, predicate) do
      Enum.reduce(frequencies, 0, fn {value, count}, invalid ->
        if predicate.(value), do: invalid + count, else: invalid
      end)
    end

    @doc false
    def amplification(
          config,
          snapshot,
          committed,
          repo_queries,
          store,
          before,
          after_snapshot,
          pre_activation_polls
        ) do
      event_rows = scalar("SELECT count(*) FROM docket_events")
      run_rows = scalar("SELECT count(*) FROM docket_runs")

      committed_count =
        exact_numeric_sum(
          snapshot,
          [:docket, :lifecycle, :committed],
          :count,
          sum(committed, :count)
        )

      repo_query_count =
        exact_observation_count(
          snapshot,
          [:docket, :benchmark, :repo, :query],
          %{benchmark_query: :workload},
          length(repo_queries)
        )

      %{
        durable_run_rows: run_rows,
        durable_event_rows: event_rows,
        events_per_completed_run: ratio(event_rows, config.runs),
        committed_moments_per_run: ratio(committed_count, config.runs),
        repo_queries_per_run: ratio(repo_query_count, config.runs),
        store_attempted_rows:
          exact_numeric_sum(
            snapshot,
            [:docket, :postgres, :store],
            :attempted_rows,
            sum(store, :attempted_rows)
          ),
        store_encoded_bytes:
          exact_numeric_sum(
            snapshot,
            [:docket, :postgres, :store],
            :encoded_bytes,
            sum(store, :encoded_bytes)
          ),
        wal_bytes: after_snapshot.wal_bytes_position - before.wal_bytes_position,
        database_size_bytes_delta:
          after_snapshot.database_size_bytes - before.database_size_bytes,
        postgres_database_counters_delta:
          map_delta(after_snapshot.database_counters, before.database_counters),
        postgres_contention: contention_change(before.contention, after_snapshot.contention),
        physical_delta_scope:
          "after workload staging and before runtime startup through the post-terminal snapshot",
        pre_activation_dispatcher_polls_in_scope: pre_activation_polls,
        caveat:
          "Physical deltas include runtime startup and pre-activation polling; WAL and pg_stat_database can also include concurrent server activity and stats lag."
      }
    end

    @doc false
    def event_records(events, event) do
      for {^event, measurements, metadata, observed_at} <- event_list(events),
          do: {measurements, metadata, observed_at}
    end

    @doc false
    def event_measurements(events, event) do
      Enum.map(event_records(events, event), fn {measurements, _metadata, _observed_at} ->
        measurements
      end)
    end

    @doc false
    def correlation_times(records, selector) do
      Enum.reduce(records, %{}, fn {_measurements, metadata, observed_at}, times ->
        case metadata[:correlation_id] do
          nil ->
            times

          id when selector == :first ->
            Map.update(times, id, observed_at, &min(&1, observed_at))

          id ->
            Map.update(times, id, observed_at, &max(&1, observed_at))
        end
      end)
    end

    @doc false
    def native_event_distribution(events, event),
      do: native_metric_distribution(event_measurements(events, event), :duration)

    @doc false
    def native_metric_distribution(measurements, key) do
      measurements
      |> Enum.flat_map(fn measurement ->
        if is_number(measurement[key]), do: [measurement[key]], else: []
      end)
      |> Docket.Benchmark.Stats.native_distribution()
    end

    @doc false
    def sum(measurements, key),
      do:
        Enum.reduce(measurements, 0, fn measurement, total -> total + (measurement[key] || 0) end)

    @doc false
    def max_value([], _key), do: 0
    @doc false
    def max_value(measurements, key), do: measurements |> Enum.map(&(&1[key] || 0)) |> Enum.max()
    @doc false
    def ratio(_value, 0), do: nil
    @doc false
    def ratio(value, denominator), do: Float.round(value / denominator, 3)

    @doc false
    def physical_snapshot do
      database_counters = database_counter_snapshot()

      %{rows: [values]} =
        Ecto.Adapters.SQL.query!(
          Repo,
          """
          WITH activity AS (
            SELECT
              count(*)::bigint AS other_backends,
              count(*) FILTER (WHERE state = 'active')::bigint AS active_backends,
              count(*) FILTER (WHERE state = 'idle in transaction')::bigint AS idle_in_transaction_backends,
              count(*) FILTER (
                WHERE state = 'active' AND wait_event_type IS NOT NULL
              )::bigint AS active_waiting_backends,
              count(*) FILTER (
                WHERE state = 'active' AND wait_event_type = 'Lock'
              )::bigint AS lock_waiting_backends,
              coalesce(
                max(
                  floor(
                    greatest(extract(epoch FROM (clock_timestamp() - query_start)), 0) * 1000
                  )
                ) FILTER (WHERE state = 'active'),
                0
              )::bigint AS longest_active_query_ms
            FROM pg_stat_activity
            WHERE datname = current_database() AND pid <> pg_backend_pid()
          ), locks AS (
            SELECT
              count(*)::bigint AS lock_rows,
              count(*) FILTER (WHERE NOT locks.granted)::bigint AS ungranted_lock_rows,
              count(DISTINCT locks.pid) FILTER (WHERE NOT locks.granted)::bigint
                AS backends_with_ungranted_locks,
              count(*) FILTER (
                WHERE locks.granted AND locks.mode = 'AccessExclusiveLock'
              )::bigint AS granted_access_exclusive_locks,
              count(*) FILTER (
                WHERE locks.granted AND locks.mode = 'RowExclusiveLock'
              )::bigint AS granted_row_exclusive_locks
            FROM pg_locks AS locks
            JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
            WHERE activity.datname = current_database() AND locks.pid <> pg_backend_pid()
          )
          SELECT
            pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint,
            pg_database_size(current_database()),
            activity.other_backends,
            activity.active_backends,
            activity.idle_in_transaction_backends,
            activity.active_waiting_backends,
            activity.lock_waiting_backends,
            activity.longest_active_query_ms,
            locks.lock_rows,
            locks.ungranted_lock_rows,
            locks.backends_with_ungranted_locks,
            locks.granted_access_exclusive_locks,
            locks.granted_row_exclusive_locks
          FROM activity
          CROSS JOIN locks
          """,
          []
        )

      [
        wal_bytes_position,
        database_size_bytes,
        other_backends,
        active_backends,
        idle_in_transaction_backends,
        active_waiting_backends,
        lock_waiting_backends,
        longest_active_query_ms,
        lock_rows,
        ungranted_lock_rows,
        backends_with_ungranted_locks,
        granted_access_exclusive_locks,
        granted_row_exclusive_locks
      ] = values

      %{
        wal_bytes_position: wal_bytes_position,
        database_size_bytes: database_size_bytes,
        database_counters: database_counters,
        contention: %{
          activity: %{
            other_backends: other_backends,
            active_backends: active_backends,
            idle_in_transaction_backends: idle_in_transaction_backends,
            active_waiting_backends: active_waiting_backends,
            lock_waiting_backends: lock_waiting_backends,
            longest_active_query_ms: longest_active_query_ms
          },
          locks: %{
            lock_rows: lock_rows,
            ungranted_lock_rows: ungranted_lock_rows,
            backends_with_ungranted_locks: backends_with_ungranted_locks,
            granted_access_exclusive_locks: granted_access_exclusive_locks,
            granted_row_exclusive_locks: granted_row_exclusive_locks
          }
        }
      }
    end

    @doc false
    def map_delta(after_map, before_map) do
      Map.new(after_map, fn {key, value} ->
        before_value = Map.get(before_map, key, @unavailable)

        delta =
          if is_number(value) and is_number(before_value),
            do: value - before_value,
            else: @unavailable

        {key, delta}
      end)
    end

    @doc false
    def contention_change(before, after_snapshot) do
      %{
        before: before,
        after: after_snapshot,
        gauge_delta: %{
          activity: map_delta(after_snapshot.activity, before.activity),
          locks: map_delta(after_snapshot.locks, before.locks)
        },
        scope: "point-in-time snapshots immediately before and after the measured workload",
        caveat:
          "Boundary gauges can miss transient lock and wait spikes; pg_stat_database deadlock, conflict, I/O-time, temporary-file, and session-time counters are reported separately when the server version exposes them."
      }
    end

    defp database_counter_snapshot do
      requested = Enum.map(@database_counter_names, &Atom.to_string/1)

      %{rows: rows} =
        Ecto.Adapters.SQL.query!(
          Repo,
          "SELECT attname::text FROM pg_attribute WHERE attrelid = 'pg_catalog.pg_stat_database'::regclass AND attnum > 0 AND NOT attisdropped AND attname = ANY($1::text[])",
          [requested]
        )

      available_names = rows |> Enum.map(&hd/1) |> MapSet.new()

      selected =
        Enum.filter(@database_counter_names, fn name ->
          MapSet.member?(available_names, Atom.to_string(name))
        end)

      observed =
        case selected do
          [] ->
            %{}

          names ->
            columns = names |> Enum.map(&Atom.to_string/1) |> Enum.join(", ")

            %{rows: [values]} =
              Ecto.Adapters.SQL.query!(
                Repo,
                "SELECT #{columns} FROM pg_stat_database WHERE datname = current_database()",
                []
              )

            Enum.zip(names, values) |> Map.new()
        end

      Map.new(@database_counter_names, fn name ->
        value = Map.get(observed, name, @unavailable)
        {name, if(is_number(value), do: value, else: @unavailable)}
      end)
    end

    @doc false
    def claim_only_measurements(
          snapshot,
          t0,
          control_duration,
          config,
          setup,
          physical_before,
          physical_after,
          collector_stats
        ) do
      scans = event_records(snapshot, [:docket, :postgres, :run_store, :claim])
      queries = event_measurements(snapshot, [:docket, :postgres, :run_store, :claim_query])
      attempts = event_records(snapshot, [:docket, :postgres, :claim, :attempt])
      ready = Enum.filter(attempts, fn {_m, metadata, _at} -> metadata.class == :ready end)
      expired = Enum.filter(attempts, fn {_m, metadata, _at} -> metadata.class == :expired end)

      claim_offsets = Enum.map(attempts, fn {_m, _metadata, at} -> at - t0 end)
      ready_offsets = Enum.map(ready, fn {_m, _metadata, at} -> at - t0 end)
      expired_offsets = Enum.map(expired, fn {_m, _metadata, at} -> at - t0 end)

      last_claim_at =
        exact_observed_at_max(snapshot, [:docket, :postgres, :claim, :attempt], nil)

      claim_window =
        if is_integer(last_claim_at),
          do: max(last_claim_at - t0, 0),
          else: Enum.max(claim_offsets, fn -> control_duration end)

      scan_measurements = Enum.map(scans, fn {measurement, _metadata, _at} -> measurement end)
      sampled_batch_sizes = Enum.map(scan_measurements, & &1.leases)
      event_rows_after = scalar("SELECT count(*) FROM docket_events")

      invalid_ages =
        exact_negative_count(
          snapshot,
          [:docket, :postgres, :claim, :attempt],
          :eligible_age_ms,
          Enum.count(attempts, fn {measurement, _metadata, _at} ->
            measurement.eligible_age_ms < 0
          end)
        )

      observed_claims =
        exact_observation_count(
          snapshot,
          [:docket, :postgres, :claim, :attempt],
          %{},
          length(attempts)
        )

      observed_ready =
        exact_observation_count(
          snapshot,
          [:docket, :postgres, :claim, :attempt],
          %{class: :ready},
          length(ready)
        )

      observed_expired =
        exact_observation_count(
          snapshot,
          [:docket, :postgres, :claim, :attempt],
          %{class: :expired},
          length(expired)
        )

      scan_count =
        exact_observation_count(
          snapshot,
          [:docket, :postgres, :run_store, :claim],
          %{},
          length(scans)
        )

      query_count =
        exact_observation_count(
          snapshot,
          [:docket, :postgres, :run_store, :claim_query],
          %{},
          length(queries)
        )

      empty_scan_count =
        exact_predicate_count(
          snapshot,
          [:docket, :postgres, :run_store, :claim],
          :leases_zero,
          Enum.count(sampled_batch_sizes, &(&1 == 0))
        )

      nonempty_scan_count = max(scan_count - empty_scan_count, 0)

      claimed_rows_from_scans =
        exact_numeric_sum(
          snapshot,
          [:docket, :postgres, :run_store, :claim],
          :leases,
          Enum.sum(sampled_batch_sizes)
        )

      retained_full_scan_samples =
        Enum.count(sampled_batch_sizes, &(&1 == config.batch_size))

      retained_partial_scan_samples =
        Enum.count(sampled_batch_sizes, &(&1 > 0 and &1 < config.batch_size))

      exact_global_counts_pass =
        observed_claims == config.runs and observed_ready == setup.ready_count and
          observed_expired == setup.expired_count and scan_count == query_count and
          claimed_rows_from_scans == config.runs and invalid_ages == 0

      %{
        claimed_rows: observed_claims,
        throughput_per_second: rate(config.runs, claim_window),
        observed_runs_per_second: rate(config.runs, claim_window),
        claim_window_duration_us: System.convert_time_unit(claim_window, :native, :microsecond),
        latency: %{
          burst_start_to_claim_offset_us:
            Docket.Benchmark.Stats.native_distribution(claim_offsets),
          ready_burst_start_to_claim_offset_us:
            Docket.Benchmark.Stats.native_distribution(ready_offsets),
          expired_burst_start_to_claim_offset_us:
            Docket.Benchmark.Stats.native_distribution(expired_offsets),
          claim_scan_total_us: native_metric_distribution(scan_measurements, :duration),
          claim_query_time_us: native_metric_distribution(queries, :query_time),
          claim_queue_time_us: native_metric_distribution(queries, :queue_time),
          claim_decode_time_us: native_metric_distribution(queries, :decode_time),
          ready_age_at_frozen_claim_clock_ms:
            Docket.Benchmark.Stats.millisecond_distribution(
              Enum.map(ready, fn {measurement, _metadata, _at} ->
                measurement.eligible_age_ms
              end)
            ),
          expired_overdue_after_ttl_ms:
            Docket.Benchmark.Stats.millisecond_distribution(
              Enum.map(expired, fn {measurement, _metadata, _at} ->
                measurement.overdue_after_ttl_ms
              end)
            )
        },
        batches: %{
          exact_global_counts: %{
            total_scan_events: scan_count,
            nonempty_scan_events: nonempty_scan_count,
            empty_scan_events: empty_scan_count,
            claimed_rows: claimed_rows_from_scans
          },
          retained_batch_shape_evidence: %{
            scope: "bounded retained claim-scan event sample",
            retained_scan_events: length(sampled_batch_sizes),
            full_scan_events: retained_full_scan_samples,
            partial_scan_events: retained_partial_scan_samples
          },
          rows_per_scan: Docket.Benchmark.Stats.distribution(sampled_batch_sizes, & &1, "rows"),
          mean_rows_per_nonempty_scan:
            if(nonempty_scan_count == 0,
              do: nil,
              else: Float.round(claimed_rows_from_scans / nonempty_scan_count, 3)
            )
        },
        counts: %{
          ready_claims: observed_ready,
          expired_claims: observed_expired,
          reacquired_claims:
            exact_observation_count(
              snapshot,
              [:docket, :postgres, :claim, :attempt],
              %{result: :reacquired},
              Enum.count(attempts, fn {_m, metadata, _at} ->
                metadata.result == :reacquired
              end)
            ),
          steals:
            exact_numeric_sum(
              snapshot,
              [:docket, :postgres, :run_store, :claim],
              :steals,
              sum(scan_measurements, :steals)
            ),
          poisoned:
            exact_numeric_sum(
              snapshot,
              [:docket, :postgres, :run_store, :claim],
              :poisoned,
              sum(scan_measurements, :poisoned)
            ),
          configured_claimers: config.concurrency,
          claim_query_samples: query_count
        },
        amplification: %{
          durable_event_rows_before: setup.initial_event_rows,
          durable_event_rows_after: event_rows_after,
          event_rows_written_during_claiming: event_rows_after - setup.initial_event_rows,
          wal_bytes: physical_after.wal_bytes_position - physical_before.wal_bytes_position,
          database_size_bytes_delta:
            physical_after.database_size_bytes - physical_before.database_size_bytes,
          postgres_database_counters_delta:
            map_delta(physical_after.database_counters, physical_before.database_counters),
          postgres_contention:
            contention_change(physical_before.contention, physical_after.contention),
          caveat:
            "WAL and pg_stat_database deltas can include concurrent server activity and stats lag; boundary contention gauges can miss transient waits."
        },
        collection: %{
          percentile_method: "nearest-rank, no interpolation",
          exact_global_counts: %{
            expected_claim_attempt_events: config.runs,
            claim_attempt_events: observed_claims,
            expected_ready_claim_attempt_events: setup.ready_count,
            ready_claim_attempt_events: observed_ready,
            expected_expired_claim_attempt_events: setup.expired_count,
            expired_claim_attempt_events: observed_expired,
            claim_scan_events: scan_count,
            claim_query_events: query_count,
            claimed_rows_from_scan_events: claimed_rows_from_scans,
            negative_claim_age_events: invalid_ages,
            checks_pass: exact_global_counts_pass
          },
          retained_distribution_samples: %{
            claim_attempts: length(attempts),
            ready_claim_attempts: length(ready),
            expired_claim_attempts: length(expired),
            claim_scan_events: length(scans)
          },
          control_wait_duration_us:
            System.convert_time_unit(control_duration, :native, :microsecond),
          observer: collector_stats,
          telemetry_checks_pass: exact_global_counts_pass
        }
      }
    end

    # Scenario artifact metadata, warnings, and invariants.

    @doc false
    def claim_only_invariants(config, setup, counters) do
      cutoff = DateTime.add(setup.claim_now, -config.orphan_ttl_ms, :millisecond)

      [
        invariant("all backlog rows returned once", :atomics.get(counters, 1), config.runs),
        invariant("no duplicate returned lease identities", :atomics.get(counters, 2), 0),
        invariant("no poisoned claim outcomes", :atomics.get(counters, 3), 0),
        invariant(
          "all rows retain current claims",
          scalar(
            "SELECT count(*) FROM docket_runs WHERE claim_token IS NOT NULL AND wake_at IS NULL"
          ),
          config.runs
        ),
        invariant(
          "all persisted claim tokens are unique",
          scalar(
            "SELECT count(DISTINCT claim_token) FROM docket_runs WHERE claim_token IS NOT NULL"
          ),
          config.runs
        ),
        invariant(
          "ready rows advanced to attempt one",
          count_ids_at_attempt(setup.ready_ids, 1),
          setup.ready_count
        ),
        invariant(
          "expired rows advanced to attempt two",
          count_ids_at_attempt(setup.expired_ids, 2),
          setup.expired_count
        ),
        invariant(
          "all claims use the frozen claim timestamp",
          scalar_with("SELECT count(*) FROM docket_runs WHERE claimed_at = $1", [setup.claim_now]),
          config.runs
        ),
        invariant(
          "no rows remain eligible at the frozen cutoff",
          scalar_with(
            "SELECT count(*) FROM docket_runs WHERE status = 'running' AND poisoned_at IS NULL AND ((claim_token IS NULL AND wake_at <= $1) OR (claim_token IS NOT NULL AND claimed_at < $2))",
            [setup.claim_now, cutoff]
          ),
          0
        ),
        invariant(
          "claiming appends no durable events",
          scalar("SELECT count(*) FROM docket_events"),
          setup.initial_event_rows
        ),
        invariant(
          "claiming advances no checkpoint sequence",
          scalar("SELECT coalesce(sum(checkpoint_seq), 0)::bigint FROM docket_runs"),
          setup.initial_checkpoint_sum
        ),
        invariant(
          "event sequence remains unique",
          scalar(
            "SELECT count(*) FROM (SELECT run_id, seq FROM docket_events GROUP BY run_id, seq HAVING count(*) > 1) q"
          ),
          0
        )
      ]
    end

    @doc false
    def count_ids_at_attempt([], _attempt), do: 0

    @doc false
    def count_ids_at_attempt(ids, attempt) do
      scalar_with(
        "SELECT count(*) FROM docket_runs WHERE run_id = ANY($1::text[]) AND claim_attempts = $2",
        [ids, attempt]
      )
    end

    @doc false
    def scalar_with(sql, params) do
      %{rows: [[value]]} = Ecto.Adapters.SQL.query!(Repo, sql, params)
      value
    end

    @doc false
    def invariant(name, actual, expected),
      do: %{name: name, pass: actual == expected, expected: expected, actual: actual}

    @doc false
    def artifact_parameters(config) do
      Map.drop(config, [
        :database_url,
        :output,
        :format,
        :concurrency_matrix,
        :pool_size_matrix,
        :concurrencies,
        :pool_sizes,
        :repetition,
        :scenario,
        :observer_mode,
        :observer_position,
        :observer_pair
      ])
    end

    @doc false
    def artifact_point(config) do
      point = %{
        concurrency: config.concurrency,
        pool_size: config.pool_size,
        repetition: config.repetition
      }

      if Map.get(config, :observer_abba, false) do
        Map.merge(point, %{
          observer_mode: config.observer_mode,
          observer_position: config.observer_position,
          observer_pair: config.observer_pair
        })
      else
        point
      end
    end

    @doc false
    def observer_collector_mode(%{observer_mode: "counters_only_control"}),
      do: :counters_only_control

    def observer_collector_mode(_config), do: :bounded_instrumented

    @doc false
    def observer_trial_metadata(config, collector_stats \\ nil) do
      if Map.get(config, :observer_abba, false) do
        %{
          enabled: true,
          design: "ABBA",
          mode: config.observer_mode,
          position: config.observer_position,
          pair: config.observer_pair,
          collector_capture_mode:
            if(collector_stats, do: collector_stats.capture_mode, else: config.observer_mode)
        }
      else
        %{
          enabled: false,
          design: "not_requested",
          mode: "bounded_instrumented"
        }
      end
    end

    @doc false
    def observer_warnings(%{observer_abba: true}) do
      [
        "Observer ABBA differences are raw paired diagnostics, not a causal correction or a value to subtract from workload latency.",
        "Counters-only controls still attach telemetry and maintain exact completion/correctness counters; they are not an observer-free runtime.",
        "This control covers the smoke collector only and does not estimate in-process sampler cost for blocked or future steady-state scenarios."
      ]
    end

    def observer_warnings(_config), do: []

    @doc false
    def claim_only_warnings(config) do
      [
        "Observed claim throughput is environment-specific and is not a database ceiling.",
        "This point is repetition #{config.repetition} of #{config.repetitions} with a frozen claim clock.",
        "Claim query timing is client-observed Ecto timing, not server-exclusive execution time.",
        "Run in a dedicated, quiescent BEAM; unrelated global Docket telemetry can contaminate operational distributions."
      ]
    end

    @doc false
    def blocked_warnings(config) do
      [
        "Blocked vehicles remain running and claimed; they are not durably parked runs.",
        "Only the first saturated wave is held; the gate opens for the remaining backlog after the plateau.",
        "This point is repetition #{config.repetition} of #{config.repetitions} with a #{config.hold_ms} ms stable hold.",
        "Pool busy-or-unavailable capacity is derived from public pool readiness metrics and is not an exact checkout count.",
        "Short-work probe and plateau-control queries are excluded from workload query counts but remain in physical Postgres deltas.",
        "The in-process sampler is observational work; inspect its missed ticks, self-time, and duty cycle and use paired controls before making capacity claims.",
        "Run in a dedicated, quiescent BEAM; unrelated global Docket telemetry can contaminate operational distributions."
      ]
    end

    @doc false
    def warnings(config) do
      [
        "Observed throughput is environment-specific and is not a capacity maximum.",
        "This point is repetition #{config.repetition} of #{config.repetitions} with #{config.warmup} warmup runs.",
        "A staged burst is not a steady-state arrival workload.",
        "p95/p99 values from #{config.runs} smoke samples are descriptive only.",
        "Claim query timing is client-observed Ecto timing, not server-exclusive execution time.",
        "Run in a dedicated, quiescent BEAM; unrelated global Docket telemetry can contaminate operational distributions."
      ] ++ observer_warnings(config)
    end

    @doc false
    def invariants(config) do
      queries = [
        {"no duplicate current claim tokens",
         "SELECT count(*) FROM (SELECT claim_token FROM docket_runs WHERE claim_token IS NOT NULL GROUP BY claim_token HAVING count(*) > 1) q",
         0},
        {"no active claims remain",
         "SELECT count(*) FROM docket_runs WHERE claim_token IS NOT NULL", 0},
        {"all seeded runs completed", "SELECT count(*) FROM docket_runs WHERE status = 'done'",
         config.runs},
        {"no stranded running rows", "SELECT count(*) FROM docket_runs WHERE status = 'running'",
         0},
        {"event sequence is unique",
         "SELECT count(*) FROM (SELECT run_id, seq FROM docket_events GROUP BY run_id, seq HAVING count(*) > 1) q",
         0}
      ]

      Enum.map(queries, fn {name, sql, expected} ->
        %{rows: [[actual]]} = Ecto.Adapters.SQL.query!(Repo, sql, [])
        %{name: name, pass: actual == expected, expected: expected, actual: actual}
      end)
    end

    @doc false
    def graph do
      Docket.Graph.new!(id: "docket-bench-empty-one-step")
      |> Docket.Graph.put_node!("noop", implementation: Docket.Benchmark.NoopNode)
      |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
      |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
    end

    @doc false
    def blocked_graph do
      Docket.Graph.new!(id: "docket-bench-blocked-vehicles")
      |> Docket.Graph.put_node!("blocker", implementation: Docket.Benchmark.BlockingNode)
      |> Docket.Graph.put_edge!("start-blocker", from: "$start", to: "blocker")
      |> Docket.Graph.put_edge!("blocker-finish", from: "blocker", to: "$finish")
    end

    @doc false
    def canonical_scenario(scenario), do: Scenario.canonical_name(scenario)

    @doc false
    def environment(config) do
      setting_fingerprint = setting_fingerprint()
      runtime = runtime_fingerprint()
      host = host_fingerprint()
      container = container_fingerprint()
      storage = storage_fingerprint()

      %{
        docket: git_metadata(),
        elixir: System.version(),
        otp_release: List.to_string(:erlang.system_info(:otp_release)),
        erts: List.to_string(:erlang.system_info(:version)),
        os: host.os.family,
        cpu_count: runtime.schedulers_online,
        cpu_model: host.cpu.model,
        postgres_version: scalar("SHOW server_version"),
        postgres_settings: setting_fingerprint.values,
        postgres_setting_details: setting_fingerprint.details,
        unavailable_postgres_settings: setting_fingerprint.unavailable,
        pg_stat_statements: pg_stat_statements_fingerprint(setting_fingerprint.values),
        postgres_connection: connection_fingerprint(),
        repo_pool_size: Repo.config()[:pool_size],
        repo_pool_count: Repo.config()[:pool_count] || 1,
        dispatcher_nodes: config.nodes,
        storage_class: storage.class,
        ram_bytes: host.memory.host_total_bytes,
        host: host,
        runtime: runtime,
        container: container,
        storage: storage
      }
    end

    # Host and Postgres environment capture.

    @doc false
    def settings, do: setting_fingerprint().values

    @doc false
    def setting_fingerprint do
      %{rows: rows} =
        Ecto.Adapters.SQL.query!(
          Repo,
          "SELECT name, setting, unit, source, pending_restart FROM pg_settings WHERE name = ANY($1::text[])",
          [@postgres_setting_names]
        )

      observed =
        Map.new(rows, fn [name, value, unit, source, pending_restart] ->
          {name,
           %{
             status: "available",
             value: value,
             unit: unit || "none",
             source: source,
             pending_restart: pending_restart
           }}
        end)

      details =
        Map.new(@postgres_setting_names, fn name ->
          {name,
           Map.get(observed, name, %{
             status: @unavailable,
             value: @unavailable,
             unit: @unavailable,
             source: @unavailable,
             pending_restart: @unavailable
           })}
        end)

      %{
        values: Map.new(details, fn {name, detail} -> {name, detail.value} end),
        details: details,
        unavailable: for({name, %{status: status}} <- details, status == @unavailable, do: name)
      }
    end

    @doc false
    def pg_stat_statements_fingerprint(settings) do
      result =
        Ecto.Adapters.SQL.query(
          Repo,
          "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'), to_regclass('pg_stat_statements') IS NOT NULL",
          []
        )

      case result do
        {:ok, %{rows: [[installed, view_visible]]}} ->
          shared_preloaded =
            settings
            |> Map.get("shared_preload_libraries", @unavailable)
            |> postgres_list_includes?("pg_stat_statements")

          status =
            if installed and view_visible and shared_preloaded,
              do: "available",
              else: @unavailable

          reason =
            cond do
              status == "available" -> "installed, visible, and shared-preloaded"
              not installed -> "extension is not installed"
              not view_visible -> "extension view is not visible on the search path"
              not shared_preloaded -> "library is not listed in shared_preload_libraries"
            end

          %{
            status: status,
            installed: installed,
            view_visible: view_visible,
            shared_preloaded: shared_preloaded,
            reason: reason,
            query_statistics_captured: false
          }

        _ ->
          %{
            status: @unavailable,
            installed: @unavailable,
            view_visible: @unavailable,
            shared_preloaded: @unavailable,
            reason: "availability query failed",
            query_statistics_captured: false
          }
      end
    end

    @doc false
    def connection_fingerprint do
      case Ecto.Adapters.SQL.query(
             Repo,
             "SELECT CASE WHEN inet_server_addr() IS NULL THEN 'unix_socket' ELSE 'tcp' END, inet_server_port(), current_setting('server_encoding'), current_setting('TimeZone')",
             []
           ) do
        {:ok, %{rows: [[transport, port, encoding, timezone]]}} ->
          %{
            transport: transport,
            server_port: port || @unavailable,
            server_encoding: encoding,
            timezone: timezone,
            pooler_mode: nonempty_env("DOCKET_BENCH_POOLER_MODE")
          }

        _ ->
          %{
            transport: @unavailable,
            server_port: @unavailable,
            server_encoding: @unavailable,
            timezone: @unavailable,
            pooler_mode: nonempty_env("DOCKET_BENCH_POOLER_MODE")
          }
      end
    end

    @doc false
    def scalar(sql) do
      %{rows: [[value]]} = Ecto.Adapters.SQL.query!(Repo, sql, [])
      value
    end

    @doc false
    def benchmark_scalar(sql) do
      %{rows: [[value]]} =
        Ecto.Adapters.SQL.query!(Repo, sql, [], telemetry_options: [benchmark_query: :control])

      value
    end

    @doc false
    def benchmark_scalar_with(sql, params) do
      %{rows: [[value]]} =
        Ecto.Adapters.SQL.query!(Repo, sql, params,
          telemetry_options: [benchmark_query: :control]
        )

      value
    end

    @doc false
    def benchmark_maximum_claim_age_ms do
      benchmark_scalar_with(
        "SELECT coalesce(max(floor(greatest(extract(epoch FROM ($1::timestamptz - claimed_at)), 0) * 1000)), 0)::bigint FROM docket_runs WHERE claim_token IS NOT NULL",
        [DateTime.utc_now()]
      )
    end

    @doc false
    def git_metadata do
      version = Application.spec(:docket, :vsn) |> to_string()

      {commit, dirty} =
        case System.get_env("DOCKET_BENCH_COMMIT") do
          commit when is_binary(commit) and commit != "" ->
            {commit, env_dirty()}

          _ ->
            if docket_project?() do
              {git_value(["rev-parse", "HEAD"]),
               git_value(["status", "--porcelain", "--untracked-files=normal"]) not in [nil, ""]}
            else
              {nil, nil}
            end
        end

      %{
        version: version,
        commit: commit,
        dirty: dirty
      }
    end

    @doc false
    def docket_project? do
      Code.ensure_loaded?(Mix.Project) and Mix.Project.get() != nil and
        Mix.Project.config()[:app] == :docket
    rescue
      _error -> false
    end

    @doc false
    def git_value(args) do
      case System.cmd("git", args, stderr_to_stdout: true) do
        {value, 0} -> String.trim(value)
        _ -> nil
      end
    end

    @doc false
    def env_dirty do
      case System.get_env("DOCKET_BENCH_DIRTY") do
        value when value in ["1", "true", "TRUE"] -> true
        value when value in ["0", "false", "FALSE"] -> false
        _ -> nil
      end
    end

    @doc false
    def runtime_fingerprint do
      %{
        architecture: system_info(:system_architecture),
        emulator_flavor: system_info(:emu_flavor),
        word_size_bytes: system_info(:wordsize),
        schedulers: system_info(:schedulers),
        schedulers_online: System.schedulers_online(),
        dirty_cpu_schedulers: system_info(:dirty_cpu_schedulers),
        dirty_io_schedulers: system_info(:dirty_io_schedulers),
        logical_processors: system_info(:logical_processors),
        logical_processors_available: system_info(:logical_processors_available),
        logical_processors_online: system_info(:logical_processors_online)
      }
    end

    @doc false
    def host_fingerprint do
      %{
        os: %{
          family: inspect(:os.type()),
          version: os_version(),
          kernel_release: command_output("uname", ["-r"])
        },
        cpu: %{
          model: cpu_model(),
          hardware_logical_processors: system_info(:logical_processors),
          available_logical_processors: system_info(:logical_processors_available)
        },
        memory: %{
          host_total_bytes: total_memory()
        }
      }
    end

    @doc false
    def container_fingerprint do
      runtime = container_runtime()

      %{
        detected: runtime not in ["none", @unavailable],
        runtime: runtime,
        cgroup_version: cgroup_version(),
        cpu_quota_cores: cgroup_cpu_quota(),
        memory_limit_bytes: cgroup_memory_limit()
      }
    end

    @doc false
    def storage_fingerprint do
      data_directory = scalar("SHOW data_directory")
      filesystem = filesystem_type(data_directory)
      disk = disk_space(data_directory)
      {storage_class, detection} = storage_class(data_directory, filesystem, disk.mount_source)

      disk
      |> Map.merge(%{
        class: storage_class,
        class_detection: detection,
        filesystem: filesystem,
        postgres_data_directory: data_directory
      })
    end

    @doc false
    def cpu_model do
      case :os.type() do
        {:unix, :darwin} ->
          command_output("sysctl", ["-n", "machdep.cpu.brand_string"])

        {:unix, _name} ->
          with {:ok, contents} <- File.read("/proc/cpuinfo"),
               [model | _] <-
                 Regex.run(~r/^model name\s*:\s*(.+)$/m, contents, capture: :all_but_first) do
            String.trim(model)
          else
            _ -> @unavailable
          end

        _ ->
          @unavailable
      end
    end

    @doc false
    def total_memory do
      case :os.type() do
        {:unix, :darwin} ->
          parse_integer(command_output("sysctl", ["-n", "hw.memsize"]))

        {:unix, _name} ->
          with {:ok, contents} <- File.read("/proc/meminfo"),
               [kilobytes | _] <-
                 Regex.run(~r/^MemTotal:\s+(\d+)\s+kB$/m, contents, capture: :all_but_first),
               value when is_integer(value) <- parse_integer(kilobytes) do
            value * 1_024
          else
            _ -> @unavailable
          end

        _ ->
          @unavailable
      end
    end

    defp postgres_list_includes?(value, wanted) when is_binary(value) do
      value
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.member?(wanted)
    end

    defp postgres_list_includes?(_value, _wanted), do: false

    defp system_info(key) do
      key
      |> :erlang.system_info()
      |> normalize_system_value()
    rescue
      _error -> @unavailable
    end

    defp normalize_system_value(value) when value in [:unknown, :undefined], do: @unavailable
    defp normalize_system_value(value) when is_atom(value), do: Atom.to_string(value)
    defp normalize_system_value(value) when is_list(value), do: List.to_string(value)
    defp normalize_system_value(value) when is_tuple(value), do: inspect(value)
    defp normalize_system_value(value), do: value

    defp os_version do
      case :os.version() do
        {major, minor, patch} -> Enum.join([major, minor, patch], ".")
        value -> normalize_system_value(value)
      end
    rescue
      _error -> @unavailable
    end

    defp container_runtime do
      cgroup = read_file("/proc/1/cgroup")

      cond do
        nonempty_env("KUBERNETES_SERVICE_HOST") != @unavailable -> "kubernetes"
        File.exists?("/.dockerenv") -> "docker"
        is_binary(cgroup) and String.contains?(cgroup, "kubepods") -> "kubernetes"
        is_binary(cgroup) and String.contains?(cgroup, "docker") -> "docker"
        is_binary(cgroup) and String.contains?(cgroup, "containerd") -> "containerd"
        is_binary(cgroup) and String.contains?(cgroup, "lxc") -> "lxc"
        nonempty_env("container") != @unavailable -> nonempty_env("container")
        true -> "none"
      end
    end

    defp cgroup_version do
      cond do
        File.exists?("/sys/fs/cgroup/cgroup.controllers") -> 2
        File.exists?("/proc/1/cgroup") -> 1
        true -> @unavailable
      end
    end

    defp cgroup_cpu_quota do
      cond do
        is_binary(value = read_file("/sys/fs/cgroup/cpu.max")) ->
          case String.split(value) do
            ["max", _period] -> "unlimited"
            [quota, period] -> quota_cores(quota, period)
            _ -> @unavailable
          end

        true ->
          quota = read_file("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
          period = read_file("/sys/fs/cgroup/cpu/cpu.cfs_period_us")

          case {parse_integer(quota), parse_integer(period)} do
            {quota, _period} when is_integer(quota) and quota < 0 ->
              "unlimited"

            {quota, period} when is_integer(quota) and is_integer(period) and period > 0 ->
              Float.round(quota / period, 3)

            _ ->
              @unavailable
          end
      end
    end

    defp quota_cores(quota, period) do
      case {parse_integer(quota), parse_integer(period)} do
        {quota, period} when is_integer(quota) and is_integer(period) and period > 0 ->
          Float.round(quota / period, 3)

        _ ->
          @unavailable
      end
    end

    defp cgroup_memory_limit do
      value =
        read_file("/sys/fs/cgroup/memory.max") ||
          read_file("/sys/fs/cgroup/memory/memory.limit_in_bytes")

      case value do
        "max" ->
          "unlimited"

        value ->
          case parse_integer(value) do
            number when is_integer(number) and number >= 9_000_000_000_000_000_000 ->
              "unlimited"

            number when is_integer(number) ->
              number

            _ ->
              @unavailable
          end
      end
    end

    defp filesystem_type(path) do
      case :os.type() do
        {:unix, :darwin} -> command_output("stat", ["-f", "%T", path])
        {:unix, _name} -> command_output("stat", ["-f", "-c", "%T", path])
        _ -> @unavailable
      end
    end

    defp disk_space(path) do
      defaults = %{
        mount_source: @unavailable,
        mount_point: @unavailable,
        total_bytes: @unavailable,
        used_bytes: @unavailable,
        available_bytes: @unavailable
      }

      case command_output("df", ["-Pk", path]) do
        @unavailable ->
          defaults

        output ->
          fields =
            output
            |> String.split("\n", trim: true)
            |> List.last("")
            |> String.split(~r/\s+/, trim: true)

          case fields do
            [source, total, used, available, _capacity | mount_parts] ->
              Map.merge(defaults, %{
                mount_source: source,
                mount_point: Enum.join(mount_parts, " "),
                total_bytes: kilobytes_to_bytes(total),
                used_bytes: kilobytes_to_bytes(used),
                available_bytes: kilobytes_to_bytes(available)
              })

            _ ->
              defaults
          end
      end
    end

    defp storage_class(path, filesystem, source) do
      override = nonempty_env("DOCKET_BENCH_STORAGE_CLASS")

      cond do
        override != @unavailable ->
          {override, "DOCKET_BENCH_STORAGE_CLASS"}

        filesystem in ["tmpfs", "ramfs"] ->
          {"memory", "filesystem"}

        filesystem in ["nfs", "nfs4", "smbfs", "cifs"] ->
          {"network", "filesystem"}

        filesystem in ["overlay", "overlayfs"] ->
          {"container_overlay", "filesystem"}

        match?({:unix, :darwin}, :os.type()) ->
          darwin_storage_class(path)

        is_binary(source) and String.starts_with?(source, "/dev/") ->
          linux_storage_class(source)

        true ->
          {@unavailable, @unavailable}
      end
    end

    defp darwin_storage_class(path) do
      case command_output("diskutil", ["info", path]) do
        @unavailable ->
          {@unavailable, @unavailable}

        output ->
          cond do
            Regex.match?(~r/^\s*Solid State:\s+Yes\s*$/mi, output) ->
              {"solid_state", "diskutil"}

            Regex.match?(~r/^\s*Solid State:\s+No\s*$/mi, output) ->
              {"rotational", "diskutil"}

            true ->
              {@unavailable, @unavailable}
          end
      end
    end

    defp linux_storage_class(source) do
      case command_output("lsblk", ["-ndo", "ROTA", source]) do
        @unavailable ->
          {@unavailable, @unavailable}

        output ->
          rotations = String.split(output, ~r/\s+/, trim: true)

          cond do
            rotations != [] and Enum.all?(rotations, &(&1 == "0")) ->
              {"solid_state", "lsblk"}

            "1" in rotations ->
              {"rotational", "lsblk"}

            true ->
              {@unavailable, @unavailable}
          end
      end
    end

    defp command_output(command, args) do
      with executable when is_binary(executable) <- System.find_executable(command),
           {output, 0} <- System.cmd(executable, args, stderr_to_stdout: true),
           value when value != "" <- String.trim(output) do
        value
      else
        _ -> @unavailable
      end
    rescue
      _error -> @unavailable
    end

    defp nonempty_env(name) do
      case System.get_env(name) do
        value when is_binary(value) and value != "" -> value
        _ -> @unavailable
      end
    end

    defp read_file(path) do
      case File.read(path) do
        {:ok, value} -> String.trim(value)
        _ -> nil
      end
    end

    defp parse_integer(value) when is_binary(value) do
      case Integer.parse(String.trim(value)) do
        {number, ""} -> number
        _ -> @unavailable
      end
    end

    defp parse_integer(_value), do: @unavailable

    defp kilobytes_to_bytes(value) do
      case parse_integer(value) do
        number when is_integer(number) -> number * 1_024
        _ -> @unavailable
      end
    end

    @doc false
    def rate(_runs, 0), do: nil

    @doc false
    def rate(runs, duration_native) do
      duration_us = System.convert_time_unit(duration_native, :native, :microsecond)
      if duration_us == 0, do: nil, else: Float.round(runs * 1_000_000 / duration_us, 3)
    end

    @doc false
    def schema_version, do: @artifact_schema_version

    # Cross-repetition suite summaries and atomic result writing. Every run,
    # including a single trial, writes the same suite envelope.
    @doc false
    def suite_summary_payload(artifacts) do
      observer_enabled = observer_abba_enabled?(artifacts)

      summaries =
        artifacts
        |> Enum.group_by(fn artifact ->
          {artifact.point.concurrency, artifact.point.pool_size}
        end)
        |> Enum.map(fn {{concurrency, pool_size}, points} ->
          successful = Enum.filter(points, & &1.success)

          reported =
            if observer_enabled do
              Enum.filter(successful, fn point ->
                get_in(point, [:observer_control, :mode]) == "bounded_instrumented"
              end)
            else
              successful
            end

          %{
            concurrency: concurrency,
            pool_size: pool_size,
            repetitions:
              if(observer_enabled,
                do: points |> Enum.map(& &1.point.repetition) |> Enum.uniq() |> length(),
                else: length(points)
              ),
            observer_trial_count: if(observer_enabled, do: length(points), else: nil),
            reported_instrumented_trial_count:
              if(observer_enabled, do: length(reported), else: nil),
            successful_repetitions:
              reported
              |> Enum.map(& &1.point.repetition)
              |> Enum.uniq()
              |> length(),
            success: Enum.all?(points, & &1.success),
            throughput_per_second:
              Docket.Benchmark.Stats.repetition_summary(
                Enum.map(reported, & &1.measurements.throughput_per_second),
                "work_items_per_second"
              ),
            measured_duration_us:
              Docket.Benchmark.Stats.repetition_summary(
                Enum.map(reported, & &1.duration_us),
                "us"
              ),
            latency_across_repetitions: suite_latency_summary(reported, hd(points).scenario),
            bottleneck_evidence: bottleneck_evidence(reported)
          }
        end)
        |> Enum.sort_by(&{&1.concurrency, &1.pool_size})

      concurrency_knee = concurrency_knee_analysis(summaries, hd(artifacts).scenario)
      observer_effect_control = observer_effect_analysis(artifacts)

      %{
        schema_version: schema_version(),
        kind: "benchmark_suite",
        scenario: hd(artifacts).scenario,
        classification: "exploratory",
        success: Enum.all?(artifacts, & &1.success),
        matrix_point_count:
          artifacts
          |> Enum.map(&{&1.point.concurrency, &1.point.pool_size})
          |> Enum.uniq()
          |> length(),
        trial_count: length(artifacts),
        expected_repetitions: hd(artifacts).parameters.repetitions,
        observer_effect_control: observer_effect_control,
        concurrency_knee: concurrency_knee,
        summary: summaries,
        points: artifacts
      }
    end

    @doc false
    def observer_effect_analysis(artifacts) do
      if observer_abba_enabled?(artifacts) do
        pairs =
          artifacts
          |> Enum.group_by(fn artifact ->
            {
              artifact.point.concurrency,
              artifact.point.pool_size,
              artifact.point.repetition,
              artifact.point.observer_pair
            }
          end)
          |> Enum.map(fn {{concurrency, pool_size, repetition, pair}, trials} ->
            observer_pair_record(concurrency, pool_size, repetition, pair, trials)
          end)
          |> Enum.sort_by(&{&1.concurrency, &1.pool_size, &1.repetition, &1.pair})

        valid_pairs = Enum.filter(pairs, & &1.valid)

        cells =
          pairs
          |> Enum.group_by(&{&1.concurrency, &1.pool_size})
          |> Enum.map(fn {{concurrency, pool_size}, cell_pairs} ->
            valid = Enum.filter(cell_pairs, & &1.valid)

            %{
              concurrency: concurrency,
              pool_size: pool_size,
              status: pair_status(valid, cell_pairs),
              pair_count: length(cell_pairs),
              valid_pair_count: length(valid),
              duration_instrumented_minus_control_us:
                paired_summary(valid, [:delta, :duration_us], "us"),
              duration_instrumented_minus_control_percent:
                paired_summary(valid, [:delta, :duration_percent], "percent"),
              throughput_instrumented_minus_control_per_second:
                paired_summary(
                  valid,
                  [:delta, :throughput_per_second],
                  "work_items_per_second"
                ),
              throughput_instrumented_minus_control_percent:
                paired_summary(valid, [:delta, :throughput_percent], "percent"),
              pairs: cell_pairs
            }
          end)
          |> Enum.sort_by(&{&1.concurrency, &1.pool_size})

        %{
          enabled: true,
          status: pair_status(valid_pairs, pairs),
          design: "ABBA",
          sequence: [
            "bounded_instrumented",
            "counters_only_control",
            "counters_only_control",
            "bounded_instrumented"
          ],
          delta_direction: "instrumented_minus_control",
          pair_count: length(pairs),
          valid_pair_count: length(valid_pairs),
          control:
            "counters-only telemetry handler with exact completion/correctness counters and no retained distribution samples",
          interpretation:
            "Raw paired differences diagnose observer sensitivity; they are not a causal correction and must not be subtracted from workload latency.",
          sampler_scope:
            "Not controlled. This option is smoke-only and does not quantify sampler cost for blocked or future steady-state scenarios.",
          cells: cells
        }
      else
        %{
          enabled: false,
          status: "not_requested",
          design: "none",
          interpretation: "Use --observer-abba with smoke/empty_one_step for paired controls."
        }
      end
    end

    defp observer_abba_enabled?(artifacts) do
      Enum.any?(artifacts, &(get_in(&1, [:observer_control, :enabled]) == true))
    end

    defp observer_pair_record(concurrency, pool_size, repetition, pair, trials) do
      instrumented =
        Enum.find(trials, &(get_in(&1, [:observer_control, :mode]) == "bounded_instrumented"))

      control =
        Enum.find(
          trials,
          &(get_in(&1, [:observer_control, :mode]) == "counters_only_control")
        )

      valid =
        instrumented != nil and control != nil and instrumented.success and control.success and
          is_number(instrumented.duration_us) and is_number(control.duration_us) and
          is_number(get_in(instrumented, [:measurements, :throughput_per_second])) and
          is_number(get_in(control, [:measurements, :throughput_per_second]))

      %{
        concurrency: concurrency,
        pool_size: pool_size,
        repetition: repetition,
        pair: pair,
        valid: valid,
        order:
          trials
          |> Enum.sort_by(& &1.point.observer_position)
          |> Enum.map(& &1.point.observer_mode),
        instrumented: observer_trial_projection(instrumented),
        control: observer_trial_projection(control),
        delta: if(valid, do: observer_delta(instrumented, control), else: nil)
      }
    end

    defp observer_trial_projection(nil), do: nil

    defp observer_trial_projection(artifact) do
      %{
        success: artifact.success,
        position: artifact.point.observer_position,
        duration_us: artifact.duration_us,
        throughput_per_second: get_in(artifact, [:measurements, :throughput_per_second]),
        collector_capture_mode:
          get_in(artifact, [:measurements, :collection, :observer, :capture_mode])
      }
    end

    defp observer_delta(instrumented, control) do
      instrumented_throughput = get_in(instrumented, [:measurements, :throughput_per_second])
      control_throughput = get_in(control, [:measurements, :throughput_per_second])

      %{
        duration_us: instrumented.duration_us - control.duration_us,
        duration_percent: percent_delta(instrumented.duration_us, control.duration_us),
        throughput_per_second: instrumented_throughput - control_throughput,
        throughput_percent: percent_delta(instrumented_throughput, control_throughput)
      }
    end

    defp percent_delta(_instrumented, 0), do: nil

    defp percent_delta(instrumented, control),
      do: Float.round((instrumented - control) * 100 / control, 3)

    defp paired_summary(pairs, path, unit) do
      values = Enum.flat_map(pairs, fn pair -> numeric_path(pair, path) end)
      Docket.Benchmark.Stats.repetition_summary(values, unit)
    end

    defp numeric_path(value, path) do
      case get_in(value, path) do
        number when is_number(number) -> [number]
        _other -> []
      end
    end

    defp pair_status(valid, all) when valid == all and all != [], do: "complete"
    defp pair_status([], _all), do: "invalid"
    defp pair_status(_valid, _all), do: "partial"

    @doc false
    def bottleneck_evidence(points) do
      latency = %{
        claim_scan_p95_us: bottleneck_latency_summary(points, :claim_scan_total_us),
        claim_queue_p95_us: bottleneck_latency_summary(points, :claim_queue_time_us),
        repo_queue_p95_us: bottleneck_latency_summary(points, :repo_queue_time_us),
        repo_query_p95_us: bottleneck_latency_summary(points, :repo_query_time_us),
        lifecycle_transaction_p95_us:
          bottleneck_latency_summary(points, :lifecycle_transaction_us),
        node_execution_p95_us: bottleneck_latency_summary(points, :node_execution_us),
        vehicle_total_p95_us: bottleneck_latency_summary(points, :vehicle_total_us),
        vehicle_moment_loop_p95_ms:
          bottleneck_latency_summary(points, :vehicle_moment_loop_ms, "ms"),
        dispatcher_poll_p95_us: bottleneck_latency_summary(points, :dispatcher_poll_us),
        dispatcher_launch_p95_us: bottleneck_latency_summary(points, :dispatcher_launch_us)
      }

      database = %{
        active_time_ms_delta:
          repetition_value_summary(
            points,
            [:measurements, :amplification, :postgres_database_counters_delta, :active_time],
            "ms"
          ),
        active_time_percent_of_measured_wall:
          repetition_derived_summary(points, &database_active_time_percent/1, "percent"),
        deadlocks_delta:
          repetition_value_summary(
            points,
            [:measurements, :amplification, :postgres_database_counters_delta, :deadlocks],
            "count"
          ),
        conflicts_delta:
          repetition_value_summary(
            points,
            [:measurements, :amplification, :postgres_database_counters_delta, :conflicts],
            "count"
          ),
        active_backends_max_boundary:
          repetition_derived_summary(
            points,
            &contention_boundary_max(&1, :activity, :active_backends),
            "backends"
          ),
        active_waiting_backends_max_boundary:
          repetition_derived_summary(
            points,
            &contention_boundary_max(&1, :activity, :active_waiting_backends),
            "backends"
          ),
        lock_waiting_backends_max_boundary:
          repetition_derived_summary(
            points,
            &contention_boundary_max(&1, :activity, :lock_waiting_backends),
            "backends"
          ),
        ungranted_lock_rows_max_boundary:
          repetition_derived_summary(
            points,
            &contention_boundary_max(&1, :locks, :ungranted_lock_rows),
            "locks"
          )
      }

      summaries = Map.values(latency) ++ Map.values(database)

      %{
        aggregation:
          "median across successful reported trials of each trial's p95 latency or physical counter/gauge value",
        latency: latency,
        database: database,
        availability: %{
          requested_signal_count: length(summaries),
          available_signal_count: Enum.count(summaries, &(Map.get(&1, :sample_count, 0) > 0))
        },
        caveats: [
          "Latency signals are nested and overlapping spans; they must not be summed.",
          "Per-trial p95 values come from the bounded retained sample and can be unstable for small sample counts.",
          "pg_stat_database active_time is aggregate backend activity, not database CPU utilization, can exceed measured wall time with concurrent sessions, and has a broader physical snapshot scope than the point duration; it is context only.",
          "pg_stat_activity and pg_locks are boundary snapshots and can miss transient waits or locks.",
          "Unavailable server-version counters remain explicit zero-sample summaries rather than being inferred as zero."
        ]
      }
    end

    defp bottleneck_attribution(previous, current, tail_latency_path) do
      tail_latency = get_in(current, tail_latency_path)

      stage_evidence = %{
        repo_pool_queue:
          stage_bottleneck_signal(
            previous,
            current,
            [:bottleneck_evidence, :latency, :repo_queue_p95_us, :median],
            tail_latency,
            @bottleneck_queue_share_threshold_percent
          ),
        claim_scan:
          stage_bottleneck_signal(
            previous,
            current,
            [:bottleneck_evidence, :latency, :claim_scan_p95_us, :median],
            tail_latency,
            @bottleneck_queue_share_threshold_percent
          ),
        lifecycle_transaction:
          stage_bottleneck_signal(
            previous,
            current,
            [:bottleneck_evidence, :latency, :lifecycle_transaction_p95_us, :median],
            tail_latency,
            @bottleneck_lifecycle_share_threshold_percent
          ),
        node_execution:
          stage_bottleneck_signal(
            previous,
            current,
            [:bottleneck_evidence, :latency, :node_execution_p95_us, :median],
            tail_latency,
            @bottleneck_node_share_threshold_percent
          ),
        dispatcher_poll:
          stage_bottleneck_signal(
            previous,
            current,
            [:bottleneck_evidence, :latency, :dispatcher_poll_p95_us, :median],
            tail_latency,
            @bottleneck_queue_share_threshold_percent
          )
      }

      database = database_bottleneck_signal(current)

      candidates =
        stage_evidence
        |> Enum.flat_map(fn {name, evidence} ->
          if evidence.supported,
            do: [%{name: Atom.to_string(name), score: evidence.score}],
            else: []
        end)
        |> Kernel.++(
          if(database.supported,
            do: [%{name: "database_pressure", score: database.score}],
            else: []
          )
        )
        |> Enum.sort_by(&{-&1.score, &1.name})

      %{
        status: if(candidates == [], do: "inconclusive", else: "evidence_supported"),
        primary: candidates |> List.first() |> then(&if(&1, do: &1.name, else: nil)),
        contributors: Enum.map(candidates, & &1.name),
        tail_latency_p95_us: tail_latency,
        evidence: Map.put(stage_evidence, :database_pressure, database),
        method:
          "conservative threshold checks at the detected knee versus the preceding safe cell",
        caveats: [
          "Nested telemetry spans overlap and are never summed into a latency budget.",
          "Attribution is a diagnostic hypothesis, not proof of exclusive causality.",
          "Database active time is context only and cannot support attribution without a positive wait/lock gauge; boundary gauges can miss transient pressure."
        ]
      }
    end

    defp stage_bottleneck_signal(previous, current, path, tail_latency, share_threshold) do
      previous_value = get_in(previous, path)
      current_value = get_in(current, path)
      growth = percent_change(current_value, previous_value)

      share =
        if is_number(current_value) and is_number(tail_latency) and tail_latency > 0,
          do: Float.round(current_value * 100 / tail_latency, 3),
          else: nil

      supported =
        is_number(current_value) and current_value >= @bottleneck_minimum_latency_us and
          ((is_number(share) and share >= share_threshold) or
             (is_number(growth) and growth >= @bottleneck_growth_threshold_percent))

      %{
        supported: supported,
        previous_p95_us: previous_value,
        knee_p95_us: current_value,
        growth_percent: growth,
        share_of_knee_tail_percent: share,
        thresholds: %{
          minimum_latency_us: @bottleneck_minimum_latency_us,
          minimum_growth_percent: @bottleneck_growth_threshold_percent,
          minimum_tail_share_percent: share_threshold
        },
        score: bottleneck_score(share, growth)
      }
    end

    defp database_bottleneck_signal(current) do
      active_time =
        get_in(
          current,
          [
            :bottleneck_evidence,
            :database,
            :active_time_percent_of_measured_wall,
            :median
          ]
        )

      waiting =
        get_in(
          current,
          [
            :bottleneck_evidence,
            :database,
            :active_waiting_backends_max_boundary,
            :median
          ]
        )

      lock_waiting =
        get_in(
          current,
          [
            :bottleneck_evidence,
            :database,
            :lock_waiting_backends_max_boundary,
            :median
          ]
        )

      ungranted =
        get_in(
          current,
          [
            :bottleneck_evidence,
            :database,
            :ungranted_lock_rows_max_boundary,
            :median
          ]
        )

      wait_evidence = Enum.any?([waiting, lock_waiting, ungranted], &(is_number(&1) and &1 > 0))

      # pg_stat_database.active_time is summed across backend sessions and its
      # physical snapshot scope includes startup/pre-activation work. It is
      # useful context, but cannot establish database pressure on its own.
      supported = wait_evidence

      %{
        supported: supported,
        active_time_percent_of_measured_wall: active_time,
        active_waiting_backends_max_boundary: waiting,
        lock_waiting_backends_max_boundary: lock_waiting,
        ungranted_lock_rows_max_boundary: ungranted,
        active_time_supports_attribution: false,
        support_basis:
          "requires a positive boundary wait/lock signal; aggregate active_time is context only",
        score:
          if(wait_evidence,
            do: max(if(is_number(active_time), do: active_time, else: 0), 100.0),
            else: 0.0
          )
      }
    end

    defp bottleneck_score(share, growth) do
      max(
        if(is_number(share), do: share, else: 0),
        if(is_number(growth), do: growth, else: 0)
      )
    end

    defp bottleneck_latency_summary(points, metric, unit \\ "us") do
      repetition_value_summary(points, [:measurements, :latency, metric, :p95], unit)
    end

    defp repetition_derived_summary(points, fun, unit) do
      values = Enum.flat_map(points, fn point -> numeric_value(fun.(point)) end)
      Docket.Benchmark.Stats.repetition_summary(values, unit)
    end

    defp database_active_time_percent(point) do
      active_time_ms =
        get_in(
          point,
          [:measurements, :amplification, :postgres_database_counters_delta, :active_time]
        )

      duration_us = point[:duration_us]

      if is_number(active_time_ms) and is_number(duration_us) and duration_us > 0 do
        Float.round(active_time_ms * 100_000 / duration_us, 3)
      end
    end

    defp contention_boundary_max(point, section, metric) do
      before =
        get_in(
          point,
          [:measurements, :amplification, :postgres_contention, :before, section, metric]
        )

      after_snapshot =
        get_in(
          point,
          [:measurements, :amplification, :postgres_contention, :after, section, metric]
        )

      case Enum.filter([before, after_snapshot], &is_number/1) do
        [] -> nil
        values -> Enum.max(values)
      end
    end

    defp numeric_value(value) when is_number(value), do: [value]
    defp numeric_value(_value), do: []

    @doc false
    def concurrency_knee_analysis(summaries, "steady_arrival") do
      pools =
        summaries
        |> Enum.group_by(& &1.pool_size)
        |> Enum.sort_by(fn {pool_size, _cells} -> pool_size end)
        |> Enum.map(fn {pool_size, cells} ->
          steady_arrival_capacity_analysis(pool_size, cells)
        end)

      %{
        data_status: if(pools == [], do: "insufficient", else: "exploratory"),
        method: %{
          comparison: "generic throughput/latency knee disabled for open-loop steady arrivals",
          minimum_valid_cells_per_pool: @knee_minimum_valid_cells,
          minimum_successful_repetitions_per_cell: @knee_minimum_successful_repetitions,
          generic_safe_capacity_recommendation: "disabled",
          reason:
            "drain-inclusive throughput can hide arrival-window overload; offered/achieved rate, exact terminal backlog, and lag trends require workload-specific interpretation",
          sustainability_inputs: [
            "offered_rate_per_second",
            "achieved_arrival_window_rate_per_second",
            "due_outstanding_at_arrival_window_end",
            "backlog_growth_runs_per_second",
            "oldest_due_lag_growth_ms_per_second",
            "retained_completion_lag_p95_us"
          ]
        },
        pools: pools
      }
    end

    def concurrency_knee_analysis(summaries, scenario) do
      {tail_latency_metric, tail_latency_path} = knee_tail_latency(scenario)

      pools =
        summaries
        |> Enum.group_by(& &1.pool_size)
        |> Enum.sort_by(fn {pool_size, _cells} -> pool_size end)
        |> Enum.map(fn {pool_size, cells} ->
          pool_knee_analysis(pool_size, cells, tail_latency_metric, tail_latency_path)
        end)

      insufficient_count = Enum.count(pools, &(&1.status == "insufficient_data"))

      %{
        data_status:
          cond do
            pools == [] or insufficient_count == length(pools) -> "insufficient"
            insufficient_count > 0 -> "partial"
            true -> "sufficient"
          end,
        method: %{
          comparison: "adjacent successful concurrency cells sorted ascending",
          minimum_valid_cells_per_pool: @knee_minimum_valid_cells,
          minimum_successful_repetitions_per_cell: @knee_minimum_successful_repetitions,
          repetition_aggregation: "median throughput and median of per-trial p95 latency",
          thresholds: %{
            minimum_throughput_gain_percent: @knee_minimum_throughput_gain_percent,
            tail_latency_increase_percent: @knee_tail_latency_increase_percent
          }
        },
        pools: pools
      }
    end

    defp pool_knee_analysis(pool_size, cells, tail_latency_metric, tail_latency_path) do
      cells = Enum.sort_by(cells, & &1.concurrency)

      valid_cells =
        Enum.filter(cells, fn cell ->
          throughput = get_in(cell, [:throughput_per_second, :median])
          tail_latency = if tail_latency_path, do: get_in(cell, tail_latency_path)

          cell.success and
            successful_repetition_count(cell) >= @knee_minimum_successful_repetitions and
            is_number(throughput) and throughput > 0 and
            is_number(tail_latency) and tail_latency >= 0
        end)

      baseline = List.first(valid_cells)
      peak = peak_throughput_cell(valid_cells)

      base = %{
        pool_size: pool_size,
        status: nil,
        tail_latency_metric: tail_latency_metric,
        tested_cell_count: length(cells),
        valid_cell_count: length(valid_cells),
        minimum_successful_repetitions_per_cell: @knee_minimum_successful_repetitions,
        baseline: knee_point(baseline, tail_latency_path),
        peak_throughput: knee_point(peak, tail_latency_path),
        knee_point: nil,
        knee_reason: nil,
        bottleneck_attribution: nil,
        recommended_safe_concurrency: nil,
        recommendation_basis: nil,
        insufficient_data_reason: nil
      }

      cond do
        length(valid_cells) < @knee_minimum_valid_cells ->
          %{
            base
            | status: "insufficient_data",
              insufficient_data_reason:
                "requires at least #{@knee_minimum_valid_cells} successful concurrency cells with numeric throughput and p95 tail latency, each backed by at least #{@knee_minimum_successful_repetitions} successful repetitions"
          }

        knee = find_knee(valid_cells, tail_latency_path) ->
          %{previous: previous, current: current, reason: reason, changes: changes} = knee

          %{
            base
            | status: "knee_detected",
              knee_point:
                current
                |> knee_point(tail_latency_path)
                |> Map.put(:changes_from_previous_percent, changes),
              knee_reason: reason,
              bottleneck_attribution:
                bottleneck_attribution(previous, current, tail_latency_path),
              recommended_safe_concurrency: previous.concurrency,
              recommendation_basis: "highest tested concurrency before the detected knee"
          }

        true ->
          highest = List.last(valid_cells)

          %{
            base
            | status: "knee_not_observed",
              recommended_safe_concurrency: highest.concurrency,
              recommendation_basis:
                "highest successful tested concurrency before any observed knee"
          }
      end
    end

    defp steady_arrival_capacity_analysis(pool_size, cells) do
      cells = Enum.sort_by(cells, & &1.concurrency)

      observations =
        Enum.map(cells, fn cell ->
          offered =
            get_in(
              cell,
              [:latency_across_repetitions, :offered_rate_per_second, :median]
            )

          achieved =
            get_in(
              cell,
              [
                :latency_across_repetitions,
                :achieved_arrival_window_rate_per_second,
                :median
              ]
            )

          %{
            concurrency: cell.concurrency,
            successful_repetitions: successful_repetition_count(cell),
            evidence_grade:
              cell.success and
                successful_repetition_count(cell) >= @knee_minimum_successful_repetitions,
            offered_rate_per_second: offered,
            achieved_arrival_window_rate_per_second: achieved,
            achieved_percent_of_offered:
              if(is_number(offered) and offered > 0 and is_number(achieved),
                do: Float.round(achieved * 100 / offered, 3),
                else: nil
              ),
            due_outstanding_at_arrival_window_end:
              get_in(
                cell,
                [
                  :latency_across_repetitions,
                  :due_outstanding_at_arrival_window_end,
                  :median
                ]
              ),
            backlog_growth_runs_per_second:
              get_in(
                cell,
                [:latency_across_repetitions, :backlog_growth_runs_per_second, :median]
              ),
            oldest_due_lag_growth_ms_per_second:
              get_in(
                cell,
                [
                  :latency_across_repetitions,
                  :oldest_due_lag_growth_ms_per_second,
                  :median
                ]
              ),
            retained_completion_lag_p95_us:
              get_in(
                cell,
                [:latency_across_repetitions, :retained_completion_lag_p95_us, :median]
              )
          }
        end)

      evidence_grade_count = Enum.count(observations, & &1.evidence_grade)

      %{
        pool_size: pool_size,
        status: "exploratory_only",
        tested_cell_count: length(cells),
        evidence_grade_cell_count: evidence_grade_count,
        minimum_successful_repetitions_per_cell: @knee_minimum_successful_repetitions,
        sustainability_observations: observations,
        knee_point: nil,
        knee_reason: nil,
        recommended_safe_concurrency: nil,
        recommendation_basis:
          "none: the generic drain-inclusive throughput knee is disabled for steady_arrival",
        insufficient_data_reason:
          if(evidence_grade_count < @knee_minimum_valid_cells,
            do:
              "fewer than #{@knee_minimum_valid_cells} cells have at least #{@knee_minimum_successful_repetitions} successful repetitions",
            else: nil
          )
      }
    end

    defp successful_repetition_count(cell) do
      case cell[:successful_repetitions] do
        count when is_integer(count) and count >= 0 -> count
        _other -> get_in(cell, [:throughput_per_second, :sample_count]) || 0
      end
    end

    defp find_knee(cells, tail_latency_path) do
      cells
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.find_value(fn [previous, current] ->
        previous_throughput = get_in(previous, [:throughput_per_second, :median])
        current_throughput = get_in(current, [:throughput_per_second, :median])
        previous_latency = get_in(previous, tail_latency_path)
        current_latency = get_in(current, tail_latency_path)

        throughput_change = percent_change(current_throughput, previous_throughput)
        latency_change = percent_change(current_latency, previous_latency)

        throughput_plateau =
          is_number(throughput_change) and
            throughput_change < @knee_minimum_throughput_gain_percent

        latency_increase =
          is_number(latency_change) and
            latency_change >= @knee_tail_latency_increase_percent

        reason = knee_reason(throughput_plateau, latency_increase, throughput_change)

        if reason do
          %{
            previous: previous,
            current: current,
            reason: reason,
            changes: %{
              throughput: throughput_change,
              tail_latency_p95: latency_change
            }
          }
        end
      end)
    end

    defp knee_reason(true, true, _throughput_change),
      do: "throughput_plateau_and_tail_latency_increase"

    defp knee_reason(true, false, throughput_change) when throughput_change < 0,
      do: "throughput_regression"

    defp knee_reason(true, false, _throughput_change), do: "throughput_plateau"
    defp knee_reason(false, true, _throughput_change), do: "tail_latency_increase"
    defp knee_reason(false, false, _throughput_change), do: nil

    defp peak_throughput_cell([]), do: nil

    defp peak_throughput_cell(cells) do
      Enum.max_by(cells, fn cell ->
        {get_in(cell, [:throughput_per_second, :median]), -cell.concurrency}
      end)
    end

    defp knee_point(nil, _tail_latency_path), do: nil

    defp knee_point(cell, tail_latency_path) do
      %{
        concurrency: cell.concurrency,
        throughput_per_second: get_in(cell, [:throughput_per_second, :median]),
        tail_latency_p95_us: get_in(cell, tail_latency_path)
      }
    end

    defp percent_change(_current, 0), do: nil

    defp percent_change(current, previous) when is_number(current) and is_number(previous) do
      Float.round((current - previous) * 100 / previous, 3)
    end

    defp percent_change(_current, _previous), do: nil

    defp knee_tail_latency("claim_only"),
      do:
        {"burst_start_to_claim_p95_us",
         [:latency_across_repetitions, :burst_start_to_claim_p95_us, :median]}

    defp knee_tail_latency("blocked_vehicles"),
      do:
        {"unrelated_short_query_p95_us",
         [:latency_across_repetitions, :unrelated_short_query_p95_us, :median]}

    defp knee_tail_latency("steady_arrival"),
      do:
        {"retained_completion_lag_p95_us",
         [:latency_across_repetitions, :retained_completion_lag_p95_us, :median]}

    defp knee_tail_latency(scenario)
         when scenario in [
                "empty_one_step",
                "cyclic_vs_one_step",
                "mixed_service_times",
                "parked_wait_vs_blocking_wait"
              ],
         do:
           {"burst_activation_to_terminal_commit_p95_us",
            [
              :latency_across_repetitions,
              :burst_activation_to_terminal_commit_p95_us,
              :median
            ]}

    defp knee_tail_latency(_scenario), do: {"unavailable", nil}

    @doc false
    def suite_latency_summary(points, "empty_one_step") do
      common_burst_latency_summary(points)
    end

    def suite_latency_summary(points, scenario)
        when scenario in [
               "cyclic_vs_one_step",
               "mixed_service_times",
               "parked_wait_vs_blocking_wait"
             ] do
      points
      |> common_burst_latency_summary()
      |> Map.put(:cohorts, cohort_repetition_summary(points))
    end

    @doc false
    def suite_latency_summary(points, "claim_only") do
      %{
        burst_start_to_claim_p50_us:
          repetition_latency_summary(points, :burst_start_to_claim_offset_us, :p50),
        burst_start_to_claim_p95_us:
          repetition_latency_summary(points, :burst_start_to_claim_offset_us, :p95),
        claim_scan_total_p50_us: repetition_latency_summary(points, :claim_scan_total_us, :p50),
        claim_scan_total_p95_us: repetition_latency_summary(points, :claim_scan_total_us, :p95)
      }
    end

    @doc false
    def suite_latency_summary(points, "steady_arrival") do
      %{
        offered_rate_per_second:
          repetition_value_summary(
            points,
            [:measurements, :steady_arrival, :offered_rate_per_second],
            "runs_per_second"
          ),
        achieved_arrival_window_rate_per_second:
          repetition_value_summary(
            points,
            [:measurements, :steady_arrival, :achieved_arrival_window_rate_per_second],
            "runs_per_second"
          ),
        due_outstanding_at_arrival_window_end:
          repetition_value_summary(
            points,
            [:measurements, :steady_arrival, :due_outstanding_at_arrival_window_end],
            "runs"
          ),
        retained_completion_lag_p50_us:
          repetition_value_summary(
            points,
            [:measurements, :steady_arrival, :retained_completion_lag_us, :p50],
            "us"
          ),
        retained_completion_lag_p95_us:
          repetition_value_summary(
            points,
            [:measurements, :steady_arrival, :retained_completion_lag_us, :p95],
            "us"
          ),
        backlog_growth_runs_per_second:
          repetition_value_summary(
            points,
            [:measurements, :steady_arrival, :backlog_growth_runs_per_second],
            "runs_per_second"
          ),
        oldest_due_lag_growth_ms_per_second:
          repetition_value_summary(
            points,
            [:measurements, :steady_arrival, :oldest_due_lag_growth_ms_per_second],
            "ms_per_second"
          )
      }
    end

    @doc false
    def suite_latency_summary(points, "blocked_vehicles") do
      %{
        plateau_fill_duration_us:
          repetition_value_summary(
            points,
            [:measurements, :blocked_vehicles, :plateau_fill_duration_us],
            "us"
          ),
        activation_to_blocked_node_p95_us:
          repetition_value_summary(
            points,
            [
              :measurements,
              :blocked_vehicles,
              :latency,
              :activation_to_blocked_node_offset_us,
              :p95
            ],
            "us"
          ),
        gate_release_to_terminal_p95_us:
          repetition_value_summary(
            points,
            [
              :measurements,
              :blocked_vehicles,
              :latency,
              :gate_release_to_terminal_commit_us,
              :p95
            ],
            "us"
          ),
        unrelated_short_query_p95_us:
          repetition_value_summary(
            points,
            [
              :measurements,
              :blocked_vehicles,
              :latency,
              :unrelated_short_query_round_trip_us,
              :p95
            ],
            "us"
          )
      }
    end

    defp common_burst_latency_summary(points) do
      %{
        burst_activation_to_first_commit_p50_us:
          repetition_latency_summary(
            points,
            :burst_activation_to_first_commit_offset_us,
            :p50
          ),
        burst_activation_to_first_commit_p95_us:
          repetition_latency_summary(
            points,
            :burst_activation_to_first_commit_offset_us,
            :p95
          ),
        first_commit_to_terminal_p50_us:
          repetition_latency_summary(points, :first_commit_to_terminal_us, :p50),
        first_commit_to_terminal_p95_us:
          repetition_latency_summary(points, :first_commit_to_terminal_us, :p95),
        burst_activation_to_terminal_commit_p50_us:
          repetition_latency_summary(
            points,
            :burst_activation_to_terminal_commit_offset_us,
            :p50
          ),
        burst_activation_to_terminal_commit_p95_us:
          repetition_latency_summary(
            points,
            :burst_activation_to_terminal_commit_offset_us,
            :p95
          )
      }
    end

    defp cohort_repetition_summary(points) do
      points
      |> Enum.flat_map(fn point ->
        Map.keys(get_in(point, [:measurements, :cohorts]) || %{})
      end)
      |> Enum.uniq()
      |> Map.new(fn label ->
        cohort = [:measurements, :cohorts, label]

        {label,
         %{
           activation_to_terminal_p50_us:
             repetition_value_summary(
               points,
               cohort ++ [:activation_to_terminal_commit_offset_us, :p50],
               "us"
             ),
           activation_to_terminal_p95_us:
             repetition_value_summary(
               points,
               cohort ++ [:activation_to_terminal_commit_offset_us, :p95],
               "us"
             ),
           first_claim_to_terminal_p50_us:
             repetition_value_summary(
               points,
               cohort ++ [:first_claim_to_terminal_commit_us, :p50],
               "us"
             ),
           queue_share_of_median_percent:
             repetition_value_summary(
               points,
               cohort ++ [:queue_share_of_median_percent],
               "percent"
             )
         }}
      end)
    end

    @doc false
    def repetition_value_summary(points, path, unit) do
      values =
        Enum.flat_map(points, fn point ->
          case get_in(point, path) do
            value when is_number(value) -> [value]
            _other -> []
          end
        end)

      Docket.Benchmark.Stats.repetition_summary(values, unit)
    end

    @doc false
    def repetition_latency_summary(points, metric, statistic) do
      values =
        Enum.flat_map(points, fn point ->
          case get_in(point, [:measurements, :latency, metric, statistic]) do
            value when is_number(value) -> [value]
            _other -> []
          end
        end)

      Docket.Benchmark.Stats.repetition_summary(values, "us")
    end

    @doc false
    def write_results!(path, format, artifacts, payload) do
      path = Path.expand(path)
      File.mkdir_p!(Path.dirname(path))
      temp = path <> ".tmp-#{System.unique_integer([:positive])}"

      contents =
        case format do
          "json" ->
            JSON.encode_to_iodata!(payload)

          "ndjson" ->
            summary = payload |> Map.delete(:points) |> Map.put(:record_type, "suite_summary")

            Enum.map(artifacts, fn artifact ->
              [JSON.encode_to_iodata!(Map.put(artifact, :record_type, "point")), ?\n]
            end) ++ [[JSON.encode_to_iodata!(summary), ?\n]]
        end

      File.write!(temp, contents)
      File.rename!(temp, path)
    end
  end
end
