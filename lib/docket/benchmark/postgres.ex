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
    @artifact_schema_version 4
    @minimum_runtime_ready_lead_ms 250
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
        point: %{
          concurrency: config.concurrency,
          pool_size: config.pool_size,
          repetition: config.repetition
        },
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
          phase_samples,
          maximum_claim_age_ms_at_release,
          quiescence_at,
          config
        ) do
      events =
        Enum.filter(events, fn {_event, _measurements, _metadata, observed_at} ->
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

      complete =
        gate_final.observed_runs == config.runs and gate_final.duplicate_runs == 0 and
          gate_final.unknown_runs == 0 and gate_final.invalid_attempts == 0 and
          gate_final.invalid_nodes == 0 and length(node_executions) == config.runs and
          length(drains) == config.runs and length(release_to_first) == config.concurrency and
          length(release_to_terminal) == config.concurrency and
          length(release.blocked_arrival_times) == config.concurrency and
          Enum.count(probes, & &1.success) == config.probe_count and
          length(probe_query_measurements) == config.probe_count and
          invalid_negative_release_offsets == 0

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
          maximum_vehicle_reported_claim_held_ms: max_value(drains, :claim_held_ms)
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
          expected_blocked_arrivals: config.concurrency,
          observed_blocked_arrivals: length(release.blocked_arrival_times),
          expected_gate_observations: config.runs,
          observed_gate_observations: gate_final.observed_runs,
          expected_node_execution_samples: config.runs,
          observed_node_execution_samples: length(node_executions),
          expected_vehicle_drain_samples: config.runs,
          observed_vehicle_drain_samples: length(drains),
          expected_plateau_release_pairs: config.concurrency,
          observed_release_to_first_pairs: length(release_to_first),
          observed_release_to_terminal_pairs: length(release_to_terminal),
          expected_probe_query_telemetry_samples: config.probe_count,
          observed_probe_query_telemetry_samples: length(probe_query_measurements),
          invalid_negative_release_offsets: invalid_negative_release_offsets,
          complete_sample_set: complete
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
          blocked.collection.invalid_negative_release_offsets,
          0
        ),
        invariant(
          "blocked workload retains complete correlated samples",
          blocked.collection.complete_sample_set,
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
      ] ++ extra
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
          events,
          t0,
          duration_native,
          config,
          physical_before,
          physical_after,
          collector_stats
        ) do
      pre_activation_events =
        Enum.count(events, fn {event, _measurements, _metadata, observed_at} ->
          observed_at < t0 and
            event in [
              [:docket, :checkpoint, :committed],
              [:docket, :run, :completed],
              [:docket, :postgres, :claim, :attempt]
            ]
        end)

      pre_activation_polls =
        Enum.count(events, fn {event, _measurements, _metadata, observed_at} ->
          observed_at < t0 and event == [:docket, :postgres, :dispatcher, :poll]
        end)

      events =
        Enum.filter(events, fn {_event, _measurements, _metadata, observed_at} ->
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
        Enum.count(repo_query_records, fn {_measurement, metadata, _at} ->
          metadata.benchmark_query == :probe
        end)

      benchmark_control_queries =
        Enum.count(repo_query_records, fn {_measurement, metadata, _at} ->
          metadata.benchmark_query == :control
        end)

      store = event_measurements(events, [:docket, :postgres, :store])

      completion_event_times = correlation_times(completions, :last)
      first_commit_times = correlation_times(checkpoints, :first)

      terminal_commit_times =
        checkpoints
        |> Enum.filter(fn {_measurements, metadata, _observed_at} ->
          metadata.checkpoint_type == "run_completed"
        end)
        |> correlation_times(:last)

      terminal_counts =
        checkpoints
        |> Enum.filter(fn {_measurements, metadata, _observed_at} ->
          metadata.checkpoint_type == "run_completed" and not is_nil(metadata.correlation_id)
        end)
        |> Enum.frequencies_by(fn {_measurements, metadata, _observed_at} ->
          metadata.correlation_id
        end)

      completion_offsets =
        Enum.map(terminal_commit_times, fn {_id, observed_at} -> observed_at - t0 end)

      first_commit_offsets =
        Enum.map(first_commit_times, fn {_id, observed_at} -> observed_at - t0 end)

      first_to_terminal =
        for {id, first_at} <- first_commit_times,
            terminal_at = terminal_commit_times[id],
            is_integer(terminal_at),
            do: terminal_at - first_at

      checkpoint_counts =
        checkpoints
        |> Enum.reject(fn {_measurements, metadata, _observed_at} ->
          is_nil(metadata.correlation_id)
        end)
        |> Enum.frequencies_by(fn {_measurements, metadata, _observed_at} ->
          metadata.correlation_id
        end)

      flexible_checkpoint_shapes = Map.get(config, :flexible_checkpoint_shapes, false)

      invalid_checkpoint_shapes =
        Enum.count(checkpoint_counts, fn {_id, count} ->
          if flexible_checkpoint_shapes, do: count < 2, else: count != 2
        end)

      invalid_terminal_shapes =
        Enum.count(checkpoint_counts, fn {id, _count} -> terminal_counts[id] != 1 end)

      unknown_correlation_events =
        Enum.count(checkpoints ++ completions, fn {_measurements, metadata, _observed_at} ->
          is_nil(metadata.correlation_id)
        end)

      burst_duration_native = Enum.max(completion_offsets, fn -> duration_native end)

      ready_lags = Enum.map(ready_attempts, fn {m, _meta, _at} -> m.eligible_age_ms end)
      invalid_ready_lags = Enum.count(ready_lags, &(&1 < 0))

      expected_ready_claim_samples = Map.get(config, :expected_ready_claim_samples, config.runs)

      %{
        completed_runs: length(completions),
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
          claim_scans: length(claim_scans),
          claim_query_samples: length(claim_queries),
          claim_leases: sum(claim_scans, :leases),
          claim_attempts: length(attempts),
          reacquired_claims:
            Enum.count(attempts, fn {_m, meta, _at} -> meta.result == :reacquired end),
          steals: sum(claim_scans, :steals),
          poisoned: sum(claim_scans, :poisoned),
          dispatcher_polls: length(polls),
          empty_polls: Enum.count(polls, &(&1.leases == 0 and &1.poisoned == 0)),
          maximum_in_flight_vehicles: max_value(states, :in_flight),
          committed_moments: sum(committed, :count),
          repo_queries: length(repo_queries),
          benchmark_probe_queries_excluded: benchmark_probe_queries,
          benchmark_control_queries_excluded: benchmark_control_queries
        },
        amplification:
          amplification(
            config,
            committed,
            repo_queries,
            store,
            physical_before,
            physical_after,
            pre_activation_polls
          ),
        collection: %{
          percentile_method: "nearest-rank, no interpolation",
          expected_ready_claim_samples: expected_ready_claim_samples,
          observed_ready_claim_samples: length(ready_attempts),
          observed_first_commit_samples: map_size(first_commit_times),
          observed_first_to_terminal_pairs: length(first_to_terminal),
          observed_terminal_commit_samples: map_size(terminal_commit_times),
          observed_completion_event_samples: map_size(completion_event_times),
          invalid_negative_ready_lag_samples: invalid_ready_lags,
          invalid_checkpoint_shapes: invalid_checkpoint_shapes,
          invalid_terminal_shapes: invalid_terminal_shapes,
          unknown_correlation_events: unknown_correlation_events,
          pre_activation_work_events: pre_activation_events,
          pre_activation_dispatcher_polls_in_physical_scope: pre_activation_polls,
          control_wait_duration_us:
            System.convert_time_unit(duration_native, :native, :microsecond),
          observer: collector_stats,
          complete_sample_set:
            length(ready_attempts) == expected_ready_claim_samples and
              map_size(first_commit_times) == config.runs and
              length(first_to_terminal) == config.runs and
              map_size(terminal_commit_times) == config.runs and
              map_size(completion_event_times) == config.runs and
              invalid_ready_lags == 0 and invalid_checkpoint_shapes == 0 and
              invalid_terminal_shapes == 0 and
              unknown_correlation_events == 0 and pre_activation_events == 0,
          telemetry_events: Enum.map(Docket.Benchmark.Collector.events(), &Enum.join(&1, "."))
        }
      }
    end

    # Shared event-to-artifact measurement derivations.

    @doc false
    def amplification(
          config,
          committed,
          repo_queries,
          store,
          before,
          after_snapshot,
          pre_activation_polls
        ) do
      event_rows = scalar("SELECT count(*) FROM docket_events")
      run_rows = scalar("SELECT count(*) FROM docket_runs")
      committed_count = sum(committed, :count)

      %{
        durable_run_rows: run_rows,
        durable_event_rows: event_rows,
        events_per_completed_run: ratio(event_rows, config.runs),
        committed_moments_per_run: ratio(committed_count, config.runs),
        repo_queries_per_run: ratio(length(repo_queries), config.runs),
        store_attempted_rows: sum(store, :attempted_rows),
        store_encoded_bytes: sum(store, :encoded_bytes),
        wal_bytes: after_snapshot.wal_bytes_position - before.wal_bytes_position,
        database_size_bytes_delta:
          after_snapshot.database_size_bytes - before.database_size_bytes,
        postgres_database_counters_delta:
          map_delta(after_snapshot.database_counters, before.database_counters),
        physical_delta_scope:
          "after workload staging and before runtime startup through the post-terminal snapshot",
        pre_activation_dispatcher_polls_in_scope: pre_activation_polls,
        caveat:
          "Physical deltas include runtime startup and pre-activation polling; WAL and pg_stat_database can also include concurrent server activity and stats lag."
      }
    end

    @doc false
    def event_records(events, event) do
      for {^event, measurements, metadata, observed_at} <- events,
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
      %{rows: [[wal_bytes_position, database_size_bytes]]} =
        Ecto.Adapters.SQL.query!(
          Repo,
          "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, pg_database_size(current_database())",
          []
        )

      columns =
        ~w(xact_commit xact_rollback blks_read blks_hit tup_returned tup_fetched tup_inserted tup_updated tup_deleted)a

      %{rows: [values]} =
        Ecto.Adapters.SQL.query!(
          Repo,
          "SELECT xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted FROM pg_stat_database WHERE datname = current_database()",
          []
        )

      %{
        wal_bytes_position: wal_bytes_position,
        database_size_bytes: database_size_bytes,
        database_counters: Enum.zip(columns, values) |> Map.new()
      }
    end

    @doc false
    def map_delta(after_map, before_map),
      do: Map.new(after_map, fn {key, value} -> {key, value - before_map[key]} end)

    @doc false
    def claim_only_measurements(
          events,
          t0,
          control_duration,
          config,
          setup,
          physical_before,
          physical_after,
          collector_stats
        ) do
      scans = event_records(events, [:docket, :postgres, :run_store, :claim])
      queries = event_measurements(events, [:docket, :postgres, :run_store, :claim_query])
      attempts = event_records(events, [:docket, :postgres, :claim, :attempt])
      ready = Enum.filter(attempts, fn {_m, metadata, _at} -> metadata.class == :ready end)
      expired = Enum.filter(attempts, fn {_m, metadata, _at} -> metadata.class == :expired end)

      claim_offsets = Enum.map(attempts, fn {_m, _metadata, at} -> at - t0 end)
      ready_offsets = Enum.map(ready, fn {_m, _metadata, at} -> at - t0 end)
      expired_offsets = Enum.map(expired, fn {_m, _metadata, at} -> at - t0 end)
      claim_window = Enum.max(claim_offsets, fn -> control_duration end)
      scan_measurements = Enum.map(scans, fn {measurement, _metadata, _at} -> measurement end)
      batch_sizes = Enum.map(scan_measurements, & &1.leases)
      nonempty = Enum.count(batch_sizes, &(&1 > 0))
      event_rows_after = scalar("SELECT count(*) FROM docket_events")

      invalid_ages =
        Enum.count(attempts, fn {measurement, _metadata, _at} ->
          measurement.eligible_age_ms < 0
        end)

      %{
        claimed_rows: length(attempts),
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
          total_scans: length(scans),
          nonempty_scans: nonempty,
          empty_scans: Enum.count(batch_sizes, &(&1 == 0)),
          full_scans: Enum.count(batch_sizes, &(&1 == config.batch_size)),
          partial_scans: Enum.count(batch_sizes, &(&1 > 0 and &1 < config.batch_size)),
          rows_per_scan: Docket.Benchmark.Stats.distribution(batch_sizes, & &1, "rows"),
          mean_rows_per_nonempty_scan:
            if(nonempty == 0, do: nil, else: Float.round(config.runs / nonempty, 3))
        },
        counts: %{
          ready_claims: length(ready),
          expired_claims: length(expired),
          reacquired_claims:
            Enum.count(attempts, fn {_m, metadata, _at} -> metadata.result == :reacquired end),
          steals: sum(scan_measurements, :steals),
          poisoned: sum(scan_measurements, :poisoned),
          configured_claimers: config.concurrency,
          claim_query_samples: length(queries)
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
          caveat:
            "WAL and pg_stat_database deltas can include concurrent server activity and stats lag."
        },
        collection: %{
          percentile_method: "nearest-rank, no interpolation",
          expected_claim_samples: config.runs,
          observed_claim_samples: length(attempts),
          expected_ready_samples: setup.ready_count,
          observed_ready_samples: length(ready),
          expected_expired_samples: setup.expired_count,
          observed_expired_samples: length(expired),
          scan_query_sample_match: length(scans) == length(queries),
          invalid_negative_age_samples: invalid_ages,
          control_wait_duration_us:
            System.convert_time_unit(control_duration, :native, :microsecond),
          observer: collector_stats,
          complete_sample_set:
            length(attempts) == config.runs and length(ready) == setup.ready_count and
              length(expired) == setup.expired_count and length(scans) == length(queries) and
              invalid_ages == 0
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
        :scenario
      ])
    end

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
      ]
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
      %{
        docket: git_metadata(),
        elixir: System.version(),
        otp_release: List.to_string(:erlang.system_info(:otp_release)),
        erts: List.to_string(:erlang.system_info(:version)),
        os: inspect(:os.type()),
        cpu_count: System.schedulers_online(),
        cpu_model: cpu_model(),
        postgres_version: scalar("SHOW server_version"),
        postgres_settings: settings(),
        repo_pool_size: Repo.config()[:pool_size],
        repo_pool_count: Repo.config()[:pool_count] || 1,
        dispatcher_nodes: config.nodes,
        storage_class: "unreported",
        ram_bytes: total_memory()
      }
    end

    # Host and Postgres environment capture.

    @doc false
    def settings do
      names =
        ~w(synchronous_commit fsync full_page_writes wal_level max_connections shared_buffers)

      Enum.into(names, %{}, fn name -> {name, scalar("SHOW #{name}")} end)
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
    def cpu_model do
      case System.cmd(
             "sh",
             [
               "-c",
               "sysctl -n machdep.cpu.brand_string 2>/dev/null || awk -F: '/model name/{print $2; exit}' /proc/cpuinfo"
             ],
             stderr_to_stdout: true
           ) do
        {value, 0} -> String.trim(value)
        _ -> "unreported"
      end
    end

    @doc false
    def total_memory do
      case System.cmd(
             "sh",
             [
               "-c",
               "sysctl -n hw.memsize 2>/dev/null || awk '/MemTotal/{print $2 * 1024}' /proc/meminfo"
             ],
             stderr_to_stdout: true
           ) do
        {value, 0} ->
          case Integer.parse(String.trim(value)) do
            {number, _} -> number
            _ -> nil
          end

        _ ->
          nil
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
      summaries =
        artifacts
        |> Enum.group_by(fn artifact ->
          {artifact.point.concurrency, artifact.point.pool_size}
        end)
        |> Enum.map(fn {{concurrency, pool_size}, points} ->
          successful = Enum.filter(points, & &1.success)

          %{
            concurrency: concurrency,
            pool_size: pool_size,
            repetitions: length(points),
            success: Enum.all?(points, & &1.success),
            throughput_per_second:
              Docket.Benchmark.Stats.repetition_summary(
                Enum.map(successful, & &1.measurements.throughput_per_second),
                "work_items_per_second"
              ),
            measured_duration_us:
              Docket.Benchmark.Stats.repetition_summary(
                Enum.map(successful, & &1.duration_us),
                "us"
              ),
            latency_across_repetitions: suite_latency_summary(successful, hd(points).scenario)
          }
        end)
        |> Enum.sort_by(&{&1.concurrency, &1.pool_size})

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
        summary: summaries,
        points: artifacts
      }
    end

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
