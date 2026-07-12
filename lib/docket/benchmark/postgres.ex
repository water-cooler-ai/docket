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
    alias Docket.Benchmark.Repo

    @migration_version 20_260_711_000_038
    @runtime Docket.Benchmark.Runtime
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

    def run(config) do
      points = Docket.Benchmark.plan(config)

      with {:ok, artifacts} <- run_points(points) do
        payload =
          if config.format == "ndjson",
            do: suite_summary_payload(artifacts),
            else: suite_payload(artifacts)

        write_results!(config.output, config.format, artifacts, payload)

        result = %{output: Path.expand(config.output), artifact: payload, artifacts: artifacts}

        if payload.success do
          {:ok, result}
        else
          {:error,
           "one or more benchmark trials failed; results were written to #{result.output}"}
        end
      end
    end

    defp run_points(points) do
      artifacts =
        Enum.map(points, fn point ->
          case run_point(point) do
            {:ok, artifact} -> artifact
            {:error, reason} -> failure_artifact(point, reason)
          end
        end)

      {:ok, artifacts}
    end

    defp failure_artifact(config, reason) do
      %{
        schema_version: 3,
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
        cleanup: %{isolated_database_removed: nil},
        failure_stage: "setup_or_execution",
        error: if(is_binary(reason), do: reason, else: inspect(reason))
      }
    end

    defp run_point(config) do
      try do
        run_point!(config)
      rescue
        error -> {:error, Exception.message(error)}
      catch
        kind, reason -> {:error, Exception.format_banner(kind, reason)}
      end
    end

    defp run_point!(config) do
      database = isolated_database(config)
      previous_repo_config = Application.fetch_env(:docket, Repo)

      repo_config = [
        url: database.url,
        pool_size: config.pool_size,
        log: false,
        telemetry_prefix: [:docket, :benchmark, :repo]
      ]

      Application.put_env(:docket, Repo, repo_config)

      primary = primary_config(config.database_url)

      try do
        create_database!(primary, database.name)
        run_created_database(config, database, primary)
      after
        try do
          Docket.Postgres.GraphCache.clear()
        after
          restore_repo_config(previous_repo_config)
        end
      end
    end

    defp run_created_database(config, database, primary) do
      execution =
        try do
          {:ok, repo} = Repo.start_link()

          try do
            :ok = Ecto.Migrator.up(Repo, @migration_version, Migration, log: false)
            execute(config, database, repo)
          after
            if Process.alive?(repo), do: GenServer.stop(repo, :normal, 5_000)
          end
        rescue
          error -> {:error, Exception.message(error)}
        catch
          kind, reason -> {:error, Exception.format_banner(kind, reason)}
        end

      merge_database_cleanup(execution, drop_database(primary, database.name))
    end

    defp merge_database_cleanup({:ok, artifact}, :ok) do
      {:ok, Map.put(artifact, :cleanup, %{isolated_database_removed: true})}
    end

    defp merge_database_cleanup({:ok, artifact}, {:error, reason}) do
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

    defp merge_database_cleanup({:error, reason}, :ok), do: {:error, reason}

    defp merge_database_cleanup({:error, reason}, {:error, cleanup_reason}),
      do: {:error, "#{reason}; isolated database cleanup also failed: #{cleanup_reason}"}

    defp restore_repo_config({:ok, config}), do: Application.put_env(:docket, Repo, config)
    defp restore_repo_config(:error), do: Application.delete_env(:docket, Repo)

    defp execute(%{scenario: "claim_only"} = config, database, repo),
      do: execute_claim_only(config, database, repo)

    defp execute(config, database, repo), do: execute_empty_one_step(config, database, repo)

    defp execute_empty_one_step(config, _database, _repo) do
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
          schema_version: 3,
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

    defp execute_claim_only(config, _database, _repo) do
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
           schema_version: 3,
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

    defp stage_claim_only(expired_ids, ready_at, expired_claimed_at) do
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

    defp run_claimers!(config, claim_now, leases, counters) do
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

    defp claim_worker(backlog, policy, leases, counters) do
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

    defp record_claim!(lease, leases, counters) do
      unique_id? = :ets.insert_new(leases, {{:run, lease.run_id}, true})
      unique_token? = :ets.insert_new(leases, {{:token, lease.claim_token}, true})

      if unique_id? and unique_token? do
        :atomics.add(counters, 1, 1)
      else
        :atomics.add(counters, 2, 1)
        raise "claim-only scenario observed a duplicate lease identity"
      end
    end

    defp runtime_opts(config, extra \\ []) do
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

    defp seed_runs(ref, count) do
      Enum.map(1..count, fn _ ->
        {:ok, run} = Docket.start_run(@runtime, ref, %{})
        run.id
      end)
    end

    defp wait_for_completion(collector, expected, timeout_ms) do
      deadline = System.monotonic_time(:millisecond) + timeout_ms
      wait(collector, expected, deadline)
    end

    defp wait(collector, expected, deadline) do
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

    defp stage_activation(activation_at) do
      Ecto.Adapters.SQL.query!(
        Repo,
        "UPDATE docket_runs SET wake_at = $1 WHERE status = 'running' AND claim_token IS NULL",
        [activation_at]
      )
    end

    defp run_warmup(config, run_ids) do
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

    defp activation_boundary do
      lead_ms = 250

      {
        DateTime.add(DateTime.utc_now(), lead_ms, :millisecond),
        System.monotonic_time() + System.convert_time_unit(lead_ms, :millisecond, :native)
      }
    end

    defp prepare_measured_activation(lead_ms \\ 250) do
      activation_at = DateTime.add(DateTime.utc_now(), lead_ms, :millisecond)
      stage_activation(activation_at)
      physical_before = physical_snapshot()
      remaining_ms = DateTime.diff(activation_at, DateTime.utc_now(), :millisecond)

      if remaining_ms >= 100 do
        t0 =
          System.monotonic_time() +
            System.convert_time_unit(remaining_ms, :millisecond, :native)

        {activation_at, t0, physical_before}
      else
        prepare_measured_activation(lead_ms * 2)
      end
    end

    defp sleep_until(activation_monotonic) do
      remaining = activation_monotonic - System.monotonic_time()

      if remaining > 0 do
        Process.sleep(System.convert_time_unit(remaining, :native, :millisecond))
      end
    end

    defp start_runtime!(opts, collector) do
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

    defp cleanup_collector(collector) do
      if :ets.info(collector.table) != :undefined do
        Docket.Benchmark.Collector.stop(collector)
      end
    end

    defp with_manual_runtime(opts, fun) do
      {:ok, runtime} = Docket.Runtime.Supervisor.start_link(opts)

      try do
        fun.()
      after
        if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)
      end
    end

    defp measurements(
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
      repo_queries = event_measurements(events, [:docket, :benchmark, :repo, :query])
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

      invalid_checkpoint_shapes =
        Enum.count(checkpoint_counts, fn {_id, count} -> count != 2 end)

      invalid_terminal_shapes =
        Enum.count(checkpoint_counts, fn {id, _count} -> terminal_counts[id] != 1 end)

      unknown_correlation_events =
        Enum.count(checkpoints ++ completions, fn {_measurements, metadata, _observed_at} ->
          is_nil(metadata.correlation_id)
        end)

      burst_duration_native = Enum.max(completion_offsets, fn -> duration_native end)

      ready_lags = Enum.map(ready_attempts, fn {m, _meta, _at} -> m.eligible_age_ms end)
      invalid_ready_lags = Enum.count(ready_lags, &(&1 < 0))

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
          repo_queries: length(repo_queries)
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
          expected_ready_claim_samples: config.runs,
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
            length(ready_attempts) == config.runs and map_size(first_commit_times) == config.runs and
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

    defp amplification(
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

    defp event_records(events, event) do
      for {^event, measurements, metadata, observed_at} <- events,
          do: {measurements, metadata, observed_at}
    end

    defp event_measurements(events, event) do
      Enum.map(event_records(events, event), fn {measurements, _metadata, _observed_at} ->
        measurements
      end)
    end

    defp correlation_times(records, selector) do
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

    defp native_event_distribution(events, event),
      do: native_metric_distribution(event_measurements(events, event), :duration)

    defp native_metric_distribution(measurements, key) do
      measurements
      |> Enum.flat_map(fn measurement ->
        if is_number(measurement[key]), do: [measurement[key]], else: []
      end)
      |> Docket.Benchmark.Stats.native_distribution()
    end

    defp sum(measurements, key),
      do:
        Enum.reduce(measurements, 0, fn measurement, total -> total + (measurement[key] || 0) end)

    defp max_value([], _key), do: 0
    defp max_value(measurements, key), do: measurements |> Enum.map(&(&1[key] || 0)) |> Enum.max()
    defp ratio(_value, 0), do: nil
    defp ratio(value, denominator), do: Float.round(value / denominator, 3)

    defp physical_snapshot do
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

    defp map_delta(after_map, before_map),
      do: Map.new(after_map, fn {key, value} -> {key, value - before_map[key]} end)

    defp claim_only_measurements(
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

    defp claim_only_invariants(config, setup, counters) do
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

    defp count_ids_at_attempt([], _attempt), do: 0

    defp count_ids_at_attempt(ids, attempt) do
      scalar_with(
        "SELECT count(*) FROM docket_runs WHERE run_id = ANY($1::text[]) AND claim_attempts = $2",
        [ids, attempt]
      )
    end

    defp scalar_with(sql, params) do
      %{rows: [[value]]} = Ecto.Adapters.SQL.query!(Repo, sql, params)
      value
    end

    defp invariant(name, actual, expected),
      do: %{name: name, pass: actual == expected, expected: expected, actual: actual}

    defp artifact_parameters(config) do
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

    defp claim_only_warnings(config) do
      [
        "Observed claim throughput is environment-specific and is not a database ceiling.",
        "This point is repetition #{config.repetition} of #{config.repetitions} with a frozen claim clock.",
        "Claim query timing is client-observed Ecto timing, not server-exclusive execution time.",
        "Run in a dedicated, quiescent BEAM; unrelated global Docket telemetry can contaminate operational distributions."
      ]
    end

    defp warnings(config) do
      [
        "Observed throughput is environment-specific and is not a capacity maximum.",
        "This point is repetition #{config.repetition} of #{config.repetitions} with #{config.warmup} warmup runs.",
        "A staged burst is not a steady-state arrival workload.",
        "p95/p99 values from #{config.runs} smoke samples are descriptive only.",
        "Claim query timing is client-observed Ecto timing, not server-exclusive execution time.",
        "Run in a dedicated, quiescent BEAM; unrelated global Docket telemetry can contaminate operational distributions."
      ]
    end

    defp invariants(config) do
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

    defp graph do
      Docket.Graph.new!(id: "docket-bench-empty-one-step")
      |> Docket.Graph.put_node!("noop", implementation: Docket.Benchmark.NoopNode)
      |> Docket.Graph.put_edge!("start-noop", from: "$start", to: "noop")
      |> Docket.Graph.put_edge!("noop-finish", from: "noop", to: "$finish")
    end

    defp canonical_scenario("smoke"), do: "empty_one_step"
    defp canonical_scenario(scenario), do: scenario

    defp environment(config) do
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

    defp settings do
      names =
        ~w(synchronous_commit fsync full_page_writes wal_level max_connections shared_buffers)

      Enum.into(names, %{}, fn name -> {name, scalar("SHOW #{name}")} end)
    end

    defp scalar(sql) do
      %{rows: [[value]]} = Ecto.Adapters.SQL.query!(Repo, sql, [])
      value
    end

    defp git_metadata do
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

    defp docket_project? do
      Code.ensure_loaded?(Mix.Project) and Mix.Project.get() != nil and
        Mix.Project.config()[:app] == :docket
    rescue
      _error -> false
    end

    defp git_value(args) do
      case System.cmd("git", args, stderr_to_stdout: true) do
        {value, 0} -> String.trim(value)
        _ -> nil
      end
    end

    defp env_dirty do
      case System.get_env("DOCKET_BENCH_DIRTY") do
        value when value in ["1", "true", "TRUE"] -> true
        value when value in ["0", "false", "FALSE"] -> false
        _ -> nil
      end
    end

    defp cpu_model do
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

    defp total_memory do
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

    defp rate(_runs, 0), do: nil

    defp rate(runs, duration_native) do
      duration_us = System.convert_time_unit(duration_native, :native, :microsecond)
      if duration_us == 0, do: nil, else: Float.round(runs * 1_000_000 / duration_us, 3)
    end

    defp suite_payload([artifact]), do: artifact
    defp suite_payload(artifacts), do: suite_summary_payload(artifacts)

    defp suite_summary_payload(artifacts) do
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
        schema_version: 3,
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

    defp suite_latency_summary(points, "empty_one_step") do
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

    defp suite_latency_summary(points, "claim_only") do
      %{
        burst_start_to_claim_p50_us:
          repetition_latency_summary(points, :burst_start_to_claim_offset_us, :p50),
        burst_start_to_claim_p95_us:
          repetition_latency_summary(points, :burst_start_to_claim_offset_us, :p95),
        claim_scan_total_p50_us: repetition_latency_summary(points, :claim_scan_total_us, :p50),
        claim_scan_total_p95_us: repetition_latency_summary(points, :claim_scan_total_us, :p95)
      }
    end

    defp repetition_latency_summary(points, metric, statistic) do
      values =
        Enum.flat_map(points, fn point ->
          case get_in(point, [:measurements, :latency, metric, statistic]) do
            value when is_number(value) -> [value]
            _other -> []
          end
        end)

      Docket.Benchmark.Stats.repetition_summary(values, "us")
    end

    defp write_results!(path, format, artifacts, payload) do
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

    defp isolated_database(config) do
      uri = URI.parse(config.database_url)

      name =
        "docket_bench_#{System.system_time(:millisecond)}_#{System.unique_integer([:positive])}"

      %{name: name, url: %{uri | path: "/" <> name} |> URI.to_string()}
    end

    defp primary_config(url) do
      uri = URI.parse(url)

      [username, password] =
        case String.split(uri.userinfo || System.get_env("USER") || "postgres", ":", parts: 2) do
          [username, password] -> [URI.decode(username), URI.decode(password)]
          [username] -> [URI.decode(username), nil]
        end

      [
        hostname: uri.host || "localhost",
        port: uri.port || 5432,
        username: username,
        password: password,
        database: "postgres",
        pool_size: 1
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    end

    defp create_database!(config, name) do
      {:ok, pid} = Postgrex.start_link(config)

      try do
        Postgrex.query!(pid, "CREATE DATABASE \"#{name}\"", [])
      after
        GenServer.stop(pid)
      end
    end

    defp drop_database(config, name) do
      try do
        do_drop_database(config, name)
      rescue
        error -> {:error, "failed to remove isolated benchmark database: #{error_message(error)}"}
      catch
        kind, reason ->
          {:error,
           "failed to remove isolated benchmark database: #{Exception.format_banner(kind, reason)}"}
      end
    end

    defp do_drop_database(config, name) do
      case Postgrex.start_link(config) do
        {:ok, pid} ->
          try do
            case Postgrex.query(pid, "DROP DATABASE IF EXISTS \"#{name}\" WITH (FORCE)", []) do
              {:ok, _result} ->
                :ok

              {:error, reason} ->
                {:error, "failed to drop isolated benchmark database: #{error_message(reason)}"}
            end
          after
            GenServer.stop(pid)
          end

        {:error, reason} ->
          {:error,
           "failed to connect for isolated benchmark database cleanup: #{error_message(reason)}"}
      end
    end

    defp error_message(%{__struct__: module} = error) do
      if function_exported?(module, :message, 1),
        do: Exception.message(error),
        else: inspect(error)
    end

    defp error_message(reason), do: inspect(reason)
  end
end
