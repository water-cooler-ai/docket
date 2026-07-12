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
    @drain_event [:docket, :postgres, :vehicle, :drain]
    @claim_attempt_event [:docket, :postgres, :claim, :attempt]
    @checkpoint_event [:docket, :checkpoint, :committed]

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
      collector = Docket.Benchmark.Collector.start(run_ids, activation_at: t0)

      measurement_config =
        config
        |> Map.put(:expected_ready_claim_samples, expected_claims)
        |> Map.put(:flexible_checkpoint_shapes, kind != :mixed_service_times)

      extra = [executor: Docket.Executor.Task]

      extra =
        if kind in [:mixed_service_times, :parked_wait_vs_blocking_wait],
          do: Keyword.put(extra, :vehicle, comparative_attempt_budget(config)),
          else: extra

      runtime = start_runtime!(runtime_opts(config, extra), collector)

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

        measurement_config = expected_claims_after_yields(measurement_config, events, kind)

        comparative =
          events
          |> comparative_measurements(t0, labels, kind)
          |> add_mixed_slot_evidence(config, labels, kind)

        drain_fairness = drain_fairness_measurements(events, comparative.cohorts, config, kind)

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
          |> Map.put(:cohorts, comparative.cohorts)
          |> Map.put(:fairness, comparative.fairness)
          |> maybe_put_drain_fairness(drain_fairness)
          |> maybe_put_observed_cycle_commits(kind)

        invariants =
          invariants(config) ++
            drain_fairness_invariants(measurements[:drain_fairness], labels, kind)

        passed =
          Enum.all?(invariants, & &1.pass) and measurements.collection.telemetry_checks_pass

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
             drain_budget: runtime_drain_budget(config, kind),
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
               mixed_slot_warnings(comparative, kind) ++
               cycle_semantics_warnings(kind) ++
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
      slow =
        config.runs
        |> Kernel.*(config.slow_percent)
        |> div(100)
        |> max(1)
        |> min(config.runs - 1)

      labels = interleave(:slow, slow, :fast, config.runs - slow)

      specs = [slow: sleep_graph(config.hold_ms), fast: noop_graph("mixed-fast")]

      shape = %{
        kind: "interleaved_slow_and_fast",
        requested_slow_percent: config.slow_percent,
        actual_slow_percent: Float.round(slow * 100 / config.runs, 3),
        slow_runs: slow,
        fast_runs: config.runs - slow,
        slow_cohort_has_enough_runs_to_fill_all_slots: slow >= config.concurrency
      }

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
      max_supersteps = terminal_max_supersteps(config.cycle_moments)
      specs = [cyclic: cycle_graph(config.cycle_moments), one_step: noop_graph("cyclic-control")]

      shape = %{
        kind: "bounded_drain_cycle_vs_one_step",
        requested_cycle_iterations: config.cycle_moments,
        compatibility_option: "--cycle-moments",
        iteration_semantics:
          "one configured iteration increments the cycle counter and also traverses a separate decision superstep",
        terminal_max_supersteps: max_supersteps
      }

      {specs, labels, config.runs, shape}
    end

    defp add_mixed_slot_evidence(comparative, config, labels, :mixed_service_times) do
      slow_runs = Enum.count(labels, &(&1 == :slow))
      fast_runs = Enum.count(labels, &(&1 == :fast))
      can_fill_all_slots = slow_runs >= config.concurrency
      fast = Map.fetch!(comparative.cohorts, :fast)

      evidence = %{
        requested_slow_percent: config.slow_percent,
        actual_slow_percent: Float.round(slow_runs * 100 / config.runs, 3),
        slow_runs: slow_runs,
        fast_runs: fast_runs,
        configured_slots: config.concurrency,
        maximum_simultaneous_slow_runs_from_cohort_size: min(slow_runs, config.concurrency),
        maximum_slow_slot_occupancy_percent_from_cohort_size:
          Float.round(min(slow_runs, config.concurrency) * 100 / config.concurrency, 3),
        slow_cohort_has_enough_runs_to_fill_all_slots: can_fill_all_slots,
        observed_simultaneous_slow_slot_occupancy: "not_instrumented",
        evidence_scope:
          "capacity derived from cohort cardinality; this point does not attach run identity to active-slot telemetry"
      }

      fast_tail = %{
        basis:
          "per-run activation-to-terminal divided by first-claim-to-terminal service; no percentile subtraction",
        normalized_slowdown_p50_ratio: fast.normalized_slowdown.p50,
        normalized_slowdown_p95_ratio: fast.normalized_slowdown.p95,
        activation_to_first_claim_p95_us: fast.activation_to_first_claim_offset_us.p95,
        activation_to_terminal_p95_us: fast.activation_to_terminal_commit_offset_us.p95,
        first_claim_to_terminal_p95_us: fast.first_claim_to_terminal_commit_us.p95
      }

      fairness =
        comparative.fairness
        |> Map.put(:slow_slot_occupancy, evidence)
        |> Map.put(:fast_cohort_tail_slowdown, fast_tail)
        |> Map.put(:paired_all_fast_control, %{
          executed: false,
          reason:
            "a separate all-fast timed phase is not run inside this isolated burst; use normalized per-run slowdown or run a matched empty_one_step point"
        })

      %{comparative | fairness: fairness}
    end

    defp add_mixed_slot_evidence(comparative, _config, _labels, _kind), do: comparative

    defp mixed_slot_warnings(comparative, :mixed_service_times) do
      evidence = comparative.fairness.slow_slot_occupancy

      if evidence.slow_cohort_has_enough_runs_to_fill_all_slots do
        []
      else
        [
          "Slow cohort has #{evidence.slow_runs} runs for #{evidence.configured_slots} dispatcher slots and cannot occupy every slot; increase --slow-percent or --runs before using this point as an all-slot slow-hog test."
        ]
      end
    end

    defp mixed_slot_warnings(_comparative, _kind), do: []

    defp cycle_semantics_warnings(:cyclic_vs_one_step) do
      [
        "--cycle-moments is retained for CLI compatibility but controls cycle iterations, not an exact committed-moment count; use observed_commits for measured moment volume."
      ]
    end

    defp cycle_semantics_warnings(_kind), do: []

    defp expected_claims_after_yields(config, snapshot, :cyclic_vs_one_step) do
      Map.put(
        config,
        :expected_ready_claim_samples,
        config.runs + budget_yield_counts(snapshot).total
      )
    end

    defp expected_claims_after_yields(config, _snapshot, _kind), do: config

    defp drain_fairness_measurements(snapshot, cohorts, config, :cyclic_vs_one_step) do
      yields = budget_yield_counts(snapshot)
      correlations = Docket.Benchmark.Collector.correlation_summary(snapshot)

      checkpoint_frequencies =
        correlations.checkpoint_count_frequencies
        |> Enum.sort_by(fn {checkpoint_commits, _run_count} -> checkpoint_commits end)
        |> Enum.map(fn {checkpoint_commits, run_count} ->
          %{checkpoint_commits_per_run: checkpoint_commits, run_count: run_count}
        end)

      retained_sample_checkpoint_commits =
        Enum.reduce(checkpoint_frequencies, 0, fn frequency, total ->
          total + frequency.checkpoint_commits_per_run * frequency.run_count
        end)

      exact_checkpoint_commits =
        Docket.Benchmark.Collector.observation_count(snapshot, @checkpoint_event)

      total_claims =
        Docket.Benchmark.Collector.observation_count(snapshot, @claim_attempt_event)

      reacquisitions = max(total_claims - config.runs, 0)
      fast = Map.fetch!(cohorts, :one_step)

      %{
        budget_yields: yields,
        claims: %{
          initial_claims: config.runs,
          total_claims: total_claims,
          reacquisitions_after_yield: reacquisitions,
          yield_reacquisition_difference: reacquisitions - yields.total
        },
        observed_commits: %{
          exact_checkpoint_commits_across_all_cohorts: exact_checkpoint_commits,
          exact_lifecycle_commits_across_all_cohorts: nil,
          checkpoint_commits_per_run_retained_sample: %{
            sampled_runs: correlations.sampled,
            population_runs: correlations.expected,
            retained_sample_checkpoint_commits: retained_sample_checkpoint_commits,
            frequencies: checkpoint_frequencies,
            scope:
              "deterministic bounded correlation sample only; frequencies are not a global count when sampled_runs is smaller than population_runs"
          },
          scope:
            "exact global aggregate checkpoint and lifecycle observations after activation across both cohorts; counts include step and terminal commits but exclude pre-collector initialization"
        },
        proof_scope:
          "aggregate-only; bounded metric telemetry deliberately carries no run identity, so yield and reacquisition counts are not attributed per cyclic run",
        fast_one_step_tail_impact: %{
          cohort: "one_step",
          runs: fast.runs,
          completed_runs: fast.completed_runs,
          activation_to_first_claim_p95_us: fast.activation_to_first_claim_offset_us.p95,
          activation_to_terminal_p95_us: fast.activation_to_terminal_commit_offset_us.p95,
          first_claim_to_terminal_p95_us: fast.first_claim_to_terminal_commit_us.p95
        }
      }
    end

    defp drain_fairness_measurements(_snapshot, _cohorts, _config, _kind), do: nil

    defp maybe_put_drain_fairness(measurements, nil), do: measurements

    defp maybe_put_drain_fairness(measurements, drain_fairness),
      do: Map.put(measurements, :drain_fairness, drain_fairness)

    defp maybe_put_observed_cycle_commits(measurements, :cyclic_vs_one_step) do
      put_in(
        measurements,
        [:drain_fairness, :observed_commits, :exact_lifecycle_commits_across_all_cohorts],
        measurements.counts.committed_moments
      )
    end

    defp maybe_put_observed_cycle_commits(measurements, _kind), do: measurements

    defp budget_yield_counts(snapshot) do
      counts =
        Map.new([:max_moments, :max_elapsed_ms, :both], fn budget ->
          {budget,
           Docket.Benchmark.Collector.observation_count(snapshot, @drain_event, %{
             budget: budget
           })}
        end)

      Map.put(counts, :total, Enum.sum(Map.values(counts)))
    end

    defp drain_fairness_invariants(nil, _labels, _kind), do: []

    defp drain_fairness_invariants(drain_fairness, labels, :cyclic_vs_one_step) do
      cyclic_runs = Enum.count(labels, &(&1 == :cyclic))
      fast_runs = Enum.count(labels, &(&1 == :one_step))
      yields = drain_fairness.budget_yields.total
      reacquisitions = drain_fairness.claims.reacquisitions_after_yield
      completed_fast = drain_fairness.fast_one_step_tail_impact.completed_runs

      checkpoint_commits =
        drain_fairness.observed_commits.exact_checkpoint_commits_across_all_cohorts

      lifecycle_commits =
        drain_fairness.observed_commits.exact_lifecycle_commits_across_all_cohorts

      [
        %{
          name: "aggregate budget-yield count is at least the cyclic cohort size",
          pass: yields >= cyclic_runs,
          expected: %{at_least: cyclic_runs},
          actual: yields
        },
        %{
          name: "aggregate claim reacquisitions equal aggregate budget yields",
          pass: reacquisitions == yields,
          expected: yields,
          actual: reacquisitions
        },
        %{
          name: "exact aggregate lifecycle commits equal exact aggregate checkpoint commits",
          pass: lifecycle_commits == checkpoint_commits,
          expected: checkpoint_commits,
          actual: lifecycle_commits
        },
        %{
          name: "fast one-step cohort contributes complete tail samples",
          pass:
            completed_fast == fast_runs and
              is_number(drain_fairness.fast_one_step_tail_impact.activation_to_terminal_p95_us),
          expected: fast_runs,
          actual: completed_fast
        }
      ]
    end

    defp runtime_drain_budget(config, :cyclic_vs_one_step) do
      config
      |> cyclic_drain_budget()
      |> Map.new()
      |> Map.put_new(:max_elapsed_ms, "infinity")
    end

    defp runtime_drain_budget(_config, _kind),
      do: %{max_moments: 100, max_elapsed_ms: 1_000}

    defp interleave(a, a_count, b, b_count) do
      pairs = min(a_count, b_count)

      List.duplicate([a, b], pairs)
      |> List.flatten()
      |> Kernel.++(List.duplicate(a, a_count - pairs))
      |> Kernel.++(List.duplicate(b, b_count - pairs))
    end

    @doc false
    def comparative_measurements(events, t0, labels, kind) do
      terminal_at =
        events
        |> event_records([:docket, :checkpoint, :committed])
        |> Enum.filter(fn {_m, metadata, _at} ->
          metadata.checkpoint_type == "run_completed" and is_integer(metadata.correlation_id)
        end)
        |> Map.new(fn {_m, metadata, at} -> {metadata.correlation_id, at} end)

      claims_by_id =
        events
        |> event_records([:docket, :postgres, :claim, :attempt])
        |> Enum.filter(fn {_m, metadata, _at} -> is_integer(metadata[:correlation_id]) end)
        |> Enum.group_by(fn {_m, metadata, _at} -> metadata.correlation_id end)
        |> Map.new(fn {correlation_id, records} ->
          {correlation_id, Enum.sort_by(records, fn {_m, _metadata, at} -> at end)}
        end)

      first_claim_at =
        Map.new(claims_by_id, fn {correlation_id, [{_m, _metadata, at} | _records]} ->
          {correlation_id, at}
        end)

      sampling = sampling_context(events, labels, claims_by_id, terminal_at)

      terminal_rank =
        terminal_at
        |> Enum.sort_by(fn {correlation_id, at} -> {at, correlation_id} end)
        |> Enum.with_index(1)
        |> Map.new(fn {{correlation_id, _at}, rank} -> {correlation_id, rank} end)

      cohorts =
        labels
        |> Enum.uniq()
        |> Map.new(fn label ->
          correlation_ids =
            for {cohort, index} <- Enum.with_index(labels, 1), cohort == label, do: index

          retained_correlation_ids =
            Enum.filter(correlation_ids, fn id ->
              Map.has_key?(claims_by_id, id) or Map.has_key?(terminal_at, id)
            end)

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

          normalized_slowdowns =
            for id <- correlation_ids,
                claim = first_claim_at[id],
                terminal = terminal_at[id],
                is_integer(claim) and is_integer(terminal),
                service_time = terminal - claim,
                flow_time = terminal - t0,
                service_time > 0 and flow_time >= 0,
                do: Float.round(flow_time / service_time, 6)

          ranks =
            for id <- correlation_ids, rank = terminal_rank[id], is_integer(rank), do: rank

          rank_deltas =
            if sampling.complete_population do
              for id <- correlation_ids,
                  rank = terminal_rank[id],
                  is_integer(rank),
                  do: rank - id
            else
              []
            end

          claim_records =
            Enum.flat_map(retained_correlation_ids, fn id -> Map.get(claims_by_id, id, []) end)

          claims_per_run =
            Enum.map(retained_correlation_ids, fn id -> length(Map.get(claims_by_id, id, [])) end)

          subsequent_claim_records =
            Enum.flat_map(retained_correlation_ids, fn id ->
              case Map.get(claims_by_id, id, []) do
                [] ->
                  []

                [_first | subsequent] ->
                  first_at = first_claim_at[id]

                  Enum.map(subsequent, fn {measurements, metadata, at} ->
                    {id, measurements, metadata, at, first_at}
                  end)
              end
            end)

          subsequent_claim_offsets =
            Enum.map(subsequent_claim_records, fn {_id, _m, _metadata, at, _first_at} ->
              at - t0
            end)

          first_to_subsequent_claim =
            Enum.map(subsequent_claim_records, fn {_id, _m, _metadata, at, first_at} ->
              at - first_at
            end)

          subsequent_ready_age_ms =
            Enum.flat_map(subsequent_claim_records, fn {_id, measurements, _metadata, _at,
                                                        _first_at} ->
              case measurements[:eligible_age_ms] do
                value when is_number(value) and value >= 0 -> [value]
                _other -> []
              end
            end)

          terminal_distribution = Docket.Benchmark.Stats.native_distribution(terminal_offsets)

          first_claim_distribution =
            Docket.Benchmark.Stats.native_distribution(first_claim_offsets)

          cohort = %{
            runs: length(correlation_ids),
            completed_runs: length(terminal_offsets),
            sampling: %{
              population_runs: length(correlation_ids),
              retained_correlation_samples: length(retained_correlation_ids),
              retained_first_claim_samples: length(first_claim_offsets),
              retained_terminal_samples: length(terminal_offsets),
              complete_population:
                length(retained_correlation_ids) == length(correlation_ids) and
                  sampling.complete_population
            },
            activation_to_terminal_commit_offset_us: terminal_distribution,
            activation_to_first_claim_offset_us: first_claim_distribution,
            first_claim_to_terminal_commit_us:
              Docket.Benchmark.Stats.native_distribution(service_times),
            normalized_slowdown:
              Docket.Benchmark.Stats.distribution(normalized_slowdowns, & &1, "ratio"),
            terminal_rank_in_retained_sample:
              Docket.Benchmark.Stats.distribution(ranks, & &1, "rank"),
            claims: %{
              retained_observations: length(claim_records),
              claims_per_run: Docket.Benchmark.Stats.distribution(claims_per_run, & &1, "claims"),
              retained_subsequent_observations: length(subsequent_claim_records),
              retained_runs_with_subsequent_claims: Enum.count(claims_per_run, &(&1 > 1)),
              activation_to_subsequent_claim_offset_us:
                Docket.Benchmark.Stats.native_distribution(subsequent_claim_offsets),
              first_to_subsequent_claim_us:
                Docket.Benchmark.Stats.native_distribution(first_to_subsequent_claim),
              subsequent_ready_age_at_scan_start_ms:
                Docket.Benchmark.Stats.millisecond_distribution(subsequent_ready_age_ms)
            },
            queue_share_of_median_percent:
              queue_share(first_claim_distribution, terminal_distribution)
          }

          cohort =
            if sampling.complete_population do
              Map.merge(cohort, %{
                terminal_rank_minus_staged_ordinal:
                  Docket.Benchmark.Stats.distribution(rank_deltas, & &1, "rank"),
                terminal_order: %{
                  finished_ahead_of_staged_ordinal: Enum.count(rank_deltas, &(&1 < 0)),
                  finished_at_staged_ordinal: Enum.count(rank_deltas, &(&1 == 0)),
                  finished_behind_staged_ordinal: Enum.count(rank_deltas, &(&1 > 0))
                }
              })
            else
              cohort
            end

          {label, cohort}
        end)

      %{
        cohorts: cohorts,
        fairness:
          %{
            normalized_slowdown_basis:
              "per-run activation-to-terminal offset divided by first-claim-to-terminal duration",
            terminal_rank_basis: terminal_rank_basis(sampling.complete_population),
            subsequent_claim_basis:
              "retained claim observations after a run's first correlated claim; ready age is measured at claim-scan start",
            sampling: sampling
          }
          |> Map.merge(normalized_slowdown_comparison(cohorts, kind))
      }
    end

    defp sampling_context(events, labels, claims_by_id, terminal_at) do
      retained_ids =
        claims_by_id
        |> Map.keys()
        |> Kernel.++(Map.keys(terminal_at))
        |> MapSet.new()

      {collector_selected, selection} =
        case events do
          %Docket.Benchmark.Collector.Snapshot{correlations: correlations} ->
            {correlations.sampled, "deterministic evenly spaced staged ordinals"}

          events when is_list(events) ->
            {MapSet.size(retained_ids), "provided correlated event set"}
        end

      retained = MapSet.size(retained_ids)
      population = length(labels)

      %{
        population_runs: population,
        collector_selected_correlations: collector_selected,
        retained_correlation_samples: retained,
        complete_population: collector_selected == population and retained == population,
        selection: selection
      }
    end

    defp terminal_rank_basis(true) do
      "1 is the earliest terminal commit; every correlation was retained, ties break by staged ordinal, and negative rank-minus-staged-ordinal values finished ahead of staged order"
    end

    defp terminal_rank_basis(false) do
      "rank is relative only to retained deterministic correlation samples; global terminal rank and staged-order deltas are omitted"
    end

    defp normalized_slowdown_comparison(cohorts, :mixed_service_times) do
      %{
        fast_to_slow_normalized_slowdown_p50_ratio:
          distribution_ratio(cohorts, :fast, :slow, :p50),
        fast_to_slow_normalized_slowdown_p95_ratio:
          distribution_ratio(cohorts, :fast, :slow, :p95),
        fast_to_slow_normalized_slowdown_interpretation:
          "values above 1 mean fast runs experienced more queue-inclusive slowdown relative to their own post-first-claim duration"
      }
    end

    defp normalized_slowdown_comparison(_cohorts, _kind), do: %{}

    defp distribution_ratio(cohorts, numerator, denominator, statistic) do
      numerator_value = get_in(cohorts, [numerator, :normalized_slowdown, statistic])
      denominator_value = get_in(cohorts, [denominator, :normalized_slowdown, statistic])

      if is_number(numerator_value) and is_number(denominator_value) and denominator_value > 0,
        do: Float.round(numerator_value / denominator_value, 3),
        else: nil
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

    defp cycle_graph(cycle_moments) do
      alias Docket.Guard

      target = cycle_moments * 1.0

      Docket.Graph.new!(id: "docket-bench-cycle-#{cycle_moments}")
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
        guard: Guard.not(Guard.equals(Guard.path("count", []), target))
      )
      |> Docket.Graph.put_edge!("finish",
        from: "decide",
        to: "$finish",
        guard: Guard.equals(Guard.path("count", []), target)
      )
      |> Docket.Graph.policy!("max_supersteps", terminal_max_supersteps(cycle_moments))
    end

    defp terminal_max_supersteps(cycle_moments), do: cycle_moments * 4 + 10
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
