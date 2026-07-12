defmodule Docket.Benchmark.Collector do
  @moduledoc false

  defmodule Snapshot do
    @moduledoc false
    @enforce_keys [:events, :exact, :correlations]
    defstruct [:events, :exact, :correlations, :activation_at, :measurement_end_at]
  end

  @default_samples_per_event 4_096
  @modes [:bounded_instrumented, :counters_only_control]

  @events [
    [:docket, :lifecycle, :transaction, :stop],
    [:docket, :lifecycle, :committed],
    [:docket, :checkpoint, :committed],
    [:docket, :run, :completed],
    [:docket, :postgres, :run_store, :claim],
    [:docket, :postgres, :run_store, :claim_query],
    [:docket, :postgres, :claim, :attempt],
    [:docket, :postgres, :dispatcher, :poll],
    [:docket, :postgres, :dispatcher, :state],
    [:docket, :postgres, :dispatcher, :launch],
    [:docket, :postgres, :vehicle, :stop],
    [:docket, :postgres, :vehicle, :drain],
    [:docket, :postgres, :graph_cache, :fetch],
    [:docket, :postgres, :graph, :fetch, :stop],
    [:docket, :postgres, :graph, :compile, :stop],
    [:docket, :postgres, :store],
    [:docket, :node, :execution],
    [:docket, :benchmark, :repo, :query]
  ]

  @checkpoint [:docket, :checkpoint, :committed]
  @completion [:docket, :run, :completed]
  @claim_attempt [:docket, :postgres, :claim, :attempt]
  @correlated_events [@checkpoint, @completion, @claim_attempt]

  @numeric_aggregates %{
    [:docket, :postgres, :run_store, :claim] => [:leases, :steals, :poisoned],
    [:docket, :postgres, :dispatcher, :poll] => [:leases, :poisoned],
    [:docket, :postgres, :dispatcher, :state] => [:in_flight],
    [:docket, :lifecycle, :committed] => [:count],
    [:docket, :postgres, :store] => [:attempted_rows, :encoded_bytes],
    [:docket, :postgres, :vehicle, :drain] => [:claim_held_ms]
  }

  def start(correlation_ids \\ [], opts \\ []) do
    mode = Keyword.get(opts, :mode, :bounded_instrumented)

    if mode not in @modes do
      raise ArgumentError,
            "collector mode must be one of #{inspect(@modes)}, got: #{inspect(mode)}"
    end

    max_samples =
      if mode == :bounded_instrumented,
        do: Keyword.get(opts, :max_samples_per_event, @default_samples_per_event),
        else: 0

    if mode == :bounded_instrumented and not (is_integer(max_samples) and max_samples > 0) do
      raise ArgumentError, "max_samples_per_event must be a positive integer"
    end

    table = :ets.new(__MODULE__, [:set, :public, write_concurrency: true])
    counters = :ets.new(__MODULE__.Counters, [:set, :public, write_concurrency: true])
    handler_id = {__MODULE__, make_ref()}

    total_correlations = length(correlation_ids)

    sampled_ordinals =
      if mode == :bounded_instrumented,
        do: sampled_ordinals(total_correlations, max_samples),
        else: MapSet.new()

    # Keep only the correlations used for retained distributions and sampled
    # per-run shape checks. Exact population totals remain streaming counters;
    # indexing every run here would make collector memory grow with backlog.
    Enum.reduce(correlation_ids, 1, fn run_id, ordinal ->
      if MapSet.member?(sampled_ordinals, ordinal) do
        true = :ets.insert(counters, {{:correlation, run_id}, ordinal})
        true = :ets.insert(counters, {{:sampled_correlation, ordinal}, true})
      end

      ordinal + 1
    end)

    true = :ets.insert(counters, {{:collector, :total_correlations}, total_correlations})

    true =
      :ets.insert(
        counters,
        {{:collector, :fully_indexed_correlations},
         total_correlations > 0 and total_correlations == MapSet.size(sampled_ordinals)}
      )

    :ok =
      :telemetry.attach_many(
        handler_id,
        @events,
        &__MODULE__.handle/4,
        %{
          table: table,
          counters: counters,
          correlate?: correlation_ids != [],
          activation_at: Keyword.get(opts, :activation_at),
          measurement_end_at: Keyword.get(opts, :measurement_end_at),
          max_samples: max_samples,
          mode: mode
        }
      )

    %{
      table: table,
      counters: counters,
      handler_id: handler_id,
      max_samples: max_samples,
      total_correlations: total_correlations,
      activation_at: Keyword.get(opts, :activation_at),
      measurement_end_at: Keyword.get(opts, :measurement_end_at),
      mode: mode
    }
  end

  def stop(
        %{
          table: table,
          counters: counters,
          handler_id: handler_id,
          total_correlations: total_correlations
        } = collector
      ) do
    :telemetry.detach(handler_id)

    events = raw_sampled_events(table) ++ correlated_samples(counters)
    exact = exact_summary(counters)
    correlations = correlation_summary(counters, total_correlations)

    :ets.delete(table)
    :ets.delete(counters)

    %Snapshot{
      events: events,
      exact: exact,
      correlations: correlations,
      activation_at: Map.get(collector, :activation_at),
      measurement_end_at: Map.get(collector, :measurement_end_at)
    }
  end

  def count(collector, event, metadata \\ %{})

  def count(%{counters: counters}, event, metadata) when map_size(metadata) == 0 do
    case :ets.lookup(counters, event) do
      [{^event, count}] -> count
      [] -> 0
    end
  end

  def count(
        %{counters: counters},
        [:docket, :checkpoint, :committed] = event,
        %{checkpoint_type: checkpoint_type}
      ) do
    counter(counters, {event, {:correlated_checkpoint_type, checkpoint_type}})
  end

  def count(%{counters: counters}, event, metadata) when map_size(metadata) == 1 do
    [{key, value}] = Map.to_list(metadata)

    Enum.reduce([:measured, :post_measurement], 0, fn phase, total ->
      total + counter(counters, {:metadata_count, phase, event, key, value})
    end)
  end

  def handle(
        event,
        measurements,
        metadata,
        %{
          table: table,
          counters: counters,
          correlate?: correlate?
        } = config
      ) do
    observed_at = System.monotonic_time()
    safe_metadata = safe_metadata(event, metadata, counters)
    phase = observation_phase(observed_at, config.activation_at, config.measurement_end_at)
    observation = increment(counters, {:observations_total, event})

    increment(counters, {:observations, phase, event})
    count_metadata(counters, phase, event, safe_metadata)
    aggregate_measurements(counters, phase, event, measurements, config.mode)
    update_number(counters, {:observed_at_max, phase, event}, observed_at, &max/2)
    aggregate_metadata_observed_at(counters, phase, event, safe_metadata, observed_at)

    if phase != :pre_activation do
      count_observation(counters, phase, event, safe_metadata, correlate?)
      track_correlation(counters, event, measurements, safe_metadata, observed_at)
    end

    if config.mode == :bounded_instrumented and retain_raw?(event, safe_metadata) do
      retain_sample(
        table,
        event,
        observation,
        config.max_samples,
        measurements,
        safe_metadata,
        observed_at
      )
    end

    :ok
  end

  def events, do: @events

  def stats(%{
        table: table,
        counters: counters,
        max_samples: max_samples,
        total_correlations: total_correlations,
        measurement_end_at: measurement_end_at,
        mode: mode
      }) do
    observed = total_observations(counters)
    retained = :ets.info(table, :size) + length(correlated_samples(counters))
    indexed = sampled_correlation_count(counters)
    fully_indexed? = counter(counters, {:collector, :fully_indexed_correlations}) == true
    uniqueness_available? = fully_indexed? and total_correlations > 0

    %{
      capture_mode:
        if(mode == :bounded_instrumented,
          do: "bounded_streaming_reservoir",
          else: "counters_only_control"
        ),
      captured_events: observed,
      observed_events: observed,
      retained_event_samples: retained,
      aggregated_events: max(observed - retained, 0),
      exact_counters: true,
      distribution_sketch:
        if(mode == :bounded_instrumented,
          do: "deterministic bounded reservoir",
          else: "none"
        ),
      max_samples_per_event: max_samples,
      correlation_index_limit: max_samples,
      sampled_correlations: indexed,
      indexed_correlations: indexed,
      correlation_population: total_correlations,
      peak_correlation_cardinality: indexed,
      per_run_correlation_state_bounded: true,
      full_population_shape_coverage: fully_indexed?,
      full_population_uniqueness_available: uniqueness_available?,
      uniqueness_scope:
        cond do
          uniqueness_available? -> "exact_full_population"
          total_correlations == 0 -> "correlation_population_not_configured"
          true -> "bounded_correlation_sample"
        end,
      correlation_correctness_scope:
        cond do
          total_correlations == 0 -> "correlation_population_not_configured"
          fully_indexed? -> "exact_full_population"
          indexed == 0 -> "exact_global_counts_without_per_run_shape_proof"
          true -> "exact_global_counts_with_bounded_per_run_sample"
        end,
      phase_scoped_exact_aggregates: true,
      histogram_scope:
        if(mode == :bounded_instrumented, do: "retained_bounded_event_sample", else: "none"),
      default_exact_aggregate_scope: "activation_through_collector_stop",
      measurement_end_boundary:
        if(is_integer(measurement_end_at), do: "configured", else: "not_configured"),
      observer_effect:
        if(mode == :bounded_instrumented,
          do: "not_quantified unless this trial is part of an opt-in paired AB/BA suite",
          else:
            "counters-only control still attaches telemetry and maintains exact aggregate counts; per-run telemetry shape is not retained"
        )
    }
  end

  def sampled_events(%Snapshot{events: events}), do: events

  def observation_count(snapshot, event, metadata \\ %{})

  def observation_count(%Snapshot{} = snapshot, event, metadata),
    do:
      active_phase_sum(snapshot, fn phase ->
        phase_observation_count(snapshot, phase, event, metadata)
      end)

  def phase_observation_count(snapshot, phase, event, metadata \\ %{})

  def phase_observation_count(%Snapshot{exact: exact}, phase, event, metadata)
      when map_size(metadata) == 0,
      do: Map.get(exact, {:observations, phase, event}, 0)

  def phase_observation_count(%Snapshot{exact: exact}, phase, event, metadata)
      when map_size(metadata) == 1 do
    [{key, value}] = Map.to_list(metadata)
    Map.get(exact, {:metadata_count, phase, event, key, value}, 0)
  end

  def phase_observation_count(_snapshot, _phase, _event, _metadata), do: 0

  def unique_count(%Snapshot{exact: exact} = snapshot, @completion = event) do
    if full_population_uniqueness_available?(exact) do
      active_phase_sum(snapshot, fn phase -> Map.get(exact, {:unique_count, phase, event}, 0) end)
    else
      :unavailable
    end
  end

  def unique_count(%Snapshot{}, _event), do: :unsupported

  def full_population_unique_count(%Snapshot{} = snapshot, event) do
    case unique_count(snapshot, event) do
      count when is_integer(count) -> {:ok, count}
      :unavailable -> {:unavailable, uniqueness_scope(snapshot)}
      :unsupported -> {:unsupported, event}
    end
  end

  def uniqueness_scope(%Snapshot{exact: exact}) do
    cond do
      full_population_uniqueness_available?(exact) ->
        :exact_full_population

      Map.get(exact, {:collector, :total_correlations}, 0) == 0 ->
        :correlation_population_not_configured

      true ->
        :bounded_correlation_sample
    end
  end

  def uniqueness_scope(%Snapshot{} = snapshot, @completion), do: uniqueness_scope(snapshot)
  def uniqueness_scope(%Snapshot{}, _event), do: :unsupported

  defp full_population_uniqueness_available?(exact) do
    Map.get(exact, {:collector, :fully_indexed_correlations}, false) and
      Map.get(exact, {:collector, :total_correlations}, 0) > 0
  end

  def numeric_sum(%Snapshot{} = snapshot, event, key),
    do: active_phase_sum(snapshot, &phase_numeric_sum(snapshot, &1, event, key))

  def phase_numeric_sum(%Snapshot{exact: exact}, phase, event, key),
    do: Map.get(exact, {:numeric_sum, phase, event, key}, 0)

  def numeric_max(%Snapshot{} = snapshot, event, key),
    do: active_phase_max(snapshot, &phase_numeric_max(snapshot, &1, event, key))

  def phase_numeric_max(%Snapshot{exact: exact}, phase, event, key),
    do: Map.get(exact, {:numeric_max, phase, event, key})

  def observed_at_max(snapshot, event, metadata \\ %{})

  def observed_at_max(%Snapshot{} = snapshot, event, metadata),
    do: active_phase_max(snapshot, &phase_observed_at_max(snapshot, &1, event, metadata))

  def phase_observed_at_max(snapshot, phase, event, metadata \\ %{})

  def phase_observed_at_max(%Snapshot{exact: exact}, phase, event, metadata)
      when map_size(metadata) == 0,
      do: Map.get(exact, {:observed_at_max, phase, event})

  def phase_observed_at_max(%Snapshot{exact: exact}, phase, event, metadata)
      when map_size(metadata) == 1 do
    [{key, value}] = Map.to_list(metadata)
    Map.get(exact, {:observed_at_max_metadata, phase, event, key, value})
  end

  def phase_observed_at_max(_snapshot, _phase, _event, _metadata), do: nil

  def phase_count(%Snapshot{exact: exact}, phase, event),
    do: Map.get(exact, {:observations, phase, event}, 0)

  def histogram(%Snapshot{} = snapshot, event, key) do
    active_phase_maps(snapshot, &phase_histogram(snapshot, &1, event, key))
  end

  def phase_histogram(%Snapshot{} = snapshot, phase, event, key) do
    snapshot.events
    |> Enum.flat_map(fn
      {^event, measurements, _metadata, observed_at} ->
        if snapshot_phase(snapshot, observed_at) == phase and is_number(measurements[key]),
          do: [measurements[key]],
          else: []

      _other ->
        []
    end)
    |> Enum.frequencies()
  end

  def negative_count(%Snapshot{} = snapshot, event, key),
    do: active_phase_sum(snapshot, &phase_negative_count(snapshot, &1, event, key))

  def phase_negative_count(%Snapshot{exact: exact}, phase, event, key),
    do: Map.get(exact, {:negative, phase, event, key}, 0)

  def predicate_count(%Snapshot{} = snapshot, event, predicate),
    do: active_phase_sum(snapshot, &phase_predicate_count(snapshot, &1, event, predicate))

  def phase_predicate_count(%Snapshot{exact: exact}, phase, event, predicate),
    do: Map.get(exact, {:predicate, phase, event, predicate}, 0)

  def correlation_summary(%Snapshot{correlations: correlations}), do: correlations

  defp active_phase_sum(_snapshot, fun) do
    Enum.reduce([:measured, :post_measurement], 0, fn phase, total -> total + fun.(phase) end)
  end

  defp active_phase_max(_snapshot, fun) do
    [:measured, :post_measurement]
    |> Enum.flat_map(fn phase ->
      case fun.(phase) do
        value when is_number(value) -> [value]
        _other -> []
      end
    end)
    |> Enum.max(fn -> nil end)
  end

  defp active_phase_maps(_snapshot, fun) do
    Enum.reduce([:measured, :post_measurement], %{}, fn phase, total ->
      Map.merge(total, fun.(phase), fn _key, left, right -> left + right end)
    end)
  end

  defp safe_metadata(event, metadata, counters)
       when event in [[:docket, :checkpoint, :committed], [:docket, :run, :completed]] do
    %{
      correlation_id: correlation_id(counters, metadata[:run_id]),
      checkpoint_type: checkpoint_type(metadata)
    }
  end

  defp safe_metadata([:docket, :benchmark, :repo, :query], metadata, _counters) do
    classification = Keyword.get(metadata[:options] || [], :benchmark_query, :workload)

    %{
      benchmark_query:
        if(classification in [:control, :probe], do: classification, else: :workload)
    }
  end

  defp safe_metadata([:docket, :postgres, :claim, :attempt] = event, metadata, counters) do
    event
    |> Docket.Telemetry.metric_metadata(metadata)
    |> Map.put(:correlation_id, correlation_id(counters, metadata[:run_id]))
  end

  defp safe_metadata([:docket, :postgres, :run_store, :claim_query], _metadata, _counters),
    do: %{}

  defp safe_metadata(event, metadata, _counters),
    do: Docket.Telemetry.metric_metadata(event, metadata)

  defp correlation_id(counters, run_id) do
    case :ets.lookup(counters, {:correlation, run_id}) do
      [{{:correlation, ^run_id}, ordinal}] ->
        ordinal

      [] ->
        cond do
          is_nil(run_id) -> nil
          counter(counters, {:collector, :total_correlations}) == 0 -> nil
          counter(counters, {:collector, :fully_indexed_correlations}) == true -> nil
          true -> :unsampled
        end
    end
  end

  defp checkpoint_type(%{event: %{metadata: %{"checkpoint_type" => type}}}), do: type
  defp checkpoint_type(_metadata), do: nil

  defp count_observation(counters, phase, event, metadata, correlate?)
       when event in [[:docket, :checkpoint, :committed], [:docket, :run, :completed]] and
              correlate? do
    case metadata.correlation_id do
      nil ->
        increment(counters, {:unknown_correlation, event})

      correlation_id ->
        increment(counters, event)

        if event == [:docket, :checkpoint, :committed] do
          increment(counters, {event, {:correlated_checkpoint_type, metadata.checkpoint_type}})
        end

        if is_integer(correlation_id) do
          marker = {:correlation_observed, event, correlation_id, metadata.checkpoint_type}

          if :ets.insert_new(counters, {marker, true}) do
            increment(counters, {:unique_count, phase, event})
          end
        end
    end
  end

  defp count_observation(counters, _phase, event, _metadata, _correlate?),
    do: increment(counters, event)

  defp increment(counters, key),
    do: :ets.update_counter(counters, key, {2, 1}, {key, 0})

  defp counter(table, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] -> value
      [] -> 0
    end
  end

  defp sampled_ordinals(0, _limit), do: MapSet.new()

  defp sampled_ordinals(total, limit) when total <= limit,
    do: MapSet.new(1..total)

  defp sampled_ordinals(total, limit) do
    0..(limit - 1)
    |> Enum.map(fn index -> div(index * total, limit) + 1 end)
    |> MapSet.new()
  end

  defp retain_raw?(event, %{correlation_id: correlation_id})
       when event in @correlated_events and not is_nil(correlation_id),
       do: false

  defp retain_raw?(_event, _metadata), do: true

  defp retain_sample(table, event, observation, limit, measurements, metadata, observed_at) do
    slot =
      cond do
        observation <= limit -> observation
        true -> :erlang.phash2({event, observation}, observation) + 1
      end

    if slot <= limit do
      true = :ets.insert(table, {{event, slot}, event, measurements, metadata, observed_at})
    end
  end

  defp raw_sampled_events(table) do
    Enum.map(:ets.tab2list(table), fn {_key, event, measurements, metadata, observed_at} ->
      {event, measurements, metadata, observed_at}
    end)
  end

  defp count_metadata(counters, phase, event, metadata) do
    metadata
    |> Map.drop([:correlation_id])
    |> Enum.each(fn {key, value} ->
      increment(counters, {:metadata_count, phase, event, key, value})
    end)
  end

  defp aggregate_measurements(counters, phase, event, measurements, _mode) do
    Enum.each(Map.get(@numeric_aggregates, event, []), fn key ->
      case measurements[key] do
        value when is_number(value) ->
          update_number(counters, {:numeric_sum, phase, event, key}, value, &Kernel.+/2)
          update_number(counters, {:numeric_max, phase, event, key}, value, &max/2)

        _other ->
          :ok
      end
    end)

    if event == [:docket, :postgres, :run_store, :claim] and measurements[:leases] == 0 do
      increment(counters, {:predicate, phase, event, :leases_zero})
    end

    if event == [:docket, :postgres, :dispatcher, :poll] and
         measurements[:leases] == 0 and measurements[:poisoned] == 0 do
      increment(counters, {:predicate, phase, event, :empty})
    end

    if event == @claim_attempt and is_number(measurements[:eligible_age_ms]) and
         measurements.eligible_age_ms < 0 do
      increment(counters, {:negative, phase, event, :eligible_age_ms})
    end
  end

  defp aggregate_metadata_observed_at(
         counters,
         phase,
         @checkpoint,
         %{checkpoint_type: "run_completed"},
         observed_at
       ) do
    update_number(
      counters,
      {:observed_at_max_metadata, phase, @checkpoint, :checkpoint_type, "run_completed"},
      observed_at,
      &max/2
    )
  end

  defp aggregate_metadata_observed_at(_counters, _phase, _event, _metadata, _observed_at),
    do: :ok

  defp update_number(table, key, value, fun) do
    case :ets.lookup(table, key) do
      [] ->
        if not :ets.insert_new(table, {key, value}), do: update_number(table, key, value, fun)

      [{^key, current}] ->
        updated = fun.(current, value)

        if updated != current and
             :ets.select_replace(table, [{{key, current}, [], [{:const, {key, updated}}]}]) == 0 do
          update_number(table, key, value, fun)
        end
    end
  end

  defp observation_phase(_observed_at, nil, _measurement_end_at), do: :measured

  defp observation_phase(observed_at, activation_at, _measurement_end_at)
       when observed_at < activation_at,
       do: :pre_activation

  defp observation_phase(observed_at, _activation_at, measurement_end_at)
       when is_integer(measurement_end_at) and observed_at >= measurement_end_at,
       do: :post_measurement

  defp observation_phase(_observed_at, _activation_at, _measurement_end_at), do: :measured

  defp snapshot_phase(%Snapshot{} = snapshot, observed_at),
    do: observation_phase(observed_at, snapshot.activation_at, snapshot.measurement_end_at)

  defp track_correlation(_counters, _event, _measurements, %{correlation_id: nil}, _at),
    do: :ok

  defp track_correlation(counters, event, measurements, metadata, observed_at)
       when event in @correlated_events do
    ordinal = metadata.correlation_id
    increment(counters, {:correlation_event_count, ordinal, event})

    if event == @checkpoint do
      increment(
        counters,
        {:correlation_checkpoint_type_count, ordinal, metadata.checkpoint_type}
      )
    end

    if counter(counters, {:sampled_correlation, ordinal}) == true do
      record = {measurements, metadata, observed_at}
      put_correlation_record(counters, ordinal, event, record)
    end
  end

  defp track_correlation(_counters, _event, _measurements, _metadata, _observed_at), do: :ok

  defp put_correlation_record(counters, ordinal, @checkpoint, record) do
    :ets.insert_new(counters, {{:correlation_record, ordinal, :first_checkpoint}, record})

    {_measurements, metadata, _at} = record

    if metadata.checkpoint_type == "run_completed" do
      true = :ets.insert(counters, {{:correlation_record, ordinal, :terminal_checkpoint}, record})
    end
  end

  defp put_correlation_record(counters, ordinal, @completion, record) do
    true = :ets.insert(counters, {{:correlation_record, ordinal, :completion}, record})
  end

  defp put_correlation_record(counters, ordinal, @claim_attempt, record) do
    :ets.insert_new(counters, {{:correlation_record, ordinal, :first_claim}, record})
    true = :ets.insert(counters, {{:correlation_record, ordinal, :last_claim}, record})
  end

  defp correlated_samples(counters) do
    counters
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{:sampled_correlation, ordinal}, true} -> correlation_records(counters, ordinal)
      _other -> []
    end)
  end

  defp correlation_records(counters, ordinal) do
    [
      {@checkpoint, lookup_record(counters, ordinal, :first_checkpoint)},
      {@checkpoint, lookup_record(counters, ordinal, :terminal_checkpoint)},
      {@completion, lookup_record(counters, ordinal, :completion)},
      {@claim_attempt, lookup_record(counters, ordinal, :first_claim)},
      {@claim_attempt, lookup_record(counters, ordinal, :last_claim)}
    ]
    |> Enum.flat_map(fn
      {_event, nil} ->
        []

      {event, {measurements, metadata, observed_at}} ->
        [{event, measurements, metadata, observed_at}]
    end)
    |> Enum.uniq()
  end

  defp lookup_record(counters, ordinal, name) do
    case :ets.lookup(counters, {:correlation_record, ordinal, name}) do
      [{{:correlation_record, ^ordinal, ^name}, record}] -> record
      [] -> nil
    end
  end

  defp exact_summary(counters) do
    :ets.foldl(
      fn
        {{:observations, _phase, _event} = key, value}, acc ->
          Map.put(acc, key, value)

        {{:metadata_count, _phase, _event, _key, _value} = key, value}, acc ->
          Map.put(acc, key, value)

        {{:numeric_sum, _phase, _event, _key} = key, value}, acc ->
          Map.put(acc, key, value)

        {{:numeric_max, _phase, _event, _key} = key, value}, acc ->
          Map.put(acc, key, value)

        {{:observed_at_max, _phase, _event} = key, value}, acc ->
          Map.put(acc, key, value)

        {{:observed_at_max_metadata, _phase, _event, _key, _value} = key, observed_at}, acc ->
          Map.put(acc, key, observed_at)

        {{:negative, _phase, _event, _key} = key, value}, acc ->
          Map.put(acc, key, value)

        {{:predicate, _phase, _event, _predicate} = key, value}, acc ->
          Map.put(acc, key, value)

        {{:unique_count, _phase, _event} = key, value}, acc ->
          Map.put(acc, key, value)

        {{:collector, :fully_indexed_correlations} = key, value}, acc ->
          Map.put(acc, key, value)

        {{:collector, :total_correlations} = key, value}, acc ->
          Map.put(acc, key, value)

        _other, acc ->
          acc
      end,
      %{},
      counters
    )
  end

  defp correlation_summary(counters, total) do
    ordinals = sampled_correlation_ordinals(counters)
    sampled = length(ordinals)
    fully_indexed? = counter(counters, {:collector, :fully_indexed_correlations}) == true

    %{
      expected: total,
      population_expected: total,
      sampled_expected: sampled,
      sampled: sampled,
      per_run_shape_scope:
        cond do
          total == 0 -> "correlation_population_not_configured"
          fully_indexed? -> "exact_full_population"
          true -> "bounded_correlation_sample"
        end,
      full_population_shape_coverage: fully_indexed?,
      full_population_uniqueness_available: fully_indexed? and total > 0,
      checkpoint_count_frequencies:
        correlation_count_frequencies(counters, ordinals, @checkpoint),
      terminal_checkpoint_count_frequencies:
        correlation_checkpoint_type_frequencies(counters, ordinals, "run_completed"),
      completion_count_frequencies:
        correlation_count_frequencies(counters, ordinals, @completion),
      claim_count_frequencies: correlation_count_frequencies(counters, ordinals, @claim_attempt),
      unknown_events:
        Map.new(@correlated_events, fn event ->
          {event, counter(counters, {:unknown_correlation, event})}
        end),
      unknown_correlation_scope:
        cond do
          total == 0 -> "correlation_population_not_configured"
          fully_indexed? -> "exact_full_population"
          true -> "nil_ids_only; unsampled run ids are not membership-checked"
        end
    }
  end

  defp correlation_count_frequencies(counters, ordinals, event) do
    Enum.frequencies_by(ordinals, fn ordinal ->
      counter(counters, {:correlation_event_count, ordinal, event})
    end)
  end

  defp correlation_checkpoint_type_frequencies(counters, ordinals, type) do
    Enum.frequencies_by(ordinals, fn ordinal ->
      counter(counters, {:correlation_checkpoint_type_count, ordinal, type})
    end)
  end

  defp sampled_correlation_count(counters) do
    :ets.select_count(counters, [{{{:sampled_correlation, :_}, true}, [], [true]}])
  end

  defp sampled_correlation_ordinals(counters) do
    :ets.select(counters, [{{{:sampled_correlation, :"$1"}, true}, [], [:"$1"]}])
    |> Enum.sort()
  end

  defp total_observations(counters) do
    :ets.foldl(
      fn
        {{:observations_total, _event}, value}, total -> total + value
        _other, total -> total
      end,
      0,
      counters
    )
  end
end

defmodule Docket.Benchmark.Stats do
  @moduledoc false

  def native_distribution(values), do: distribution(values, &native_to_us/1, "us")
  def millisecond_distribution(values), do: distribution(values, & &1, "ms")

  def repetition_summary([], unit), do: %{unit: unit, sample_count: 0}

  def repetition_summary(values, unit) do
    sorted = Enum.sort(values)
    count = length(sorted)
    minimum = hd(sorted)
    maximum = List.last(sorted)
    median = median(sorted)

    %{
      unit: unit,
      sample_count: count,
      min: minimum,
      median: median,
      max: maximum,
      mean: Float.round(Enum.sum(sorted) / count, 3),
      spread: maximum - minimum,
      spread_percent_of_median:
        if(median == 0, do: nil, else: Float.round((maximum - minimum) * 100 / median, 3))
    }
  end

  def distribution([], _convert, unit), do: %{unit: unit, sample_count: 0}

  def distribution(values, convert, unit) do
    sorted = values |> Enum.map(convert) |> Enum.sort()
    count = length(sorted)

    %{
      unit: unit,
      sample_count: count,
      min: hd(sorted),
      p50: percentile(sorted, 0.50),
      p95: percentile(sorted, 0.95),
      p99: percentile(sorted, 0.99),
      max: List.last(sorted),
      mean: Float.round(Enum.sum(sorted) / count, 3)
    }
  end

  defp percentile(sorted, percentile) do
    index = ceil(percentile * length(sorted)) - 1
    Enum.at(sorted, max(index, 0))
  end

  defp median(sorted) do
    count = length(sorted)
    middle = div(count, 2)

    if rem(count, 2) == 1 do
      Enum.at(sorted, middle)
    else
      (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
    end
  end

  defp native_to_us(value),
    do: System.convert_time_unit(value, :native, :microsecond)
end
