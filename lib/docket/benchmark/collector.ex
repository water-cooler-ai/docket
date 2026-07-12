defmodule Docket.Benchmark.Collector do
  @moduledoc false

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

  def start(correlation_ids \\ []) do
    table = :ets.new(__MODULE__, [:ordered_set, :public, write_concurrency: true])
    counters = :ets.new(__MODULE__.Counters, [:set, :public, write_concurrency: true])
    handler_id = {__MODULE__, make_ref()}

    # Telemetry reads the handler config from its ETS table on every event.
    # Keep the potentially large correlation set behind a table reference so
    # emitting processes never copy the whole set into their heaps.
    Enum.each(Enum.with_index(correlation_ids, 1), fn {run_id, ordinal} ->
      true = :ets.insert(counters, {{:correlation, run_id}, ordinal})
    end)

    :ok =
      :telemetry.attach_many(
        handler_id,
        @events,
        &__MODULE__.handle/4,
        %{table: table, counters: counters, correlate?: correlation_ids != []}
      )

    %{table: table, counters: counters, handler_id: handler_id}
  end

  def stop(%{table: table, counters: counters, handler_id: handler_id}) do
    :telemetry.detach(handler_id)

    events =
      Enum.map(:ets.tab2list(table), fn {_key, event, measurements, metadata, observed_at} ->
        {event, measurements, metadata, observed_at}
      end)

    :ets.delete(table)
    :ets.delete(counters)
    events
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

  def count(%{table: table}, event, metadata) do
    :ets.foldl(
      fn
        {_key, ^event, _measurements, observed, _observed_at}, count ->
          if Enum.all?(metadata, fn {key, value} -> observed[key] == value end),
            do: count + 1,
            else: count

        _other, count ->
          count
      end,
      0,
      table
    )
  end

  def handle(event, measurements, metadata, %{
        table: table,
        counters: counters,
        correlate?: correlate?
      }) do
    safe_metadata = safe_metadata(event, metadata, counters)

    key = System.unique_integer([:monotonic, :positive])

    true =
      :ets.insert(
        table,
        {key, event, measurements, safe_metadata, System.monotonic_time()}
      )

    count_observation(counters, event, safe_metadata, correlate?)

    :ok
  end

  def events, do: @events

  def stats(%{table: table}) do
    %{
      capture_mode: "full_event_capture",
      captured_events: :ets.info(table, :size),
      observer_effect: "not_quantified"
    }
  end

  def observer_memory_bytes(%{table: table, counters: counters}) do
    word_size = :erlang.system_info(:wordsize)

    Enum.reduce([table, counters], 0, fn tid, bytes ->
      case :ets.info(tid, :memory) do
        words when is_integer(words) -> bytes + words * word_size
        _undefined -> bytes
      end
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

  defp safe_metadata([:docket, :postgres, :run_store, :claim_query], _metadata, _counters),
    do: %{}

  defp safe_metadata(event, metadata, _counters),
    do: Docket.Telemetry.metric_metadata(event, metadata)

  defp correlation_id(counters, run_id) do
    case :ets.lookup(counters, {:correlation, run_id}) do
      [{{:correlation, ^run_id}, ordinal}] -> ordinal
      [] -> nil
    end
  end

  defp checkpoint_type(%{event: %{metadata: %{"checkpoint_type" => type}}}), do: type
  defp checkpoint_type(_metadata), do: nil

  defp count_observation(counters, event, metadata, correlate?)
       when event in [[:docket, :checkpoint, :committed], [:docket, :run, :completed]] and
              correlate? do
    case metadata.correlation_id do
      nil ->
        increment(counters, {:unknown_correlation, event})

      correlation_id ->
        marker =
          {:correlation_observed, event, correlation_id, metadata.checkpoint_type}

        if :ets.insert_new(counters, {marker, true}) do
          increment(counters, event)

          if event == [:docket, :checkpoint, :committed] do
            increment(
              counters,
              {event, {:correlated_checkpoint_type, metadata.checkpoint_type}}
            )
          end
        end
    end
  end

  defp count_observation(counters, event, _metadata, _correlate?),
    do: increment(counters, event)

  defp increment(counters, key),
    do: :ets.update_counter(counters, key, {2, 1}, {key, 0})

  defp counter(table, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] -> value
      [] -> 0
    end
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
