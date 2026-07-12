defmodule Docket.Benchmark.Collector do
  @moduledoc false

  @events [
    [:docket, :lifecycle, :transaction, :stop],
    [:docket, :lifecycle, :committed],
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

  def start do
    table = :ets.new(__MODULE__, [:ordered_set, :public, write_concurrency: true])
    handler_id = {__MODULE__, make_ref()}

    :ok = :telemetry.attach_many(handler_id, @events, &__MODULE__.handle/4, table)
    %{table: table, handler_id: handler_id}
  end

  def stop(%{table: table, handler_id: handler_id}) do
    :telemetry.detach(handler_id)

    events =
      Enum.map(:ets.tab2list(table), fn {_key, event, measurements, metadata, observed_at} ->
        {event, measurements, metadata, observed_at}
      end)

    :ets.delete(table)
    events
  end

  def count(%{table: table}, event, metadata \\ %{}) do
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

  def handle(event, measurements, metadata, table) do
    safe_metadata =
      if event in [
           [:docket, :benchmark, :repo, :query],
           [:docket, :postgres, :run_store, :claim_query]
         ] do
        %{}
      else
        Docket.Telemetry.metric_metadata(event, metadata)
      end

    key = System.unique_integer([:monotonic, :positive])

    true =
      :ets.insert(
        table,
        {key, event, measurements, safe_metadata, System.monotonic_time()}
      )

    :ok
  end

  def events, do: @events
end

defmodule Docket.Benchmark.Stats do
  @moduledoc false

  def native_distribution(values), do: distribution(values, &native_to_us/1, "us")
  def millisecond_distribution(values), do: distribution(values, & &1, "ms")

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

  defp native_to_us(value),
    do: System.convert_time_unit(value, :native, :microsecond)
end
