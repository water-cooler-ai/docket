defmodule Docket.Benchmark.Timeline do
  @moduledoc false

  # Buckets are stored newest-first so each sample prepends in constant time
  # instead of copying the list; chronological order is restored during
  # compaction and in artifact/1.
  defstruct max_buckets: 256,
            buckets: [],
            bucket_count: 0,
            raw_sample_count: 0,
            compactions: 0

  def new(max_buckets) when is_integer(max_buckets) and max_buckets > 0,
    do: %__MODULE__{max_buckets: max_buckets}

  def add(%__MODULE__{} = timeline, offset_us, metrics)
      when is_integer(offset_us) and offset_us >= 0 and is_map(metrics) do
    buckets = [raw_bucket(offset_us, metrics) | timeline.buckets]
    bucket_count = timeline.bucket_count + 1

    if bucket_count > timeline.max_buckets do
      %{
        timeline
        | buckets: compact_once(buckets),
          bucket_count: bucket_count - 1,
          raw_sample_count: timeline.raw_sample_count + 1,
          compactions: timeline.compactions + 1
      }
    else
      %{
        timeline
        | buckets: buckets,
          bucket_count: bucket_count,
          raw_sample_count: timeline.raw_sample_count + 1
      }
    end
  end

  def artifact(%__MODULE__{} = timeline) do
    buckets = Enum.reverse(timeline.buckets)

    summary =
      case buckets do
        [] -> %{}
        [first | rest] -> rest |> Enum.reduce(first, &merge_bucket(&2, &1)) |> metric_artifact()
      end

    %{
      raw_sample_count: timeline.raw_sample_count,
      retained_bucket_count: timeline.bucket_count,
      max_retained_buckets: timeline.max_buckets,
      compactions: timeline.compactions,
      compaction_strategy: "merge_adjacent_lowest_weight_pair",
      represented_sample_count: Enum.reduce(buckets, 0, &(&1.sample_count + &2)),
      maximum_samples_represented_by_one_bucket:
        buckets |> Enum.map(& &1.sample_count) |> Enum.max(fn -> 0 end),
      summary: summary,
      buckets: Enum.map(buckets, &bucket_artifact/1)
    }
  end

  defp raw_bucket(offset_us, metrics) do
    metrics =
      metrics
      |> Enum.flat_map(fn
        {key, value} when is_atom(key) and is_number(value) ->
          [
            {key,
             %{
               min: value,
               max: value,
               sum: value,
               first: value,
               last: value,
               last_observed_offset_us: offset_us,
               sample_count: 1
             }}
          ]

        _other ->
          []
      end)
      |> Map.new()

    %{
      start_offset_us: offset_us,
      end_offset_us: offset_us,
      sample_count: 1,
      metrics: metrics
    }
  end

  defp compact_once(newest_first) do
    newest_first
    |> Enum.reverse()
    |> compact_chronological_once()
    |> Enum.reverse()
  end

  defp compact_chronological_once(buckets) do
    {_weight, index} =
      buckets
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.with_index()
      |> Enum.map(fn {[left, right], index} ->
        {left.sample_count + right.sample_count, index}
      end)
      |> Enum.min()

    {prefix, [left, right | suffix]} = Enum.split(buckets, index)
    prefix ++ [merge_bucket(left, right)] ++ suffix
  end

  defp merge_bucket(left, right) do
    %{
      start_offset_us: left.start_offset_us,
      end_offset_us: right.end_offset_us,
      sample_count: left.sample_count + right.sample_count,
      metrics:
        Map.merge(left.metrics, right.metrics, fn _key, left_metric, right_metric ->
          %{
            min: min(left_metric.min, right_metric.min),
            max: max(left_metric.max, right_metric.max),
            sum: left_metric.sum + right_metric.sum,
            first: left_metric.first,
            last: right_metric.last,
            last_observed_offset_us: right_metric.last_observed_offset_us,
            sample_count: left_metric.sample_count + right_metric.sample_count
          }
        end)
    }
  end

  defp bucket_artifact(bucket) do
    %{
      start_offset_us: bucket.start_offset_us,
      end_offset_us: bucket.end_offset_us,
      represented_samples: bucket.sample_count,
      metrics: metric_artifact(bucket)
    }
  end

  defp metric_artifact(%{metrics: metrics}) do
    Map.new(metrics, fn {key, metric} ->
      {key,
       %{
         min: metric.min,
         max: metric.max,
         first: metric.first,
         last: metric.last,
         delta: metric.last - metric.first,
         sample_mean: Float.round(metric.sum / metric.sample_count, 3),
         last_observed_offset_us: metric.last_observed_offset_us,
         sample_count: metric.sample_count
       }}
    end)
  end
end

defmodule Docket.Benchmark.Sampler do
  @moduledoc false

  @dispatcher_state [:docket, :postgres, :dispatcher, :state]
  @claim_scan [:docket, :postgres, :run_store, :claim]
  @events [@dispatcher_state, @claim_scan]

  def start(opts) do
    start_at = Keyword.fetch!(opts, :start_at)
    interval_ms = Keyword.fetch!(opts, :interval_ms)
    max_buckets = Keyword.fetch!(opts, :max_buckets)
    sample = Keyword.fetch!(opts, :sample)
    stop_timeout_ms = Keyword.get(opts, :stop_timeout_ms, 5_000)

    unless is_integer(start_at) do
      raise ArgumentError, "sample start_at must be a monotonic integer"
    end

    unless is_integer(interval_ms) and interval_ms > 0 do
      raise ArgumentError, "sample interval must be a positive integer"
    end

    unless is_integer(max_buckets) and max_buckets > 0 do
      raise ArgumentError, "sample bucket limit must be a positive integer"
    end

    unless is_function(sample, 1) do
      raise ArgumentError, "sample callback must accept the current event-derived gauges"
    end

    unless is_integer(stop_timeout_ms) and stop_timeout_ms > 0 do
      raise ArgumentError, "sampler stop timeout must be a positive integer"
    end

    timeline = Docket.Benchmark.Timeline.new(max_buckets)
    gauges = :atomics.new(7, signed: false)
    handler_id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(handler_id, @events, &__MODULE__.handle/4, gauges)
    owner = self()

    pid =
      spawn(fn ->
        try do
          loop(%{
            start_at: start_at,
            next_at: start_at,
            interval_native: System.convert_time_unit(interval_ms, :millisecond, :native),
            sample: sample,
            gauges: gauges,
            timeline: timeline,
            missed_ticks: 0,
            failed_samples: 0,
            maximum_lateness_us: 0,
            scheduled_samples: 0,
            forced_phase_samples: 0,
            forced_final_samples: 0,
            unavailable_metric_observations: %{},
            summed_sampler_self_time_us: 0,
            maximum_sampler_self_time_us: 0,
            last_sample_offset_us: nil
          })
        after
          :telemetry.detach(handler_id)
        end
      end)

    watchdog = spawn(fn -> watch_owner(owner, pid, handler_id) end)

    %{
      pid: pid,
      watchdog: watchdog,
      gauges: gauges,
      handler_id: handler_id,
      stop_timeout_ms: stop_timeout_ms
    }
  end

  def stop(%{pid: pid, handler_id: handler_id, stop_timeout_ms: timeout}) do
    reference = make_ref()

    if Process.alive?(pid) do
      send(pid, {:stop, self(), reference})

      receive do
        {:stopped, ^reference, artifact} ->
          :telemetry.detach(handler_id)
          artifact
      after
        timeout ->
          :telemetry.detach(handler_id)
          terminate_sampler(pid)
          raise "benchmark sampler did not stop"
      end
    else
      :telemetry.detach(handler_id)
      raise "benchmark sampler stopped unexpectedly"
    end
  end

  def force_sample(%{pid: pid, handler_id: handler_id, stop_timeout_ms: timeout}) do
    reference = make_ref()
    send(pid, {:force_sample, self(), reference})

    receive do
      {:sampled, ^reference, sample} -> sample
    after
      timeout ->
        :telemetry.detach(handler_id)
        terminate_sampler(pid)
        raise "benchmark sampler did not produce a forced sample"
    end
  end

  def snapshot(%{gauges: gauges}), do: gauge_snapshot(gauges)

  def handle(@dispatcher_state, measurements, _metadata, gauges) do
    :atomics.add(gauges, 7, 1)

    try do
      :atomics.put(gauges, 1, non_negative(measurements[:in_flight]))
      update_max(gauges, 2, non_negative(measurements[:in_flight]))
      :atomics.put(gauges, 3, non_negative(measurements[:demand]))
      :atomics.put(gauges, 4, non_negative(measurements[:poll_active]))
      :atomics.put(gauges, 5, non_negative(measurements[:poll_pending]))
    after
      :atomics.add(gauges, 7, 1)
    end

    :ok
  end

  def handle(@claim_scan, measurements, _metadata, gauges) do
    :atomics.add(gauges, 6, non_negative(measurements[:leases]))
    :ok
  end

  defp loop(state) do
    now = System.monotonic_time()
    wait_native = max(state.next_at - now, 0)
    wait_us = System.convert_time_unit(wait_native, :native, :microsecond)
    wait_ms = div(wait_us + 999, 1_000)

    receive do
      {:stop, caller, reference} ->
        {state, _sample} = take_sample(state, :final)
        send(caller, {:stopped, reference, artifact(state)})

      {:force_sample, caller, reference} ->
        {state, sample} = take_sample(state, :phase)
        send(caller, {:sampled, reference, sample})
        loop(state)

      _other ->
        loop(state)
    after
      wait_ms ->
        {state, _sample} = take_sample(state, :scheduled)
        now = System.monotonic_time()
        nominal_next = state.next_at + state.interval_native
        overdue = max(now - nominal_next, 0)

        missed =
          if overdue == 0,
            do: 0,
            else: div(overdue + state.interval_native - 1, state.interval_native)

        next_at = nominal_next + missed * state.interval_native
        loop(%{state | next_at: next_at, missed_ticks: state.missed_ticks + missed})
    end
  end

  defp take_sample(state, kind) do
    sampler_started = System.monotonic_time()
    observed_at = System.monotonic_time()
    offset = max(observed_at - state.start_at, 0)
    lateness = if(kind == :scheduled, do: max(observed_at - state.next_at, 0), else: nil)
    sampling_started = System.monotonic_time()
    gauges = gauge_snapshot(state.gauges)

    {metrics, failed?} =
      try do
        case state.sample.(gauges) do
          metrics when is_map(metrics) -> {metrics, false}
          _other -> {%{}, true}
        end
      rescue
        _error -> {%{}, true}
      catch
        _kind, _reason -> {%{}, true}
      end

    sampling_duration = System.monotonic_time() - sampling_started

    metrics =
      metrics
      |> Map.merge(gauges)
      |> Map.put(:sampler_probe_callback_duration_us, native_to_us(sampling_duration))

    metrics =
      if is_integer(lateness),
        do: Map.put(metrics, :sampler_tick_lateness_us, native_to_us(lateness)),
        else: metrics

    unavailable =
      Enum.reduce(metrics, state.unavailable_metric_observations, fn
        {key, value}, counts when is_atom(key) and is_number(value) -> counts
        {key, _value}, counts when is_atom(key) -> Map.update(counts, key, 1, &(&1 + 1))
        _other, counts -> counts
      end)

    offset_us = native_to_us(offset)
    timeline = Docket.Benchmark.Timeline.add(state.timeline, offset_us, metrics)
    sampler_self_time_us = native_to_us(System.monotonic_time() - sampler_started)

    state = %{
      state
      | timeline: timeline,
        failed_samples: state.failed_samples + if(failed?, do: 1, else: 0),
        maximum_lateness_us:
          max(
            state.maximum_lateness_us,
            if(is_integer(lateness), do: native_to_us(lateness), else: 0)
          ),
        scheduled_samples: state.scheduled_samples + if(kind == :scheduled, do: 1, else: 0),
        forced_phase_samples: state.forced_phase_samples + if(kind == :phase, do: 1, else: 0),
        forced_final_samples: state.forced_final_samples + if(kind == :final, do: 1, else: 0),
        unavailable_metric_observations: unavailable,
        summed_sampler_self_time_us: state.summed_sampler_self_time_us + sampler_self_time_us,
        maximum_sampler_self_time_us:
          max(state.maximum_sampler_self_time_us, sampler_self_time_us),
        last_sample_offset_us: offset_us
    }

    sample_metrics =
      metrics
      |> Enum.filter(fn {_key, value} -> is_number(value) end)
      |> Map.new()

    {state, %{offset_us: offset_us, metrics: sample_metrics, kind: kind}}
  end

  defp artifact(state) do
    sample_count =
      state.scheduled_samples + state.forced_phase_samples + state.forced_final_samples

    sampling_span_us = max(state.last_sample_offset_us || 0, 1)

    state.timeline
    |> Docket.Benchmark.Timeline.artifact()
    |> Map.merge(%{
      interval_ms: System.convert_time_unit(state.interval_native, :native, :millisecond),
      missed_ticks: state.missed_ticks,
      failed_samples: state.failed_samples,
      maximum_lateness_us: state.maximum_lateness_us,
      scheduled_sample_count: state.scheduled_samples,
      forced_phase_sample_count: state.forced_phase_samples,
      forced_final_sample_count: state.forced_final_samples,
      sampling_end_offset_us: state.last_sample_offset_us,
      unavailable_metric_observations: state.unavailable_metric_observations,
      observer_diagnostics: %{
        summed_sampler_self_time_us: state.summed_sampler_self_time_us,
        maximum_sampler_self_time_us: state.maximum_sampler_self_time_us,
        mean_sampler_self_time_us:
          if(sample_count == 0,
            do: 0.0,
            else: Float.round(state.summed_sampler_self_time_us / sample_count, 3)
          ),
        serial_sampler_duty_cycle_percent:
          Float.round(state.summed_sampler_self_time_us * 100 / sampling_span_us, 3),
        interpretation:
          "Serial sampler self-time is diagnostic; it is not proof of zero indirect observer effect."
      },
      metric_semantics: %{
        counters: [:cumulative_claim_leases],
        high_watermarks: [
          :dispatcher_maximum_in_flight_vehicles,
          :maximum_blocked_node_calls
        ],
        summary_mean: "sample_count_weighted_not_time_weighted"
      },
      event_scope: "global_docket_telemetry_requires_quiescent_beam"
    })
  end

  defp gauge_snapshot(gauges) do
    version = :atomics.get(gauges, 7)

    if rem(version, 2) == 1 do
      gauge_snapshot(gauges)
    else
      snapshot = %{
        dispatcher_in_flight_vehicles: :atomics.get(gauges, 1),
        dispatcher_maximum_in_flight_vehicles: :atomics.get(gauges, 2),
        dispatcher_demand: :atomics.get(gauges, 3),
        dispatcher_poll_active: :atomics.get(gauges, 4),
        dispatcher_poll_pending: :atomics.get(gauges, 5),
        cumulative_claim_leases: :atomics.get(gauges, 6)
      }

      if :atomics.get(gauges, 7) == version, do: snapshot, else: gauge_snapshot(gauges)
    end
  end

  defp update_max(atomics, index, value) do
    current = :atomics.get(atomics, index)

    cond do
      value <= current ->
        :ok

      :atomics.compare_exchange(atomics, index, current, value) == :ok ->
        :ok

      true ->
        update_max(atomics, index, value)
    end
  end

  defp non_negative(value) when is_integer(value) and value >= 0, do: value
  defp non_negative(_value), do: 0

  defp terminate_sampler(pid) do
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    after
      1_000 -> Process.demonitor(monitor, [:flush])
    end
  end

  defp watch_owner(owner, sampler, handler_id) do
    owner_monitor = Process.monitor(owner)
    sampler_monitor = Process.monitor(sampler)

    try do
      receive do
        {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
          :telemetry.detach(handler_id)
          terminate_sampler(sampler)

        {:DOWN, ^sampler_monitor, :process, ^sampler, _reason} ->
          :telemetry.detach(handler_id)
      end
    after
      Process.demonitor(owner_monitor, [:flush])
      Process.demonitor(sampler_monitor, [:flush])
    end
  end

  defp native_to_us(value),
    do: System.convert_time_unit(value, :native, :microsecond)
end
