defmodule Docket.Telemetry do
  @moduledoc """
  Live `:telemetry` emission for run events.

  Every committed transition emits one telemetry event per `Docket.Event`
  it produced, so live UIs and instrumentation can observe runs without
  parsing checkpoint payloads. Emission is observability-only: production
  emits after the backend commit, handlers run via `:telemetry.execute/3`
  with its usual failing-handler detachment, and delivery never affects the
  run.

  ## Event catalog

  | Event name                          | From event type        |
  | ----------------------------------- | ---------------------- |
  | `[:docket, :run, :initialized]`     | `:run_initialized`     |
  | `[:docket, :run, :completed]`       | `:run_completed`       |
  | `[:docket, :run, :failed]`          | `:run_failed`          |
  | `[:docket, :checkpoint, :committed]`| `:checkpoint_committed`|
  | `[:docket, :node, :completed]`      | `:node_completed`      |
  | `[:docket, :node, :failed]`         | `:node_failed`         |
  | `[:docket, :channel, :updated]`     | `:channel_updated`     |
  | `[:docket, :edge, :triggered]`      | `:edge_triggered`      |
  | `[:docket, :interrupt, :requested]` | `:interrupt_requested` |
  | `[:docket, :interrupt, :resolved]`  | `:interrupt_resolved`  |

  Measurements: `%{step: non_neg_integer(), seq: pos_integer()}`.

  Metadata: `%{run_id, graph_id, graph_hash, node_id, channel_id, task_id,
  payload, event}` — `event` is the full `Docket.Event`; the ID keys are
  `nil` when the event does not carry them, and payloads never contain
  channel values (matching the event contract).
  """

  @event_names %{
    run_initialized: [:docket, :run, :initialized],
    run_completed: [:docket, :run, :completed],
    run_failed: [:docket, :run, :failed],
    checkpoint_committed: [:docket, :checkpoint, :committed],
    node_completed: [:docket, :node, :completed],
    node_failed: [:docket, :node, :failed],
    channel_updated: [:docket, :channel, :updated],
    edge_triggered: [:docket, :edge, :triggered],
    interrupt_requested: [:docket, :interrupt, :requested],
    interrupt_resolved: [:docket, :interrupt, :resolved]
  }

  @lifecycle_key {__MODULE__, :lifecycle}

  @metric_metadata %{
    [:docket, :lifecycle, :transaction, :stop] => [:operation, :result],
    [:docket, :lifecycle, :transaction, :exception] => [:operation, :result],
    [:docket, :store, :operation, :stop] => [:operation, :result],
    [:docket, :store, :operation, :exception] => [:operation, :result],
    [:docket, :checkpoint, :observer, :stop] => [:checkpoint_type, :result, :durable_success],
    [:docket, :checkpoint, :observer, :failure] => [:checkpoint_type, :result, :durable_success],
    [:docket, :lifecycle, :committed] => [:checkpoint_type, :disposition, :result],
    [:docket, :node, :execution] => [:result]
  }

  @doc false
  @spec span([atom()], map(), (-> {term(), map()})) :: term()
  def span(name, metadata, fun) when is_list(name) and is_map(metadata) and is_function(fun, 0) do
    started = System.monotonic_time()
    :telemetry.execute(name ++ [:start], %{system_time: System.system_time()}, metadata)

    try do
      {result, stop_metadata} = fun.()

      :telemetry.execute(
        name ++ [:stop],
        %{duration: System.monotonic_time() - started},
        Map.merge(metadata, stop_metadata)
      )

      result
    rescue
      error ->
        :telemetry.execute(
          name ++ [:exception],
          %{duration: System.monotonic_time() - started},
          Map.put(metadata, :result, :exception)
        )

        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        :telemetry.execute(
          name ++ [:exception],
          %{duration: System.monotonic_time() - started},
          Map.put(metadata, :result, :throw)
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @doc false
  @spec lifecycle_span(atom(), (-> term())) :: term()
  def lifecycle_span(operation, fun) when operation in [:start, :moment, :signal] do
    lifecycle_ref = make_ref()
    previous = Process.get(@lifecycle_key)
    Process.put(@lifecycle_key, %{lifecycle_ref: lifecycle_ref, lifecycle_operation: operation})

    try do
      span(
        [:docket, :lifecycle, :transaction],
        %{operation: operation, lifecycle_ref: lifecycle_ref},
        fn ->
          result = fun.()
          {result, %{result: result_kind(result), lifecycle_ref: lifecycle_ref}}
        end
      )
    after
      if previous,
        do: Process.put(@lifecycle_key, previous),
        else: Process.delete(@lifecycle_key)
    end
  end

  @doc false
  @spec correlation_metadata() :: map()
  def correlation_metadata, do: Process.get(@lifecycle_key, %{})

  @doc "Returns the bounded metadata projection safe to use as metric labels."
  @spec metric_metadata([atom()], map()) :: map()
  def metric_metadata(event, metadata) do
    metadata
    |> Map.take(Map.get(@metric_metadata, event, []))
    |> Enum.filter(&bounded_metric_metadata_value?(event, &1))
    |> Map.new()
  end

  defp bounded_metric_metadata_value?(_event, _pair), do: true

  @doc false
  @spec result_kind(term()) :: atom()
  def result_kind({:ok, _}), do: :ok
  def result_kind(:ok), do: :ok
  def result_kind({:error, :stale_fence}), do: :stale_fence
  def result_kind({:error, _}), do: :error
  def result_kind({:skipped, _}), do: :skipped
  def result_kind(_), do: :other

  @doc """
  Emits one telemetry event per run event.

  Unknown event types are skipped rather than raised: telemetry must never
  take down a run.
  """
  @spec emit_events(Docket.Run.t(), [Docket.Event.t()]) :: :ok
  def emit_events(run, events) do
    Enum.each(events, fn %Docket.Event{} = event ->
      case Map.fetch(@event_names, event.type) do
        {:ok, name} ->
          :telemetry.execute(
            name,
            %{step: event.step, seq: event.seq},
            %{
              run_id: event.run_id,
              graph_id: run.graph_id,
              graph_hash: run.graph_hash,
              node_id: event.node_id,
              channel_id: event.channel_id,
              task_id: event.task_id,
              payload: event.payload,
              event: event
            }
          )

        :error ->
          :ok
      end
    end)
  end
end
