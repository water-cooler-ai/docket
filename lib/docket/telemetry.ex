defmodule Docket.Telemetry do
  @moduledoc """
  Live `:telemetry` emission for run events.

  Every committed transition emits one telemetry event per `Docket.Event`
  it produced, so live UIs and instrumentation can observe runs without
  parsing checkpoint payloads. Emission is observability-only: it happens
  after the transition's sync checkpoint is accepted (or alongside the
  pending async checkpoint), handlers run via `:telemetry.execute/3` with
  its usual failing-handler detachment, and delivery never affects the run.

  ## Event catalog

  | Event name                          | From event type        |
  | ----------------------------------- | ---------------------- |
  | `[:docket, :run, :initialized]`     | `:run_initialized`     |
  | `[:docket, :run, :completed]`       | `:run_completed`       |
  | `[:docket, :run, :failed]`          | `:run_failed`          |
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
    node_completed: [:docket, :node, :completed],
    node_failed: [:docket, :node, :failed],
    channel_updated: [:docket, :channel, :updated],
    edge_triggered: [:docket, :edge, :triggered],
    interrupt_requested: [:docket, :interrupt, :requested],
    interrupt_resolved: [:docket, :interrupt, :resolved]
  }

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
