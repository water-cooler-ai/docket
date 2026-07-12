defmodule Docket.Test.TelemetryRelay do
  @moduledoc false

  def tagged(_event, measurements, metadata, {pid, tag}) do
    send(pid, {tag, measurements, metadata})
  end

  def tagged_event(event, measurements, metadata, {pid, tag}) do
    send(pid, {tag, event, measurements, metadata})
  end

  def event(event, measurements, metadata, pid) do
    send(pid, {:telemetry, event, measurements, metadata})
  end

  def raw(event, measurements, metadata, pid) do
    send(pid, {event, measurements, metadata})
  end

  def count_query(_event, _measurements, metadata, agent) do
    Agent.update(agent, &Map.update(&1, metadata.query, 1, fn count -> count + 1 end))
  end

  def filtered_name(event, _measurements, %{run_id: run_id}, %{pid: pid, run_id: run_id}) do
    send(pid, {:telemetry, event})
  end

  def filtered_name(_event, _measurements, _metadata, _config), do: :ok
end
