defmodule Docket.Bench.Scorecard.Probe do
  @moduledoc "Periodic SQL sampler recording ready-backlog, in-flight, and done time series."

  alias Docket.Bench.Scorecard.Db

  def start(ctx, interval_ms) do
    started = System.monotonic_time(:millisecond)
    spawn(fn -> loop(ctx, interval_ms, started, []) end)
  end

  def stop(pid) do
    send(pid, {:stop, self()})

    receive do
      {:samples, samples} -> samples
    after
      5_000 -> []
    end
  end

  def shutdown(pid) do
    Process.unlink(pid)
    Process.exit(pid, :kill)
    :ok
  end

  def sample(ctx, started) do
    runs = Db.table(ctx.prefix, "docket_runs")

    query =
      "SELECT " <>
        "count(*) FILTER (WHERE status = 'running' AND claim_token IS NULL AND wake_at <= now()), " <>
        "count(*) FILTER (WHERE claim_token IS NOT NULL), " <>
        "count(*) FILTER (WHERE finished_at IS NOT NULL) FROM #{runs}"

    case Db.repo().query(query) do
      {:ok, %{rows: [[ready_backlog, in_flight, done]]}} ->
        %{
          t_ms: System.monotonic_time(:millisecond) - started,
          ready_backlog: ready_backlog,
          in_flight: in_flight,
          done: done
        }

      _ ->
        nil
    end
  end

  defp loop(ctx, interval_ms, started, samples) do
    samples =
      case sample(ctx, started) do
        nil -> samples
        sample -> [sample | samples]
      end

    receive do
      {:stop, from} -> send(from, {:samples, Enum.reverse(samples)})
    after
      interval_ms -> loop(ctx, interval_ms, started, samples)
    end
  end
end
