defmodule Docket.Benchmark.Progress do
  @moduledoc false

  @frames ~w(⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏)
  @tick_ms 100

  def start(total, opts \\ [])

  def start(total, opts) when is_integer(total) and total > 0 do
    case resolve_mode(Keyword.get(opts, :mode, :auto)) do
      :off ->
        :off

      mode ->
        state = %{
          device: Keyword.get(opts, :device, :standard_error),
          mode: mode,
          total: total,
          completed: 0,
          current: nil,
          frame: 0
        }

        %{pid: spawn(fn -> loop(state) end)}
    end
  end

  def start(_total, _opts), do: :off

  def point_started(:off, _index, _point), do: :ok

  def point_started(%{pid: pid}, index, point) do
    send(pid, {:point_started, index, label(point)})
    :ok
  end

  def point_finished(:off, _index, _success?), do: :ok

  def point_finished(%{pid: pid}, index, success?) do
    send(pid, {:point_finished, index, success?})
    :ok
  end

  def stop(:off), do: :ok

  def stop(%{pid: pid}) do
    ref = Process.monitor(pid)
    send(pid, :stop)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    after
      1_000 ->
        Process.demonitor(ref, [:flush])
        Process.exit(pid, :kill)
        :ok
    end
  end

  def label(point) do
    base = "#{point.scenario} c=#{point.concurrency} pool=#{point.pool_size}"

    if Map.get(point, :repetitions, 1) > 1 do
      "#{base} rep=#{point.repetition}/#{point.repetitions}"
    else
      base
    end
  end

  defp resolve_mode(:auto), do: if(IO.ANSI.enabled?(), do: :animated, else: :plain)
  defp resolve_mode(mode) when mode in [:animated, :plain, :off], do: mode

  defp loop(%{mode: :animated} = state) do
    receive do
      message -> handle(message, state)
    after
      @tick_ms ->
        state = %{state | frame: state.frame + 1}
        render(state)
        loop(state)
    end
  end

  defp loop(state) do
    receive do
      message -> handle(message, state)
    end
  end

  defp handle({:point_started, index, label}, state) do
    state = %{state | current: {index, label, System.monotonic_time(:millisecond)}}

    case state.mode do
      :plain ->
        IO.write(state.device, "trial #{index}/#{state.total} · #{label} · started\n")

      :animated ->
        render(state)
    end

    loop(state)
  end

  defp handle({:point_finished, index, success?}, state) do
    line = "trial #{index}/#{state.total} · #{finished_details(state, index, success?)}\n"

    case state.mode do
      :plain -> IO.write(state.device, line)
      :animated -> IO.write(state.device, "\r\e[K" <> line)
    end

    state = %{state | completed: state.completed + 1, current: nil}
    if state.mode == :animated, do: render(state)
    loop(state)
  end

  defp handle(:stop, state) do
    if state.mode == :animated, do: IO.write(state.device, "\r\e[K")
    :ok
  end

  defp finished_details(state, index, success?) do
    status = if success?, do: "PASS", else: "FAIL"

    case state.current do
      {^index, label, started_at} ->
        elapsed = format_elapsed(System.monotonic_time(:millisecond) - started_at)
        "#{label} · #{status} · #{elapsed}"

      _other ->
        status
    end
  end

  defp render(state) do
    frame = Enum.at(@frames, rem(state.frame, length(@frames)))

    line =
      case state.current do
        {index, label, started_at} ->
          elapsed = format_elapsed(System.monotonic_time(:millisecond) - started_at)
          "trial #{index}/#{state.total} · running #{label} · #{elapsed}"

        nil ->
          "trials #{state.completed}/#{state.total} complete"
      end

    IO.write(state.device, "\r\e[K#{frame} #{line}")
  end

  defp format_elapsed(elapsed_ms) do
    seconds = elapsed_ms / 1_000

    if seconds < 60 do
      "#{:erlang.float_to_binary(seconds, decimals: 1)}s"
    else
      whole = trunc(seconds)
      remainder = rem(whole, 60) |> Integer.to_string() |> String.pad_leading(2, "0")
      "#{div(whole, 60)}m #{remainder}s"
    end
  end
end
