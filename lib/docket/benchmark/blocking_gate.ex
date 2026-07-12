defmodule Docket.Benchmark.BlockingNode do
  @moduledoc false
  @behaviour Docket.Node

  @impl true
  def config_schema, do: Docket.Schema.object(%{})

  @impl true
  def call(_state, _config, context) do
    %{gate: gate, token: token} = Map.fetch!(context.application, :blocking_benchmark)

    case Docket.Benchmark.BlockingGate.await(
           gate,
           token,
           context.run_id,
           context.node_id,
           context.attempt
         ) do
      :ok -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end
end

defmodule Docket.Benchmark.BlockingGate do
  @moduledoc false
  use GenServer

  def start(opts) do
    token = make_ref()
    counters = :atomics.new(7, signed: false)
    opts = opts |> Keyword.put(:token, token) |> Keyword.put(:counters, counters)
    {:ok, pid} = GenServer.start_link(__MODULE__, opts)
    %{pid: pid, token: token, counters: counters}
  end

  def await(gate, token, run_id, node_id, attempt),
    do: GenServer.call(gate, {:await, token, run_id, node_id, attempt}, :infinity)

  def open(%{pid: gate}), do: GenServer.call(gate, :open, 5_000)

  def snapshot(%{pid: gate, counters: counters}) do
    state = GenServer.call(gate, :snapshot)

    Map.merge(state, gauges(%{counters: counters}))
  end

  def gauges(%{counters: counters}) do
    %{
      currently_blocked: :atomics.get(counters, 1),
      maximum_blocked: :atomics.get(counters, 2),
      observed_runs: :atomics.get(counters, 3),
      duplicate_runs: :atomics.get(counters, 4),
      unknown_runs: :atomics.get(counters, 5),
      invalid_attempts: :atomics.get(counters, 6),
      invalid_nodes: :atomics.get(counters, 7)
    }
  end

  def blocked_pids(%{pid: gate}), do: GenServer.call(gate, :blocked_pids)

  def stop(%{pid: gate} = handle) do
    if Process.alive?(gate) do
      _summary = open(handle)
      GenServer.stop(gate, :normal, 5_000)
    end

    :ok
  end

  @impl true
  def init(opts) do
    allowed = Keyword.fetch!(opts, :allowed_run_ids) |> MapSet.new()
    target = Keyword.fetch!(opts, :target)

    unless is_integer(target) and target > 0 and target <= MapSet.size(allowed) do
      raise ArgumentError, "blocking gate target must fit within the allowed run set"
    end

    {:ok,
     %{
       owner: Keyword.fetch!(opts, :owner),
       token: Keyword.fetch!(opts, :token),
       counters: Keyword.fetch!(opts, :counters),
       allowed: allowed,
       target: target,
       seen: MapSet.new(),
       waiters: %{},
       arrival_times: [],
       plateau_at: nil,
       release: nil,
       open?: false
     }}
  end

  @impl true
  def handle_call({:await, token, run_id, node_id, attempt}, from, state) do
    cond do
      token != state.token or not MapSet.member?(state.allowed, run_id) ->
        :atomics.add(state.counters, 5, 1)
        {:reply, {:error, :unknown_blocking_run}, state}

      MapSet.member?(state.seen, run_id) ->
        :atomics.add(state.counters, 4, 1)
        {:reply, {:error, :duplicate_blocking_run}, state}

      attempt != 1 ->
        :atomics.add(state.counters, 6, 1)
        {:reply, {:error, :invalid_blocking_attempt}, state}

      node_id != "blocker" ->
        :atomics.add(state.counters, 7, 1)
        {:reply, {:error, :invalid_blocking_node}, state}

      state.open? ->
        :atomics.add(state.counters, 3, 1)
        {:reply, :ok, %{state | seen: MapSet.put(state.seen, run_id)}}

      true ->
        observed_at = System.monotonic_time()
        waiters = Map.put(state.waiters, run_id, %{from: from, pid: elem(from, 0)})
        current = map_size(waiters)
        :atomics.put(state.counters, 1, current)
        update_max(state.counters, 2, current)
        :atomics.add(state.counters, 3, 1)

        state = %{
          state
          | seen: MapSet.put(state.seen, run_id),
            waiters: waiters,
            arrival_times: [observed_at | state.arrival_times]
        }

        state =
          if current == state.target and is_nil(state.plateau_at) do
            send(
              state.owner,
              {:docket_benchmark_blocking_plateau, self(), state.target, observed_at}
            )

            %{state | plateau_at: observed_at}
          else
            state
          end

        {:noreply, state}
    end
  end

  def handle_call(:open, _from, %{open?: true} = state), do: {:reply, state.release, state}

  def handle_call(:open, _from, state) do
    started = System.monotonic_time()
    Enum.each(state.waiters, fn {_run_id, waiter} -> GenServer.reply(waiter.from, :ok) end)
    finished = System.monotonic_time()
    :atomics.put(state.counters, 1, 0)

    release = %{
      blocked_count: map_size(state.waiters),
      blocked_run_ids: Map.keys(state.waiters),
      blocked_arrival_times: Enum.reverse(state.arrival_times),
      plateau_at: state.plateau_at,
      release_started_at: started,
      release_completed_at: finished,
      fanout_duration: finished - started
    }

    {:reply, release, %{state | open?: true, waiters: %{}, release: release}}
  end

  def handle_call(:snapshot, _from, state) do
    {:reply,
     %{
       target: state.target,
       open: state.open?,
       blocked_waiters: map_size(state.waiters),
       plateau_reached: not is_nil(state.plateau_at),
       total_allowed_runs: MapSet.size(state.allowed)
     }, state}
  end

  def handle_call(:blocked_pids, _from, state) do
    {:reply, Enum.map(state.waiters, fn {_run_id, waiter} -> waiter.pid end), state}
  end

  defp update_max(atomics, index, value) do
    current = :atomics.get(atomics, index)

    cond do
      value <= current -> :ok
      :atomics.compare_exchange(atomics, index, current, value) == :ok -> :ok
      true -> update_max(atomics, index, value)
    end
  end
end
