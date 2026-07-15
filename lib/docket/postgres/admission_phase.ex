if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.AdmissionPhase do
    @moduledoc false

    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, :ready, Keyword.take(opts, [:name]))
    end

    def run(server, demand, fun) when is_integer(demand) and demand > 0 and is_function(fun, 1) do
      {token, preference} = GenServer.call(server, {:checkout, demand}, :infinity)

      try do
        result = fun.(preference)
        :ok = GenServer.call(server, {:complete, token, result}, :infinity)
        result
      catch
        kind, reason ->
          GenServer.cast(server, {:abandon, token})
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end

    @impl true
    def init(preference), do: {:ok, %{preference: preference, owner: nil, queue: :queue.new()}}

    @impl true
    def handle_call({:checkout, demand}, from, %{owner: nil} = state) do
      {reply, state} = take_ownership(state, from, demand)
      {:reply, reply, state}
    end

    def handle_call({:checkout, demand}, from, state) do
      {:noreply, %{state | queue: :queue.in({from, demand}, state.queue)}}
    end

    def handle_call({:complete, token, result}, _from, %{owner: %{token: token}} = state) do
      state = maybe_advance(state, result)
      {:reply, :ok, release(state)}
    end

    def handle_call({:complete, _token, _result}, _from, state), do: {:reply, :stale, state}

    @impl true
    def handle_cast({:abandon, token}, %{owner: %{token: token}} = state),
      do: {:noreply, release(state)}

    def handle_cast({:abandon, _token}, state), do: {:noreply, state}

    @impl true
    def handle_info(
          {:DOWN, monitor, :process, _pid, _reason},
          %{owner: %{monitor: monitor}} = state
        ),
        do: {:noreply, release(state)}

    def handle_info({:DOWN, _monitor, :process, _pid, _reason}, state), do: {:noreply, state}

    defp take_ownership(state, {pid, _tag}, demand) do
      token = make_ref()
      monitor = Process.monitor(pid)
      reply = {token, state.preference}
      {reply, %{state | owner: %{token: token, monitor: monitor, demand: demand}}}
    end

    defp release(%{owner: %{monitor: monitor}} = state) do
      Process.demonitor(monitor, [:flush])
      state = %{state | owner: nil}

      case :queue.out(state.queue) do
        {{:value, {from, demand}}, queue} ->
          {reply, state} = take_ownership(%{state | queue: queue}, from, demand)
          GenServer.reply(from, reply)
          state

        {:empty, _queue} ->
          state
      end
    end

    defp maybe_advance(%{owner: %{demand: 1}, preference: :ready} = state, {:ok, _}),
      do: %{state | preference: :expired}

    defp maybe_advance(%{owner: %{demand: 1}, preference: :expired} = state, {:ok, _}),
      do: %{state | preference: :ready}

    defp maybe_advance(state, _result), do: state
  end
end
