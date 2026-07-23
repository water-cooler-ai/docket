if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.AdmissionPhase do
    @moduledoc """
    Coordinates the admission preference shared by one PostgreSQL backend instance.

    The legacy claim policy gives single-run claims (`demand == 1`) a choice between
    ready work and expired leases. To prevent one class from being preferred
    indefinitely, successful single-run attempts alternate their first choice:
    `:ready`, then `:expired`, then `:ready` again. Here, successful means any
    `{:ok, _}` result, including an empty or poison-only batch; it does not mean that
    a lease was returned. A query may also fall through to the non-preferred class,
    so this alternates the first choice rather than guaranteeing alternating result
    classes.

    Every admission receives the current preference. The legacy policy ignores it
    for multi-run ordering because that query already makes progress across both
    classes, and multi-run attempts do not advance the phase. Other policy
    implementations remain free to interpret the supplied value.

    This process owns exactly three pieces of transient, instance-local state:

      * `preference` — the next first-choice class, initially `:ready`;
      * `owner` — the current attempt's token, demand, and caller monitor;
      * `queue` — callers waiting in FIFO order for the current owner to finish.

    `run/3` executes the callback in the calling process while holding exclusive
    ownership, supplies it with the current preference, and advances the preference
    only after a successful single-run attempt. Error results, exceptions, caller
    exits, and multi-run attempts release ownership without advancing it. Caller
    monitoring ensures a process that exits cannot strand the queue.

    Checkout and completion intentionally have no timeout: claim serialization must
    outlive ordinary call timeouts. Consequently, a live callback that never returns
    blocks the queue, and a callback must not call `run/3` recursively for the same
    phase. The owning claim path is responsible for bounding its database operation;
    process death is the phase's recovery boundary.

    Production dispatcher mode and synchronous testing/drain mode both use this
    abstraction, with one phase process for the active mode of each backend instance.
    Keeping the phase here, rather than in a caller or per-call options, gives that
    mode one serialized ordering history. In production it belongs to the Runner's
    `:one_for_all` accounting unit alongside the dispatcher and vehicle supervisor;
    restarting that unit resets the phase to `:ready`. In testing mode it is the
    backend's standalone child. The state is scheduling coordination only and is
    deliberately not durable.
    """

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
