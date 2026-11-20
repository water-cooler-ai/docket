if Code.ensure_loaded?(Exqlite.Sqlite3) do
  defmodule Docket.SQLite.Runner do
    @moduledoc """
    Claims due runs and launches one vehicle task per claim.

    The runner replaces the Postgres dispatcher, admission phase, and
    LISTEN/NOTIFY notifier. Because this node owns the database:

      * stores wake the runner directly when they commit work that is due
        (`{:docket_sqlite_wake, at}`), so new and resumed runs start without
        waiting for a poll;
      * the runner keeps one timer for the earliest future wake and falls back
        to `:poll_interval_ms` only to catch expired claims;
      * on boot it releases every claim left by a previous process instead of
        waiting out the orphan TTL;
      * when a vehicle task exits abnormally its claim is released at once.
        The claim attempt it used still counts, so a run that keeps crashing
        its vehicle is poisoned after `:max_claim_attempts`.
    """

    use GenServer

    alias Docket.SQLite.{Store, Vehicle}

    @min_timer_ms 5

    def start_link(opts),
      do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

    @impl true
    def init(opts) do
      context = Keyword.fetch!(opts, :context)
      graph_cache = :ets.new(__MODULE__, [:set, :public, read_concurrency: true])

      state = %{
        context: context,
        clock: Keyword.fetch!(opts, :clock),
        concurrency: Keyword.fetch!(opts, :concurrency),
        poll_interval_ms: Keyword.fetch!(opts, :poll_interval_ms),
        orphan_ttl_ms: Keyword.fetch!(opts, :orphan_ttl_ms),
        max_claim_attempts: Keyword.fetch!(opts, :max_claim_attempts),
        on_poisoned: Keyword.get(opts, :on_poisoned, fn _poisoned -> :ok end),
        task_supervisor: Keyword.fetch!(opts, :task_supervisor),
        vehicle_opts: Keyword.put(Keyword.fetch!(opts, :vehicle_opts), :graph_cache, graph_cache),
        vehicles: %{},
        poll_queued?: false,
        timer: nil
      }

      _released = Store.release_stale_claims(context)
      {:ok, queue_poll(state)}
    end

    @impl true
    def handle_info(:poll, state), do: {:noreply, poll(%{state | poll_queued?: false})}

    def handle_info({:docket_sqlite_wake, nil}, state), do: {:noreply, queue_poll(state)}

    def handle_info({:docket_sqlite_wake, %DateTime{} = at}, state),
      do: {:noreply, schedule(state, DateTime.diff(at, state.clock.(), :millisecond))}

    def handle_info({:timer, ref}, %{timer: {ref, _timer_ref, _fires_at}} = state),
      do: {:noreply, queue_poll(%{state | timer: nil})}

    def handle_info({:timer, _stale}, state), do: {:noreply, state}

    def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
      case Map.pop(state.vehicles, monitor) do
        {nil, _vehicles} ->
          {:noreply, state}

        {lease, vehicles} ->
          if reason != :normal do
            :ok =
              Store.release_claim(
                state.context,
                :system,
                lease.run_id,
                lease.claim_token,
                state.clock.()
              )
          end

          {:noreply, queue_poll(%{state | vehicles: vehicles})}
      end
    end

    def handle_info(_message, state), do: {:noreply, state}

    defp queue_poll(%{poll_queued?: true} = state), do: state

    defp queue_poll(state) do
      send(self(), :poll)
      %{state | poll_queued?: true}
    end

    defp poll(state) do
      free = state.concurrency - map_size(state.vehicles)

      state =
        if free > 0 do
          {:ok, %{leases: leases, poisoned: poisoned}} =
            Store.claim_due(state.context, :system, %{
              now: state.clock.(),
              limit: free,
              orphan_ttl_ms: state.orphan_ttl_ms,
              max_claim_attempts: state.max_claim_attempts
            })

          if poisoned != [], do: state.on_poisoned.(poisoned)
          Enum.reduce(leases, state, &launch/2)
        else
          state
        end

      # A full runner polls again when a vehicle exits. Otherwise wake for the
      # earliest scheduled run, and at least every poll interval for expired
      # claims.
      if map_size(state.vehicles) >= state.concurrency do
        state
      else
        delay =
          case Store.next_wake(state.context) do
            nil -> state.poll_interval_ms
            at -> min(DateTime.diff(at, state.clock.(), :millisecond), state.poll_interval_ms)
          end

        schedule(state, delay)
      end
    end

    defp launch(lease, state) do
      opts = state.vehicle_opts

      {:ok, pid} =
        Task.Supervisor.start_child(state.task_supervisor, fn -> Vehicle.drain(lease, opts) end)

      %{state | vehicles: Map.put(state.vehicles, Process.monitor(pid), lease)}
    end

    # Keeps exactly one timer: a request later than the pending one is
    # ignored, an earlier one replaces it.
    defp schedule(state, delay_ms) do
      delay_ms = max(delay_ms, @min_timer_ms)
      fires_at = System.monotonic_time(:millisecond) + delay_ms

      case state.timer do
        {_ref, _timer_ref, pending} when pending <= fires_at ->
          state

        pending ->
          with {_ref, timer_ref, _} <- pending, do: Process.cancel_timer(timer_ref)
          ref = make_ref()
          timer_ref = Process.send_after(self(), {:timer, ref}, delay_ms)
          %{state | timer: {ref, timer_ref, fires_at}}
      end
    end
  end
end
