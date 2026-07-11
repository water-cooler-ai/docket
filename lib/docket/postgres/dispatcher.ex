if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Dispatcher do
    @moduledoc false

    use GenServer

    @default_poll_interval_ms 1_000
    @default_drain_timeout_ms 30_000

    @type option ::
            {:name, GenServer.name()}
            | {:context, Docket.Storage.ctx()}
            | {:run_store, module()}
            | {:concurrency, pos_integer()}
            | {:poll_interval_ms, pos_integer()}
            | {:orphan_ttl_ms, non_neg_integer()}
            | {:max_claim_attempts, pos_integer()}
            | {:drain_timeout_ms, non_neg_integer()}
            | {:launch, (Docket.Storage.Runs.claim_lease() -> {:ok, pid()} | {:error, term()})}
            | {:on_poisoned, ([Docket.Storage.Runs.poisoned_claim()] -> term())}
            | {:clock, (-> DateTime.t())}
            | {:jitter, (pos_integer() -> non_neg_integer())}

    @spec start_link([option()]) :: GenServer.on_start()
    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
    end

    @doc "Requests an immediate claim poll. Bursts collapse to one pending poll."
    @spec request_poll(GenServer.server()) :: :ok
    def request_poll(dispatcher) do
      GenServer.cast(dispatcher, :request_poll)
    end

    def child_spec(opts) do
      drain_timeout = Keyword.get(opts, :drain_timeout_ms, @default_drain_timeout_ms)

      %{
        id: Keyword.get(opts, :name, __MODULE__),
        start: {__MODULE__, :start_link, [opts]},
        shutdown: drain_timeout + 1_000
      }
    end

    @impl true
    def init(opts) do
      Process.flag(:trap_exit, true)

      state = %{
        context: Keyword.fetch!(opts, :context),
        run_store: Keyword.get(opts, :run_store, Docket.Postgres.RunStore),
        concurrency: positive!(opts, :concurrency),
        poll_interval_ms: positive!(opts, :poll_interval_ms, @default_poll_interval_ms),
        orphan_ttl_ms: non_negative!(opts, :orphan_ttl_ms),
        max_claim_attempts: positive!(opts, :max_claim_attempts),
        drain_timeout_ms: non_negative!(opts, :drain_timeout_ms, @default_drain_timeout_ms),
        launch: Keyword.fetch!(opts, :launch),
        on_poisoned: Keyword.get(opts, :on_poisoned, fn _ -> :ok end),
        clock: Keyword.get(opts, :clock, &DateTime.utc_now/0),
        jitter: Keyword.get(opts, :jitter, &:rand.uniform/1),
        poll: nil,
        poll_pending?: false,
        poll_timer: nil,
        preference: :ready,
        vehicles: %{}
      }

      validate_callbacks!(state)
      {:ok, state, {:continue, :initial_poll}}
    end

    @impl true
    def handle_continue(:initial_poll, state), do: {:noreply, request_poll_now(state)}

    @impl true
    def handle_cast(:request_poll, state), do: {:noreply, request_poll_now(state)}

    @impl true
    def handle_info({:scheduled_poll, token}, %{poll_timer: {_timer, token}} = state) do
      {:noreply, request_poll_now(%{state | poll_timer: nil})}
    end

    def handle_info({:scheduled_poll, _stale_token}, state), do: {:noreply, state}

    def handle_info(
          {:claim_result, request_ref, result},
          %{poll: {_, _, request_ref, demand}} = state
        ) do
      state = finish_poll(state)
      state = alternate_preference(result, demand, state)
      state = consume_claim_result(result, state)
      {:noreply, resume_polling(state)}
    end

    def handle_info({:claim_result, _request_ref, _result}, state), do: {:noreply, state}

    def handle_info({:DOWN, monitor, :process, pid, _reason}, state) do
      cond do
        match?({^pid, ^monitor, _, _}, state.poll) ->
          state = %{state | poll: nil}
          {:noreply, resume_polling(state)}

        Map.get(state.vehicles, monitor) == pid ->
          state = %{state | vehicles: Map.delete(state.vehicles, monitor)}
          {:noreply, request_poll_now(state)}

        true ->
          {:noreply, state}
      end
    end

    # Launchers may use start_link. Vehicle demand is released by the monitor's
    # :DOWN, so linked exits are deliberately ignored here to avoid double work.
    def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

    @impl true
    def terminate(_reason, state) do
      cancel_timer(state.poll_timer)
      deadline = System.monotonic_time(:millisecond) + state.drain_timeout_ms
      state = settle_poll_for_shutdown(state, deadline)
      await_vehicles(state.vehicles, deadline)
      :ok
    end

    defp request_poll_now(%{poll: nil} = state), do: start_poll(cancel_poll_timer(state))
    defp request_poll_now(state), do: %{state | poll_pending?: true}

    defp resume_polling(%{poll_pending?: true} = state),
      do: start_poll(%{state | poll_pending?: false})

    defp resume_polling(state), do: schedule_poll(state)

    defp start_poll(state) do
      demand = max(state.concurrency - map_size(state.vehicles), 0)

      if demand == 0 do
        schedule_poll(state)
      else
        parent = self()
        request_ref = make_ref()
        policy = claim_policy(state, demand)

        {pid, monitor} =
          spawn_monitor(fn ->
            result = state.run_store.claim_due(state.context, :system, policy)
            send(parent, {:claim_result, request_ref, result})
          end)

        %{state | poll: {pid, monitor, request_ref, demand}}
      end
    end

    defp finish_poll(%{poll: {_pid, monitor, _request_ref, _demand}} = state) do
      Process.demonitor(monitor, [:flush])
      %{state | poll: nil}
    end

    # Demand-1 polls alternate the preferred candidate class across
    # consecutive successful polls so neither continuously eligible class
    # starves at concurrency one. The phase is process-local and resets on
    # restart.
    defp alternate_preference({:ok, _batch}, 1, %{preference: :ready} = state),
      do: %{state | preference: :expired}

    defp alternate_preference({:ok, _batch}, 1, %{preference: :expired} = state),
      do: %{state | preference: :ready}

    defp alternate_preference(_result, _demand, state), do: state

    defp consume_claim_result({:ok, %{leases: leases, poisoned: poisoned}}, state)
         when is_list(leases) and is_list(poisoned) do
      observe_poisoned(state.on_poisoned, poisoned)

      state = Enum.reduce(leases, state, &launch_lease/2)

      if poisoned != [] and map_size(state.vehicles) < state.concurrency,
        do: %{state | poll_pending?: true},
        else: state
    end

    defp consume_claim_result(_error, state), do: state

    defp launch_lease(lease, state) do
      if map_size(state.vehicles) >= state.concurrency do
        release_unlaunched(lease, state, :capacity_exhausted)
      else
        case launch(state.launch, lease) do
          {:ok, pid} when is_pid(pid) -> monitor_vehicle(pid, state)
          {:error, _reason} -> release_unlaunched(lease, state)
          other -> release_unlaunched(lease, state, {:invalid_return, other})
        end
      end
    end

    defp launch(callback, lease) do
      callback.(lease)
    rescue
      exception -> {:error, {:raised, exception, __STACKTRACE__}}
    catch
      kind, reason -> {:error, {kind, reason}}
    end

    defp release_unlaunched(lease, state, _reason \\ :launch_failed) do
      try do
        state.run_store.release_claim(
          state.context,
          :system,
          lease.run_id,
          lease.claim_token,
          state.clock.()
        )
      rescue
        _exception -> :ok
      catch
        _kind, _reason -> :ok
      end

      state
    end

    defp observe_poisoned(_callback, []), do: :ok

    defp observe_poisoned(callback, poisoned) do
      callback.(poisoned)
      :ok
    rescue
      _exception -> :ok
    catch
      _kind, _reason -> :ok
    end

    defp monitor_vehicle(pid, state) do
      monitor = Process.monitor(pid)
      %{state | vehicles: Map.put(state.vehicles, monitor, pid)}
    end

    defp claim_policy(state, demand) do
      %{
        now: state.clock.(),
        limit: demand,
        orphan_ttl_ms: state.orphan_ttl_ms,
        max_claim_attempts: state.max_claim_attempts,
        preference: state.preference
      }
    end

    defp schedule_poll(%{poll_timer: nil} = state) do
      delay = state.jitter.(state.poll_interval_ms) |> max(1) |> min(state.poll_interval_ms)
      token = make_ref()
      timer = Process.send_after(self(), {:scheduled_poll, token}, delay)
      %{state | poll_timer: {timer, token}}
    end

    defp schedule_poll(state), do: state

    defp cancel_poll_timer(state) do
      cancel_timer(state.poll_timer)
      %{state | poll_timer: nil}
    end

    defp cancel_timer(nil), do: :ok

    defp cancel_timer({timer, _token}),
      do: Process.cancel_timer(timer, async: false, info: false)

    defp settle_poll_for_shutdown(%{poll: nil} = state, _deadline), do: state

    defp settle_poll_for_shutdown(
           %{poll: {pid, monitor, request_ref, _demand}} = state,
           deadline
         ) do
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:claim_result, ^request_ref, {:ok, %{leases: leases, poisoned: poisoned}}}
        when is_list(leases) and is_list(poisoned) ->
          Process.demonitor(monitor, [:flush])
          observe_poisoned(state.on_poisoned, poisoned)
          state = Enum.reduce(leases, state, &release_unlaunched(&1, &2, :shutdown))
          %{state | poll: nil}

        {:claim_result, ^request_ref, _error} ->
          Process.demonitor(monitor, [:flush])
          %{state | poll: nil}

        {:DOWN, ^monitor, :process, ^pid, _reason} ->
          %{state | poll: nil}
      after
        remaining ->
          Process.demonitor(monitor, [:flush])
          Process.exit(pid, :kill)
          %{state | poll: nil}
      end
    end

    defp await_vehicles(vehicles, _deadline) when map_size(vehicles) == 0, do: :ok

    defp await_vehicles(vehicles, deadline) do
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:DOWN, monitor, :process, _pid, _reason} ->
          await_vehicles(Map.delete(vehicles, monitor), deadline)
      after
        remaining -> :ok
      end
    end

    defp positive!(opts, key, default \\ nil) do
      value = Keyword.get(opts, key, default)

      if is_integer(value) and value > 0,
        do: value,
        else: raise(ArgumentError, "#{inspect(key)} must be a positive integer")
    end

    defp non_negative!(opts, key, default \\ nil) do
      value = Keyword.get(opts, key, default)

      if is_integer(value) and value >= 0,
        do: value,
        else: raise(ArgumentError, "#{inspect(key)} must be a non-negative integer")
    end

    defp validate_callbacks!(state) do
      for {key, callback, arity} <- [
            {:launch, state.launch, 1},
            {:on_poisoned, state.on_poisoned, 1},
            {:clock, state.clock, 0},
            {:jitter, state.jitter, 1}
          ],
          not is_function(callback, arity) do
        raise ArgumentError, "#{inspect(key)} must be a function of arity #{arity}"
      end
    end
  end
end
