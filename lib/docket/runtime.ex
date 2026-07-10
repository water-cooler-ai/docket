defmodule Docket.Runtime do
  @moduledoc """
  GenServer shell owning one active run.

  The Runtime is the only process allowed to mutate a run. It holds the
  compiled runtime graph and the current committed `Docket.Run` in memory,
  drives shared runtime-loop transitions on self-scheduled ticks, and
  submits async checkpoint effects to the runtime instance's task
  supervisor. All graph semantics live in the shared loop; this module owns
  only mailbox, lifecycle, and delivery mechanics.

  Runtimes are started through `Docket.run/4` / `Docket.resume/4` and are
  addressed by runtime instance and `run_id`; PIDs never leave the library.
  A Runtime exits normally once its run is terminal and all async
  checkpoint deliveries have settled, after which `Docket.get_run/3`
  returns `:not_found`.
  """

  use GenServer, restart: :temporary

  require Logger

  alias Docket.{Error, Run}
  alias Docket.Runtime.{Config, Dispatcher, Loop}

  @doc false
  def start_link({rtg, run, opts, reply_to}) do
    GenServer.start_link(__MODULE__, {rtg, run, opts, reply_to}, name: Keyword.get(opts, :name))
  end

  @doc false
  def get_run(pid), do: GenServer.call(pid, :get_run)

  @doc false
  def resolve_interrupt(pid, interrupt_id, value) do
    GenServer.call(pid, {:resolve_interrupt, interrupt_id, value})
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init({rtg, run, opts, reply_to}) do
    case Loop.init(rtg, run, opts) do
      {:ok, run, effects} ->
        state = %{
          rtg: rtg,
          run: run,
          opts: opts,
          config: Config.resolve(opts),
          task_supervisor: Keyword.get(opts, :task_supervisor),
          pending_deliveries: %{},
          draining?: false
        }

        state = deliver_effects(state, effects)

        # Sent before init returns, so the message is guaranteed to be in the
        # caller's mailbox by the time start_child returns {:ok, pid}.
        notify_started(reply_to, run)

        {:ok, state, {:continue, :after_init}}

      {:error, %Error{} = error} ->
        # :shutdown keeps the failed start out of crash reports; the caller
        # receives {:error, {:shutdown, error}} from start_child.
        {:stop, {:shutdown, error}}
    end
  end

  defp notify_started(nil, _run), do: :ok
  defp notify_started({caller, ref}, run), do: send(caller, {ref, {:ok, run}})

  @impl true
  def handle_continue(:after_init, state) do
    if Run.terminal?(state.run) do
      finish(state)
    else
      schedule_tick()
      {:noreply, state}
    end
  end

  @impl true
  def handle_call(:get_run, _from, state) do
    {:reply, {:ok, state.run}, state}
  end

  def handle_call({:resolve_interrupt, interrupt_id, value}, _from, state) do
    case Loop.resolve_interrupt(state.rtg, state.run, interrupt_id, value, state.opts) do
      {:ok, run, effects} ->
        state = deliver_effects(%{state | run: run}, effects)
        schedule_tick()
        {:reply, {:ok, run}, state}

      {:error, %Error{} = error} ->
        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    if Run.terminal?(state.run) do
      finish(state)
    else
      tick(state)
    end
  end

  # Async checkpoint delivery task completed.
  def handle_info({ref, result}, state) when is_map_key(state.pending_deliveries, ref) do
    Process.demonitor(ref, [:flush])
    {type, state} = pop_delivery(state, ref)

    case result do
      :ok -> :ok
      {:error, reason} -> log_delivery_failure(state.run, type, reason)
    end

    maybe_stop_after_drain(state)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state)
      when is_map_key(state.pending_deliveries, ref) do
    {type, state} = pop_delivery(state, ref)
    log_delivery_failure(state.run, type, {:exited, reason})
    maybe_stop_after_drain(state)
  end

  # Stale or unknown messages (for example late completions from an executor
  # task that already timed out) are ignored: results only enter the run
  # through the barrier that dispatched them.
  def handle_info(_message, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Tick loop
  # ---------------------------------------------------------------------------

  defp tick(state) do
    case Loop.plan(state.rtg, state.run, state.opts) do
      {:execute, run, activations} ->
        results = Dispatcher.dispatch(activations, state.rtg, run, state.config)

        case Loop.apply_results(state.rtg, run, activations, results, state.opts) do
          {:ok, run, effects} ->
            state = deliver_effects(%{state | run: run}, effects)

            if Run.terminal?(run) do
              finish(state)
            else
              schedule_tick()
              {:noreply, state}
            end

          {:park, run, park, effects} ->
            state = deliver_effects(%{state | run: run}, effects)
            schedule_tick_after(park.wait_ms)
            {:noreply, state}

          {:error, %Error{} = error} ->
            # Sync checkpoint failure: the previous committed run remains the
            # durable truth; stop and let the host resume, re-executing the
            # uncommitted superstep with identical idempotency keys.
            {:stop, {:shutdown, error}, state}
        end

      {:wait, run, _interrupt_ids} ->
        # Blocked on open interrupts; resolve_interrupt schedules the next tick.
        {:noreply, %{state | run: run}}

      {:park, run, park} ->
        # Parked mid-superstep for a retry deadline. The mailbox stays live:
        # get_run and resolve_interrupt are served during backoff, and an
        # early tick simply parks again with the remaining wait.
        schedule_tick_after(park.wait_ms)
        {:noreply, %{state | run: run}}

      {:terminal, run, effects} ->
        finish(deliver_effects(%{state | run: run}, effects))

      {:error, %Error{} = error} ->
        {:stop, {:shutdown, error}, state}
    end
  end

  defp schedule_tick, do: send(self(), :tick)

  # Erlang timers cap out below extreme retry deadlines; a clamped early
  # tick just parks again with the remaining wait.
  @max_timer_ms 4_294_967_295

  defp schedule_tick_after(0), do: schedule_tick()

  defp schedule_tick_after(ms) do
    Process.send_after(self(), :tick, min(ms, @max_timer_ms))
  end

  # Terminal run: exit normally once every async delivery has settled, so the
  # final checkpoints are never abandoned mid-flight.
  defp finish(state) do
    maybe_stop_after_drain(%{state | draining?: true})
  end

  defp maybe_stop_after_drain(state) do
    if state.draining? and map_size(state.pending_deliveries) == 0 do
      {:stop, :normal, state}
    else
      {:noreply, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Checkpoint effect delivery
  # ---------------------------------------------------------------------------

  # Sync checkpoints were already accepted inside the loop transition. Async
  # ones are submitted to the instance task supervisor and tracked; a failed
  # or crashed delivery is logged without blocking the active run.
  defp deliver_effects(state, effects) do
    Enum.reduce(effects, state, fn
      {:checkpoint, _checkpoint, _context, :accepted}, state ->
        state

      {:checkpoint, checkpoint, context, :pending}, state ->
        deliver_async(state, checkpoint, context)
    end)
  end

  defp deliver_async(%{task_supervisor: nil} = state, checkpoint, context) do
    case Loop.deliver_checkpoint(state.config.checkpoint, checkpoint, context) do
      :ok -> :ok
      {:error, reason} -> log_delivery_failure(state.run, checkpoint.type, reason)
    end

    state
  end

  defp deliver_async(state, checkpoint, context) do
    sink = state.config.checkpoint

    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        Loop.deliver_checkpoint(sink, checkpoint, context)
      end)

    put_in(state.pending_deliveries[task.ref], checkpoint.type)
  end

  defp pop_delivery(state, ref) do
    {type, pending} = Map.pop(state.pending_deliveries, ref)
    {type, %{state | pending_deliveries: pending}}
  end

  defp log_delivery_failure(run, type, reason) do
    Logger.warning(
      "Docket async #{inspect(type)} checkpoint delivery failed for run " <>
        "#{inspect(run.id)}: #{inspect(reason)}"
    )
  end
end
