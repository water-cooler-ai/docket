defmodule Docket.Runtime.RunMutation do
  @moduledoc """
  Pure, named mutations of an already-committed `Docket.Run`.

  Each successful state change returns exactly one uncommitted
  `Docket.Runtime.Moment`. Callers supply the proposal timestamp explicitly,
  so calculation is deterministic and performs no storage, process,
  checkpoint-handler, telemetry, or scheduling work.

  An already-cancelled run is an idempotent success and is returned unchanged;
  there is no new moment to commit.
  """

  alias Docket.{Error, Run, Schema, Wire}
  alias Docket.Run.{InterruptState, PendingWrite, TaskState, TimerState}
  alias Docket.Runtime.{Algorithm, Graph, Moment}

  @type mutation_result :: {:ok, Moment.t()} | {:error, Error.t()}
  @type cancellation_result :: mutation_result() | {:unchanged, Run.t()}
  @type completion_result :: mutation_result() | {:unchanged, Run.t()}

  @durable_active [:running, :waiting]

  @doc """
  Resolves an open interrupt and proposes the run's next wake.

  Terminal status is checked before interrupt lookup. A resolved interrupt is
  distinguished from an unknown interrupt, and the stored schema plus graph
  field contract both validate the resolution value.

  Resolution proposes an immediate wake unless every active attempt is
  parked behind a future retry deadline; then it parks at the earliest
  deadline, so the committed wake never precedes the first dispatchable
  attempt.
  """
  @spec resolve_interrupt(Graph.t(), Run.t(), String.t(), term(), DateTime.t()) ::
          mutation_result()
  def resolve_interrupt(rtg, %Run{} = run, interrupt_id, value, %DateTime{} = now) do
    cond do
      Run.terminal?(run) ->
        inactive_run(run, "resumed")

      run.status not in @durable_active ->
        non_durable_run(run)

      match?({:ok, %InterruptState{status: :resolved}}, Map.fetch(run.interrupts, interrupt_id)) ->
        {:error, Error.new(:already_resolved, "interrupt #{inspect(interrupt_id)} is resolved")}

      not match?({:ok, %InterruptState{status: :open}}, Map.fetch(run.interrupts, interrupt_id)) ->
        {:error, Error.new(:not_found, "no interrupt #{inspect(interrupt_id)}")}

      true ->
        interrupt = Map.fetch!(run.interrupts, interrupt_id)

        with {:ok, value} <- durable_resolution(value),
             :ok <- validate_resolution_schema(interrupt, value),
             {:ok, update} <- validate_resolution_write(rtg, interrupt, value) do
          {channels, changed_fields, _writers} =
            Algorithm.apply_state_writes(rtg, run.channels, [{interrupt.node_id, update}])

          resolved = %{interrupt | status: :resolved, resolved_at: now}
          changed_channel_ids = Enum.map(changed_fields, &("state:" <> &1))

          proposed = %{
            run
            | channels: channels,
              changed_channels:
                Enum.reduce(changed_channel_ids, run.changed_channels, &MapSet.put(&2, &1)),
              interrupts: Map.put(run.interrupts, interrupt.id, resolved),
              status: :running,
              updated_at: now
          }

          entries =
            [
              entry(:interrupt_resolved, proposed.step,
                node_id: interrupt.node_id,
                payload: %{
                  "interrupt_id" => interrupt.id,
                  "resume_channel" => interrupt.resume_channel
                }
              )
            ] ++
              Enum.map(changed_channel_ids, fn channel_id ->
                entry(:channel_updated, proposed.step,
                  channel_id: channel_id,
                  payload: %{"writers" => [interrupt.node_id]}
                )
              end)

          {:ok,
           Moment.propose(
             proposed,
             :interrupt_resolved,
             entries,
             resolution_disposition(proposed, now, :interrupt_resolved),
             now
           )}
        end
    end
  end

  @doc """
  Applies a detached attempt's late result and proposes the run's next wake.

  The mutation is fenced on the run still holding exactly `task_id` detached
  at exactly `attempt` with no result yet recorded: a terminal run, a task
  that is absent, no longer `:detached`, at a different attempt, or already
  carrying a reported failure all return `{:unchanged, run}` — a stale,
  duplicate, or superseded result changes nothing and consumes no
  sequences. The fence re-evaluates identically under signal retry, so
  exactly one completion per detached attempt applies to durable state.

  One premature case is distinguished from staleness: a completion for a
  task in the run's **current** step that is neither parked nor pending has
  arrived before its detach park committed and returns
  `{:error, %Docket.Error{type: :detach_pending}}` so the caller can retry
  instead of silently losing the result.

  A `{:ok, update}` result validates against the graph like a node return
  and parks as a pending write at the same attempt; the next advancement
  absorbs it at the update barrier. A `{:error, reason}` result expires the
  task's deadline immediately and records the reason; the deadline
  disposition policy (reschedule or fail) settles it on the next
  advancement. External effects remain at-least-once either way.
  """
  @spec complete_detached(
          Graph.t(),
          Run.t(),
          String.t(),
          pos_integer(),
          {:ok, map()} | {:error, term()},
          DateTime.t()
        ) :: completion_result()
  def complete_detached(rtg, %Run{} = run, task_id, attempt, result, %DateTime{} = now) do
    cond do
      Run.terminal?(run) ->
        {:unchanged, run}

      run.status not in @durable_active ->
        non_durable_run(run)

      true ->
        case Map.fetch(run.active_tasks, task_id) do
          {:ok, %TaskState{status: :detached, attempt: ^attempt} = task} ->
            if Map.has_key?(task.metadata, "detach_error"),
              do: {:unchanged, run},
              else: apply_detached_result(rtg, run, task, result, now)

          _other ->
            premature_or_stale(run, task_id)
        end
    end
  end

  # A completion misses the fence either because it is stale (the attempt
  # was absorbed, superseded, or rescheduled) or because it arrived before
  # its detach park committed. Only the current step with no parked task and
  # no pending write can be the premature case; it is retryable, never a
  # silent no-op, so a fast worker's result cannot be lost to the window
  # between dispatch and the park commit.
  defp premature_or_stale(run, task_id) do
    current_step? = String.starts_with?(task_id, "#{run.id}:#{run.step}:")
    pending? = Enum.any?(run.pending_writes, &(&1.task_id == task_id))

    if current_step? and not pending? and not Map.has_key?(run.active_tasks, task_id) do
      {:error,
       Error.new(
         :detach_pending,
         "task #{inspect(task_id)} is not detached yet; retry after the detach park commits"
       )}
    else
      {:unchanged, run}
    end
  end

  defp apply_detached_result(rtg, run, task, {:ok, update}, now) do
    with {:ok, update} <- durable_detached_update(update),
         {:ok, validated} <- validate_detached_write(rtg, task, update) do
      pending = %PendingWrite{
        task_id: task.task_id,
        node_id: task.node_id,
        attempt: task.attempt,
        kind: :update,
        value: validated
      }

      proposed = %{
        run
        | active_tasks: Map.delete(run.active_tasks, task.task_id),
          timers: Map.delete(run.timers, task.task_id),
          pending_writes: run.pending_writes ++ [pending],
          status: :running,
          updated_at: now
      }

      {:ok,
       Moment.propose(
         proposed,
         :detach_resolved,
         [],
         resolution_disposition(proposed, now, :detach_resolved),
         now
       )}
    end
  end

  # A reported failure does not settle the attempt here: it expires the
  # deadline in place and lets the next advancement dispose per the node's
  # deadline policy, so timeout and reported failure share one machine.
  defp apply_detached_result(_rtg, run, task, {:error, reason}, now) do
    expired = %{
      task
      | deadline_at: now,
        metadata: Map.put(task.metadata, "detach_error", failure_reason(reason))
    }

    proposed = %{
      run
      | active_tasks: Map.put(run.active_tasks, task.task_id, expired),
        timers:
          Map.put(run.timers, task.task_id, %TimerState{kind: :detached_deadline, fires_at: now}),
        status: :running,
        updated_at: now
    }

    {:ok,
     Moment.propose(
       proposed,
       :detach_resolved,
       [],
       resolution_disposition(proposed, now, :detach_resolved),
       now
     )}
  end

  defp apply_detached_result(_rtg, _run, _task, other, _now) do
    {:error,
     Error.new(
       :invalid_input,
       "a detached result must be {:ok, update} or {:error, reason}, got #{inspect(other)}"
     )}
  end

  defp failure_reason(reason) when is_binary(reason), do: reason
  defp failure_reason(reason), do: inspect(reason)

  defp durable_detached_update(update) do
    case Wire.dump_value(update) do
      {:ok, coerced} when is_map(coerced) ->
        {:ok, coerced}

      {:ok, other} ->
        {:error,
         Error.new(:invalid_input, "a detached update must be a map, got #{inspect(other)}")}

      {:error, reason} ->
        {:error, Error.new(:invalid_input, "detached result value is not durable: #{reason}")}
    end
  end

  defp validate_detached_write(rtg, task, update) do
    case Algorithm.validate_state_update(rtg, task.node_id, update) do
      {:ok, validated} ->
        {:ok, validated}

      {:error, reasons} ->
        {:error,
         Error.new(:invalid_input, "detached result value is invalid",
           details: %{reasons: reasons}
         )}
    end
  end

  @doc """
  Cancels a durable active run and proposes a terminal park.

  Cancellation absorbs any parked superstep. Calling this again for the
  resulting run returns that exact run without consuming sequences.
  """
  @spec cancel_run(Run.t(), DateTime.t()) :: cancellation_result()
  def cancel_run(%Run{status: :cancelled} = run, %DateTime{}), do: {:unchanged, run}

  def cancel_run(%Run{status: status} = run, %DateTime{} = now)
      when status in @durable_active do
    proposed = %{
      run
      | status: :cancelled,
        finished_at: now,
        updated_at: now,
        active_tasks: %{},
        pending_writes: [],
        timers: %{}
    }

    entries = [entry(:run_cancelled, proposed.step, payload: %{})]

    {:ok,
     Moment.propose(proposed, :run_cancelled, entries, {:park, :terminal, :run_cancelled}, now)}
  end

  def cancel_run(%Run{status: status} = run, %DateTime{}) when status in [:done, :failed],
    do: inactive_run(run, "cancelled")

  def cancel_run(%Run{} = run, %DateTime{}), do: non_durable_run(run)

  # An active attempt without a timer is dispatchable now; committed parks
  # always write one timer per active task.
  defp resolution_disposition(%Run{active_tasks: tasks} = run, now, reason)
       when map_size(tasks) > 0 do
    deadlines =
      Enum.map(tasks, fn {task_id, _task} ->
        case Map.fetch(run.timers, task_id) do
          {:ok, %TimerState{fires_at: fires_at}} -> fires_at
          :error -> nil
        end
      end)

    if Enum.any?(deadlines, &(is_nil(&1) or DateTime.compare(&1, now) != :gt)) do
      {:park, :immediate, reason}
    else
      {:park, {:at, Enum.min(deadlines, DateTime)}, reason}
    end
  end

  defp resolution_disposition(%Run{}, _now, reason), do: {:park, :immediate, reason}

  defp durable_resolution(value) do
    case Wire.dump_value(value) do
      {:ok, coerced} ->
        {:ok, coerced}

      {:error, reason} ->
        {:error,
         Error.new(:invalid_input, "interrupt resolution value is not durable: #{reason}")}
    end
  end

  defp validate_resolution_schema(%InterruptState{schema: nil}, _value), do: :ok

  defp validate_resolution_schema(%InterruptState{schema: schema}, value) do
    case Schema.validate(schema, value) do
      :ok -> :ok
      {:error, reasons} -> {:error, invalid_resolution(reasons)}
    end
  end

  defp validate_resolution_write(rtg, interrupt, value) do
    case Algorithm.validate_state_update(rtg, interrupt.node_id, %{
           interrupt.resume_channel => value
         }) do
      {:ok, update} -> {:ok, update}
      {:error, reasons} -> {:error, invalid_resolution(reasons)}
    end
  end

  defp invalid_resolution(reasons) do
    Error.new(:invalid_input, "interrupt resolution value is invalid",
      details: %{reasons: reasons}
    )
  end

  defp inactive_run(run, action) do
    {:error,
     Error.new(:inactive_run, "run #{inspect(run.id)} is #{run.status} and cannot be #{action}")}
  end

  defp non_durable_run(run) do
    {:error,
     Error.new(
       :invalid_run,
       "run #{inspect(run.id)} has non-durable status #{inspect(run.status)}"
     )}
  end

  defp entry(type, step, opts) do
    Moment.event_entry(type, step, opts)
  end
end
