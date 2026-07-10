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
  alias Docket.Run.InterruptState
  alias Docket.Runtime.{Algorithm, Moment}

  @type result :: {:ok, Moment.t() | Run.t()} | {:error, Error.t()}

  @doc """
  Resolves an open interrupt and proposes an immediate wake.

  Terminal status is checked before interrupt lookup. A resolved interrupt is
  distinguished from an unknown interrupt, and the stored schema plus graph
  field contract both validate the resolution value.
  """
  @spec resolve_interrupt(term(), Run.t(), String.t(), term(), DateTime.t()) :: result()
  def resolve_interrupt(rtg, %Run{} = run, interrupt_id, value, %DateTime{} = now) do
    cond do
      Run.terminal?(run) ->
        inactive_run(run, "resumed")

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
           moment(
             proposed,
             :interrupt_resolved,
             entries,
             {:park, :immediate, :interrupt_resolved},
             now
           )}
        end
    end
  end

  @doc """
  Cancels a durable active run and proposes a terminal park.

  Cancellation absorbs any parked superstep. Calling this again for the
  resulting run returns that exact run without consuming sequences.
  """
  @spec cancel_run(Run.t(), DateTime.t()) :: result()
  def cancel_run(%Run{status: :cancelled} = run, %DateTime{}), do: {:ok, run}

  def cancel_run(%Run{status: status} = run, %DateTime{} = now)
      when status in [:running, :waiting] do
    proposed = %{
      run
      | status: :cancelled,
        failure: nil,
        finished_at: now,
        updated_at: now,
        active_tasks: %{},
        pending_writes: [],
        timers: %{}
    }

    entries = [entry(:run_cancelled, proposed.step, payload: %{})]

    {:ok, moment(proposed, :run_cancelled, entries, {:park, :terminal, :run_cancelled}, now)}
  end

  def cancel_run(%Run{status: status} = run, %DateTime{}) when status in [:done, :failed],
    do: inactive_run(run, "cancelled")

  def cancel_run(%Run{} = run, %DateTime{}) do
    {:error,
     Error.new(
       :invalid_run,
       "run #{inspect(run.id)} has non-durable status #{inspect(run.status)}"
     )}
  end

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

  defp entry(type, step, opts) do
    %{
      type: type,
      step: step,
      node_id: Keyword.get(opts, :node_id),
      channel_id: Keyword.get(opts, :channel_id),
      task_id: Keyword.get(opts, :task_id),
      payload: Keyword.get(opts, :payload, %{})
    }
  end

  defp moment(run, type, entries, disposition, now) do
    Moment.propose(run, type, entries, disposition, now)
  end
end
