defmodule Docket.Runtime.Moment do
  @moduledoc """
  Substrate-neutral pre-commit value for exactly one runtime transition.

  Initialization, advancement, and graph signals each calculate one moment:
  the proposed `Docket.Run`, the runtime events already assigned from the
  run's sequences, the checkpoint type/metadata for the commit boundary, and
  an explicit disposition telling the driver what the run needs next.
  Calculating a moment performs no storage write, no checkpoint delivery,
  and no telemetry emission.

  A moment is not a committed `Docket.Checkpoint` and carries no storage
  vocabulary. Drivers own commitment:

  - A durable driver persists the proposed run and assigned events inside
    its outer storage transaction and, only after transaction success,
    builds the committed checkpoint with `checkpoint/1`/`context/2` and
    delivers observers and telemetry. A lost fence or failed event append
    discards the moment; no committed checkpoint value ever exists for a
    discarded moment, and observer failure after commit cannot change
    durable state.
  - The processless `Docket.Test` shell applies the moment directly and
    returns its checkpoint as a read-only assertion value. Nothing in the
    checkpoint path can veto the transition.

  Dispositions:

  | disposition | meaning |
  | --- | --- |
  | `:continue` | the run is advanceable now; propose the next moment |
  | `{:park, :immediate, reason}` | commit, then wake immediately |
  | `{:park, :external, reason}` | nothing dispatchable until an external signal (open interrupts) |
  | `{:park, {:at, timestamp}, reason}` | nothing dispatchable before `timestamp` (earliest retry deadline) |
  | `{:park, :terminal, reason}` | the run is terminal; it never wakes again |

  `{:park, :immediate, reason}` is reserved for driver yield boundaries and
  graph signals; resolving an interrupt produces this disposition unless
  every active attempt is parked behind a future retry deadline, in which
  case resolution parks at the earliest deadline.

  `checkpoint_metadata` is the JSON-safe identity envelope shared with the
  moment's `:checkpoint_committed` event. It records the checkpoint fence,
  checkpoint type, committed graph step, park reason, wake disposition, and
  any active retry-superstep/attempt identity.

  Disposition is decided by the runtime core; storage contracts receive
  only the schedule effect a lifecycle composer derives from it.
  """

  alias Docket.{Checkpoint, Event, Run}

  @enforce_keys [:run, :events, :checkpoint_type, :disposition, :proposed_at]
  defstruct [
    :run,
    :events,
    :checkpoint_type,
    :disposition,
    :proposed_at,
    pending_attempts: [],
    checkpoint_metadata: %{}
  ]

  @type park_kind :: :immediate | :external | {:at, DateTime.t()} | :terminal

  @type disposition :: :continue | {:park, park_kind(), term()}

  @type event_entry :: %{
          type: Event.type(),
          step: non_neg_integer(),
          node_id: String.t() | nil,
          channel_id: String.t() | nil,
          task_id: String.t() | nil,
          payload: map()
        }

  @type t :: %__MODULE__{
          run: Docket.Run.t(),
          events: [Docket.Event.t()],
          checkpoint_type: Checkpoint.type(),
          checkpoint_metadata: map(),
          disposition: disposition(),
          proposed_at: DateTime.t(),
          pending_attempts: [Docket.Run.PendingWrite.t()]
        }

  @doc false
  @spec propose(
          Run.t(),
          Checkpoint.type(),
          [event_entry()],
          disposition(),
          DateTime.t(),
          keyword()
        ) :: t()
  def propose(%Run{} = run, type, entries, disposition, %DateTime{} = now, opts \\ []) do
    run = %{run | checkpoint_seq: run.checkpoint_seq + 1}

    {runtime_events, event_seq} =
      Enum.map_reduce(entries, run.event_seq, fn entry, seq ->
        seq = seq + 1

        {%Event{
           run_id: run.id,
           seq: seq,
           type: entry.type,
           step: entry.step,
           node_id: entry.node_id,
           channel_id: entry.channel_id,
           task_id: entry.task_id,
           timestamp: now,
           payload: entry.payload
         }, seq}
      end)

    pending_attempts = Keyword.get(opts, :pending_attempts, [])
    metadata = checkpoint_metadata(run, runtime_events, type, disposition, pending_attempts)
    checkpoint_event_seq = event_seq + 1

    checkpoint_event = %Event{
      run_id: run.id,
      seq: checkpoint_event_seq,
      type: :checkpoint_committed,
      step: run.step,
      timestamp: now,
      metadata: metadata
    }

    %__MODULE__{
      run: %{run | event_seq: checkpoint_event_seq},
      events: runtime_events ++ [checkpoint_event],
      checkpoint_type: type,
      checkpoint_metadata: metadata,
      pending_attempts: pending_attempts,
      disposition: disposition,
      proposed_at: now
    }
  end

  @doc false
  @spec event_entry(Event.type(), non_neg_integer(), keyword()) :: event_entry()
  def event_entry(type, step, opts \\ []) do
    %{
      type: type,
      step: step,
      node_id: Keyword.get(opts, :node_id),
      channel_id: Keyword.get(opts, :channel_id),
      task_id: Keyword.get(opts, :task_id),
      payload: Keyword.get(opts, :payload, %{})
    }
  end

  @doc """
  Narrows a `:continue` moment to an immediate driver-yield park.

  This is the only sanctioned way for a driver to rewrite a proposed
  moment's disposition. It accepts exactly a structurally valid moment whose
  disposition is `:continue` and returns the same moment parked as
  `{:park, :immediate, reason}`, with the checkpoint metadata envelope and
  the single final `:checkpoint_committed` event's metadata rebuilt
  consistently for the new disposition.

  Everything else is preserved: the proposed run and its sequences, every
  event's sequence and timestamp, runtime event payloads, the checkpoint
  type (a yielded barrier keeps `:step_committed` or
  `:interrupt_requested`), pending-attempt identity, and `proposed_at`.

  A moment that already parks (including terminal) is never overridden and
  returns `{:error, {:not_continue, disposition}}`. A malformed moment -
  missing, duplicated, or misplaced final checkpoint event, or an envelope
  that does not match that event - returns `{:error, :malformed_moment}`.
  """
  @spec yield(t(), atom()) :: {:ok, t()} | {:error, term()}
  def yield(%__MODULE__{disposition: :continue} = moment, reason) when is_atom(reason) do
    case Enum.split(moment.events, -1) do
      {runtime_events, [%Event{type: :checkpoint_committed} = checkpoint_event]}
      when checkpoint_event.metadata == moment.checkpoint_metadata ->
        if Enum.any?(runtime_events, &(&1.type == :checkpoint_committed)) do
          {:error, :malformed_moment}
        else
          disposition = {:park, :immediate, reason}

          metadata =
            checkpoint_metadata(
              moment.run,
              runtime_events,
              moment.checkpoint_type,
              disposition,
              moment.pending_attempts
            )

          {:ok,
           %{
             moment
             | disposition: disposition,
               checkpoint_metadata: metadata,
               events: runtime_events ++ [%{checkpoint_event | metadata: metadata}]
           }}
        end

      _other ->
        {:error, :malformed_moment}
    end
  end

  def yield(%__MODULE__{disposition: disposition}, reason) when is_atom(reason),
    do: {:error, {:not_continue, disposition}}

  @doc """
  Builds the committed `Docket.Checkpoint` value for a moment.

  Production callers build this value only after the moment has durably
  committed. `Docket.Test` may build it while driving processless semantics.
  """
  @spec checkpoint(t()) :: Checkpoint.t()
  def checkpoint(%__MODULE__{} = moment) do
    %Checkpoint{
      type: moment.checkpoint_type,
      seq: moment.run.checkpoint_seq,
      run: moment.run,
      events: moment.events,
      created_at: moment.proposed_at,
      metadata: moment.checkpoint_metadata
    }
  end

  @doc """
  Builds the `Docket.Checkpoint.Context` for a moment's checkpoint.

  `application` is the host application context configured on the runtime.
  """
  @spec context(t(), map()) :: Checkpoint.Context.t()
  def context(%__MODULE__{} = moment, application \\ %{}) do
    identity = identity(moment.run, moment.events, moment.pending_attempts)

    %Checkpoint.Context{
      run_id: moment.run.id,
      graph_id: moment.run.graph_id,
      graph_hash: moment.run.graph_hash,
      checkpoint_seq: moment.run.checkpoint_seq,
      graph_step: moment.run.step,
      active_superstep: identity.active_superstep,
      node_attempts: identity.node_attempts,
      application: application
    }
  end

  @doc false
  @spec checkpoint_metadata(
          Docket.Run.t(),
          [Docket.Event.t()],
          Checkpoint.type(),
          disposition(),
          [Docket.Run.PendingWrite.t()]
        ) :: map()
  def checkpoint_metadata(
        run,
        runtime_events,
        checkpoint_type,
        disposition,
        new_pending_attempts \\ []
      ) do
    identity = identity(run, runtime_events, new_pending_attempts)
    {wake_disposition, park_reason} = disposition_metadata(disposition)

    %{
      "checkpoint_seq" => run.checkpoint_seq,
      "checkpoint_type" => Atom.to_string(checkpoint_type),
      "graph_step" => run.step,
      "park_reason" => park_reason,
      "wake_disposition" => wake_disposition,
      "active_superstep" => dump_active_superstep(identity.active_superstep),
      "node_attempts" => Enum.map(identity.node_attempts, &dump_node_attempt/1)
    }
  end

  defp identity(run, events, new_pending_writes) do
    active_tasks = active_tasks(run)
    pending_attempts = pending_attempts(run.pending_writes)
    new_pending_attempts = pending_attempts(new_pending_writes)

    %{
      active_superstep:
        case {active_tasks, pending_attempts} do
          {[], []} -> nil
          {tasks, pending} -> %{step: run.step, tasks: tasks, pending_attempts: pending}
        end,
      node_attempts: node_attempts(events, active_tasks, new_pending_attempts)
    }
  end

  defp active_tasks(run) do
    run.active_tasks
    |> Map.values()
    |> Enum.sort_by(&{&1.node_id, &1.task_id})
    |> Enum.map(fn task ->
      %{
        task_id: task.task_id,
        node_id: task.node_id,
        scheduled_attempt: task.attempt,
        idempotency_key: task.idempotency_key
      }
    end)
  end

  defp pending_attempts(pending_writes) do
    pending_writes
    |> Enum.sort_by(&{&1.node_id, &1.task_id})
    |> Enum.map(fn pending ->
      %{
        task_id: pending.task_id,
        node_id: pending.node_id,
        attempted: pending.attempt,
        kind: pending.kind,
        idempotency_key: Docket.Run.TaskState.idempotency_key(pending.task_id, pending.attempt)
      }
    end)
  end

  defp node_attempts(events, active_tasks, pending_attempts) do
    scheduled = Map.new(active_tasks, &{&1.task_id, &1.scheduled_attempt})

    committed =
      events
      |> Enum.filter(&(&1.type in [:node_completed, :node_failed, :interrupt_requested]))
      |> Enum.flat_map(fn event ->
        case event.payload do
          %{"attempt" => attempt} when is_integer(attempt) and attempt > 0 ->
            [
              %{
                task_id: event.task_id,
                node_id: event.node_id,
                attempted: attempt,
                outcome: attempt_outcome(event.type),
                next_scheduled_attempt: Map.get(scheduled, event.task_id)
              }
            ]

          _other ->
            []
        end
      end)

    committed_ids = MapSet.new(committed, & &1.task_id)

    pending =
      pending_attempts
      |> Enum.reject(&MapSet.member?(committed_ids, &1.task_id))
      |> Enum.map(fn pending ->
        %{
          task_id: pending.task_id,
          node_id: pending.node_id,
          attempted: pending.attempted,
          outcome: if(pending.kind == :update, do: :pending_update, else: :pending_interrupt),
          next_scheduled_attempt: nil
        }
      end)

    committed ++ pending
  end

  defp attempt_outcome(:node_completed), do: :completed
  defp attempt_outcome(:node_failed), do: :failed
  defp attempt_outcome(:interrupt_requested), do: :interrupted

  defp disposition_metadata(:continue), do: {"continue", nil}

  defp disposition_metadata({:park, :immediate, reason}),
    do: {"immediate", metadata_reason(reason)}

  defp disposition_metadata({:park, :external, reason}),
    do: {"external", metadata_reason(reason)}

  defp disposition_metadata({:park, {:at, %DateTime{}}, reason}),
    do: {"at", metadata_reason(reason)}

  defp disposition_metadata({:park, :terminal, reason}),
    do: {"terminal", metadata_reason(reason)}

  defp metadata_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp metadata_reason(reason) when is_binary(reason), do: reason
  defp metadata_reason(reason), do: inspect(reason)

  defp dump_active_superstep(nil), do: nil

  defp dump_active_superstep(%{step: step, tasks: tasks, pending_attempts: pending_attempts}) do
    %{
      "graph_step" => step,
      "tasks" =>
        Enum.map(tasks, fn task ->
          %{
            "task_id" => task.task_id,
            "node_id" => task.node_id,
            "scheduled_attempt" => task.scheduled_attempt,
            "idempotency_key" => task.idempotency_key
          }
        end),
      "pending_attempts" =>
        Enum.map(pending_attempts, fn pending ->
          %{
            "task_id" => pending.task_id,
            "node_id" => pending.node_id,
            "attempted" => pending.attempted,
            "kind" => Atom.to_string(pending.kind),
            "idempotency_key" => pending.idempotency_key
          }
        end)
    }
  end

  defp dump_node_attempt(attempt) do
    %{
      "task_id" => attempt.task_id,
      "node_id" => attempt.node_id,
      "attempted" => attempt.attempted,
      "outcome" => Atom.to_string(attempt.outcome),
      "next_scheduled_attempt" => attempt.next_scheduled_attempt
    }
  end
end
