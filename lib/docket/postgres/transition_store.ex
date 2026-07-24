if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.TransitionStore do
    @moduledoc """
    PostgreSQL implementation of the version-1 semantic transition contract.

    Claimed moments use the fused one-statement path. Initialization and
    unclaimed commits use backend-private PostgreSQL transactions; transaction
    handles and callbacks never cross the transition boundary into core.
    """

    @behaviour Docket.Backend.TransitionStore

    alias Docket.Backend.TransitionStore
    alias Docket.Postgres.{EventStore, MomentStore, RunStore, Storage}

    @limits TransitionStore.portable_limits()

    @impl true
    def initialize(ctx, owner_scope, proposal, events) do
      with :ok <- TransitionStore.validate_limits(proposal, events, @limits),
           :ok <- validate_transition_id(proposal.transition_id),
           %{run: %Docket.Run{} = run, checkpoint_type: checkpoint_type, wake_at: wake_at} <-
             proposal do
        Storage.transaction(ctx, fn tx ->
          with {:ok, stored} <-
                 RunStore.insert_run(tx, owner_scope, run, checkpoint_type, wake_at),
               :ok <- EventStore.append_events(tx, owner_scope, run.id, events) do
            {:ok, stored}
          end
        end)
        |> normalize_error()
      else
        {:error, reason} -> {:error, reason}
        _invalid -> {:error, :invalid_transition}
      end
    end

    @impl true
    def commit_claimed(ctx, scope, proposal, events) do
      with :ok <- TransitionStore.validate_limits(proposal, events, @limits),
           :ok <- validate_transition_id(proposal.transition_id) do
        proposal
        |> Map.delete(:transition_id)
        |> then(&MomentStore.commit(ctx, scope, &1, events))
        |> normalize_error()
      end
    end

    @impl true
    def commit_unclaimed(ctx, scope, expected_checkpoint_seq, proposal, events)
        when is_integer(expected_checkpoint_seq) and expected_checkpoint_seq >= 0 do
      with :ok <- TransitionStore.validate_limits(proposal, events, @limits),
           :ok <- validate_transition_id(proposal.transition_id),
           %{run: %Docket.Run{} = run} <- proposal do
        Storage.transaction(ctx, fn tx ->
          mutation = fn current ->
            if current.checkpoint_seq == expected_checkpoint_seq do
              {:commit, run, proposal.checkpoint_type, proposal.schedule, run}
            else
              {:error, :conflict}
            end
          end

          with {:ok, {:committed, stored}} <-
                 RunStore.mutate_run(tx, scope, run.id, mutation),
               :ok <- EventStore.append_events(tx, scope, run.id, events) do
            {:ok, stored}
          end
        end)
        |> normalize_error()
      else
        {:error, reason} -> {:error, reason}
        _invalid -> {:error, :invalid_transition}
      end
    end

    def commit_unclaimed(_ctx, _scope, _expected_checkpoint_seq, _proposal, _events),
      do: {:error, :invalid_transition}

    defp validate_transition_id(value) when is_binary(value) and byte_size(value) > 0, do: :ok
    defp validate_transition_id(_value), do: {:error, :invalid_transition}

    defp normalize_error({:error, reason})
         when reason in [
                :invalid_run,
                :invalid_commit,
                :invalid_mutation,
                :invalid_events,
                :invalid_event_sequence,
                :event_run_mismatch
              ],
         do: {:error, :invalid_transition}

    defp normalize_error({:error, :stale_fence}), do: {:error, :conflict}
    defp normalize_error({:error, :already_exists}), do: {:error, :conflict}
    defp normalize_error(result), do: result
  end
end
