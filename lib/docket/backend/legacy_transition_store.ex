defmodule Docket.Backend.LegacyTransitionStore do
  @moduledoc """
  0.1.x compatibility adapter for backends that have not declared transitions.

  The adapter implements semantic operations through the deprecated public
  transaction/store composition contract. It cannot strengthen the legacy
  backend's replay, durability, or portability guarantees. New backends should
  declare transition contract v1 and implement
  `Docket.Backend.TransitionStore` directly.
  """

  @behaviour Docket.Backend.TransitionStore

  @impl true
  def initialize(
        {backend, context},
        owner_scope,
        %{run: %Docket.Run{} = run, checkpoint_type: checkpoint_type, wake_at: wake_at} =
          proposal,
        events
      ) do
    with :ok <-
           Docket.Backend.TransitionStore.validate(
             :initialize,
             0,
             proposal,
             events,
             Docket.Backend.TransitionStore.portable_limits()
           ) do
      runs = backend.runs()
      event_store = backend.events()

      apply(backend, :transaction, [
        context,
        fn tx ->
          with {:ok, stored} <-
                 apply(runs, :insert_run, [tx, owner_scope, run, checkpoint_type, wake_at]),
               :ok <- apply(event_store, :append_events, [tx, owner_scope, run.id, events]) do
            {:ok, stored}
          end
        end
      ])
      |> normalize_error()
    end
  end

  def initialize(_context, _owner_scope, _proposal, _events),
    do: {:error, :invalid_transition}

  @impl true
  def commit_claimed(
        {backend, context},
        scope,
        %{run: %Docket.Run{} = run} = proposal,
        events
      ) do
    with :ok <-
           Docket.Backend.TransitionStore.validate(
             :claimed,
             Map.get(proposal, :expected_checkpoint_seq, -1),
             proposal,
             events,
             Docket.Backend.TransitionStore.portable_limits()
           ) do
      runs = backend.runs()
      event_store = backend.events()
      legacy_proposal = Map.delete(proposal, :transition_id)

      apply(backend, :transaction, [
        context,
        fn tx ->
          with {:ok, stored} <- apply(runs, :commit, [tx, scope, legacy_proposal]),
               :ok <- apply(event_store, :append_events, [tx, scope, run.id, events]) do
            {:ok, stored}
          end
        end
      ])
      |> normalize_error()
    end
  end

  def commit_claimed(_context, _scope, _proposal, _events),
    do: {:error, :invalid_transition}

  @impl true
  def commit_unclaimed(
        {backend, context},
        scope,
        expected_checkpoint_seq,
        %{run: %Docket.Run{} = run} = proposal,
        events
      )
      when is_integer(expected_checkpoint_seq) and expected_checkpoint_seq >= 0 do
    with :ok <-
           Docket.Backend.TransitionStore.validate(
             :unclaimed,
             expected_checkpoint_seq,
             proposal,
             events,
             Docket.Backend.TransitionStore.portable_limits()
           ) do
      runs = backend.runs()
      event_store = backend.events()

      apply(backend, :transaction, [
        context,
        fn tx ->
          mutation = fn current ->
            cond do
              not immutable_binding?(current, run) ->
                {:error, :invalid_transition}

              current.checkpoint_seq != expected_checkpoint_seq ->
                {:error, :stale_checkpoint}

              current.event_seq != proposal.expected_event_seq ->
                {:error, :stale_checkpoint}

              true ->
                {:commit, run, proposal.checkpoint_type, proposal.schedule, run}
            end
          end

          with {:ok, {:committed, stored}} <-
                 apply(runs, :mutate_run, [tx, scope, run.id, mutation]),
               :ok <- apply(event_store, :append_events, [tx, scope, run.id, events]) do
            {:ok, stored}
          end
        end
      ])
      |> normalize_error()
    end
  end

  def commit_unclaimed(_context, _scope, _expected_checkpoint_seq, _proposal, _events),
    do: {:error, :invalid_transition}

  defp immutable_binding?(stored, proposed) do
    stored.id == proposed.id and stored.graph_id == proposed.graph_id and
      stored.graph_hash == proposed.graph_hash and stored.started_at == proposed.started_at
  end

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

  defp normalize_error({:error, :stale_fence}), do: {:error, :stale_checkpoint}
  defp normalize_error({:error, :already_exists}), do: {:error, :conflict}
  defp normalize_error(result), do: result
end
