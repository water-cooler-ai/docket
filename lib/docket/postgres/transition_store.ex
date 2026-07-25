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

    alias Docket.Postgres.{
      EventStore,
      GraphStore,
      MomentStore,
      RunStore,
      Storage,
      TransitionError,
      TransitionReceipt
    }

    @limits TransitionStore.portable_limits()

    @impl true
    def initialize(ctx, owner_scope, proposal, events) do
      transition(fn ->
        with :ok <- TransitionStore.validate(:initialize, 0, proposal, events, @limits) do
          digest = TransitionStore.digest(:initialize, 0, proposal, events)
          result = TransitionStore.encode_result(proposal.run)

          Storage.transaction(ctx, fn tx ->
            case TransitionReceipt.reserve(
                   tx,
                   owner_scope,
                   :initialize,
                   proposal.transition_id,
                   digest,
                   result
                 ) do
              :replay ->
                {:ok, proposal.run}

              :fresh ->
                with :ok <- lock_run_id(tx, proposal.run.id),
                     {:ok, _graph} <-
                       GraphStore.fetch_graph(
                         tx,
                         owner_scope,
                         proposal.run.graph_id,
                         proposal.run.graph_hash
                       ),
                     :ok <- require_run_absent(tx, owner_scope, proposal.run.id),
                     {:ok, stored} <-
                       RunStore.insert_transition(
                         tx,
                         owner_scope,
                         proposal.run,
                         proposal.checkpoint_type,
                         proposal.wake_at
                       ),
                     :ok <-
                       EventStore.append_transition_events(
                         tx,
                         owner_scope,
                         proposal.run.id,
                         events
                       ) do
                  {:ok, stored}
                end

              {:error, reason} ->
                {:error, reason}
            end
          end)
        end
      end)
    end

    @impl true
    def commit_claimed(ctx, scope, proposal, events) do
      transition(fn ->
        expected_checkpoint_seq = Map.get(proposal, :expected_checkpoint_seq, -1)

        with :ok <-
               TransitionStore.validate(
                 :claimed,
                 expected_checkpoint_seq,
                 proposal,
                 events,
                 @limits
               ) do
          receipt = %{
            transition_id: proposal.transition_id,
            scope_key: TransitionReceipt.scope_key(scope),
            operation: :claimed,
            digest: TransitionStore.digest(:claimed, expected_checkpoint_seq, proposal, events),
            result: TransitionStore.encode_result(proposal.run),
            attempt_id: Ecto.UUID.generate()
          }

          proposal
          |> Map.delete(:transition_id)
          |> then(&MomentStore.commit(ctx, scope, &1, events, receipt))
        end
      end)
    end

    @impl true
    def commit_unclaimed(ctx, scope, expected_checkpoint_seq, proposal, events)
        when is_integer(expected_checkpoint_seq) and expected_checkpoint_seq >= 0 do
      transition(fn ->
        with :ok <-
               TransitionStore.validate(
                 :unclaimed,
                 expected_checkpoint_seq,
                 proposal,
                 events,
                 @limits
               ) do
          digest = TransitionStore.digest(:unclaimed, expected_checkpoint_seq, proposal, events)
          result = TransitionStore.encode_result(proposal.run)

          Storage.transaction(ctx, fn tx ->
            case TransitionReceipt.reserve(
                   tx,
                   scope,
                   :unclaimed,
                   proposal.transition_id,
                   digest,
                   result
                 ) do
              :replay ->
                {:ok, proposal.run}

              :fresh ->
                mutation = fn current ->
                  cond do
                    not immutable_binding?(current, proposal.run) ->
                      {:error, :invalid_transition}

                    current.checkpoint_seq != expected_checkpoint_seq ->
                      {:error, :stale_checkpoint}

                    current.event_seq != proposal.expected_event_seq ->
                      {:error, :stale_checkpoint}

                    true ->
                      {:commit, proposal.run, proposal.checkpoint_type, proposal.schedule,
                       proposal.run}
                  end
                end

                with {:ok, {:committed, stored}} <-
                       RunStore.mutate_transition(tx, scope, proposal.run.id, mutation),
                     :ok <-
                       EventStore.append_transition_events(
                         tx,
                         scope,
                         proposal.run.id,
                         events
                       ) do
                  {:ok, stored}
                end

              {:error, reason} ->
                {:error, reason}
            end
          end)
        end
      end)
    end

    def commit_unclaimed(_ctx, _scope, _expected_checkpoint_seq, _proposal, _events),
      do: {:error, :invalid_transition}

    defp transition(fun) do
      fun.()
      |> normalize_error()
    rescue
      error in [Postgrex.Error, DBConnection.ConnectionError] ->
        {:error, TransitionError.normalize(error)}
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
    defp normalize_error({:error, reason}), do: {:error, TransitionError.normalize(reason)}
    defp normalize_error({:ok, %Docket.Run{}} = result), do: result
    defp normalize_error(:ok), do: :ok

    defp require_run_absent(ctx, owner_scope, run_id) do
      case RunStore.fetch_run(ctx, :system, run_id) do
        {:error, :not_found} ->
          :ok

        {:ok, _run} ->
          case RunStore.fetch_run(ctx, owner_scope, run_id) do
            {:ok, _same_owner} -> {:error, :conflict}
            {:error, :not_found} -> {:error, :not_found}
          end
      end
    end

    defp lock_run_id(ctx, run_id) do
      {repo, _prefix} = Storage.context!(ctx)

      case repo.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [run_id],
             log: false
           ) do
        {:ok, _result} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end

    defp immutable_binding?(stored, proposed) do
      stored.id == proposed.id and stored.graph_id == proposed.graph_id and
        stored.graph_hash == proposed.graph_hash and stored.started_at == proposed.started_at
    end
  end
end
