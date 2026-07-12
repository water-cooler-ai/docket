defmodule Docket.Storage.Events do
  @moduledoc """
  Persistence contract for append-only run events.

  Event retention is a backend policy. Lifecycle orchestration calls this
  store in the same backend transaction as the corresponding run
  insert or commit so durable state and its retained facts cannot diverge.
  """

  @type ctx :: Docket.Storage.ctx()
  @type scope :: Docket.Storage.scope()

  @doc """
  Appends retained events for `run_id` in sequence order.

  The operation must be idempotent for an already-persisted `{run_id, seq}`
  containing the same event. Conflicting content for that key is an error.
  Every event must belong to `run_id`; a backend must reject a mismatched
  event rather than storing it under the supplied run.

  Event identities are assigned before this callback. The store never derives
  a sequence from the maximum already-stored sequence, never substitutes the
  run's checkpoint sequence, and accepts gaps left by persistence filtering. In particular,
  `:checkpoint_committed` is an ordinary assigned event at this boundary.

  For a non-empty append, `scope` is checked through the owning run and a
  tenant mismatch is reported as `{:error, :not_found}`. An empty event list
  validates the scope value but succeeds without a run lookup or storage
  change.
  """
  @callback append_events(
              ctx(),
              scope(),
              run_id :: String.t(),
              events :: [Docket.Event.t()]
            ) :: :ok | {:error, term()}

  @doc """
  Reads a page of committed retained events for `run_id`.

  Events are returned in ascending sequence order, restricted to sequences
  greater than `opts.after_seq` and limited to at most `opts.limit` rows. This
  keyset scan skips pruned and persistence-filtered sequences, so a page and
  the retention bounds are not promised contiguous.

  Ownership is enforced through the owning run: a wrong tenant and an unknown
  run both report `{:error, :not_found}`. The page rows and the returned
  retention bounds are observed from one consistent snapshot. A corrupt or
  undecodable stored row is a typed error and is never silently skipped: a
  corrupt event row returns `{:error, %Docket.Error{type: :corrupt_event_row}}`,
  while a corrupt owning-run row propagates the same way as run reads, by
  raising the typed error.

  Options are trusted to be pre-validated by the caller: `after_seq` is a
  non-negative integer and `limit` is a positive integer. A backend may assert
  these rather than validate them.
  """
  @callback list_events(
              ctx(),
              scope(),
              run_id :: String.t(),
              opts :: %{
                required(:after_seq) => non_neg_integer(),
                required(:limit) => pos_integer()
              }
            ) :: {:ok, Docket.EventPage.t()} | {:error, :not_found} | {:error, term()}
end
