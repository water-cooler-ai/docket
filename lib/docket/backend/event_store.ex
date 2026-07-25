defmodule Docket.Backend.EventStore do
  @moduledoc """
  Persistence contract for append-only run event reads.

  Event retention is a backend policy. Lifecycle orchestration publishes
  events through `Docket.Backend.TransitionStore`; this contract owns the
  focused reads over what a backend retained.
  """

  @type ctx :: Docket.Backend.ctx()
  @type scope :: Docket.Backend.scope()

  @doc """
  Reads one retained event by its assigned positive sequence.

  Ownership is enforced through the owning run. An unknown or out-of-scope
  run and an absent or pruned sequence all return `{:error, :not_found}`.
  A present corrupt row returns a typed
  `{:error, %Docket.Error{type: :corrupt_event_row}}` and is never reported as
  absent.

  `seq` is trusted to be pre-validated by the caller as a positive integer.
  """
  @callback fetch_event(
              ctx(),
              scope(),
              run_id :: String.t(),
              seq :: pos_integer()
            ) :: {:ok, Docket.Event.t()} | {:error, :not_found} | {:error, term()}

  @doc """
  Reads the highest-sequence retained event for a run.

  Ownership is enforced through the owning run. An unknown or out-of-scope
  run returns `{:error, :not_found}`. A visible run with no retained events
  returns `{:ok, nil}`, including when its complete history has been pruned.
  A present corrupt latest row returns a typed
  `{:error, %Docket.Error{type: :corrupt_event_row}}` and is never skipped in
  favor of an older event.
  """
  @callback fetch_latest_event(
              ctx(),
              scope(),
              run_id :: String.t()
            ) :: {:ok, Docket.Event.t() | nil} | {:error, :not_found} | {:error, term()}

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
