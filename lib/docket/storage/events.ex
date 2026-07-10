defmodule Docket.Storage.Events do
  @moduledoc """
  Persistence contract for append-only run events.

  Event retention is a backend policy. Lifecycle orchestration calls this
  store in the same `Docket.Storage.transaction/2` as the corresponding run
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
end
