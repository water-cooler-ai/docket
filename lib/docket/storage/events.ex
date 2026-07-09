defmodule Docket.Storage.Events do
  @moduledoc """
  Persistence contract for append-only run events.

  Event retention is a backend policy. Lifecycle orchestration calls this
  store in the same `Docket.Storage.transaction/2` as the corresponding run
  insert or commit so durable state and its retained facts cannot diverge.
  """

  @type ctx :: Docket.Storage.ctx()

  @doc """
  Appends retained events for `run_id` in sequence order.

  The operation must be idempotent for an already-persisted `{run_id, seq}`
  containing the same event. Conflicting content for that key is an error.
  Every event must belong to `run_id`; a backend must reject a mismatched
  event rather than storing it under the supplied run.

  An empty event list succeeds without changing storage. `opts` may carry
  backend-independent scoping such as `:tenant_id`.
  """
  @callback append_events(
              ctx(),
              run_id :: String.t(),
              events :: [Docket.Event.t()],
              opts :: keyword()
            ) :: :ok | {:error, term()}
end
