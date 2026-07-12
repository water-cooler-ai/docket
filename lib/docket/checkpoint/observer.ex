defmodule Docket.Checkpoint.Observer do
  @moduledoc """
  Best-effort notification of an already-committed durable checkpoint.

  Production observers are configured with `checkpoint_observers:`. The
  separate `checkpoint:` sink option is limited to processless `Docket.Test`
  helpers.
  The legacy callback is a host-owned committer and may veto an in-process
  transition; an observer runs only after a backend transaction commits and
  can never roll durable state back. Delivery runs asynchronously under the
  Docket instance's task supervisor and may be lost or duplicated, so
  long-lived consumers must use retained events or an export mechanism.
  """

  @doc "Observes an already-committed checkpoint. Failures are isolated and logged."
  @callback observe(Docket.Checkpoint.t(), Docket.Checkpoint.Context.t()) ::
              :ok | {:error, term()}
end
