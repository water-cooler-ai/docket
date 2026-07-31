defmodule Docket.Node do
  @moduledoc """
  Behaviour implemented by executable node modules.

  ## Failure

  A node signals failure in one of four ways, all normalized identically by
  the dispatcher:

  - returning `{:error, reason}`,
  - raising an exception,
  - exiting or throwing, or
  - exceeding its finite runtime-owned effective attempt timeout.

  Each is a node *attempt* failure. The dispatcher retries the attempt per the
  node's resolved retry policy (`max_attempts`/`backoff_ms`); when retries are
  exhausted the failure becomes *permanent*.

  A permanent node failure fails the **entire run**, not just the node. v0.1 has
  no per-node error recovery or error-edge routing: at the update barrier a
  permanent failure commits **no writes** from that superstep (including writes
  from sibling nodes that succeeded), transitions the run to the terminal
  `:failed` status, and emits a sync `:run_failed` checkpoint carrying the
  failing node IDs. The run is then terminal and execution does not resume.

  Return `{:error, reason}` only for failures that *should* halt the run.
  For an *expected* failure that the graph should handle rather than abort on
  (for example an HTTP call whose non-2xx response should route to a fallback
  branch), return `{:ok, state_update}` with the error encoded in a state
  field and branch on that field with an edge guard — do not return
  `{:error, ...}`.

  ## Detachment

  Returning `{:detach, token, worker}` hands the attempt's work back to the
  runtime: the attempt is not consumed and no failure is recorded, the run
  parks durably with the task's identity and a mandatory deadline, and the
  claim releases. The runtime starts `worker` (a one-arity function
  receiving the `Docket.Detached` identity) under its task supervisor
  **only after the detach park commits**, so the worker's completion can
  never race a park that is not yet durable — never start the work inside
  the node body; the activation process is killed at its attempt timeout
  and a park that loses its commit fence must not leave a worker behind.

  Returning `{:detach, token}` detaches without a runtime-started worker:
  an external system completes the attempt instead, using an identity the
  node built with `Docket.Detached.from_context/1`. An external completion
  arriving before the park commits receives a retryable
  `%Docket.Error{type: :detach_pending}`.

  Either way the work completes through `Docket.complete_detached/4`, or
  the deadline recovers the run per the node's `"detach"` policy. The token
  must be a durable value; it is retained on the parked task for
  correlation. External effects remain at-least-once: an attempt
  re-executed after deadline expiry may repeat them, deduplicated only by
  the stable idempotency key.
  """

  @callback config_schema() :: Docket.Schema.t()

  @doc """
  Executes the node against its state snapshot, resolved config, and runtime
  context.
  """
  @callback call(state :: map(), config :: map(), context :: map()) ::
              {:ok, state_update :: map()}
              | {:interrupt, Docket.Interrupt.t()}
              | {:detach, term()}
              | {:detach, term(), (Docket.Detached.t() -> any())}
              | {:error, term()}
end
