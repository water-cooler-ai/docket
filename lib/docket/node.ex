defmodule Docket.Node do
  @moduledoc """
  Behaviour implemented by executable node modules.

  ## Failure

  A node signals failure in one of four ways, all normalized identically by
  the dispatcher:

  - returning `{:error, reason}`,
  - raising an exception,
  - exiting or throwing, or
  - exceeding its resolved `timeout_ms` (with `Docket.Executor.Task`).

  Each is a node *attempt* failure. The dispatcher retries the attempt per the
  node's resolved retry policy (`max_attempts`/`backoff_ms`); when retries are
  exhausted the failure becomes *permanent*.

  A permanent node failure fails the **entire run**, not just the node. v1 has
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

  `{:await, term()}` is reserved for post-v1 late-completion protocols; in v1
  the dispatcher treats it as a permanent node failure.
  """

  @callback config_schema() :: Docket.Schema.t()

  @doc """
  Executes the node against its state snapshot, resolved config, and runtime
  context.

  `{:await, term()}` is reserved for post-v1 late-completion protocols; in
  v1 the dispatcher treats it as a permanent node failure.
  """
  @callback call(state :: map(), config :: map(), context :: map()) ::
              {:ok, state_update :: map()}
              | {:interrupt, Docket.Interrupt.t()}
              | {:await, term()}
              | {:error, term()}
end
