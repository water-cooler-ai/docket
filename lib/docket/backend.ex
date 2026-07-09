defmodule Docket.Backend do
  @moduledoc """
  Bundle contract for a durable Docket backend.

  A backend is the configuration and substitution boundary. It supplies one
  transaction implementation and graph, run, and event capabilities that all
  understand the transaction context produced by that implementation. Store
  modules remain focused and independently testable, but callers must not
  assemble capabilities from unrelated backends.

  The backend also owns its supervision entry point. `child_spec/1` receives
  the backend options selected by the host application and returns the single
  child specification the host places in its supervision tree.
  """

  @typedoc "A module implementing one of Docket's storage capability contracts."
  @type capability :: module()

  @doc "Returns the backend's `Docket.Storage` implementation."
  @callback storage() :: capability()

  @doc "Returns the backend's `Docket.Storage.Graphs` implementation."
  @callback graphs() :: capability()

  @doc "Returns the backend's `Docket.Storage.Runs` implementation."
  @callback runs() :: capability()

  @doc "Returns the backend's `Docket.Storage.Events` implementation."
  @callback events() :: capability()

  @doc "Builds the backend's supervision child specification."
  @callback child_spec(opts :: keyword()) :: Supervisor.child_spec()
end
