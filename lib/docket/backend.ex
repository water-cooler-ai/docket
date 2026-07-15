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

  @typedoc "A module implementing one of Docket's focused store contracts."
  @type capability :: module()

  @typedoc "Opaque backend context passed through without interpretation by core."
  @type ctx :: term()

  @typedoc "Authorization and tenancy scope for a run or its events."
  @type scope :: :system | :tenantless | {:tenant, String.t()}

  @typedoc "Scope that determines graph/run ownership; tenant identifiers are non-empty."
  @type owner_scope :: :tenantless | {:tenant, String.t()}

  @type transaction_result :: {:ok, term()} | {:error, term()}
  @type transaction_fun :: (ctx() -> transaction_result())

  @doc """
  Runs `fun` in one backend transaction.

  The callback receives a transaction-scoped opaque context, which must be
  passed to every graph, run, and event operation participating in the
  transaction. It returns `{:ok, value}` to commit or `{:error, reason}` to
  roll back. The backend returns that result unchanged, which lets lifecycle
  code compose store operations naturally with `with`.

  Exceptions and throws also roll back, then propagate unchanged. A backend
  joins a transaction already represented by `ctx` rather than opening an
  invalid nested transaction.

  Publication must be concurrency safe. An implementation may serialize
  transactions or compare-and-swap their publication, but it must never take
  an unlocked snapshot and later replace newer committed state blindly.
  """
  @callback transaction(ctx(), transaction_fun()) :: transaction_result()

  @doc "Returns the backend's `Docket.Backend.GraphStore` implementation."
  @callback graphs() :: capability()

  @doc "Returns the backend's `Docket.Backend.RunStore` implementation."
  @callback runs() :: capability()

  @doc "Returns the backend's `Docket.Backend.EventStore` implementation."
  @callback events() :: capability()

  @doc "Builds the backend's supervision child specification."
  @callback child_spec(opts :: keyword()) :: Supervisor.child_spec()

  @doc "Resolves the opaque root context passed to the backend transaction boundary."
  @callback context(opts :: keyword()) :: ctx()

  @optional_callbacks context: 1

  @doc false
  def resolve_context(backend, opts) when is_atom(backend) and is_list(opts) do
    if function_exported?(backend, :context, 1) do
      backend.context(opts)
    else
      Keyword.fetch!(opts, :name)
    end
  end
end
