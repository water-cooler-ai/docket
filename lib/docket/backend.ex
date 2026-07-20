defmodule Docket.Backend do
  @moduledoc """
  Bundle contract for a durable Docket backend.

  A backend is the configuration and substitution boundary. It supplies one
  transaction implementation and graph, run, and event capabilities that all
  understand the transaction context produced by that implementation. Store
  modules remain focused and independently testable, but callers must not
  assemble capabilities from unrelated backends.

  The backend also owns its supervision entry point. `child_spec/2` receives
  the backend options selected by the host application and the already-resolved
  opaque context as separate arguments. It returns the single child
  specification the host places in its supervision tree. Keeping the context
  separate prevents core from injecting private keys into backend-owned options.

  Testing execution is also explicit. `drain_runs/2` receives the same resolved
  context separately; `:manual` instances invoke it only through the public
  drain operation, while `:inline` instances invoke it after committed work is
  scheduled. Backends return a summary containing `:limit_reached`.
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
  @type drain_summary :: %{
          required(:limit_reached) => boolean(),
          optional(atom()) => term()
        }

  @doc """
  Runs `fun` in one backend transaction.

  The callback receives a transaction-scoped opaque context, which must be
  passed to every graph, run, and event operation participating in the
  transaction. It returns `{:ok, value}` to commit or `{:error, reason}` to
  roll back. The backend returns that result unchanged, which lets lifecycle
  code compose store operations naturally with `with`.

  Exceptions and throws also roll back, then propagate unchanged. A backend
  joins a transaction already represented by `ctx` rather than opening an
  invalid nested transaction. Returning any other shape raises
  `ArgumentError` and rolls back. If a nested callback fails and its result or
  raised value is swallowed, the containing transaction is rollback-only and
  returns `{:error, :rollback}` instead of publishing partial work.

  Transaction-scoped describes participation, not value lifetime or identity.
  A backend may yield an ephemeral transaction object or reuse a normalized
  root-context representation whose active transaction is owned by the
  process, connection, or substrate. Callers must use the yielded value
  unchanged inside the callback and must not rely on its behavior afterward.

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

  @doc "Returns the backend's optional claim-policy administration capability."
  @callback claim_policy_admin() :: capability()

  @doc "Builds the backend's supervision child specification from options and its resolved context."
  @callback child_spec(opts :: keyword(), ctx()) :: Supervisor.child_spec()

  @doc "Resolves the opaque root context passed to the backend transaction boundary."
  @callback context(opts :: keyword()) :: ctx()

  @doc "Synchronously claims and drains due runs using the resolved backend context."
  @callback drain_runs(ctx(), opts :: keyword()) ::
              {:ok, drain_summary()} | {:error, term()}

  @optional_callbacks claim_policy_admin: 0
end
