defmodule Docket.Storage do
  @moduledoc """
  Transaction boundary shared by a durable backend's stores.

  Graphs, runs, and events have separate persistence contracts in
  `Docket.Storage.Graphs`, `Docket.Storage.Runs`, and `Docket.Storage.Events`.
  This behaviour supplies the common transaction context that lets lifecycle
  orchestration compose those focused stores without exposing backend details.

  All callbacks take an opaque backend context. Core passes it through and
  never interprets it.

  Run and event access always carries an explicit scope. `:system` is reserved
  for trusted dispatcher and recovery work. `:tenantless` can see only runs
  whose tenant is `nil`, while `{:tenant, tenant_id}` can see only that
  tenant's runs. Omitting scope is impossible at the behaviour boundary, so a
  missing public option can never become a privileged unscoped operation.
  """

  @type ctx :: term()

  @typedoc "Authorization and tenancy scope for a run or its events."
  @type scope :: :system | :tenantless | {:tenant, String.t()}

  @typedoc "Scope that also determines the tenant stored on a newly inserted run."
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
  may join a transaction already represented by `ctx` rather than opening a
  nested transaction.

  Publication must be concurrency safe. An implementation may serialize
  transactions or compare-and-swap their publication, but it must never take
  an unlocked snapshot and later replace newer committed state blindly.
  """
  @callback transaction(ctx(), transaction_fun()) :: transaction_result()
end
