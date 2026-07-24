defmodule Docket.Backend do
  @moduledoc """
  Bundle contract for a durable Docket backend.

  A backend is the configuration and substitution boundary. It supplies
  semantic lifecycle transitions plus focused graph, run, and event
  capabilities. Store modules remain independently testable, but callers must
  not assemble capabilities from unrelated backends.

  Since 0.1.2, lifecycle writes use `Docket.Backend.TransitionStore`. Backends
  declare that capability explicitly through `capabilities/0`; an undeclared
  0.1.x backend is routed through the legacy composition adapter. Public
  callback transactions remain only for that compatibility window and are
  removed in 0.2.

  The backend also owns its supervision entry point. `child_spec/2` receives
  the options nested under `{BackendModule, options}`, the small set of
  runtime-owned policies needed for execution, and the runtime-generated name.
  The already-resolved opaque context remains a separate argument. The callback
  returns the single child specification the host places in its supervision
  tree.

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
  @type capabilities :: %{
          required(:contract_version) => 1 | 2,
          optional(:transitions) => %{
            required(:version) => pos_integer(),
            required(:limits) => Docket.Backend.TransitionStore.limits(),
            optional(atom()) => term()
          }
        }
  @type drain_summary :: %{
          required(:limit_reached) => boolean(),
          optional(atom()) => term()
        }

  @doc deprecated:
         "lifecycle transaction composition is deprecated; implement " <>
           "Docket.Backend.TransitionStore and transitions/0"
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

  @doc """
  Declares the backend contract and semantic transition capability.

  Version 2 requires `transitions/0`, transition version 1, and a module that
  implements every `Docket.Backend.TransitionStore` callback. Backends written
  against 0.1.0 or 0.1.1 may omit this callback and are treated as contract
  version 1 during the 0.1.x compatibility window.
  """
  @callback capabilities() :: capabilities()

  @doc "Returns the backend's `Docket.Backend.TransitionStore` implementation."
  @callback transitions() :: capability()

  @doc "Returns the backend's `Docket.Backend.GraphStore` implementation."
  @callback graphs() :: capability()

  @doc "Returns the backend's `Docket.Backend.RunStore` implementation."
  @callback runs() :: capability()

  @doc "Returns the backend's `Docket.Backend.EventStore` implementation."
  @callback events() :: capability()

  @doc deprecated: "use Docket.Backend.TransitionStore.commit_claimed/4 through transitions/0"
  @doc """
  Optionally commits one claim-fenced run transition and its assigned events
  through a backend-native fused operation.

  Lifecycle invokes this callback directly when the backend exports it. The
  proposal and events carry the same substrate-neutral values otherwise sent
  to `RunStore.commit/3` and `EventStore.append_events/4`. The callback itself
  must be atomic; it must not require an outer `transaction/2` merely to make
  the run and event writes indivisible. Implementations must preserve those
  callbacks' validation, scope, fencing, conflict, and failure semantics.

  Backends that do not implement this optional callback retain the portable
  composed store path.
  """
  @callback commit_transition(
              ctx(),
              scope(),
              Docket.Backend.RunStore.commit_proposal(),
              [Docket.Event.t()]
            ) :: {:ok, Docket.Run.t()} | {:error, term()}

  @doc "Builds the backend's supervision child specification from options and its resolved context."
  @callback child_spec(opts :: keyword(), ctx()) :: Supervisor.child_spec()

  @doc "Resolves the opaque root context passed to the backend transaction boundary."
  @callback context(opts :: keyword()) :: ctx()

  @doc "Synchronously claims and drains due runs using the resolved backend context."
  @callback drain_runs(ctx(), opts :: keyword()) ::
              {:ok, drain_summary()} | {:error, term()}

  @optional_callbacks capabilities: 0, transitions: 0, commit_transition: 4

  @doc false
  @spec transition_store(module(), ctx()) :: {module(), ctx()}
  def transition_store(backend, context) when is_atom(backend) do
    case declared_capabilities(backend) do
      :legacy ->
        {Docket.Backend.LegacyTransitionStore, {backend, context}}

      %{contract_version: 1} ->
        {Docket.Backend.LegacyTransitionStore, {backend, context}}

      %{contract_version: 2, transitions: %{version: version}} when version == 1 ->
        store = backend.transitions()
        validate_transition_store!(backend, store)
        {store, context}

      capabilities ->
        raise ArgumentError,
              "backend #{inspect(backend)} declares an unsupported transition contract: " <>
                inspect(capabilities)
    end
  end

  @doc false
  @spec validate_contract!(module()) :: :ok
  def validate_contract!(backend) when is_atom(backend) do
    _resolved = transition_store(backend, :validation_context)
    :ok
  end

  @doc false
  @spec declared_capabilities(module()) :: :legacy | capabilities()
  def declared_capabilities(backend) when is_atom(backend) do
    if function_exported?(backend, :capabilities, 0) do
      case backend.capabilities() do
        %{contract_version: 1} = capabilities ->
          capabilities

        %{
          contract_version: 2,
          transitions: %{version: version, limits: limits}
        } = capabilities
        when is_integer(version) and version > 0 ->
          validate_limits!(backend, limits)
          capabilities

        capabilities ->
          raise ArgumentError,
                "backend #{inspect(backend)} returned invalid capabilities: " <>
                  inspect(capabilities)
      end
    else
      :legacy
    end
  end

  defp validate_transition_store!(backend, store) when is_atom(store) do
    Code.ensure_loaded?(store) ||
      raise ArgumentError,
            "backend #{inspect(backend)} transitions/0 -> #{inspect(store)} could not be loaded"

    missing =
      Docket.Backend.TransitionStore.behaviour_info(:callbacks)
      |> Enum.reject(fn {name, arity} -> function_exported?(store, name, arity) end)

    if missing != [] do
      callbacks = Enum.map_join(missing, ", ", fn {name, arity} -> "#{name}/#{arity}" end)

      raise ArgumentError,
            "backend #{inspect(backend)} transitions/0 -> #{inspect(store)} is incomplete; " <>
              "missing #{callbacks}"
    end

    :ok
  end

  defp validate_transition_store!(backend, store) do
    raise ArgumentError,
          "backend #{inspect(backend)} transitions/0 must return a module, got: " <>
            inspect(store)
  end

  defp validate_limits!(backend, limits) when is_map(limits) do
    floor = Docket.Backend.TransitionStore.portable_limits()

    below_floor =
      Enum.flat_map(floor, fn {name, minimum} ->
        case Map.get(limits, name) do
          value when is_integer(value) and value >= minimum -> []
          value -> ["#{name}=#{inspect(value)} (minimum #{minimum})"]
        end
      end)

    if below_floor != [] do
      raise ArgumentError,
            "backend #{inspect(backend)} transition limits do not satisfy the portable floor: " <>
              Enum.join(below_floor, ", ")
    end

    :ok
  end

  defp validate_limits!(backend, limits) do
    raise ArgumentError,
          "backend #{inspect(backend)} transition limits must be a map, got: " <>
            inspect(limits)
  end
end
