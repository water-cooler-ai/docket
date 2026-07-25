defmodule Docket.Backend do
  @moduledoc """
  Bundle contract for a durable Docket backend.

  A backend is the configuration and substitution boundary. It supplies
  semantic lifecycle transitions plus focused graph, run, and event
  capabilities. Store modules remain independently testable, but callers must
  not assemble capabilities from unrelated backends.

  Lifecycle writes use `Docket.Backend.TransitionStore`. Every backend
  declares that capability explicitly through `capabilities/0` and resolves
  its implementation through `transitions/0`; contract negotiation rejects
  backends that do not. Backend-native transactions are private
  implementation details and never cross this contract.

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

  @type capabilities :: %{
          required(:contract_version) => 2,
          required(:transitions) => %{
            required(:version) => pos_integer(),
            optional(atom()) => term()
          }
        }
  @type drain_summary :: %{
          required(:limit_reached) => boolean(),
          optional(atom()) => term()
        }

  @doc """
  Declares the backend contract and semantic transition capability.

  Version 2 requires `transitions/0`, transition version 1, and a module that
  implements every `Docket.Backend.TransitionStore` callback. Contract
  negotiation rejects a backend that omits this callback or declares any
  other contract shape.
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

  @doc "Builds the backend's supervision child specification from options and its resolved context."
  @callback child_spec(opts :: keyword(), ctx()) :: Supervisor.child_spec()

  @doc "Resolves the opaque root context passed to the backend's stores and transition store."
  @callback context(opts :: keyword()) :: ctx()

  @doc "Synchronously claims and drains due runs using the resolved backend context."
  @callback drain_runs(ctx(), opts :: keyword()) ::
              {:ok, drain_summary()} | {:error, term()}

  @doc false
  @spec transition_store(module(), ctx()) :: {module(), ctx()}
  def transition_store(backend, context) when is_atom(backend) do
    case declared_capabilities(backend) do
      %{contract_version: 2, transitions: %{version: version}} when version == 1 ->
        unless function_exported?(backend, :transitions, 0) do
          raise ArgumentError,
                "backend #{inspect(backend)} declares transition contract version 2 " <>
                  "but does not export transitions/0"
        end

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
  @spec declared_capabilities(module()) :: capabilities()
  def declared_capabilities(backend) when is_atom(backend) do
    unless function_exported?(backend, :capabilities, 0) do
      raise ArgumentError,
            "backend #{inspect(backend)} does not export capabilities/0; " <>
              "since 0.2 every backend must declare contract version 2 and " <>
              "implement Docket.Backend.TransitionStore"
    end

    case backend.capabilities() do
      %{
        contract_version: 2,
        transitions: %{version: version}
      } = capabilities
      when is_integer(version) and version > 0 ->
        capabilities

      %{contract_version: 1} ->
        raise ArgumentError,
              "backend #{inspect(backend)} declares contract version 1, which was " <>
                "removed in 0.2; declare contract version 2 and implement " <>
                "Docket.Backend.TransitionStore"

      capabilities ->
        raise ArgumentError,
              "backend #{inspect(backend)} returned invalid capabilities: " <>
                inspect(capabilities)
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
end
