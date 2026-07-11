defmodule Docket.Runtime.Supervisor do
  @moduledoc """
  Supervision tree for one named Docket runtime instance.

  Hosts start one per runtime instance, usually through the `use Docket`
  host module, or directly:

      children = [
        {Docket.Runtime.Supervisor,
         name: MyApp.DocketRuntime,
         checkpoint: MyApp.DocketCheckpoint}
      ]

  `:name` is required and becomes the runtime identity passed to
  `Docket.run/4` and friends. All other options are stored as the instance's
  default run options and merged under per-call options. A configured
  `backend: BackendModule` contributes its own supervised child and is the
  only public durable backend substitution point; individual stores cannot be mixed.

  The tree owns a unique registry (one active `Docket.Runtime` per run ID),
  a `Task.Supervisor` for node tasks and async checkpoint delivery, and a
  `DynamicSupervisor` for the per-run Runtime processes.
  """

  use Supervisor

  alias Docket.Runtime.Registry, as: RuntimeRegistry

  @doc "DynamicSupervisor name for the per-run Runtime processes."
  def run_supervisor(runtime) when is_atom(runtime), do: Module.concat(runtime, RunSupervisor)

  @doc "Task.Supervisor name for node tasks and async checkpoint delivery."
  def task_supervisor(runtime) when is_atom(runtime), do: Module.concat(runtime, TaskSupervisor)

  def start_link(opts) do
    {name, defaults} = Keyword.pop(opts, :name)

    unless is_atom(name) and name != nil do
      raise ArgumentError, "Docket.Runtime.Supervisor requires a :name atom, got #{inspect(name)}"
    end

    Supervisor.start_link(__MODULE__, {name, defaults}, name: name)
  end

  @impl true
  def init({name, defaults}) do
    {backend_children, defaults} = backend_children(name, defaults)

    children =
      [RuntimeRegistry.child_spec(name)] ++
        backend_children ++
        [
          defaults_child(name, defaults),
          {Task.Supervisor, name: task_supervisor(name)},
          {DynamicSupervisor, name: run_supervisor(name), strategy: :one_for_one}
        ]

    # If the registry dies, running Runtime processes are unreachable
    # orphans; restart the whole tree and let hosts resume runs from their
    # latest checkpoints.
    Supervisor.init(children, strategy: :one_for_all)
  end

  defp backend_children(name, defaults) do
    reject_component_configuration!(defaults)

    case Keyword.get(defaults, :backend) do
      nil ->
        {[], defaults}

      backend when is_atom(backend) ->
        validate_backend!(backend)
        backend_opts = Keyword.put(defaults, :name, Module.concat(name, Backend))
        context = Docket.Backend.resolve_context(backend, backend_opts)
        {[backend.child_spec(backend_opts)], Keyword.put(defaults, :backend_context, context)}

      other ->
        raise ArgumentError, ":backend must be one Docket.Backend module, got: #{inspect(other)}"
    end
  end

  defp reject_component_configuration!(defaults) do
    forbidden =
      [:storage, :transaction, :graph_store, :run_store, :event_store, :coordinator]
      |> Enum.filter(&Keyword.has_key?(defaults, &1))

    if forbidden != [] do
      raise ArgumentError,
            "configure one :backend bundle, not legacy or individual components: " <>
              Enum.map_join(forbidden, ", ", &inspect/1)
    end
  end

  defp validate_backend!(backend) do
    Code.ensure_loaded?(backend) ||
      raise ArgumentError, ":backend module #{inspect(backend)} could not be loaded"

    missing =
      for {name, arity} <- [storage: 0, graphs: 0, runs: 0, events: 0, child_spec: 1],
          not function_exported?(backend, name, arity),
          do: "#{name}/#{arity}"

    if missing != [] do
      raise ArgumentError,
            ":backend module #{inspect(backend)} does not implement Docket.Backend; " <>
              "missing #{Enum.join(missing, ", ")}"
    end
  end

  # Stores the instance defaults as registry metadata synchronously during
  # tree startup, so Docket.run/4 can never observe a started tree without
  # defaults. `:ignore` keeps it processless.
  defp defaults_child(name, defaults) do
    %{
      id: :defaults,
      start: {__MODULE__, :put_defaults, [name, defaults]},
      restart: :transient
    }
  end

  @doc false
  def put_defaults(name, defaults) do
    :ok = RuntimeRegistry.put_defaults(name, defaults)
    :ignore
  end
end
