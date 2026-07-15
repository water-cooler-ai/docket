defmodule Docket.Runtime.Supervisor do
  @moduledoc """
  Supervision tree for one named Docket runtime instance.

  Hosts start one per runtime instance, usually through the `use Docket`
  host module, or directly:

      children = [
        {Docket.Runtime.Supervisor,
         name: MyApp.DocketRuntime,
         backend: Docket.Postgres,
         repo: MyApp.Repo}
      ]

  `:name` is required and becomes the runtime identity passed to
  durable facade functions. All other options are stored as the instance's
  default run options and merged under per-call options. A configured
  `backend: BackendModule` contributes its own supervised child and is the
  only public durable backend substitution point; individual stores cannot be mixed.

  A production instance always owns one backend bundle. The tree also owns a
  small instance-configuration process and a `Task.Supervisor` used by backend
  vehicles for node execution and observer delivery.
  """

  use Supervisor

  @doc "Task.Supervisor name for node tasks and after-commit observer delivery."
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

    # Shared configuration and execution supervision must exist before the
    # backend can claim already-due persisted work during its own startup.
    children =
      [
        {Docket.Runtime.Instance, {name, defaults}},
        {Task.Supervisor, name: task_supervisor(name)}
      ] ++ backend_children

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp backend_children(name, defaults) do
    reject_component_configuration!(defaults)

    case Keyword.get(defaults, :backend) do
      nil ->
        raise ArgumentError,
              "Docket.Runtime.Supervisor requires one :backend implementing Docket.Backend"

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
    if Keyword.has_key?(defaults, :checkpoint) do
      raise ArgumentError,
            "production :checkpoint configuration was removed; use :checkpoint_observers " <>
              "or Docket.Test processless helpers"
    end

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
      for {name, arity} <- [transaction: 2, graphs: 0, runs: 0, events: 0, child_spec: 1],
          not function_exported?(backend, name, arity),
          do: "#{name}/#{arity}"

    if missing != [] do
      raise ArgumentError,
            ":backend module #{inspect(backend)} does not implement Docket.Backend; " <>
              "missing #{Enum.join(missing, ", ")}"
    end
  end
end
