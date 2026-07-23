defmodule Docket.Runtime.Supervisor do
  @moduledoc """
  Supervision tree for one named Docket runtime instance.

  Hosts start one per runtime instance, usually through the `use Docket`
  host module, or directly:

      children = [
        {Docket.Runtime.Supervisor,
         name: MyApp.DocketRuntime,
         backend: {MyApp.DocketBackend, backend_option: :value}}
      ]

  `:name` is required and becomes the runtime identity passed to
  durable facade functions. All other options are stored as the instance's
  instance configuration. Execution policy is resolved once at startup and
  cannot be replaced by per-call options. A configured
  `backend: {BackendModule, options}` contributes its own supervised child
  and keeps backend-specific configuration under that substitution point.
  An optionless backend may use `backend: BackendModule`; individual stores
  cannot be mixed.

  A production instance always owns one backend bundle. The tree also owns a
  small instance-configuration process and a `Task.Supervisor` used by backend
  vehicles for node execution and observer delivery.
  """

  use Supervisor

  @backend_runtime_options [:tenant_mode, :testing] ++ Docket.Runtime.Config.instance_keys()
  @reserved_backend_options [
                              :name,
                              :backend,
                              :backend_options,
                              :backend_context
                            ] ++ @backend_runtime_options

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
    {backend, configured_options} = normalize_backend!(Keyword.get(defaults, :backend))

    defaults =
      defaults
      |> Keyword.put(:backend, backend)
      |> Keyword.put(:backend_options, configured_options)

    Docket.Runtime.Config.validate_runtime!(defaults)
    validate_backend!(backend)

    backend_opts =
      configured_options
      |> Keyword.merge(Keyword.take(defaults, @backend_runtime_options))
      |> Keyword.put(:name, Module.concat(name, Backend))

    context = backend.context(backend_opts)

    {[backend.child_spec(backend_opts, context)],
     Keyword.put(defaults, :backend_context, context)}
  end

  defp normalize_backend!(nil) do
    raise ArgumentError,
          "Docket.Runtime.Supervisor requires one :backend implementing Docket.Backend"
  end

  defp normalize_backend!({backend, options}) when is_atom(backend) do
    unless Keyword.keyword?(options) do
      raise ArgumentError,
            ":backend options must be a keyword list, got: #{inspect(options)}"
    end

    case Enum.filter(Keyword.keys(options), &(&1 in @reserved_backend_options)) do
      [] ->
        :ok

      reserved ->
        raise ArgumentError,
              ":backend options cannot redefine Docket runtime options: #{inspect(reserved)}"
    end

    {backend, options}
  end

  defp normalize_backend!(backend) when is_atom(backend), do: {backend, []}

  defp normalize_backend!(other) do
    raise ArgumentError,
          ":backend must be a Docket.Backend module or {module, options}, got: #{inspect(other)}"
  end

  defp reject_component_configuration!(defaults) do
    if Keyword.has_key?(defaults, :backend_context) do
      raise ArgumentError,
            ":backend_context is resolved internally and cannot be configured on a runtime"
    end

    if Keyword.has_key?(defaults, :backend_options) do
      raise ArgumentError,
            ":backend_options is resolved from :backend and cannot be configured separately"
    end

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
      for {name, arity} <- [
            transaction: 2,
            graphs: 0,
            runs: 0,
            events: 0,
            context: 1,
            child_spec: 2,
            drain_runs: 2
          ],
          not function_exported?(backend, name, arity),
          do: "#{name}/#{arity}"

    if missing != [] do
      raise ArgumentError,
            ":backend module #{inspect(backend)} does not implement Docket.Backend; " <>
              "missing #{Enum.join(missing, ", ")}"
    end
  end
end
