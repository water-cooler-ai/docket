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
  default run options and merged under per-call options.

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
    children = [
      RuntimeRegistry.child_spec(name),
      defaults_child(name, defaults),
      {Task.Supervisor, name: task_supervisor(name)},
      {DynamicSupervisor, name: run_supervisor(name), strategy: :one_for_one}
    ]

    # If the registry dies, running Runtime processes are unreachable
    # orphans; restart the whole tree and let hosts resume runs from their
    # latest checkpoints.
    Supervisor.init(children, strategy: :one_for_all)
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
