defmodule Docket.Runtime.Registry do
  @moduledoc false

  # Maps a runtime instance plus run ID to the active `Docket.Runtime`
  # process and enforces one active owner per run ID (unique keys). Also
  # holds the runtime instance's default run options as registry metadata so
  # they live and die with the supervision tree.
  #
  # PIDs never leave the library: public APIs resolve them here and keep the
  # lookup internal.

  @doc "Registry process name for a runtime instance name."
  def name(runtime) when is_atom(runtime), do: Module.concat(runtime, Registry)

  @doc "Child spec for the registry under `Docket.Runtime.Supervisor`."
  def child_spec(runtime) do
    Registry.child_spec(keys: :unique, name: name(runtime))
  end

  @doc "Via tuple registering a Runtime process as the owner of `run_id`."
  def via(runtime, run_id), do: {:via, Registry, {name(runtime), run_id}}

  @doc "Resolves the active Runtime process for `run_id`."
  def whereis(runtime, run_id) do
    case Registry.lookup(name(runtime), run_id) do
      [{pid, _value}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc "Stores the runtime instance's default run options."
  def put_defaults(runtime, defaults) do
    Registry.put_meta(name(runtime), :defaults, defaults)
  end

  @doc """
  Reads the runtime instance's default run options.

  Returns `:error` when the runtime instance is not running (the registry
  does not exist or holds no defaults).
  """
  def defaults(runtime) do
    Registry.meta(name(runtime), :defaults)
  rescue
    ArgumentError -> :error
  end
end
