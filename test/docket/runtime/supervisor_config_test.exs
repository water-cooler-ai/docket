defmodule Docket.Runtime.SupervisorConfigTest do
  use ExUnit.Case, async: true

  alias Docket.Test.MemoryBackend

  @runtime Module.concat(__MODULE__, Runtime)

  defmodule IncompleteBackend do
  end

  defmodule MissingContextBackend do
    def transaction(context, fun), do: fun.(context)
    def graphs, do: Docket.Test.MemoryBackend
    def runs, do: Docket.Test.MemoryBackend
    def events, do: Docket.Test.MemoryBackend

    def child_spec(_opts, _context),
      do: %{id: __MODULE__, start: {Task, :start_link, [fn -> :ok end]}}
  end

  defmodule MissingChildSpecBackend do
    def transaction(context, fun), do: fun.(context)
    def graphs, do: Docket.Test.MemoryBackend
    def runs, do: Docket.Test.MemoryBackend
    def events, do: Docket.Test.MemoryBackend
    def context(opts), do: Keyword.fetch!(opts, :name)
  end

  defmodule StrictBackend do
    @behaviour Docket.Backend

    @allowed_options [:backend, :custom, :name, :test_pid]

    @impl true
    def transaction(context, fun), do: fun.(context)

    @impl true
    def graphs, do: Docket.Test.MemoryBackend

    @impl true
    def runs, do: Docket.Test.MemoryBackend

    @impl true
    def events, do: Docket.Test.MemoryBackend

    @impl true
    def context(opts) do
      validate_options!(opts)
      context = {:strict, Keyword.fetch!(opts, :name), make_ref()}

      if test_pid = opts[:test_pid], do: send(test_pid, {:strict_context_resolved, context})
      context
    end

    @impl true
    def child_spec(opts, context) do
      validate_options!(opts)
      if test_pid = opts[:test_pid], do: send(test_pid, {:strict_child_context, context})

      %{
        id: __MODULE__,
        start: {__MODULE__, :start_link, [opts, context]}
      }
    end

    def start_link(_opts, _context), do: Task.start_link(fn -> Process.sleep(:infinity) end)

    defp validate_options!(opts) do
      case Keyword.keys(opts) -- @allowed_options do
        [] -> :ok
        unknown -> raise ArgumentError, "unknown backend options: #{inspect(unknown)}"
      end
    end
  end

  test "one backend bundle contributes its child and resolved context" do
    assert {:ok, {_flags, children}} =
             Docket.Runtime.Supervisor.init({@runtime, backend: MemoryBackend})

    assert %{start: {MemoryBackend, :start_link, [backend_opts]}} =
             Enum.find(children, &(&1.id == MemoryBackend))

    assert Keyword.fetch!(backend_opts, :name) == Module.concat(@runtime, Backend)
    refute Keyword.has_key?(backend_opts, :backend_context)

    assert %{start: {Docket.Runtime.Instance, :start_link, [{@runtime, defaults}]}} =
             Enum.find(children, &(&1.id == Docket.Runtime.Instance))

    assert Keyword.fetch!(defaults, :backend) == MemoryBackend
    assert Keyword.fetch!(defaults, :backend_context) == Module.concat(@runtime, Backend)
  end

  test "resolved context is separate from strict backend-owned options" do
    assert {:ok, {_flags, children}} =
             Docket.Runtime.Supervisor.init(
               {@runtime, backend: StrictBackend, custom: :accepted, test_pid: self()}
             )

    assert %{start: {StrictBackend, :start_link, [backend_opts, context]}} =
             Enum.find(children, &(&1.id == StrictBackend))

    assert Keyword.keys(backend_opts) |> Enum.sort() == [:backend, :custom, :name, :test_pid]
    assert {:strict, runtime_backend, _identity} = context
    assert runtime_backend == Module.concat(@runtime, Backend)
    assert_receive {:strict_context_resolved, ^context}
    assert_receive {:strict_child_context, ^context}
    refute_receive {:strict_context_resolved, _other_context}

    assert %{start: {Docket.Runtime.Instance, :start_link, [{@runtime, defaults}]}} =
             Enum.find(children, &(&1.id == Docket.Runtime.Instance))

    assert Keyword.fetch!(defaults, :backend_context) === context
  end

  test "runtime configuration cannot spoof the internally resolved backend context" do
    assert_raise ArgumentError, ~r/:backend_context is resolved internally/, fn ->
      Docket.Runtime.Supervisor.init(
        {@runtime, backend: StrictBackend, backend_context: :spoofed}
      )
    end
  end

  test "a production instance requires a backend" do
    assert_raise ArgumentError, ~r/requires one :backend implementing Docket.Backend/, fn ->
      Docket.Runtime.Supervisor.init({@runtime, []})
    end
  end

  test "the host-owned checkpoint committer is rejected" do
    assert_raise ArgumentError, ~r/production :checkpoint configuration was removed/, fn ->
      Docket.Runtime.Supervisor.init(
        {@runtime, backend: MemoryBackend, checkpoint: __MODULE__.RemovedCheckpointHandler}
      )
    end
  end

  test "the former storage option is rejected instead of silently ignored" do
    assert_raise ArgumentError, ~r/configure one :backend bundle.*:storage/, fn ->
      Docket.Runtime.Supervisor.init({@runtime, storage: MemoryBackend})
    end
  end

  test "individual backend components remain unsupported" do
    assert_raise ArgumentError, ~r/configure one :backend bundle.*:graph_store/, fn ->
      Docket.Runtime.Supervisor.init({@runtime, graph_store: MemoryBackend})
    end
  end

  test "backend must be a loaded Docket.Backend module" do
    assert_raise ArgumentError, ~r/:backend must be one Docket.Backend module/, fn ->
      Docket.Runtime.Supervisor.init({@runtime, backend: %{module: MemoryBackend}})
    end

    assert_raise ArgumentError, ~r/:backend module .* could not be loaded/, fn ->
      Docket.Runtime.Supervisor.init({@runtime, backend: __MODULE__.MissingBackend})
    end

    assert_raise ArgumentError, ~r/does not implement Docket.Backend.*transaction\/2/, fn ->
      Docket.Runtime.Supervisor.init({@runtime, backend: IncompleteBackend})
    end

    assert_raise ArgumentError, ~r/does not implement Docket.Backend.*context\/1/, fn ->
      Docket.Runtime.Supervisor.init({@runtime, backend: MissingContextBackend})
    end

    assert_raise ArgumentError, ~r/does not implement Docket.Backend.*child_spec\/2/, fn ->
      Docket.Runtime.Supervisor.init({@runtime, backend: MissingChildSpecBackend})
    end
  end

  test "the production tree has no per-run registry or dynamic supervisor" do
    assert {:ok, {_flags, children}} =
             Docket.Runtime.Supervisor.init({@runtime, backend: MemoryBackend})

    refute Enum.any?(children, &match?(%{start: {Registry, _, _}}, &1))
    refute Enum.any?(children, &match?(%{start: {DynamicSupervisor, _, _}}, &1))
    refute Code.ensure_loaded?(Docket.Runtime.Registry)
    refute Code.ensure_loaded?(Docket.Runtime)
  end

  test "shared task supervision starts before a backend can claim work" do
    assert {:ok, {_flags, children}} =
             Docket.Runtime.Supervisor.init({@runtime, backend: MemoryBackend})

    task_index = Enum.find_index(children, &match?(%{start: {Task.Supervisor, _, _}}, &1))
    backend_index = Enum.find_index(children, &(&1.id == MemoryBackend))
    assert task_index < backend_index
  end
end
