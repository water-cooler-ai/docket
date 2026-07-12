defmodule Docket.Runtime.SupervisorConfigTest do
  use ExUnit.Case, async: true

  alias Docket.Test.MemoryBackend

  @runtime Module.concat(__MODULE__, Runtime)

  defmodule IncompleteBackend do
  end

  test "one backend bundle contributes its child and resolved context" do
    assert {:ok, {_flags, children}} =
             Docket.Runtime.Supervisor.init({@runtime, backend: MemoryBackend})

    assert %{start: {MemoryBackend, :start_link, [backend_opts]}} =
             Enum.find(children, &(&1.id == MemoryBackend))

    assert Keyword.fetch!(backend_opts, :name) == Module.concat(@runtime, Backend)

    assert %{start: {Docket.Runtime.Instance, :start_link, [{@runtime, defaults}]}} =
             Enum.find(children, &(&1.id == Docket.Runtime.Instance))

    assert Keyword.fetch!(defaults, :backend) == MemoryBackend
    assert Keyword.fetch!(defaults, :backend_context) == Module.concat(@runtime, Backend)
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

    assert_raise ArgumentError, ~r/does not implement Docket.Backend.*storage\/0/, fn ->
      Docket.Runtime.Supervisor.init({@runtime, backend: IncompleteBackend})
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
