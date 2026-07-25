defmodule Docket.Backend.StoreCapabilitiesTest do
  use ExUnit.Case, async: true

  defmodule MissingRunStore do
    @moduledoc false
  end

  defmodule IncompleteBundle do
    @moduledoc false

    def graphs, do: Docket.Test.MemoryBackend
    def runs, do: MissingRunStore
    def events, do: Docket.Test.MemoryBackend
    def drain_runs(_context, _opts), do: {:error, :unsupported}
    def context(opts), do: Keyword.fetch!(opts, :name)

    def child_spec(_opts, _context),
      do: %{id: __MODULE__, start: {Task, :start_link, [fn -> :ok end]}}
  end

  defmodule IncompleteTransitionStore do
    def initialize(_context, _scope, proposal, _events), do: {:ok, proposal.run}
  end

  defmodule PartiallyUpgradedBundle do
    def capabilities do
      %{
        contract_version: 2,
        transitions: %{version: 1}
      }
    end

    def transitions, do: IncompleteTransitionStore
  end

  defmodule MissingTransitionAccessorBundle do
    def capabilities do
      %{
        contract_version: 2,
        transitions: %{version: 1}
      }
    end
  end

  defmodule ContractV1Bundle do
    @moduledoc false

    def capabilities, do: %{contract_version: 1}
  end

  defmodule IncompleteStoreBundle do
    @moduledoc false

    def capabilities do
      %{
        contract_version: 2,
        transitions: %{version: 1}
      }
    end

    def transitions, do: Docket.Test.MemoryBackend
    def graphs, do: Docket.Test.MemoryBackend
    def runs, do: MissingRunStore
    def events, do: Docket.Test.MemoryBackend
    def drain_runs(_context, _opts), do: {:error, :unsupported}
    def context(opts), do: Keyword.fetch!(opts, :name)

    def child_spec(_opts, _context),
      do: %{id: __MODULE__, start: {Task, :start_link, [fn -> :ok end]}}
  end

  test "the backend owns versioned transitions and focused stores" do
    callbacks = Docket.Backend.behaviour_info(:callbacks)
    optional_callbacks = Docket.Backend.behaviour_info(:optional_callbacks)

    assert {:capabilities, 0} in callbacks
    assert {:transitions, 0} in callbacks
    assert {:context, 1} in callbacks
    assert {:child_spec, 2} in callbacks
    assert {:drain_runs, 2} in callbacks
    refute {:capabilities, 0} in optional_callbacks
    refute {:transitions, 0} in optional_callbacks
    refute {:storage, 0} in callbacks
    refute Code.ensure_loaded?(Docket.Storage)
    refute Code.ensure_loaded?(Docket.Storage.Graphs)
    refute Code.ensure_loaded?(Docket.Storage.Runs)
    refute Code.ensure_loaded?(Docket.Storage.Events)
  end

  test "run and event read callbacks are part of the backend contracts" do
    run_callbacks = Docket.Backend.RunStore.behaviour_info(:callbacks)
    event_callbacks = Docket.Backend.EventStore.behaviour_info(:callbacks)

    assert {:list_runs, 3} in run_callbacks
    assert {:fetch_event, 4} in event_callbacks
    assert {:fetch_latest_event, 3} in event_callbacks
    assert {:list_events, 4} in event_callbacks
  end

  test "the shared memory backend implements every new read callback" do
    assert Code.ensure_loaded?(Docket.Test.MemoryBackend)
    assert Docket.Test.MemoryBackend.capabilities().contract_version == 2
    assert Docket.Test.MemoryBackend.transitions() == Docket.Test.MemoryBackend
    refute function_exported?(Docket.Test.MemoryBackend, :storage, 0)

    for {name, arity} <- [
          fetch_graph: 4,
          fetch_latest_graph_ref: 3,
          list_graph_versions: 4,
          list_runs: 3,
          fetch_event: 4,
          fetch_latest_event: 3
        ] do
      assert function_exported?(Docket.Test.MemoryBackend, name, arity)
    end
  end

  test "an undeclared backend fails contract validation" do
    error =
      assert_raise ArgumentError, fn ->
        Docket.Backend.validate_contract!(IncompleteBundle)
      end

    assert error.message =~ "does not export capabilities/0"
  end

  test "a contract version 1 backend is rejected as removed" do
    error =
      assert_raise ArgumentError, fn ->
        Docket.Backend.validate_contract!(ContractV1Bundle)
      end

    assert error.message =~ "declares contract version 1, which was removed in 0.2"
  end

  test "partially upgraded transition declarations fail clearly" do
    error =
      assert_raise ArgumentError, fn ->
        Docket.Backend.validate_contract!(PartiallyUpgradedBundle)
      end

    assert error.message =~ "missing"
    assert error.message =~ "commit_claimed/4"
    assert error.message =~ "commit_unclaimed/5"
  end

  test "a declared transition contract without transitions/0 fails clearly" do
    error =
      assert_raise ArgumentError, fn ->
        Docket.Backend.validate_contract!(MissingTransitionAccessorBundle)
      end

    assert error.message =~ "does not export transitions/0"
  end

  test "shared backend completeness failures name the accessor and exact callback" do
    violations = Docket.BackendTests.Contract.violations(IncompleteStoreBundle)

    assert Enum.any?(violations, fn violation ->
             violation ==
               "backend #{inspect(IncompleteStoreBundle)} runs/0 -> #{inspect(MissingRunStore)}: " <>
                 "missing Docket.Backend.RunStore.fetch_run/3"
           end)
  end
end
