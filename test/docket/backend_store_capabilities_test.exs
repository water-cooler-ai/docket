defmodule Docket.Backend.StoreCapabilitiesTest do
  use ExUnit.Case, async: true

  defmodule MissingRunStore do
    @moduledoc false
  end

  defmodule IncompleteBundle do
    @moduledoc false

    def transaction(context, fun), do: fun.(context)
    def graphs, do: Docket.Test.MemoryBackend
    def runs, do: MissingRunStore
    def events, do: Docket.Test.MemoryBackend
    def child_spec(_opts), do: %{id: __MODULE__, start: {Task, :start_link, [fn -> :ok end]}}
  end

  test "the backend owns the transaction boundary and focused stores" do
    callbacks = Docket.Backend.behaviour_info(:callbacks)
    optional_callbacks = Docket.Backend.behaviour_info(:optional_callbacks)

    assert {:transaction, 2} in callbacks
    assert {:context, 1} in optional_callbacks
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

  test "the conformance backend implements every new read callback" do
    assert Code.ensure_loaded?(Docket.Test.MemoryBackend)
    assert function_exported?(Docket.Test.MemoryBackend, :transaction, 2)
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

  test "conformance completeness failures name the accessor and exact callback" do
    violations = Docket.Backend.Conformance.Contract.violations(IncompleteBundle)

    assert Enum.any?(violations, fn violation ->
             violation ==
               "backend #{inspect(IncompleteBundle)} runs/0 -> #{inspect(MissingRunStore)}: " <>
                 "missing Docket.Backend.RunStore.commit/3"
           end)
  end
end
