defmodule Docket.Storage.ReadCapabilitiesTest do
  use ExUnit.Case, async: true

  test "run and event read callbacks are part of the backend contracts" do
    run_callbacks = Docket.Storage.Runs.behaviour_info(:callbacks)
    event_callbacks = Docket.Storage.Events.behaviour_info(:callbacks)

    assert {:list_runs, 3} in run_callbacks
    assert {:fetch_event, 4} in event_callbacks
    assert {:fetch_latest_event, 3} in event_callbacks
    assert {:list_events, 4} in event_callbacks
  end

  test "the conformance backend implements every new read callback" do
    assert Code.ensure_loaded?(Docket.Test.MemoryBackend)

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
end
