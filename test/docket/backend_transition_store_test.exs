defmodule Docket.Backend.TransitionStoreTest do
  use ExUnit.Case, async: true

  alias Docket.Backend.TransitionStore

  @run %Docket.Run{id: "run", graph_id: "graph", graph_hash: "hash", checkpoint_seq: 1}
  @proposal %{
    transition_id: "docket:v1:initialize:run:1",
    run: @run,
    checkpoint_type: :run_initialized,
    wake_at: ~U[2026-07-24 00:00:00Z]
  }
  @event %Docket.Event{
    run_id: "run",
    seq: 1,
    type: :run_initialized,
    step: 0,
    timestamp: ~U[2026-07-24 00:00:00Z]
  }

  test "portable bounds accept the exact limit and reject limit plus one" do
    run_bytes = encoded_size(@run)
    event_bytes = encoded_size(@event)
    transition_bytes = encoded_size(@proposal) + event_bytes

    exact = %{
      max_run_bytes: run_bytes,
      max_events: 1,
      max_event_bytes: event_bytes,
      max_transition_bytes: transition_bytes
    }

    assert :ok = TransitionStore.validate_limits(@proposal, [@event], exact)

    for {bound, value} <- [
          max_run_bytes: run_bytes - 1,
          max_events: 0,
          max_event_bytes: event_bytes - 1,
          max_transition_bytes: transition_bytes - 1
        ] do
      assert {:error, :too_large} =
               TransitionStore.validate_limits(@proposal, [@event], Map.put(exact, bound, value))
    end
  end

  test "a malformed or missing transition id is rejected before sizing" do
    limits = TransitionStore.portable_limits()

    assert {:error, :invalid_transition} =
             TransitionStore.validate_limits(Map.delete(@proposal, :transition_id), [], limits)

    assert {:error, :invalid_transition} =
             TransitionStore.validate_limits(%{@proposal | transition_id: ""}, [], limits)
  end

  defp encoded_size(term) do
    term
    |> :erlang.term_to_binary([:deterministic, minor_version: 2])
    |> byte_size()
  end
end
