defmodule Docket.Backend.TransitionStoreTest do
  use ExUnit.Case, async: true

  alias Docket.Backend.TransitionStore

  defmodule TimeoutAfterCommitStore do
    @behaviour Docket.Backend.TransitionStore

    @impl true
    def initialize({backend, gate}, scope, proposal, events) do
      result = Docket.Test.MemoryBackend.initialize(backend, scope, proposal, events)
      inject? = Agent.get_and_update(gate, fn inject? -> {inject?, false} end)

      case {result, inject?} do
        {{:ok, _run}, true} -> {:error, {:retryable, :timeout_after_commit}}
        _other -> result
      end
    end

    @impl true
    def commit_claimed({backend, _gate}, scope, proposal, events),
      do: Docket.Test.MemoryBackend.commit_claimed(backend, scope, proposal, events)

    @impl true
    def commit_unclaimed({backend, _gate}, scope, expected, proposal, events),
      do:
        Docket.Test.MemoryBackend.commit_unclaimed(
          backend,
          scope,
          expected,
          proposal,
          events
        )
  end

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

  test "timeout after commit is recovered by exact receipt replay" do
    now = ~U[2026-07-24 00:00:00.000000Z]
    {:ok, backend} = Docket.Test.MemoryBackend.start_link(clock: fn -> now end)
    {:ok, gate} = Agent.start_link(fn -> true end)

    on_exit(fn ->
      if Process.alive?(backend), do: Agent.stop(backend)
      if Process.alive?(gate), do: Agent.stop(gate)
    end)

    graph = Docket.Graph.new!(id: "ambiguous-graph")

    graph_hash =
      :graph
      |> Docket.DurableCodec.encode!(graph)
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    assert :ok =
             Docket.Test.MemoryBackend.save_graph(
               backend,
               :tenantless,
               graph.id,
               graph_hash,
               graph
             )

    run = %Docket.Run{
      id: "ambiguous-run",
      graph_id: graph.id,
      graph_hash: graph_hash,
      status: :running,
      input: %{},
      started_at: now,
      updated_at: now,
      checkpoint_seq: 1
    }

    proposal = %{
      transition_id: "ambiguous-transition",
      run: run,
      checkpoint_type: :run_initialized,
      wake_at: now
    }

    assert {:error, {:retryable, :timeout_after_commit}} =
             TimeoutAfterCommitStore.initialize(
               {backend, gate},
               :tenantless,
               proposal,
               []
             )

    assert {:ok, ^run} =
             Docket.Test.MemoryBackend.fetch_run(backend, :tenantless, run.id)

    assert {:ok, ^run} =
             TimeoutAfterCommitStore.initialize(
               {backend, gate},
               :tenantless,
               proposal,
               []
             )

    conflicting = %{proposal | run: %{run | metadata: %{"different" => true}}}

    assert {:error, :conflict} =
             TimeoutAfterCommitStore.initialize(
               {backend, gate},
               :tenantless,
               conflicting,
               []
             )
  end

  if Code.ensure_loaded?(Postgrex) do
    test "PostgreSQL infrastructure errors normalize into the closed algebra" do
      assert {:retryable, :serialization_failure} =
               Docket.Postgres.TransitionError.normalize(%Postgrex.Error{
                 postgres: %{code: :serialization_failure}
               })

      assert {:permanent, {:postgres, :check_violation}} =
               Docket.Postgres.TransitionError.normalize(%Postgrex.Error{
                 postgres: %{code: :check_violation}
               })
    end
  end

  defp encoded_size(term) do
    term
    |> :erlang.term_to_binary([:deterministic, minor_version: 2])
    |> byte_size()
  end
end
