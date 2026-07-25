defmodule Docket.Backend.TransitionStoreTest do
  use ExUnit.Case, async: true

  alias Docket.Backend.TransitionStore

  @run %Docket.Run{id: "run", graph_id: "graph", graph_hash: "hash", checkpoint_seq: 1}
  @proposal %{
    run: @run,
    checkpoint_type: :run_initialized,
    wake_at: ~U[2026-07-24 00:00:00Z]
  }

  test "malformed proposals are rejected before any lookup" do
    assert {:error, :invalid_transition} =
             TransitionStore.validate(:initialize, 0, Map.delete(@proposal, :wake_at), [])

    assert {:error, :invalid_transition} =
             TransitionStore.validate(:initialize, 0, %{@proposal | checkpoint_type: :other}, [])

    assert {:error, :invalid_transition} =
             TransitionStore.validate(:claimed, 0, @proposal, [])

    assert {:error, :invalid_transition} =
             TransitionStore.validate(:initialize, 0, @proposal, [:not_an_event])
  end

  test "an ambiguous initialize outcome surfaces as a conflict on retry" do
    now = ~U[2026-07-24 00:00:00.000000Z]
    {:ok, backend} = Docket.Test.MemoryBackend.start_link(clock: fn -> now end)

    on_exit(fn ->
      if Process.alive?(backend), do: Agent.stop(backend)
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

    proposal = %{run: run, checkpoint_type: :run_initialized, wake_at: now}

    assert {:ok, ^run} =
             Docket.Test.MemoryBackend.initialize(backend, :tenantless, proposal, [])

    assert {:ok, ^run} =
             Docket.Test.MemoryBackend.fetch_run(backend, :tenantless, run.id)

    assert {:error, :conflict} =
             Docket.Test.MemoryBackend.initialize(backend, :tenantless, proposal, [])
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
end
