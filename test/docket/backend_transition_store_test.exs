defmodule Docket.Backend.TransitionStoreTest do
  use ExUnit.Case, async: true

  alias Docket.Backend.TransitionStore

  @run %Docket.Run{id: "run", graph_id: "graph", graph_hash: "hash", checkpoint_seq: 1}
  @proposal %{
    run: @run,
    checkpoint_type: :run_initialized,
    wake_at: ~U[2026-07-24 00:00:00Z]
  }

  @durable_run %Docket.Run{
    id: "run",
    graph_id: "graph",
    graph_hash: "hash",
    status: :running,
    input: %{},
    started_at: ~U[2026-07-24 00:00:00.000000Z],
    updated_at: ~U[2026-07-24 00:00:00.000000Z],
    checkpoint_seq: 1
  }
  @durable_proposal %{
    run: @durable_run,
    checkpoint_type: :run_initialized,
    wake_at: ~U[2026-07-24 00:00:00.000000Z]
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

  test "initialize proposals accept only a scheduled initialized running run" do
    assert :ok = TransitionStore.validate(:initialize, 0, @durable_proposal, [])

    for status <- [:created, :waiting, :done, :failed, :cancelled] do
      assert {:error, :invalid_transition} =
               TransitionStore.validate(
                 :initialize,
                 0,
                 %{@durable_proposal | run: %{@durable_run | status: status}},
                 []
               )
    end

    assert {:error, :invalid_transition} =
             TransitionStore.validate(
               :initialize,
               0,
               %{@durable_proposal | run: %{@durable_run | checkpoint_seq: 0}},
               []
             )

    assert {:error, :invalid_transition} =
             TransitionStore.validate(
               :initialize,
               0,
               %{@durable_proposal | run: %{@durable_run | updated_at: nil}},
               []
             )

    assert {:error, :invalid_transition} =
             TransitionStore.validate(:initialize, 0, %{@durable_proposal | wake_at: nil}, [])

    assert {:error, :invalid_transition} =
             TransitionStore.validate(
               :initialize,
               0,
               %{@durable_proposal | checkpoint_type: nil},
               []
             )
  end

  test "terminal failed transitions require the failure payload" do
    failed = %{
      @durable_run
      | status: :failed,
        checkpoint_seq: 2,
        finished_at: ~U[2026-07-24 00:00:01.000000Z]
    }

    proposal = %{
      run: failed,
      checkpoint_type: :step_committed,
      schedule: {:release_claim, :terminal}
    }

    assert {:error, :invalid_transition} =
             TransitionStore.validate(:unclaimed, 1, proposal, [])

    with_failure = %{
      failed
      | failure: %Docket.Run.Failure{code: "boom", message: "node exploded"}
    }

    assert :ok =
             TransitionStore.validate(:unclaimed, 1, %{proposal | run: with_failure}, [])
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
end
