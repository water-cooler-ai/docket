defmodule Docket.MemoryBackendTest do
  use ExUnit.Case, async: true

  alias Docket.{Checkpoint, Event, Run}
  alias Docket.Test.MemoryBackend

  @graph_hash String.duplicate("ab", 32)
  @graph_document %{"id" => "g", "nodes" => []}
  @initial_wake ~U[2026-07-09 12:00:00Z]

  setup do
    {:ok, backend} = MemoryBackend.start_link()
    %{backend: backend}
  end

  defp run(id, opts \\ []) do
    %Run{
      id: id,
      graph_id: "g",
      graph_hash: @graph_hash,
      status: Keyword.get(opts, :status, :running),
      input: %{},
      checkpoint_seq: Keyword.get(opts, :checkpoint_seq, 1)
    }
  end

  defp checkpoint(run, opts \\ []) do
    %Checkpoint{
      type: Keyword.get(opts, :type, :step_committed),
      delivery: Keyword.get(opts, :delivery, :async),
      seq: run.checkpoint_seq,
      run: run,
      events: Keyword.get(opts, :events, []),
      created_at: DateTime.utc_now()
    }
  end

  defp initialize(backend, run, opts \\ []) do
    cp = checkpoint(run, type: :run_initialized, delivery: :sync)

    MemoryBackend.initialize_run(
      backend,
      run.graph_id,
      run.graph_hash,
      @graph_document,
      cp,
      @initial_wake,
      opts
    )
  end

  defp commit(backend, checkpoint, token, disposition, expected_seq \\ nil) do
    fence = %{
      expected_seq: expected_seq || checkpoint.seq - 1,
      claim_token: token
    }

    MemoryBackend.commit(backend, checkpoint, fence, disposition, [])
  end

  defp event(run_id, seq) do
    %Event{
      run_id: run_id,
      seq: seq,
      type: :node_completed,
      step: seq,
      timestamp: DateTime.utc_now()
    }
  end

  test "initialize atomically stores graph, run, checkpoint, schedule, and events", %{backend: b} do
    r = run("r1")
    assert {:ok, ^r} = initialize(b, r)
    assert {:ok, ^r} = MemoryBackend.fetch_run(b, "r1", [])
    assert {:ok, @graph_document} = MemoryBackend.fetch_graph(b, "g", @graph_hash, [])
    assert [%Checkpoint{type: :run_initialized}] = MemoryBackend.checkpoints(b, "r1")
    assert @initial_wake == MemoryBackend.wake_at(b, "r1")
    assert {:error, :not_found} = MemoryBackend.fetch_run(b, "missing", [])
  end

  test "duplicate run id errors without changing the stored graph", %{backend: b} do
    r = run("r1")
    assert {:ok, ^r} = initialize(b, r)
    assert {:error, :already_exists} = initialize(b, r)
    assert {:ok, @graph_document} = MemoryBackend.fetch_graph(b, "g", @graph_hash, [])
  end

  test "the same graph key cannot be reused with different content", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))
    r2 = run("r2")
    cp2 = checkpoint(r2, type: :run_initialized, delivery: :sync)

    assert {:error, :graph_hash_conflict} =
             MemoryBackend.initialize_run(
               b,
               "g",
               @graph_hash,
               %{"id" => "different"},
               cp2,
               @initial_wake,
               []
             )

    assert {:error, :not_found} = MemoryBackend.fetch_run(b, "r2", [])
  end

  test "tenant scoping", %{backend: b} do
    r = run("r1")
    assert {:ok, ^r} = initialize(b, r, tenant_id: "t1")
    assert {:ok, ^r} = MemoryBackend.fetch_run(b, "r1", tenant_id: "t1")
    assert {:error, :not_found} = MemoryBackend.fetch_run(b, "r1", tenant_id: "t2")
    assert {:ok, ^r} = MemoryBackend.fetch_run(b, "r1", [])
  end

  test "commit fence leaves every durable effect untouched on failure", %{backend: b} do
    stored = run("r1", checkpoint_seq: 5)
    {:ok, _} = initialize(b, stored)
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "tok", [])
    next = run("r1", checkpoint_seq: 6)
    cp = checkpoint(next, events: [event("r1", 1)])

    assert {:error, :stale_fence} = commit(b, cp, "tok", :continue, 4)
    assert {:error, :stale_fence} = commit(b, cp, "wrong", :continue, 5)
    assert {:ok, ^stored} = MemoryBackend.fetch_run(b, "r1", [])
    assert [] == MemoryBackend.events(b, "r1")
    assert [_initialized] = MemoryBackend.checkpoints(b, "r1")

    assert {:ok, ^next} = commit(b, cp, "tok", :continue)
    assert [%Event{seq: 1}] = MemoryBackend.events(b, "r1")
    assert [_initialized, ^cp] = MemoryBackend.checkpoints(b, "r1")
  end

  test "claim win, held, refresh, and idempotent token-guarded release", %{backend: b} do
    {:ok, _} = initialize(b, run("r1"))
    assert {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])
    assert {:error, :claim_held} = MemoryBackend.claim_run(b, "r1", "b", [])
    assert :ok = MemoryBackend.refresh_claim(b, "r1", "a", [])
    assert {:error, :claim_lost} = MemoryBackend.refresh_claim(b, "r1", "b", [])
    assert :ok = MemoryBackend.release_claim(b, "r1", "b", [])
    assert "a" = MemoryBackend.claim(b, "r1")
    assert :ok = MemoryBackend.release_claim(b, "r1", "a", [])
    assert nil == MemoryBackend.claim(b, "r1")
    assert :ok = MemoryBackend.release_claim(b, "r1", "a", [])
  end

  test "claim steal invalidates the stale holder", %{backend: _} do
    {:ok, b} = MemoryBackend.start_link(orphan_ttl_ms: 0)
    {:ok, _} = initialize(b, run("r1"))
    assert {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])
    assert {:ok, _} = MemoryBackend.claim_run(b, "r1", "b", [])
    assert "b" = MemoryBackend.claim(b, "r1")
    assert {:error, :claim_lost} = MemoryBackend.refresh_claim(b, "r1", "a", [])

    cp = checkpoint(run("r1", checkpoint_seq: 2))
    assert {:error, :stale_fence} = commit(b, cp, "a", :continue)
  end

  test "mid-drain commit persists effects and retains the claim", %{backend: b} do
    {:ok, _} = initialize(b, run("r1"))
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])
    cp = checkpoint(run("r1", checkpoint_seq: 2), events: [event("r1", 1)])

    assert {:ok, committed} = commit(b, cp, "a", :continue)
    assert committed.checkpoint_seq == 2
    assert [%Event{seq: 1}] = MemoryBackend.events(b, "r1")
    assert "a" = MemoryBackend.claim(b, "r1")
  end

  test "park commit atomically releases the claim and records wake_at", %{backend: b} do
    {:ok, _} = initialize(b, run("r1"))
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])
    wake = ~U[2026-07-10 12:00:00Z]
    cp = checkpoint(run("r1", checkpoint_seq: 2))

    assert {:ok, _} = commit(b, cp, "a", {:park, wake})
    assert nil == MemoryBackend.claim(b, "r1")
    assert ^wake = MemoryBackend.wake_at(b, "r1")
  end

  test "signal-style commit revokes a live claim and wins the sequence fence", %{backend: b} do
    {:ok, _} = initialize(b, run("r1"))
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])
    advance = checkpoint(run("r1", checkpoint_seq: 2), events: [event("r1", 1)])
    signal = checkpoint(run("r1", checkpoint_seq: 2), events: [event("r1", 99)])

    assert {:ok, _} = commit(b, signal, nil, {:park, nil})
    assert nil == MemoryBackend.claim(b, "r1")
    assert {:error, :stale_fence} = commit(b, advance, "a", :continue)
    assert [%Event{seq: 99}] = MemoryBackend.events(b, "r1")
  end

  test "mutate_run serializes a signal and atomically revokes the claim", %{backend: b} do
    {:ok, _} = initialize(b, run("r1"))
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])

    mutation = fn current ->
      cancelled = %{
        current
        | status: :cancelled,
          checkpoint_seq: current.checkpoint_seq + 1,
          finished_at: DateTime.utc_now()
      }

      {:commit, checkpoint(cancelled, type: :run_cancelled, delivery: :sync), {:park, nil}}
    end

    assert {:ok, %Run{status: :cancelled, checkpoint_seq: 2}} =
             MemoryBackend.mutate_run(b, "r1", mutation, [])

    assert nil == MemoryBackend.claim(b, "r1")
    assert nil == MemoryBackend.wake_at(b, "r1")

    assert [_initialized, %Checkpoint{type: :run_cancelled}] =
             MemoryBackend.checkpoints(b, "r1")
  end

  test "mutate_run leaves storage untouched on validation or proposal error", %{backend: b} do
    stored = run("r1")
    {:ok, _} = initialize(b, stored, tenant_id: "t1")

    assert {:error, :not_found} =
             MemoryBackend.mutate_run(b, "r1", fn _ -> flunk("must not run") end, tenant_id: "t2")

    assert {:error, :invalid_signal} =
             MemoryBackend.mutate_run(b, "r1", fn _ -> {:error, :invalid_signal} end,
               tenant_id: "t1"
             )

    assert {:ok, ^stored} = MemoryBackend.fetch_run(b, "r1", [])
    assert [_initialized] = MemoryBackend.checkpoints(b, "r1")
  end

  test "retry_poisoned_run changes only operational state and wake", %{backend: b} do
    stored = run("r1")
    {:ok, _} = initialize(b, stored)
    :ok = MemoryBackend.poison(b, "r1")
    assert :poisoned == MemoryBackend.operational_status(b, "r1")

    assert {:ok, ^stored} = MemoryBackend.retry_poisoned_run(b, "r1", [])
    assert :active == MemoryBackend.operational_status(b, "r1")
    assert %DateTime{} = MemoryBackend.wake_at(b, "r1")
    assert {:ok, ^stored} = MemoryBackend.fetch_run(b, "r1", [])
    assert [_initialized] = MemoryBackend.checkpoints(b, "r1")
  end

  test "expiry alone does not invalidate an unstolen token", %{backend: _} do
    {:ok, b} = MemoryBackend.start_link(orphan_ttl_ms: 0)
    {:ok, _} = initialize(b, run("r1"))
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])
    cp = checkpoint(run("r1", checkpoint_seq: 2))
    assert {:ok, _} = commit(b, cp, "a", :continue)
    assert "a" = MemoryBackend.claim(b, "r1")
  end

  test "commit on an unknown run is not_found", %{backend: b} do
    cp = checkpoint(run("missing", checkpoint_seq: 2))
    assert {:error, :not_found} = commit(b, cp, nil, {:park, nil})
  end
end
