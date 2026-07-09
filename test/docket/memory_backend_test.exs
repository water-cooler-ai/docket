defmodule Docket.MemoryBackendTest do
  use ExUnit.Case, async: true

  alias Docket.{Checkpoint, Event, Run}
  alias Docket.Test.MemoryBackend

  setup do
    {:ok, backend} = MemoryBackend.start_link()
    %{backend: backend}
  end

  defp run(id, opts \\ []) do
    %Run{
      id: id,
      graph_id: "g",
      graph_hash: String.duplicate("ab", 32),
      status: Keyword.get(opts, :status, :running),
      input: %{},
      checkpoint_seq: Keyword.get(opts, :checkpoint_seq, 0)
    }
  end

  defp checkpoint(run, opts \\ []) do
    %Checkpoint{
      type: :step_committed,
      delivery: :async,
      seq: run.checkpoint_seq,
      run: run,
      events: Keyword.get(opts, :events, []),
      created_at: DateTime.utc_now()
    }
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

  # --- run store round-trips ------------------------------------------------

  test "insert then fetch round-trips the run; unknown id is not_found", %{backend: b} do
    r = run("r1")
    assert {:ok, ^r} = MemoryBackend.insert_run(b, r, [])
    assert {:ok, ^r} = MemoryBackend.fetch_run(b, "r1", [])
    assert {:error, :not_found} = MemoryBackend.fetch_run(b, "missing", [])
  end

  test "duplicate insert errors", %{backend: b} do
    r = run("r1")
    assert {:ok, ^r} = MemoryBackend.insert_run(b, r, [])
    assert {:error, :already_exists} = MemoryBackend.insert_run(b, r, [])
  end

  test "tenant scoping", %{backend: b} do
    r = run("r1")
    assert {:ok, ^r} = MemoryBackend.insert_run(b, r, tenant_id: "t1")

    assert {:ok, ^r} = MemoryBackend.fetch_run(b, "r1", tenant_id: "t1")
    assert {:error, :not_found} = MemoryBackend.fetch_run(b, "r1", tenant_id: "t2")
    # unscoped fetch by run_id succeeds regardless of stored tenant
    assert {:ok, ^r} = MemoryBackend.fetch_run(b, "r1", [])
  end

  # --- update_run fence -----------------------------------------------------

  test "update_run fence: expected seq succeeds, stale seq and wrong token fail", %{backend: b} do
    stored = run("r1", checkpoint_seq: 5)
    {:ok, _} = MemoryBackend.insert_run(b, stored, [])
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "tok", [])

    next = run("r1", checkpoint_seq: 6)

    # stale seq: stored is 5, expect 4
    assert {:error, :stale_fence} =
             MemoryBackend.update_run(b, next, %{expected_seq: 4, claim_token: nil}, [])

    assert {:ok, ^stored} = MemoryBackend.fetch_run(b, "r1", [])

    # wrong claim token at the right seq
    assert {:error, :stale_fence} =
             MemoryBackend.update_run(b, next, %{expected_seq: 5, claim_token: "nope"}, [])

    assert {:ok, ^stored} = MemoryBackend.fetch_run(b, "r1", [])

    # right seq, right token
    assert {:ok, ^next} =
             MemoryBackend.update_run(b, next, %{expected_seq: 5, claim_token: "tok"}, [])

    assert {:ok, ^next} = MemoryBackend.fetch_run(b, "r1", [])
  end

  # --- claim lifecycle ------------------------------------------------------

  test "claim win, held, refresh, release", %{backend: b} do
    {:ok, _} = MemoryBackend.insert_run(b, run("r1"), [])

    assert {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])
    assert {:error, :claim_held} = MemoryBackend.claim_run(b, "r1", "b", [])
    assert :ok = MemoryBackend.refresh_claim(b, "r1", "a", [])
    assert {:error, :claim_lost} = MemoryBackend.refresh_claim(b, "r1", "b", [])

    # release under a stale token is a no-op :ok, holder undisturbed
    assert :ok = MemoryBackend.release_claim(b, "r1", "b", [])
    assert "a" = MemoryBackend.claim(b, "r1")

    assert :ok = MemoryBackend.release_claim(b, "r1", "a", [])
    assert nil == MemoryBackend.claim(b, "r1")
    # idempotent release
    assert :ok = MemoryBackend.release_claim(b, "r1", "a", [])
  end

  test "claim steal after expiry, and refresh reports claim_lost after steal", %{backend: _} do
    {:ok, b} = MemoryBackend.start_link(orphan_ttl_ms: 0)
    {:ok, _} = MemoryBackend.insert_run(b, run("r1"), [])

    assert {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])
    # ttl 0 means the existing claim is immediately stealable
    assert {:ok, _} = MemoryBackend.claim_run(b, "r1", "b", [])
    assert "b" = MemoryBackend.claim(b, "r1")
    assert {:error, :claim_lost} = MemoryBackend.refresh_claim(b, "r1", "a", [])
  end

  # --- commit ---------------------------------------------------------------

  test "commit mid-drain persists run and events and retains claim", %{backend: b} do
    {:ok, _} = MemoryBackend.insert_run(b, run("r1", checkpoint_seq: 0), [])
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])

    cp = checkpoint(run("r1", checkpoint_seq: 1), events: [event("r1", 1)])

    assert {:ok, committed} = MemoryBackend.commit(b, cp, "a", :continue, [])
    assert committed.checkpoint_seq == 1
    assert {:ok, ^committed} = MemoryBackend.fetch_run(b, "r1", [])
    assert [%Event{seq: 1}] = MemoryBackend.events(b, "r1")
    assert "a" = MemoryBackend.claim(b, "r1")
  end

  test "commit park releases claim and records wake_at", %{backend: b} do
    {:ok, _} = MemoryBackend.insert_run(b, run("r1", checkpoint_seq: 0), [])
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])

    wake = ~U[2026-07-09 12:00:00Z]
    cp = checkpoint(run("r1", checkpoint_seq: 1))

    assert {:ok, _} = MemoryBackend.commit(b, cp, "a", {:park, wake}, [])
    assert nil == MemoryBackend.claim(b, "r1")
    assert ^wake = MemoryBackend.wake_at(b, "r1")

    # a subsequent park with nil wake (terminal / external source)
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "c", [])
    cp2 = checkpoint(run("r1", checkpoint_seq: 2))
    assert {:ok, _} = MemoryBackend.commit(b, cp2, "c", {:park, nil}, [])
    assert nil == MemoryBackend.claim(b, "r1")
    assert nil == MemoryBackend.wake_at(b, "r1")
  end

  test "fence race: signal commit wins and the advance commit gets stale_fence", %{backend: b} do
    {:ok, _} = MemoryBackend.insert_run(b, run("r1", checkpoint_seq: 0), [])
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])

    # advance worker read seq 0, prepared a commit to seq 1 with its token
    advance = checkpoint(run("r1", checkpoint_seq: 1), events: [event("r1", 1)])
    # signal commit (nil token) commits to seq 1 first
    signal = checkpoint(run("r1", checkpoint_seq: 1), events: [event("r1", 99)])

    assert {:ok, _} = MemoryBackend.commit(b, signal, nil, {:park, nil}, [])
    assert {:error, :stale_fence} = MemoryBackend.commit(b, advance, "a", :continue, [])

    # only the signal's events were persisted
    assert [%Event{seq: 99}] = MemoryBackend.events(b, "r1")
  end

  test "expired but unstolen claim can still commit", %{backend: _} do
    {:ok, b} = MemoryBackend.start_link(orphan_ttl_ms: 0)
    {:ok, _} = MemoryBackend.insert_run(b, run("r1", checkpoint_seq: 0), [])
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])

    # ttl 0 makes the claim immediately stealable, but nobody steals it:
    # expiry alone never fails the fence
    cp = checkpoint(run("r1", checkpoint_seq: 1))
    assert {:ok, _} = MemoryBackend.commit(b, cp, "a", :continue, [])
    assert "a" = MemoryBackend.claim(b, "r1")
  end

  test "nil-token :continue commit leaves a live claim untouched", %{backend: b} do
    {:ok, _} = MemoryBackend.insert_run(b, run("r1", checkpoint_seq: 0), [])
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])

    cp = checkpoint(run("r1", checkpoint_seq: 1))
    assert {:ok, _} = MemoryBackend.commit(b, cp, nil, :continue, [])
    assert "a" = MemoryBackend.claim(b, "r1")
  end

  test "stale worker after steal cannot commit even at the right seq", %{backend: _} do
    {:ok, b} = MemoryBackend.start_link(orphan_ttl_ms: 0)
    {:ok, _} = MemoryBackend.insert_run(b, run("r1", checkpoint_seq: 0), [])
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "a", [])
    {:ok, _} = MemoryBackend.claim_run(b, "r1", "b", [])

    cp = checkpoint(run("r1", checkpoint_seq: 1))
    assert {:error, :stale_fence} = MemoryBackend.commit(b, cp, "a", :continue, [])
  end

  test "commit on unknown run is not_found", %{backend: b} do
    cp = checkpoint(run("r1", checkpoint_seq: 1))
    assert {:error, :not_found} = MemoryBackend.commit(b, cp, nil, {:park, nil}, [])
  end

  # --- graph store ----------------------------------------------------------

  test "graph store put/fetch round-trip, idempotent double put, unknown hash", %{backend: b} do
    doc = %{"nodes" => []}
    assert :ok = MemoryBackend.put_graph(b, "g", "h1", doc, [])
    assert {:ok, ^doc} = MemoryBackend.fetch_graph(b, "g", "h1", [])
    # double put of the same {id, hash} is :ok
    assert :ok = MemoryBackend.put_graph(b, "g", "h1", doc, [])
    assert {:ok, ^doc} = MemoryBackend.fetch_graph(b, "g", "h1", [])
    assert {:error, :not_found} = MemoryBackend.fetch_graph(b, "g", "h2", [])
  end

  # --- event persistence ----------------------------------------------------

  test "persist_events appends in order across calls", %{backend: b} do
    {:ok, _} = MemoryBackend.insert_run(b, run("r1"), [])
    assert :ok = MemoryBackend.persist_events(b, "r1", [event("r1", 1), event("r1", 2)], [])
    assert :ok = MemoryBackend.persist_events(b, "r1", [event("r1", 3)], [])
    assert [1, 2, 3] == Enum.map(MemoryBackend.events(b, "r1"), & &1.seq)
  end
end
