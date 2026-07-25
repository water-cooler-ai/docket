defmodule Docket.MemoryBackendTest do
  use ExUnit.Case, async: true

  alias Docket.{Event, Run}
  alias Docket.Test.MemoryBackend

  @graph %Docket.Graph{id: "g"}
  @graph_hash @graph
              |> then(&Docket.DurableCodec.encode!(:graph, &1))
              |> then(&:crypto.hash(:sha256, &1))
              |> Base.encode16(case: :lower)
  @initial_wake ~U[2026-07-09 11:00:00Z]
  @now ~U[2026-07-09 12:00:00Z]

  setup do
    {:ok, backend} = MemoryBackend.start_link()
    %{backend: backend}
  end

  defp run(id, opts \\ []) do
    %Run{
      id: id,
      graph_id: Keyword.get(opts, :graph_id, "g"),
      graph_hash: @graph_hash,
      status: Keyword.get(opts, :status, :running),
      input: %{},
      started_at: @initial_wake,
      updated_at: @initial_wake,
      checkpoint_seq: Keyword.get(opts, :checkpoint_seq, 1)
    }
  end

  defp event(run_id, seq, opts \\ []) do
    %Event{
      run_id: run_id,
      seq: seq,
      type: Keyword.get(opts, :type, :node_completed),
      step: Keyword.get(opts, :step, seq),
      timestamp: @now,
      payload: Keyword.get(opts, :payload, %{}),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  defp initialize(backend, run, opts \\ []) do
    owner_scope = Keyword.get(opts, :scope, :tenantless)
    graph = Keyword.get(opts, :graph, @graph)
    wake_at = Keyword.get(opts, :wake_at, @initial_wake)
    events = Keyword.get(opts, :events, [])

    with :ok <-
           MemoryBackend.save_graph(
             backend,
             owner_scope,
             run.graph_id,
             run.graph_hash,
             graph
           ) do
      MemoryBackend.initialize(
        backend,
        owner_scope,
        %{run: run, checkpoint_type: :run_initialized, wake_at: wake_at},
        events
      )
    end
  end

  defp claim_policy(now, opts) do
    %{
      now: now,
      limit: Keyword.get(opts, :limit, 10),
      orphan_ttl_ms: Keyword.get(opts, :orphan_ttl_ms, 60_000),
      max_claim_attempts: Keyword.get(opts, :max_claim_attempts, 3),
      preference: Keyword.get(opts, :preference)
    }
  end

  defp claim_due(backend, now, opts) do
    MemoryBackend.claim_due(backend, :system, claim_policy(now, opts))
  end

  defp abandon_policy(now, retry_at, opts) do
    %{
      expected_checkpoint_seq: Keyword.get(opts, :expected_checkpoint_seq, 1),
      now: now,
      retry_at: retry_at,
      max_claim_abandons: Keyword.get(opts, :max_claim_abandons, 3)
    }
  end

  defp claim_one(backend, now, opts \\ []) do
    assert {:ok, %{leases: [lease], poisoned: []}} =
             claim_due(backend, now, Keyword.put(opts, :limit, 1))

    lease
  end

  test "backend is one bundle for compatible capabilities", %{backend: backend} do
    refute function_exported?(MemoryBackend, :storage, 0)
    assert MemoryBackend.graphs() == MemoryBackend
    assert MemoryBackend.runs() == MemoryBackend
    assert MemoryBackend.events() == MemoryBackend

    opts = [name: {:global, {:memory_backend, backend}}]

    assert %{start: {MemoryBackend, :start_link, [^opts]}} =
             MemoryBackend.child_spec(opts, backend)
  end

  test "initialization composes graph, run, schedule, and assigned events", %{backend: b} do
    initialized = run("r1")
    retained = event("r1", 7)

    assert {:ok, ^initialized} = initialize(b, initialized, events: [retained])
    assert {:ok, ^initialized} = MemoryBackend.fetch_run(b, :tenantless, "r1")
    assert {:ok, @graph} = MemoryBackend.fetch_graph(b, :tenantless, "g", @graph_hash)
    assert [^retained] = MemoryBackend.events(b, :system, "r1")
    assert @initial_wake == MemoryBackend.wake_at(b, "r1")
  end

  test "graph storage is content-addressed and structurally idempotent", %{backend: b} do
    first = %{@graph | metadata: %{"a" => 1, "b" => 2}}
    equal = %{@graph | metadata: %{"b" => 2, "a" => 1}}
    different = %{@graph | metadata: %{"a" => 2}}
    graph_hash = durable_hash(first)

    assert :ok = MemoryBackend.save_graph(b, :tenantless, "g", graph_hash, first)
    assert :ok = MemoryBackend.save_graph(b, :tenantless, "g", graph_hash, equal)

    assert {:error, :invalid_graph_hash} =
             MemoryBackend.save_graph(b, :tenantless, "g", graph_hash, different)

    assert {:ok, ^first} = MemoryBackend.fetch_graph(b, :tenantless, "g", graph_hash)
  end

  test "latest graph lookup follows distinct publication order", %{backend: b} do
    first = %{@graph | metadata: %{"revision" => 1}}
    second = %{@graph | metadata: %{"revision" => 2}}
    first_hash = durable_hash(first)
    second_hash = durable_hash(second)

    assert :ok = MemoryBackend.save_graph(b, :tenantless, "g", first_hash, first)
    assert :ok = MemoryBackend.save_graph(b, :tenantless, "g", second_hash, second)
    assert :ok = MemoryBackend.save_graph(b, :tenantless, "g", first_hash, first)

    assert {:ok, %Docket.GraphRef{graph_id: "g", graph_hash: ^second_hash}} =
             MemoryBackend.fetch_latest_graph_ref(b, :tenantless, "g")

    assert {:ok, %Docket.GraphVersionPage{versions: versions}} =
             MemoryBackend.list_graph_versions(b, :tenantless, "g", %{limit: 10, before: nil})

    assert Enum.map(versions, & &1.ref.graph_hash) == [second_hash, first_hash]
    assert {:error, :not_found} = MemoryBackend.fetch_latest_graph_ref(b, :tenantless, "missing")
  end

  defp durable_hash(graph) do
    graph
    |> then(&Docket.DurableCodec.encode!(:graph, &1))
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  test "inspect_run exposes operational state but never the claim token", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))
    lease = claim_one(b, @now)

    assert {:ok, %Docket.RunInfo{} = info} = MemoryBackend.inspect_run(b, :system, "r1")
    assert info.run.id == "r1"
    assert info.claimed_at == @now
    assert info.claim_attempts == 1
    assert info.wake_at == nil
    refute Docket.RunInfo.poisoned?(info)
    refute Map.has_key?(info, :claim_token)
    assert lease.claim_token == MemoryBackend.claim(b, "r1")
  end

  test "claim_due batches ready and expired candidates with backend-minted tokens", %{backend: b} do
    assert {:ok, _} = initialize(b, run("a"))
    assert {:ok, _} = initialize(b, run("b"))

    first = claim_one(b, @now, limit: 1)
    assert first.run_id == "a"
    assert first.claim_attempt == 1
    assert is_binary(first.claim_token) and first.claim_token != ""
    assert MemoryBackend.wake_at(b, "a") == nil

    later = DateTime.add(@now, 61, :second)

    assert {:ok, %{leases: leases, poisoned: []}} =
             claim_due(b, later, limit: 2, orphan_ttl_ms: 60_000)

    assert Enum.map(leases, & &1.run_id) |> Enum.sort() == ["a", "b"]
    stolen = Enum.find(leases, &(&1.run_id == "a"))
    assert stolen.claim_token != first.claim_token
    assert stolen.claim_attempt == 2
  end

  test "claim_due reserves one outcome per non-empty class at demand two or more", %{backend: b} do
    victim_wake = DateTime.add(@now, -2000, :second)
    assert {:ok, _} = initialize(b, run("victim"), wake_at: victim_wake)
    _lease = claim_one(b, DateTime.add(@now, -1999, :second))

    # Every ready row is older than the expired claim, so oldest-first alone
    # would fill the whole batch from the ready class.
    for id <- ["ready-1", "ready-2", "ready-3"] do
      assert {:ok, _} = initialize(b, run(id), wake_at: DateTime.add(@now, -3000, :second))
    end

    assert {:ok, %{leases: leases, poisoned: []}} =
             claim_due(b, @now, limit: 3, orphan_ttl_ms: 60_000)

    assert Enum.map(leases, & &1.run_id) |> Enum.sort() == ["ready-1", "ready-2", "victim"]
  end

  test "claim_due demand-1 preference serves the named class and falls through", %{backend: b} do
    assert {:ok, _} =
             initialize(b, run("expired-old"), wake_at: DateTime.add(@now, -5000, :second))

    _lease = claim_one(b, DateTime.add(@now, -4000, :second))

    assert {:ok, _} = initialize(b, run("ready-new"), wake_at: DateTime.add(@now, -10, :second))

    # The expired claim is older, but the preference overrides age at demand 1.
    assert claim_one(b, @now, preference: :ready).run_id == "ready-new"
    assert claim_one(b, @now, preference: :expired).run_id == "expired-old"

    assert {:ok, _} = initialize(b, run("ready-only"), wake_at: DateTime.add(@now, -5, :second))

    # Empty preferred class falls through without wasting the demand.
    assert claim_one(b, @now, preference: :expired).run_id == "ready-only"
  end

  test "claim_due excludes future wakes until they become due", %{backend: b} do
    future = DateTime.add(@now, 60, :second)
    assert {:ok, _} = initialize(b, run("future"), wake_at: future)

    assert {:ok, %{leases: [], poisoned: []}} = claim_due(b, @now, limit: 1)

    assert {:ok, %{leases: [%{run_id: "future"}], poisoned: []}} =
             claim_due(b, future, limit: 1)
  end

  test "maximum N launches exactly N attempts before poisoning", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))

    for attempt <- 1..3 do
      now = DateTime.add(@now, attempt, :millisecond)

      assert {:ok, %{leases: [lease], poisoned: []}} =
               claim_due(b, now,
                 limit: 1,
                 orphan_ttl_ms: 0,
                 max_claim_attempts: 3
               )

      assert lease.claim_attempt == attempt
    end

    poison_time = DateTime.add(@now, 4, :millisecond)

    assert {:ok, %{leases: [], poisoned: [poison]}} =
             claim_due(b, poison_time,
               limit: 1,
               orphan_ttl_ms: 0,
               max_claim_attempts: 3
             )

    assert poison.run_id == "r1"
    assert poison.poisoned_at == poison_time

    assert {:ok, info} = MemoryBackend.inspect_run(b, :system, "r1")
    assert info.claim_attempts == 3
    assert info.poisoned_at == poison_time
    assert info.wake_at == nil
    assert MemoryBackend.claim(b, "r1") == nil
    assert {:ok, %Run{checkpoint_seq: 1}} = MemoryBackend.fetch_run(b, :system, "r1")
    assert MemoryBackend.record(b, "r1").latest_checkpoint_type == :run_initialized

    assert {:ok, %{leases: [], poisoned: []}} =
             claim_due(b, DateTime.add(poison_time, 1, :second),
               limit: 1,
               orphan_ttl_ms: 0,
               max_claim_attempts: 3
             )
  end

  test "a claim becomes stealable only after the TTL boundary", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))
    first = claim_one(b, @now)

    boundary = DateTime.add(@now, 60, :second)

    assert {:ok, %{leases: [], poisoned: []}} =
             claim_due(b, boundary, limit: 1, orphan_ttl_ms: 60_000)

    after_boundary = DateTime.add(boundary, 1, :millisecond)

    assert {:ok, %{leases: [stolen], poisoned: []}} =
             claim_due(b, after_boundary, limit: 1, orphan_ttl_ms: 60_000)

    assert stolen.claim_token != first.claim_token
  end

  test "concurrent claim_due calls produce one current lease", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))
    policy = claim_policy(@now, limit: 1, orphan_ttl_ms: 60_000)

    results =
      1..2
      |> Task.async_stream(
        fn _ -> MemoryBackend.claim_due(b, :system, policy) end,
        ordered: false,
        max_concurrency: 2
      )
      |> Enum.map(fn {:ok, result} -> result end)

    leases = for {:ok, %{leases: batch}} <- results, lease <- batch, do: lease
    assert [%{run_id: "r1"}] = leases
  end

  test "refresh and release are token guarded and stale release is harmless", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))
    first = claim_one(b, @now)
    refreshed_at = DateTime.add(@now, 1, :second)

    assert {:error, :claim_lost} =
             MemoryBackend.refresh_claim(b, :system, "r1", "wrong", refreshed_at)

    assert :ok = MemoryBackend.refresh_claim(b, :system, "r1", first.claim_token, refreshed_at)

    # A refresh never moves the claimed time backward: an earlier caller
    # clock succeeds but leaves the fresher stamp in place.
    earlier = DateTime.add(refreshed_at, -10, :second)
    assert :ok = MemoryBackend.refresh_claim(b, :system, "r1", first.claim_token, earlier)
    assert {:ok, info} = MemoryBackend.inspect_run(b, :system, "r1")
    assert info.claimed_at == refreshed_at

    assert :ok = MemoryBackend.release_claim(b, :system, "r1", "wrong", refreshed_at)
    assert MemoryBackend.claim(b, "r1") == first.claim_token

    stolen =
      claim_one(b, DateTime.add(refreshed_at, 1, :millisecond),
        orphan_ttl_ms: 0,
        max_claim_attempts: 3
      )

    assert stolen.claim_token != first.claim_token

    assert :ok =
             MemoryBackend.release_claim(
               b,
               :system,
               "r1",
               first.claim_token,
               refreshed_at
             )

    assert MemoryBackend.claim(b, "r1") == stolen.claim_token
    released_at = DateTime.add(refreshed_at, 1, :second)

    assert :ok =
             MemoryBackend.release_claim(b, :system, "r1", stolen.claim_token, released_at)

    assert MemoryBackend.claim(b, "r1") == nil
    assert MemoryBackend.wake_at(b, "r1") == released_at
  end

  test "pre-execution abandon is fenced, hands the attempt back, and escalates to poison", %{
    backend: b
  } do
    assert {:ok, _} = initialize(b, run("r1"))
    first = claim_one(b, @now)
    abandoned_at = DateTime.add(@now, 1, :second)
    retry_at = DateTime.add(abandoned_at, 30, :second)
    policy = abandon_policy(abandoned_at, retry_at, max_claim_abandons: 2)

    assert_raise ArgumentError, fn ->
      MemoryBackend.abandon_claim(b, :tenantless, "r1", first.claim_token, policy)
    end

    assert_raise ArgumentError, fn ->
      MemoryBackend.abandon_claim(b, :system, "r1", first.claim_token, %{
        policy
        | retry_at: DateTime.add(abandoned_at, -1, :second)
      })
    end

    assert {:ok, :stale} = MemoryBackend.abandon_claim(b, :system, "r1", "wrong", policy)

    assert {:ok, :stale} =
             MemoryBackend.abandon_claim(b, :system, "r1", first.claim_token, %{
               policy
               | expected_checkpoint_seq: 99
             })

    assert MemoryBackend.claim(b, "r1") == first.claim_token

    assert {:ok, :rescheduled} =
             MemoryBackend.abandon_claim(b, :system, "r1", first.claim_token, policy)

    assert {:ok, info} = MemoryBackend.inspect_run(b, :system, "r1")
    assert info.claim_attempts == 0
    assert info.claim_abandons == 1
    assert info.wake_at == retry_at
    assert MemoryBackend.claim(b, "r1") == nil

    second = claim_one(b, retry_at)

    assert {:ok, :rescheduled} =
             MemoryBackend.abandon_claim(b, :system, "r1", second.claim_token, %{
               policy
               | now: retry_at,
                 retry_at: DateTime.add(retry_at, 30, :second)
             })

    third = claim_one(b, DateTime.add(retry_at, 30, :second))

    assert {:ok, :poisoned} =
             MemoryBackend.abandon_claim(b, :system, "r1", third.claim_token, %{
               policy
               | now: DateTime.add(retry_at, 31, :second),
                 retry_at: DateTime.add(retry_at, 60, :second)
             })

    assert {:ok, info} = MemoryBackend.inspect_run(b, :system, "r1")
    assert info.poison_reason == "max_claim_abandons_exceeded"
    assert info.poisoned_at == DateTime.add(retry_at, 31, :second)
    assert info.claim_abandons == 2
    assert info.wake_at == nil
    assert MemoryBackend.claim(b, "r1") == nil

    recovered_at = DateTime.add(retry_at, 90, :second)
    assert {:ok, _} = MemoryBackend.retry_poisoned_run(b, :system, "r1", recovered_at)

    assert {:ok, info} = MemoryBackend.inspect_run(b, :system, "r1")
    assert info.claim_abandons == 0
    assert info.claim_attempts == 0
    refute Docket.RunInfo.poisoned?(info)
  end

  test "retry_poisoned_run is terminal-first and fully resets non-terminal poison", %{backend: b} do
    assert {:ok, _} = initialize(b, run("running"))

    # One launch is permitted, then the next recovery need poisons.
    assert {:ok, %{leases: [_], poisoned: []}} =
             claim_due(b, @now, limit: 1, orphan_ttl_ms: 0, max_claim_attempts: 1)

    poisoned_at = DateTime.add(@now, 1, :millisecond)

    assert {:ok, %{leases: [], poisoned: [_]}} =
             claim_due(b, poisoned_at,
               limit: 1,
               orphan_ttl_ms: 0,
               max_claim_attempts: 1
             )

    retry_at = DateTime.add(@now, 1, :second)

    assert {:ok, %Run{id: "running"}} =
             MemoryBackend.retry_poisoned_run(b, :tenantless, "running", retry_at)

    assert {:ok, info} = MemoryBackend.inspect_run(b, :tenantless, "running")
    assert info.poisoned_at == nil
    assert info.poison_reason == nil
    assert info.claim_attempts == 0
    assert info.wake_at == retry_at
    assert MemoryBackend.claim(b, "running") == nil

    unchanged = MemoryBackend.record(b, "running")

    assert {:ok, %Run{id: "running"}} =
             MemoryBackend.retry_poisoned_run(
               b,
               :tenantless,
               "running",
               DateTime.add(retry_at, 1, :second)
             )

    assert MemoryBackend.record(b, "running") == unchanged

    assert {:ok, _} = initialize(b, run("terminal"))

    done_run = %{run("terminal") | status: :done, checkpoint_seq: 2, finished_at: @now}

    assert {:ok, %Run{id: "terminal", status: :done}} =
             MemoryBackend.commit_unclaimed(
               b,
               :tenantless,
               1,
               %{
                 run: done_run,
                 checkpoint_type: :run_completed,
                 schedule: {:release_claim, :terminal}
               },
               []
             )

    :ok = MemoryBackend.poison(b, "terminal", %{"code" => "test"})

    assert {:error, :inactive_run} =
             MemoryBackend.retry_poisoned_run(b, :tenantless, "terminal", retry_at)
  end

  describe "list_events pages retained events with retention-aware bounds" do
    defp prune_events(backend, run_id, seqs) do
      Agent.update(backend, fn state ->
        update_in(state.runs[run_id].events, &Map.drop(&1, seqs))
      end)
    end

    test "enforces run ownership through scope", %{backend: b} do
      owned = %{run("owned") | event_seq: 2}

      assert {:ok, _} =
               initialize(b, owned,
                 scope: {:tenant, "a"},
                 events: [event("owned", 1), event("owned", 2)]
               )

      opts = %{after_seq: 0, limit: 10}

      assert {:ok, %Docket.EventPage{}} =
               MemoryBackend.list_events(b, {:tenant, "a"}, "owned", opts)

      assert {:error, :not_found} =
               MemoryBackend.list_events(b, {:tenant, "b"}, "owned", opts)

      assert {:error, :not_found} =
               MemoryBackend.list_events(b, :tenantless, "owned", opts)

      assert {:error, :not_found} = MemoryBackend.list_events(b, :system, "missing", opts)
    end

    test "an empty page beyond the latest echoes the cursor and reports no more", %{backend: b} do
      run = %{run("r1") | event_seq: 2}
      assert {:ok, _} = initialize(b, run, events: [event("r1", 1), event("r1", 2)])

      assert {:ok, page} =
               MemoryBackend.list_events(b, :tenantless, "r1", %{after_seq: 2, limit: 10})

      assert page.events == []
      assert page.next_after_seq == 2
      refute page.has_more?
      assert page.oldest_available_seq == 1
      assert page.latest_available_seq == 2
      assert page.latest_seq == 2
    end

    test "honors the default and boundary limits", %{backend: b} do
      run = %{run("r1") | event_seq: 3}
      events = [event("r1", 1), event("r1", 2), event("r1", 3)]
      assert {:ok, _} = initialize(b, run, events: events)

      assert {:ok, %Docket.EventPage{events: ^events, has_more?: false}} =
               MemoryBackend.list_events(b, :tenantless, "r1", %{after_seq: 0, limit: 250})

      assert {:ok, page} =
               MemoryBackend.list_events(b, :tenantless, "r1", %{after_seq: 0, limit: 1})

      assert page.events == [event("r1", 1)]
      assert page.next_after_seq == 1
      assert page.has_more?
    end

    test "paginates a run across pages using next_after_seq", %{backend: b} do
      run = %{run("r1") | event_seq: 5}
      all = for seq <- 1..5, do: event("r1", seq)
      assert {:ok, _} = initialize(b, run, events: all)

      assert {:ok, first} =
               MemoryBackend.list_events(b, :tenantless, "r1", %{after_seq: 0, limit: 2})

      assert first.events == [event("r1", 1), event("r1", 2)]
      assert first.next_after_seq == 2
      assert first.has_more?

      assert {:ok, second} =
               MemoryBackend.list_events(b, :tenantless, "r1", %{
                 after_seq: first.next_after_seq,
                 limit: 2
               })

      assert second.events == [event("r1", 3), event("r1", 4)]
      assert second.next_after_seq == 4
      assert second.has_more?

      assert {:ok, third} =
               MemoryBackend.list_events(b, :tenantless, "r1", %{
                 after_seq: second.next_after_seq,
                 limit: 2
               })

      assert third.events == [event("r1", 5)]
      assert third.next_after_seq == 5
      refute third.has_more?
    end

    test "tolerates ordinary sequence gaps", %{backend: b} do
      run = %{run("r1") | event_seq: 5}
      events = [event("r1", 1), event("r1", 2), event("r1", 5)]
      assert {:ok, _} = initialize(b, run, events: events)

      assert {:ok, page} =
               MemoryBackend.list_events(b, :tenantless, "r1", %{after_seq: 0, limit: 10})

      assert page.events == events
      assert page.oldest_available_seq == 1
      assert page.latest_available_seq == 5
      assert page.next_after_seq == 5
      refute page.has_more?
    end

    test "reflects retention gaps in the oldest available sequence", %{backend: b} do
      run = %{run("r1") | event_seq: 4}
      assert {:ok, _} = initialize(b, run, events: for(seq <- 1..4, do: event("r1", seq)))

      prune_events(b, "r1", [1, 2])

      assert {:ok, page} =
               MemoryBackend.list_events(b, :tenantless, "r1", %{after_seq: 0, limit: 10})

      assert page.events == [event("r1", 3), event("r1", 4)]
      assert page.oldest_available_seq == 3
      assert page.latest_available_seq == 4
      assert page.latest_seq == 4
    end

    test "keeps latest_seq after a fully pruned history", %{backend: b} do
      run = %{run("r1") | event_seq: 4}
      assert {:ok, _} = initialize(b, run, events: for(seq <- 1..4, do: event("r1", seq)))

      prune_events(b, "r1", [1, 2, 3, 4])

      assert {:ok, page} =
               MemoryBackend.list_events(b, :tenantless, "r1", %{after_seq: 0, limit: 10})

      assert page.events == []
      assert page.oldest_available_seq == nil
      assert page.latest_available_seq == nil
      assert page.next_after_seq == 0
      refute page.has_more?
      assert page.latest_seq == 4
    end
  end
end
