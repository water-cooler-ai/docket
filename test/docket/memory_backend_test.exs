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
  @commit_now ~U[2026-07-09 12:30:00Z]

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

    MemoryBackend.transaction(backend, fn transaction ->
      with :ok <-
             MemoryBackend.save_graph(
               transaction,
               owner_scope,
               run.graph_id,
               run.graph_hash,
               graph
             ),
           {:ok, initialized} <-
             MemoryBackend.insert_run(
               transaction,
               owner_scope,
               run,
               :run_initialized,
               wake_at
             ),
           :ok <- MemoryBackend.append_events(transaction, owner_scope, run.id, events) do
        {:ok, initialized}
      end
    end)
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

  defp commit(backend, next_run, token, schedule, opts \\ []) do
    scope = Keyword.get(opts, :scope, :system)
    expected = Keyword.get(opts, :expected_seq, next_run.checkpoint_seq - 1)
    checkpoint_type = Keyword.get(opts, :checkpoint_type, :step_committed)
    events = Keyword.get(opts, :events, [])

    proposal = %{
      run: next_run,
      expected_checkpoint_seq: expected,
      claim_token: token,
      checkpoint_type: checkpoint_type,
      schedule: schedule
    }

    MemoryBackend.transaction(backend, fn transaction ->
      with {:ok, committed} <- MemoryBackend.commit(transaction, scope, proposal),
           :ok <- MemoryBackend.append_events(transaction, scope, next_run.id, events) do
        {:ok, committed}
      end
    end)
  end

  test "backend is one bundle for compatible capabilities", %{backend: backend} do
    assert MemoryBackend.storage() == MemoryBackend
    assert MemoryBackend.graphs() == MemoryBackend
    assert MemoryBackend.runs() == MemoryBackend
    assert MemoryBackend.events() == MemoryBackend

    assert %{start: {MemoryBackend, :start_link, [_opts]}} =
             MemoryBackend.child_spec(name: {:global, {:memory_backend, backend}})
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

  test "durable insertion accepts only a scheduled initialized running run", %{backend: b} do
    assert_raise ArgumentError, ~r/run owner scope must be/, fn ->
      MemoryBackend.insert_run(
        b,
        {:tenant, ""},
        run("empty-tenant"),
        :run_initialized,
        @initial_wake
      )
    end

    for status <- [:created, :waiting, :done, :failed, :cancelled] do
      id = Atom.to_string(status)

      assert {:error, :invalid_run} =
               MemoryBackend.insert_run(
                 b,
                 :tenantless,
                 run(id, status: status),
                 :run_initialized,
                 @initial_wake
               )

      assert {:error, :not_found} = MemoryBackend.fetch_run(b, :system, id)
    end

    assert {:error, :invalid_run} =
             MemoryBackend.insert_run(
               b,
               :tenantless,
               run("missing-checkpoint-type"),
               nil,
               @initial_wake
             )

    assert {:error, :invalid_run} =
             MemoryBackend.insert_run(
               b,
               :tenantless,
               run("wrong-sequence", checkpoint_seq: 0),
               :run_initialized,
               @initial_wake
             )

    assert {:error, :invalid_run} =
             MemoryBackend.insert_run(
               b,
               :tenantless,
               run("wrong-checkpoint-type"),
               :step_committed,
               @initial_wake
             )

    assert {:error, :invalid_run} =
             MemoryBackend.insert_run(
               b,
               :tenantless,
               %{run("missing-updated-at") | updated_at: nil},
               :run_initialized,
               @initial_wake
             )
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

  defp publish_default!(backend) do
    assert :ok = MemoryBackend.save_graph(backend, :tenantless, "g", @graph_hash, @graph)
  end

  test "failed event append rolls graph and run initialization back", %{backend: b} do
    initialized = run("r1")
    mismatched = event("another-run", 1)

    assert {:error, :event_run_mismatch} =
             initialize(b, initialized, events: [mismatched])

    assert {:error, :not_found} =
             MemoryBackend.fetch_graph(b, :tenantless, "g", @graph_hash)

    assert {:error, :not_found} = MemoryBackend.fetch_run(b, :system, "r1")
  end

  test "transaction errors, exceptions, and throws roll back and propagate", %{backend: b} do
    publish_default!(b)

    assert {:error, :stop} =
             MemoryBackend.transaction(b, fn tx ->
               assert {:ok, _} =
                        MemoryBackend.insert_run(
                          tx,
                          :tenantless,
                          run("error"),
                          :run_initialized,
                          @initial_wake
                        )

               {:error, :stop}
             end)

    assert_raise RuntimeError, "boom", fn ->
      MemoryBackend.transaction(b, fn tx ->
        assert {:ok, _} =
                 MemoryBackend.insert_run(
                   tx,
                   :tenantless,
                   run("raise"),
                   :run_initialized,
                   @initial_wake
                 )

        raise "boom"
      end)
    end

    assert catch_throw(
             MemoryBackend.transaction(b, fn tx ->
               assert {:ok, _} =
                        MemoryBackend.insert_run(
                          tx,
                          :tenantless,
                          run("throw"),
                          :run_initialized,
                          @initial_wake
                        )

               throw(:boom)
             end)
           ) == :boom

    for id <- ~w(error raise throw) do
      assert {:error, :not_found} = MemoryBackend.fetch_run(b, :system, id)
    end
  end

  test "nested transactions join the outer transaction", %{backend: b} do
    publish_default!(b)

    assert {:ok, :outer} =
             MemoryBackend.transaction(b, fn tx ->
               assert {:ok, _} =
                        MemoryBackend.insert_run(
                          tx,
                          :tenantless,
                          run("outer"),
                          :run_initialized,
                          @initial_wake
                        )

               assert {:ok, :inner} =
                        MemoryBackend.transaction(tx, fn nested ->
                          assert {:ok, _} =
                                   MemoryBackend.insert_run(
                                     nested,
                                     :tenantless,
                                     run("inner"),
                                     :run_initialized,
                                     @initial_wake
                                   )

                          {:ok, :inner}
                        end)

               {:ok, :outer}
             end)

    assert {:ok, %Run{id: "outer"}} = MemoryBackend.fetch_run(b, :system, "outer")
    assert {:ok, %Run{id: "inner"}} = MemoryBackend.fetch_run(b, :system, "inner")
  end

  test "overlapping transactions cannot erase each other's commits", %{backend: b} do
    publish_default!(b)
    parent = self()

    first =
      Task.async(fn ->
        MemoryBackend.transaction(b, fn tx ->
          send(parent, :first_entered)

          receive do
            :release_first -> :ok
          end

          assert {:ok, _} =
                   MemoryBackend.insert_run(
                     tx,
                     :tenantless,
                     run("first"),
                     :run_initialized,
                     @initial_wake
                   )

          {:ok, :first}
        end)
      end)

    assert_receive :first_entered

    second =
      Task.async(fn ->
        send(parent, :second_attempting)

        MemoryBackend.transaction(b, fn tx ->
          send(parent, :second_entered)

          assert {:ok, _} =
                   MemoryBackend.insert_run(
                     tx,
                     :tenantless,
                     run("second"),
                     :run_initialized,
                     @initial_wake
                   )

          {:ok, :second}
        end)
      end)

    assert_receive :second_attempting
    refute_receive :second_entered, 100
    send(first.pid, :release_first)
    assert {:ok, :first} = Task.await(first)
    assert_receive :second_entered, 1_000
    assert {:ok, :second} = Task.await(second)

    assert {:ok, %Run{id: "first"}} = MemoryBackend.fetch_run(b, :system, "first")
    assert {:ok, %Run{id: "second"}} = MemoryBackend.fetch_run(b, :system, "second")
  end

  test "a rolled-back overlapping transaction cannot erase a later commit", %{backend: b} do
    publish_default!(b)
    parent = self()

    first =
      Task.async(fn ->
        MemoryBackend.transaction(b, fn tx ->
          send(parent, :rollback_entered)

          receive do
            :release_rollback -> :ok
          end

          assert {:ok, _} =
                   MemoryBackend.insert_run(
                     tx,
                     :tenantless,
                     run("rolled-back"),
                     :run_initialized,
                     @initial_wake
                   )

          {:error, :rollback}
        end)
      end)

    assert_receive :rollback_entered

    second =
      Task.async(fn ->
        send(parent, :commit_attempting)

        MemoryBackend.transaction(b, fn tx ->
          send(parent, :commit_entered)

          with {:ok, _} <-
                 MemoryBackend.insert_run(
                   tx,
                   :tenantless,
                   run("committed"),
                   :run_initialized,
                   @initial_wake
                 ) do
            {:ok, :committed}
          end
        end)
      end)

    assert_receive :commit_attempting
    refute_receive :commit_entered, 100
    send(first.pid, :release_rollback)
    assert {:error, :rollback} = Task.await(first)
    assert_receive :commit_entered, 1_000
    assert {:ok, :committed} = Task.await(second)

    assert {:error, :not_found} = MemoryBackend.fetch_run(b, :system, "rolled-back")
    assert {:ok, %Run{id: "committed"}} = MemoryBackend.fetch_run(b, :system, "committed")
  end

  test "an overlapping direct root write cannot be overwritten by a transaction", %{backend: b} do
    publish_default!(b)
    parent = self()

    transaction =
      Task.async(fn ->
        MemoryBackend.transaction(b, fn tx ->
          send(parent, :transaction_entered)

          receive do
            :release_transaction -> :ok
          end

          with {:ok, _} <-
                 MemoryBackend.insert_run(
                   tx,
                   :tenantless,
                   run("transaction"),
                   :run_initialized,
                   @initial_wake
                 ) do
            {:ok, :transaction}
          end
        end)
      end)

    assert_receive :transaction_entered

    direct =
      Task.async(fn ->
        send(parent, :direct_attempting)

        result =
          MemoryBackend.insert_run(
            b,
            :tenantless,
            run("direct"),
            :run_initialized,
            @initial_wake
          )

        send(parent, :direct_finished)
        result
      end)

    assert_receive :direct_attempting
    refute_receive :direct_finished, 100
    send(transaction.pid, :release_transaction)
    assert {:ok, :transaction} = Task.await(transaction)
    assert {:ok, %Run{id: "direct"}} = Task.await(direct)
    assert_receive :direct_finished

    assert {:ok, %Run{id: "transaction"}} =
             MemoryBackend.fetch_run(b, :system, "transaction")

    assert {:ok, %Run{id: "direct"}} = MemoryBackend.fetch_run(b, :system, "direct")
  end

  test "scope is explicit and cannot fail open", %{backend: b} do
    assert {:ok, _} = initialize(b, run("tenantless"))
    assert {:ok, _} = initialize(b, run("tenant"), scope: {:tenant, "t1"})

    assert {:ok, _} = MemoryBackend.fetch_run(b, :system, "tenantless")
    assert {:ok, _} = MemoryBackend.fetch_run(b, :system, "tenant")
    assert {:ok, _} = MemoryBackend.fetch_run(b, :tenantless, "tenantless")
    assert {:error, :not_found} = MemoryBackend.fetch_run(b, :tenantless, "tenant")
    assert {:ok, _} = MemoryBackend.fetch_run(b, {:tenant, "t1"}, "tenant")
    assert {:error, :not_found} = MemoryBackend.fetch_run(b, {:tenant, "t2"}, "tenant")

    assert_raise ArgumentError, ~r/scope must be/, fn ->
      MemoryBackend.fetch_run(b, nil, "tenant")
    end
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

  test "empty event append is a no-op and assigned event replay is idempotent", %{backend: b} do
    assert :ok = MemoryBackend.append_events(b, :tenantless, "missing", [])
    assert {:ok, _} = initialize(b, run("r1"))

    checkpoint_fact =
      event("r1", 41,
        type: :checkpoint_committed,
        step: 3,
        metadata: %{
          "checkpoint_seq" => 2,
          "checkpoint_type" => "step_committed",
          "park_reason" => "budget",
          "wake_disposition" => "immediate"
        }
      )

    assert :ok = MemoryBackend.append_events(b, :tenantless, "r1", [checkpoint_fact])
    assert :ok = MemoryBackend.append_events(b, :tenantless, "r1", [checkpoint_fact])
    assert [^checkpoint_fact] = MemoryBackend.events(b, :tenantless, "r1")
    assert checkpoint_fact.seq != checkpoint_fact.metadata["checkpoint_seq"]

    conflicting = put_in(checkpoint_fact.metadata["checkpoint_seq"], 3)

    assert {:error, :event_conflict} =
             MemoryBackend.append_events(b, :tenantless, "r1", [conflicting])

    assert {:error, :event_run_mismatch} =
             MemoryBackend.append_events(b, :tenantless, "r1", [event("other", 42)])

    assert {:error, :not_found} =
             MemoryBackend.append_events(b, {:tenant, "t1"}, "r1", [event("r1", 42)])

    earlier = event("r1", 3)
    middle = event("r1", 10)
    assert :ok = MemoryBackend.append_events(b, :tenantless, "r1", [middle, earlier])

    assert {:ok, %Docket.EventPage{events: [^middle, ^checkpoint_fact]}} =
             MemoryBackend.list_events(b, :tenantless, "r1", %{after_seq: 3, limit: 2})

    assert [^earlier, ^middle, ^checkpoint_fact] =
             MemoryBackend.events(b, :tenantless, "r1")
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

  test "advance commit requires the current non-nil token and exact next sequence", %{backend: b} do
    stored = run("r1", checkpoint_seq: 5)
    assert {:ok, _} = initialize(b, stored)
    lease = claim_one(b, @now)
    next = run("r1", checkpoint_seq: 6)
    retained = event("r1", 20)

    assert {:error, :invalid_commit} =
             commit(b, next, nil, :retain_claim, events: [retained])

    assert {:error, :stale_fence} =
             commit(b, next, "wrong", :retain_claim, events: [retained])

    assert {:error, :invalid_commit} =
             commit(b, next, lease.claim_token, :retain_claim,
               expected_seq: 4,
               events: [retained]
             )

    assert {:error, :stale_fence} =
             commit(b, run("r1", checkpoint_seq: 5), lease.claim_token, :retain_claim,
               expected_seq: 4,
               events: [retained]
             )

    jumped = run("r1", checkpoint_seq: 7)

    assert {:error, :invalid_commit} =
             commit(b, jumped, lease.claim_token, :retain_claim,
               expected_seq: 5,
               events: [retained]
             )

    assert {:ok, ^stored} = MemoryBackend.fetch_run(b, :system, "r1")
    assert [] == MemoryBackend.events(b, :system, "r1")

    assert {:ok, ^next} =
             commit(b, next, lease.claim_token, :retain_claim,
               checkpoint_type: :step_committed,
               events: [retained]
             )

    assert [^retained] = MemoryBackend.events(b, :system, "r1")
    assert MemoryBackend.claim(b, "r1") == lease.claim_token
    assert {:ok, %{claim_attempts: 0}} = MemoryBackend.inspect_run(b, :system, "r1")
    assert MemoryBackend.record(b, "r1").latest_checkpoint_type == :step_committed
  end

  test "event failure rolls an otherwise valid run commit back", %{backend: b} do
    stored = run("r1")
    assert {:ok, _} = initialize(b, stored)
    lease = claim_one(b, @now)
    next = run("r1", checkpoint_seq: 2)

    assert {:error, :event_run_mismatch} =
             commit(b, next, lease.claim_token, :retain_claim, events: [event("other", 1)])

    assert {:ok, ^stored} = MemoryBackend.fetch_run(b, :system, "r1")
    assert MemoryBackend.claim(b, "r1") == lease.claim_token
    assert [] == MemoryBackend.events(b, :system, "r1")
  end

  test "commits and mutations cannot rebind immutable graph or start identity", %{backend: b} do
    stored = run("r1")
    assert {:ok, _} = initialize(b, stored)
    lease = claim_one(b, @now)

    rebound = %{run("r1", checkpoint_seq: 2) | graph_id: "other"}

    assert {:error, :invalid_commit} =
             commit(b, rebound, lease.claim_token, :retain_claim)

    moved = %{run("r1", checkpoint_seq: 2) | started_at: DateTime.add(@initial_wake, 1, :second)}

    assert {:error, :invalid_commit} =
             commit(b, moved, lease.claim_token, :retain_claim)

    assert {:error, :invalid_commit} =
             commit(b, run("r1", checkpoint_seq: 2), lease.claim_token, :retain_claim,
               checkpoint_type: nil
             )

    assert {:error, :invalid_mutation} =
             MemoryBackend.mutate_run(b, :tenantless, "r1", fn current ->
               rebound = %{
                 current
                 | graph_hash: String.duplicate("cd", 32),
                   checkpoint_seq: current.checkpoint_seq + 1
               }

               {:commit, rebound, :interrupt_resolved, {:release_claim, :immediate}, :rebound}
             end)

    assert {:error, :invalid_mutation} =
             MemoryBackend.mutate_run(b, :tenantless, "r1", fn current ->
               moved = %{
                 current
                 | started_at: DateTime.add(current.started_at, 1, :second),
                   checkpoint_seq: current.checkpoint_seq + 1
               }

               {:commit, moved, :interrupt_resolved, {:release_claim, :immediate}, :moved}
             end)

    assert {:ok, ^stored} = MemoryBackend.fetch_run(b, :system, "r1")
    assert MemoryBackend.claim(b, "r1") == lease.claim_token
  end

  test "two commits under the same fence produce one durable winner", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))
    lease = claim_one(b, @now)

    results =
      [10, 20]
      |> Task.async_stream(
        fn seq ->
          next = %{run("r1", checkpoint_seq: 2) | metadata: %{"winner" => seq}}

          commit(b, next, lease.claim_token, {:release_claim, :immediate},
            events: [event("r1", seq)]
          )
        end,
        ordered: false,
        max_concurrency: 2
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &match?({:error, :stale_fence}, &1)) == 1
    assert [_winner] = MemoryBackend.events(b, :system, "r1")
  end

  test "commit schedule effects retain, wake, and park without ambiguity", %{backend: _} do
    {:ok, b} = MemoryBackend.start_link(clock: fn -> @commit_now end)

    for id <- ~w(retain immediate future external terminal) do
      assert {:ok, _} = initialize(b, run(id))
    end

    first = claim_one(b, @now, limit: 1)

    # Claim deterministically in run-id order through separate due scans.
    leases =
      [first | claim_all_remaining(b, DateTime.add(@now, 1, :second), 4)]
      |> Map.new(&{&1.run_id, &1})

    assert {:ok, _} =
             commit(
               b,
               run("retain", checkpoint_seq: 2),
               leases["retain"].claim_token,
               :retain_claim
             )

    assert MemoryBackend.claim(b, "retain") == leases["retain"].claim_token
    assert {:ok, %{claimed_at: @commit_now}} = MemoryBackend.inspect_run(b, :system, "retain")

    assert {:ok, _} =
             commit(
               b,
               run("immediate", checkpoint_seq: 2),
               leases["immediate"].claim_token,
               {:release_claim, :immediate}
             )

    assert MemoryBackend.wake_at(b, "immediate") == @commit_now
    assert MemoryBackend.claim(b, "immediate") == nil

    future = ~U[2026-07-10 12:00:00Z]

    assert {:ok, _} =
             commit(
               b,
               run("future", checkpoint_seq: 2),
               leases["future"].claim_token,
               {:release_claim, {:at, future}}
             )

    assert MemoryBackend.wake_at(b, "future") == future
    assert MemoryBackend.claim(b, "future") == nil

    assert {:ok, _} =
             commit(
               b,
               run("external", checkpoint_seq: 2, status: :waiting),
               leases["external"].claim_token,
               {:release_claim, :external}
             )

    assert MemoryBackend.wake_at(b, "external") == nil
    assert MemoryBackend.claim(b, "external") == nil

    done = run("terminal", checkpoint_seq: 2, status: :done)

    assert {:ok, _} =
             commit(
               b,
               done,
               leases["terminal"].claim_token,
               {:release_claim, :terminal}
             )

    assert MemoryBackend.wake_at(b, "terminal") == nil
    assert MemoryBackend.claim(b, "terminal") == nil
  end

  test "a failed terminal commit requires its failure payload", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))
    lease = claim_one(b, @now)

    missing_failure = run("r1", checkpoint_seq: 2, status: :failed)

    assert {:error, :invalid_commit} =
             commit(b, missing_failure, lease.claim_token, {:release_claim, :terminal},
               checkpoint_type: :run_failed
             )

    failed = %{
      missing_failure
      | failure: Docket.Run.Failure.new("node_failed", "node n1 failed permanently")
    }

    assert {:ok, _} =
             commit(b, failed, lease.claim_token, {:release_claim, :terminal},
               checkpoint_type: :run_failed
             )

    assert {:ok, %Run{failure: %Docket.Run.Failure{code: "node_failed"}}} =
             MemoryBackend.fetch_run(b, :system, "r1")
  end

  defp claim_all_remaining(backend, now, limit) do
    assert {:ok, %{leases: leases, poisoned: []}} =
             claim_due(backend, now, limit: limit, orphan_ttl_ms: 60_000)

    leases
  end

  test "serialized mutation returns opaque commit data and revokes a live claim", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))
    _lease = claim_one(b, @now)
    retained = event("r1", 1)

    mutation = fn current ->
      next = %{current | checkpoint_seq: current.checkpoint_seq + 1}
      {:commit, next, :interrupt_resolved, {:release_claim, :immediate}, [retained]}
    end

    assert {:ok, {:committed, [^retained]}} =
             MemoryBackend.transaction(b, fn tx ->
               with {:ok, {:committed, events}} <-
                      MemoryBackend.mutate_run(tx, :tenantless, "r1", mutation),
                    :ok <- MemoryBackend.append_events(tx, :tenantless, "r1", events) do
                 {:ok, {:committed, events}}
               end
             end)

    assert {:ok, %Run{checkpoint_seq: 2}} = MemoryBackend.fetch_run(b, :tenantless, "r1")
    assert MemoryBackend.claim(b, "r1") == nil
    assert %DateTime{} = MemoryBackend.wake_at(b, "r1")
    assert [^retained] = MemoryBackend.events(b, :tenantless, "r1")
  end

  test "serialized mutation event failure rolls the row and claim back", %{backend: b} do
    stored = run("r1")
    assert {:ok, _} = initialize(b, stored)
    lease = claim_one(b, @now)

    mutation = fn current ->
      next = %{current | checkpoint_seq: current.checkpoint_seq + 1}

      {:commit, next, :interrupt_resolved, {:release_claim, :immediate}, [event("other", 1)]}
    end

    assert {:error, :event_run_mismatch} =
             MemoryBackend.transaction(b, fn tx ->
               with {:ok, {:committed, events}} <-
                      MemoryBackend.mutate_run(tx, :tenantless, "r1", mutation),
                    :ok <- MemoryBackend.append_events(tx, :tenantless, "r1", events) do
                 {:ok, :committed}
               end
             end)

    assert {:ok, ^stored} = MemoryBackend.fetch_run(b, :tenantless, "r1")
    assert MemoryBackend.claim(b, "r1") == lease.claim_token
    assert [] == MemoryBackend.events(b, :tenantless, "r1")
  end

  test "serialized no-change result leaves the aggregate byte-for-byte untouched", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"))
    _lease = claim_one(b, @now)
    before = MemoryBackend.record(b, "r1")

    assert {:ok, {:unchanged, :already_resolved}} =
             MemoryBackend.mutate_run(b, :tenantless, "r1", fn _current ->
               {:no_change, :already_resolved}
             end)

    assert MemoryBackend.record(b, "r1") == before
  end

  test "serialized mutation checks scope before invoking the callback", %{backend: b} do
    assert {:ok, _} = initialize(b, run("r1"), scope: {:tenant, "t1"})

    assert {:error, :not_found} =
             MemoryBackend.mutate_run(b, {:tenant, "t2"}, "r1", fn _ ->
               flunk("mutation must not run outside scope")
             end)

    assert {:error, :invalid_signal} =
             MemoryBackend.mutate_run(b, {:tenant, "t1"}, "r1", fn _ ->
               {:error, :invalid_signal}
             end)
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

    assert {:ok, {:committed, :done}} =
             MemoryBackend.mutate_run(b, :tenantless, "terminal", fn current ->
               terminal = %{current | status: :done, checkpoint_seq: current.checkpoint_seq + 1}
               {:commit, terminal, :run_completed, {:release_claim, :terminal}, :done}
             end)

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
