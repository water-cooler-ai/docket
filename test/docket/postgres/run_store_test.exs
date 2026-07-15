if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunStoreTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres

    alias Docket.DurableCodec
    alias Docket.Postgres.{RunCodec, RunStore}
    alias Docket.Postgres.RunStoreTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.GraphVersion
    alias Docket.Postgres.Schemas.Run
    alias Docket.Run.ChannelState

    @now ~U[2026-07-10 12:00:00.000000Z]
    @committed_at ~U[2026-07-10 11:59:58.123456Z]
    @migration_version 20_260_710_000_015
    @prefixed_migration_version 20_260_710_000_016

    defmodule InstallDocket do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule InstallDocketPrefixed do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(prefix: "docket_private")
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_private")
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      insert_graph!()
      :ok
    end

    test "inserts and reconstructs the exact committed run document" do
      run =
        initialized_run("inserted",
          updated_at: @committed_at,
          input: %{
            "exponent_float" => 1.0e20,
            "negative_zero" => -0.0,
            "escaped_nul" => "before\0after"
          },
          channels: %{
            "input:exponent_float" => %ChannelState{
              channel_id: "input:exponent_float",
              value: 1.0e20,
              version: 1
            }
          },
          changed_channels: MapSet.new(["input:exponent_float"])
        )

      assert {:ok, ^run} =
               RunStore.insert_run(
                 TestRepo,
                 :tenantless,
                 run,
                 :run_initialized,
                 @now
               )

      assert {:ok, fetched} = RunStore.fetch_run(TestRepo, :system, run.id)
      assert fetched === run
      assert {:ok, ^run} = RunStore.fetch_run(TestRepo, :tenantless, run.id)

      assert is_float(fetched.input["exponent_float"])
      assert fetched.input["negative_zero"] === -0.0
      assert fetched.input["escaped_nul"] == "before\0after"

      assert {:ok, %Docket.RunInfo{run: ^run} = info} =
               RunStore.inspect_run(TestRepo, :tenantless, run.id)

      assert info.wake_at == @now
      assert info.claimed_at == nil
      assert info.claim_attempts == 0
      refute Docket.RunInfo.poisoned?(info)

      row = row!(run.id)
      assert row.tenant_id == nil
      assert row.latest_checkpoint_type == :run_initialized
      assert row.updated_at == @committed_at

      assert {:error, :already_exists} =
               RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)
    end

    test "run listing applies graph and status filters in SQL without decoding state" do
      TestRepo.insert!(
        GraphVersion.changeset(%{
          tenant_id: "tenant",
          graph_id: "other-graph",
          graph_hash: "other-hash",
          graph: <<131, 106>>
        })
      )

      insert_run!("base-running", tenant_id: "tenant", started_at: @now)

      insert_run!("other-running",
        tenant_id: "tenant",
        graph_id: "other-graph",
        graph_hash: "other-hash",
        started_at: DateTime.add(@now, 2, :second)
      )

      insert_run!("other-waiting",
        tenant_id: "tenant",
        graph_id: "other-graph",
        graph_hash: "other-hash",
        status: :waiting,
        wake_at: nil,
        started_at: DateTime.add(@now, 1, :second)
      )

      # A collection read must never touch the full durable state column.
      TestRepo.update_all(
        from(run in Run, where: run.run_id == "other-running"),
        set: [state: <<0, 1, 2, 3>>]
      )

      assert {:ok, graph_page} =
               RunStore.list_runs(
                 TestRepo,
                 {:tenant, "tenant"},
                 list_query(limit: 10, graph_id: "other-graph")
               )

      assert Enum.map(graph_page.runs, & &1.id) == ["other-running", "other-waiting"]

      assert Enum.map(graph_page.runs, &Docket.RunSummary.graph_ref/1) == [
               %Docket.GraphRef{graph_id: "other-graph", graph_hash: "other-hash"},
               %Docket.GraphRef{graph_id: "other-graph", graph_hash: "other-hash"}
             ]

      assert {:ok, hash_page} =
               RunStore.list_runs(
                 TestRepo,
                 {:tenant, "tenant"},
                 list_query(limit: 10, graph_hash: "other-hash", statuses: [:waiting])
               )

      assert [%Docket.RunSummary{id: "other-waiting", status: :waiting}] = hash_page.runs

      assert {:ok, running_page} =
               RunStore.list_runs(
                 TestRepo,
                 {:tenant, "tenant"},
                 list_query(limit: 10, statuses: [:running])
               )

      assert Enum.map(running_page.runs, & &1.id) == ["other-running", "base-running"]
    end

    test "run listing rejects malformed trusted queries and invalid scopes" do
      invalid_queries = [
        list_query(limit: 0),
        list_query(before: {@now, ""}),
        list_query(graph_id: ""),
        list_query(graph_hash: 123),
        list_query(statuses: []),
        list_query(statuses: [:created]),
        Map.delete(list_query(), :before)
      ]

      for query <- invalid_queries do
        assert_raise ArgumentError, fn -> RunStore.list_runs(TestRepo, :system, query) end
      end

      assert_raise ArgumentError, ~r/scope must be/, fn ->
        RunStore.list_runs(TestRepo, {:tenant, nil}, list_query())
      end
    end

    test "durable insertion rejects every non-initialized shape and missing graph binding" do
      base = initialized_run("invalid")

      invalid = [
        {%{base | id: ""}, :run_initialized, @now},
        {%{base | graph_id: ""}, :run_initialized, @now},
        {%{base | graph_hash: ""}, :run_initialized, @now},
        {%{base | status: :created}, :run_initialized, @now},
        {%{base | status: :waiting}, :run_initialized, @now},
        {%{base | checkpoint_seq: 0}, :run_initialized, @now},
        {%{base | started_at: nil}, :run_initialized, @now},
        {%{base | updated_at: nil}, :run_initialized, @now},
        {%{base | started_at: ~U[2026-07-10 12:00:00Z]}, :run_initialized, @now},
        {%{base | updated_at: ~U[2026-07-10 12:00:00.123Z]}, :run_initialized, @now},
        {%{base | output: %{"premature" => true}}, :run_initialized, @now},
        {%{base | finished_at: @now}, :run_initialized, @now},
        {%{base | input: nil}, :run_initialized, @now},
        {base, :step_committed, @now},
        {base, :run_initialized, nil}
      ]

      for {run, checkpoint_type, wake_at} <- invalid do
        assert {:error, :invalid_run} =
                 RunStore.insert_run(
                   TestRepo,
                   :tenantless,
                   run,
                   checkpoint_type,
                   wake_at
                 )
      end

      unpublished = initialized_run("unpublished", graph_hash: "missing")

      assert_raise ArgumentError, ~r/run owner scope must be/, fn ->
        RunStore.insert_run(TestRepo, {:tenant, ""}, base, :run_initialized, @now)
      end

      assert {:error, :not_found} =
               RunStore.insert_run(
                 TestRepo,
                 :tenantless,
                 unpublished,
                 :run_initialized,
                 @now
               )

      assert TestRepo.aggregate(Run, :count) == 0

      assert_raise ArgumentError, ~r/run owner scope must be/, fn ->
        RunStore.insert_run(TestRepo, :system, base, :run_initialized, @now)
      end
    end

    test "fetch and inspect raise on corrupt persisted state rather than returning not_found" do
      run = initialized_run("corrupt")
      assert {:ok, ^run} = RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)
      canonical_state = row!(run.id).state

      corrupt_states = [
        canonical_state <> <<0>>,
        DurableCodec.encode!(:run, %{input: %{}})
      ]

      readers = [
        fn -> RunStore.fetch_run(TestRepo, :system, run.id) end,
        fn -> RunStore.inspect_run(TestRepo, :system, run.id) end
      ]

      for corrupt_state <- corrupt_states do
        TestRepo.update_all(
          from(row in Run, where: row.run_id == ^run.id),
          set: [state: corrupt_state]
        )

        for reader <- readers do
          error = assert_raise Docket.Error, reader
          assert error.type == :corrupt_run_row
        end
      end
    end

    test "operational transitions change RunInfo but never the committed run or expose its token" do
      run = initialized_run("stable", updated_at: @committed_at)
      assert {:ok, ^run} = RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)
      assert {:ok, ^run} = RunStore.fetch_run(TestRepo, :system, run.id)

      assert {:ok, initial_info} = RunStore.inspect_run(TestRepo, :system, run.id)
      assert initial_info.run == run
      assert initial_info.wake_at == @now
      assert initial_info.claim_attempts == 0

      lease = claim_one(@now)
      assert lease.run_id == run.id
      assert {:ok, ^run} = RunStore.fetch_run(TestRepo, :system, run.id)

      assert {:ok, claimed_info} = RunStore.inspect_run(TestRepo, :system, run.id)
      assert claimed_info.run == run
      assert claimed_info.wake_at == nil
      assert claimed_info.claimed_at == @now
      assert claimed_info.claim_attempts == 1
      refute Map.has_key?(claimed_info, :claim_token)

      claimed_row = row!(run.id)
      assert claimed_row.claim_token == lease.claim_token
      refute inspect(claimed_row) =~ lease.claim_token
      assert :claim_token in Run.__schema__(:redact_fields)

      replacement_token = Ecto.UUID.generate()
      redacted_changeset = Ecto.Changeset.change(claimed_row, claim_token: replacement_token)

      refute inspect(redacted_changeset) =~ claimed_row.claim_token
      refute inspect(redacted_changeset) =~ replacement_token
      assert inspect(redacted_changeset) =~ "**redacted**"

      refreshed_at = DateTime.add(@now, 1, :second)

      assert :ok =
               RunStore.refresh_claim(
                 TestRepo,
                 :system,
                 run.id,
                 lease.claim_token,
                 refreshed_at
               )

      assert {:ok, ^run} = RunStore.fetch_run(TestRepo, :system, run.id)
      assert {:ok, refreshed_info} = RunStore.inspect_run(TestRepo, :system, run.id)
      assert refreshed_info.run == run
      assert DateTime.compare(refreshed_info.claimed_at, @now) == :gt

      released_at = DateTime.add(@now, 2, :second)

      assert :ok =
               RunStore.release_claim(
                 TestRepo,
                 :system,
                 run.id,
                 lease.claim_token,
                 released_at
               )

      assert {:ok, ^run} = RunStore.fetch_run(TestRepo, :system, run.id)
      assert {:ok, released_info} = RunStore.inspect_run(TestRepo, :system, run.id)
      assert released_info.run == run
      assert released_info.wake_at == released_at
      assert released_info.claimed_at == nil
      assert released_info.claim_attempts == 1

      poisoned_at = DateTime.add(@now, 3, :second)

      assert {:ok, %{leases: [], poisoned: [%{run_id: "stable"}]}} =
               claim_due(poisoned_at, max_claim_attempts: 1)

      assert {:ok, ^run} = RunStore.fetch_run(TestRepo, :system, run.id)
      assert {:ok, poisoned_info} = RunStore.inspect_run(TestRepo, :system, run.id)
      assert poisoned_info.run == run
      assert poisoned_info.wake_at == nil
      assert poisoned_info.claimed_at == nil
      assert poisoned_info.claim_attempts == 1
      assert poisoned_info.poisoned_at == poisoned_at
      assert Docket.RunInfo.poisoned?(poisoned_info)
      assert row!(run.id).updated_at == @committed_at
    end

    test "operational timestamps normalize to UTC microsecond database precision" do
      run = initialized_run("time-normalized")
      wake_at = ~U[2026-07-10 12:00:00Z]

      assert {:ok, ^run} =
               RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, wake_at)

      assert {:ok, initial} = RunStore.inspect_run(TestRepo, :system, run.id)
      assert initial.wake_at.microsecond == {0, 6}

      claimed_at = ~U[2026-07-10 12:00:00Z]
      lease = claim_one(claimed_at)
      assert lease.claimed_at.microsecond == {0, 6}

      refreshed_at = ~U[2026-07-10 12:00:01.123Z]

      assert :ok =
               RunStore.refresh_claim(
                 TestRepo,
                 :system,
                 run.id,
                 lease.claim_token,
                 refreshed_at
               )

      # Refresh writes the database clock, so only monotone advance is
      # observable here; precision is the column's native microsecond.
      assert {:ok, refreshed} = RunStore.inspect_run(TestRepo, :system, run.id)
      assert DateTime.compare(refreshed.claimed_at, claimed_at) == :gt

      released_at = ~U[2026-07-10 12:00:02Z]

      assert :ok =
               RunStore.release_claim(
                 TestRepo,
                 :system,
                 run.id,
                 lease.claim_token,
                 released_at
               )

      assert {:ok, released} = RunStore.inspect_run(TestRepo, :system, run.id)
      assert released.wake_at.microsecond == {0, 6}
      assert {:ok, ^run} = RunStore.fetch_run(TestRepo, :system, run.id)
    end

    test "an expired-claim steal changes authority but not the committed run" do
      run = initialized_run("stolen", updated_at: @committed_at)
      assert {:ok, ^run} = RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)

      first = claim_one(@now)
      stolen_at = DateTime.add(@now, 10, :second)
      second = claim_one(stolen_at, orphan_ttl_ms: 1_000)

      assert second.run_id == run.id
      assert second.claim_token != first.claim_token
      assert second.claim_attempt == 2
      assert {:ok, ^run} = RunStore.fetch_run(TestRepo, :system, run.id)

      assert {:ok, %Docket.RunInfo{} = info} = RunStore.inspect_run(TestRepo, :system, run.id)
      assert info.run == run
      assert info.claimed_at == stolen_at
      assert info.claim_attempts == 2

      assert {:error, :claim_lost} =
               RunStore.refresh_claim(TestRepo, :system, run.id, first.claim_token, stolen_at)

      assert :ok =
               RunStore.release_claim(TestRepo, :system, run.id, second.claim_token, stolen_at)

      assert {:ok, ^run} = RunStore.fetch_run(TestRepo, :system, run.id)
      assert row!(run.id).updated_at == @committed_at
    end

    test "commit rejects malformed proposals before lookup and invalid bindings" do
      missing = initialized_run("missing")
      next = %{missing | checkpoint_seq: missing.checkpoint_seq + 1}

      assert {:error, :invalid_commit} =
               RunStore.commit(TestRepo, :system, proposal(next, nil, missing.checkpoint_seq))

      assert {:error, :invalid_commit} =
               RunStore.commit(
                 TestRepo,
                 :system,
                 proposal(
                   %{next | checkpoint_seq: 10},
                   Ecto.UUID.generate(),
                   missing.checkpoint_seq
                 )
               )

      run = initialized_run("binding")
      assert {:ok, ^run} = RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)
      lease = claim_one(@now)
      changed = %{run | graph_hash: "other", checkpoint_seq: run.checkpoint_seq + 1}

      assert {:error, :invalid_commit} =
               RunStore.commit(
                 TestRepo,
                 :system,
                 proposal(changed, lease.claim_token, run.checkpoint_seq)
               )

      changed_started_at = %{
        run
        | started_at: DateTime.add(run.started_at, 1, :second),
          checkpoint_seq: run.checkpoint_seq + 1
      }

      assert {:error, :invalid_commit} =
               RunStore.commit(
                 TestRepo,
                 :system,
                 proposal(changed_started_at, lease.claim_token, run.checkpoint_seq)
               )
    end

    test "commit validates schedule against proposed status" do
      run = initialized_run("schedule")
      assert {:ok, ^run} = RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)
      lease = claim_one(@now)
      next = %{run | checkpoint_seq: run.checkpoint_seq + 1}

      assert {:error, :invalid_commit} =
               RunStore.commit(
                 TestRepo,
                 :system,
                 proposal(
                   next,
                   lease.claim_token,
                   run.checkpoint_seq,
                   {:release_claim, :external}
                 )
               )
    end

    test "returns an empty typed batch when no work is eligible" do
      assert {:ok, %{leases: [], poisoned: []}} = claim_due(@now)

      assert_raise ArgumentError, fn ->
        RunStore.claim_due(TestRepo, :tenantless, policy(@now))
      end

      for invalid <- [
            %{policy(@now) | limit: 0},
            %{policy(@now) | orphan_ttl_ms: -1},
            %{policy(@now) | max_claim_attempts: 0}
          ] do
        assert_raise ArgumentError, fn -> RunStore.claim_due(TestRepo, :system, invalid) end
      end
    end

    test "claims ready and expired paths under one demand bound and returns lightweight leases" do
      old = DateTime.add(@now, -10, :second)
      ready = insert_run!("ready", wake_at: DateTime.add(old, -1, :second))

      expired =
        insert_run!("expired",
          wake_at: nil,
          claim_token: Ecto.UUID.generate(),
          claimed_at: old
        )

      fresh_token = Ecto.UUID.generate()

      insert_run!("fresh",
        wake_at: nil,
        claim_token: fresh_token,
        claimed_at: @now
      )

      insert_run!("future", wake_at: DateTime.add(@now, 1, :second))

      assert {:ok, %{leases: leases, poisoned: []}} = claim_due(@now, limit: 2)
      assert Enum.map(leases, & &1.run_id) |> Enum.sort() == ["expired", "ready"]

      for lease <- leases do
        assert Map.keys(lease) |> Enum.sort() ==
                 ~w(claim_attempt claim_token claimed_at checkpoint_seq graph_hash graph_id orphan_ttl_ms owner_scope run_id)a
                 |> Enum.sort()

        assert lease.owner_scope == :tenantless
        assert lease.graph_id == "graph"
        assert lease.graph_hash == "hash"
        assert lease.checkpoint_seq == 7
        assert lease.claim_attempt == 1
        assert lease.orphan_ttl_ms == 1_000
        assert {:ok, _} = Ecto.UUID.cast(lease.claim_token)

        row = row!(lease.run_id)
        assert row.claim_token == lease.claim_token
        assert row.claimed_at == @now
        assert row.wake_at == nil
      end

      assert row!(ready.run_id).claim_token != nil
      assert row!(expired.run_id).claim_token != expired.claim_token
      assert row!("fresh").claim_token == fresh_token
      assert row!("future").claim_token == nil
    end

    test "many fresh claims never hide a due ready row" do
      for index <- 1..20 do
        insert_run!("fresh-#{index}",
          wake_at: nil,
          claim_token: Ecto.UUID.generate(),
          claimed_at: @now
        )
      end

      insert_run!("due", wake_at: DateTime.add(@now, -1, :second))

      assert {:ok, %{leases: [%{run_id: "due"}], poisoned: []}} =
               claim_due(@now, limit: 1)
    end

    test "repeated limited polls make progress on both continuously eligible paths" do
      for index <- 1..3 do
        insert_run!("ready-#{index}", wake_at: DateTime.add(@now, -(index * 2), :second))

        insert_run!("expired-#{index}",
          wake_at: nil,
          claim_token: Ecto.UUID.generate(),
          claimed_at: DateTime.add(@now, -(index * 2 + 1), :second)
        )
      end

      claimed = for _ <- 1..6, do: claim_one(@now, limit: 1).run_id

      assert Enum.count(claimed, &String.starts_with?(&1, "ready-")) == 3
      assert Enum.count(claimed, &String.starts_with?(&1, "expired-")) == 3
    end

    test "demand of two or more reserves one outcome per non-empty class" do
      # All ready rows are older than the expired row, so oldest-first alone
      # would fill the whole batch from the ready class.
      for index <- 1..3 do
        insert_run!("ready-#{index}", wake_at: DateTime.add(@now, -(50 - index), :second))
      end

      insert_run!("expired-1",
        wake_at: nil,
        claim_token: Ecto.UUID.generate(),
        claimed_at: DateTime.add(@now, -10, :second)
      )

      assert {:ok, %{leases: leases, poisoned: []}} = claim_due(@now, limit: 3)
      claimed = leases |> Enum.map(& &1.run_id) |> Enum.sort()

      # One reserved slot per class, remainder by oldest eligibility.
      assert claimed == ["expired-1", "ready-1", "ready-2"]
    end

    test "the reservation also holds when the expired class dominates by age" do
      for index <- 1..3 do
        insert_run!("expired-#{index}",
          wake_at: nil,
          claim_token: Ecto.UUID.generate(),
          claimed_at: DateTime.add(@now, -(50 - index), :second)
        )
      end

      insert_run!("ready-1", wake_at: DateTime.add(@now, -10, :second))

      assert {:ok, %{leases: leases, poisoned: []}} = claim_due(@now, limit: 3)
      claimed = leases |> Enum.map(& &1.run_id) |> Enum.sort()

      assert claimed == ["expired-1", "expired-2", "ready-1"]
    end

    test "a poisoned head consumes its class's reserved slot as an outcome" do
      insert_run!("expired-exhausted",
        wake_at: nil,
        claim_token: Ecto.UUID.generate(),
        claimed_at: DateTime.add(@now, -30, :second),
        claim_attempts: 3
      )

      insert_run!("ready-1", wake_at: DateTime.add(@now, -20, :second))
      insert_run!("ready-2", wake_at: DateTime.add(@now, -10, :second))

      assert {:ok, %{leases: [lease], poisoned: [poison]}} = claim_due(@now, limit: 2)
      assert lease.run_id == "ready-1"
      assert poison.run_id == "expired-exhausted"
    end

    test "demand one serves the preferred class first and falls through when empty" do
      insert_run!("expired-old",
        wake_at: nil,
        claim_token: Ecto.UUID.generate(),
        claimed_at: DateTime.add(@now, -60, :second)
      )

      insert_run!("ready-new", wake_at: DateTime.add(@now, -5, :second))

      # The expired row is older, but the preference overrides age at demand 1.
      assert claim_one(@now, limit: 1, preference: :ready).run_id == "ready-new"
      assert claim_one(@now, limit: 1, preference: :expired).run_id == "expired-old"

      insert_run!("ready-only", wake_at: DateTime.add(@now, -1, :second))

      # Empty preferred class falls through without wasting the demand.
      assert claim_one(@now, limit: 1, preference: :expired).run_id == "ready-only"

      assert_raise ArgumentError, fn ->
        RunStore.claim_due(TestRepo, :system, policy(@now, preference: :sideways))
      end
    end

    test "alternating demand-1 preference makes progress on both classes despite age skew" do
      # Every expired row is older than every ready row, so neutral
      # oldest-first would drain the whole expired class before any ready row.
      for index <- 1..3 do
        insert_run!("ready-#{index}", wake_at: DateTime.add(@now, -index, :second))

        insert_run!("expired-#{index}",
          wake_at: nil,
          claim_token: Ecto.UUID.generate(),
          claimed_at: DateTime.add(@now, -(index + 100), :second)
        )
      end

      claimed =
        for preference <- [:ready, :expired, :ready, :expired],
            do: claim_one(@now, limit: 1, preference: preference).run_id

      assert Enum.count(claimed, &String.starts_with?(&1, "ready-")) == 2
      assert Enum.count(claimed, &String.starts_with?(&1, "expired-")) == 2
    end

    test "concurrent mixed-class polling drains both classes exactly once under contention" do
      for index <- 1..6 do
        insert_run!("ready-#{index}", wake_at: DateTime.add(@now, -index, :second))

        insert_run!("expired-#{index}",
          wake_at: nil,
          claim_token: Ecto.UUID.generate(),
          claimed_at: DateTime.add(@now, -(index + 100), :second)
        )
      end

      # Four claimants poll concurrently until the queue is dry; SKIP LOCKED
      # contention may skip rows within a poll but never duplicates or
      # strands one class while the other drains.
      drain_all = fn drain_all ->
        case claim_due(@now, limit: 2) do
          {:ok, %{leases: [], poisoned: []}} -> []
          {:ok, %{leases: leases, poisoned: []}} -> leases ++ drain_all.(drain_all)
        end
      end

      claimed =
        1..4
        |> Task.async_stream(fn _worker -> drain_all.(drain_all) end,
          max_concurrency: 4,
          timeout: 30_000
        )
        |> Enum.flat_map(fn {:ok, leases} -> leases end)

      run_ids = Enum.map(claimed, & &1.run_id)
      assert length(run_ids) == 12
      assert Enum.uniq(run_ids) == run_ids
      assert Enum.count(run_ids, &String.starts_with?(&1, "ready-")) == 6
      assert Enum.count(run_ids, &String.starts_with?(&1, "expired-")) == 6

      tokens = Enum.map(claimed, & &1.claim_token)
      assert Enum.uniq(tokens) == tokens
    end

    test "claim scans emit identity-free telemetry with class counts and fallback" do
      handler_id = "claim-telemetry-#{System.unique_integer([:positive])}"
      parent = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:docket, :postgres, :run_store, :claim],
          &Docket.Test.TelemetryRelay.tagged/4,
          {parent, :claim_telemetry}
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      insert_run!("ready-1", wake_at: DateTime.add(@now, -20, :second))

      insert_run!("expired-1",
        wake_at: nil,
        claim_token: Ecto.UUID.generate(),
        claimed_at: DateTime.add(@now, -30, :second)
      )

      insert_run!("expired-poison",
        wake_at: nil,
        claim_token: Ecto.UUID.generate(),
        claimed_at: DateTime.add(@now, -40, :second),
        claim_attempts: 3
      )

      assert {:ok, %{leases: [_, _], poisoned: [_]}} = claim_due(@now, limit: 3)

      assert_receive {:claim_telemetry, measurements, metadata}
      assert measurements.demand == 3
      assert measurements.leases == 2
      assert measurements.poisoned == 1
      assert measurements.ready_candidates == 1
      assert measurements.expired_candidates == 2
      assert measurements.ready_selected == 1
      assert measurements.expired_selected == 2
      assert measurements.steals == 1
      assert measurements.ready_oldest_age_ms == 20_000
      assert measurements.expired_oldest_age_ms == 40_000
      assert metadata == %{preference: nil, fallback: false, result: :ok}

      # Preferred-but-empty class reports the fallthrough.
      insert_run!("expired-2",
        wake_at: nil,
        claim_token: Ecto.UUID.generate(),
        claimed_at: DateTime.add(@now, -30, :second)
      )

      assert {:ok, %{leases: [%{run_id: "expired-2"}]}} =
               claim_due(@now, limit: 1, preference: :ready)

      assert_receive {:claim_telemetry, %{demand: 1, expired_selected: 1},
                      %{preference: :ready, fallback: true}}
    end

    test "leases and poison outcomes across both paths share one limit" do
      insert_run!("poison-ready",
        wake_at: DateTime.add(@now, -30, :second),
        claim_attempts: 3
      )

      insert_run!("lease-expired",
        wake_at: nil,
        claim_token: Ecto.UUID.generate(),
        claimed_at: DateTime.add(@now, -20, :second)
      )

      untouched = insert_run!("untouched-ready", wake_at: DateTime.add(@now, -10, :second))

      assert {:ok, %{leases: [lease], poisoned: [poison]}} = claim_due(@now, limit: 2)
      assert lease.run_id == "lease-expired"
      assert poison.run_id == "poison-ready"
      assert row!(untouched.run_id).claim_token == nil
      assert row!(untouched.run_id).poisoned_at == nil
    end

    test "configured maximum launches exactly that many vehicles before poison" do
      for max_attempts <- 1..3 do
        run_id = "run-#{max_attempts}"
        insert_run!(run_id)

        for attempt <- 1..max_attempts do
          now = DateTime.add(@now, attempt, :millisecond)

          assert {:ok, %{leases: [%{run_id: ^run_id, claim_attempt: ^attempt}], poisoned: []}} =
                   claim_due(now,
                     orphan_ttl_ms: 0,
                     max_claim_attempts: max_attempts
                   )
        end

        poison_time = DateTime.add(@now, max_attempts + 1, :millisecond)

        assert {:ok, %{leases: [], poisoned: [poison]}} =
                 claim_due(poison_time,
                   orphan_ttl_ms: 0,
                   max_claim_attempts: max_attempts
                 )

        assert poison.run_id == run_id
        assert poison.poisoned_at == poison_time
        assert poison.poison_reason == "max_claim_attempts_exceeded"

        row = row!(run_id)
        assert row.claim_attempts == max_attempts
        assert row.claim_token == nil
        assert row.claimed_at == nil
        assert row.wake_at == nil
        assert row.poisoned_at == poison_time
        assert row.checkpoint_seq == 7

        assert {:ok, %{leases: [], poisoned: []}} =
                 claim_due(DateTime.add(poison_time, 1, :second),
                   orphan_ttl_ms: 0,
                   max_claim_attempts: max_attempts
                 )
      end
    end

    test "expiry alone preserves authority; steal invalidates stale refresh, release, and commit fence" do
      insert_run!("run")
      first = claim_one(@now)
      expired_at = DateTime.add(@now, 2, :second)

      parent = self()
      handler = "claim-operation-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :claim, :operation],
        &Docket.Test.TelemetryRelay.raw/4,
        parent
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      original = row!("run")

      assert {:error, :claim_lost} =
               RunStore.refresh_claim(TestRepo, :system, "run", "wrong", @now)

      assert_receive {[:docket, :postgres, :claim, :operation], %{duration: refresh_duration},
                      %{operation: :refresh, result: :claim_lost}}

      assert is_integer(refresh_duration) and refresh_duration >= 0

      assert :ok = RunStore.release_claim(TestRepo, :system, "run", "wrong", @now)

      assert_receive {[:docket, :postgres, :claim, :operation],
                      %{duration: release_duration, matched: 0},
                      %{operation: :release, result: :claim_lost}}

      assert is_integer(release_duration) and release_duration >= 0
      assert row!("run") == original

      assert :ok =
               RunStore.refresh_claim(TestRepo, :system, "run", first.claim_token, expired_at)

      refreshed = row!("run")
      assert DateTime.compare(refreshed.claimed_at, original.claimed_at) == :gt
      assert {:ok, %{leases: [], poisoned: []}} = claim_due(expired_at)

      stolen_at = DateTime.add(DateTime.utc_now(), 10, :second)
      stolen = claim_one(stolen_at)
      assert stolen.claim_token != first.claim_token

      refute current_claim?("run", first.claim_token)
      assert current_claim?("run", stolen.claim_token)

      winner = row!("run")
      stale_time = DateTime.add(stolen_at, 1, :second)

      stale_fence = RunStore.current_claim("run", first.claim_token)

      assert {0, nil} =
               Run
               |> where(^stale_fence)
               |> where([run], run.checkpoint_seq == ^winner.checkpoint_seq)
               |> TestRepo.update_all(inc: [checkpoint_seq: 1])

      assert {:error, :claim_lost} =
               RunStore.refresh_claim(TestRepo, :system, "run", first.claim_token, stale_time)

      assert row!("run") == winner
      assert :ok = RunStore.release_claim(TestRepo, :system, "run", first.claim_token, stale_time)
      assert row!("run") == winner

      {:ok, committed} = RunStore.fetch_run(TestRepo, :system, "run")
      loser_next = %{committed | checkpoint_seq: committed.checkpoint_seq + 1}

      assert {:error, :stale_fence} =
               RunStore.commit(
                 TestRepo,
                 :system,
                 proposal(loser_next, first.claim_token, committed.checkpoint_seq)
               )

      assert row!("run") == winner

      released_at = DateTime.add(stale_time, 1, :second)

      assert :ok =
               RunStore.release_claim(TestRepo, :system, "run", stolen.claim_token, released_at)

      released = row!("run")
      assert released.claim_token == nil
      assert released.claimed_at == nil
      assert released.wake_at == released_at
      assert released.checkpoint_seq == winner.checkpoint_seq
      assert released.claim_attempts == winner.claim_attempts

      snapshot = released

      assert :ok =
               RunStore.release_claim(TestRepo, :system, "run", stolen.claim_token, released_at)

      assert row!("run") == snapshot
    end

    test "pre-execution abandon hands the attempt back and schedules the retry wake" do
      insert_run!("abandon")
      lease = claim_one(@now)
      assert lease.claim_attempt == 1

      before_abandon = row!("abandon")
      abandoned_at = DateTime.add(@now, 1, :second)
      retry_at = DateTime.add(abandoned_at, 30, :second)

      assert {:ok, :rescheduled} =
               RunStore.abandon_claim(
                 TestRepo,
                 :system,
                 "abandon",
                 lease.claim_token,
                 abandon_policy(abandoned_at, retry_at)
               )

      abandoned = row!("abandon")
      assert abandoned.claim_token == nil
      assert abandoned.claimed_at == nil
      assert abandoned.wake_at == retry_at
      assert abandoned.claim_attempts == 0
      assert abandoned.claim_abandons == 1
      assert abandoned.poisoned_at == nil
      assert abandoned.poison_reason == nil
      assert abandoned.checkpoint_seq == before_abandon.checkpoint_seq
      assert abandoned.updated_at == before_abandon.updated_at
      assert abandoned.state == before_abandon.state

      assert {:ok, %{run: run} = info} = RunStore.inspect_run(TestRepo, :system, "abandon")
      assert info.claim_abandons == 1
      assert info.claim_attempts == 0
      assert info.wake_at == retry_at
      refute Docket.RunInfo.poisoned?(info)
      assert run.updated_at == before_abandon.updated_at

      assert {:ok, %{leases: [], poisoned: []}} = claim_due(abandoned_at)
      relaunched = claim_one(retry_at)
      assert relaunched.claim_attempt == 1
      assert relaunched.claim_token != lease.claim_token
    end

    test "abandon requires :system scope and a coherent future-wake policy" do
      insert_run!("abandon-policy")
      lease = claim_one(@now)
      policy = abandon_policy(@now, DateTime.add(@now, 1, :second))

      assert_raise ArgumentError, fn ->
        RunStore.abandon_claim(TestRepo, :tenantless, "abandon-policy", lease.claim_token, policy)
      end

      for invalid <- [
            Map.delete(policy, :retry_at),
            %{policy | retry_at: DateTime.add(policy.now, -1, :second)},
            %{policy | max_claim_abandons: 0},
            %{policy | expected_checkpoint_seq: -1},
            %{policy | now: nil}
          ] do
        assert_raise ArgumentError, fn ->
          RunStore.abandon_claim(TestRepo, :system, "abandon-policy", lease.claim_token, invalid)
        end
      end

      assert row!("abandon-policy").claim_token == lease.claim_token
    end

    test "stale abandon after steal, commit, or signal mutation cannot disturb the winner" do
      insert_run!("stale-abandon")
      first = claim_one(@now)
      stolen_at = DateTime.add(@now, 4, :second)
      stolen = claim_one(stolen_at, orphan_ttl_ms: 0)
      assert stolen.claim_token != first.claim_token

      winner = row!("stale-abandon")
      policy = abandon_policy(stolen_at, DateTime.add(stolen_at, 30, :second))

      assert {:ok, :stale} =
               RunStore.abandon_claim(
                 TestRepo,
                 :system,
                 "stale-abandon",
                 first.claim_token,
                 policy
               )

      assert {:ok, :stale} =
               RunStore.abandon_claim(TestRepo, :system, "stale-abandon", "not-a-uuid", policy)

      assert {:ok, :stale} =
               RunStore.abandon_claim(TestRepo, :system, "missing-run", first.claim_token, policy)

      assert row!("stale-abandon") == winner

      # A retain_claim commit keeps the token but advances the fence; a later
      # abandon carrying the lease's original sequence must not regress the
      # freshly reset attempt counter.
      run =
        initialized_run("stale-abandon", checkpoint_seq: 8, started_at: @now, updated_at: @now)

      proposal = proposal(run, stolen.claim_token, 7)
      assert {:ok, ^run} = RunStore.commit(TestRepo, :system, proposal)

      committed = row!("stale-abandon")
      assert committed.claim_attempts == 0
      assert committed.claim_token == stolen.claim_token

      assert {:ok, :stale} =
               RunStore.abandon_claim(
                 TestRepo,
                 :system,
                 "stale-abandon",
                 stolen.claim_token,
                 policy
               )

      assert row!("stale-abandon") == committed
    end

    test "abandon races a concurrent steal to exactly one effect" do
      insert_run!("abandon-race")
      first = claim_one(@now)
      race_at = DateTime.add(@now, 4, :second)
      policy = abandon_policy(race_at, DateTime.add(race_at, 30, :second))

      [abandon_result, steal_result] =
        [
          fn ->
            RunStore.abandon_claim(TestRepo, :system, "abandon-race", first.claim_token, policy)
          end,
          fn -> claim_due(race_at, orphan_ttl_ms: 0) end
        ]
        |> Task.async_stream(& &1.(), ordered: true, timeout: 15_000)
        |> Enum.map(fn {:ok, result} -> result end)

      row = row!("abandon-race")

      case abandon_result do
        {:ok, :rescheduled} ->
          case steal_result do
            {:ok, %{leases: [lease], poisoned: []}} ->
              # The steal re-claimed the already-abandoned row.
              assert lease.claim_token != first.claim_token
              assert row.claim_token == lease.claim_token
              assert row.claim_attempts == 1
              assert row.claim_abandons == 1

            {:ok, %{leases: [], poisoned: []}} ->
              assert row.claim_token == nil
              assert row.wake_at == policy.retry_at
              assert row.claim_attempts == 0
              assert row.claim_abandons == 1
          end

        {:ok, :stale} ->
          # The steal won first; the stale abandon left its claim intact.
          assert {:ok, %{leases: [lease], poisoned: []}} = steal_result
          assert row.claim_token == lease.claim_token
          assert row.claim_attempts == 2
          assert row.claim_abandons == 0
      end
    end

    test "abandon at the configured maximum poisons with its own reason" do
      insert_run!("abandon-poison")
      max = 2

      final_now =
        Enum.reduce(1..max, @now, fn cycle, now ->
          lease = claim_one(now)
          abandoned_at = DateTime.add(now, 1, :second)
          retry_at = DateTime.add(abandoned_at, 1, :second)
          policy = abandon_policy(abandoned_at, retry_at, max_claim_abandons: max)

          assert {:ok, :rescheduled} =
                   RunStore.abandon_claim(
                     TestRepo,
                     :system,
                     "abandon-poison",
                     lease.claim_token,
                     policy
                   )

          assert row!("abandon-poison").claim_abandons == cycle
          retry_at
        end)

      lease = claim_one(final_now)
      poisoned_at = DateTime.add(final_now, 1, :second)

      policy =
        abandon_policy(poisoned_at, DateTime.add(poisoned_at, 1, :second),
          max_claim_abandons: max
        )

      assert {:ok, :poisoned} =
               RunStore.abandon_claim(
                 TestRepo,
                 :system,
                 "abandon-poison",
                 lease.claim_token,
                 policy
               )

      poisoned = row!("abandon-poison")
      assert poisoned.status == :running
      assert poisoned.claim_token == nil
      assert poisoned.claimed_at == nil
      assert poisoned.wake_at == nil
      assert poisoned.claim_abandons == max
      assert poisoned.poisoned_at == poisoned_at
      assert poisoned.poison_reason == "max_claim_abandons_exceeded"

      assert {:ok, %{leases: [], poisoned: []}} =
               claim_due(DateTime.add(poisoned_at, 1, :hour), orphan_ttl_ms: 0)

      recovered_at = DateTime.add(poisoned_at, 2, :hour)

      assert {:ok, _run} =
               RunStore.retry_poisoned_run(TestRepo, :system, "abandon-poison", recovered_at)

      recovered = row!("abandon-poison")
      assert recovered.claim_abandons == 0
      assert recovered.claim_attempts == 0
      assert recovered.poisoned_at == nil
      assert recovered.wake_at == recovered_at
    end

    test "abandons never advance attempt poison and committed progress resets the abandon count" do
      insert_run!("abandon-progress")

      final_now =
        Enum.reduce(1..5, @now, fn _cycle, now ->
          lease = claim_one(now, max_claim_attempts: 3)
          assert lease.claim_attempt == 1

          abandoned_at = DateTime.add(now, 1, :second)
          retry_at = DateTime.add(abandoned_at, 1, :second)

          assert {:ok, :rescheduled} =
                   RunStore.abandon_claim(
                     TestRepo,
                     :system,
                     "abandon-progress",
                     lease.claim_token,
                     abandon_policy(abandoned_at, retry_at, max_claim_abandons: 10)
                   )

          retry_at
        end)

      assert row!("abandon-progress").claim_abandons == 5
      assert row!("abandon-progress").claim_attempts == 0

      lease = claim_one(final_now, max_claim_attempts: 3)

      run =
        initialized_run("abandon-progress", checkpoint_seq: 8, started_at: @now, updated_at: @now)

      proposal = proposal(run, lease.claim_token, 7, {:release_claim, :immediate})
      assert {:ok, ^run} = RunStore.commit(TestRepo, :system, proposal)

      committed = row!("abandon-progress")
      assert committed.claim_abandons == 0
      assert committed.claim_attempts == 0
    end

    test "a claim is stealable only after the exact TTL boundary" do
      insert_run!("run")
      first = claim_one(@now)
      boundary = DateTime.add(@now, 1, :second)

      assert {:ok, %{leases: [], poisoned: []}} =
               claim_due(boundary, orphan_ttl_ms: 1_000)

      assert current_claim?("run", first.claim_token)

      assert {:ok, %{leases: [stolen], poisoned: []}} =
               claim_due(DateTime.add(boundary, 1, :millisecond), orphan_ttl_ms: 1_000)

      assert stolen.claim_token != first.claim_token
    end

    test "concurrent dispatchers claim disjoint demand-bounded batches" do
      for index <- 1..8, do: insert_run!("run-#{index}")

      parent = self()

      tasks =
        for _ <- 1..4 do
          Task.async(fn ->
            TestRepo.transaction(fn ->
              %{rows: [[backend_pid]]} = TestRepo.query!("SELECT pg_backend_pid()")
              send(parent, {:ready, self()})

              receive do
                :go ->
                  assert {:ok, batch} = claim_due(@now, limit: 2)
                  {backend_pid, batch}
              end
            end)
          end)
        end

      pids = for _ <- tasks, do: receive(do: ({:ready, pid} -> pid))
      Enum.each(pids, &send(&1, :go))
      results = Enum.map(tasks, &Task.await(&1, 15_000))

      assert Enum.map(results, fn {:ok, {backend_pid, _batch}} -> backend_pid end)
             |> Enum.uniq()
             |> length() == 4

      for {:ok, {_backend_pid, batch}} <- results do
        assert length(batch.leases) + length(batch.poisoned) <= 2
      end

      leases =
        for {:ok, {_backend_pid, %{leases: leases}}} <- results, lease <- leases, do: lease

      assert length(leases) == 8
      assert Enum.uniq_by(leases, & &1.run_id) == leases
      assert Enum.uniq_by(leases, & &1.claim_token) == leases

      for lease <- leases do
        assert row!(lease.run_id).claim_token == lease.claim_token
      end
    end

    test "the plan fences both limited index scans before the bounded update" do
      for index <- 1..12 do
        insert_run!("ready-#{index}", wake_at: DateTime.add(@now, -index, :second))

        insert_run!("expired-#{index}",
          wake_at: nil,
          claim_token: Ecto.UUID.generate(),
          claimed_at: DateTime.add(@now, -(index + 10), :second)
        )
      end

      cutoff = DateTime.add(@now, -1, :second)

      plan =
        TestRepo.transaction(fn ->
          TestRepo.query!("SET LOCAL enable_seqscan = off")

          TestRepo.query!(
            "EXPLAIN (COSTS OFF) " <> RunStore.claim_statement(),
            [@now, cutoff, 3, 3, nil]
          ).rows
        end)
        |> elem(1)
        |> List.flatten()
        |> Enum.join("\n")

      assert plan =~ "CTE ready_candidates"
      assert plan =~ "CTE expired_candidates"
      assert plan =~ "CTE candidates"
      assert plan =~ "docket_runs_wake_at_id_index"
      assert plan =~ "docket_runs_claimed_at_id_index"
      assert length(Regex.scan(~r/\bLockRows\b/, plan)) >= 2
      assert length(Regex.scan(~r/\bLimit\b/, plan)) >= 3
      assert plan =~ "CTE Scan on candidates"
      assert plan =~ "Update on docket_runs"

      assert {:ok, batch} = claim_due(@now, limit: 3)
      assert length(batch.leases) + length(batch.poisoned) == 3

      returned_ids =
        Enum.map(batch.leases, & &1.run_id) ++ Enum.map(batch.poisoned, & &1.run_id)

      persisted_ids =
        TestRepo.all(
          from(run in Run,
            where: run.claimed_at == ^@now or run.poisoned_at == ^@now,
            select: run.run_id
          )
        )

      assert Enum.sort(persisted_ids) == Enum.sort(returned_ids)
    end

    test "repo/prefix context claims inside a dedicated schema" do
      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @prefixed_migration_version,
                 InstallDocketPrefixed,
                 log: false
               )

      insert_graph!("docket_private")
      ctx = %{repo: TestRepo, prefix: "docket_private"}
      run = initialized_run("prefixed")

      assert {:ok, ^run} =
               RunStore.insert_run(
                 ctx,
                 {:tenant, "prefix-tenant"},
                 run,
                 :run_initialized,
                 @now
               )

      assert {:ok, ^run} = RunStore.fetch_run(ctx, {:tenant, "prefix-tenant"}, run.id)
      assert {:error, :not_found} = RunStore.fetch_run(ctx, :tenantless, run.id)

      assert {:ok, %Docket.RunInfo{run: ^run, wake_at: @now}} =
               RunStore.inspect_run(ctx, {:tenant, "prefix-tenant"}, run.id)

      assert {:ok, %{leases: [lease], poisoned: []}} =
               RunStore.claim_due(ctx, :system, policy(@now))

      assert lease.owner_scope == {:tenant, "prefix-tenant"}
      assert row!("prefixed", "docket_private").wake_at == nil

      refreshed_at = DateTime.add(@now, 1, :second)

      assert :ok =
               RunStore.refresh_claim(ctx, :system, "prefixed", lease.claim_token, refreshed_at)

      assert DateTime.compare(row!("prefixed", "docket_private").claimed_at, @now) == :gt

      released_at = DateTime.add(@now, 2, :second)

      assert :ok =
               RunStore.release_claim(ctx, :system, "prefixed", lease.claim_token, released_at)

      assert row!("prefixed", "docket_private").wake_at == released_at
      assert {:ok, ^run} = RunStore.fetch_run(ctx, :system, run.id)
    end

    defp policy(now, overrides \\ []) do
      Map.merge(
        %{now: now, limit: 1, orphan_ttl_ms: 1_000, max_claim_attempts: 3},
        Map.new(overrides)
      )
    end

    defp list_query(overrides \\ []) do
      Map.merge(
        %{limit: 100, before: nil, graph_id: nil, graph_hash: nil, statuses: nil},
        Map.new(overrides)
      )
    end

    defp abandon_policy(now, retry_at, overrides \\ []) do
      Map.merge(
        %{expected_checkpoint_seq: 7, now: now, retry_at: retry_at, max_claim_abandons: 3},
        Map.new(overrides)
      )
    end

    test "serialized mutation validates scope before SQL and preserves no-change rows" do
      run = initialized_run("mutation", checkpoint_seq: 7)

      assert {:ok, ^run} =
               RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)

      before = row!(run.id)

      assert {:ok, {:unchanged, :same}} =
               RunStore.mutate_run(TestRepo, :tenantless, run.id, fn current ->
                 assert current == run
                 {:no_change, :same}
               end)

      assert row!(run.id) == before

      assert_raise ArgumentError, ~r/scope must be/, fn ->
        RunStore.mutate_run(TestRepo, {:tenant, nil}, run.id, fn _ ->
          flunk("an invalid scope must not invoke the mutation")
        end)
      end

      assert row!(run.id) == before
    end

    test "serialized mutation rejects malformed proposals without changing the row" do
      run = initialized_run("invalid-mutation", checkpoint_seq: 7)
      assert {:ok, ^run} = RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)
      before = row!(run.id)

      assert {:error, :invalid_mutation} =
               RunStore.mutate_run(TestRepo, :tenantless, run.id, fn current ->
                 {:commit, current, :step_committed, :retain_claim, :bad}
               end)

      assert row!(run.id) == before
    end

    test "serialized mutation holds the row lock while the pure decision runs" do
      run = initialized_run("locked-mutation", checkpoint_seq: 7)
      assert {:ok, ^run} = RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)
      parent = self()

      mutation =
        Task.async(fn ->
          RunStore.mutate_run(TestRepo, :tenantless, run.id, fn current ->
            send(parent, :mutation_locked)

            receive do
              :continue -> {:no_change, current.checkpoint_seq}
            end
          end)
        end)

      assert_receive :mutation_locked

      # claim_due uses SKIP LOCKED, so it must not claim the row whose signal
      # decision is currently serialized.
      assert {:ok, %{leases: [], poisoned: []}} = claim_due(@now)

      send(mutation.pid, :continue)
      assert {:ok, {:unchanged, 7}} = Task.await(mutation, 1_000)

      assert {:ok, %{leases: [%{run_id: "locked-mutation"}], poisoned: []}} =
               claim_due(@now)
    end

    test "poison recovery is exact, idempotent, scoped, and terminal-first" do
      run = initialized_run("recover", checkpoint_seq: 7)
      assert {:ok, ^run} = RunStore.insert_run(TestRepo, :tenantless, run, :run_initialized, @now)

      poisoned_at = DateTime.add(@now, 1, :second)

      TestRepo.update_all(
        from(stored in Run, where: stored.run_id == ^run.id),
        set: [
          wake_at: nil,
          claim_attempts: 3,
          poisoned_at: poisoned_at,
          poison_reason: "max_claim_attempts_exceeded"
        ]
      )

      recovered_at = DateTime.add(@now, 2, :second)

      assert {:ok, ^run} =
               RunStore.retry_poisoned_run(TestRepo, :tenantless, run.id, recovered_at)

      recovered = row!(run.id)
      assert recovered.wake_at == recovered_at
      assert recovered.claim_attempts == 0
      assert recovered.poisoned_at == nil
      assert recovered.poison_reason == nil
      assert recovered.checkpoint_seq == run.checkpoint_seq
      assert recovered.latest_checkpoint_type == :run_initialized

      assert {:ok, ^run} =
               RunStore.retry_poisoned_run(
                 TestRepo,
                 :tenantless,
                 run.id,
                 DateTime.add(recovered_at, 1, :hour)
               )

      assert row!(run.id) == recovered

      assert {:error, :not_found} =
               RunStore.retry_poisoned_run(TestRepo, {:tenant, "x"}, run.id, @now)

      terminal_source = initialized_run("terminal-recovery", checkpoint_seq: 7)

      {:ok, terminal_moment} =
        Docket.Runtime.RunMutation.cancel_run(terminal_source, recovered_at)

      terminal = terminal_moment.run
      {:ok, attrs} = RunCodec.dump(terminal)

      attrs
      |> Map.merge(%{
        tenant_id: nil,
        latest_checkpoint_type: :run_cancelled,
        wake_at: nil,
        claim_attempts: 0
      })
      |> Run.changeset()
      |> TestRepo.insert!()

      terminal_before = row!(terminal.id)

      assert {:error, :inactive_run} =
               RunStore.retry_poisoned_run(TestRepo, :tenantless, terminal.id, @now)

      assert row!(terminal.id) == terminal_before
    end

    defp claim_due(now, overrides \\ []) do
      RunStore.claim_due(TestRepo, :system, policy(now, overrides))
    end

    defp claim_one(now, overrides \\ []) do
      assert {:ok, %{leases: [lease], poisoned: []}} = claim_due(now, overrides)
      lease
    end

    defp proposal(run, token, expected, schedule \\ :retain_claim) do
      %{
        run: run,
        expected_checkpoint_seq: expected,
        claim_token: token,
        checkpoint_type: :step_committed,
        schedule: schedule
      }
    end

    defp current_claim?(run_id, token) do
      predicate = RunStore.current_claim(run_id, token)
      TestRepo.exists?(from(run in Run, where: ^predicate))
    end

    defp insert_graph!(prefix \\ nil) do
      for tenant_id <- [nil, "t1", "t2", "tenant-1", "tenant-2", "tenant", "prefix-tenant", "x"] do
        changeset =
          GraphVersion.changeset(%{
            tenant_id: tenant_id,
            graph_id: "graph",
            graph_hash: "hash",
            graph: <<131, 106>>
          })

        TestRepo.insert!(changeset, prefix: prefix)
      end
    end

    defp initialized_run(run_id, overrides \\ []) do
      %Docket.Run{
        id: run_id,
        graph_id: "graph",
        graph_hash: "hash",
        status: :running,
        input: %{"prompt" => "hello"},
        metadata: %{"source" => "run-store-test"},
        started_at: @committed_at,
        updated_at: @committed_at,
        checkpoint_seq: 1
      }
      |> struct(Map.new(overrides))
    end

    defp insert_run!(run_id, overrides \\ [], prefix \\ nil) do
      {:ok, durable_attrs} =
        run_id
        |> initialized_run(checkpoint_seq: 7, started_at: @now, updated_at: @now)
        |> RunCodec.dump()

      attrs =
        durable_attrs
        |> Map.put(:wake_at, @now)
        |> Map.merge(Map.new(overrides))

      attrs
      |> Run.changeset()
      |> TestRepo.insert!(prefix: prefix)
    end

    defp row!(run_id, prefix \\ nil) do
      query =
        from(run in Run, where: run.run_id == ^run_id)
        |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)

      TestRepo.one!(query)
    end
  end
end
