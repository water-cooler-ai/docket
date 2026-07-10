if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunStoreTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres

    alias Docket.Postgres.RunStore
    alias Docket.Postgres.RunStoreTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.GraphArtifact
    alias Docket.Postgres.Schemas.GraphVersion
    alias Docket.Postgres.Schemas.Run

    @now ~U[2026-07-10 12:00:00.000000Z]
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
                 ~w(claim_attempt claim_token claimed_at checkpoint_seq graph_compiler_abi graph_hash graph_id run_id)a
                 |> Enum.sort()

        assert lease.graph_id == "graph"
        assert lease.graph_hash == "hash"
        assert lease.graph_compiler_abi == "docket-runtime-graph/v1"
        assert lease.checkpoint_seq == 7
        assert lease.claim_attempt == 1
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

    test "maximum three launches exactly three attempts before poison" do
      insert_run!("run")

      for attempt <- 1..3 do
        now = DateTime.add(@now, attempt, :millisecond)

        assert {:ok, %{leases: [%{claim_attempt: ^attempt}], poisoned: []}} =
                 claim_due(now, orphan_ttl_ms: 0, max_claim_attempts: 3)
      end

      poison_time = DateTime.add(@now, 4, :millisecond)

      assert {:ok, %{leases: [], poisoned: [poison]}} =
               claim_due(poison_time, orphan_ttl_ms: 0, max_claim_attempts: 3)

      assert poison.run_id == "run"
      assert poison.poisoned_at == poison_time

      assert poison.poison_reason == %{
               "type" => "max_claim_attempts_exceeded",
               "max_claim_attempts" => 3,
               "claim_attempts" => 3
             }

      row = row!("run")
      assert row.claim_attempts == 3
      assert row.claim_token == nil
      assert row.claimed_at == nil
      assert row.wake_at == nil
      assert row.poisoned_at == poison_time
      assert row.checkpoint_seq == 7

      assert {:ok, %{leases: [], poisoned: []}} =
               claim_due(DateTime.add(poison_time, 1, :second), orphan_ttl_ms: 0)
    end

    test "expiry alone preserves authority; steal invalidates stale refresh, release, and commit fence" do
      insert_run!("run")
      first = claim_one(@now)
      expired_at = DateTime.add(@now, 2, :second)

      original = row!("run")

      assert {:error, :claim_lost} =
               RunStore.refresh_claim(TestRepo, :system, "run", "wrong", @now)

      assert :ok = RunStore.release_claim(TestRepo, :system, "run", "wrong", @now)
      assert row!("run") == original

      assert :ok =
               RunStore.refresh_claim(TestRepo, :system, "run", first.claim_token, expired_at)

      assert row!("run").claimed_at == expired_at
      assert {:ok, %{leases: [], poisoned: []}} = claim_due(expired_at)

      stolen_at = DateTime.add(@now, 4, :second)
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
            [@now, cutoff, 3, 3, Docket.Runtime.Graph.Artifact.compiler_abi()]
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

      assert TestRepo.aggregate(
               from(run in Run, where: run.updated_at == ^@now),
               :count
             ) == 3
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
      insert_run!("prefixed", [], "docket_private")
      ctx = %{repo: TestRepo, prefix: "docket_private"}

      assert {:ok, %{leases: [lease], poisoned: []}} =
               RunStore.claim_due(ctx, :system, policy(@now))

      assert row!("prefixed", "docket_private").wake_at == nil

      refreshed_at = DateTime.add(@now, 1, :second)

      assert :ok =
               RunStore.refresh_claim(ctx, :system, "prefixed", lease.claim_token, refreshed_at)

      assert row!("prefixed", "docket_private").claimed_at == refreshed_at

      released_at = DateTime.add(@now, 2, :second)

      assert :ok =
               RunStore.release_claim(ctx, :system, "prefixed", lease.claim_token, released_at)

      assert row!("prefixed", "docket_private").wake_at == released_at
    end

    defp policy(now, overrides \\ []) do
      Map.merge(
        %{now: now, limit: 1, orphan_ttl_ms: 1_000, max_claim_attempts: 3},
        Map.new(overrides)
      )
    end

    defp claim_due(now, overrides \\ []) do
      RunStore.claim_due(TestRepo, :system, policy(now, overrides))
    end

    defp claim_one(now, overrides \\ []) do
      assert {:ok, %{leases: [lease], poisoned: []}} = claim_due(now, overrides)
      lease
    end

    defp current_claim?(run_id, token) do
      predicate = RunStore.current_claim(run_id, token)
      TestRepo.exists?(from(run in Run, where: ^predicate))
    end

    defp insert_graph!(prefix \\ nil) do
      changeset = GraphVersion.changeset(%{graph_id: "graph", graph_hash: "hash", graph: %{}})
      TestRepo.insert!(changeset, prefix: prefix)

      GraphArtifact.changeset(%{
        graph_id: "graph",
        graph_hash: "hash",
        compiler_abi: "docket-runtime-graph/v1",
        artifact_hash: "artifact",
        artifact: %{}
      })
      |> TestRepo.insert!(prefix: prefix)
    end

    defp insert_run!(run_id, overrides \\ [], prefix \\ nil) do
      attrs =
        %{
          run_id: run_id,
          graph_id: "graph",
          graph_hash: "hash",
          graph_compiler_abi: "docket-runtime-graph/v1",
          status: :running,
          input: %{},
          state: %{"version" => 2},
          checkpoint_seq: 7,
          started_at: @now,
          wake_at: @now
        }
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
