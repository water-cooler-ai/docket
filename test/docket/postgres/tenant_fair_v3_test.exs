if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.TenantFairV3Test do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.TenantFair.{Budgets, QueryShapes}
    alias Docket.Postgres.TestRepo

    @migration_version 20_260_717_000_176

    defmodule InstallDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      :ok
    end

    test "ratifies one canonical fixed MVP budget set" do
      assert Budgets.as_map() == %{
               scan_inspections: 32,
               grant_outcomes: 8,
               run_lock_attempts: 16,
               max_grants_per_scan_call: 32,
               max_outcomes_per_scan_call: 256,
               max_run_lock_attempts_per_scan_call: 512,
               max_run_rows_mutated_per_scan_call: 256
             }
    end

    test "candidate shapes exclude rejected global and hidden-lock discovery" do
      schedule = ~s("docket_claim_schedule")
      runs = ~s("docket_runs")
      scan = QueryShapes.scan_positions(schedule)
      ready_candidates = QueryShapes.run_candidates(runs, :ready)
      expired_candidates = QueryShapes.run_candidates(runs, :expired)
      rotating_ready = QueryShapes.rotating_run_candidates(runs, :ready)

      for statement <- [
            scan,
            ready_candidates,
            expired_candidates,
            rotating_ready
          ] do
        refute statement =~ ";"
        refute statement =~ "DISTINCT ON"
        refute statement =~ "ROW_NUMBER"
        refute statement =~ "GROUP BY"
        refute statement =~ "FOR UPDATE"
        refute statement =~ "SKIP LOCKED"
      end

      assert scan =~ "WHERE unfinished_count > 0"
      assert scan =~ "visit_ordinal < 32"
      assert ready_candidates =~ "candidate.scope_key = $1"
      assert ready_candidates =~ "LIMIT 16"
      assert expired_candidates =~ "candidate.scope_key = $1"
      assert expired_candidates =~ "LIMIT 16"
      assert rotating_ready =~ "(candidate.wake_at, candidate.id) > ($3, $4)"
      assert rotating_ready =~ "(candidate.wake_at, candidate.id) <= ($3, $4)"

      exact_lock = QueryShapes.exact_run_lock_attempts(runs)
      assert exact_lock =~ "unnest(($1::bigint[])[1:16])"
      assert exact_lock =~ "LIMIT 16"
      assert exact_lock =~ "FOR UPDATE OF runs SKIP LOCKED"
    end

    test "active traversal is fixed at S, excludes zero-count tenants, and wraps" do
      seed_runs(40)

      TestRepo.query!("""
      UPDATE docket_runs
      SET status = 'done', claim_token = NULL, claimed_at = NULL, wake_at = NULL,
          finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
      WHERE scope_key NOT IN ('tenant-0001', 'tenant-0040')
      """)

      [[first_position]] =
        TestRepo.query!(
          "SELECT ring_position FROM docket_claim_schedule WHERE scope_key = 'tenant-0001'"
        ).rows

      rows =
        TestRepo.query!(
          QueryShapes.scan_positions(~s("docket_claim_schedule")),
          [first_position]
        ).rows

      assert length(rows) == Budgets.scan_inspections()
      assert Enum.map(rows, &Enum.at(&1, 5)) == Enum.to_list(1..Budgets.scan_inspections())

      assert rows |> Enum.map(&Enum.at(&1, 1)) |> Enum.uniq() |> Enum.sort() ==
               ["tenant-0001", "tenant-0040"]

      assert rows |> Enum.map(&Enum.at(&1, 6)) |> List.last() == 16

      TestRepo.query!(
        "UPDATE docket_runs " <>
          "SET status = 'done', claim_token = NULL, claimed_at = NULL, wake_at = NULL, " <>
          "finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP " <>
          "WHERE scope_key = 'tenant-0040'"
      )

      one_position_rows =
        TestRepo.query!(
          QueryShapes.scan_positions(~s("docket_claim_schedule")),
          [0]
        ).rows

      assert length(one_position_rows) == Budgets.scan_inspections()
      assert Enum.uniq(Enum.map(one_position_rows, &Enum.at(&1, 1))) == ["tenant-0001"]
      assert one_position_rows |> List.last() |> Enum.at(6) == 31
    end

    test "unfinished counts follow nonterminal state atomically and protect membership" do
      seed_runs(1)

      assert TestRepo.query!(
               "SELECT unfinished_count FROM docket_claim_schedule " <>
                 "WHERE scope_key = 'tenant-0001'"
             ).rows == [[2]]

      assert {:error, :rollback} =
               TestRepo.transaction(fn ->
                 TestRepo.query!("""
                 UPDATE docket_runs
                 SET status = 'done', wake_at = NULL, finished_at = CURRENT_TIMESTAMP
                 WHERE run_id = 'ready-1'
                 """)

                 assert TestRepo.query!(
                          "SELECT unfinished_count FROM docket_claim_schedule " <>
                            "WHERE scope_key = 'tenant-0001'"
                        ).rows == [[1]]

                 TestRepo.rollback(:rollback)
               end)

      assert TestRepo.query!(
               "SELECT unfinished_count FROM docket_claim_schedule " <>
                 "WHERE scope_key = 'tenant-0001'"
             ).rows == [[2]]

      assert_raise Postgrex.Error, fn ->
        TestRepo.query!("DELETE FROM docket_claim_partitions WHERE scope_key = 'tenant-0001'")
      end

      TestRepo.query!("""
      UPDATE docket_runs
      SET status = 'done', claim_token = NULL, claimed_at = NULL, wake_at = NULL,
          finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
      WHERE scope_key = 'tenant-0001'
      """)

      assert TestRepo.query!(
               "SELECT unfinished_count FROM docket_claim_schedule " <>
                 "WHERE scope_key = 'tenant-0001'"
             ).rows == [[0]]

      TestRepo.query!("""
      UPDATE docket_runs
      SET status = 'running', finished_at = NULL, wake_at = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
      WHERE run_id = 'ready-1'
      """)

      assert TestRepo.query!(
               "SELECT unfinished_count FROM docket_claim_schedule " <>
                 "WHERE scope_key = 'tenant-0001'"
             ).rows == [[1]]

      TestRepo.query!("""
      UPDATE docket_runs
      SET status = 'done', wake_at = NULL, finished_at = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
      WHERE run_id = 'ready-1'
      """)

      assert TestRepo.query!(
               "SELECT unfinished_count FROM docket_claim_schedule " <>
                 "WHERE scope_key = 'tenant-0001'"
             ).rows == [[0]]

      TestRepo.query!("DELETE FROM docket_claim_partitions WHERE scope_key = 'tenant-0001'")
      assert TestRepo.query!("SELECT count(*) FROM docket_claim_schedule").rows == [[0]]
    end

    test "truncate paths fail closed and preserve authoritative unfinished membership" do
      seed_runs(1)

      run_message =
        "TRUNCATE docket_runs is unsupported because unfinished counts are trigger-maintained"

      schedule_message =
        "TRUNCATE docket_claim_schedule is unsupported because unfinished membership is authoritative"

      attempts = [
        {"TRUNCATE docket_runs CASCADE", run_message},
        {"TRUNCATE docket_events, docket_runs", run_message},
        {"TRUNCATE docket_claim_schedule", schedule_message},
        {"TRUNCATE docket_claim_partitions CASCADE", schedule_message}
      ]

      for {statement, expected_message} <- attempts do
        error = assert_raise Postgrex.Error, fn -> TestRepo.query!(statement) end
        assert error.postgres.code == :integrity_constraint_violation
        assert error.postgres.message == expected_message

        assert TestRepo.query!("SELECT count(*) FROM docket_runs").rows == [[2]]
        assert TestRepo.query!("SELECT count(*) FROM docket_claim_partitions").rows == [[1]]

        assert TestRepo.query!(
                 "SELECT unfinished_count FROM docket_claim_schedule " <>
                   "WHERE scope_key = 'tenant-0001'"
               ).rows == [[2]]
      end
    end

    test "waiting runs stay unfinished and cannot be orphaned before resuming" do
      seed_runs(1)

      TestRepo.query!("""
      UPDATE docket_runs
      SET status = 'waiting', claim_token = NULL, claimed_at = NULL, wake_at = NULL,
          updated_at = CURRENT_TIMESTAMP
      WHERE run_id = 'ready-1'
      """)

      assert TestRepo.query!(
               "SELECT unfinished_count FROM docket_claim_schedule " <>
                 "WHERE scope_key = 'tenant-0001'"
             ).rows == [[2]]

      assert_raise Postgrex.Error, fn ->
        TestRepo.query!("DELETE FROM docket_claim_partitions WHERE scope_key = 'tenant-0001'")
      end

      TestRepo.query!("""
      UPDATE docket_runs
      SET status = 'running', wake_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
      WHERE run_id = 'ready-1'
      """)

      assert TestRepo.query!(
               "SELECT unfinished_count FROM docket_claim_schedule " <>
                 "WHERE scope_key = 'tenant-0001'"
             ).rows == [[2]]
    end

    test "persisted ready continuation reaches poison beyond two cap-denied K pages" do
      now = DateTime.utc_now()

      TestRepo.query!("""
      INSERT INTO docket_graph_versions
        (tenant_id, graph_id, graph_hash, graph, inserted_at)
      VALUES ('deep', 'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP)
      """)

      TestRepo.query!("INSERT INTO docket_claim_partitions (scope_key) VALUES ('deep')")

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, claim_attempts,
           inserted_at, started_at, updated_at)
        SELECT 'deep-' || series, 'deep', 'graph', 'hash', 'running',
               decode('01', 'hex'), 1,
               $1::timestamptz + series * interval '1 microsecond',
               CASE WHEN series = 33 THEN 5 ELSE 0 END,
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, 33) AS series
        """,
        [now]
      )

      threshold = DateTime.add(now, 60, :second)

      first_page =
        TestRepo.query!(
          QueryShapes.run_candidates(~s("docket_runs"), :ready),
          ["deep", threshold]
        ).rows

      assert length(first_page) == Budgets.run_lock_attempts()
      assert Enum.all?(first_page, &(Enum.at(&1, 2) == 0))
      [last_id, last_eligible_at | _] = first_page |> List.last() |> Enum.take(2)

      TestRepo.query!(
        """
        UPDATE docket_claim_schedule
        SET ready_candidate_cursor_at = $1,
            ready_candidate_cursor_id = $2
        WHERE scope_key = 'deep'
        """,
        [last_eligible_at, last_id]
      )

      [[persisted_at, persisted_id]] =
        TestRepo.query!(
          "SELECT ready_candidate_cursor_at, ready_candidate_cursor_id " <>
            "FROM docket_claim_schedule WHERE scope_key = 'deep'"
        ).rows

      second_page =
        TestRepo.query!(
          QueryShapes.rotating_run_candidates(~s("docket_runs"), :ready),
          ["deep", threshold, persisted_at, persisted_id]
        ).rows

      assert length(second_page) == Budgets.run_lock_attempts()
      assert Enum.all?(second_page, &(Enum.at(&1, 2) == 0))
      [last_id, last_eligible_at | _] = second_page |> List.last() |> Enum.take(2)

      TestRepo.query!(
        """
        UPDATE docket_claim_schedule
        SET ready_candidate_cursor_at = $1,
            ready_candidate_cursor_id = $2
        WHERE scope_key = 'deep'
        """,
        [last_eligible_at, last_id]
      )

      continued =
        TestRepo.query!(
          QueryShapes.rotating_run_candidates(~s("docket_runs"), :ready),
          ["deep", threshold, last_eligible_at, last_id]
        ).rows

      assert length(continued) == Budgets.run_lock_attempts()
      assert [poison | _] = continued
      assert Enum.at(poison, 2) == 5
      assert Enum.at(poison, 4) == false
      assert Enum.count(continued, &Enum.at(&1, 4)) == 15
    end

    test "exact partition authority attempt does not skip to a later partition" do
      seed_partitions(2)
      parent = self()

      blocker =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!(
              "SELECT scope_key FROM docket_claim_partitions " <>
                "WHERE scope_key = 'tenant-0001' FOR UPDATE"
            )

            send(parent, {:partition_locked, self()})

            receive do
              :release -> :released
            after
              5_000 -> raise "timed out waiting to release partition lock"
            end
          end)
        end)

      assert_receive {:partition_locked, blocker_pid}, 2_000

      try do
        assert TestRepo.query!(
                 QueryShapes.partition_lock_attempt(~s("docket_claim_partitions")),
                 ["tenant-0001"]
               ).rows == []

        assert [["tenant-0002", nil, 0, 0]] =
                 TestRepo.query!(
                   QueryShapes.partition_lock_attempt(~s("docket_claim_partitions")),
                   ["tenant-0002"]
                 ).rows
      after
        send(blocker_pid, :release)
      end

      assert Task.await(blocker, 2_000) == {:ok, :released}
    end

    test "the singleton scan cursor serializes pollers and rollback preserves state" do
      parent = self()

      blocker =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            assert [[0]] =
                     TestRepo.query!(QueryShapes.scan_cursor_lock(~s("docket_claim_policy"))).rows

            TestRepo.query!(
              "UPDATE docket_claim_policy " <>
                "SET scan_ring_position = 99 WHERE id = 1"
            )

            send(parent, {:cursor_locked, self()})

            receive do
              :rollback -> TestRepo.rollback(:intentional)
            after
              5_000 -> raise "timed out waiting to roll back cursor lock"
            end
          end)
        end)

      assert_receive {:cursor_locked, blocker_pid}, 2_000

      error =
        assert_raise Postgrex.Error, fn ->
          TestRepo.checkout(fn ->
            TestRepo.query!("SET lock_timeout = '100ms'")

            try do
              TestRepo.query!(QueryShapes.scan_cursor_lock(~s("docket_claim_policy")))
            after
              TestRepo.query!("SET lock_timeout = 0")
            end
          end)
        end

      assert error.postgres.code == :lock_not_available
      send(blocker_pid, :rollback)
      assert Task.await(blocker, 2_000) == {:error, :intentional}

      assert TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy").rows == [[0]]
    end

    test "exact-key lock attempts cannot scan past a locked prefix" do
      TestRepo.query!("CREATE TABLE lock_probe_runs (id bigint PRIMARY KEY)")
      TestRepo.query!("INSERT INTO lock_probe_runs (id) SELECT generate_series(1, 100)")
      parent = self()

      blocker =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT id FROM lock_probe_runs WHERE id = 1 FOR UPDATE")
            send(parent, {:locked, self()})

            receive do
              :release -> :released
            after
              5_000 -> raise "timed out waiting to release exact-key lock"
            end
          end)
        end)

      assert_receive {:locked, blocker_pid}, 2_000

      try do
        assert TestRepo.query!(
                 QueryShapes.exact_run_lock_attempts(~s("lock_probe_runs")),
                 [Enum.to_list(1..100)]
               ).rows == Enum.map(2..16, &[&1, &1])
      after
        send(blocker_pid, :release)
      end

      assert Task.await(blocker, 2_000) == {:ok, :released}

      assert TestRepo.query!(QueryShapes.mutation_ids(), [Enum.to_list(1..100)]).rows ==
               Enum.map(1..Budgets.grant_outcomes(), &[&1, &1])
    end

    defp seed_partitions(count) do
      TestRepo.query!(
        """
        INSERT INTO docket_claim_partitions (scope_key)
        SELECT 'tenant-' || lpad(series::text, 4, '0')
        FROM generate_series(1, $1) AS series
        """,
        [count]
      )
    end

    defp seed_runs(count) do
      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        SELECT 'tenant-' || lpad(series::text, 4, '0'),
               'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP
        FROM generate_series(1, $1) AS series
        """,
        [count]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_claim_partitions (scope_key)
        SELECT 'tenant-' || lpad(series::text, 4, '0')
        FROM generate_series(1, $1) AS series
        ON CONFLICT (scope_key) DO NOTHING
        """,
        [count]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        SELECT 'ready-' || series,
               'tenant-' || lpad(series::text, 4, '0'),
               'graph', 'hash', 'running', decode('01', 'hex'), 1,
               CURRENT_TIMESTAMP - interval '1 minute',
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, $1) AS series
        """,
        [count]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, claim_token, claimed_at,
           inserted_at, started_at, updated_at)
        SELECT 'expired-' || series,
               'tenant-' || lpad(series::text, 4, '0'),
               'graph', 'hash', 'running', decode('01', 'hex'), 1,
               ('00000000-0000-0000-0000-' || lpad(series::text, 12, '0'))::uuid,
               CURRENT_TIMESTAMP - interval '2 hours',
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, $1) AS series
        """,
        [count]
      )
    end
  end
end
