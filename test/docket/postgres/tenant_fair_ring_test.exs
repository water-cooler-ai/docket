if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.TenantFairRingTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.TenantFair.{Budgets, QueryShapes, RingFunction}
    alias Docket.Postgres.TestRepo

    @migration_version 20_260_717_000_176
    @trace_columns [
      :row_kind,
      :error_reason,
      :run_id,
      :tenant_id,
      :graph_id,
      :graph_hash,
      :checkpoint_seq,
      :claim_token,
      :claimed_at,
      :claim_attempt,
      :poisoned_at,
      :poison_reason,
      :work_class,
      :eligible_at,
      :call_token,
      :transaction_id,
      :visit_ordinal,
      :outcome_ordinal,
      :demand,
      :cursor_before,
      :cursor_after,
      :ring_position,
      :scope_key,
      :disposition,
      :outcome_count,
      :epoch_delta
    ]

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

    test "raw trace exposes one deterministic H=1 grant without changing public columns" do
      seed_runs(1)
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      cutoff = DateTime.add(now, -3_600, :second)

      rows =
        TestRepo.query!(
          """
          SELECT claimed.*
          FROM docket_tenant_fair_claim($1, $2, $3, $4, $5, $6, true)
            AS claimed(#{RingFunction.result_definition()})
          ORDER BY claimed.visit_ordinal, claimed.outcome_ordinal NULLS LAST,
                   claimed.row_kind
          """,
          [now, cutoff, 2, 5, nil, 4]
        ).rows

      outcomes = Enum.filter(rows, &(hd(&1) == "outcome"))
      [inspection] = Enum.filter(rows, &(hd(&1) == "inspection"))

      assert length(outcomes) == 2
      assert Enum.map(outcomes, &Enum.at(&1, 16)) == [1, 1]
      assert Enum.map(outcomes, &Enum.at(&1, 17)) == [1, 2]
      assert Enum.at(inspection, 16) == 1
      assert Enum.at(inspection, 19) == 0
      assert Enum.at(inspection, 20) == Enum.at(inspection, 21)
      assert Enum.at(inspection, 22) == "tenant-0001"
      assert Enum.at(inspection, 23) == "grant"
      assert Enum.at(inspection, 24) == 2
      assert Enum.at(inspection, 25) == 1

      assert [[cursor]] =
               TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy").rows

      assert cursor == Enum.at(inspection, 20)

      assert [[1]] =
               TestRepo.query!(
                 "SELECT admission_epoch FROM docket_claim_partitions " <>
                   "WHERE scope_key = 'tenant-0001'"
               ).rows
    end

    test "installed function never substitutes K+1 when all first-K exact locks miss" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      seed_scope("locked", 100)

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, claim_attempts,
           inserted_at, started_at, updated_at)
        SELECT 'locked-' || series, 'locked', 'graph', 'hash', 'running',
               decode('01', 'hex'), 1,
               $1::timestamptz - interval '1 minute' + series * interval '1 microsecond',
               0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, 17) AS series
        """,
        [now]
      )

      ids =
        TestRepo.query!(
          "SELECT id FROM docket_runs ORDER BY wake_at, id LIMIT $1",
          [Budgets.run_lock_attempts()]
        ).rows
        |> List.flatten()

      parent = self()

      blocker =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!(
              "SELECT id FROM docket_runs WHERE id = ANY($1::bigint[]) FOR UPDATE",
              [ids]
            )

            send(parent, {:first_k_locked, self()})

            receive do
              :release -> :released
            after
              5_000 -> raise "timed out waiting to release first-K locks"
            end
          end)
        end)

      assert_receive {:first_k_locked, blocker_pid}, 2_000

      rows = raw_claim!(now, demand: 1, default_max: 100)
      inspections = Enum.filter(rows, &(&1.row_kind == "inspection"))

      assert Enum.filter(rows, &(&1.row_kind == "outcome")) == []
      assert length(inspections) == Budgets.scan_inspections()
      assert Enum.all?(inspections, &(&1.disposition == "lock_miss"))
      assert Enum.all?(inspections, &(&1.outcome_count == 0 and &1.epoch_delta == 0))
      assert Enum.uniq(Enum.map(inspections, & &1.scope_key)) == ["locked"]
      assert length(Enum.uniq(Enum.map(inspections, & &1.ring_position))) == 1

      send(blocker_pid, :release)
      assert Task.await(blocker, 2_000) == {:ok, :released}

      assert [[nil, nil, 0]] =
               TestRepo.query!(
                 "SELECT claim_token, claimed_at, claim_attempts FROM docket_runs " <>
                   "WHERE run_id = 'locked-17'"
               ).rows

      assert [[0]] =
               TestRepo.query!(
                 "SELECT admission_epoch FROM docket_claim_partitions " <>
                   "WHERE scope_key = 'locked'"
               ).rows
    end

    test "installed function skips a partial exact-lock miss within one frozen attempt set" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      seed_scope("partial", 10)

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        SELECT 'partial-' || series, 'partial', 'graph', 'hash', 'running',
               decode('01', 'hex'), 1,
               $1::timestamptz - interval '1 minute' + series * interval '1 microsecond',
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, 2) AS series
        """,
        [now]
      )

      [[first_id]] =
        TestRepo.query!("SELECT id FROM docket_runs ORDER BY wake_at, id LIMIT 1").rows

      parent = self()

      blocker =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT id FROM docket_runs WHERE id = $1 FOR UPDATE", [first_id])
            send(parent, {:first_locked, self()})

            receive do
              :release -> :released
            after
              5_000 -> raise "timed out waiting to release first exact lock"
            end
          end)
        end)

      assert_receive {:first_locked, blocker_pid}, 2_000

      [outcome] =
        raw_claim!(now, demand: 1, default_max: 10)
        |> Enum.filter(&(&1.row_kind == "outcome"))

      assert %{run_id: "partial-2", visit_ordinal: 1, outcome_ordinal: 1} = outcome

      send(blocker_pid, :release)
      assert Task.await(blocker, 2_000) == {:ok, :released}

      assert [[nil, nil, 0]] =
               TestRepo.query!(
                 "SELECT claim_token, claimed_at, claim_attempts FROM docket_runs " <>
                   "WHERE run_id = 'partial-1'"
               ).rows
    end

    test "expired service preserves capped ready continuation across K pages in one call" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      seed_scope("deep", 1)

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, claim_token, claimed_at, claim_attempts,
           inserted_at, started_at, updated_at)
        VALUES
          ('deep-live', 'deep', 'graph', 'hash', 'running', decode('01', 'hex'),
           1, pg_catalog.gen_random_uuid(), $1::timestamptz - interval '2 hours', 1,
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [now]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, claim_attempts,
           inserted_at, started_at, updated_at)
        SELECT 'deep-' || series, 'deep', 'graph', 'hash', 'running',
               decode('01', 'hex'), 1,
               $1::timestamptz - interval '1 minute' + series * interval '1 microsecond',
               CASE WHEN series = 33 THEN 5 ELSE 0 END,
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, 33) AS series
        """,
        [now]
      )

      rows = raw_claim!(now, demand: 2, default_max: 1, max_attempts: 5)
      outcomes = Enum.filter(rows, &(&1.row_kind == "outcome"))
      inspections = Enum.filter(rows, &(&1.row_kind == "inspection"))

      assert Enum.map(outcomes, & &1.work_class) == ["expired", "ready"]
      assert Enum.map(outcomes, & &1.visit_ordinal) == [1, 3]

      outcome = List.last(outcomes)

      assert %{
               run_id: "deep-33",
               claim_token: nil,
               work_class: "ready",
               visit_ordinal: 3,
               poison_reason: "max_claim_attempts_exceeded"
             } = outcome

      assert Enum.map(inspections, & &1.disposition) == ["grant", "empty_page", "grant"]
      assert Enum.map(inspections, & &1.epoch_delta) == [1, 0, 1]

      assert [[nil, nil]] =
               TestRepo.query!(
                 "SELECT ready_candidate_cursor_at, ready_candidate_cursor_id " <>
                   "FROM docket_claim_schedule WHERE scope_key = 'deep'"
               ).rows

      assert [[32]] =
               TestRepo.query!(
                 "SELECT count(*)::integer FROM docket_runs WHERE scope_key = 'deep' " <>
                   "AND run_id <> 'deep-live' AND run_id <> 'deep-33' " <>
                   "AND claim_token IS NULL AND poisoned_at IS NULL AND claim_attempts = 0"
               ).rows

      assert [[2]] =
               TestRepo.query!(
                 "SELECT admission_epoch FROM docket_claim_partitions " <>
                   "WHERE scope_key = 'deep'"
               ).rows
    end

    test "installed function traverses only the active ring for S visits and persists its cursor" do
      seed_runs(40)

      TestRepo.query!("""
      UPDATE docket_runs
      SET status = 'waiting', claim_token = NULL, claimed_at = NULL, wake_at = NULL,
          updated_at = CURRENT_TIMESTAMP
      """)

      TestRepo.query!("""
      UPDATE docket_runs
      SET status = 'done', finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
      WHERE scope_key = 'tenant-0020'
      """)

      positions =
        TestRepo.query!(
          "SELECT ring_position, scope_key FROM docket_claim_schedule " <>
            "WHERE unfinished_count > 0 ORDER BY ring_position"
        ).rows

      [cursor_before, _scope] = Enum.at(positions, -2)
      [max_position, max_scope] = List.last(positions)
      [min_position, min_scope] = hd(positions)

      TestRepo.query!(
        "UPDATE docket_claim_policy SET scan_ring_position = $1 WHERE id = 1",
        [cursor_before]
      )

      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      rows = raw_claim!(now, demand: 1, default_max: 4)
      inspections = Enum.filter(rows, &(&1.row_kind == "inspection"))

      assert Enum.filter(rows, &(&1.row_kind == "outcome")) == []
      assert length(inspections) == Budgets.scan_inspections()
      assert Enum.map(inspections, & &1.visit_ordinal) == Enum.to_list(1..32)
      assert Enum.map(inspections, & &1.scope_key) |> Enum.uniq() |> length() == 32
      refute Enum.any?(inspections, &(&1.scope_key == "tenant-0020"))

      expected_positions =
        positions
        |> Enum.split_while(fn [position, _scope] -> position <= cursor_before end)
        |> then(fn {before_cursor, after_cursor} -> after_cursor ++ before_cursor end)
        |> Stream.cycle()
        |> Enum.take(Budgets.scan_inspections())

      assert Enum.map(inspections, &[&1.ring_position, &1.scope_key]) == expected_positions
      assert hd(inspections).ring_position == max_position
      assert hd(inspections).scope_key == max_scope
      assert Enum.at(inspections, 1).ring_position == min_position
      assert Enum.at(inspections, 1).scope_key == min_scope
      assert hd(inspections).cursor_before == cursor_before

      assert Enum.zip(inspections, tl(inspections))
             |> Enum.all?(fn {left, right} -> left.cursor_after == right.cursor_before end)

      assert [[List.last(inspections).cursor_after]] ==
               TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy WHERE id = 1").rows
    end

    test "installed function repeats a short active ring in exact cyclic order through S visits" do
      seed_runs(3)

      TestRepo.query!("""
      UPDATE docket_runs
      SET status = 'waiting', claim_token = NULL, claimed_at = NULL, wake_at = NULL,
          updated_at = CURRENT_TIMESTAMP
      """)

      positions =
        TestRepo.query!(
          "SELECT ring_position, scope_key FROM docket_claim_schedule " <>
            "WHERE unfinished_count > 0 ORDER BY ring_position"
        ).rows

      [cursor_before, _scope] = List.last(positions)

      TestRepo.query!(
        "UPDATE docket_claim_policy SET scan_ring_position = $1 WHERE id = 1",
        [cursor_before]
      )

      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      inspections =
        raw_claim!(now, demand: 1, default_max: 4)
        |> Enum.filter(&(&1.row_kind == "inspection"))

      expected_positions =
        positions
        |> Stream.cycle()
        |> Enum.take(Budgets.scan_inspections())

      assert length(inspections) == Budgets.scan_inspections()
      assert Enum.map(inspections, &[&1.ring_position, &1.scope_key]) == expected_positions
      assert Enum.map(inspections, & &1.visit_ordinal) == Enum.to_list(1..32)

      final_cursor = List.last(inspections).cursor_after

      assert [[^final_cursor]] =
               TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy WHERE id = 1").rows
    end

    test "installed function leaves the cursor unchanged when the active ring is empty" do
      TestRepo.query!("UPDATE docket_claim_policy SET scan_ring_position = 42 WHERE id = 1")

      [[cursor_before]] =
        TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy WHERE id = 1").rows

      assert cursor_before == 42

      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      assert raw_claim!(now, demand: 1, default_max: 4) == []

      assert [[^cursor_before]] =
               TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy WHERE id = 1").rows
    end

    test "class reservation carries across partitions before filling the final slot" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      seed_scope("ready", 10)
      seed_scope("expired", 10)

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        SELECT 'ready-' || series, 'ready', 'graph', 'hash', 'running',
               decode('01', 'hex'), 1,
               $1::timestamptz - interval '1 minute' + series * interval '1 microsecond',
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, 3) AS series
        """,
        [now]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, claim_token, claimed_at, claim_attempts,
           inserted_at, started_at, updated_at)
        VALUES
          ('expired-1', 'expired', 'graph', 'hash', 'running', decode('01', 'hex'),
           1, pg_catalog.gen_random_uuid(), $1::timestamptz - interval '2 hours', 1,
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [now]
      )

      [[expired_position]] =
        TestRepo.query!(
          "SELECT ring_position FROM docket_claim_schedule WHERE scope_key = 'expired'"
        ).rows

      TestRepo.query!(
        "UPDATE docket_claim_policy SET scan_ring_position = $1 WHERE id = 1",
        [expired_position]
      )

      outcomes =
        raw_claim!(now, demand: 3, default_max: 10)
        |> Enum.filter(&(&1.row_kind == "outcome"))

      assert Enum.map(outcomes, & &1.work_class) == ["ready", "ready", "expired"]
      assert Enum.map(outcomes, & &1.visit_ordinal) == [1, 1, 2]

      assert [[nil]] =
               TestRepo.query!("SELECT claim_token FROM docket_runs WHERE run_id = 'ready-3'").rows
    end

    test "poison serves its class and releases the final reserved outcome" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      for scope <- ["class-a", "class-b", "class-c"] do
        seed_scope(scope, 10)
      end

      [ready_before, expired_poison, ready_after] =
        TestRepo.query!("SELECT scope_key FROM docket_claim_schedule ORDER BY ring_position").rows
        |> List.flatten()

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        VALUES
          ('ready-before', $1, 'graph', 'hash', 'running', decode('01', 'hex'),
           1, $3::timestamptz - interval '1 minute',
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
          ('ready-after', $2, 'graph', 'hash', 'running', decode('01', 'hex'),
           1, $3::timestamptz - interval '1 minute',
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [ready_before, ready_after, now]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, claim_token, claimed_at, claim_attempts,
           inserted_at, started_at, updated_at)
        VALUES
          ('expired-poison', $1, 'graph', 'hash', 'running', decode('01', 'hex'),
           1, pg_catalog.gen_random_uuid(), $2::timestamptz - interval '2 hours', 5,
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [expired_poison, now]
      )

      [[cursor_before]] =
        TestRepo.query!(
          "SELECT ring_position FROM docket_claim_schedule WHERE scope_key = $1",
          [ready_after]
        ).rows

      TestRepo.query!(
        "UPDATE docket_claim_policy SET scan_ring_position = $1 WHERE id = 1",
        [cursor_before]
      )

      outcomes =
        raw_claim!(now, demand: 3, default_max: 10)
        |> Enum.filter(&(&1.row_kind == "outcome"))

      assert Enum.map(outcomes, & &1.run_id) ==
               ["ready-before", "expired-poison", "ready-after"]

      assert Enum.map(outcomes, & &1.work_class) == ["ready", "expired", "ready"]
      assert Enum.map(outcomes, & &1.visit_ordinal) == [1, 2, 3]

      assert %{claim_token: nil, poison_reason: "max_claim_attempts_exceeded"} =
               Enum.at(outcomes, 1)

      assert %{claim_token: token} = List.last(outcomes)
      assert token != nil
    end

    test "class reservation conservatively underfills when the other class is absent" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      seed_scope("ready-a", 10)
      seed_scope("ready-b", 10)

      for scope <- ["ready-a", "ready-b"] do
        TestRepo.query!(
          """
          INSERT INTO docket_runs
            (run_id, tenant_id, graph_id, graph_hash, status, state,
             checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
          SELECT $1 || '-' || series, $1, 'graph', 'hash', 'running',
                 decode('01', 'hex'), 1,
                 $2::timestamptz - interval '1 minute' + series * interval '1 microsecond',
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          FROM generate_series(1, 2) AS series
          """,
          [scope, now]
        )
      end

      rows = raw_claim!(now, demand: 2, default_max: 10)
      outcomes = Enum.filter(rows, &(&1.row_kind == "outcome"))
      inspections = Enum.filter(rows, &(&1.row_kind == "inspection"))

      assert length(outcomes) == 1
      assert hd(outcomes).work_class == "ready"
      assert length(inspections) == Budgets.scan_inspections()

      assert [[1]] =
               TestRepo.query!(
                 "SELECT count(*)::integer FROM docket_runs WHERE claim_token IS NOT NULL"
               ).rows
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

    defp seed_scope(scope_key, max_active) do
      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, 'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP)
        """,
        [scope_key]
      )

      TestRepo.query!(
        "INSERT INTO docket_claim_partitions (scope_key, max_active) VALUES ($1, $2)",
        [scope_key, max_active]
      )
    end

    defp raw_claim!(now, options) do
      cutoff = Keyword.get(options, :cutoff, DateTime.add(now, -3_600, :second))
      demand = Keyword.fetch!(options, :demand)
      max_attempts = Keyword.get(options, :max_attempts, 5)
      preference = Keyword.get(options, :preference)
      default_max = Keyword.fetch!(options, :default_max)

      TestRepo.query!(
        """
        SELECT claimed.*
        FROM docket_tenant_fair_claim($1, $2, $3, $4, $5, $6, true)
          AS claimed(#{RingFunction.result_definition()})
        ORDER BY claimed.visit_ordinal NULLS FIRST,
                 claimed.outcome_ordinal NULLS FIRST,
                 claimed.row_kind
        """,
        [now, cutoff, demand, max_attempts, preference, default_max]
      ).rows
      |> Enum.map(fn row -> Map.new(Enum.zip(@trace_columns, row)) end)
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
