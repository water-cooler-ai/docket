if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.FairRotationDatabaseProofTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.TenantFair.{Budgets, RingFunction}
    alias Docket.Postgres.{RunStore, TestRepo}
    alias Docket.Test.ConcurrentAdmissionHarness.FairRotationOracle
    alias Docket.Test.FairRotationTargetWitness

    @migration_version 20_260_719_000_179
    @now ~U[2026-07-19 12:00:00.000000Z]
    @trace_columns RingFunction.result_columns() |> Keyword.keys()

    defmodule InstallDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule SecondRepo do
      use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      Application.put_env(:docket, SecondRepo, Keyword.put(config, :pool_size, 4))
      start_supervised!(SecondRepo)
      :ok
    end

    for backlog <- [2, 10, 1_000] do
      test "Legacy bypass is #{backlog} while TenantFair remains backlog-independent" do
        backlog = unquote(backlog)
        seed_work(backlog)

        assert legacy_bypasses_before_target(backlog) == backlog

        TestRepo.query!("DELETE FROM docket_runs")

        # Switch and initialize the engine before the qualified window begins.
        # The empty active ring leaves the cursor unchanged and emits no trace.
        assert raw_tenant_fair_claim!() == []

        seed_work(backlog)

        ring = ring_snapshot()
        policy = policy_snapshot()
        epochs = epoch_snapshot()

        assert Enum.map(ring, &elem(&1, 1)) == ["hot", "target"]
        target_before_first = target_admissible?()
        assert target_before_first

        first_call = %{rows: raw_tenant_fair_claim!(), committed: true}
        ring_after_first = ring_snapshot()
        policy_after_first = policy_snapshot()
        epochs_after_first = epoch_snapshot()

        target_before_second = target_admissible?()
        assert target_before_second

        second_call = %{rows: raw_tenant_fair_claim!(), committed: true}
        ring_after_second = ring_snapshot()
        policy_after_second = policy_snapshot()
        epochs_after_second = epoch_snapshot()

        window = %{
          calls: [first_call, second_call],
          ring_snapshots: [ring, ring_after_first, ring_after_second],
          policy_snapshots: [policy, policy_after_first, policy_after_second],
          epoch_snapshots: [epochs, epochs_after_first, epochs_after_second],
          target_admissible_before_calls: [target_before_first, target_before_second],
          repair_active: false,
          instrumentation_complete: true
        }

        result =
          FairRotationOracle.assert_database_trace!(window,
            target: "target",
            cohort: ["hot", "target"],
            ring: ring,
            scan_budget: Budgets.scan_inspections(),
            quantum: Budgets.grant_outcomes(),
            lock_failures: 0
          )

        assert result.observed_other_grants == 1
        assert result.observed_other_outcomes == 1
        assert result.observed_scan_calls == 2
        assert result.observed_database_calls == 2
        assert result.observed_other_grants <= result.other_grants
        assert result.observed_other_outcomes <= result.other_outcomes
        assert result.observed_scan_calls <= result.scan_calls
      end
    end

    test "database trace reaches the competing-outcome Q bound" do
      assert raw_tenant_fair_claim!() == []
      seed_work(7)
      insert_expired_hot()

      ring = ring_snapshot()
      policy = policy_snapshot()
      epochs = epoch_snapshot()
      target_before = target_admissible?()
      assert target_before

      call = %{rows: raw_tenant_fair_claim!(demand: 9), committed: true}

      window = %{
        calls: [call],
        ring_snapshots: [ring, ring_snapshot()],
        policy_snapshots: [policy, policy_snapshot()],
        epoch_snapshots: [epochs, epoch_snapshot()],
        target_admissible_before_calls: [target_before],
        repair_active: false,
        instrumentation_complete: true
      }

      result =
        FairRotationOracle.assert_database_trace!(window,
          target: "target",
          cohort: ["hot", "target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 0
        )

      assert result.observed_other_grants == 1
      assert result.observed_other_outcomes == Budgets.grant_outcomes()
      assert result.observed_other_outcomes == result.other_outcomes
      assert result.observed_scan_calls == 1
    end

    test "database trace reaches the demand-aware qualifying-call bound" do
      assert raw_tenant_fair_claim!() == []
      seed_long_ring()

      ring = ring_snapshot()
      policy = policy_snapshot()
      epochs = epoch_snapshot()
      assert length(ring) == 35

      target_before_first = target_admissible?()
      first = %{rows: raw_tenant_fair_claim!(), committed: true}
      ring_after_first = ring_snapshot()
      policy_after_first = policy_snapshot()
      epochs_after_first = epoch_snapshot()

      target_before_second = target_admissible?()
      second = %{rows: raw_tenant_fair_claim!(), committed: true}
      ring_after_second = ring_snapshot()
      policy_after_second = policy_snapshot()
      epochs_after_second = epoch_snapshot()

      target_before_third = target_admissible?()
      third = %{rows: raw_tenant_fair_claim!(), committed: true}

      assert Enum.count(second.rows, &(&1.row_kind == "inspection")) ==
               Budgets.scan_inspections()

      window = %{
        calls: [first, second, third],
        ring_snapshots: [ring, ring_after_first, ring_after_second, ring_snapshot()],
        policy_snapshots: [policy, policy_after_first, policy_after_second, policy_snapshot()],
        epoch_snapshots: [epochs, epochs_after_first, epochs_after_second, epoch_snapshot()],
        target_admissible_before_calls: [
          target_before_first,
          target_before_second,
          target_before_third
        ],
        repair_active: false,
        instrumentation_complete: true
      }

      result =
        FairRotationOracle.assert_database_trace!(window,
          target: "target",
          cohort: ["hot", "target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 0
        )

      assert result.observed_other_grants == 1
      assert result.observed_other_outcomes == 1
      assert result.observed_scan_calls == 3
      assert result.observed_scan_calls == result.scan_calls
    end

    for {label, lock_sql, expected_disposition} <- [
          {"partition lock skip",
           "SELECT scope_key FROM docket_claim_partitions WHERE scope_key = 'target' FOR UPDATE",
           "partition_lock_skip"},
          {"run lock miss", "SELECT id FROM docket_runs WHERE run_id = 'target' FOR UPDATE",
           "lock_miss"}
        ] do
      test "scripted #{label} is counted in L and remains bounded" do
        assert raw_tenant_fair_claim!() == []
        seed_work(3)

        ring = ring_snapshot()
        policy = policy_snapshot()
        epochs = epoch_snapshot()
        target_before_first = target_admissible?()
        first = %{rows: raw_tenant_fair_claim!(), committed: true}
        ring_after_first = ring_snapshot()
        policy_after_first = policy_snapshot()
        epochs_after_first = epoch_snapshot()

        parent = self()
        gate = make_ref()

        blocker =
          Task.async(fn ->
            SecondRepo.transaction(fn ->
              SecondRepo.query!(unquote(lock_sql))
              send(parent, {gate, :locked})

              receive do
                {^gate, :release} -> :ok
              after
                5_000 -> raise "timed out waiting to release scripted fairness lock"
              end
            end)
          end)

        assert_receive {^gate, :locked}, 2_000

        target_before_second = target_admissible?()
        second = %{rows: raw_tenant_fair_claim!(), committed: true}
        ring_after_second = ring_snapshot()
        policy_after_second = policy_snapshot()
        epochs_after_second = epoch_snapshot()

        inspections = Enum.filter(second.rows, &(&1.row_kind == "inspection"))

        assert Enum.map(inspections, &{&1.scope_key, &1.disposition}) == [
                 {"target", unquote(expected_disposition)},
                 {"hot", "grant"}
               ]

        send(blocker.pid, {gate, :release})
        assert {:ok, :ok} = Task.await(blocker, 2_000)

        target_before_third = target_admissible?()
        third = %{rows: raw_tenant_fair_claim!(), committed: true}

        window = %{
          calls: [first, second, third],
          ring_snapshots: [ring, ring_after_first, ring_after_second, ring_snapshot()],
          policy_snapshots: [policy, policy_after_first, policy_after_second, policy_snapshot()],
          epoch_snapshots: [epochs, epochs_after_first, epochs_after_second, epoch_snapshot()],
          target_admissible_before_calls: [
            target_before_first,
            target_before_second,
            target_before_third
          ],
          repair_active: false,
          instrumentation_complete: true
        }

        result =
          FairRotationOracle.assert_database_trace!(window,
            target: "target",
            cohort: ["hot", "target"],
            ring: ring,
            scan_budget: Budgets.scan_inspections(),
            quantum: Budgets.grant_outcomes(),
            lock_failures: 1
          )

        assert result.observed_other_grants == 2
        assert result.observed_other_grants == result.other_grants
        assert result.observed_scan_calls == 3

        assert_raise ArgumentError, ~r/target failed 1 inspections; L allows 0/, fn ->
          FairRotationOracle.assert_database_trace!(window,
            target: "target",
            cohort: ["hot", "target"],
            ring: ring,
            scan_budget: Budgets.scan_inspections(),
            quantum: Budgets.grant_outcomes(),
            lock_failures: 0
          )
        end
      end
    end

    test "two consecutive target lock failures prove live L=2 and reject L=1" do
      assert raw_tenant_fair_claim!() == []
      seed_work(5)

      ring = ring_snapshot()
      policy = policy_snapshot()
      epochs = epoch_snapshot()
      target_before_first = target_admissible?()
      first = %{rows: raw_tenant_fair_claim!(), committed: true}
      ring_after_first = ring_snapshot()
      policy_after_first = policy_snapshot()
      epochs_after_first = epoch_snapshot()

      parent = self()
      gate = make_ref()

      blocker =
        Task.async(fn ->
          SecondRepo.transaction(fn ->
            SecondRepo.query!(
              "SELECT scope_key FROM docket_claim_partitions WHERE scope_key = 'target' FOR UPDATE"
            )

            send(parent, {gate, :locked})

            receive do
              {^gate, :release} -> :ok
            after
              5_000 -> raise "timed out holding target through two failed inspections"
            end
          end)
        end)

      assert_receive {^gate, :locked}, 2_000

      target_before_second = target_admissible?()
      second = %{rows: raw_tenant_fair_claim!(), committed: true}
      ring_after_second = ring_snapshot()
      policy_after_second = policy_snapshot()
      epochs_after_second = epoch_snapshot()

      target_before_third = target_admissible?()
      third = %{rows: raw_tenant_fair_claim!(), committed: true}
      ring_after_third = ring_snapshot()
      policy_after_third = policy_snapshot()
      epochs_after_third = epoch_snapshot()

      assert Enum.map([second, third], fn call ->
               Enum.map(
                 Enum.filter(call.rows, &(&1.row_kind == "inspection")),
                 &{&1.scope_key, &1.disposition}
               )
             end) == [
               [{"target", "partition_lock_skip"}, {"hot", "grant"}],
               [{"target", "partition_lock_skip"}, {"hot", "grant"}]
             ]

      send(blocker.pid, {gate, :release})
      assert {:ok, :ok} = Task.await(blocker, 2_000)

      target_before_fourth = target_admissible?()
      fourth = %{rows: raw_tenant_fair_claim!(), committed: true}

      window = %{
        calls: [first, second, third, fourth],
        ring_snapshots: [
          ring,
          ring_after_first,
          ring_after_second,
          ring_after_third,
          ring_snapshot()
        ],
        policy_snapshots: [
          policy,
          policy_after_first,
          policy_after_second,
          policy_after_third,
          policy_snapshot()
        ],
        epoch_snapshots: [
          epochs,
          epochs_after_first,
          epochs_after_second,
          epochs_after_third,
          epoch_snapshot()
        ],
        target_admissible_before_calls: [
          target_before_first,
          target_before_second,
          target_before_third,
          target_before_fourth
        ],
        instrumentation_complete: true
      }

      result =
        FairRotationOracle.assert_database_trace!(window,
          target: "target",
          cohort: ["hot", "target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 2
        )

      assert result.observed_other_grants == 3
      assert result.observed_other_grants == result.other_grants
      assert result.observed_scan_calls == 4

      assert_raise ArgumentError, ~r/target failed 2 inspections; L allows 1/, fn ->
        FairRotationOracle.assert_database_trace!(window,
          target: "target",
          cohort: ["hot", "target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 1
        )
      end
    end

    test "concurrent pollers commit one contiguous cursor sequence without duplicate service" do
      assert raw_tenant_fair_claim!() == []
      Enum.each(1..4, &insert_ready_scope("scope-#{&1}"))

      ring = ring_snapshot()

      [[cursor_before]] =
        TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy").rows

      parent = self()
      gate = make_ref()

      first_task =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT id FROM docket_claim_policy WHERE id = 1 FOR UPDATE")
            send(parent, {gate, :first_holds_cursor})

            receive do
              {^gate, :run_first} -> raw_tenant_fair_claim!()
            after
              5_000 -> raise "timed out waiting to run first serialized poller"
            end
          end)
        end)

      assert_receive {^gate, :first_holds_cursor}, 2_000

      second_task =
        Task.async(fn ->
          send(parent, {gate, :second_started})
          raw_tenant_fair_claim!(repo: SecondRepo)
        end)

      assert_receive {^gate, :second_started}, 2_000
      send(first_task.pid, {gate, :run_first})

      assert {:ok, first} = Task.await(first_task, 5_000)
      second = Task.await(second_task, 5_000)

      assert first_inspection(first).cursor_before == cursor_before
      assert first_inspection(first).cursor_after == first_inspection(second).cursor_before

      expected =
        ring
        |> Stream.cycle()
        |> Enum.take(2)

      assert Enum.map([first, second], fn rows ->
               inspection = first_inspection(rows)
               {inspection.ring_position, inspection.scope_key}
             end) == expected

      call_tokens = Enum.map([first, second], &first_inspection(&1).call_token)
      run_ids = Enum.flat_map([first, second], &outcome_run_ids/1)

      assert length(Enum.uniq(call_tokens)) == 2
      assert length(run_ids) == 2
      assert length(Enum.uniq(run_ids)) == 2
      assert Enum.all?([first, second], &(first_inspection(&1).visit_ordinal == 1))

      final_cursor = first_inspection(second).cursor_after

      assert [[^final_cursor]] =
               TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy").rows
    end

    test "an actual rolled-back trace persists no evidence and is ineligible" do
      assert raw_tenant_fair_claim!() == []
      seed_work(1)

      ring = ring_snapshot()
      policy = policy_snapshot()
      cursor_before = persisted_cursor()
      epochs_before = epoch_snapshot()
      target_before = target_admissible?()

      assert {:error, rolled_back_rows} =
               TestRepo.transaction(fn ->
                 rows = raw_tenant_fair_claim!()
                 TestRepo.rollback(rows)
               end)

      assert persisted_cursor() == cursor_before
      assert epoch_snapshot() == epochs_before

      assert TestRepo.query!("SELECT count(*) FROM docket_runs WHERE claim_token IS NOT NULL").rows ==
               [[0]]

      window = %{
        calls: [%{rows: rolled_back_rows, committed: false}],
        ring_snapshots: [ring, ring_snapshot()],
        policy_snapshots: [policy, policy_snapshot()],
        epoch_snapshots: [epochs_before, epoch_snapshot()],
        target_admissible_before_calls: [target_before],
        repair_active: false,
        instrumentation_complete: true
      }

      assert_raise ArgumentError, ~r/rolled-back call/, fn ->
        FairRotationOracle.assert_database_trace!(window,
          target: "target",
          cohort: ["hot", "target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 0
        )
      end
    end

    test "an actual ring join makes the database window ineligible" do
      assert raw_tenant_fair_claim!() == []
      seed_work(1)

      ring = ring_snapshot()
      policy = policy_snapshot()
      epochs = epoch_snapshot()
      target_before_first = target_admissible?()
      first = %{rows: raw_tenant_fair_claim!(), committed: true}

      insert_ready_scope("newcomer")

      changed_ring = ring_snapshot()
      changed_epochs = epoch_snapshot()
      target_before_second = target_admissible?()
      second = %{rows: raw_tenant_fair_claim!(), committed: true}

      window = %{
        calls: [first, second],
        ring_snapshots: [ring, changed_ring, ring_snapshot()],
        policy_snapshots: [policy, policy_snapshot(), policy_snapshot()],
        epoch_snapshots: [epochs, changed_epochs, epoch_snapshot()],
        target_admissible_before_calls: [target_before_first, target_before_second],
        repair_active: false,
        instrumentation_complete: true
      }

      assert_raise ArgumentError, ~r/changed its frozen ring/, fn ->
        FairRotationOracle.assert_database_trace!(window,
          target: "target",
          cohort: ["hot", "target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 0
        )
      end
    end

    test "an actual policy change makes the database window ineligible" do
      assert raw_tenant_fair_claim!() == []
      seed_work(1)

      ring = ring_snapshot()
      policy = policy_snapshot()
      epochs = epoch_snapshot()
      target_before_first = target_admissible?()
      first = %{rows: raw_tenant_fair_claim!(), committed: true}

      TestRepo.query!("""
      UPDATE docket_claim_policy
      SET max_active = max_active + 1,
          policy_version = policy_version + 1,
          updated_at = CURRENT_TIMESTAMP
      WHERE id = 1
      """)

      changed_policy = policy_snapshot()
      epochs_after_first = epoch_snapshot()
      target_before_second = target_admissible?()
      second = %{rows: raw_tenant_fair_claim!(), committed: true}

      window = %{
        calls: [first, second],
        ring_snapshots: [ring, ring_snapshot(), ring_snapshot()],
        policy_snapshots: [policy, changed_policy, policy_snapshot()],
        epoch_snapshots: [epochs, epochs_after_first, epoch_snapshot()],
        target_admissible_before_calls: [target_before_first, target_before_second],
        repair_active: false,
        instrumentation_complete: true
      }

      assert_raise ArgumentError, ~r/changed its policy or engine/, fn ->
        FairRotationOracle.assert_database_trace!(window,
          target: "target",
          cohort: ["hot", "target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 0
        )
      end
    end

    test "a discovery-to-lock mutation is traced as stale before target progress" do
      assert raw_tenant_fair_claim!() == []
      insert_ready_scope("stale")
      insert_ready_scope("target")

      ring = ring_snapshot()
      policy = policy_snapshot()
      epochs = epoch_snapshot()
      target_before = target_admissible?()
      parent = self()
      gate = make_ref()

      barrier =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT pg_advisory_xact_lock(790079)")
            send(parent, {gate, :barrier_locked})

            receive do
              {^gate, :release} -> :ok
            after
              5_000 -> raise "timed out waiting to release stale trace barrier"
            end
          end)
        end)

      assert_receive {^gate, :barrier_locked}, 2_000

      claimant =
        Task.async(fn ->
          SecondRepo.transaction(fn ->
            SecondRepo.query!("SET LOCAL docket.trace_candidate_barrier = 'on'")
            [[backend_pid]] = SecondRepo.query!("SELECT pg_backend_pid()").rows
            send(parent, {gate, :claimant_pid, backend_pid})
            raw_tenant_fair_claim!(repo: SecondRepo)
          end)
        end)

      assert_receive {^gate, :claimant_pid, backend_pid}, 2_000
      assert wait_for_advisory_lock(backend_pid)

      TestRepo.query!(
        "UPDATE docket_runs SET wake_at = $1, updated_at = $2 WHERE scope_key = 'stale'",
        [DateTime.add(@now, 60, :second), @now]
      )

      send(barrier.pid, {gate, :release})
      assert {:ok, :ok} = Task.await(barrier, 2_000)
      assert {:ok, rows} = Task.await(claimant, 5_000)
      call = %{rows: rows, committed: true}

      assert Enum.map(
               Enum.filter(rows, &(&1.row_kind == "inspection")),
               &{&1.scope_key, &1.disposition}
             ) == [{"stale", "stale"}, {"target", "grant"}]

      result =
        FairRotationOracle.assert_database_trace!(
          %{
            calls: [call],
            ring_snapshots: [ring, ring_snapshot()],
            policy_snapshots: [policy, policy_snapshot()],
            epoch_snapshots: [epochs, epoch_snapshot()],
            target_admissible_before_calls: [target_before],
            repair_active: false,
            instrumentation_complete: true
          },
          target: "target",
          cohort: ["target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 0
        )

      assert result.observed_other_grants == 0
      assert result.observed_scan_calls == 1
    end

    test "a capped competitor is unsuccessful before the admissible target grants" do
      assert raw_tenant_fair_claim!() == []
      insert_ready_scope("capped")
      insert_claimed_run("capped", "capped-live")

      TestRepo.query!(
        "UPDATE docket_claim_partitions SET max_active = 1 WHERE scope_key = 'capped'"
      )

      insert_ready_scope("target")

      ring = ring_snapshot()
      policy = policy_snapshot()
      epochs = epoch_snapshot()
      target_before = target_admissible?()
      call = %{rows: raw_tenant_fair_claim!(), committed: true}

      assert Enum.map(
               Enum.filter(call.rows, &(&1.row_kind == "inspection")),
               &{&1.scope_key, &1.disposition}
             ) == [{"capped", "cap_debt_denial"}, {"target", "grant"}]

      result =
        FairRotationOracle.assert_database_trace!(
          %{
            calls: [call],
            ring_snapshots: [ring, ring_snapshot()],
            policy_snapshots: [policy, policy_snapshot()],
            epoch_snapshots: [epochs, epoch_snapshot()],
            target_admissible_before_calls: [target_before],
            repair_active: false,
            instrumentation_complete: true
          },
          target: "target",
          cohort: ["target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 0
        )

      assert result.observed_other_grants == 0
      assert result.observed_scan_calls == 1
    end

    test "database target witness counts admission markers instead of transient tokens" do
      assert raw_tenant_fair_claim!() == []
      insert_ready_scope("target")
      insert_claimed_run("target", "target-live")

      TestRepo.query!(
        "UPDATE docket_claim_partitions SET max_active = 1 WHERE scope_key = 'target'"
      )

      witness = target_witness()
      assert witness["admitted_count"] == 1
      refute witness["queued_promotion"]
      refute witness["eligible"]

      TestRepo.query!(
        "UPDATE docket_runs SET tenant_admitted_at = NULL WHERE run_id = 'target-live'"
      )

      witness = target_witness()
      assert witness["admitted_count"] == 0
      assert witness["queued_promotion"]
      assert witness["eligible"]
    end

    test "database target witness rejects markerless expired ordinary and poison rows" do
      assert raw_tenant_fair_claim!() == []
      insert_ready_scope("target")

      TestRepo.query!(
        """
        UPDATE docket_runs
        SET wake_at = NULL,
            claim_token = pg_catalog.gen_random_uuid(),
            claimed_at = $1,
            tenant_admitted_at = NULL,
            claim_attempts = 1
        WHERE run_id = 'run-target'
        """,
        [DateTime.add(@now, -7_200, :second)]
      )

      witness = target_witness()
      refute witness["expired"]
      refute witness["eligible"]

      TestRepo.query!("UPDATE docket_runs SET claim_attempts = 5 WHERE run_id = 'run-target'")

      witness = target_witness()
      refute witness["expired_poison"]
      refute witness["eligible"]
    end

    test "a poison grant remains bounded before the admissible target" do
      assert raw_tenant_fair_claim!() == []
      insert_ready_scope("poison")
      TestRepo.query!("UPDATE docket_runs SET claim_attempts = 5 WHERE scope_key = 'poison'")
      insert_waiting_run("poison", "poison-resumable")
      insert_ready_scope("target")

      ring = ring_snapshot()
      policy = policy_snapshot()
      epochs = epoch_snapshot()
      target_before_first = target_admissible?()
      first = %{rows: raw_tenant_fair_claim!(), committed: true}
      ring_after_first = ring_snapshot()
      policy_after_first = policy_snapshot()
      epochs_after_first = epoch_snapshot()
      target_before_second = target_admissible?()
      second = %{rows: raw_tenant_fair_claim!(), committed: true}

      assert Enum.map(
               Enum.filter(first.rows, &(&1.row_kind == "outcome")),
               &{&1.scope_key, &1.poison_reason}
             ) == [{"poison", "max_claim_attempts_exceeded"}]

      assert Enum.map(
               Enum.filter(second.rows, &(&1.row_kind == "outcome")),
               &{&1.scope_key, &1.poison_reason}
             ) == [{"target", nil}]

      result =
        FairRotationOracle.assert_database_trace!(
          %{
            calls: [first, second],
            ring_snapshots: [ring, ring_after_first, ring_snapshot()],
            policy_snapshots: [policy, policy_after_first, policy_snapshot()],
            epoch_snapshots: [epochs, epochs_after_first, epoch_snapshot()],
            target_admissible_before_calls: [target_before_first, target_before_second],
            repair_active: false,
            instrumentation_complete: true
          },
          target: "target",
          cohort: ["poison", "target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 0
        )

      assert result.observed_other_grants == 1
      assert result.observed_other_outcomes == 1
    end

    test "queue depth cannot exceed the per-call inspection and mutation budgets" do
      assert raw_tenant_fair_claim!() == []
      insert_deep_ready_scope(2_000)

      rows = raw_tenant_fair_claim!(demand: 1_000)
      inspections = Enum.filter(rows, &(&1.row_kind == "inspection"))
      outcomes = Enum.filter(rows, &(&1.row_kind == "outcome"))

      assert length(inspections) == Budgets.scan_inspections()
      assert length(outcomes) == Budgets.max_run_rows_mutated_per_scan_call()
      assert Enum.all?(inspections, &(&1.outcome_count == Budgets.grant_outcomes()))
      assert Enum.sum(Enum.map(inspections, & &1.outcome_count)) == length(outcomes)
      assert length(Enum.uniq(Enum.map(outcomes, & &1.run_id))) == length(outcomes)
    end

    test "dormant population cannot exceed the per-call scan budget" do
      assert raw_tenant_fair_claim!() == []
      insert_dormant_scopes(1_000)

      rows = raw_tenant_fair_claim!()
      inspections = Enum.filter(rows, &(&1.row_kind == "inspection"))

      assert length(inspections) == Budgets.scan_inspections()
      assert Enum.filter(rows, &(&1.row_kind == "outcome")) == []
      assert Enum.all?(inspections, &(&1.disposition == "empty_page"))

      assert length(Enum.uniq(Enum.map(inspections, & &1.scope_key))) ==
               Budgets.scan_inspections()
    end

    defp legacy_bypasses_before_target(expected_backlog) do
      context = Docket.Postgres.context(repo: TestRepo)

      Enum.reduce_while(0..expected_backlog, 0, fn _call, bypasses ->
        assert {:ok, %{leases: [lease], poisoned: []}} =
                 RunStore.claim_due(context, :system, policy())

        complete_claim(lease)

        if lease.run_id == "target" do
          {:halt, bypasses}
        else
          assert String.starts_with?(lease.run_id, "hot-")
          {:cont, bypasses + 1}
        end
      end)
    end

    defp raw_tenant_fair_claim!(opts \\ []) do
      repo = Keyword.get(opts, :repo, TestRepo)
      demand = Keyword.get(opts, :demand, 1)

      repo.query!(
        """
        SELECT claimed.*
        FROM docket_tenant_fair_claim($1, $2, $3, $4, $5, $6, true)
          AS claimed(#{RingFunction.result_definition()})
        ORDER BY claimed.visit_ordinal NULLS FIRST,
                 claimed.outcome_ordinal NULLS FIRST,
                 claimed.row_kind
        """,
        [@now, DateTime.add(@now, -3_600, :second), demand, 5, nil, 2_000]
      ).rows
      |> Enum.map(fn row -> Map.new(Enum.zip(@trace_columns, row)) end)
    end

    defp seed_work(backlog) do
      TestRepo.query!("""
      INSERT INTO docket_graph_versions
        (tenant_id, graph_id, graph_hash, graph, inserted_at)
      VALUES
        ('hot', 'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP),
        ('target', 'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP)
      ON CONFLICT (scope_key, graph_id, graph_hash) DO NOTHING
      """)

      TestRepo.query!("""
      INSERT INTO docket_claim_partitions (scope_key)
      VALUES ('hot'), ('target')
      ON CONFLICT (scope_key) DO NOTHING
      """)

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        SELECT 'hot-' || series, 'hot', 'graph', 'hash', 'running',
               decode('01', 'hex'), 1,
               $1::timestamptz - interval '2 minutes' + series * interval '1 microsecond',
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, $2) AS series
        """,
        [@now, backlog]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        VALUES
          ('target', 'target', 'graph', 'hash', 'running', decode('01', 'hex'), 1,
           $1::timestamptz - interval '1 minute',
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [@now]
      )
    end

    defp insert_expired_hot do
      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, claim_token, claimed_at, tenant_admitted_at, claim_attempts,
           inserted_at, started_at, updated_at)
        VALUES
          ('hot-expired', 'hot', 'graph', 'hash', 'running', decode('01', 'hex'), 1,
           '00000000-0000-0000-0000-000000000001'::uuid,
           $1::timestamptz - interval '2 hours',
           $1::timestamptz - interval '2 hours', 1,
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [@now]
      )
    end

    defp seed_long_ring do
      insert_ready_scope("hot")

      Enum.each(1..33, fn index ->
        insert_waiting_scope("dormant-#{String.pad_leading(Integer.to_string(index), 2, "0")}")
      end)

      insert_ready_scope("target")
    end

    defp insert_ready_scope(scope_key) do
      insert_scope(scope_key, "running", DateTime.add(@now, -60, :second))
    end

    defp insert_waiting_scope(scope_key) do
      insert_scope(scope_key, "waiting", nil)
    end

    defp insert_scope(scope_key, status, wake_at) do
      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, 'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP)
        """,
        [scope_key]
      )

      TestRepo.query!("INSERT INTO docket_claim_partitions (scope_key) VALUES ($1)", [scope_key])

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        VALUES ($1, $2, 'graph', 'hash', $3, decode('01', 'hex'), 1, $4,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        ["run-#{scope_key}", scope_key, status, wake_at]
      )
    end

    defp insert_claimed_run(scope_key, run_id) do
      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, claim_token, claimed_at, tenant_admitted_at, claim_attempts,
           inserted_at, started_at, updated_at)
        VALUES ($1, $2, 'graph', 'hash', 'running', decode('01', 'hex'), 1,
                pg_catalog.gen_random_uuid(), $3, $3, 1,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [run_id, scope_key, @now]
      )
    end

    defp insert_waiting_run(scope_key, run_id) do
      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, inserted_at, started_at, updated_at)
        VALUES ($1, $2, 'graph', 'hash', 'waiting', decode('01', 'hex'), 1,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [run_id, scope_key]
      )
    end

    defp insert_deep_ready_scope(depth) do
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
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        SELECT 'deep-' || series, 'deep', 'graph', 'hash', 'running',
               decode('01', 'hex'), 1,
               $1::timestamptz - interval '1 minute' + series * interval '1 microsecond',
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, $2) AS series
        """,
        [@now, depth]
      )
    end

    defp insert_dormant_scopes(count) do
      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        SELECT 'dormant-' || lpad(series::text, 4, '0'),
               'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP
        FROM generate_series(1, $1) AS series
        """,
        [count]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_claim_partitions (scope_key)
        SELECT 'dormant-' || lpad(series::text, 4, '0')
        FROM generate_series(1, $1) AS series
        """,
        [count]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, inserted_at, started_at, updated_at)
        SELECT 'waiting-' || series,
               'dormant-' || lpad(series::text, 4, '0'),
               'graph', 'hash', 'waiting', decode('01', 'hex'), 1,
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM generate_series(1, $1) AS series
        """,
        [count]
      )
    end

    defp first_inspection(rows) do
      Enum.find(rows, &(&1.row_kind == "inspection"))
    end

    defp outcome_run_ids(rows) do
      rows
      |> Enum.filter(&(&1.row_kind == "outcome"))
      |> Enum.map(& &1.run_id)
    end

    defp wait_for_advisory_lock(backend_pid, attempts \\ 100)

    defp wait_for_advisory_lock(_backend_pid, 0), do: false

    defp wait_for_advisory_lock(backend_pid, attempts) do
      waiting? =
        TestRepo.query!(
          "SELECT wait_event = 'advisory' FROM pg_stat_activity WHERE pid = $1",
          [backend_pid]
        ).rows == [[true]]

      if waiting? do
        true
      else
        Process.sleep(10)
        wait_for_advisory_lock(backend_pid, attempts - 1)
      end
    end

    defp persisted_cursor do
      TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy WHERE id = 1").rows
    end

    defp epoch_snapshot do
      TestRepo.query!(
        "SELECT scope_key, admission_epoch FROM docket_claim_partitions ORDER BY scope_key"
      ).rows
      |> Map.new(fn [scope_key, epoch] -> {scope_key, epoch} end)
    end

    defp complete_claim(lease) do
      TestRepo.query!(
        """
        UPDATE docket_runs
        SET status = 'done', claim_token = NULL, claimed_at = NULL,
            finished_at = $3, updated_at = $3
        WHERE run_id = $1 AND claim_token = $2
        """,
        [lease.run_id, Ecto.UUID.dump!(lease.claim_token), @now]
      )
    end

    defp ring_snapshot do
      TestRepo.query!("""
      SELECT ring_position, scope_key
      FROM docket_claim_schedule
      WHERE unfinished_count > 0
      ORDER BY ring_position
      """).rows
      |> Enum.map(&List.to_tuple/1)
    end

    defp policy_snapshot do
      [[engine, default_max_active, version]] =
        TestRepo.query!("""
        SELECT admission_mode, max_active, policy_version
        FROM docket_claim_policy
        WHERE id = 1
        """).rows

      partition_policy =
        TestRepo.query!("""
        SELECT scope_key, max_active, partition_version
        FROM docket_claim_partitions
        ORDER BY scope_key
        """).rows
        |> Map.new(fn [scope_key, max_active, partition_version] ->
          {scope_key, {max_active, partition_version}}
        end)

      [[function_hash]] =
        TestRepo.query!("""
        SELECT md5(prosrc)
        FROM pg_proc
        WHERE oid = 'docket_tenant_fair_claim(timestamp with time zone,
                                               timestamp with time zone,
                                               integer, integer, text, integer,
                                               boolean)'::regprocedure
        """).rows

      %{
        engine: if(engine == "tenant_fair", do: :tenant_fair, else: :legacy),
        default_max_active: default_max_active,
        version: version,
        partition_policy: partition_policy,
        function_hash: function_hash
      }
    end

    defp target_admissible? do
      target_witness()["eligible"]
    end

    defp target_witness do
      FairRotationTargetWitness.read!(
        TestRepo,
        "target",
        @now,
        DateTime.add(@now, -3_600, :second),
        5
      )
    end

    defp policy do
      %{
        now: @now,
        limit: 1,
        orphan_ttl_ms: 3_600_000,
        max_claim_attempts: 5,
        preference: nil
      }
    end
  end
end
