if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.FairRotationCommittedJournalTest do
    use ExUnit.Case, async: false

    @moduletag :postgres
    @moduletag :proof

    alias Docket.Postgres.ClaimPolicy.TenantFair.{Budgets, RingFunction}
    alias Docket.Postgres.TestRepo
    alias Docket.Test.ConcurrentAdmissionHarness.FairRotationOracle
    alias Docket.Test.FairRotationProofJournal

    @migration_version 20_260_719_000_279
    @prefixed_migration_version 20_260_719_000_280
    @now ~U[2026-07-19 12:00:00.000000Z]
    @cutoff DateTime.add(@now, -3_600, :second)
    @trace_columns RingFunction.result_columns() |> Keyword.keys()

    defmodule InstallDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule InstallPrefixedDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(prefix: "docket_private")
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_private")
    end

    defmodule JournalRepo do
      use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      # Freeze engine/default-cap setup before opening any proof window.
      assert raw_claim!(trace: false) == []
      FairRotationProofJournal.install!(TestRepo)
      :ok
    end

    test "committed journal proves hot-versus-target service from database evidence" do
      seed_ready_scope("hot", 100, -120)
      seed_ready_scope("target", 1, -60)
      window = open_window("target")

      first = FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)
      second = FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)

      assert outcome_scopes(first) == ["hot"]
      assert outcome_scopes(second) == ["target"]

      result = FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)

      assert result.derived_cohort == ["hot", "target"]
      assert result.observed_other_grants == 1
      assert result.observed_other_outcomes == 1
      assert result.observed_scan_calls == 2
      assert result.committed_call_sequences == [1, 2]
      assert result.full_trace_rows == 4
    end

    test "concurrent pollers are ordered only by the committed database journal" do
      config = TestRepo.config()
      Application.put_env(:docket, JournalRepo, Keyword.put(config, :pool_size, 6))
      start_supervised!(JournalRepo)

      Enum.each(["scope-a", "scope-b", "scope-c", "target"], fn scope ->
        seed_ready_scope(scope, 1, -60)
      end)

      window = open_window("target")
      parent = self()
      gate = make_ref()

      tasks =
        Enum.map(1..4, fn worker ->
          Task.async(fn ->
            send(parent, {gate, :ready, worker})

            receive do
              {^gate, :go} ->
                FairRotationProofJournal.claim!(JournalRepo, window, @now, @cutoff)
            after
              5_000 -> raise "timed out releasing concurrent proof poller"
            end
          end)
        end)

      Enum.each(1..4, fn worker -> assert_receive {^gate, :ready, ^worker}, 2_000 end)
      Enum.each(tasks, &send(&1.pid, {gate, :go}))
      Enum.each(tasks, &Task.await(&1, 5_000))

      result = FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)

      assert result.committed_call_sequences == [1, 2, 3, 4]
      assert result.derived_cohort == ["scope-a", "scope-b", "scope-c", "target"]
      assert result.observed_other_grants == 3
      assert result.observed_scan_calls == 4
    end

    test "rollback leaves no committed call evidence and cannot be relabeled as a pass" do
      seed_ready_scope("target", 1, -60)
      window = open_window("target")
      cursor_before = cursor()

      assert {:error, provisional_rows} =
               TestRepo.transaction(fn ->
                 rows = FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)
                 TestRepo.rollback(rows)
               end)

      assert outcome_scopes(provisional_rows) == ["target"]
      assert cursor() == cursor_before
      assert TestRepo.query!("SELECT count(*) FROM docket_fair_proof_calls").rows == [[0]]
      assert TestRepo.query!("SELECT count(*) FROM docket_fair_proof_rows").rows == [[0]]

      assert_raise ArgumentError, ~r/contains no qualifying calls/, fn ->
        FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)
      end

      FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)
      assert FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)
    end

    test "target inadmissible then admissible is rejected despite equal boundaries" do
      seed_ready_scope("target", 1, -60)
      window = open_window("target")

      TestRepo.query!("UPDATE docket_runs SET wake_at = $1 WHERE scope_key = 'target'", [
        DateTime.add(@now, 60, :second)
      ])

      TestRepo.query!("UPDATE docket_runs SET wake_at = $1 WHERE scope_key = 'target'", [
        DateTime.add(@now, -60, :second)
      ])

      FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)

      assert_raise ArgumentError, ~r/target admissibility changed/, fn ->
        FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)
      end
    end

    test "ring join then leave is rejected despite restoring the original ring" do
      seed_ready_scope("target", 1, -60)
      window = open_window("target")

      seed_waiting_scope("transient")

      TestRepo.query!(
        """
        UPDATE docket_runs
        SET status = 'done', finished_at = $1, updated_at = $1
        WHERE scope_key = 'transient'
        """,
        [@now]
      )

      TestRepo.query!("DELETE FROM docket_runs WHERE scope_key = 'transient'")
      TestRepo.query!("DELETE FROM docket_claim_partitions WHERE scope_key = 'transient'")

      FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)

      assert_raise ArgumentError, ~r/ring membership changed/, fn ->
        FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)
      end
    end

    test "policy change then restore is rejected despite identical snapshots" do
      seed_ready_scope("target", 1, -60)
      window = open_window("target")

      TestRepo.query!("UPDATE docket_claim_policy SET max_active = max_active + 1 WHERE id = 1")
      TestRepo.query!("UPDATE docket_claim_policy SET max_active = max_active - 1 WHERE id = 1")
      FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)

      assert_raise ArgumentError, ~r/qualification fact changed.*policy/, fn ->
        FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)
      end
    end

    test "function change then restore is rejected despite identical function identity" do
      seed_ready_scope("target", 1, -60)
      window = open_window("target")
      signature = RingFunction.identity_arguments()

      TestRepo.query!("ALTER FUNCTION docket_tenant_fair_claim(#{signature}) COST 101")
      TestRepo.query!("ALTER FUNCTION docket_tenant_fair_claim(#{signature}) COST 100")
      FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)

      assert_raise ArgumentError, ~r/qualification fact changed.*function/, fn ->
        FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)
      end
    end

    for target_form <- [:ready, :expired_debt, :ready_poison_debt, :expired_poison_debt] do
      test "database witness proves #{target_form} target admissibility" do
        target_form = unquote(target_form)
        seed_target_form(target_form)
        window = open_window("target")

        rows = FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)
        outcomes = Enum.filter(rows, &(&1.row_kind == "outcome"))
        assert length(outcomes) == 1

        case target_form do
          form when form in [:ready_poison_debt, :expired_poison_debt] ->
            assert hd(outcomes).poison_reason == "max_claim_attempts_exceeded"
            assert hd(outcomes).claim_token == nil

          _ordinary ->
            assert hd(outcomes).poison_reason == nil
            assert hd(outcomes).claim_token != nil
        end

        result = FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)
        assert result.derived_cohort == ["target"]
        assert result.observed_other_grants == 0
      end
    end

    test "deep backlog records every hidden-work counter within S, K, Q, and M" do
      seed_ready_scope("target", 2_000, -60)
      window = open_window("target")

      rows =
        FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff, demand: 1_000)

      inspections = Enum.filter(rows, &(&1.row_kind == "inspection"))
      assert length(inspections) == Budgets.scan_inspections()
      assert Enum.sum(Enum.map(inspections, & &1.exact_lock_attempt_count)) == 512
      assert Enum.sum(Enum.map(inspections, & &1.mutation_input_count)) == 256
      assert Enum.sum(Enum.map(inspections, & &1.outcome_count)) == 256
      assert Enum.all?(inspections, &(&1.ready_structural_count == 16))
      assert Enum.all?(inspections, &(&1.attempt_set_count == 16))
      assert Enum.all?(inspections, &(&1.mutation_input_count == 8))

      result = FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)
      assert result.full_trace_rows == 288
    end

    test "trace and production modes are behaviorally equivalent" do
      seed_ready_scope("hot", 3, -120)
      seed_ready_scope("target", 1, -60)

      production = raw_claim!(trace: false, demand: 2)
      production_state = durable_scheduler_state()

      reset_seeded_work!()
      seed_ready_scope("hot", 3, -120)
      seed_ready_scope("target", 1, -60)

      traced = raw_claim!(trace: true, demand: 2)
      trace_state = durable_scheduler_state()

      assert normalize_outcomes(production) == normalize_outcomes(traced)
      assert production_state == trace_state
    end

    test "tenantless scope is an ordinary qualified fairness target" do
      seed_ready_scope("", 1, -60)
      window = open_window("")
      rows = FairRotationProofJournal.claim!(TestRepo, window, @now, @cutoff)

      assert outcome_scopes(rows) == [""]
      assert Enum.find(rows, &(&1.row_kind == "outcome")).tenant_id == nil

      result = FairRotationProofJournal.verify!(TestRepo, window, lock_failures: 0)
      assert result.derived_cohort == [""]
    end

    test "qualified and search-path prefix calls share one physical fairness domain" do
      TestRepo.query!("CREATE SCHEMA docket_private")

      :ok =
        Ecto.Migrator.up(
          TestRepo,
          @prefixed_migration_version,
          InstallPrefixedDocket,
          log: false
        )

      assert raw_prefixed_claim!(false, :qualified) == []
      seed_prefixed_ready_work(100)

      ring =
        TestRepo.query!("""
        SELECT ring_position, scope_key
        FROM docket_private.docket_claim_schedule
        WHERE unfinished_count > 0
        ORDER BY ring_position
        """).rows
        |> Enum.map(&List.to_tuple/1)

      first = raw_prefixed_claim!(true, :qualified)
      second = raw_prefixed_claim!(true, :search_path)

      assert outcome_scopes(first) == ["hot"]
      assert outcome_scopes(second) == ["target"]

      events =
        [first, second]
        |> Enum.with_index(1)
        |> Enum.flat_map(fn {rows, call} ->
          rows
          |> Enum.filter(&(&1.row_kind == "inspection"))
          |> Enum.map(fn row ->
            %{
              call: call,
              ordinal: row.visit_ordinal,
              cursor_before: row.cursor_before,
              cursor_after: row.cursor_after,
              demand: row.demand,
              partition: row.scope_key,
              disposition: if(row.disposition == "grant", do: :grant, else: :empty),
              outcomes: row.outcome_count,
              epoch_delta: row.epoch_delta,
              committed: true
            }
          end)
        end)

      result =
        FairRotationOracle.assert_trace!(events,
          target: "target",
          cohort: ["hot", "target"],
          ring: ring,
          scan_budget: Budgets.scan_inspections(),
          quantum: Budgets.grant_outcomes(),
          lock_failures: 0
        )

      assert result.observed_other_grants == 1

      assert hd(Enum.filter(first, &(&1.row_kind == "inspection"))).cursor_after ==
               hd(Enum.filter(second, &(&1.row_kind == "inspection"))).cursor_before
    end

    defp open_window(target) do
      FairRotationProofJournal.open_window!(TestRepo, target, @now, @cutoff, 5)
    end

    defp seed_target_form(:ready) do
      seed_ready_scope("target", 1, -60)
    end

    defp seed_target_form(:expired_debt) do
      seed_scope_authority("target", 1)
      insert_claimed("target", "target-expired", DateTime.add(@now, -7_200, :second), 1)
      insert_claimed("target", "target-live", @now, 1)
    end

    defp seed_target_form(:ready_poison_debt) do
      seed_scope_authority("target", 1)
      insert_claimed("target", "target-live-1", @now, 1)
      insert_claimed("target", "target-live-2", @now, 1)
      insert_ready("target", "target-ready-poison", DateTime.add(@now, -60, :second), 5)

      TestRepo.query!(
        "UPDATE docket_runs SET tenant_admitted_at = $1 WHERE run_id = 'target-ready-poison'",
        [@now]
      )
    end

    defp seed_target_form(:expired_poison_debt) do
      seed_scope_authority("target", 1)
      insert_claimed("target", "target-expired-poison", DateTime.add(@now, -7_200, :second), 5)
      insert_claimed("target", "target-live", @now, 1)
    end

    defp seed_ready_scope(scope, count, seconds_offset) do
      seed_scope_authority(scope, nil)

      Enum.each(1..count, fn index ->
        run_id =
          if count == 1,
            do: "#{display_scope(scope)}-run",
            else:
              "#{display_scope(scope)}-#{String.pad_leading(Integer.to_string(index), 5, "0")}"

        insert_ready(scope, run_id, DateTime.add(@now, seconds_offset, :second), 0)
      end)
    end

    defp seed_waiting_scope(scope) do
      seed_scope_authority(scope, nil)

      TestRepo.query!(
        """
        INSERT INTO docket_runs (
          run_id, tenant_id, graph_id, graph_hash, status, state,
          checkpoint_seq, inserted_at, started_at, updated_at
        ) VALUES ($1, $2, 'graph', 'hash', 'waiting', decode('01', 'hex'),
                  1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        ["#{display_scope(scope)}-waiting", tenant_id(scope)]
      )
    end

    defp seed_scope_authority(scope, cap) do
      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, 'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP)
        ON CONFLICT (scope_key, graph_id, graph_hash) DO NOTHING
        """,
        [tenant_id(scope)]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_claim_partitions (scope_key, max_active)
        VALUES ($1, $2)
        ON CONFLICT (scope_key) DO UPDATE SET max_active = EXCLUDED.max_active
        """,
        [scope, cap]
      )
    end

    defp insert_ready(scope, run_id, wake_at, attempts) do
      TestRepo.query!(
        """
        INSERT INTO docket_runs (
          run_id, tenant_id, graph_id, graph_hash, status, state,
          checkpoint_seq, wake_at, claim_attempts,
          inserted_at, started_at, updated_at
        ) VALUES ($1, $2, 'graph', 'hash', 'running', decode('01', 'hex'),
                  1, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [run_id, tenant_id(scope), wake_at, attempts]
      )
    end

    defp insert_claimed(scope, run_id, claimed_at, attempts) do
      TestRepo.query!(
        """
        INSERT INTO docket_runs (
          run_id, tenant_id, graph_id, graph_hash, status, state,
          checkpoint_seq, claim_token, claimed_at, tenant_admitted_at, claim_attempts,
          inserted_at, started_at, updated_at
        ) VALUES ($1, $2, 'graph', 'hash', 'running', decode('01', 'hex'),
                  1, pg_catalog.gen_random_uuid(), $3, $3, $4,
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [run_id, tenant_id(scope), claimed_at, attempts]
      )
    end

    defp raw_claim!(opts) do
      trace = Keyword.fetch!(opts, :trace)
      demand = Keyword.get(opts, :demand, 1)

      TestRepo.query!(
        """
        SELECT claimed.*
        FROM docket_tenant_fair_claim($1, $2, $3, $4, $5, $6, $7)
          AS claimed(#{RingFunction.result_definition()})
        ORDER BY claimed.visit_ordinal NULLS FIRST,
                 claimed.outcome_ordinal NULLS FIRST,
                 claimed.row_kind
        """,
        [@now, @cutoff, demand, 5, nil, 2_000, trace]
      ).rows
      |> Enum.map(&Map.new(Enum.zip(@trace_columns, &1)))
    end

    defp raw_prefixed_claim!(trace, route) do
      query =
        """
        SELECT claimed.*
        FROM #{if(route == :qualified, do: "docket_private.", else: "")}docket_tenant_fair_claim(
          $1, $2, 1, 5, NULL, 2000, $3
        ) AS claimed(#{RingFunction.result_definition()})
        ORDER BY claimed.visit_ordinal NULLS FIRST,
                 claimed.outcome_ordinal NULLS FIRST,
                 claimed.row_kind
        """

      run = fn ->
        TestRepo.query!(query, [@now, @cutoff, trace]).rows
        |> Enum.map(&Map.new(Enum.zip(@trace_columns, &1)))
      end

      if route == :search_path do
        {:ok, rows} =
          TestRepo.transaction(fn ->
            TestRepo.query!("SET LOCAL search_path TO docket_private, public")
            run.()
          end)

        rows
      else
        run.()
      end
    end

    defp seed_prefixed_ready_work(backlog) do
      TestRepo.query!("""
      INSERT INTO docket_private.docket_graph_versions
        (tenant_id, graph_id, graph_hash, graph, inserted_at)
      VALUES
        ('hot', 'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP),
        ('target', 'graph', 'hash', decode('01', 'hex'), CURRENT_TIMESTAMP)
      """)

      TestRepo.query!("""
      INSERT INTO docket_private.docket_claim_partitions (scope_key)
      VALUES ('hot'), ('target')
      """)

      TestRepo.query!(
        """
        INSERT INTO docket_private.docket_runs (
          run_id, tenant_id, graph_id, graph_hash, status, state,
          checkpoint_seq, wake_at, inserted_at, started_at, updated_at
        )
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
        INSERT INTO docket_private.docket_runs (
          run_id, tenant_id, graph_id, graph_hash, status, state,
          checkpoint_seq, wake_at, inserted_at, started_at, updated_at
        ) VALUES (
          'target-run', 'target', 'graph', 'hash', 'running', decode('01', 'hex'),
          1, $1::timestamptz - interval '1 minute',
          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        """,
        [@now]
      )
    end

    defp reset_seeded_work! do
      TestRepo.query!("DELETE FROM docket_runs")
      TestRepo.query!("UPDATE docket_claim_partitions SET admission_epoch = 0")
      TestRepo.query!("UPDATE docket_claim_policy SET scan_ring_position = 0 WHERE id = 1")
    end

    defp durable_scheduler_state do
      runs =
        TestRepo.query!("""
        SELECT run_id, scope_key, status, claim_token IS NOT NULL,
               claimed_at, claim_attempts, poisoned_at, poison_reason, wake_at
        FROM docket_runs
        ORDER BY run_id
        """).rows

      cursor_and_epochs =
        TestRepo.query!("""
        SELECT policy.scan_ring_position,
               COALESCE(jsonb_object_agg(partition.scope_key, partition.admission_epoch), '{}'::jsonb)::text
        FROM docket_claim_policy AS policy
        CROSS JOIN docket_claim_partitions AS partition
        WHERE policy.id = 1
        GROUP BY policy.scan_ring_position
        """).rows

      {Enum.map(runs, &normalize_run_state/1), cursor_and_epochs}
    end

    defp normalize_run_state([
           run_id,
           scope,
           status,
           claimed?,
           claimed_at,
           attempts,
           poisoned_at,
           poison_reason,
           wake_at
         ]) do
      {run_id, scope, status, claimed?, claimed_at, attempts, poisoned_at != nil, poison_reason,
       wake_at}
    end

    defp normalize_outcomes(rows) do
      rows
      |> Enum.filter(&(&1.row_kind == "outcome"))
      |> Enum.map(fn row ->
        {row.run_id, row.tenant_id, row.work_class, row.claim_token != nil, row.poison_reason,
         row.claim_attempt}
      end)
    end

    defp cursor do
      [[cursor]] = TestRepo.query!("SELECT scan_ring_position FROM docket_claim_policy").rows
      cursor
    end

    defp outcome_scopes(rows) do
      rows
      |> Enum.filter(&(&1.row_kind == "outcome"))
      |> Enum.map(& &1.scope_key)
    end

    defp tenant_id(""), do: nil
    defp tenant_id(scope), do: scope
    defp display_scope(""), do: "tenantless"
    defp display_scope(scope), do: scope
  end
end
