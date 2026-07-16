if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyAdminTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.Admin
    alias Docket.Postgres.ClaimPolicyAdminTestRepo, as: TestRepo

    @migration_version 20_260_716_000_066
    @private_migration_version 20_260_716_000_067
    @policy %{preferred_active: 2, max_active: 4, weight: 1, borrowing: false}

    defmodule InstallDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule InstallPrivateDocket do
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

      :ok =
        Ecto.Migrator.up(TestRepo, @private_migration_version, InstallPrivateDocket, log: false)

      %{context: Docket.Postgres.context(repo: TestRepo)}
    end

    test "bootstrap is explicit, one-time, versioned, audited, and replayable", %{context: ctx} do
      assert {:error, :not_initialized} = Admin.get_default(ctx)
      assert {:error, :not_initialized} = Admin.get_effective(ctx, :tenantless)

      assert {:ok,
              %{
                outcome: :applied,
                target: :default,
                previous_version: 0,
                version: 1,
                audit_id: audit_id
              }} = Admin.bootstrap_default(ctx, @policy, cas("bootstrap", 0))

      assert {:ok, %{version: 1, max_active: 4}} = Admin.get_default(ctx)

      assert {:ok, %{outcome: :replayed, original: %{audit_id: ^audit_id, version: 1}}} =
               Admin.bootstrap_default(ctx, @policy, cas("bootstrap", 0, actor: "retry-host"))

      assert {:error, {:event_conflict, %{source: "test", event_id: "bootstrap"}}} =
               Admin.bootstrap_default(
                 ctx,
                 %{@policy | max_active: 5},
                 cas("bootstrap", 0)
               )

      assert {:error, {:already_initialized, 1}} =
               Admin.bootstrap_default(ctx, @policy, cas("other-bootstrap", 0))

      assert gate_state() == [["not_ready", "legacy", 0, 0]]
    end

    test "default CAS increments on same values, rejects stale versions, and replays after change",
         %{
           context: ctx
         } do
      bootstrap!(ctx)

      assert {:ok, %{version: 2, previous_version: 1, audit_id: first_audit}} =
               Admin.put_default(ctx, @policy, cas("default-same", 1))

      downgraded = %{@policy | preferred_active: 0, max_active: 0}

      assert {:ok, %{version: 3}} =
               Admin.put_default(ctx, downgraded, cas("default-down", 2))

      assert {:ok, %{outcome: :replayed, original: %{audit_id: ^first_audit, version: 2}}} =
               Admin.put_default(ctx, @policy, cas("default-same", 1, actor: "new-actor"))

      assert {:error, {:version_conflict, %{target: :default, expected: 1, actual: 3}}} =
               Admin.put_default(ctx, @policy, cas("default-stale", 1))
    end

    test "virtual partitions materialize at version one and reset preserves identity and state",
         %{
           context: ctx
         } do
      bootstrap!(ctx)

      override = %{preferred_active: 1, max_active: 2, weight: 7, borrowing: true}

      assert {:ok, %{target: {:tenant, "tenant-a"}, previous_version: 0, version: 1}} =
               Admin.put_override(ctx, {:tenant, "tenant-a"}, override, cas("override", 0))

      assert {:ok,
              %{
                policy_source: :override,
                max_active: 2,
                weight: 7,
                partition_present: true,
                partition_version: 1,
                state: :running
              }} = Admin.get_effective(ctx, {:tenant, "tenant-a"})

      assert {:ok, %{version: 2}} =
               Admin.put_state(ctx, {:tenant, "tenant-a"}, :drain, cas("drain", 1))

      assert {:ok, %{version: 3}} =
               Admin.reset_override(ctx, {:tenant, "tenant-a"}, cas("reset", 2))

      assert {:ok,
              %{
                policy_source: :default,
                max_active: 4,
                partition_present: true,
                partition_version: 3,
                state: :drain
              }} = Admin.get_effective(ctx, {:tenant, "tenant-a"})

      assert partition_row("tenant-a") == [["tenant-a", nil, nil, nil, nil, "drain", 3, 0]]
    end

    test "first reset and running-state CAS materialize canonical tenantless authority", %{
      context: ctx
    } do
      bootstrap!(ctx)

      assert {:ok, %{target: :tenantless, previous_version: 0, version: 1}} =
               Admin.reset_override(ctx, :tenantless, cas("tenantless-reset", 0))

      assert {:ok, %{partition_present: true, partition_version: 1, state: :running}} =
               Admin.get_effective(ctx, :tenantless)

      assert {:ok, %{previous_version: 0, version: 1}} =
               Admin.put_state(ctx, {:tenant, "fresh"}, :running, cas("fresh-running", 0))
    end

    test "bulk changes sort by binary key, reject duplicates, and roll back every conflict", %{
      context: ctx
    } do
      bootstrap!(ctx)

      changes = [
        %{owner_scope: {:tenant, "z"}, expected_version: 0, operation: {:put_state, :hold_new}},
        %{owner_scope: :tenantless, expected_version: 0, operation: :reset_override},
        %{owner_scope: {:tenant, "a"}, expected_version: 0, operation: {:put_state, :drain}}
      ]

      assert {:ok, %{target: [:tenantless, {:tenant, "a"}, {:tenant, "z"}], version: versions}} =
               Admin.apply_partition_changes(ctx, changes, event("bulk"))

      assert Enum.map(versions, & &1.version) == [1, 1, 1]

      conflicting = [
        %{owner_scope: {:tenant, "a"}, expected_version: 0, operation: :reset_override},
        %{owner_scope: {:tenant, "new"}, expected_version: 0, operation: :reset_override}
      ]

      assert {:error,
              {:version_conflict,
               %{conflicts: [%{target: {:tenant, "a"}, expected: 0, actual: 1}]}}} =
               Admin.apply_partition_changes(ctx, conflicting, event("bulk-conflict"))

      assert partition_row("new") == []

      duplicate = [
        %{owner_scope: {:tenant, "a"}, expected_version: 1, operation: :reset_override},
        %{owner_scope: {:tenant, "a"}, expected_version: 1, operation: {:put_state, :running}}
      ]

      assert {:error, :duplicate_partition_target} =
               Admin.apply_partition_changes(ctx, duplicate, event("duplicate"))
    end

    test "effective reads report inherited state, live debt, and gate facts coherently", %{
      context: ctx
    } do
      bootstrap!(ctx)

      assert {:ok,
              %{
                policy_source: :default,
                partition_present: false,
                partition_version: 0,
                state: :running,
                live_count: 0,
                debt: 0,
                readiness: :not_ready,
                mode: :legacy,
                mode_epoch: 0
              }} = Admin.get_effective(ctx, {:tenant, "absent"})

      assert {:ok, state} = Admin.get_prefix_state(ctx)
      assert state.schema_generation == 2
      assert state.backfill_target_id == nil
      assert state.backfill_cursor == nil
      assert state.backfill_batches == 0
      assert state.backfill_rows == 0
      assert state.backfill_retries == 0
      assert state.backfill_phase == :not_started
      assert state.default_version == 1
      assert state.default == @policy
      assert state.readiness == :not_ready
      assert state.mode == :legacy
      assert state.dormant_partition_count == 0
    end

    test "audit pages decode complete before/after values and receipts outlive pruning", %{
      context: ctx
    } do
      bootstrap!(ctx)

      assert {:ok, %{audit_id: changed_audit}} =
               Admin.put_default(ctx, %{@policy | max_active: 5}, cas("change", 1))

      assert {:ok, %{events: [first], has_more: true, next_after_audit_id: first_id}} =
               Admin.list_events(ctx, limit: 1)

      assert first.audit_id == first_id
      assert first.before_value.policy_version == 0
      assert first.after_value.max_active == 4

      assert {:ok, %{events: [second], has_more: false}} =
               Admin.list_events(ctx, after_audit_id: first_id, limit: 10)

      assert second.audit_id == changed_audit
      assert second.before_versions == [1]
      assert second.after_versions == [2]

      assert {:ok, %{through_audit_id: ^changed_audit}} =
               Admin.export_events(ctx,
                 through_audit_id: changed_audit,
                 location_fingerprint: :crypto.hash(:sha256, "external-object"),
                 source: "test",
                 event_id: "export",
                 actor: "operator"
               )

      assert {:ok, %{deleted_count: 2}} =
               Admin.prune_events(ctx,
                 cutoff: DateTime.add(DateTime.utc_now(), 60, :second),
                 limit: 500,
                 source: "test",
                 event_id: "prune",
                 actor: "operator"
               )

      assert {:ok, %{outcome: :replayed, original: %{audit_id: ^changed_audit}}} =
               Admin.put_default(ctx, %{@policy | max_active: 5}, cas("change", 1))

      assert receipt_count() == 4
      assert {:ok, %{max_active: 5, version: 2}} = Admin.get_default(ctx)
    end

    test "legal holds union-protect exported audit while pruning cannot alter live policy", %{
      context: ctx
    } do
      %{audit_id: bootstrap_audit} = bootstrap!(ctx)

      assert {:ok, %{audit_id: second_audit}} =
               Admin.put_default(ctx, %{@policy | max_active: 6}, cas("held-change", 1))

      assert {:ok, %{hold_id: hold_id}} =
               Admin.put_legal_hold(ctx,
                 first_audit_id: bootstrap_audit,
                 last_audit_id: bootstrap_audit,
                 reason: "investigation",
                 source: "test",
                 event_id: "hold",
                 actor: "legal"
               )

      assert {:ok, %{through_audit_id: through}} =
               Admin.export_events(ctx,
                 through_audit_id: second_audit,
                 location_fingerprint: :crypto.hash(:sha256, "held-export"),
                 source: "test",
                 event_id: "held-export",
                 actor: "operator"
               )

      assert through == second_audit

      assert {:ok, %{deleted_count: 1, last_deleted_audit_id: ^second_audit}} =
               Admin.prune_events(ctx,
                 cutoff: DateTime.add(DateTime.utc_now(), 60, :second),
                 source: "test",
                 event_id: "held-prune",
                 actor: "operator"
               )

      assert audit_ids() |> Enum.member?(bootstrap_audit)
      assert {:ok, %{max_active: 6, version: 2}} = Admin.get_default(ctx)

      assert {:ok, %{hold_id: ^hold_id}} =
               Admin.delete_legal_hold(ctx, hold_id, event("delete-hold"))
    end

    test "invalid contexts and transaction-scoped mutators issue no Admin SQL", %{context: ctx} do
      assert {:error, :invalid_admin_context} =
               Admin.bootstrap_default(TestRepo, @policy, cas("bare", 0))

      assert {:error, :invalid_admin_context} =
               Admin.bootstrap_default(
                 %{repo: TestRepo, prefix: "public"},
                 @policy,
                 cas("raw", 0)
               )

      assert {:error, :transaction_context_forbidden} =
               Docket.Postgres.Storage.transaction(ctx, fn tx ->
                 Admin.bootstrap_default(tx, @policy, cas("outer", 0))
               end)

      assert {:error, :not_initialized} = Admin.get_default(ctx)
    end

    test "custom prefixes are isolated and tenantless targets remain empty-string authority", %{
      context: public
    } do
      private = Docket.Postgres.context(repo: TestRepo, prefix: "docket_private")
      bootstrap!(private)

      assert {:ok, %{version: 1}} =
               Admin.put_state(private, :tenantless, :hold_new, cas("private-state", 0))

      assert {:ok, %{state: :hold_new, partition_present: true}} =
               Admin.get_effective(private, :tenantless)

      assert {:error, :not_initialized} = Admin.get_default(public)
      assert partition_row("", "docket_private") != []
      assert partition_row("") == []
    end

    test "default lock timeout is bounded and reports the exact authority", %{context: ctx} do
      bootstrap!(ctx)
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT id FROM docket_claim_policy WHERE id = 1 FOR UPDATE")
            send(parent, :default_locked)

            receive do
              :release_default -> :ok
            end
          end)
        end)

      assert_receive :default_locked

      started = System.monotonic_time(:millisecond)

      assert {:error, {:lock_timeout, :default}} =
               Admin.put_default(ctx, %{@policy | max_active: 5}, cas("blocked", 1))

      assert System.monotonic_time(:millisecond) - started < 2_500
      send(holder.pid, :release_default)
      Task.await(holder)
      assert {:ok, %{version: 1}} = Admin.get_default(ctx)
    end

    test "admission default share lock serializes a cap downgrade before its version commit", %{
      context: ctx
    } do
      bootstrap!(ctx)
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            [[backend_pid, 2, 4, 1]] =
              TestRepo.query!("""
              SELECT pg_backend_pid(), preferred_active, max_active, policy_version
              FROM docket_claim_policy
              WHERE id = 1
              FOR SHARE
              """).rows

            send(parent, {:admission_default_shared, self(), backend_pid})

            receive do
              :release_admission_default -> :ok
            end
          end)
        end)

      assert_receive {:admission_default_shared, holder_task, holder_backend}
      assert holder_task == holder.pid

      downgrade =
        Task.async(fn ->
          Admin.put_default(ctx, %{@policy | max_active: 2}, cas("serialized-downgrade", 1))
        end)

      wait_until(fn -> default_update_waiter_pids() != [] end)
      [waiting_backend] = default_update_waiter_pids()
      refute waiting_backend == holder_backend
      assert Task.yield(downgrade, 0) == nil

      send(holder.pid, :release_admission_default)
      assert {:ok, _} = Task.await(holder)

      assert {:ok, %{previous_version: 1, version: 2}} = Task.await(downgrade)

      reader = Task.async(fn -> Admin.get_default(ctx) end)
      assert {:ok, %{max_active: 2, version: 2}} = Task.await(reader)
    end

    test "post-update receipt fault rolls a waited downgrade back behind admission share lock", %{
      context: ctx
    } do
      bootstrap!(ctx)
      baseline = {audit_ids(), receipt_count()}

      TestRepo.query!("""
      CREATE FUNCTION fail_scheduled_admin_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE
        observed_cap integer;
        observed_version bigint;
      BEGIN
        SELECT max_active, policy_version
          INTO observed_cap, observed_version
        FROM docket_claim_policy
        WHERE id = 1;

        IF observed_cap <> 2 OR observed_version <> 2 THEN
          RAISE EXCEPTION 'receipt ran before expected default update: cap %, version %',
            observed_cap, observed_version;
        END IF;

        RAISE EXCEPTION 'scheduled receipt fault after default update';
      END
      $$
      """)

      TestRepo.query!("""
      CREATE TRIGGER fail_scheduled_admin_receipt
      BEFORE INSERT ON docket_claim_policy_receipts
      FOR EACH ROW EXECUTE FUNCTION fail_scheduled_admin_receipt()
      """)

      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            [[backend_pid, 2, 4, 1]] =
              TestRepo.query!("""
              SELECT pg_backend_pid(), preferred_active, max_active, policy_version
              FROM docket_claim_policy
              WHERE id = 1
              FOR SHARE
              """).rows

            send(parent, {:rollback_default_shared, self(), backend_pid})

            receive do
              :release_rollback_default -> :ok
            end
          end)
        end)

      assert_receive {:rollback_default_shared, holder_task, holder_backend}
      assert holder_task == holder.pid

      downgrade =
        Task.async(fn ->
          Admin.put_default(ctx, %{@policy | max_active: 2}, cas("rolled-back-downgrade", 1))
        end)

      wait_until(fn -> default_update_waiter_pids() != [] end)
      [waiting_backend] = default_update_waiter_pids()
      refute waiting_backend == holder_backend
      assert Task.yield(downgrade, 0) == nil

      send(holder.pid, :release_rollback_default)
      assert {:ok, _} = Task.await(holder)
      assert {:error, :invalid_admin_context} = Task.await(downgrade)

      TestRepo.query!("DROP TRIGGER fail_scheduled_admin_receipt ON docket_claim_policy_receipts")

      TestRepo.query!("DROP FUNCTION fail_scheduled_admin_receipt()")

      assert {:ok, %{max_active: 4, version: 1}} = Admin.get_default(ctx)
      assert {audit_ids(), receipt_count()} == baseline
      refute "rolled-back-downgrade" in audit_event_ids()
    end

    test "a receipt failure rolls back policy and audit before a clean retry", %{context: ctx} do
      bootstrap!(ctx)

      TestRepo.query!("""
      CREATE FUNCTION fail_admin_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'receipt fault';
      END
      $$
      """)

      TestRepo.query!("""
      CREATE TRIGGER fail_admin_receipt
      BEFORE INSERT ON docket_claim_policy_receipts
      FOR EACH ROW EXECUTE FUNCTION fail_admin_receipt()
      """)

      assert {:error, :invalid_admin_context} =
               Admin.put_default(ctx, %{@policy | max_active: 8}, cas("faulted", 1))

      assert {:ok, %{version: 1, max_active: 4}} = Admin.get_default(ctx)
      refute "faulted" in audit_event_ids()

      TestRepo.query!("DROP TRIGGER fail_admin_receipt ON docket_claim_policy_receipts")
      TestRepo.query!("DROP FUNCTION fail_admin_receipt()")

      assert {:ok, %{version: 2, max_active: 8}} =
               Admin.put_default(ctx, %{@policy | max_active: 8}, cas("faulted", 1))
               |> then(fn {:ok, result} -> {:ok, Map.merge(result, %{max_active: 8})} end)
    end

    test "concurrent identical bootstrap linearizes to applied plus replay", %{context: ctx} do
      parent = self()

      tasks =
        for _ <- 1..2 do
          Task.async(fn ->
            send(parent, {:ready, self()})

            receive do
              :go -> Admin.bootstrap_default(ctx, @policy, cas("racing-bootstrap", 0))
            end
          end)
        end

      pids =
        for _ <- tasks do
          assert_receive {:ready, pid}
          pid
        end

      Enum.each(pids, &send(&1, :go))
      results = Enum.map(tasks, &Task.await(&1, 5_000))

      assert Enum.count(results, &match?({:ok, %{outcome: :applied}}, &1)) == 1
      assert Enum.count(results, &match?({:ok, %{outcome: :replayed}}, &1)) == 1
      assert receipt_count() == 1
    end

    test "bundle provenance rejects arbitrary identities, swapped bindings, and replacement policies",
         %{
           context: ctx
         } do
      refute function_exported?(Docket.Postgres.ClaimPolicy, :bind_admin, 3)
      refute function_exported?(Docket.Postgres, :valid_admin_identity?, 4)

      assert :missing ==
               :persistent_term.get(
                 {Docket.Postgres, :admin_provenance_secret},
                 :missing
               )

      arbitrary_identity = make_ref()

      arbitrary_policy = %{
        ctx.claim_policy
        | admin_repo: TestRepo,
          admin_identity: arbitrary_identity
      }

      assert {:error, :invalid_admin_context} =
               Admin.get_default(%{
                 ctx
                 | postgres_admin_identity: arbitrary_identity,
                   claim_policy: arbitrary_policy
               })

      swapped = %{ctx | repo: Docket.Postgres.TestRepo}
      assert {:error, :invalid_admin_context} = Admin.get_default(swapped)

      assert {:error, :invalid_admin_context} =
               Admin.get_default(%{ctx | prefix: "docket_private"})

      replacement =
        Docket.Postgres.ClaimPolicy.new([], %{
          repo: TestRepo,
          prefix: "public",
          postgres_admin_identity: ctx.postgres_admin_identity
        })

      assert replacement.admin_identity == nil
      assert replacement.admin_repo == nil

      assert {:error, :invalid_admin_context} =
               Admin.get_default(%{ctx | claim_policy: replacement})
    end

    test "root contexts are rejected inside raw Repo transactions before mutation", %{
      context: ctx
    } do
      assert {:ok, :checked} =
               TestRepo.transaction(fn ->
                 assert {:error, :transaction_context_forbidden} =
                          Admin.bootstrap_default(ctx, @policy, cas("raw-outer", 0))

                 :checked
               end)

      assert {:error, :not_initialized} = Admin.get_default(ctx)
    end

    test "transaction-scoped reads preserve caller timeout and do not doom outer writes", %{
      context: ctx
    } do
      assert {:ok, :committed} =
               Docket.Postgres.Storage.transaction(ctx, fn tx ->
                 assert tx.postgres_admin_identity == ctx.postgres_admin_identity
                 assert tx.claim_policy == ctx.claim_policy
                 assert tx.repo == ctx.repo
                 assert tx.prefix == ctx.prefix
                 TestRepo.query!("SET LOCAL statement_timeout = '100ms'")
                 TestRepo.query!("SET LOCAL lock_timeout = '75ms'")

                 assert {:error, :not_initialized} = Admin.get_default(tx)

                 TestRepo.query!(
                   "INSERT INTO docket_claim_partitions (scope_key) VALUES ('outer-kept')"
                 )

                 assert settings() == [["100ms", "75ms"]]
                 {:ok, :committed}
               end)

      assert partition_row("outer-kept") != []

      bootstrap!(ctx)

      assert {:ok, :read} =
               Docket.Postgres.Storage.transaction(ctx, fn tx ->
                 TestRepo.query!("SET LOCAL statement_timeout = '100ms'")
                 assert {:ok, %{version: 1}} = Admin.get_default(tx)
                 assert settings() == [["100ms", "0"]]
                 {:ok, :read}
               end)
    end

    test "raw Repo transaction reads use savepoints for ordinary results and preserve commits", %{
      context: ctx
    } do
      assert {:ok, :not_initialized_committed} =
               TestRepo.transaction(fn ->
                 TestRepo.query!("SET LOCAL statement_timeout = '100ms'")
                 TestRepo.query!("SET LOCAL lock_timeout = '75ms'")

                 assert {:error, :not_initialized} = Admin.get_default(ctx)
                 assert [[1]] = TestRepo.query!("SELECT 1").rows
                 assert settings() == [["100ms", "75ms"]]
                 lifecycle_insert("raw-read-not-initialized")
                 :not_initialized_committed
               end)

      assert partition_row("raw-read-not-initialized") != []
      bootstrap!(ctx)

      assert {:ok, :success_committed} =
               TestRepo.transaction(fn ->
                 TestRepo.query!("SET LOCAL statement_timeout = '100ms'")
                 assert {:ok, %{version: 1}} = Admin.get_default(ctx)
                 assert [[1]] = TestRepo.query!("SELECT 1").rows
                 assert settings() == [["100ms", "0"]]
                 lifecycle_insert("raw-read-success")
                 :success_committed
               end)

      assert partition_row("raw-read-success") != []
    end

    test "gate, rollout, and partition contention report exact bounded authorities", %{
      context: ctx
    } do
      bootstrap!(ctx)

      assert_lock_error(
        "SELECT id FROM docket_claim_admission_gate WHERE id = 1 FOR UPDATE",
        {:lock_timeout, :gate},
        fn -> Admin.put_default(ctx, %{@policy | max_active: 5}, cas("gate-lock", 1)) end
      )

      assert_lock_error(
        "SELECT id FROM docket_claim_rollout WHERE id = 1 FOR UPDATE",
        {:lock_timeout, :rollout},
        fn -> Admin.put_default(ctx, %{@policy | max_active: 5}, cas("rollout-lock", 1)) end
      )

      assert {:ok, %{version: 1}} =
               Admin.put_state(ctx, {:tenant, "locked"}, :running, cas("locked-row", 0))

      assert_lock_error(
        "SELECT scope_key FROM docket_claim_partitions WHERE scope_key = 'locked' FOR UPDATE",
        {:lock_timeout, {:partition, {:tenant, "locked"}}},
        fn -> Admin.put_state(ctx, {:tenant, "locked"}, :drain, cas("partition-lock", 1)) end
      )
    end

    test "lifecycle and Admin first-writer schedules preserve the winning control row", %{
      context: ctx
    } do
      bootstrap!(ctx)

      lifecycle_insert("lifecycle-first")

      assert {:ok, %{version: 1}} =
               Admin.put_state(
                 ctx,
                 {:tenant, "lifecycle-first"},
                 :drain,
                 cas("lifecycle-first-admin", 0)
               )

      assert {:ok, %{version: 1}} =
               Admin.put_state(
                 ctx,
                 {:tenant, "admin-first"},
                 :hold_new,
                 cas("admin-first", 0)
               )

      lifecycle_insert("admin-first")

      assert partition_row("admin-first") == [
               ["admin-first", nil, nil, nil, nil, "hold_new", 1, 0]
             ]

      parent = self()

      lifecycle =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            lifecycle_insert("uniqueness-race")
            send(parent, :lifecycle_inserted)

            receive do
              :commit_lifecycle -> :ok
            end
          end)
        end)

      assert_receive :lifecycle_inserted

      admin =
        Task.async(fn ->
          Admin.put_state(
            ctx,
            {:tenant, "uniqueness-race"},
            :drain,
            cas("uniqueness-admin", 0)
          )
        end)

      assert Task.yield(admin, 50) == nil
      send(lifecycle.pid, :commit_lifecycle)
      assert {:ok, _} = Task.await(lifecycle)
      assert {:ok, %{version: 1}} = Task.await(admin)
    end

    test "inverse bulk input order cannot deadlock and conflicting event reuse changes one target only",
         %{
           context: ctx
         } do
      bootstrap!(ctx)

      forward = [
        %{owner_scope: {:tenant, "a"}, expected_version: 0, operation: :reset_override},
        %{owner_scope: {:tenant, "b"}, expected_version: 0, operation: :reset_override}
      ]

      reverse = Enum.reverse(forward)
      parent = self()

      tasks =
        [
          {forward, event("forward")},
          {reverse, event("reverse")}
        ]
        |> Enum.map(fn {changes, opts} ->
          Task.async(fn ->
            send(parent, {:bulk_ready, self()})
            receive do: (:go -> Admin.apply_partition_changes(ctx, changes, opts))
          end)
        end)

      pids =
        for _ <- tasks,
            do:
              (
                assert_receive {:bulk_ready, pid}
                pid
              )

      Enum.each(pids, &send(&1, :go))
      results = Enum.map(tasks, &Task.await(&1, 5_000))
      assert Enum.count(results, &match?({:ok, _}, &1)) == 1
      assert Enum.count(results, &match?({:error, {:version_conflict, _}}, &1)) == 1

      same_event = event("cross-target-event")

      first =
        Task.async(fn ->
          Admin.put_state(ctx, {:tenant, "c"}, :drain, same_event ++ [expected_version: 0])
        end)

      second =
        Task.async(fn ->
          Admin.put_state(ctx, {:tenant, "d"}, :drain, same_event ++ [expected_version: 0])
        end)

      race = [Task.await(first), Task.await(second)]
      assert Enum.count(race, &match?({:ok, _}, &1)) == 1
      assert Enum.count(race, &match?({:error, {:event_conflict, _}}, &1)) == 1
      assert length(Enum.filter([partition_row("c"), partition_row("d")], &(&1 != []))) == 1
    end

    test "bounded audit validation errors never write audit, receipt, or live state", %{
      context: ctx
    } do
      %{audit_id: bootstrap_audit} = bootstrap!(ctx)
      baseline = {audit_ids(), receipt_count(), Admin.get_default(ctx)}

      assert {:error, :invalid_export_watermark} =
               Admin.export_events(ctx,
                 through_audit_id: bootstrap_audit + 100,
                 location_fingerprint: :crypto.hash(:sha256, "ahead"),
                 source: "test",
                 event_id: "ahead",
                 actor: "operator"
               )

      assert {:error, :invalid_audit_range} =
               Admin.put_legal_hold(ctx,
                 first_audit_id: bootstrap_audit,
                 last_audit_id: bootstrap_audit + 1,
                 reason: "future",
                 source: "test",
                 event_id: "future-hold",
                 actor: "legal"
               )

      assert {:error, :audit_export_required} =
               Admin.prune_events(ctx,
                 cutoff: DateTime.utc_now(),
                 source: "test",
                 event_id: "no-export-prune",
                 actor: "operator"
               )

      assert {:error, :invalid_hold_id} = Admin.delete_legal_hold(ctx, "bad", event("bad-hold"))

      assert {:error, :invalid_audit_options} =
               Admin.put_legal_hold(ctx, :not_options)

      assert {:error, :invalid_audit_options} = Admin.prune_events(ctx, [1])
      assert {audit_ids(), receipt_count(), Admin.get_default(ctx)} == baseline
    end

    test "rollout barrier makes disjoint audit allocation follow commit order", %{context: ctx} do
      bootstrap!(ctx)

      TestRepo.query!("""
      CREATE FUNCTION block_slow_admin_audit() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.event_id = 'slow-audit' THEN
          PERFORM pg_advisory_xact_lock(660066);
        END IF;
        RETURN NEW;
      END
      $$
      """)

      TestRepo.query!("""
      CREATE TRIGGER block_slow_admin_audit
      BEFORE INSERT ON docket_claim_policy_events
      FOR EACH ROW EXECUTE FUNCTION block_slow_admin_audit()
      """)

      parent = self()

      advisory_holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT pg_advisory_xact_lock(660066)")
            send(parent, :advisory_held)
            receive do: (:release_advisory -> :ok)
          end)
        end)

      assert_receive :advisory_held

      slow =
        Task.async(fn ->
          Admin.put_state(ctx, {:tenant, "slow"}, :drain, cas("slow-audit", 0))
        end)

      wait_until(fn -> advisory_waiter_count() == 1 end)

      fast =
        Task.async(fn ->
          Admin.put_state(ctx, {:tenant, "fast"}, :drain, cas("fast-audit", 0))
        end)

      assert Task.yield(fast, 100) == nil
      send(advisory_holder.pid, :release_advisory)
      assert {:ok, _} = Task.await(advisory_holder)
      assert {:ok, %{audit_id: slow_id}} = Task.await(slow)
      assert {:ok, %{audit_id: fast_id}} = Task.await(fast)
      assert slow_id < fast_id
      assert audit_event_order(["slow-audit", "fast-audit"]) == ["slow-audit", "fast-audit"]
    end

    test "timed-out transaction-scoped inspection preserves outer transaction", %{context: ctx} do
      bootstrap!(ctx)
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("LOCK TABLE docket_claim_policy_events IN ACCESS EXCLUSIVE MODE")
            send(parent, :audit_table_locked)
            receive do: (:release_audit_table -> :ok)
          end)
        end)

      assert_receive :audit_table_locked

      assert {:ok, :outer_committed} =
               Docket.Postgres.Storage.transaction(ctx, fn tx ->
                 TestRepo.query!("SET LOCAL statement_timeout = '50ms'")
                 assert {:error, :admin_timeout} = Admin.list_events(tx, limit: 10)
                 assert settings() == [["50ms", "0"]]
                 lifecycle_insert("after-read-timeout")
                 {:ok, :outer_committed}
               end)

      send(holder.pid, :release_audit_table)
      assert {:ok, _} = Task.await(holder)
      assert partition_row("after-read-timeout") != []
    end

    test "timed-out inspection in a raw Repo transaction preserves outer transaction", %{
      context: ctx
    } do
      bootstrap!(ctx)
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("LOCK TABLE docket_claim_policy_events IN ACCESS EXCLUSIVE MODE")
            send(parent, :raw_audit_table_locked)
            receive do: (:release_raw_audit_table -> :ok)
          end)
        end)

      assert_receive :raw_audit_table_locked

      assert {:ok, :outer_committed} =
               TestRepo.transaction(fn ->
                 TestRepo.query!("SET LOCAL statement_timeout = '50ms'")
                 assert {:error, :admin_timeout} = Admin.list_events(ctx, limit: 10)
                 assert [[1]] = TestRepo.query!("SELECT 1").rows
                 assert settings() == [["50ms", "0"]]
                 lifecycle_insert("after-raw-read-timeout")
                 :outer_committed
               end)

      send(holder.pid, :release_raw_audit_table)
      assert {:ok, _} = Task.await(holder)
      assert partition_row("after-raw-read-timeout") != []
    end

    test "exact validation bounds reject NUL, overflow, malformed options, and oversized policy",
         %{
           context: ctx
         } do
      baseline = {audit_ids(), receipt_count()}

      assert {:error, :invalid_admin_options} =
               Admin.bootstrap_default(ctx, @policy, cas("nul\0event", 0))

      assert {:error, :invalid_admin_options} =
               Admin.bootstrap_default(
                 ctx,
                 @policy,
                 cas("big-version", 9_223_372_036_854_775_807)
               )

      assert {:error, :invalid_policy} =
               Admin.bootstrap_default(
                 ctx,
                 %{@policy | weight: 2_147_483_648},
                 cas("weight-overflow", 0)
               )

      assert {:error, :invalid_target} =
               Admin.put_state(ctx, {:tenant, "bad\0tenant"}, :drain, cas("bad-target", 0))

      assert {:error, :invalid_audit_options} = Admin.export_events(ctx, :not_options)

      assert {:error, :invalid_audit_options} =
               Admin.put_legal_hold(ctx,
                 first_audit_id: 9_223_372_036_854_775_808,
                 last_audit_id: 9_223_372_036_854_775_808,
                 reason: "overflow",
                 source: "test",
                 event_id: "overflow-hold",
                 actor: "legal"
               )

      assert {audit_ids(), receipt_count()} == baseline
      multibyte_source = String.duplicate("é", 32)

      assert {:ok, _} =
               Admin.bootstrap_default(ctx, @policy,
                 expected_version: 0,
                 source: multibyte_source,
                 event_id: "control-char",
                 actor: "operator#{<<31>>}"
               )
    end

    defp bootstrap!(ctx) do
      assert {:ok, result} = Admin.bootstrap_default(ctx, @policy, cas("bootstrap", 0))
      result
    end

    defp cas(event_id, version, overrides \\ []) do
      event(event_id, overrides) ++ [expected_version: version]
    end

    defp event(event_id, overrides \\ []) do
      [source: "test", event_id: event_id, actor: Keyword.get(overrides, :actor, "operator")]
    end

    defp partition_row(scope_key, prefix \\ "public") do
      TestRepo.query!(
        """
        SELECT scope_key, preferred_active, max_active, weight, borrowing, admin_state,
               partition_version, admission_epoch
        FROM \"#{prefix}\".docket_claim_partitions
        WHERE scope_key = $1
        """,
        [scope_key]
      ).rows
    end

    defp gate_state do
      TestRepo.query!("""
      SELECT readiness, admission_mode, readiness_epoch, mode_epoch
      FROM docket_claim_admission_gate
      """).rows
    end

    defp receipt_count do
      TestRepo.query!("SELECT count(*) FROM docket_claim_policy_receipts").rows
      |> hd()
      |> hd()
    end

    defp audit_ids do
      TestRepo.query!("SELECT audit_id FROM docket_claim_policy_events ORDER BY audit_id").rows
      |> List.flatten()
    end

    defp audit_event_ids do
      TestRepo.query!("SELECT event_id FROM docket_claim_policy_events").rows |> List.flatten()
    end

    defp audit_event_order(event_ids) do
      TestRepo.query!(
        """
        SELECT event_id
        FROM docket_claim_policy_events
        WHERE event_id = ANY($1::text[])
        ORDER BY audit_id
        """,
        [event_ids]
      ).rows
      |> List.flatten()
    end

    defp advisory_waiter_count do
      TestRepo.query!("""
      SELECT count(*)
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND wait_event_type = 'Lock'
        AND wait_event = 'advisory'
      """).rows
      |> hd()
      |> hd()
    end

    defp default_update_waiter_pids do
      TestRepo.query!("""
      SELECT activity.pid
      FROM pg_stat_activity AS activity
      WHERE activity.datname = current_database()
        AND activity.pid <> pg_backend_pid()
        AND activity.wait_event_type = 'Lock'
        AND activity.query LIKE '%docket_claim_policy%'
        AND activity.query LIKE '%FOR UPDATE%'
        AND EXISTS (
          SELECT 1
          FROM pg_locks AS waiting_lock
          WHERE waiting_lock.pid = activity.pid
            AND waiting_lock.granted = false
        )
      ORDER BY activity.pid
      """).rows
      |> List.flatten()
    end

    defp wait_until(predicate, attempts \\ 100)

    defp wait_until(predicate, attempts) when attempts > 0 do
      if predicate.() do
        :ok
      else
        receive do
        after
          10 -> wait_until(predicate, attempts - 1)
        end
      end
    end

    defp wait_until(_predicate, 0), do: flunk("deterministic database barrier was not reached")

    defp settings do
      TestRepo.query!(
        "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')"
      ).rows
    end

    defp lifecycle_insert(scope_key) do
      TestRepo.query!(
        """
        INSERT INTO docket_claim_partitions (scope_key)
        VALUES ($1)
        ON CONFLICT (scope_key) DO NOTHING
        """,
        [scope_key]
      )
    end

    defp assert_lock_error(lock_sql, expected, operation) do
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!(lock_sql)
            send(parent, {:authority_locked, self()})

            receive do
              :release_authority -> :ok
            end
          end)
        end)

      assert_receive {:authority_locked, holder_pid}
      assert {:error, ^expected} = operation.()
      send(holder_pid, :release_authority)
      assert {:ok, _} = Task.await(holder)
    end
  end
end
