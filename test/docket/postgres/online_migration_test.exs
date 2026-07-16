if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.OnlineMigrationTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.{Admin, Backfill, OnlineDDL, Readiness}
    alias Docket.Postgres.ClaimPolicyAdminTestRepo, as: TestRepo
    alias Docket.Postgres.OnlineMigration

    @migration_version 20_260_716_000_172
    @private_migration_version 20_260_716_000_173
    @online_migration_version 20_260_716_000_174
    @populated_v1_version 20_260_716_000_175
    @populated_v2_version 20_260_716_000_176
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

    defmodule CompleteOnlineDocket do
      use Ecto.Migration
      @disable_ddl_transaction true
      def up, do: Docket.Postgres.OnlineMigration.up(repo: repo(), prefix: "public")
      def down, do: Docket.Postgres.OnlineMigration.down(repo: repo(), prefix: "public")
    end

    defmodule InterruptedOnlineDocket do
      use Ecto.Migration
      @disable_ddl_transaction true

      def up do
        Docket.Postgres.OnlineMigration.up(repo: repo(), prefix: "public")
        raise "injected host interruption after online DDL"
      end

      def down, do: :ok
    end

    defmodule NoLockRepo do
      use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
    end

    defmodule InstallPopulatedV1 do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(prefix: "populated_v1", version: 1)
      def down, do: Docket.Postgres.Migration.down(prefix: "populated_v1", version: 1)
    end

    defmodule UpgradePopulatedV2 do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(prefix: "populated_v1", version: 2)
      def down, do: Docket.Postgres.Migration.down(prefix: "populated_v1", version: 2)
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      :ok =
        Ecto.Migrator.up(TestRepo, @private_migration_version, InstallPrivateDocket, log: false)

      %{
        context: Docket.Postgres.context(repo: TestRepo),
        private_context: Docket.Postgres.context(repo: TestRepo, prefix: "docket_private")
      }
    end

    test "FK installation is gated after exact indexes and a rerun completes all checkpoints", %{
      context: context
    } do
      assert_raise RuntimeError, ~r/requires dual-write attestation/, fn ->
        OnlineMigration.up(repo: TestRepo)
      end

      assert [["live_index", 3, "foreign_key_failed", true, true, "absent"]] =
               rollout_state("public")

      prepare_prefix!(context, "public")
      assert :ok = OnlineMigration.up(repo: TestRepo)

      assert [["complete", 7, nil, true, true, "validated"]] = rollout_state("public")

      assert {:ok, %{ready_index_valid: true, live_index_valid: true, fk_disposition: :validated}} =
               OnlineMigration.inspect_state(TestRepo, "public")

      assert index_names("public") ==
               Enum.sort([OnlineDDL.index_name(:ready), OnlineDDL.index_name(:live)])
    end

    test "unknown commit after concurrent DDL resumes by catalog inspection without rebuilding",
         %{
           context: context
         } do
      prepare_prefix!(context, "public")
      Process.put({OnlineMigration, :fail_after_ddl}, :ready_index)
      on_exit(fn -> Process.delete({OnlineMigration, :fail_after_ddl}) end)

      assert_raise RuntimeError, ~r/injected Docket online interruption/, fn ->
        OnlineMigration.up(repo: TestRepo)
      end

      Process.delete({OnlineMigration, :fail_after_ddl})
      oid_before = index_oid("public", OnlineDDL.index_name(:ready))

      assert [["not_started", 1, "ready_index_failed", false, false, "absent"]] =
               rollout_state("public")

      assert :ok = OnlineMigration.up(repo: TestRepo)
      assert index_oid("public", OnlineDDL.index_name(:ready)) == oid_before
      assert [["complete", 5, nil, true, true, "validated"]] = rollout_state("public")
    end

    test "every later committed DDL boundary resumes from authoritative catalog state", %{
      context: context
    } do
      prepare_prefix!(context, "public")
      on_exit(fn -> Process.delete({OnlineMigration, :fail_after_ddl}) end)

      for phase <- [:live_index, :foreign_key, :foreign_key_validation] do
        Process.put({OnlineMigration, :fail_after_ddl}, phase)

        assert_raise RuntimeError, ~r/injected Docket online interruption/, fn ->
          OnlineMigration.up(repo: TestRepo)
        end

        Process.delete({OnlineMigration, :fail_after_ddl})
        before = online_object_identity(phase)
        assert :ok = OnlineMigration.up(repo: TestRepo)
        assert online_object_identity(phase) == before

        assert [["complete", _attempts, nil, true, true, "validated"]] =
                 rollout_state("public")

        unless phase == :foreign_key_validation do
          assert :ok = OnlineMigration.down(repo: TestRepo)
        end
      end
    end

    test "NOT VALID FK immediately rejects an old non-dual-writing writer and validation retries",
         %{
           context: context
         } do
      prepare_prefix!(context, "public")
      Process.put({OnlineMigration, :fail_after_ddl}, :foreign_key)
      on_exit(fn -> Process.delete({OnlineMigration, :fail_after_ddl}) end)

      assert_raise RuntimeError, ~r/injected Docket online interruption/, fn ->
        OnlineMigration.up(repo: TestRepo)
      end

      Process.delete({OnlineMigration, :fail_after_ddl})

      assert TestRepo.query!("""
             SELECT convalidated FROM pg_constraint
             WHERE conname = '#{OnlineDDL.foreign_key_name()}'
             """).rows == [[false]]

      now = DateTime.utc_now()

      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ('old-writer', 'old-graph', 'old-hash', '\\x00', $1)
        """,
        [now]
      )

      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation} = pg}} =
               TestRepo.query(
                 """
                 INSERT INTO docket_runs
                   (run_id, tenant_id, graph_id, graph_hash, status, state,
                    checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
                 VALUES ('old-run', 'old-writer', 'old-graph', 'old-hash', 'running',
                         '\\x00', 1, $1, $1, $1, $1)
                 """,
                 [now]
               )

      assert pg.constraint == OnlineDDL.foreign_key_name()
      assert :ok = OnlineMigration.up(repo: TestRepo)

      assert TestRepo.query!("""
             SELECT convalidated FROM pg_constraint
             WHERE conname = '#{OnlineDDL.foreign_key_name()}'
             """).rows == [[true]]
    end

    test "real concurrent-index and FK-validation cancellation leave retryable catalog state", %{
      context: context
    } do
      insert_bulk_runs!(100_000)
      prepare_prefix!(context, "public")

      canceled_index =
        TestRepo.checkout(fn ->
          TestRepo.query!("SET statement_timeout = '1ms'")
          result = TestRepo.query(OnlineDDL.create_index_sql("public", :ready), [])
          TestRepo.query!("RESET statement_timeout")
          result
        end)

      assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} = canceled_index

      assert TestRepo.query!("""
             SELECT indisvalid
             FROM pg_index
             WHERE indexrelid = 'docket_runs_scope_ready_claim_index'::regclass
             """).rows == [[false]]

      Process.put({OnlineMigration, :fail_after_ddl}, :foreign_key)
      on_exit(fn -> Process.delete({OnlineMigration, :fail_after_ddl}) end)

      assert_raise RuntimeError, ~r/injected Docket online interruption/, fn ->
        OnlineMigration.up(repo: TestRepo)
      end

      Process.delete({OnlineMigration, :fail_after_ddl})

      canceled_validation =
        TestRepo.checkout(fn ->
          TestRepo.query!("SET statement_timeout = '1ms'")
          result = TestRepo.query(OnlineDDL.validate_foreign_key_sql("public"), [])
          TestRepo.query!("RESET statement_timeout")
          result
        end)

      assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
               canceled_validation

      assert :ok = OnlineMigration.up(repo: TestRepo)
      assert [["complete", _attempts, nil, true, true, "validated"]] = rollout_state("public")
    end

    test "generated migration executes under advisory migration lock and helper refuses other repos",
         %{
           context: context
         } do
      prepare_prefix!(context, "public")

      assert :ok =
               Ecto.Migrator.up(TestRepo, @online_migration_version, CompleteOnlineDocket,
                 log: false
               )

      assert [["complete", 4, nil, true, true, "validated"]] = rollout_state("public")

      no_lock_config =
        TestRepo.config()
        |> Keyword.put(:pool_size, 1)
        |> Keyword.put(:migration_lock, :table_lock)

      Application.put_env(:docket, NoLockRepo, no_lock_config)

      start_supervised!(NoLockRepo)

      assert_raise ArgumentError, ~r/migration_lock: :pg_advisory_lock/, fn ->
        OnlineMigration.up(repo: NoLockRepo)
      end
    end

    test "Ecto reruns unknown-commit DDL before recording the migration under its advisory lock",
         %{
           context: context
         } do
      prepare_prefix!(context, "public")

      assert_raise RuntimeError, ~r/injected host interruption after online DDL/, fn ->
        Ecto.Migrator.up(TestRepo, @online_migration_version, InterruptedOnlineDocket, log: false)
      end

      assert TestRepo.query!(
               "SELECT 1 FROM schema_migrations WHERE version = $1",
               [@online_migration_version]
             ).rows == []

      ready_oid = index_oid("public", OnlineDDL.index_name(:ready))
      ecto_lock = :erlang.phash2({:ecto, nil, TestRepo})

      TestRepo.query!("""
      CREATE FUNCTION docket_assert_ecto_migration_lock() RETURNS trigger
      LANGUAGE plpgsql AS $docket$
      BEGIN
        IF NEW.version = #{@online_migration_version} AND NOT EXISTS (
          SELECT 1
          FROM pg_locks
          WHERE locktype = 'advisory'
            AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND classid = 0
            AND objid = #{ecto_lock}
            AND objsubid = 1
            AND granted
        ) THEN
          RAISE EXCEPTION 'Ecto advisory migration lock missing during schema_migrations insert';
        END IF;
        RETURN NEW;
      END
      $docket$
      """)

      TestRepo.query!("""
      CREATE TRIGGER docket_assert_ecto_migration_lock_trigger
      BEFORE INSERT ON schema_migrations
      FOR EACH ROW EXECUTE FUNCTION docket_assert_ecto_migration_lock()
      """)

      assert :ok =
               Ecto.Migrator.up(TestRepo, @online_migration_version, CompleteOnlineDocket,
                 log: false
               )

      assert index_oid("public", OnlineDDL.index_name(:ready)) == ready_oid

      assert TestRepo.query!(
               "SELECT 1 FROM schema_migrations WHERE version = $1",
               [@online_migration_version]
             ).rows == [[1]]

      TestRepo.query!(
        "DROP TRIGGER docket_assert_ecto_migration_lock_trigger ON schema_migrations"
      )

      TestRepo.query!("DROP FUNCTION docket_assert_ecto_migration_lock()")
    end

    test "custom prefixes are isolated and use the same prefix-neutral fingerprints", %{
      private_context: context
    } do
      prepare_prefix!(context, "docket_private")
      assert :ok = OnlineMigration.up(repo: TestRepo, prefix: "docket_private")

      assert [["complete", 4, nil, true, true, "validated"]] =
               rollout_state("docket_private")

      assert rollout_state("public") == [["not_started", 0, nil, false, false, "absent"]]

      assert OnlineDDL.index_fingerprint("public", :ready) ==
               OnlineDDL.index_fingerprint("docket_private", :ready)
    end

    test "populated v1 tenantless and tenant runs upgrade through online readiness independently" do
      assert :ok =
               Ecto.Migrator.up(TestRepo, @populated_v1_version, InstallPopulatedV1, log: false)

      insert_v1_run!("populated_v1", 4, nil)
      insert_v1_run!("populated_v1", 12, "tenant-upgrade")

      assert :ok =
               Ecto.Migrator.up(TestRepo, @populated_v2_version, UpgradePopulatedV2, log: false)

      context = Docket.Postgres.context(repo: TestRepo, prefix: "populated_v1")

      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo, prefix: "populated_v1") ==
               2

      assert TestRepo.query!("SELECT readiness FROM populated_v1.docket_claim_admission_gate").rows ==
               [["not_ready"]]

      prepare_prefix!(context, "populated_v1")

      assert TestRepo.query!(
               "SELECT scope_key FROM populated_v1.docket_claim_partitions ORDER BY scope_key"
             ).rows == [[""], ["tenant-upgrade"]]

      assert :ok = OnlineMigration.up(repo: TestRepo, prefix: "populated_v1")
      fingerprints = OnlineDDL.index_fingerprints("populated_v1")

      assert {:ok, %{outcome: :applied, version: 1}} =
               Readiness.verify(context, verify_opts("populated-ready", 0, fingerprints))

      assert TestRepo.query!("""
             SELECT readiness, admission_mode, mode_epoch
             FROM populated_v1.docket_claim_admission_gate
             """).rows == [["ready", "legacy", 0]]
    end

    test "invalid indexes repair while valid foreign definitions refuse replacement", %{
      context: context
    } do
      prepare_prefix!(context, "public")
      assert :ok = OnlineMigration.up(repo: TestRepo)
      old_oid = index_oid("public", OnlineDDL.index_name(:ready))

      TestRepo.query!(
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::text::regclass",
        [~s("public"."#{OnlineDDL.index_name(:ready)}")]
      )

      assert :ok = OnlineMigration.up(repo: TestRepo)
      refute index_oid("public", OnlineDDL.index_name(:ready)) == old_oid

      TestRepo.query!(OnlineDDL.drop_index_sql("public", :live))

      TestRepo.query!("""
      CREATE INDEX "#{OnlineDDL.index_name(:live)}"
      ON "public"."docket_runs" (scope_key, id)
      WHERE status = 'running' AND claim_token IS NOT NULL
      """)

      wrong_oid = index_oid("public", OnlineDDL.index_name(:live))

      assert_raise RuntimeError, ~r/does not match/, fn ->
        OnlineMigration.up(repo: TestRepo)
      end

      assert index_oid("public", OnlineDDL.index_name(:live)) == wrong_oid

      TestRepo.query!(
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::text::regclass",
        [~s("public"."#{OnlineDDL.index_name(:live)}")]
      )

      assert_raise RuntimeError, ~r/does not match/, fn ->
        OnlineMigration.up(repo: TestRepo)
      end

      assert index_oid("public", OnlineDDL.index_name(:live)) == wrong_oid

      assert [[_phase, _attempts, "live_index_failed", _ready, _live, _fk]] =
               rollout_state("public")
    end

    test "constraint-owned exclusion indexes cannot satisfy or be repaired as online indexes", %{
      context: context
    } do
      prepare_prefix!(context, "public")
      assert :ok = OnlineMigration.up(repo: TestRepo)
      live_oid = index_oid("public", OnlineDDL.index_name(:live))
      TestRepo.query!(OnlineDDL.drop_index_sql("public", :ready))

      TestRepo.query!("""
      ALTER TABLE docket_runs
      ADD CONSTRAINT #{OnlineDDL.index_name(:ready)}
      EXCLUDE USING btree (scope_key WITH =, wake_at WITH =, id WITH =)
      WHERE (
        status = 'running' AND poisoned_at IS NULL AND
        claim_token IS NULL AND wake_at IS NOT NULL
      )
      """)

      assert TestRepo.query!("""
             SELECT index.indisexclusion, index.indisprimary, index.indimmediate,
                    con.contype::text
             FROM pg_class AS class
             JOIN pg_index AS index ON index.indexrelid = class.oid
             JOIN pg_constraint AS con ON con.conindid = class.oid
             WHERE class.oid = '#{OnlineDDL.index_name(:ready)}'::regclass
             """).rows == [[true, false, true, "x"]]

      assert_raise RuntimeError, ~r/does not match/, fn ->
        OnlineMigration.up(repo: TestRepo)
      end

      fingerprints = OnlineDDL.index_fingerprints("public")

      assert {:error, {:not_ready, [:ready_index_invalid]}} =
               Readiness.verify(context, verify_opts("constraint-index", 0, fingerprints))

      assert_raise RuntimeError, ~r/conflicting|foreign definition/, fn ->
        OnlineMigration.down(repo: TestRepo)
      end

      assert index_oid("public", OnlineDDL.index_name(:live)) == live_oid

      assert TestRepo.query!("""
             SELECT convalidated FROM pg_constraint
             WHERE conname = '#{OnlineDDL.foreign_key_name()}'
             """).rows == [[true]]
    end

    test "same-name CHECK constraint is never mistaken for an absent partition FK", %{
      context: context
    } do
      assert_foreign_constraint_conflict!(context, :check)
    end

    test "same-name UNIQUE constraint is never mistaken for an absent partition FK", %{
      context: context
    } do
      assert_foreign_constraint_conflict!(context, :unique)
    end

    test "ready prefixes refuse repair until readiness verification demotes", %{context: context} do
      prepare_prefix!(context, "public")
      assert :ok = OnlineMigration.up(repo: TestRepo)
      fingerprints = OnlineDDL.index_fingerprints("public")

      assert {:ok, %{outcome: :applied}} =
               Readiness.verify(context, verify_opts("ready-before-repair", 0, fingerprints))

      old_oid = index_oid("public", OnlineDDL.index_name(:ready))

      TestRepo.query!(
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::text::regclass",
        [~s("public"."#{OnlineDDL.index_name(:ready)}")]
      )

      assert_raise RuntimeError, ~r/requires a not-ready prefix/, fn ->
        OnlineMigration.up(repo: TestRepo)
      end

      assert index_oid("public", OnlineDDL.index_name(:ready)) == old_oid

      assert {:ok, %{outcome: :demoted, reasons: [:ready_index_invalid]}} =
               Readiness.verify(context, verify_opts("demote-before-repair", 1, fingerprints))

      TestRepo.query!(
        "UPDATE docket_claim_admission_gate SET admission_mode = 'tenant_fair', mode_epoch = 1"
      )

      assert :ok = OnlineMigration.up(repo: TestRepo)
      refute index_oid("public", OnlineDDL.index_name(:ready)) == old_oid

      assert TestRepo.query!(
               "SELECT readiness, admission_mode, mode_epoch FROM docket_claim_admission_gate"
             ).rows == [["not_ready", "tenant_fair", 1]]
    end

    test "one prefix advisory runner excludes a concurrent helper and session settings restore",
         %{
           context: context
         } do
      prepare_prefix!(context, "public")
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.checkout(fn ->
            TestRepo.query!(
              "SELECT pg_advisory_lock(hashtextextended($1, 0))",
              ["docket-v2-online-migration-v1:public"]
            )

            send(parent, :held)
            receive do: (:release -> :ok)
          end)
        end)

      assert_receive :held

      TestRepo.checkout(fn ->
        TestRepo.query!("SET lock_timeout = '321ms'")
        TestRepo.query!("SET statement_timeout = '6543ms'")

        assert_raise RuntimeError, ~r/already has a runner/, fn ->
          OnlineMigration.up(repo: TestRepo)
        end

        assert TestRepo.query!("SHOW lock_timeout").rows == [["321ms"]]
        assert TestRepo.query!("SHOW statement_timeout").rows == [["6543ms"]]
      end)

      send(holder.pid, :release)
      Task.await(holder)
    end

    test "post-DDL failure releases the runner and restores the checked-out session", %{
      context: context
    } do
      prepare_prefix!(context, "public")
      on_exit(fn -> Process.delete({OnlineMigration, :fail_after_ddl}) end)

      TestRepo.checkout(fn ->
        TestRepo.query!("SET lock_timeout = '321ms'")
        TestRepo.query!("SET statement_timeout = '6543ms'")
        Process.put({OnlineMigration, :fail_after_ddl}, :ready_index)

        assert_raise RuntimeError, ~r/injected Docket online interruption/, fn ->
          OnlineMigration.up(repo: TestRepo)
        end

        Process.delete({OnlineMigration, :fail_after_ddl})
        assert TestRepo.query!("SHOW lock_timeout").rows == [["321ms"]]
        assert TestRepo.query!("SHOW statement_timeout").rows == [["6543ms"]]

        assert TestRepo.query!(
                 "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                 ["docket-v2-online-migration-v1:public"]
               ).rows == [[true]]

        assert TestRepo.query!(
                 "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                 ["docket-v2-online-migration-v1:public"]
               ).rows == [[true]]
      end)
    end

    test "online down excludes first readiness promotion and history permanently refuses down", %{
      context: context
    } do
      prepare_prefix!(context, "public")
      assert :ok = OnlineMigration.up(repo: TestRepo)
      fingerprints = OnlineDDL.index_fingerprints("public")
      owner = self()
      token = make_ref()

      down =
        Task.async(fn ->
          Process.put({OnlineMigration, :pause_after_down_guard}, {owner, token})
          OnlineMigration.down(repo: TestRepo)
        end)

      assert_receive {:docket_online_down_guarded, ^token}

      assert {:error, {:lock_timeout, :rollout}} =
               Readiness.verify(context, verify_opts("down-race", 0, fingerprints))

      send(down.pid, {:continue_docket_online_down, token})
      assert :ok = Task.await(down)

      assert {:error,
              {:not_ready, [:foreign_key_unvalidated, :live_index_invalid, :ready_index_invalid]}} =
               Readiness.verify(context, verify_opts("down-race", 0, fingerprints))

      assert :ok = OnlineMigration.up(repo: TestRepo)

      assert {:ok, %{outcome: :applied, version: 1}} =
               Readiness.verify(context, verify_opts("down-history", 0, fingerprints))

      assert_raise RuntimeError, ~r/readiness or activation history/, fn ->
        OnlineMigration.down(repo: TestRepo)
      end
    end

    test "online migration refuses outer transaction context", %{context: context} do
      prepare_prefix!(context, "public")

      assert_raise ArgumentError, ~r/cannot run inside a transaction/, fn ->
        TestRepo.transaction(fn -> OnlineMigration.up(repo: TestRepo) end)
      end
    end

    test "readiness promotes only from live proof, demotes drift, and replay survives audit prune",
         %{
           context: context
         } do
      prepare_prefix!(context, "public")
      assert :ok = OnlineMigration.up(repo: TestRepo)
      fingerprints = OnlineDDL.index_fingerprints("public")

      assert {:ok, %{outcome: :applied, version: 1}} =
               Readiness.verify(context, verify_opts("promote", 0, fingerprints))

      assert [[first_verified_at, first_updated_at]] =
               TestRepo.query!(
                 "SELECT verified_at, updated_at FROM docket_claim_rollout WHERE id = 1"
               ).rows

      TestRepo.query!("SELECT pg_sleep(0.01)")

      assert {:ok, %{outcome: :unchanged, previous_version: 1, version: 1}} =
               Readiness.verify(context, verify_opts("verify-again", 1, fingerprints))

      assert [[second_verified_at, second_updated_at]] =
               TestRepo.query!(
                 "SELECT verified_at, updated_at FROM docket_claim_rollout WHERE id = 1"
               ).rows

      assert DateTime.compare(second_verified_at, first_verified_at) == :gt
      assert DateTime.compare(second_updated_at, first_updated_at) == :gt

      TestRepo.query!(
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::text::regclass",
        [~s("public"."#{OnlineDDL.index_name(:ready)}")]
      )

      assert {:ok,
              %{
                outcome: :demoted,
                version: 2,
                reasons: [:ready_index_invalid]
              } = demoted} = Readiness.verify(context, verify_opts("demote", 1, fingerprints))

      TestRepo.query!("DELETE FROM docket_claim_policy_events WHERE event_id = 'demote'")

      assert {:ok, %{outcome: :replayed, original: ^demoted}} =
               Readiness.verify(context, verify_opts("demote", 1, fingerprints))

      assert [["not_ready", 2]] =
               TestRepo.query!(
                 "SELECT readiness, readiness_epoch FROM docket_claim_admission_gate"
               ).rows
    end

    test "wrong hashes fail closed and a changed default demotes then re-promotes", %{
      context: context
    } do
      prepare_prefix!(context, "public")
      assert :ok = OnlineMigration.up(repo: TestRepo)
      fingerprints = OnlineDDL.index_fingerprints("public")

      assert {:error, {:not_ready, [:ready_index_invalid]}} =
               Readiness.verify(
                 context,
                 verify_opts("wrong-before-ready", 0, %{fingerprints | ready: <<0::256>>})
               )

      assert {:ok, %{outcome: :applied, version: 1}} =
               Readiness.verify(context, verify_opts("right-ready", 0, fingerprints))

      assert {:ok, %{outcome: :demoted, version: 2, reasons: [:ready_index_invalid]}} =
               Readiness.verify(
                 context,
                 verify_opts("wrong-ready", 1, %{fingerprints | ready: <<1::256>>})
               )

      assert {:ok, %{outcome: :applied, version: 3}} =
               Readiness.verify(context, verify_opts("restore-ready", 2, fingerprints))

      assert {:ok, %{version: 2}} =
               Admin.put_default(context, %{@policy | max_active: 5},
                 expected_version: 1,
                 source: "online-test",
                 event_id: "change-default",
                 actor: "operator"
               )

      assert {:ok, %{outcome: :demoted, version: 4, reasons: [:default_fingerprint_changed]}} =
               Readiness.verify(context, verify_opts("default-drift", 3, fingerprints))

      assert {:ok, %{outcome: :applied, version: 5}} =
               Readiness.verify(context, verify_opts("default-reverify", 4, fingerprints))
    end

    defp prepare_prefix!(context, prefix) do
      suffix = String.replace(prefix, "_", "-")

      assert {:ok, _} =
               Readiness.attest_dual_write(context,
                 evidence_fingerprint: :crypto.hash(:sha256, "dual-#{suffix}"),
                 source: "online-test",
                 event_id: "dual-#{suffix}",
                 actor: "operator"
               )

      advance_until_complete!(context)

      assert {:ok, _} =
               Admin.bootstrap_default(context, @policy,
                 expected_version: 0,
                 source: "online-test",
                 event_id: "default-#{suffix}",
                 actor: "operator"
               )
    end

    defp assert_foreign_constraint_conflict!(context, kind) do
      prepare_prefix!(context, "public")
      assert :ok = OnlineMigration.up(repo: TestRepo)
      ready_oid = index_oid("public", OnlineDDL.index_name(:ready))
      live_oid = index_oid("public", OnlineDDL.index_name(:live))

      TestRepo.query!("""
      ALTER TABLE docket_runs DROP CONSTRAINT #{OnlineDDL.foreign_key_name()}
      """)

      conflict_ddl =
        case kind do
          :check ->
            """
            ALTER TABLE docket_runs
            ADD CONSTRAINT #{OnlineDDL.foreign_key_name()}
            CHECK (scope_key IS NOT NULL) NOT VALID
            """

          :unique ->
            """
            ALTER TABLE docket_runs
            ADD CONSTRAINT #{OnlineDDL.foreign_key_name()}
            UNIQUE (scope_key, id)
            """
        end

      TestRepo.query!(conflict_ddl)

      expected_type = if kind == :check, do: "c", else: "u"

      assert TestRepo.query!("""
             SELECT contype::text, confrelid
             FROM pg_constraint
             WHERE conrelid = 'docket_runs'::regclass
               AND conname = '#{OnlineDDL.foreign_key_name()}'
             """).rows == [[expected_type, 0]]

      assert {:ok,
              %{
                ready_index_valid: true,
                live_index_valid: true,
                fk_disposition: :absent,
                fk_definition_valid: false
              }} = OnlineMigration.inspect_state(TestRepo, "public")

      assert_raise RuntimeError, ~r/conflicts with the approved definition/, fn ->
        OnlineMigration.up(repo: TestRepo)
      end

      fingerprints = OnlineDDL.index_fingerprints("public")

      assert {:error, {:not_ready, [:foreign_key_unvalidated]}} =
               Readiness.verify(
                 context,
                 verify_opts("#{kind}-fk-conflict", 0, fingerprints)
               )

      assert TestRepo.query!("""
             SELECT readiness, readiness_epoch, admission_mode, mode_epoch
             FROM docket_claim_admission_gate
             """).rows == [["not_ready", 0, "legacy", 0]]

      assert_raise RuntimeError, ~r/conflicting foreign key/, fn ->
        OnlineMigration.down(repo: TestRepo)
      end

      assert index_oid("public", OnlineDDL.index_name(:ready)) == ready_oid
      assert index_oid("public", OnlineDDL.index_name(:live)) == live_oid

      assert TestRepo.query!("""
             SELECT contype::text
             FROM pg_constraint
             WHERE conrelid = 'docket_runs'::regclass
               AND conname = '#{OnlineDDL.foreign_key_name()}'
             """).rows == [[expected_type]]
    end

    defp advance_until_complete!(context) do
      case Backfill.advance(context, batch_size: 10_000) do
        {:ok, %{phase: :complete}} -> :ok
        {:ok, _state} -> advance_until_complete!(context)
      end
    end

    defp verify_opts(event_id, epoch, fingerprints) do
      [
        expected_readiness_epoch: epoch,
        ready_index_ddl_sha256: fingerprints.ready,
        live_index_ddl_sha256: fingerprints.live,
        source: "online-test",
        event_id: event_id,
        actor: "operator"
      ]
    end

    defp rollout_state(prefix) do
      TestRepo.query!("""
      SELECT online_phase, online_attempts, online_last_error,
             ready_index_valid, live_index_valid, fk_disposition
      FROM "#{prefix}".docket_claim_rollout
      """).rows
    end

    defp index_names(prefix) do
      TestRepo.query!(
        """
        SELECT class.relname
        FROM pg_class AS class
        JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
        WHERE namespace.nspname = $1 AND class.relkind = 'i'
          AND class.relname = ANY($2::text[])
        ORDER BY class.relname
        """,
        [prefix, [OnlineDDL.index_name(:ready), OnlineDDL.index_name(:live)]]
      ).rows
      |> List.flatten()
    end

    defp index_oid(prefix, name) do
      [[oid]] =
        TestRepo.query!(
          """
          SELECT class.oid
          FROM pg_class AS class
          JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
          WHERE namespace.nspname = $1 AND class.relname = $2
          """,
          [prefix, name]
        ).rows

      oid
    end

    defp online_object_identity(phase) when phase in [:live_index] do
      index_oid("public", OnlineDDL.index_name(:live))
    end

    defp online_object_identity(phase) when phase in [:foreign_key, :foreign_key_validation] do
      [[oid]] =
        TestRepo.query!("""
        SELECT oid
        FROM pg_constraint
        WHERE conname = '#{OnlineDDL.foreign_key_name()}'
          AND conrelid = 'public.docket_runs'::regclass
        """).rows

      oid
    end

    defp insert_v1_run!(prefix, id, tenant_id) do
      now = DateTime.utc_now()
      graph_id = "graph-#{id}"
      graph_hash = "hash-#{id}"

      TestRepo.query!(
        """
        INSERT INTO "#{prefix}".docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, $2, $3, $4, $5)
        """,
        [tenant_id, graph_id, graph_hash, <<0>>, now]
      )

      TestRepo.query!(
        """
        INSERT INTO "#{prefix}".docket_runs
          (id, run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, 'running', $6, 1, $7, $7, $7, $7)
        """,
        [id, "run-#{id}", tenant_id, graph_id, graph_hash, <<0>>, now]
      )
    end

    defp insert_bulk_runs!(count) do
      now = DateTime.utc_now()

      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES (NULL, 'bulk-graph', 'bulk-hash', $1, $2)
        """,
        [<<0>>, now]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        SELECT 'bulk-' || id::text, NULL, 'bulk-graph', 'bulk-hash', 'running',
               $1, 1, $2, $2, $2, $2
        FROM generate_series(1, $3) AS id
        """,
        [<<0>>, now, count]
      )
    end
  end
end
