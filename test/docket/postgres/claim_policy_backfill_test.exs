if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyBackfillTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.{Backfill, Readiness}
    alias Docket.Postgres.ClaimPolicyAdminTestRepo, as: TestRepo

    @migration_version 20_260_716_000_167
    @private_migration_version 20_260_716_000_168
    @populated_v1_version 20_260_716_000_169
    @populated_v2_version 20_260_716_000_170
    @evidence :binary.copy(<<67>>, 32)

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

    test "dual-write attestation is deterministic, audited, replayable, and rollout-only", %{
      context: context
    } do
      gate_before = gate_state()

      assert {:error, :invalid_readiness_options} =
               Readiness.attest_dual_write(context, dual_opts("bad", <<1>>))

      assert {:ok,
              %{
                outcome: :applied,
                target: :dual_write,
                assertion_id: assertion_id,
                audit_id: audit_id
              }} = Readiness.attest_dual_write(context, dual_opts("deploy"))

      assert {:ok,
              %{
                outcome: :replayed,
                original: %{assertion_id: ^assertion_id, audit_id: ^audit_id}
              }} =
               Readiness.attest_dual_write(
                 context,
                 Keyword.put(dual_opts("deploy"), :actor, "retry-operator")
               )

      assert {:error, {:event_conflict, %{source: "test", event_id: "deploy"}}} =
               Readiness.attest_dual_write(context, dual_opts("deploy", :binary.copy(<<9>>, 32)))

      assert gate_state() == gate_before

      assert TestRepo.query!("""
             SELECT assertion_id::text, assertion_kind, evidence_fingerprint, expires_at,
                    audit_id
             FROM docket_claim_assertions
             """).rows == [[assertion_id, "dual_write", @evidence, nil, audit_id]]

      assert TestRepo.query!("SELECT dual_write_assertion_id::text FROM docket_claim_rollout").rows ==
               [[assertion_id]]

      assert TestRepo.query!("SELECT operation, actor FROM docket_claim_policy_events").rows ==
               [["attest_dual_write", "operator"]]

      assert TestRepo.query!("SELECT count(*) FROM docket_claim_policy_receipts").rows == [[1]]
    end

    test "finite sparse-id pages resume after every commit and ignore higher dual-written IDs", %{
      context: context
    } do
      insert_run!("public", 2, nil)
      insert_run!("public", 10, "tenant-a")
      insert_run!("public", 50, "tenant-a")

      TestRepo.query!("""
      INSERT INTO docket_claim_partitions
        (scope_key, admin_state, partition_version, admission_epoch)
      VALUES ('tenant-a', 'drain', 3, 7)
      """)

      assert {:error, :dual_write_unattested} = Backfill.advance(context, batch_size: 1)
      assert Enum.at(rollout(), 5) == 0
      attest!(context)

      assert {:ok, %{phase: :running, target_id: 50, cursor: 0, batch_rows: 0}} =
               Backfill.advance(context, batch_size: 1)

      dual_insert_run!("public", 100, "tenant-new")

      assert {:ok, %{cursor: 2, batch_rows: 1, inserted_partitions: 1}} =
               Backfill.advance(context, batch_size: 1)

      assert {:ok, %{cursor: 10, batch_rows: 1, inserted_partitions: 0}} =
               Backfill.advance(context, batch_size: 1)

      assert {:ok, %{cursor: 50, batch_rows: 1, inserted_partitions: 0}} =
               Backfill.advance(context, batch_size: 1)

      assert {:ok, %{phase: :reconciling, cursor: 50, batch_rows: 0}} =
               Backfill.advance(context, batch_size: 1)

      assert {:ok,
              %{
                outcome: :reconciled,
                phase: :complete,
                target_id: 50,
                cursor: 50,
                batches: 3,
                rows: 3,
                retries: 0,
                missing_partition_count: 0
              }} = Backfill.advance(context, batch_size: 1)

      assert partition_keys() == ["", "tenant-a", "tenant-new"]

      assert TestRepo.query!("""
             SELECT admin_state, partition_version, admission_epoch
             FROM docket_claim_partitions WHERE scope_key = 'tenant-a'
             """).rows == [["drain", 3, 7]]

      assert TestRepo.query!("SELECT count(*) FROM docket_claim_policy_events").rows == [[1]]
      assert TestRepo.query!("SELECT count(*) FROM docket_claim_policy_receipts").rows == [[1]]

      previous_updated = rollout() |> List.last()

      assert {:ok, %{outcome: :unchanged, phase: :complete, last_error: nil}} =
               Backfill.advance(context)

      assert DateTime.compare(rollout() |> List.last(), previous_updated) in [:eq, :gt]
    end

    test "custom prefixes and tenantless scope are isolated", %{
      context: context,
      private_context: private
    } do
      insert_run!("public", 1, "public-tenant")
      insert_run!("docket_private", 1, nil)
      insert_run!("docket_private", 7, "private-tenant")
      attest!(context, "public")
      attest!(private, "private")

      complete!(context, batch_size: 10)
      complete!(private, batch_size: 10)

      assert partition_keys("public") == ["public-tenant"]
      assert partition_keys("docket_private") == ["", "private-tenant"]
    end

    test "a populated v1 custom prefix upgrades and backfills without migration-time writes" do
      assert :ok =
               Ecto.Migrator.up(TestRepo, @populated_v1_version, InstallPopulatedV1, log: false)

      insert_run!("populated_v1", 4, nil)
      insert_run!("populated_v1", 12, "upgraded")

      assert :ok =
               Ecto.Migrator.up(TestRepo, @populated_v2_version, UpgradePopulatedV2, log: false)

      context = Docket.Postgres.context(repo: TestRepo, prefix: "populated_v1")
      assert partition_keys("populated_v1") == []
      attest!(context, "populated-v1")
      result = complete!(context, batch_size: 1)
      assert result.rows == 2
      assert result.missing_partition_count == 0
      assert partition_keys("populated_v1") == ["", "upgraded"]
    end

    test "a concurrent dual writer with a delayed lower ID cannot leave a final gap", %{
      context: context
    } do
      insert_run!("public", 100, "existing")
      attest!(context)
      assert {:ok, %{target_id: 100, cursor: 0}} = Backfill.advance(context)

      parent = self()
      {:ok, connection} = Postgrex.start_link(TestRepo.config())

      writer =
        Task.async(fn ->
          Postgrex.transaction(connection, fn conn ->
            now = DateTime.utc_now()

            Postgrex.query!(
              conn,
              """
              INSERT INTO docket_graph_versions
                (tenant_id, graph_id, graph_hash, graph, inserted_at)
              VALUES ('delayed', 'delayed-graph', 'delayed-hash', $1, $2)
              """,
              [<<0>>, now]
            )

            Postgrex.query!(
              conn,
              "INSERT INTO docket_claim_partitions (scope_key) VALUES ('delayed')",
              []
            )

            Postgrex.query!(
              conn,
              """
              INSERT INTO docket_runs
                (id, run_id, tenant_id, graph_id, graph_hash, status, state,
                 checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
              VALUES
                (50, 'delayed-run', 'delayed', 'delayed-graph', 'delayed-hash',
                 'running', $1, 1, $2, $2, $2, $2)
              """,
              [<<0>>, now]
            )

            send(parent, :dual_writer_open)
            receive do: (:commit_dual_writer -> :ok)
          end)
        end)

      assert_receive :dual_writer_open
      complete!(context, batch_size: 10)
      send(writer.pid, :commit_dual_writer)
      assert {:ok, _} = Task.await(writer)
      GenServer.stop(connection)

      assert {:ok, %{outcome: :unchanged, missing_partition_count: 0}} =
               Backfill.advance(context)

      assert partition_keys() == ["delayed", "existing"]
    end

    test "high source and dormant cardinality remains row bounded", %{context: context} do
      now = DateTime.utc_now()

      TestRepo.query!(
        """
        INSERT INTO docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        SELECT 'high-' || value, 'graph', 'hash', $1, $2
        FROM generate_series(1, 120) AS value
        """,
        [<<0>>, now]
      )

      TestRepo.query!(
        """
        INSERT INTO docket_runs
          (id, run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        SELECT 1000 + value, 'high-run-' || value, 'high-' || value,
               'graph', 'hash', 'running', $1, 1, $2, $2, $2, $2
        FROM generate_series(1, 120) AS value
        """,
        [<<0>>, now]
      )

      TestRepo.query!("""
      INSERT INTO docket_claim_partitions (scope_key)
      SELECT 'dormant-' || value FROM generate_series(1, 250) AS value
      """)

      attest!(context)
      result = complete!(context, batch_size: 17)
      assert result.rows == 120
      assert result.batches == 8
      assert result.missing_partition_count == 0
      assert TestRepo.query!("SELECT count(*) FROM docket_claim_partitions").rows == [[370]]
    end

    test "page inserts acquire distinct scope keys in ascending binary order", %{context: context} do
      insert_run!("public", 1, "z-scope")
      insert_run!("public", 2, "a-scope")
      attest!(context)
      assert {:ok, %{phase: :running, target_id: 2, cursor: 0}} = Backfill.advance(context)

      parent = self()
      {:ok, connection} = Postgrex.start_link(TestRepo.config())

      ordered_inserter =
        Task.async(fn ->
          Postgrex.transaction(connection, fn conn ->
            Postgrex.query!(
              conn,
              "INSERT INTO docket_claim_partitions (scope_key) VALUES ('a-scope')",
              []
            )

            send(parent, :ascending_first_key_held)
            receive do: (:insert_ascending_second_key -> :ok)

            Postgrex.query!(
              conn,
              "INSERT INTO docket_claim_partitions (scope_key) VALUES ('z-scope')",
              []
            )
          end)
        end)

      assert_receive :ascending_first_key_held

      backfill =
        Task.async(fn ->
          Backfill.advance(context,
            batch_size: 2,
            lock_timeout_ms: 1_000,
            statement_timeout_ms: 5_000
          )
        end)

      await_ungranted_transaction_lock!()
      send(ordered_inserter.pid, :insert_ascending_second_key)
      assert {:ok, _} = Task.await(ordered_inserter)
      assert {:ok, %{cursor: 2, batch_rows: 2}} = Task.await(backfill)
      GenServer.stop(connection)

      assert partition_keys() == ["a-scope", "z-scope"]
    end

    test "a lock-cancelled page records one retry and resumes from the same cursor", %{
      context: context
    } do
      insert_run!("public", 4, "locked")
      attest!(context)
      assert {:ok, %{phase: :running, cursor: 0}} = Backfill.advance(context)

      parent = self()
      {:ok, connection} = Postgrex.start_link(TestRepo.config())

      holder =
        Task.async(fn ->
          Postgrex.transaction(connection, fn conn ->
            Postgrex.query!(
              conn,
              "INSERT INTO docket_claim_partitions (scope_key) VALUES ('locked')",
              []
            )

            send(parent, :partition_insert_held)
            receive do: (:release -> :ok)
          end)
        end)

      assert_receive :partition_insert_held

      assert {:error, :backfill_lock_timeout} =
               Backfill.advance(context, batch_size: 1, lock_timeout_ms: 25)

      assert ["running", 4, 0, 0, 0, 1, nil, "lock_timeout", _updated] = rollout()
      send(holder.pid, :release)
      assert {:ok, _} = Task.await(holder)
      GenServer.stop(connection)

      assert {:ok, %{cursor: 4, retries: 1, inserted_partitions: 0, last_error: nil}} =
               Backfill.advance(context, batch_size: 1)
    end

    test "a statement-cancelled page rolls back its unit, records one retry, and restarts", %{
      context: context
    } do
      insert_run!("public", 1, "slow")
      attest!(context)
      assert {:ok, %{phase: :running}} = Backfill.advance(context)

      TestRepo.query!("""
      CREATE FUNCTION docket_test_slow_partition() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.2); RETURN NEW; END $$
      """)

      TestRepo.query!("""
      CREATE TRIGGER docket_test_slow_partition
      BEFORE INSERT ON docket_claim_partitions
      FOR EACH ROW EXECUTE FUNCTION docket_test_slow_partition()
      """)

      assert {:error, :backfill_timeout} =
               Backfill.advance(context, statement_timeout_ms: 50)

      assert ["running", 1, 0, 0, 0, 1, nil, "statement_timeout", _updated] = rollout()
      assert partition_keys() == []

      TestRepo.query!("DROP TRIGGER docket_test_slow_partition ON docket_claim_partitions")
      TestRepo.query!("DROP FUNCTION docket_test_slow_partition()")

      assert {:ok, %{cursor: 1, retries: 1, last_error: nil}} = Backfill.advance(context)
    end

    test "nonzero reconciliation persists a distinct snapshot and boundedly repairs it", %{
      context: context
    } do
      insert_run!("public", 1, "shared")
      insert_run!("public", 8, "shared")
      insert_run!("public", 20, "other")
      attest!(context)
      complete!(context, batch_size: 2)
      retries = Enum.at(rollout(), 5)

      TestRepo.query!("DELETE FROM docket_claim_partitions WHERE scope_key = 'shared'")

      assert {:ok,
              %{
                outcome: :repairing,
                phase: :reconciling,
                cursor: 0,
                missing_partition_count: 1,
                observed_missing_partitions: 1,
                retries: ^retries
              }} = Backfill.advance(context, batch_size: 1)

      assert ["reconciling", 20, 0, _batches, _rows, ^retries, 1, "missing_partitions", _] =
               rollout()

      assert {:ok, %{phase: :reconciling, cursor: 1, missing_partition_count: 1}} =
               Backfill.advance(context, batch_size: 1)

      advance_until_complete!(context, batch_size: 1)
      assert partition_keys() == ["other", "shared"]
      assert ["complete", 20, 20, _batches, _rows, ^retries, 0, nil, _] = rollout()
    end

    test "first reconciliation persists positive distinct evidence before repair", %{
      context: context
    } do
      insert_run!("public", 3, "first-gap")
      insert_run!("public", 9, "first-gap")
      attest!(context)

      assert {:ok, %{phase: :running, target_id: 9}} = Backfill.advance(context)
      assert {:ok, %{phase: :running, cursor: 9}} = Backfill.advance(context)
      assert {:ok, %{phase: :reconciling, cursor: 9}} = Backfill.advance(context)
      TestRepo.query!("DELETE FROM docket_claim_partitions WHERE scope_key = 'first-gap'")

      assert {:ok,
              %{
                phase: :reconciling,
                cursor: 0,
                missing_partition_count: 1,
                observed_missing_partitions: 1,
                retries: 0
              }} = Backfill.advance(context)

      assert ["reconciling", 9, 0, 1, 2, 0, 1, "missing_partitions", _] = rollout()

      assert {:ok, %{phase: :reconciling, cursor: 9, missing_partition_count: 1}} =
               Backfill.advance(context)

      assert {:ok, %{phase: :complete, missing_partition_count: 0, retries: 0}} =
               Backfill.advance(context)
    end

    test "gate, advisory runner, and rollout contention are exact and make no ledger progress", %{
      context: context
    } do
      attest!(context)
      baseline = rollout()

      with_held_sql("SELECT id FROM docket_claim_admission_gate WHERE id = 1 FOR UPDATE", fn ->
        assert {:error, {:lock_timeout, :gate}} =
                 Backfill.advance(context, lock_timeout_ms: 25)
      end)

      assert rollout() == baseline

      with_held_sql(
        "SELECT pg_advisory_xact_lock(hashtextextended('docket-claim-partition-backfill-v1:public', 0))",
        fn ->
          assert {:error, :backfill_running} = Backfill.advance(context, lock_timeout_ms: 25)
        end
      )

      assert rollout() == baseline

      with_held_sql("SELECT id FROM docket_claim_rollout WHERE id = 1 FOR UPDATE", fn ->
        assert {:error, {:lock_timeout, :rollout}} =
                 Backfill.advance(context, lock_timeout_ms: 25)
      end)

      assert rollout() == baseline

      with_held_sql("SELECT id FROM docket_claim_rollout WHERE id = 1 FOR UPDATE", fn ->
        assert {:error, :backfill_timeout} =
                 Backfill.advance(context,
                   lock_timeout_ms: 1_000,
                   statement_timeout_ms: 25
                 )
      end)

      assert rollout() == baseline
    end

    test "complete drift waits for DCKT-72 evidence before reopening repair", %{
      context: context
    } do
      insert_run!("public", 1, "ready-drift")
      attest!(context)
      complete!(context)
      retries = Enum.at(rollout(), 5)
      TestRepo.query!("DELETE FROM docket_claim_partitions WHERE scope_key = 'ready-drift'")
      TestRepo.query!("UPDATE docket_claim_admission_gate SET readiness = 'ready'")

      assert {:error, :prefix_ready} = Backfill.advance(context)

      assert ["complete", 1, 1, _batches, _rows, ^retries, 0, "prefix_ready", _] =
               rollout()

      TestRepo.transaction(fn ->
        TestRepo.query!("""
        UPDATE docket_claim_admission_gate
        SET readiness = 'not_ready', readiness_epoch = readiness_epoch + 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """)

        TestRepo.query!("""
        UPDATE docket_claim_rollout
        SET missing_partition_count = 1, updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """)
      end)

      assert ["complete", 1, 1, _batches, _rows, ^retries, 1, "prefix_ready", _] = rollout()

      assert {:ok,
              %{
                outcome: :repairing,
                phase: :reconciling,
                cursor: 0,
                missing_partition_count: 1,
                retries: ^retries
              }} = Backfill.advance(context)
    end

    test "option, transaction, and contention preconditions do not increment retries", %{
      context: context
    } do
      assert {:error, :invalid_backfill_options} = Backfill.advance(context, batch_size: 0)
      assert {:error, :invalid_backfill_options} = Backfill.advance(context, unknown: true)

      TestRepo.query!(
        """
        INSERT INTO docket_claim_assertions
          (assertion_id, assertion_kind, evidence_fingerprint, actor, source, event_id,
           expires_at, audit_id)
        VALUES
          ('00000000-0000-4000-8000-000000000067', 'old_binaries_absent', $1,
           'operator', 'test', 'wrong-kind', CURRENT_TIMESTAMP + interval '1 hour', 1)
        """,
        [@evidence]
      )

      TestRepo.query!("""
      UPDATE docket_claim_rollout
      SET dual_write_assertion_id = '00000000-0000-4000-8000-000000000067'
      """)

      assert {:error, :dual_write_unattested} = Backfill.advance(context)
      assert Enum.at(rollout(), 5) == 0

      assert {:ok, :checked} =
               Docket.Postgres.transaction(context, fn transaction_context ->
                 assert {:error, :transaction_context_forbidden} =
                          Backfill.advance(transaction_context)

                 assert {:error, :transaction_context_forbidden} =
                          Readiness.attest_dual_write(transaction_context, dual_opts("tx"))

                 {:ok, :checked}
               end)

      assert Enum.at(rollout(), 5) == 0
    end

    defp complete!(context, opts \\ []) do
      advance_until_complete!(context, opts)
    end

    defp advance_until_complete!(context, opts, remaining \\ 100)
    defp advance_until_complete!(_context, _opts, 0), do: flunk("backfill did not complete")

    defp advance_until_complete!(context, opts, remaining) do
      case Backfill.advance(context, opts) do
        {:ok, %{phase: :complete} = result} -> result
        {:ok, _result} -> advance_until_complete!(context, opts, remaining - 1)
        other -> flunk("backfill failed: #{inspect(other)}")
      end
    end

    defp attest!(context, event_id \\ "deploy") do
      assert {:ok, %{outcome: :applied}} =
               Readiness.attest_dual_write(context, dual_opts(event_id))
    end

    defp dual_opts(event_id, evidence \\ @evidence) do
      [
        evidence_fingerprint: evidence,
        actor: "operator",
        source: "test",
        event_id: event_id
      ]
    end

    defp insert_run!(prefix, id, tenant_id) do
      quoted = ~s("#{prefix}")
      graph_id = "graph-#{id}"
      graph_hash = "hash-#{id}"
      now = DateTime.utc_now()

      TestRepo.query!(
        """
        INSERT INTO #{quoted}.docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, $2, $3, $4, $5)
        """,
        [tenant_id, graph_id, graph_hash, <<0>>, now]
      )

      TestRepo.query!(
        """
        INSERT INTO #{quoted}.docket_runs
          (id, run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, wake_at, inserted_at, started_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, 'running', $6, 1, $7, $7, $7, $7)
        """,
        [id, "run-#{prefix}-#{id}", tenant_id, graph_id, graph_hash, <<0>>, now]
      )
    end

    defp dual_insert_run!(prefix, id, tenant_id) do
      quoted = ~s("#{prefix}")
      scope_key = tenant_id || ""

      TestRepo.transaction(fn ->
        TestRepo.query!(
          "INSERT INTO #{quoted}.docket_claim_partitions (scope_key) VALUES ($1) ON CONFLICT DO NOTHING",
          [scope_key]
        )

        insert_run!(prefix, id, tenant_id)
      end)
    end

    defp partition_keys(prefix \\ "public") do
      TestRepo.query!(
        ~s(SELECT scope_key FROM "#{prefix}".docket_claim_partitions ORDER BY scope_key)
      ).rows
      |> List.flatten()
    end

    defp rollout do
      [row] =
        TestRepo.query!("""
        SELECT backfill_phase, backfill_target_id, backfill_cursor,
               backfill_batches, backfill_rows, backfill_retries,
               missing_partition_count, backfill_last_error, updated_at
        FROM docket_claim_rollout WHERE id = 1
        """).rows

      row
    end

    defp gate_state do
      TestRepo.query!("""
      SELECT readiness, readiness_epoch, admission_mode, mode_epoch, updated_at
      FROM docket_claim_admission_gate WHERE id = 1
      """).rows
    end

    defp await_ungranted_transaction_lock!(remaining \\ 100)
    defp await_ungranted_transaction_lock!(0), do: flunk("backfill did not reach lock barrier")

    defp await_ungranted_transaction_lock!(remaining) do
      case TestRepo.query!("""
           SELECT count(*)
           FROM pg_locks
           WHERE locktype = 'transactionid' AND NOT granted
           """).rows do
        [[count]] when count > 0 ->
          :ok

        _rows ->
          Process.sleep(10)
          await_ungranted_transaction_lock!(remaining - 1)
      end
    end

    defp with_held_sql(sql, fun) do
      parent = self()
      {:ok, connection} = Postgrex.start_link(TestRepo.config())

      holder =
        Task.async(fn ->
          Postgrex.transaction(connection, fn conn ->
            Postgrex.query!(conn, sql, [])
            send(parent, :backfill_lock_held)
            receive do: (:release_backfill_lock -> :ok)
          end)
        end)

      assert_receive :backfill_lock_held

      try do
        fun.()
      after
        send(holder.pid, :release_backfill_lock)
        assert {:ok, _} = Task.await(holder)
        GenServer.stop(connection)
      end
    end
  end
end
