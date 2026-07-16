if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.MigrationTest do
    # Runs against a live Postgres; excluded by default. See test_helper.exs.
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.TestRepo

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

    defmodule InstallDocketReservedPrefix do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(prefix: "select")
      def down, do: Docket.Postgres.Migration.down(prefix: "select")
    end

    defmodule InstallDocketV1 do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(version: 1)
      def down, do: Docket.Postgres.Migration.down(version: 1)
    end

    defmodule UpgradeDocketV2 do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(version: 2)
      def down, do: Docket.Postgres.Migration.down(version: 2)
    end

    defmodule RepeatDocketV2 do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up(version: 2)
      def down, do: :ok
    end

    defmodule DestructiveDocket do
      use Ecto.Migration

      def up, do: :ok

      def down do
        Docket.Postgres.Migration.destructive_down(
          stopped_fleet: true,
          audit_exported: true,
          acknowledge_receipt_loss: true,
          acknowledge_partition_loss: true
        )
      end
    end

    defmodule DestructiveDocketPrefixed do
      use Ecto.Migration

      def up, do: :ok

      def down do
        Docket.Postgres.Migration.destructive_down(
          prefix: "docket_private",
          stopped_fleet: true,
          audit_exported: true,
          acknowledge_receipt_loss: true,
          acknowledge_partition_loss: true
        )
      end
    end

    defmodule DestructiveDocketReservedPrefix do
      use Ecto.Migration

      def up, do: :ok

      def down do
        Docket.Postgres.Migration.destructive_down(
          prefix: "select",
          stopped_fleet: true,
          audit_exported: true,
          acknowledge_receipt_loss: true,
          acknowledge_partition_loss: true
        )
      end
    end

    defmodule DestructiveUpgradeDocketV2 do
      use Ecto.Migration

      def up, do: :ok

      def down do
        Docket.Postgres.Migration.destructive_down(
          version: 2,
          stopped_fleet: true,
          audit_exported: true,
          acknowledge_receipt_loss: true,
          acknowledge_partition_loss: true
        )
      end
    end

    @migration_version 20_260_709_000_001
    @prefixed_migration_version 20_260_709_000_007
    @reserved_prefix_migration_version 20_260_709_000_008
    @v1_migration_version 20_260_709_000_004
    @v2_upgrade_migration_version 20_260_709_000_005
    @repeat_v2_migration_version 20_260_709_000_006

    @v1_tables ~w(docket_events docket_graph_versions docket_runs)
    @v2_tables ~w(
      docket_claim_admission_gate
      docket_claim_assertions
      docket_claim_audit_exports
      docket_claim_capabilities
      docket_claim_partitions
      docket_claim_policy
      docket_claim_policy_events
      docket_claim_policy_holds
      docket_claim_policy_receipts
      docket_claim_rollout
    )
    @tables Enum.sort(@v1_tables ++ @v2_tables)

    # {column, canonical PostgreSQL type, NOT NULL?, normalized default, identity}
    @v2_column_catalog %{
      "docket_claim_policy" => [
        {"id", "smallint", true, "1", nil},
        {"preferred_active", "integer", false, nil, nil},
        {"max_active", "integer", false, nil, nil},
        {"weight", "integer", false, nil, nil},
        {"borrowing", "boolean", false, nil, nil},
        {"policy_version", "bigint", true, "0", nil},
        {"initialized_at", "timestamp with time zone", false, nil, nil},
        {"updated_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil}
      ],
      "docket_claim_partitions" => [
        {"scope_key", "text", true, nil, nil},
        {"preferred_active", "integer", false, nil, nil},
        {"max_active", "integer", false, nil, nil},
        {"weight", "integer", false, nil, nil},
        {"borrowing", "boolean", false, nil, nil},
        {"admin_state", "character varying(16)", true, "'running'", nil},
        {"partition_version", "bigint", true, "0", nil},
        {"admission_epoch", "bigint", true, "0", nil},
        {"inserted_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil},
        {"updated_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil}
      ],
      "docket_claim_policy_receipts" => [
        {"source", "character varying(64)", true, nil, nil},
        {"event_id", "character varying(255)", true, nil, nil},
        {"request_fingerprint", "bytea", true, nil, nil},
        {"target_kind", "character varying(16)", true, nil, nil},
        {"target_fingerprints", "bytea[]", true, nil, nil},
        {"outcome", "character varying(16)", true, nil, nil},
        {"previous_versions", "bigint[]", true, nil, nil},
        {"versions", "bigint[]", true, nil, nil},
        {"audit_id", "bigint", true, nil, nil},
        {"created_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil}
      ],
      "docket_claim_policy_events" => [
        {"audit_id", "bigint", true, nil, :always},
        {"target_kind", "character varying(16)", true, nil, nil},
        {"target_keys", "text[]", true, nil, nil},
        {"operation", "character varying(32)", true, nil, nil},
        {"actor", "character varying(255)", true, nil, nil},
        {"source", "character varying(64)", true, nil, nil},
        {"event_id", "character varying(255)", true, nil, nil},
        {"request_fingerprint", "bytea", true, nil, nil},
        {"before_value", "jsonb", true, nil, nil},
        {"after_value", "jsonb", true, nil, nil},
        {"before_versions", "bigint[]", true, nil, nil},
        {"after_versions", "bigint[]", true, nil, nil},
        {"mode_epoch", "bigint", false, nil, nil},
        {"occurred_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil}
      ],
      "docket_claim_policy_holds" => [
        {"hold_id", "uuid", true, nil, nil},
        {"first_audit_id", "bigint", true, nil, nil},
        {"last_audit_id", "bigint", true, nil, nil},
        {"reason", "character varying(512)", true, nil, nil},
        {"actor", "character varying(255)", true, nil, nil},
        {"source", "character varying(64)", true, nil, nil},
        {"event_id", "character varying(255)", true, nil, nil},
        {"created_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil}
      ],
      "docket_claim_audit_exports" => [
        {"export_id", "uuid", true, nil, nil},
        {"through_audit_id", "bigint", true, nil, nil},
        {"location_fingerprint", "bytea", true, nil, nil},
        {"actor", "character varying(255)", true, nil, nil},
        {"source", "character varying(64)", true, nil, nil},
        {"event_id", "character varying(255)", true, nil, nil},
        {"completed_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil}
      ],
      "docket_claim_assertions" => [
        {"assertion_id", "uuid", true, nil, nil},
        {"assertion_kind", "character varying(32)", true, nil, nil},
        {"evidence_fingerprint", "bytea", true, nil, nil},
        {"actor", "character varying(255)", true, nil, nil},
        {"source", "character varying(64)", true, nil, nil},
        {"event_id", "character varying(255)", true, nil, nil},
        {"asserted_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil},
        {"expires_at", "timestamp with time zone", false, nil, nil},
        {"audit_id", "bigint", true, nil, nil}
      ],
      "docket_claim_rollout" => [
        {"id", "smallint", true, "1", nil},
        {"schema_generation", "integer", true, "2", nil},
        {"dual_write_assertion_id", "uuid", false, nil, nil},
        {"backfill_phase", "character varying(24)", true, "'not_started'", nil},
        {"backfill_cursor", "bigint", false, nil, nil},
        {"backfill_batches", "bigint", true, "0", nil},
        {"backfill_rows", "bigint", true, "0", nil},
        {"backfill_completed_at", "timestamp with time zone", false, nil, nil},
        {"backfill_last_error", "character varying(512)", false, nil, nil},
        {"ready_index_valid", "boolean", true, "false", nil},
        {"live_index_valid", "boolean", true, "false", nil},
        {"fk_disposition", "character varying(16)", true, "'absent'", nil},
        {"missing_partition_count", "bigint", false, nil, nil},
        {"verified_default_fingerprint", "bytea", false, nil, nil},
        {"verified_at", "timestamp with time zone", false, nil, nil},
        {"updated_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil}
      ],
      "docket_claim_admission_gate" => [
        {"id", "smallint", true, "1", nil},
        {"readiness", "character varying(16)", true, "'not_ready'", nil},
        {"readiness_epoch", "bigint", true, "0", nil},
        {"admission_mode", "character varying(16)", true, "'legacy'", nil},
        {"mode_epoch", "bigint", true, "0", nil},
        {"required_function_contract", "integer", true, "1", nil},
        {"updated_at", "timestamp with time zone", true, "CURRENT_TIMESTAMP", nil}
      ],
      "docket_claim_capabilities" => [
        {"instance_id", "uuid", true, nil, nil},
        {"binary_fingerprint", "bytea", true, nil, nil},
        {"writer_contract", "integer", true, nil, nil},
        {"gate_contract", "integer", true, nil, nil},
        {"function_contract", "integer", true, nil, nil},
        {"last_seen_at", "timestamp with time zone", true, nil, nil},
        {"expires_at", "timestamp with time zone", true, nil, nil}
      ]
    }

    # {table, index, primary?, unique?, ordered columns}
    @v2_index_catalog [
      {"docket_claim_admission_gate", "docket_claim_admission_gate_pkey", true, true, ["id"]},
      {"docket_claim_assertions", "docket_claim_assertions_pkey", true, true, ["assertion_id"]},
      {"docket_claim_assertions", "docket_claim_assertions_source_event_index", false, true,
       ["source", "event_id"]},
      {"docket_claim_audit_exports", "docket_claim_audit_exports_pkey", true, true,
       ["export_id"]},
      {"docket_claim_audit_exports", "docket_claim_audit_exports_source_event_index", false, true,
       ["source", "event_id"]},
      {"docket_claim_capabilities", "docket_claim_capabilities_pkey", true, true,
       ["instance_id"]},
      {"docket_claim_partitions", "docket_claim_partitions_pkey", true, true, ["scope_key"]},
      {"docket_claim_policy", "docket_claim_policy_pkey", true, true, ["id"]},
      {"docket_claim_policy_events", "docket_claim_policy_events_pkey", true, true, ["audit_id"]},
      {"docket_claim_policy_events", "docket_claim_policy_events_source_event_index", false, true,
       ["source", "event_id"]},
      {"docket_claim_policy_holds", "docket_claim_policy_holds_pkey", true, true, ["hold_id"]},
      {"docket_claim_policy_holds", "docket_claim_policy_holds_source_event_index", false, true,
       ["source", "event_id"]},
      {"docket_claim_policy_receipts", "docket_claim_policy_receipts_pkey", true, true,
       ["source", "event_id"]},
      {"docket_claim_rollout", "docket_claim_rollout_pkey", true, true, ["id"]}
    ]

    @run_columns ~w(
      id run_id tenant_id scope_key graph_id graph_hash status step state
      checkpoint_seq latest_checkpoint_type claim_token
      claimed_at wake_at claim_attempts claim_abandons poisoned_at poison_reason
      inserted_at started_at updated_at finished_at
    )

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok
    end

    test "V02 installs, removes, and reinstalls the final schema on a fresh Postgres" do
      install!()

      assert tables("public") == @tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 2
      assert columns("docket_runs") == Enum.sort(@run_columns)

      assert column_type("docket_graph_versions", "graph") == "bytea"
      assert column_default("docket_graph_versions", "inserted_at") =~ "clock_timestamp()"

      assert column_type("docket_runs", "state") == "bytea"

      assert column_type("docket_runs", "poison_reason") == "text"
      assert column_type("docket_events", "payload") == "bytea"
      assert column_type("docket_events", "metadata") == "bytea"

      assert nullable?("docket_runs", "tenant_id")
      refute nullable?("docket_runs", "scope_key")
      assert nullable?("docket_graph_versions", "tenant_id")
      refute nullable?("docket_graph_versions", "scope_key")
      refute nullable?("docket_runs", "started_at")

      indexes = indexes("docket_runs")

      assert indexes["docket_runs_run_id_index"] =~ "CREATE UNIQUE INDEX"

      assert indexes["docket_runs_list_order_index"] =~
               "(started_at DESC, run_id DESC)"

      assert indexes["docket_runs_tenant_list_order_index"] =~
               "(tenant_id, started_at DESC, run_id DESC)"

      assert indexes["docket_runs_tenant_id_status_index"] =~
               "WHERE (tenant_id IS NOT NULL)"

      assert indexes["docket_runs_tenant_id_graph_id_status_index"] =~
               "WHERE (tenant_id IS NOT NULL)"

      ready = indexes["docket_runs_wake_at_id_index"]

      assert ready =~ "btree (wake_at, id)"
      assert ready =~ "status = 'running'"
      assert ready =~ "poisoned_at IS NULL"
      assert ready =~ "claim_token IS NULL"
      assert ready =~ "wake_at IS NOT NULL"

      expired = indexes["docket_runs_claimed_at_id_index"]

      assert expired =~ "btree (claimed_at, id)"
      assert expired =~ "status = 'running'"
      assert expired =~ "poisoned_at IS NULL"
      assert expired =~ "claim_token IS NOT NULL"

      assert indexes["docket_runs_poisoned_at_index"] =~ "WHERE (poisoned_at IS NOT NULL)"

      assert indexes["docket_runs_status_updated_at_index"] =~ "(status, updated_at)"

      terminal_retention = indexes["docket_runs_updated_at_id_index"]
      assert terminal_retention =~ "btree (updated_at, id)"
      assert terminal_retention =~ "status = ANY"

      assert indexes["docket_runs_graph_id_graph_hash_index"] =~ "(graph_id, graph_hash)"

      graph_indexes = indexes("docket_graph_versions")

      assert graph_indexes["docket_graph_versions_scope_graph_index"] =~
               "CREATE UNIQUE INDEX"

      revision_order =
        graph_indexes["docket_graph_versions_scope_revision_order_index"]

      assert revision_order =~ "(scope_key, graph_id, inserted_at DESC, graph_hash DESC)"
      refute Map.has_key?(graph_indexes, "docket_graph_versions_graph_id_graph_hash_index")
      refute Map.has_key?(graph_indexes, "docket_graph_versions_revision_order_index")

      assert indexes("docket_events")["docket_events_run_id_seq_index"] =~
               "CREATE UNIQUE INDEX"

      assert indexes("docket_events")["docket_events_inserted_at_id_index"] =~
               "(inserted_at, id)"

      assert_row_round_trip()

      assert_routine_down_refused(@migration_version, InstallDocket)

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @migration_version,
                 DestructiveDocket,
                 log: false
               )

      assert tables("public") == []
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 0

      assert :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      assert tables("public") == @tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 2
    end

    test "migrates up and down inside a dedicated schema prefix" do
      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @prefixed_migration_version,
                 InstallDocketPrefixed,
                 log: false
               )

      assert tables("docket_private") == @tables
      assert tables("public") == []

      assert Docket.Postgres.Migration.migrated_version(
               repo: TestRepo,
               prefix: "docket_private"
             ) == 2

      assert {:ok, _} = insert_run([], "docket_private")

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @prefixed_migration_version,
                 DestructiveDocketPrefixed,
                 log: false
               )

      assert tables("docket_private") == []
    end

    test "quotes a valid prefix that is also a Postgres keyword" do
      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @reserved_prefix_migration_version,
                 InstallDocketReservedPrefix,
                 log: false
               )

      assert tables("select") == @tables

      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo, prefix: "select") == 2

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @reserved_prefix_migration_version,
                 DestructiveDocketReservedPrefix,
                 log: false
               )

      assert tables("select") == []
    end

    test "nil ClaimPolicy prefix pins one physical schema at context construction" do
      install!()

      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @prefixed_migration_version,
                 InstallDocketPrefixed,
                 log: false
               )

      TestRepo.query!("""
      INSERT INTO docket_private.docket_claim_partitions (scope_key) VALUES ('private-only')
      """)

      assert {:ok, :ok} =
               TestRepo.transaction(fn ->
                 TestRepo.query!("SET LOCAL search_path TO docket_private, public")

                 assert TestRepo.query!("SELECT current_schema()").rows == [["docket_private"]]

                 context = Docket.Postgres.context(repo: TestRepo)

                 assert %{prefix: "docket_private", claim_policy: claim_policy} = context

                 assert %{prefix: "docket_private", identifiers: identifiers} =
                          Docket.Postgres.ClaimPolicy.plan_context!(context)

                 assert identifiers.runs == ~s("docket_private"."docket_runs")

                 assert identifiers.claim_partitions ==
                          ~s("docket_private"."docket_claim_partitions")

                 assert TestRepo.query!("SELECT scope_key FROM #{identifiers.claim_partitions}").rows ==
                          [["private-only"]]

                 TestRepo.query!("SET LOCAL search_path TO public")

                 plan =
                   Docket.Postgres.ClaimPolicy.build_plan(
                     claim_policy,
                     context,
                     Docket.Postgres.ClaimPolicy.effective_policy!(%{
                       now: DateTime.utc_now(),
                       limit: 1,
                       orphan_ttl_ms: 1_000,
                       max_claim_attempts: 3,
                       preference: :ready
                     })
                   )

                 assert plan.statement =~ ~s("docket_private"."docket_runs")
                 refute plan.statement =~ ~s(FROM "public"."docket_runs")

                 :ok
               end)
    end

    test "nil ClaimPolicy prefix skips an empty first search_path schema" do
      install!()
      TestRepo.query!("CREATE SCHEMA docket_empty")

      assert {:ok, :ok} =
               TestRepo.transaction(fn ->
                 TestRepo.query!("SET LOCAL search_path TO docket_empty, public")

                 assert TestRepo.query!("SELECT current_schema()").rows == [["docket_empty"]]

                 context = Docket.Postgres.context(repo: TestRepo)

                 assert %{prefix: "public", claim_policy: claim_policy} = context

                 assert %{prefix: "public", identifiers: identifiers} =
                          Docket.Postgres.ClaimPolicy.plan_context!(context)

                 assert identifiers.runs == ~s("public"."docket_runs")

                 plan =
                   Docket.Postgres.ClaimPolicy.build_plan(
                     claim_policy,
                     context,
                     Docket.Postgres.ClaimPolicy.effective_policy!(%{
                       now: DateTime.utc_now(),
                       limit: 1,
                       orphan_ttl_ms: 1_000,
                       max_claim_attempts: 3,
                       preference: :ready
                     })
                   )

                 assert plan.statement =~ ~s("public"."docket_runs")
                 refute plan.statement =~ ~s("docket_empty"."docket_runs")
                 :ok
               end)
    end

    test "a populated v1 upgrade is logically preserved and matches a fresh v2 prefix" do
      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @v1_migration_version,
                 InstallDocketV1,
                 log: false
               )

      assert tables("public") == @v1_tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 1

      assert {:ok, %{rows: [[run_id]]}} = insert_run(tenant_id: "upgrade-tenant")
      assert {:ok, _event} = insert_event(run_id, 1)
      v1_catalog = v1_schema_signature("public")

      before =
        TestRepo.query!(
          "SELECT run_id, tenant_id, scope_key, state FROM docket_runs WHERE run_id = $1",
          [run_id]
        ).rows

      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @v2_upgrade_migration_version,
                 UpgradeDocketV2,
                 log: false
               )

      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 2
      assert TestRepo.query!("SELECT count(*) FROM docket_claim_partitions").rows == [[0]]
      assert v1_schema_signature("public") == v1_catalog

      assert TestRepo.query!(
               "SELECT run_id, tenant_id, scope_key, state FROM docket_runs WHERE run_id = $1",
               [run_id]
             ).rows == before

      assert_initial_v2_state("public")

      assert_check_violation("docket_claim_policy_singleton_check", fn ->
        TestRepo.query("INSERT INTO docket_claim_policy (id) VALUES (2)")
      end)

      assert_check_violation("docket_claim_rollout_singleton_check", fn ->
        TestRepo.query("INSERT INTO docket_claim_rollout (id) VALUES (2)")
      end)

      assert_check_violation("docket_claim_admission_gate_singleton_check", fn ->
        TestRepo.query("INSERT INTO docket_claim_admission_gate (id) VALUES (2)")
      end)

      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @prefixed_migration_version,
                 InstallDocketPrefixed,
                 log: false
               )

      assert_initial_v2_state("docket_private")
      assert v2_schema_signature("public") == v2_schema_signature("docket_private")
    end

    test "a malformed partial v2 table collision aborts the whole upgrade" do
      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @v1_migration_version,
                 InstallDocketV1,
                 log: false
               )

      TestRepo.query!("CREATE TABLE docket_claim_partitions (scope_key text)")

      error =
        assert_raise Postgrex.Error, fn ->
          Ecto.Migrator.up(
            TestRepo,
            @v2_upgrade_migration_version,
            UpgradeDocketV2,
            log: false
          )
        end

      assert error.postgres.code == :duplicate_table
      assert tables("public") == Enum.sort(@v1_tables ++ ["docket_claim_partitions"])
      refute "docket_claim_policy" in tables("public")
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 1
    end

    test "the pinned v1-to-v2 down removes only V02 and preserves populated V01" do
      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @v1_migration_version,
                 InstallDocketV1,
                 log: false
               )

      assert {:ok, %{rows: [[run_id]]}} = insert_run(tenant_id: "rollback-tenant")

      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @v2_upgrade_migration_version,
                 UpgradeDocketV2,
                 log: false
               )

      assert_routine_down_refused(@v2_upgrade_migration_version, UpgradeDocketV2)

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @v2_upgrade_migration_version,
                 DestructiveUpgradeDocketV2,
                 log: false
               )

      assert tables("public") == @v1_tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 1

      assert TestRepo.query!("SELECT tenant_id FROM docket_runs WHERE run_id = $1", [run_id]).rows ==
               [["rollback-tenant"]]
    end

    test "v2 reruns are idempotent and preserve singleton timestamps and user rows" do
      install!()

      TestRepo.query!("INSERT INTO docket_claim_partitions (scope_key) VALUES ('kept')")
      singleton_state = singleton_state("public")
      signature = v2_schema_signature("public")

      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @repeat_v2_migration_version,
                 RepeatDocketV2,
                 log: false
               )

      assert singleton_state("public") == singleton_state
      assert v2_schema_signature("public") == signature
      assert TestRepo.query!("SELECT scope_key FROM docket_claim_partitions").rows == [["kept"]]
    end

    test "v2 singleton, tuple, state, receipt, and audit constraints reject raw violations" do
      install!()

      assert_initial_v2_state("public")

      assert_check_violation("docket_claim_policy_tuple_check", fn ->
        TestRepo.query("UPDATE docket_claim_policy SET max_active = 2 WHERE id = 1")
      end)

      assert %{num_rows: 1} =
               TestRepo.query!("""
               UPDATE docket_claim_policy
               SET preferred_active = 1, max_active = 2, weight = 1, borrowing = false,
                   policy_version = 1, initialized_at = CURRENT_TIMESTAMP
               WHERE id = 1
               """)

      assert_check_violation("docket_claim_policy_tuple_check", fn ->
        TestRepo.query("UPDATE docket_claim_policy SET preferred_active = 3 WHERE id = 1")
      end)

      assert_check_violation("docket_claim_partitions_tuple_check", fn ->
        TestRepo.query("""
        INSERT INTO docket_claim_partitions (scope_key, max_active) VALUES ('partial', 1)
        """)
      end)

      assert_check_violation("docket_claim_partitions_admin_state_check", fn ->
        TestRepo.query("""
        INSERT INTO docket_claim_partitions (scope_key, admin_state) VALUES ('bad-state', 'pause')
        """)
      end)

      assert_check_violation("docket_claim_partitions_version_check", fn ->
        TestRepo.query("""
        INSERT INTO docket_claim_partitions (scope_key, partition_version)
        VALUES ('bad-version', -1)
        """)
      end)

      fingerprint = :binary.copy(<<7>>, 32)

      assert_check_violation("docket_claim_policy_receipts_target_fingerprints_check", fn ->
        insert_receipt("bad-fingerprint", [<<1>>], [0], [1], fingerprint)
      end)

      assert_check_violation("docket_claim_policy_receipts_target_fingerprints_check", fn ->
        insert_receipt("null-fingerprint", [nil], [0], [1], fingerprint)
      end)

      assert_check_violation("docket_claim_policy_receipts_target_fingerprints_check", fn ->
        insert_receipt(
          "compensating-fingerprints",
          [:binary.copy(<<1>>, 31), :binary.copy(<<2>>, 33)],
          [0, 0],
          [1, 1],
          fingerprint
        )
      end)

      assert_check_violation("docket_claim_policy_receipts_target_fingerprints_check", fn ->
        insert_receipt(
          "null-compensating-fingerprints",
          [nil, :binary.copy(<<2>>, 64)],
          [0, 0],
          [1, 1],
          fingerprint
        )
      end)

      assert_check_violation("docket_claim_policy_receipts_cardinality_check", fn ->
        insert_receipt("bad-cardinality", [fingerprint], [0, 1], [1], fingerprint)
      end)

      assert_check_violation("docket_claim_policy_receipts_versions_check", fn ->
        insert_receipt("bad-version-array", [fingerprint], [-1], [0], fingerprint)
      end)

      assert_check_violation("docket_claim_policy_receipts_versions_check", fn ->
        insert_receipt("null-version-array", [fingerprint], [nil], [0], fingerprint)
      end)

      assert_check_violation("docket_claim_policy_receipts_request_fingerprint_check", fn ->
        insert_receipt("bad-request-hash", [fingerprint], [0], [1], <<1>>)
      end)

      assert {:ok, %{num_rows: 1}} =
               insert_receipt("durable-event", [fingerprint], [0], [1], fingerprint)

      assert_check_violation("docket_claim_policy_receipts_target_kind_check", fn ->
        TestRepo.query("""
        UPDATE docket_claim_policy_receipts SET target_kind = 'unknown'
        WHERE source = 'test' AND event_id = 'durable-event'
        """)
      end)

      TestRepo.query!("UPDATE docket_claim_policy SET policy_version = 2 WHERE id = 1")

      assert_unique_violation("docket_claim_policy_receipts_pkey", fn ->
        insert_receipt("durable-event", [fingerprint], [1], [2], fingerprint)
      end)

      assert {:ok, %{num_rows: 1}} = insert_policy_event("audit-event", fingerprint)
      TestRepo.query!("UPDATE docket_claim_policy SET policy_version = 3 WHERE id = 1")

      assert_unique_violation("docket_claim_policy_events_source_event_index", fn ->
        insert_policy_event("audit-event", fingerprint)
      end)

      assert_check_violation("docket_claim_policy_events_fingerprint_check", fn ->
        TestRepo.query("""
        UPDATE docket_claim_policy_events SET request_fingerprint = '\\x01'
        WHERE source = 'test' AND event_id = 'audit-event'
        """)
      end)

      assert_check_violation("docket_claim_policy_events_cardinality_check", fn ->
        TestRepo.query("""
        UPDATE docket_claim_policy_events SET before_versions = ARRAY[]::bigint[]
        WHERE source = 'test' AND event_id = 'audit-event'
        """)
      end)

      assert {:ok, %{num_rows: 1}} = insert_hold("hold-event", 1, 2)

      assert_unique_violation("docket_claim_policy_holds_source_event_index", fn ->
        insert_hold("hold-event", 2, 3)
      end)

      assert_check_violation("docket_claim_policy_holds_range_check", fn ->
        insert_hold("bad-hold-range", 2, 1)
      end)

      assert {:ok, %{num_rows: 1}} = insert_audit_export("export-event", 1, fingerprint)

      assert_unique_violation("docket_claim_audit_exports_source_event_index", fn ->
        insert_audit_export("export-event", 2, fingerprint)
      end)

      assert_check_violation("docket_claim_audit_exports_audit_id_check", fn ->
        insert_audit_export("bad-export-watermark", 0, fingerprint)
      end)

      assert_check_violation("docket_claim_audit_exports_fingerprint_check", fn ->
        insert_audit_export("bad-export-fingerprint", 1, <<1>>)
      end)

      assert {:ok, %{num_rows: 1}} =
               insert_assertion("dual_write", nil, fingerprint, "assertion-event")

      assert_unique_violation("docket_claim_assertions_source_event_index", fn ->
        insert_assertion("dual_write", nil, fingerprint, "assertion-event")
      end)

      assert_check_violation("docket_claim_assertions_expiry_check", fn ->
        insert_assertion("old_binaries_absent", nil, fingerprint)
      end)

      assert_check_violation("docket_claim_assertions_fingerprint_check", fn ->
        insert_assertion("dual_write", nil, <<1>>)
      end)

      assertion_id = Ecto.UUID.dump!(Ecto.UUID.generate())

      assert {:ok, %{num_rows: 1}} =
               TestRepo.query(
                 """
                 INSERT INTO docket_claim_assertions
                   (assertion_id, assertion_kind, evidence_fingerprint, actor, source,
                    event_id, audit_id)
                 VALUES ($1, 'dual_write', $2, 'tester', 'test', 'rollout-assertion', 1)
                 """,
                 [assertion_id, fingerprint]
               )

      TestRepo.query!(
        "UPDATE docket_claim_rollout SET dual_write_assertion_id = $1 WHERE id = 1",
        [assertion_id]
      )

      assert_foreign_key_violation("docket_claim_rollout_dual_write_assertion_fkey", fn ->
        TestRepo.query("DELETE FROM docket_claim_assertions WHERE assertion_id = $1", [
          assertion_id
        ])
      end)

      assert_foreign_key_violation("docket_claim_rollout_dual_write_assertion_fkey", fn ->
        TestRepo.query(
          "UPDATE docket_claim_assertions SET assertion_id = gen_random_uuid() " <>
            "WHERE assertion_id = $1",
          [assertion_id]
        )
      end)

      assert_foreign_key_violation("docket_claim_rollout_dual_write_assertion_fkey", fn ->
        TestRepo.query(
          "UPDATE docket_claim_rollout SET dual_write_assertion_id = gen_random_uuid()"
        )
      end)

      assert_check_violation("docket_claim_admission_gate_readiness_epoch_check", fn ->
        TestRepo.query("UPDATE docket_claim_admission_gate SET readiness_epoch = -1")
      end)

      assert_check_violation("docket_claim_admission_gate_mode_check", fn ->
        TestRepo.query("UPDATE docket_claim_admission_gate SET admission_mode = 'mixed'")
      end)

      assert_check_violation("docket_claim_rollout_generation_check", fn ->
        TestRepo.query("UPDATE docket_claim_rollout SET schema_generation = 3")
      end)

      assert_check_violation("docket_claim_rollout_fk_disposition_check", fn ->
        TestRepo.query("UPDATE docket_claim_rollout SET fk_disposition = 'waived'")
      end)

      assert_check_violation("docket_claim_rollout_fingerprint_check", fn ->
        TestRepo.query("UPDATE docket_claim_rollout SET verified_default_fingerprint = $1", [
          <<1>>
        ])
      end)

      assert_check_violation("docket_claim_capabilities_expiry_check", fn ->
        TestRepo.query(
          """
          INSERT INTO docket_claim_capabilities
            (instance_id, binary_fingerprint, writer_contract, gate_contract,
             function_contract, last_seen_at, expires_at)
          VALUES (gen_random_uuid(), $1, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          """,
          [fingerprint]
        )
      end)

      TestRepo.query!("INSERT INTO docket_claim_partitions (scope_key) VALUES ('defaults')")

      assert TestRepo.query!("""
             SELECT admin_state, partition_version, admission_epoch,
                    inserted_at IS NOT NULL, updated_at IS NOT NULL
             FROM docket_claim_partitions WHERE scope_key = 'defaults'
             """).rows == [["running", 0, 0, true, true]]
    end

    test "routine down always refuses v2 and destructive teardown enforces live guards" do
      install!()

      fingerprint = :binary.copy(<<9>>, 32)

      assert {:ok, %{num_rows: 1}} =
               insert_receipt("down-guard", [fingerprint], [0], [1], fingerprint)

      assert {:ok, %{rows: [[_run_id]]}} = insert_run([])
      TestRepo.query!("INSERT INTO docket_claim_partitions (scope_key) VALUES ('')")

      assert_routine_down_refused(@migration_version, InstallDocket)
      assert tables("public") == @tables

      TestRepo.query!("UPDATE docket_claim_admission_gate SET admission_mode = 'tenant_fair'")
      assert_destructive_down_refused("admission mode/readiness")
      assert tables("public") == @tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 2

      TestRepo.query!("""
      UPDATE docket_claim_admission_gate
      SET admission_mode = 'legacy', readiness = 'ready'
      """)

      assert_destructive_down_refused("admission mode/readiness")
      assert tables("public") == @tables

      TestRepo.query!("UPDATE docket_claim_admission_gate SET readiness = 'not_ready'")

      assert_destructive_down_refused("claim-policy receipts are retained")
      assert tables("public") == @tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 2

      TestRepo.query!("DELETE FROM docket_claim_policy_receipts")

      assert_destructive_down_refused("runs reference claim partitions")
      assert tables("public") == @tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 2

      TestRepo.query!("DELETE FROM docket_runs WHERE scope_key = ''")

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @migration_version,
                 DestructiveDocket,
                 log: false
               )

      assert tables("public") == []
    end

    test "destructive teardown requires every explicit operator acknowledgement" do
      assert_raise ArgumentError, ~r/:destructive is an internal migration marker/, fn ->
        Docket.Postgres.Migration.down(destructive: true)
      end

      for missing <- [
            :stopped_fleet,
            :audit_exported,
            :acknowledge_receipt_loss,
            :acknowledge_partition_loss
          ] do
        opts =
          [
            stopped_fleet: true,
            audit_exported: true,
            acknowledge_receipt_loss: true,
            acknowledge_partition_loss: true
          ]
          |> Keyword.delete(missing)

        assert_raise ArgumentError, ~r/#{missing}/, fn ->
          Docket.Postgres.Migration.destructive_down(opts)
        end
      end
    end

    test "destructive teardown requires a completed export covering retained audit" do
      install!()
      fingerprint = :binary.copy(<<8>>, 32)
      assert {:ok, %{rows: [[audit_id]]}} = insert_policy_event("export-required", fingerprint)

      assert_destructive_down_refused("retained audit is not fully exported")
      assert tables("public") == @tables

      TestRepo.query!(
        """
        INSERT INTO docket_claim_audit_exports
          (export_id, through_audit_id, location_fingerprint, actor, source, event_id)
        VALUES (gen_random_uuid(), $1, $2, 'operator', 'test', 'completed-export')
        """,
        [audit_id, fingerprint]
      )

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @migration_version,
                 DestructiveDocket,
                 log: false
               )

      assert tables("public") == []
    end

    test "v2 down guards only the requested physical prefix" do
      install!()

      assert :ok =
               Ecto.Migrator.up(
                 TestRepo,
                 @prefixed_migration_version,
                 InstallDocketPrefixed,
                 log: false
               )

      TestRepo.query!("""
      UPDATE docket_private.docket_claim_admission_gate SET readiness = 'ready'
      """)

      assert :ok =
               Ecto.Migrator.down(
                 TestRepo,
                 @migration_version,
                 DestructiveDocket,
                 log: false
               )

      assert tables("public") == []
      assert tables("docket_private") == @tables

      error =
        assert_raise Postgrex.Error, fn ->
          Ecto.Migrator.down(
            TestRepo,
            @prefixed_migration_version,
            DestructiveDocketPrefixed,
            log: false
          )
        end

      assert Exception.message(error) =~ "admission mode/readiness"
      assert tables("docket_private") == @tables
    end

    test "v2 down waits out a concurrent gate writer and checks its committed state" do
      install!()
      parent = self()
      {:ok, writer_connection} = Postgrex.start_link(TestRepo.config())

      writer =
        Task.async(fn ->
          Postgrex.transaction(writer_connection, fn connection ->
            Postgrex.query!(
              connection,
              """
              UPDATE docket_claim_admission_gate SET admission_mode = 'tenant_fair'
              """,
              []
            )

            send(parent, :gate_write_open)

            receive do
              :prove_down_waiting ->
                :ok = await_ungranted_down_lock(connection, 100)
                send(parent, :down_waiting_on_gate_lock)

                receive do
                  :commit_gate_write -> :ok
                end
            end
          end)
        end)

      assert_receive :gate_write_open

      down =
        Task.async(fn ->
          try do
            Ecto.Migrator.down(TestRepo, @migration_version, DestructiveDocket, log: false)
          rescue
            error in Postgrex.Error -> {:error, error}
          end
        end)

      send(writer.pid, :prove_down_waiting)
      assert_receive :down_waiting_on_gate_lock, 2_000
      assert Task.yield(down, 0) == nil
      send(writer.pid, :commit_gate_write)
      assert {:ok, :ok} = Task.await(writer)
      GenServer.stop(writer_connection)

      assert {:error, error} = Task.await(down)
      assert Exception.message(error) =~ "admission mode/readiness"
      assert tables("public") == @tables
      assert Docket.Postgres.Migration.migrated_version(repo: TestRepo) == 2
    end

    test "accepts every valid lifecycle row shape" do
      install!()

      now = DateTime.utc_now()

      valid_shapes = [
        ready_running: [],
        claimed_running: [wake_at: nil, claim_token: uuid(), claimed_at: now],
        poisoned_running: [
          wake_at: nil,
          poisoned_at: now,
          poison_reason: "max_claim_attempts_exceeded"
        ],
        waiting: [status: "waiting", wake_at: nil],
        done: [status: "done", wake_at: nil, finished_at: now],
        failed: [status: "failed", wake_at: nil, finished_at: now],
        cancelled: [status: "cancelled", wake_at: nil, finished_at: now]
      ]

      for {shape, overrides} <- valid_shapes do
        assert {:ok, _} = insert_run(overrides), "expected valid shape #{shape} to insert"
      end
    end

    test "rejects every invalid lifecycle tuple through raw SQL" do
      install!()

      now = DateTime.utc_now()

      # Each case is one minimal mutation off a valid row, so exactly the
      # named constraint fires. A non-running row carrying poison violates
      # the waiting/terminal-idle and poisoned-shape constraints
      # inseparably, so that case accepts either name.
      invalid_tuples = [
        {"unknown status", [status: "created", wake_at: nil], ~w(docket_runs_status_check)},
        {"running with finished_at", [finished_at: now], ~w(docket_runs_finished_at_check)},
        {"done without finished_at", [status: "done", wake_at: nil],
         ~w(docket_runs_finished_at_check)},
        {"claim token without claimed_at", [wake_at: nil, claim_token: uuid()],
         ~w(docket_runs_claim_pair_check)},
        {"claimed_at without claim token", [claimed_at: now], ~w(docket_runs_claim_pair_check)},
        {"poisoned_at without poison_reason", [wake_at: nil, poisoned_at: now],
         ~w(docket_runs_poison_pair_check)},
        {"poison_reason without poisoned_at", [poison_reason: "stuck"],
         ~w(docket_runs_poison_pair_check)},
        {"waiting with a wake", [status: "waiting"], ~w(docket_runs_waiting_terminal_idle_check)},
        {"waiting with a claim",
         [status: "waiting", wake_at: nil, claim_token: uuid(), claimed_at: now],
         ~w(docket_runs_waiting_terminal_idle_check)},
        {"waiting with poison",
         [
           status: "waiting",
           wake_at: nil,
           poisoned_at: now,
           poison_reason: "stuck"
         ], ~w(docket_runs_waiting_terminal_idle_check docket_runs_poisoned_shape_check)},
        {"terminal with a wake", [status: "done", finished_at: now],
         ~w(docket_runs_waiting_terminal_idle_check)},
        {"poisoned with a claim",
         [
           wake_at: nil,
           poisoned_at: now,
           poison_reason: "stuck",
           claim_token: uuid(),
           claimed_at: now
         ], ~w(docket_runs_poisoned_shape_check)},
        {"poisoned with a wake", [poisoned_at: now, poison_reason: "stuck"],
         ~w(docket_runs_poisoned_shape_check)},
        {"running with both wake and claim", [claim_token: uuid(), claimed_at: now],
         ~w(docket_runs_running_schedule_check)},
        {"running with neither wake nor claim", [wake_at: nil],
         ~w(docket_runs_running_schedule_check)},
        {"negative step", [step: -1], ~w(docket_runs_counters_check)},
        {"negative checkpoint_seq", [checkpoint_seq: -1], ~w(docket_runs_counters_check)},
        {"negative claim_attempts", [claim_attempts: -1], ~w(docket_runs_counters_check)},
        {"negative claim_abandons", [claim_abandons: -1], ~w(docket_runs_counters_check)}
      ]

      for {label, overrides, constraints} <- invalid_tuples do
        assert {:error, %Postgrex.Error{postgres: %{code: :check_violation} = pg}} =
                 insert_run(overrides),
               "expected #{label} to raise a check violation"

        assert pg.constraint in constraints,
               "expected #{label} to violate one of #{inspect(constraints)}, " <>
                 "got #{inspect(pg.constraint)}"
      end

      assert {:error, %Postgrex.Error{postgres: %{code: :not_null_violation} = pg}} =
               insert_run(started_at: nil)

      assert pg.column == "started_at"
    end

    test "referential integrity binds runs to graphs and events to runs" do
      install!()

      # A run cannot reference a graph version that was never published.
      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation} = pg}} =
               insert_run(graph_hash: "unpublished")

      assert pg.constraint == "docket_runs_graph_scope_fkey"

      assert {:ok, _} = insert_run([])

      # A referenced graph version cannot be deleted; an unreferenced one can.
      insert_graph_version!("g_unused", "hash_unused")

      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation}}} =
               TestRepo.query(
                 "DELETE FROM docket_graph_versions WHERE graph_id = 'g1'",
                 []
               )

      assert %{num_rows: 1} =
               TestRepo.query!(
                 "DELETE FROM docket_graph_versions WHERE graph_id = 'g_unused'",
                 []
               )

      # An event cannot reference a missing run.
      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation} = pg}} =
               insert_event("no_such_run", 1)

      assert pg.constraint == "docket_events_run_id_fkey"

      # Deleting a run cascades to its events.
      {:ok, %{rows: [[run_id]]}} =
        insert_run(status: "done", wake_at: nil, finished_at: DateTime.utc_now())

      assert {:ok, _} = insert_event(run_id, 1)
      assert {:ok, _} = insert_event(run_id, 2)

      assert %{num_rows: 1} =
               TestRepo.query!("DELETE FROM docket_runs WHERE run_id = $1", [run_id])

      assert %{rows: [[0]]} =
               TestRepo.query!("SELECT count(*) FROM docket_events WHERE run_id = $1", [run_id])
    end

    test "scoped graph identity binds tenantless and tenant-owned runs without NULL holes" do
      install!()

      assert {:ok, %{rows: [[tenantless_run]]}} = insert_run([])
      assert {:ok, %{rows: [[tenant_run]]}} = insert_run(tenant_id: "acme")

      assert %{rows: [["", nil]]} =
               TestRepo.query!(
                 "SELECT scope_key, tenant_id FROM docket_runs WHERE run_id = $1",
                 [tenantless_run]
               )

      assert %{rows: [["acme", "acme"]]} =
               TestRepo.query!(
                 "SELECT scope_key, tenant_id FROM docket_runs WHERE run_id = $1",
                 [tenant_run]
               )

      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation} = pg}} =
               TestRepo.query(
                 "UPDATE docket_runs SET tenant_id = 'other' WHERE run_id = $1",
                 [tenant_run]
               )

      assert pg.constraint == "docket_runs_graph_scope_fkey"

      for table <- ["docket_graph_versions", "docket_runs"] do
        assert {:error, %Postgrex.Error{postgres: %{code: :check_violation} = pg}} =
                 TestRepo.query(
                   "UPDATE #{table} SET tenant_id = '' WHERE tenant_id IS NULL",
                   []
                 )

        assert pg.constraint in [
                 "docket_graph_versions_tenant_id_check",
                 "docket_runs_tenant_id_check"
               ]
      end
    end

    test "dispatch and poison-introspection scans use their partial indexes" do
      install!()

      ready_scan = """
      SELECT id FROM docket_runs
      WHERE status = 'running' AND poisoned_at IS NULL
        AND claim_token IS NULL AND wake_at <= now()
      ORDER BY wake_at, id
      LIMIT 10
      """

      expired_scan = """
      SELECT id FROM docket_runs
      WHERE status = 'running' AND poisoned_at IS NULL
        AND claim_token IS NOT NULL AND claimed_at < now() - interval '1 minute'
      ORDER BY claimed_at, id
      LIMIT 10
      """

      poison_scan = """
      SELECT id FROM docket_runs
      WHERE poisoned_at IS NOT NULL
      ORDER BY poisoned_at
      """

      assert explain(ready_scan) =~ "docket_runs_wake_at_id_index"
      assert explain(expired_scan) =~ "docket_runs_claimed_at_id_index"
      assert explain(poison_scan) =~ "docket_runs_poisoned_at_index"
    end

    test "newest-first run collection scans use stable listing indexes" do
      install!()

      system_scan = """
      SELECT run_id, started_at FROM docket_runs
      ORDER BY started_at DESC, run_id DESC
      LIMIT 10
      """

      tenant_scan = """
      SELECT run_id, started_at FROM docket_runs
      WHERE tenant_id = 'tenant-1'
      ORDER BY started_at DESC, run_id DESC
      LIMIT 10
      """

      assert explain(system_scan) =~ "docket_runs_list_order_index"
      assert explain(tenant_scan) =~ "docket_runs_tenant_list_order_index"
    end

    defp install! do
      assert :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
    end

    defp assert_initial_v2_state(prefix) do
      quoted = ~s("#{prefix}")

      assert_v2_catalog_boundary(prefix)

      assert TestRepo.query!("""
             SELECT id, preferred_active, max_active, weight, borrowing,
                    policy_version, initialized_at IS NULL
             FROM #{quoted}.docket_claim_policy
             """).rows == [[1, nil, nil, nil, nil, 0, true]]

      assert TestRepo.query!("""
             SELECT id, schema_generation, dual_write_assertion_id, backfill_phase,
                    backfill_cursor, backfill_batches, backfill_rows,
                    backfill_completed_at, backfill_last_error, ready_index_valid,
                    live_index_valid, fk_disposition, missing_partition_count,
                    verified_default_fingerprint, verified_at
             FROM #{quoted}.docket_claim_rollout
             """).rows == [
               [
                 1,
                 2,
                 nil,
                 "not_started",
                 nil,
                 0,
                 0,
                 nil,
                 nil,
                 false,
                 false,
                 "absent",
                 nil,
                 nil,
                 nil
               ]
             ]

      assert TestRepo.query!("""
             SELECT id, readiness, readiness_epoch, admission_mode, mode_epoch,
                    required_function_contract
             FROM #{quoted}.docket_claim_admission_gate
             """).rows == [[1, "not_ready", 0, "legacy", 0, 1]]

      for table <-
            @v2_tables --
              ~w(docket_claim_policy docket_claim_rollout docket_claim_admission_gate) do
        assert TestRepo.query!("SELECT count(*) FROM #{quoted}.#{table}").rows == [[0]],
               "expected #{prefix}.#{table} to start empty"
      end
    end

    defp assert_v2_catalog_boundary(prefix) do
      assert v2_column_catalog(prefix) == @v2_column_catalog

      foreign_keys =
        TestRepo.query!(
          """
          SELECT src.relname, con.conname, dst.relname,
                 pg_get_constraintdef(con.oid, true)
          FROM pg_constraint AS con
          JOIN pg_class AS src ON src.oid = con.conrelid
          JOIN pg_class AS dst ON dst.oid = con.confrelid
          JOIN pg_namespace AS n ON n.oid = src.relnamespace
          WHERE n.nspname = $1 AND src.relname = ANY($2) AND con.contype = 'f'
          ORDER BY src.relname, con.conname
          """,
          [prefix, @v2_tables]
        ).rows

      assert [
               [
                 "docket_claim_rollout",
                 "docket_claim_rollout_dual_write_assertion_fkey",
                 "docket_claim_assertions",
                 foreign_key_definition
               ]
             ] = foreign_keys

      assert foreign_key_definition =~ "FOREIGN KEY (dual_write_assertion_id)"
      assert foreign_key_definition =~ "REFERENCES"
      assert foreign_key_definition =~ "docket_claim_assertions(assertion_id)"
      assert foreign_key_definition =~ "ON UPDATE RESTRICT ON DELETE RESTRICT"

      assert TestRepo.query!(
               """
               SELECT is_identity, identity_generation
               FROM information_schema.columns
               WHERE table_schema = $1
                 AND table_name = 'docket_claim_policy_events'
                 AND column_name = 'audit_id'
               """,
               [prefix]
             ).rows == [["YES", "ALWAYS"]]

      assert v2_index_catalog(prefix) == @v2_index_catalog

      assert TestRepo.query!(
               """
               SELECT p.proname
               FROM pg_proc AS p
               JOIN pg_namespace AS n ON n.oid = p.pronamespace
               WHERE n.nspname = $1 AND p.proname LIKE 'docket_%'
               """,
               [prefix]
             ).rows == []
    end

    defp singleton_state(prefix) do
      quoted = ~s("#{prefix}")

      for table <- ~w(docket_claim_policy docket_claim_rollout docket_claim_admission_gate),
          into: %{} do
        {table, TestRepo.query!("SELECT id, updated_at FROM #{quoted}.#{table} ORDER BY id").rows}
      end
    end

    defp v2_schema_signature(schema) do
      columns =
        TestRepo.query!(
          """
          SELECT table_name, column_name, data_type, udt_name, is_nullable,
                 COALESCE(column_default, ''), is_identity, identity_generation
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = ANY($2)
          ORDER BY table_name, ordinal_position
          """,
          [schema, @v2_tables]
        ).rows

      definitions =
        TestRepo.query!(
          """
          SELECT c.relname, con.conname, pg_get_constraintdef(con.oid, true)
          FROM pg_constraint AS con
          JOIN pg_class AS c ON c.oid = con.conrelid
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
          WHERE n.nspname = $1 AND c.relname = ANY($2)
          ORDER BY c.relname, con.conname
          """,
          [schema, @v2_tables]
        ).rows
        |> normalize_schema(schema)

      %{columns: columns, definitions: definitions}
    end

    defp v1_schema_signature(schema) do
      columns =
        TestRepo.query!(
          """
          SELECT table_name, column_name, data_type, udt_name, is_nullable,
                 COALESCE(column_default, ''), is_generated, generation_expression
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = ANY($2)
          ORDER BY table_name, ordinal_position
          """,
          [schema, @v1_tables]
        ).rows

      indexes =
        TestRepo.query!(
          """
          SELECT tablename, indexname, indexdef
          FROM pg_indexes
          WHERE schemaname = $1 AND tablename = ANY($2)
          ORDER BY tablename, indexname
          """,
          [schema, @v1_tables]
        ).rows
        |> normalize_schema(schema)

      definitions =
        TestRepo.query!(
          """
          SELECT c.relname, con.conname, pg_get_constraintdef(con.oid, true)
          FROM pg_constraint AS con
          JOIN pg_class AS c ON c.oid = con.conrelid
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
          WHERE n.nspname = $1 AND c.relname = ANY($2)
          ORDER BY c.relname, con.conname
          """,
          [schema, @v1_tables]
        ).rows
        |> normalize_schema(schema)

      %{columns: columns, indexes: indexes, definitions: definitions}
    end

    defp normalize_schema(rows, schema) do
      Enum.map(rows, fn row ->
        Enum.map(row, fn
          value when is_binary(value) ->
            value
            |> String.replace(~s("#{schema}".), "")
            |> String.replace("#{schema}.", "")

          value ->
            value
        end)
      end)
    end

    defp insert_receipt(event_id, target_fingerprints, previous_versions, versions, request) do
      TestRepo.query(
        """
        INSERT INTO docket_claim_policy_receipts
          (source, event_id, request_fingerprint, target_kind, target_fingerprints,
           outcome, previous_versions, versions, audit_id)
        VALUES ('test', $1, $2, 'default', $3, 'applied', $4, $5, 1)
        """,
        [event_id, request, target_fingerprints, previous_versions, versions]
      )
    end

    defp insert_policy_event(event_id, request) do
      TestRepo.query(
        """
        INSERT INTO docket_claim_policy_events
          (target_kind, target_keys, operation, actor, source, event_id,
           request_fingerprint, before_value, after_value, before_versions, after_versions)
        VALUES
          ('default', ARRAY['default'], 'put_default', 'tester', 'test', $1,
           $2, '{}'::jsonb, '{}'::jsonb, ARRAY[0]::bigint[], ARRAY[1]::bigint[])
        RETURNING audit_id
        """,
        [event_id, request]
      )
    end

    defp insert_hold(event_id, first_audit_id, last_audit_id) do
      TestRepo.query(
        """
        INSERT INTO docket_claim_policy_holds
          (hold_id, first_audit_id, last_audit_id, reason, actor, source, event_id)
        VALUES (gen_random_uuid(), $2, $3, 'retention', 'tester', 'test', $1)
        """,
        [event_id, first_audit_id, last_audit_id]
      )
    end

    defp insert_audit_export(event_id, through_audit_id, fingerprint) do
      TestRepo.query(
        """
        INSERT INTO docket_claim_audit_exports
          (export_id, through_audit_id, location_fingerprint, actor, source, event_id)
        VALUES (gen_random_uuid(), $2, $3, 'tester', 'test', $1)
        """,
        [event_id, through_audit_id, fingerprint]
      )
    end

    defp insert_assertion(assertion_kind, expires_at, fingerprint, event_id \\ nil) do
      event_id = event_id || assertion_kind

      TestRepo.query(
        """
        INSERT INTO docket_claim_assertions
          (assertion_id, assertion_kind, evidence_fingerprint, actor, source,
           event_id, expires_at, audit_id)
        VALUES (gen_random_uuid(), $1, $2, 'tester', 'test', $4, $3, 1)
        """,
        [assertion_kind, fingerprint, expires_at, event_id]
      )
    end

    defp assert_check_violation(constraint, fun) do
      assert {:error, %Postgrex.Error{postgres: %{code: :check_violation} = pg}} = fun.()
      assert pg.constraint == constraint
    end

    defp assert_unique_violation(constraint, fun) do
      assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation} = pg}} = fun.()
      assert pg.constraint == constraint
    end

    defp assert_foreign_key_violation(constraint, fun) do
      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation} = pg}} = fun.()
      assert pg.constraint == constraint
    end

    defp assert_routine_down_refused(version, migration) do
      error =
        assert_raise Postgrex.Error, fn ->
          Ecto.Migrator.down(TestRepo, version, migration, log: false)
        end

      assert Exception.message(error) =~ "use Docket.Postgres.Migration.destructive_down/1"
    end

    defp assert_destructive_down_refused(message) do
      error =
        assert_raise Postgrex.Error, fn ->
          Ecto.Migrator.down(TestRepo, @migration_version, DestructiveDocket, log: false)
        end

      assert Exception.message(error) =~ message
    end

    defp await_ungranted_down_lock(_connection, 0),
      do: raise("down never requested the gate table lock")

    defp await_ungranted_down_lock(connection, attempts) do
      case Postgrex.query!(
             connection,
             """
             SELECT count(*)
             FROM pg_locks
             WHERE relation = 'docket_claim_admission_gate'::regclass
               AND mode = 'AccessExclusiveLock'
               AND NOT granted
             """,
             []
           ).rows do
        [[count]] when count > 0 ->
          :ok

        [[0]] ->
          Process.sleep(10)
          await_ungranted_down_lock(connection, attempts - 1)
      end
    end

    defp assert_row_round_trip do
      now = DateTime.utc_now()

      assert {:ok, _version} =
               %{graph_id: "g1", graph_hash: "abc123", graph: <<131, 106>>}
               |> Docket.Postgres.Schemas.GraphVersion.changeset()
               |> TestRepo.insert()

      assert {:ok, run} =
               %{
                 run_id: "run_1",
                 graph_id: "g1",
                 graph_hash: "abc123",
                 status: :running,
                 state: <<131, 106>>,
                 started_at: now,
                 wake_at: now
               }
               |> Docket.Postgres.Schemas.Run.changeset()
               |> TestRepo.insert()

      assert run.step == 0
      assert run.checkpoint_seq == 0
      assert run.claim_attempts == 0
      assert run.poisoned_at == nil
      assert run.poison_reason == nil
      assert run.tenant_id == nil
      assert run.scope_key == ""

      assert {:error, changeset} =
               %{
                 run_id: "run_1",
                 graph_id: "g1",
                 graph_hash: "abc123",
                 status: :running,
                 state: <<131, 106>>,
                 started_at: now,
                 wake_at: now
               }
               |> Docket.Postgres.Schemas.Run.changeset()
               |> TestRepo.insert()

      assert {"has already been taken", _meta} = changeset.errors[:run_id]

      assert {:ok, _event} =
               %{
                 run_id: "run_1",
                 seq: 1,
                 type: :run_initialized,
                 step: 0,
                 payload: <<131, 116, 0, 0, 0, 0>>,
                 metadata: <<131, 116, 0, 0, 0, 0>>,
                 occurred_at: now
               }
               |> Docket.Postgres.Schemas.Event.changeset()
               |> TestRepo.insert()
    end

    # Inserts a docket_runs row through raw SQL — bypassing changesets on
    # purpose — as a ready `running` row unless overridden. Publishes the
    # success.
    defp insert_run(overrides, prefix \\ "public") do
      now = DateTime.utc_now()

      base = %{
        run_id: "run_#{System.unique_integer([:positive])}",
        tenant_id: nil,
        graph_id: "g1",
        graph_hash: "abc123",
        status: "running",
        step: 0,
        state: <<131, 106>>,
        checkpoint_seq: 0,
        latest_checkpoint_type: nil,
        claim_token: nil,
        claimed_at: nil,
        wake_at: now,
        claim_attempts: 0,
        claim_abandons: 0,
        poisoned_at: nil,
        poison_reason: nil,
        inserted_at: now,
        started_at: now,
        updated_at: now,
        finished_at: nil
      }

      row = Map.merge(base, Map.new(overrides))
      ensure_graph_version(prefix, row.tenant_id)
      columns = Map.keys(base)
      placeholders = Enum.map_join(1..length(columns), ", ", &"$#{&1}")

      TestRepo.query(
        """
        INSERT INTO #{prefix}.docket_runs (#{Enum.join(columns, ", ")})
        VALUES (#{placeholders})
        RETURNING run_id
        """,
        Enum.map(columns, &Map.fetch!(row, &1))
      )
    end

    defp insert_event(run_id, seq) do
      now = DateTime.utc_now()

      TestRepo.query(
        """
        INSERT INTO docket_events
          (run_id, seq, type, step, payload, metadata, occurred_at, inserted_at)
        VALUES ($1, $2, 'node_completed', 0, $3, $3, $4, $4)
        """,
        [run_id, seq, <<131, 116, 0, 0, 0, 0>>, now]
      )
    end

    defp ensure_graph_version(prefix, tenant_id) do
      insert_graph_version!("g1", "abc123", prefix, tenant_id)
    end

    defp insert_graph_version!(
           graph_id,
           graph_hash,
           prefix \\ "public",
           tenant_id \\ nil
         ) do
      TestRepo.query!(
        """
        INSERT INTO #{prefix}.docket_graph_versions
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
        """,
        [tenant_id, graph_id, graph_hash, <<131, 106>>, DateTime.utc_now()]
      )
    end

    defp uuid, do: Ecto.UUID.dump!(Ecto.UUID.generate())

    # SET LOCAL and EXPLAIN must run on the same pooled connection, so both
    # happen inside one transaction. Seq scans are disabled because the
    # planner never picks an index on an empty table otherwise.
    defp explain(sql) do
      {:ok, plan} =
        TestRepo.transaction(fn ->
          TestRepo.query!("SET LOCAL enable_seqscan = off")

          %{rows: rows} = TestRepo.query!("EXPLAIN #{sql}")

          Enum.map_join(rows, "\n", &List.first/1)
        end)

      plan
    end

    defp tables(schema) do
      %{rows: rows} =
        TestRepo.query!(
          """
          SELECT table_name FROM information_schema.tables
          WHERE table_schema = $1 AND table_name LIKE 'docket_%'
          """,
          [schema]
        )

      rows |> List.flatten() |> Enum.sort()
    end

    defp columns(table) do
      %{rows: rows} =
        TestRepo.query!(
          """
          SELECT column_name FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = $1
          """,
          [table]
        )

      rows |> List.flatten() |> Enum.sort()
    end

    defp v2_column_catalog(schema) do
      TestRepo.query!(
        """
        SELECT table_class.relname, attribute.attname,
               format_type(attribute.atttypid, attribute.atttypmod),
               attribute.attnotnull,
               pg_get_expr(default_value.adbin, default_value.adrelid),
               attribute.attidentity
        FROM pg_class AS table_class
        JOIN pg_namespace AS namespace ON namespace.oid = table_class.relnamespace
        JOIN pg_attribute AS attribute ON attribute.attrelid = table_class.oid
        LEFT JOIN pg_attrdef AS default_value
          ON default_value.adrelid = table_class.oid
         AND default_value.adnum = attribute.attnum
        WHERE namespace.nspname = $1
          AND table_class.relname = ANY($2)
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
        ORDER BY table_class.relname, attribute.attnum
        """,
        [schema, @v2_tables]
      ).rows
      |> Enum.group_by(
        &List.first/1,
        fn [_table, column, type, not_null, default, identity] ->
          {column, type, not_null, normalize_catalog_default(default),
           normalize_identity(identity)}
        end
      )
    end

    defp normalize_catalog_default(nil), do: nil

    defp normalize_catalog_default(default) do
      String.replace(default, ~r/::character varying$/, "")
    end

    defp normalize_identity("a"), do: :always
    defp normalize_identity(<<0>>), do: nil

    defp v2_index_catalog(schema) do
      TestRepo.query!(
        """
        SELECT table_class.relname, index_class.relname,
               index_metadata.indisprimary, index_metadata.indisunique,
               ARRAY(
                 SELECT attribute.attname
                 FROM unnest(index_metadata.indkey::smallint[]) WITH ORDINALITY
                      AS index_key(attnum, position)
                 JOIN pg_attribute AS attribute
                   ON attribute.attrelid = table_class.oid
                  AND attribute.attnum = index_key.attnum
                 WHERE index_key.attnum > 0
                 ORDER BY index_key.position
               )
        FROM pg_index AS index_metadata
        JOIN pg_class AS table_class ON table_class.oid = index_metadata.indrelid
        JOIN pg_class AS index_class ON index_class.oid = index_metadata.indexrelid
        JOIN pg_namespace AS namespace ON namespace.oid = table_class.relnamespace
        WHERE namespace.nspname = $1 AND table_class.relname = ANY($2)
        ORDER BY table_class.relname, index_class.relname
        """,
        [schema, @v2_tables]
      ).rows
      |> Enum.map(fn [table, index, primary?, unique?, columns] ->
        {table, index, primary?, unique?, columns}
      end)
    end

    defp nullable?(table, column) do
      %{rows: [[answer]]} =
        TestRepo.query!(
          """
          SELECT is_nullable FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
          """,
          [table, column]
        )

      answer == "YES"
    end

    defp column_type(table, column) do
      %{rows: [[data_type]]} =
        TestRepo.query!(
          """
          SELECT data_type FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
          """,
          [table, column]
        )

      data_type
    end

    defp column_default(table, column) do
      %{rows: [[default]]} =
        TestRepo.query!(
          """
          SELECT column_default FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
          """,
          [table, column]
        )

      default
    end

    defp indexes(table) do
      %{rows: rows} =
        TestRepo.query!(
          "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = $1",
          [table]
        )

      Map.new(rows, fn [name, def] -> {name, def} end)
    end
  end
end
