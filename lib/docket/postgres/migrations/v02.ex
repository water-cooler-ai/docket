if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V02 do
    @moduledoc false

    use Ecto.Migration

    alias Docket.Postgres.Storage

    @tables [
      "docket_claim_policy_receipts",
      "docket_claim_policy_events",
      "docket_claim_policy_holds",
      "docket_claim_audit_exports",
      "docket_claim_rollout",
      "docket_claim_admission_gate",
      "docket_claim_capabilities",
      "docket_claim_assertions",
      "docket_claim_partitions",
      "docket_claim_policy"
    ]

    def up(%{prefix: prefix}) do
      policy = qualified_table(prefix, "docket_claim_policy")
      partitions = qualified_table(prefix, "docket_claim_partitions")
      receipts = qualified_table(prefix, "docket_claim_policy_receipts")
      events = qualified_table(prefix, "docket_claim_policy_events")
      holds = qualified_table(prefix, "docket_claim_policy_holds")
      exports = qualified_table(prefix, "docket_claim_audit_exports")
      assertions = qualified_table(prefix, "docket_claim_assertions")
      rollout = qualified_table(prefix, "docket_claim_rollout")
      gate = qualified_table(prefix, "docket_claim_admission_gate")
      capabilities = qualified_table(prefix, "docket_claim_capabilities")

      execute("""
      CREATE TABLE #{policy} (
        id smallint PRIMARY KEY DEFAULT 1,
        preferred_active integer NULL,
        max_active integer NULL,
        weight integer NULL,
        borrowing boolean NULL,
        policy_version bigint NOT NULL DEFAULT 0,
        initialized_at timestamptz NULL,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_policy_singleton_check CHECK (id = 1),
        CONSTRAINT docket_claim_policy_version_check CHECK (policy_version >= 0),
        CONSTRAINT docket_claim_policy_tuple_check CHECK (
          (
            preferred_active IS NULL AND max_active IS NULL AND weight IS NULL AND
            borrowing IS NULL AND policy_version = 0 AND initialized_at IS NULL
          ) OR (
            preferred_active IS NOT NULL AND max_active IS NOT NULL AND weight IS NOT NULL AND
            borrowing IS NOT NULL AND policy_version >= 1 AND initialized_at IS NOT NULL AND
            preferred_active >= 0 AND preferred_active <= max_active AND
            max_active <= 2147483647 AND weight > 0
          )
        )
      )
      """)

      execute("""
      CREATE TABLE #{partitions} (
        scope_key text PRIMARY KEY,
        preferred_active integer NULL,
        max_active integer NULL,
        weight integer NULL,
        borrowing boolean NULL,
        admin_state varchar(16) NOT NULL DEFAULT 'running',
        partition_version bigint NOT NULL DEFAULT 0,
        admission_epoch bigint NOT NULL DEFAULT 0,
        inserted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_partitions_admin_state_check
          CHECK (admin_state IN ('running', 'hold_new', 'drain')),
        CONSTRAINT docket_claim_partitions_version_check CHECK (partition_version >= 0),
        CONSTRAINT docket_claim_partitions_admission_epoch_check CHECK (admission_epoch >= 0),
        CONSTRAINT docket_claim_partitions_tuple_check CHECK (
          (
            preferred_active IS NULL AND max_active IS NULL AND weight IS NULL AND
            borrowing IS NULL
          ) OR (
            preferred_active IS NOT NULL AND max_active IS NOT NULL AND weight IS NOT NULL AND
            borrowing IS NOT NULL AND preferred_active >= 0 AND
            preferred_active <= max_active AND max_active <= 2147483647 AND weight > 0
          )
        )
      )
      """)

      execute("""
      CREATE TABLE #{events} (
        audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        target_kind varchar(16) NOT NULL,
        target_keys text[] NOT NULL,
        operation varchar(32) NOT NULL,
        actor varchar(255) NOT NULL,
        source varchar(64) NOT NULL,
        event_id varchar(255) NOT NULL,
        request_fingerprint bytea NOT NULL,
        before_value jsonb NOT NULL,
        after_value jsonb NOT NULL,
        before_versions bigint[] NOT NULL,
        after_versions bigint[] NOT NULL,
        mode_epoch bigint NULL,
        occurred_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_policy_events_target_kind_check
          CHECK (target_kind IN ('default', 'partition', 'bulk', 'activation', 'readiness', 'audit')),
        CONSTRAINT docket_claim_policy_events_fingerprint_check
          CHECK (octet_length(request_fingerprint) = 32),
        CONSTRAINT docket_claim_policy_events_cardinality_check CHECK (
          cardinality(target_keys) > 0 AND
          cardinality(target_keys) = cardinality(before_versions) AND
          cardinality(target_keys) = cardinality(after_versions)
        ),
        CONSTRAINT docket_claim_policy_events_source_event_index UNIQUE (source, event_id)
      )
      """)

      execute("""
      CREATE TABLE #{receipts} (
        source varchar(64) NOT NULL,
        event_id varchar(255) NOT NULL,
        request_fingerprint bytea NOT NULL,
        target_kind varchar(16) NOT NULL,
        target_fingerprints bytea[] NOT NULL,
        outcome varchar(16) NOT NULL,
        previous_versions bigint[] NOT NULL,
        versions bigint[] NOT NULL,
        audit_id bigint NOT NULL,
        created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (source, event_id),
        CONSTRAINT docket_claim_policy_receipts_request_fingerprint_check
          CHECK (octet_length(request_fingerprint) = 32),
        CONSTRAINT docket_claim_policy_receipts_target_kind_check
          CHECK (target_kind IN ('default', 'partition', 'bulk', 'activation', 'readiness', 'audit')),
        CONSTRAINT docket_claim_policy_receipts_outcome_check
          CHECK (outcome IN ('applied', 'unchanged', 'demoted')),
        CONSTRAINT docket_claim_policy_receipts_cardinality_check CHECK (
          cardinality(target_fingerprints) > 0 AND
          cardinality(target_fingerprints) = cardinality(previous_versions) AND
          cardinality(target_fingerprints) = cardinality(versions)
        ),
        CONSTRAINT docket_claim_policy_receipts_target_fingerprints_check CHECK (
          array_position(target_fingerprints, NULL) IS NULL AND
          encode(array_send(target_fingerprints), 'hex') ~
            '^000000010000000000000011[0-9a-f]{16}(00000020[0-9a-f]{64})+$'
        ),
        CONSTRAINT docket_claim_policy_receipts_versions_check CHECK (
          array_position(previous_versions, NULL) IS NULL AND
          array_position(versions, NULL) IS NULL AND
          0 <= ALL(previous_versions) AND 0 <= ALL(versions)
        )
      )
      """)

      execute("""
      CREATE TABLE #{holds} (
        hold_id uuid PRIMARY KEY,
        first_audit_id bigint NOT NULL,
        last_audit_id bigint NOT NULL,
        reason varchar(512) NOT NULL,
        actor varchar(255) NOT NULL,
        source varchar(64) NOT NULL,
        event_id varchar(255) NOT NULL,
        created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_policy_holds_range_check
          CHECK (first_audit_id > 0 AND last_audit_id >= first_audit_id),
        CONSTRAINT docket_claim_policy_holds_source_event_index UNIQUE (source, event_id)
      )
      """)

      execute("""
      CREATE TABLE #{exports} (
        export_id uuid PRIMARY KEY,
        through_audit_id bigint NOT NULL,
        location_fingerprint bytea NOT NULL,
        actor varchar(255) NOT NULL,
        source varchar(64) NOT NULL,
        event_id varchar(255) NOT NULL,
        completed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_audit_exports_audit_id_check CHECK (through_audit_id > 0),
        CONSTRAINT docket_claim_audit_exports_fingerprint_check
          CHECK (octet_length(location_fingerprint) = 32),
        CONSTRAINT docket_claim_audit_exports_source_event_index UNIQUE (source, event_id)
      )
      """)

      execute("""
      CREATE TABLE #{assertions} (
        assertion_id uuid PRIMARY KEY,
        assertion_kind varchar(32) NOT NULL,
        evidence_fingerprint bytea NOT NULL,
        actor varchar(255) NOT NULL,
        source varchar(64) NOT NULL,
        event_id varchar(255) NOT NULL,
        asserted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        expires_at timestamptz NULL,
        audit_id bigint NOT NULL,
        CONSTRAINT docket_claim_assertions_kind_check
          CHECK (assertion_kind IN ('dual_write', 'old_binaries_absent')),
        CONSTRAINT docket_claim_assertions_fingerprint_check
          CHECK (octet_length(evidence_fingerprint) = 32),
        CONSTRAINT docket_claim_assertions_expiry_check CHECK (
          (assertion_kind = 'dual_write' AND expires_at IS NULL) OR
          (assertion_kind = 'old_binaries_absent' AND expires_at IS NOT NULL AND
            expires_at > asserted_at)
        ),
        CONSTRAINT docket_claim_assertions_source_event_index UNIQUE (source, event_id)
      )
      """)

      execute("""
      CREATE TABLE #{rollout} (
        id smallint PRIMARY KEY DEFAULT 1,
        schema_generation integer NOT NULL DEFAULT 2,
        dual_write_assertion_id uuid NULL,
        backfill_phase varchar(24) NOT NULL DEFAULT 'not_started',
        backfill_cursor bigint NULL,
        backfill_batches bigint NOT NULL DEFAULT 0,
        backfill_rows bigint NOT NULL DEFAULT 0,
        backfill_completed_at timestamptz NULL,
        backfill_last_error varchar(512) NULL,
        ready_index_valid boolean NOT NULL DEFAULT false,
        live_index_valid boolean NOT NULL DEFAULT false,
        fk_disposition varchar(16) NOT NULL DEFAULT 'absent',
        missing_partition_count bigint NULL,
        verified_default_fingerprint bytea NULL,
        verified_at timestamptz NULL,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_rollout_singleton_check CHECK (id = 1),
        CONSTRAINT docket_claim_rollout_generation_check CHECK (schema_generation = 2),
        CONSTRAINT docket_claim_rollout_backfill_phase_check
          CHECK (backfill_phase IN ('not_started', 'running', 'reconciling', 'complete')),
        CONSTRAINT docket_claim_rollout_cursor_check
          CHECK (backfill_cursor IS NULL OR backfill_cursor >= 0),
        CONSTRAINT docket_claim_rollout_counts_check
          CHECK (backfill_batches >= 0 AND backfill_rows >= 0),
        CONSTRAINT docket_claim_rollout_fk_disposition_check
          CHECK (fk_disposition IN ('absent', 'not_valid', 'validated')),
        CONSTRAINT docket_claim_rollout_missing_count_check
          CHECK (missing_partition_count IS NULL OR missing_partition_count >= 0),
        CONSTRAINT docket_claim_rollout_fingerprint_check CHECK (
          verified_default_fingerprint IS NULL OR
          octet_length(verified_default_fingerprint) = 32
        ),
        CONSTRAINT docket_claim_rollout_dual_write_assertion_fkey
          FOREIGN KEY (dual_write_assertion_id) REFERENCES #{assertions} (assertion_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT
      )
      """)

      execute("""
      CREATE TABLE #{gate} (
        id smallint PRIMARY KEY DEFAULT 1,
        readiness varchar(16) NOT NULL DEFAULT 'not_ready',
        readiness_epoch bigint NOT NULL DEFAULT 0,
        admission_mode varchar(16) NOT NULL DEFAULT 'legacy',
        mode_epoch bigint NOT NULL DEFAULT 0,
        required_function_contract integer NOT NULL DEFAULT 1,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_admission_gate_singleton_check CHECK (id = 1),
        CONSTRAINT docket_claim_admission_gate_readiness_check
          CHECK (readiness IN ('not_ready', 'ready')),
        CONSTRAINT docket_claim_admission_gate_readiness_epoch_check CHECK (readiness_epoch >= 0),
        CONSTRAINT docket_claim_admission_gate_mode_check
          CHECK (admission_mode IN ('legacy', 'tenant_fair')),
        CONSTRAINT docket_claim_admission_gate_mode_epoch_check CHECK (mode_epoch >= 0),
        CONSTRAINT docket_claim_admission_gate_function_contract_check
          CHECK (required_function_contract = 1)
      )
      """)

      execute("""
      CREATE TABLE #{capabilities} (
        instance_id uuid PRIMARY KEY,
        binary_fingerprint bytea NOT NULL,
        writer_contract integer NOT NULL,
        gate_contract integer NOT NULL,
        function_contract integer NOT NULL,
        last_seen_at timestamptz NOT NULL,
        expires_at timestamptz NOT NULL,
        CONSTRAINT docket_claim_capabilities_fingerprint_check
          CHECK (octet_length(binary_fingerprint) = 32),
        CONSTRAINT docket_claim_capabilities_contracts_check CHECK (
          writer_contract >= 0 AND gate_contract >= 0 AND function_contract >= 0
        ),
        CONSTRAINT docket_claim_capabilities_expiry_check CHECK (expires_at > last_seen_at)
      )
      """)

      execute("INSERT INTO #{policy} (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
      execute("INSERT INTO #{rollout} (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
      execute("INSERT INTO #{gate} (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
    end

    def down(%{destructive: true, prefix: prefix}) do
      gate = qualified_table(prefix, "docket_claim_admission_gate")
      rollout = qualified_table(prefix, "docket_claim_rollout")
      policy = qualified_table(prefix, "docket_claim_policy")
      receipts = qualified_table(prefix, "docket_claim_policy_receipts")
      events = qualified_table(prefix, "docket_claim_policy_events")
      exports = qualified_table(prefix, "docket_claim_audit_exports")
      partitions = qualified_table(prefix, "docket_claim_partitions")
      runs = qualified_table(prefix, "docket_runs")

      execute("""
      DO $docket_v02_down$
      BEGIN
        -- Take teardown locks in the global authority order. ACCESS EXCLUSIVE
        -- excludes every writer between the safety observations and drops;
        -- PostgreSQL retains these locks until the host migration commits.
        LOCK TABLE #{gate} IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE #{rollout} IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE #{policy} IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE #{partitions} IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE #{runs} IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE #{receipts} IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE #{events} IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE #{exports} IN ACCESS EXCLUSIVE MODE;

        IF EXISTS (
          SELECT 1 FROM #{gate}
          WHERE admission_mode <> 'legacy' OR readiness <> 'not_ready'
        ) THEN
          RAISE EXCEPTION
            'Docket v2 down refused: admission mode/readiness makes reversal unsafe';
        END IF;

        IF EXISTS (
          SELECT 1 FROM #{receipts}
        ) THEN
          RAISE EXCEPTION
            'Docket destructive v2 teardown refused: durable claim-policy receipts are retained';
        END IF;

        IF EXISTS (
          SELECT 1 FROM #{runs} AS runs
          INNER JOIN #{partitions} AS partitions USING (scope_key)
        ) THEN
          RAISE EXCEPTION
            'Docket destructive v2 teardown refused: runs reference claim partitions';
        END IF;

        IF (SELECT max(audit_id) FROM #{events}) IS NOT NULL AND
           COALESCE((SELECT max(through_audit_id) FROM #{exports}), 0) <
             (SELECT max(audit_id) FROM #{events}) THEN
          RAISE EXCEPTION
            'Docket destructive v2 teardown refused: retained audit is not fully exported';
        END IF;
      END
      $docket_v02_down$
      """)

      Enum.each(@tables, fn name ->
        execute("DROP TABLE IF EXISTS #{qualified_table(prefix, name)}")
      end)
    end

    def down(%{prefix: prefix}) do
      execute("""
      DO $docket_v02_routine_down$
      BEGIN
        RAISE EXCEPTION
          'Docket v2 down refused: use Docket.Postgres.Migration.destructive_down/1 with explicit stopped-fleet, audit-export, receipt-loss, and partition-loss acknowledgements (prefix: #{prefix})';
      END
      $docket_v02_routine_down$
      """)
    end

    defp qualified_table(prefix, name), do: Storage.qualified_table(prefix, name)
  end
end
