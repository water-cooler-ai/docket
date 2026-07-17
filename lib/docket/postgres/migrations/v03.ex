if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V03 do
    @moduledoc false

    use Ecto.Migration

    alias Docket.Postgres.ClaimPolicy.TenantFair.Budgets
    alias Docket.Postgres.Storage

    @membership_function "docket_claim_schedule_partition_v1"
    @membership_trigger "docket_claim_schedule_partition_insert"
    @membership_guard_function "docket_claim_schedule_guard_v1"
    @membership_guard_trigger "docket_claim_schedule_guard"

    def up(%{prefix: prefix}) do
      partitions = qualified_table(prefix, "docket_claim_partitions")
      schedule = qualified_table(prefix, "docket_claim_schedule")
      scan_cursor = qualified_table(prefix, "docket_claim_scan_cursor")
      ready_reconciliation = qualified_table(prefix, "docket_claim_ready_reconciliation")
      expired_reconciliation = qualified_table(prefix, "docket_claim_expired_reconciliation")
      membership_function = qualified_table(prefix, @membership_function)
      membership_guard_function = qualified_table(prefix, @membership_guard_function)
      expired_offset = Budgets.expired_reconciliation_offset()
      cadence = Budgets.reconciliation_cadence_scan_calls()

      unless expired_offset < cadence do
        raise "expired reconciliation offset must be less than its cadence"
      end

      execute("""
      CREATE TABLE #{schedule} (
        scope_key text PRIMARY KEY,
        ring_position bigint GENERATED ALWAYS AS IDENTITY NOT NULL UNIQUE,
        may_have_ready_at timestamptz NULL,
        may_have_claimed_at timestamptz NULL,
        ready_candidate_cursor_at timestamptz NULL,
        ready_candidate_cursor_id bigint NULL,
        expired_candidate_cursor_at timestamptz NULL,
        expired_candidate_cursor_id bigint NULL,
        ready_dirty boolean NOT NULL DEFAULT true,
        claimed_dirty boolean NOT NULL DEFAULT true,
        in_cohort boolean GENERATED ALWAYS AS (
          ready_dirty OR claimed_dirty OR
          may_have_ready_at IS NOT NULL OR may_have_claimed_at IS NOT NULL
        ) STORED,
        inserted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_schedule_partition_fkey
          FOREIGN KEY (scope_key) REFERENCES #{partitions} (scope_key) ON DELETE CASCADE,
        CONSTRAINT docket_claim_schedule_position_check CHECK (ring_position > 0),
        CONSTRAINT docket_claim_schedule_ready_candidate_cursor_check CHECK (
          (ready_candidate_cursor_at IS NULL) = (ready_candidate_cursor_id IS NULL)
        ),
        CONSTRAINT docket_claim_schedule_expired_candidate_cursor_check CHECK (
          (expired_candidate_cursor_at IS NULL) = (expired_candidate_cursor_id IS NULL)
        )
      )
      """)

      execute("""
      CREATE INDEX docket_claim_schedule_cohort_ring_index
      ON #{schedule} (ring_position)
      INCLUDE (
        scope_key, may_have_ready_at, may_have_claimed_at, ready_dirty, claimed_dirty
      )
      WHERE in_cohort
      """)

      execute("""
      CREATE FUNCTION #{membership_guard_function}()
      RETURNS trigger
      LANGUAGE plpgsql
      VOLATILE
      PARALLEL UNSAFE
      SECURITY INVOKER
      SET search_path TO pg_catalog, pg_temp
      AS $docket_claim_schedule_guard_v1$
      BEGIN
        IF TG_OP = 'UPDATE' THEN
          IF NEW.scope_key IS DISTINCT FROM OLD.scope_key OR
             NEW.ring_position IS DISTINCT FROM OLD.ring_position THEN
            RAISE EXCEPTION 'docket claim schedule membership is immutable'
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
          RETURN NEW;
        END IF;

        PERFORM 1 FROM #{partitions} WHERE scope_key = OLD.scope_key;
        IF FOUND THEN
          RAISE EXCEPTION 'delete the owning claim partition, not its schedule membership'
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;

        RETURN OLD;
      END;
      $docket_claim_schedule_guard_v1$
      """)

      execute("""
      CREATE TRIGGER #{@membership_guard_trigger}
      BEFORE UPDATE OR DELETE ON #{schedule}
      FOR EACH ROW
      EXECUTE FUNCTION #{membership_guard_function}()
      """)

      execute("""
      CREATE TABLE #{scan_cursor} (
        id smallint PRIMARY KEY DEFAULT 1,
        ring_position bigint NOT NULL DEFAULT 0,
        scan_call_sequence bigint NOT NULL DEFAULT 0,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_scan_cursor_singleton_check CHECK (id = 1),
        CONSTRAINT docket_claim_scan_cursor_position_check CHECK (ring_position >= 0),
        CONSTRAINT docket_claim_scan_cursor_sequence_check CHECK (scan_call_sequence >= 0)
      )
      """)

      execute("""
      CREATE TABLE #{ready_reconciliation} (
        id smallint PRIMARY KEY DEFAULT 1,
        last_scope_key text NOT NULL DEFAULT '',
        wrap_count bigint NOT NULL DEFAULT 0,
        next_scan_call bigint NOT NULL DEFAULT 0,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_ready_reconciliation_singleton_check CHECK (id = 1),
        CONSTRAINT docket_claim_ready_reconciliation_wrap_check CHECK (wrap_count >= 0),
        CONSTRAINT docket_claim_ready_reconciliation_due_check CHECK (next_scan_call >= 0)
      )
      """)

      execute("""
      CREATE TABLE #{expired_reconciliation} (
        id smallint PRIMARY KEY DEFAULT 1,
        last_scope_key text NOT NULL DEFAULT '',
        wrap_count bigint NOT NULL DEFAULT 0,
        next_scan_call bigint NOT NULL DEFAULT #{expired_offset},
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_expired_reconciliation_singleton_check CHECK (id = 1),
        CONSTRAINT docket_claim_expired_reconciliation_wrap_check CHECK (wrap_count >= 0),
        CONSTRAINT docket_claim_expired_reconciliation_due_check CHECK (next_scan_call >= 0)
      )
      """)

      execute("INSERT INTO #{scan_cursor} (id) VALUES (1)")
      execute("INSERT INTO #{ready_reconciliation} (id) VALUES (1)")
      execute("INSERT INTO #{expired_reconciliation} (id) VALUES (1)")

      execute("""
      CREATE FUNCTION #{membership_function}()
      RETURNS trigger
      LANGUAGE plpgsql
      VOLATILE
      PARALLEL UNSAFE
      SECURITY INVOKER
      SET search_path TO pg_catalog, pg_temp
      AS $docket_claim_schedule_partition_v1$
      BEGIN
        INSERT INTO #{schedule} (scope_key)
        VALUES (NEW.scope_key)
        ON CONFLICT (scope_key) DO NOTHING;
        RETURN NEW;
      END;
      $docket_claim_schedule_partition_v1$
      """)

      execute("""
      CREATE TRIGGER #{@membership_trigger}
      AFTER INSERT ON #{partitions}
      FOR EACH ROW
      EXECUTE FUNCTION #{membership_function}()
      """)

      # The supported v2-to-v3 migration is stopped and homogeneous. This lock
      # makes the schema primitive safe even if a stray first-partition writer
      # overlaps the backfill: it either precedes the snapshot and is copied or
      # follows trigger installation and creates membership itself.
      execute("LOCK TABLE #{partitions} IN SHARE ROW EXCLUSIVE MODE")

      execute("""
      INSERT INTO #{schedule} (scope_key)
      SELECT scope_key
      FROM #{partitions}
      ORDER BY scope_key
      ON CONFLICT (scope_key) DO NOTHING
      """)
    end

    def down(%{prefix: prefix}) do
      partitions = qualified_table(prefix, "docket_claim_partitions")
      membership_function = qualified_table(prefix, @membership_function)
      schedule = qualified_table(prefix, "docket_claim_schedule")
      membership_guard_function = qualified_table(prefix, @membership_guard_function)

      execute("DROP TRIGGER IF EXISTS #{@membership_trigger} ON #{partitions}")
      execute("DROP FUNCTION IF EXISTS #{membership_function}()")
      execute("DROP TRIGGER IF EXISTS #{@membership_guard_trigger} ON #{schedule}")
      execute("DROP FUNCTION IF EXISTS #{membership_guard_function}()")

      execute(
        "DROP TABLE IF EXISTS #{qualified_table(prefix, "docket_claim_expired_reconciliation")}"
      )

      execute(
        "DROP TABLE IF EXISTS #{qualified_table(prefix, "docket_claim_ready_reconciliation")}"
      )

      execute("DROP TABLE IF EXISTS #{qualified_table(prefix, "docket_claim_scan_cursor")}")
      execute("DROP TABLE IF EXISTS #{qualified_table(prefix, "docket_claim_schedule")}")
    end

    defp qualified_table(prefix, name), do: Storage.qualified_table(prefix, name)
  end
end
