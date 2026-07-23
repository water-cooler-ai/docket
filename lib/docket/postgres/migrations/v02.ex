if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V02 do
    @moduledoc false

    use Ecto.Migration

    alias Docket.Postgres.Storage

    @membership_function "docket_claim_schedule_partition_v1"
    @membership_trigger "docket_claim_schedule_partition_insert"
    @membership_guard_function "docket_claim_schedule_guard_v1"
    @membership_guard_trigger "docket_claim_schedule_guard"
    @schedule_truncate_guard_trigger "docket_claim_schedule_truncate_guard"
    @activity_function "docket_claim_schedule_activity_v1"
    @activity_trigger "docket_claim_schedule_run_activity"
    @run_truncate_guard_trigger "docket_claim_schedule_run_truncate_guard"
    @terminal_status_sql Docket.Run.terminal_statuses()
                         |> Enum.map_join(", ", &"'#{&1}'")

    def up(%{prefix: prefix}) do
      policy = qualified_table(prefix, "docket_claim_policy")
      partitions = qualified_table(prefix, "docket_claim_partitions")
      runs = qualified_table(prefix, "docket_runs")
      schedule = qualified_table(prefix, "docket_claim_schedule")
      membership_function = qualified_table(prefix, @membership_function)
      membership_guard_function = qualified_table(prefix, @membership_guard_function)
      activity_function = qualified_table(prefix, @activity_function)

      execute("""
      CREATE TABLE #{policy} (
        id smallint PRIMARY KEY DEFAULT 1,
        admission_mode varchar(16) NOT NULL DEFAULT 'legacy',
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_policy_singleton_check CHECK (id = 1),
        CONSTRAINT docket_claim_policy_mode_check CHECK (
          admission_mode IN ('legacy', 'windowed')
        )
      )
      """)

      execute("""
      CREATE TABLE #{partitions} (
        scope_key text PRIMARY KEY,
        inserted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
      )
      """)

      execute("INSERT INTO #{policy} (id) VALUES (1)")

      # Install and backfill admission and unfinished-schedule authority
      # without a gap. The stopped homogeneous migration uses the same
      # policy-before-partition-before-run order as admission.
      execute("LOCK TABLE #{policy} IN SHARE ROW EXCLUSIVE MODE")
      execute("LOCK TABLE #{partitions} IN SHARE ROW EXCLUSIVE MODE")
      execute("LOCK TABLE #{runs} IN SHARE ROW EXCLUSIVE MODE")

      execute("ALTER TABLE #{runs} ADD COLUMN tenant_admitted_at timestamptz NULL")

      execute("""
      UPDATE #{runs}
      SET tenant_admitted_at = claimed_at
      WHERE status = 'running'
        AND poisoned_at IS NULL
        AND claim_token IS NOT NULL
      """)

      execute("""
      ALTER TABLE #{runs}
      ADD CONSTRAINT docket_runs_tenant_admission_shape_check CHECK (
        tenant_admitted_at IS NULL OR (status = 'running' AND poisoned_at IS NULL)
      )
      """)

      execute("""
      INSERT INTO #{partitions} (scope_key)
      SELECT DISTINCT scope_key FROM #{runs}
      ON CONFLICT (scope_key) DO NOTHING
      """)

      execute("""
      CREATE INDEX docket_runs_scope_admitted_index
      ON #{runs} (scope_key)
      WHERE status = 'running' AND poisoned_at IS NULL AND
            tenant_admitted_at IS NOT NULL
      """)

      execute("""
      CREATE INDEX docket_runs_scope_admitted_ready_index
      ON #{runs} (scope_key, wake_at, id)
      WHERE status = 'running' AND poisoned_at IS NULL AND
            tenant_admitted_at IS NOT NULL AND claim_token IS NULL AND
            wake_at IS NOT NULL
      """)

      execute("""
      CREATE INDEX docket_runs_scope_queued_ready_index
      ON #{runs} (scope_key, wake_at, id)
      WHERE status = 'running' AND poisoned_at IS NULL AND
            tenant_admitted_at IS NULL AND claim_token IS NULL AND
            wake_at IS NOT NULL
      """)

      execute("""
      CREATE INDEX docket_runs_scope_admitted_expired_index
      ON #{runs} (scope_key, claimed_at, id)
      WHERE status = 'running' AND poisoned_at IS NULL AND
            tenant_admitted_at IS NOT NULL AND claim_token IS NOT NULL
      """)

      execute("""
      CREATE TABLE #{schedule} (
        scope_key text PRIMARY KEY,
        ring_position bigint GENERATED ALWAYS AS IDENTITY NOT NULL UNIQUE,
        unfinished_count bigint NOT NULL DEFAULT 0,
        inserted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_schedule_partition_fkey
          FOREIGN KEY (scope_key) REFERENCES #{partitions} (scope_key) ON DELETE CASCADE,
        CONSTRAINT docket_claim_schedule_position_check CHECK (ring_position > 0),
        CONSTRAINT docket_claim_schedule_unfinished_count_check CHECK (unfinished_count >= 0)
      )
      """)

      execute("""
      CREATE INDEX docket_claim_schedule_unfinished_ring_index
      ON #{schedule} (ring_position)
      INCLUDE (scope_key, unfinished_count)
      WHERE unfinished_count > 0
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
        IF TG_OP = 'TRUNCATE' THEN
          RAISE EXCEPTION 'TRUNCATE docket_claim_schedule is unsupported because unfinished membership is authoritative'
            USING ERRCODE = 'integrity_constraint_violation';
        ELSIF TG_OP = 'UPDATE' THEN
          IF NEW.scope_key IS DISTINCT FROM OLD.scope_key OR
             NEW.ring_position IS DISTINCT FROM OLD.ring_position THEN
            RAISE EXCEPTION 'docket claim schedule membership is immutable'
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;

          IF NEW.unfinished_count IS DISTINCT FROM OLD.unfinished_count AND
             pg_trigger_depth() < 2 THEN
            RAISE EXCEPTION 'docket claim schedule unfinished count is trigger-maintained'
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
          RETURN NEW;
        END IF;

        IF OLD.unfinished_count > 0 THEN
          RAISE EXCEPTION 'cannot delete a claim partition with unfinished runs'
            USING ERRCODE = 'foreign_key_violation';
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

      execute("""
      INSERT INTO #{schedule} (scope_key)
      SELECT scope_key
      FROM #{partitions}
      ORDER BY scope_key
      ON CONFLICT (scope_key) DO NOTHING
      """)

      execute("""
      CREATE FUNCTION #{activity_function}()
      RETURNS trigger
      LANGUAGE plpgsql
      VOLATILE
      PARALLEL UNSAFE
      SECURITY INVOKER
      SET search_path TO pg_catalog, pg_temp
      AS $docket_claim_schedule_activity_v1$
      BEGIN
        IF TG_OP = 'TRUNCATE' THEN
          RAISE EXCEPTION 'TRUNCATE docket_runs is unsupported because unfinished counts are trigger-maintained'
            USING ERRCODE = 'integrity_constraint_violation';
        ELSIF TG_OP = 'UPDATE' AND NEW.scope_key IS DISTINCT FROM OLD.scope_key THEN
          RAISE EXCEPTION 'docket run ownership is immutable'
            USING ERRCODE = 'integrity_constraint_violation';
        ELSIF TG_OP = 'INSERT' THEN
          IF NEW.status NOT IN (#{@terminal_status_sql}) THEN
            UPDATE #{schedule}
            SET unfinished_count = unfinished_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE scope_key = NEW.scope_key;

            IF NOT FOUND THEN
              RAISE EXCEPTION 'missing docket claim schedule membership for %', NEW.scope_key
                USING ERRCODE = 'foreign_key_violation';
            END IF;
          END IF;
        ELSIF TG_OP = 'DELETE' THEN
          IF OLD.status NOT IN (#{@terminal_status_sql}) THEN
            UPDATE #{schedule}
            SET unfinished_count = unfinished_count - 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE scope_key = OLD.scope_key AND unfinished_count > 0;

            IF NOT FOUND THEN
              RAISE EXCEPTION 'docket claim schedule unfinished count underflow for %', OLD.scope_key
                USING ERRCODE = 'check_violation';
            END IF;
          END IF;
        ELSIF OLD.status NOT IN (#{@terminal_status_sql}) AND
              NEW.status IN (#{@terminal_status_sql}) THEN
          UPDATE #{schedule}
          SET unfinished_count = unfinished_count - 1,
              updated_at = CURRENT_TIMESTAMP
          WHERE scope_key = OLD.scope_key AND unfinished_count > 0;

          IF NOT FOUND THEN
            RAISE EXCEPTION 'docket claim schedule unfinished count underflow for %', OLD.scope_key
              USING ERRCODE = 'check_violation';
          END IF;
        ELSIF OLD.status IN (#{@terminal_status_sql}) AND
              NEW.status NOT IN (#{@terminal_status_sql}) THEN
          UPDATE #{schedule}
          SET unfinished_count = unfinished_count + 1,
              updated_at = CURRENT_TIMESTAMP
          WHERE scope_key = NEW.scope_key;

          IF NOT FOUND THEN
            RAISE EXCEPTION 'missing docket claim schedule membership for %', NEW.scope_key
              USING ERRCODE = 'foreign_key_violation';
          END IF;
        END IF;

        RETURN NULL;
      END;
      $docket_claim_schedule_activity_v1$
      """)

      execute("""
      CREATE TRIGGER #{@activity_trigger}
      AFTER INSERT OR DELETE OR UPDATE OF status, tenant_id ON #{runs}
      FOR EACH ROW
      EXECUTE FUNCTION #{activity_function}()
      """)

      execute("""
      CREATE TRIGGER #{@run_truncate_guard_trigger}
      BEFORE TRUNCATE ON #{runs}
      FOR EACH STATEMENT
      EXECUTE FUNCTION #{activity_function}()
      """)

      execute("""
      UPDATE #{schedule} AS schedule
      SET unfinished_count = unfinished.unfinished_count,
          updated_at = CURRENT_TIMESTAMP
      FROM (
        SELECT scope_key, count(*)::bigint AS unfinished_count
        FROM #{runs}
        WHERE status NOT IN (#{@terminal_status_sql})
        GROUP BY scope_key
      ) AS unfinished
      WHERE schedule.scope_key = unfinished.scope_key
      """)

      execute("""
      CREATE TRIGGER #{@membership_guard_trigger}
      BEFORE UPDATE OR DELETE ON #{schedule}
      FOR EACH ROW
      EXECUTE FUNCTION #{membership_guard_function}()
      """)

      execute("""
      CREATE TRIGGER #{@schedule_truncate_guard_trigger}
      BEFORE TRUNCATE ON #{schedule}
      FOR EACH STATEMENT
      EXECUTE FUNCTION #{membership_guard_function}()
      """)
    end

    def down(%{prefix: prefix}) do
      runs = qualified_table(prefix, "docket_runs")
      partitions = qualified_table(prefix, "docket_claim_partitions")
      schedule = qualified_table(prefix, "docket_claim_schedule")
      membership_function = qualified_table(prefix, @membership_function)
      membership_guard_function = qualified_table(prefix, @membership_guard_function)
      activity_function = qualified_table(prefix, @activity_function)

      execute("DROP TRIGGER IF EXISTS #{@run_truncate_guard_trigger} ON #{runs}")
      execute("DROP TRIGGER IF EXISTS #{@activity_trigger} ON #{runs}")
      execute("DROP FUNCTION IF EXISTS #{activity_function}()")
      execute("DROP TRIGGER IF EXISTS #{@membership_trigger} ON #{partitions}")
      execute("DROP FUNCTION IF EXISTS #{membership_function}()")
      execute("DROP TRIGGER IF EXISTS #{@schedule_truncate_guard_trigger} ON #{schedule}")
      execute("DROP TRIGGER IF EXISTS #{@membership_guard_trigger} ON #{schedule}")
      execute("DROP FUNCTION IF EXISTS #{membership_guard_function}()")
      execute("DROP TABLE IF EXISTS #{schedule}")

      execute(
        "DROP INDEX IF EXISTS #{qualified_table(prefix, "docket_runs_scope_admitted_expired_index")}"
      )

      execute(
        "DROP INDEX IF EXISTS #{qualified_table(prefix, "docket_runs_scope_queued_ready_index")}"
      )

      execute(
        "DROP INDEX IF EXISTS #{qualified_table(prefix, "docket_runs_scope_admitted_ready_index")}"
      )

      execute(
        "DROP INDEX IF EXISTS #{qualified_table(prefix, "docket_runs_scope_admitted_index")}"
      )

      execute("ALTER TABLE #{runs} DROP CONSTRAINT docket_runs_tenant_admission_shape_check")
      execute("ALTER TABLE #{runs} DROP COLUMN tenant_admitted_at")
      execute("DROP TABLE IF EXISTS #{qualified_table(prefix, "docket_claim_partitions")}")
      execute("DROP TABLE IF EXISTS #{qualified_table(prefix, "docket_claim_policy")}")
    end

    defp qualified_table(prefix, name), do: Storage.qualified_table(prefix, name)
  end
end
