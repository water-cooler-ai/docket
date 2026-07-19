if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V03 do
    @moduledoc false

    use Ecto.Migration

    alias Docket.Postgres.ClaimPolicy.TenantFair.{RingFunction, RingFunctionV3}
    alias Docket.Postgres.Storage

    def up(%{prefix: prefix}) do
      policy = Storage.qualified_table(prefix, "docket_claim_policy")
      partitions = Storage.qualified_table(prefix, "docket_claim_partitions")
      runs = Storage.qualified_table(prefix, "docket_runs")

      # V3 is a stopped, homogeneous upgrade. The table lock makes the
      # admission backfill and claim-function replacement one atomic boundary
      # with respect to all lifecycle writers.
      execute("LOCK TABLE #{policy} IN SHARE ROW EXCLUSIVE MODE")
      execute("LOCK TABLE #{partitions} IN SHARE ROW EXCLUSIVE MODE")
      execute("LOCK TABLE #{runs} IN ACCESS EXCLUSIVE MODE")
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

      execute(RingFunction.drop_sql(prefix))
      execute(RingFunctionV3.create_sql(prefix))
    end

    def down(%{prefix: prefix}) do
      policy = Storage.qualified_table(prefix, "docket_claim_policy")
      partitions = Storage.qualified_table(prefix, "docket_claim_partitions")
      runs = Storage.qualified_table(prefix, "docket_runs")

      execute("LOCK TABLE #{policy} IN SHARE ROW EXCLUSIVE MODE")
      execute("LOCK TABLE #{partitions} IN SHARE ROW EXCLUSIVE MODE")
      execute("LOCK TABLE #{runs} IN ACCESS EXCLUSIVE MODE")
      execute(RingFunctionV3.drop_sql(prefix))

      execute(
        "DROP INDEX #{Storage.qualified_table(prefix, "docket_runs_scope_admitted_expired_index")}"
      )

      execute(
        "DROP INDEX #{Storage.qualified_table(prefix, "docket_runs_scope_queued_ready_index")}"
      )

      execute(
        "DROP INDEX #{Storage.qualified_table(prefix, "docket_runs_scope_admitted_ready_index")}"
      )

      execute("DROP INDEX #{Storage.qualified_table(prefix, "docket_runs_scope_admitted_index")}")

      execute("ALTER TABLE #{runs} DROP CONSTRAINT docket_runs_tenant_admission_shape_check")

      execute("ALTER TABLE #{runs} DROP COLUMN tenant_admitted_at")
      execute(RingFunction.create_sql(prefix))
    end
  end
end
