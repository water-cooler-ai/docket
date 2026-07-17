if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V02 do
    @moduledoc false

    use Ecto.Migration

    alias Docket.Postgres.ClaimPolicy.TenantFair.Function
    alias Docket.Postgres.Storage

    def up(%{prefix: prefix}) do
      policy = qualified_table(prefix, "docket_claim_policy")
      partitions = qualified_table(prefix, "docket_claim_partitions")
      runs = qualified_table(prefix, "docket_runs")

      execute("""
      CREATE TABLE #{policy} (
        id smallint PRIMARY KEY DEFAULT 1,
        admission_mode varchar(16) NOT NULL DEFAULT 'legacy',
        max_active integer NULL,
        policy_version bigint NOT NULL DEFAULT 0,
        initialized_at timestamptz NULL,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_policy_singleton_check CHECK (id = 1),
        CONSTRAINT docket_claim_policy_mode_check CHECK (
          admission_mode IN ('legacy', 'tenant_fair')
        ),
        CONSTRAINT docket_claim_policy_version_check CHECK (policy_version >= 0),
        CONSTRAINT docket_claim_policy_shape_check CHECK (
          (max_active IS NULL AND policy_version = 0 AND initialized_at IS NULL) OR
          (max_active > 0 AND max_active <= 2147483647 AND
           policy_version >= 1 AND initialized_at IS NOT NULL)
        )
      )
      """)

      execute("""
      CREATE TABLE #{partitions} (
        scope_key text PRIMARY KEY,
        max_active integer NULL,
        partition_version bigint NOT NULL DEFAULT 0,
        admission_epoch bigint NOT NULL DEFAULT 0,
        inserted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_claim_partitions_max_check CHECK (
          max_active IS NULL OR (max_active > 0 AND max_active <= 2147483647)
        ),
        CONSTRAINT docket_claim_partitions_version_check CHECK (partition_version >= 0),
        CONSTRAINT docket_claim_partitions_epoch_check CHECK (admission_epoch >= 0)
      )
      """)

      execute("INSERT INTO #{policy} (id) VALUES (1)")

      # This ordinary transactional backfill is sufficient for the v0.1.0
      # migration contract. Block v1 inserts from crossing the snapshot; new
      # binaries maintain the invariant transactionally after this commit.
      execute("LOCK TABLE #{runs} IN SHARE MODE")

      execute("""
      INSERT INTO #{partitions} (scope_key)
      SELECT DISTINCT scope_key FROM #{runs}
      ON CONFLICT (scope_key) DO NOTHING
      """)

      execute("""
      CREATE INDEX docket_runs_scope_ready_index
      ON #{runs} (scope_key, wake_at, id)
      WHERE status = 'running' AND poisoned_at IS NULL AND
            claim_token IS NULL AND wake_at IS NOT NULL
      """)

      execute("""
      CREATE INDEX docket_runs_scope_live_index
      ON #{runs} (scope_key, id)
      WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL
      """)

      execute("""
      CREATE INDEX docket_runs_scope_expired_index
      ON #{runs} (scope_key, claimed_at, id)
      WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL
      """)

      execute(Function.create_sql(prefix))
    end

    def down(%{prefix: prefix}) do
      execute(Function.drop_sql(prefix))

      execute(
        "DROP INDEX IF EXISTS #{qualified_table(prefix, "docket_runs_scope_expired_index")}"
      )

      execute("DROP INDEX IF EXISTS #{qualified_table(prefix, "docket_runs_scope_live_index")}")
      execute("DROP INDEX IF EXISTS #{qualified_table(prefix, "docket_runs_scope_ready_index")}")
      execute("DROP TABLE IF EXISTS #{qualified_table(prefix, "docket_claim_partitions")}")
      execute("DROP TABLE IF EXISTS #{qualified_table(prefix, "docket_claim_policy")}")
    end

    defp qualified_table(prefix, name), do: Storage.qualified_table(prefix, name)
  end
end
