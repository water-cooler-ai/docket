if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V02 do
    @moduledoc false

    use Ecto.Migration

    alias Docket.Postgres.Storage

    def up(%{prefix: prefix}) do
      graphs = Storage.qualified_table(prefix, "docket_graph_versions")
      runs = Storage.qualified_table(prefix, "docket_runs")

      execute("ALTER TABLE #{runs} DROP CONSTRAINT docket_runs_graph_hash_fkey")

      execute("ALTER TABLE #{graphs} ADD COLUMN tenant_id text")

      execute(
        "ALTER TABLE #{graphs} ADD COLUMN scope_key text " <>
          "GENERATED ALWAYS AS (COALESCE(tenant_id, '')) STORED"
      )

      execute("ALTER TABLE #{graphs} ALTER COLUMN scope_key SET NOT NULL")

      execute("ALTER TABLE #{graphs} ALTER COLUMN inserted_at SET DEFAULT clock_timestamp()")

      execute(
        "ALTER TABLE #{graphs} ADD CONSTRAINT docket_graph_versions_tenant_id_check " <>
          "CHECK (tenant_id IS NULL OR tenant_id <> '')"
      )

      execute(
        "ALTER TABLE #{runs} ADD COLUMN scope_key text " <>
          "GENERATED ALWAYS AS (COALESCE(tenant_id, '')) STORED"
      )

      execute("ALTER TABLE #{runs} ALTER COLUMN scope_key SET NOT NULL")

      execute(
        "ALTER TABLE #{runs} ADD CONSTRAINT docket_runs_tenant_id_check " <>
          "CHECK (tenant_id IS NULL OR tenant_id <> '')"
      )

      execute(
        "DROP INDEX #{Storage.qualified_table(prefix, "docket_graph_versions_graph_id_graph_hash_index")}"
      )

      execute(
        "DROP INDEX #{Storage.qualified_table(prefix, "docket_graph_versions_revision_order_index")}"
      )

      # V01 graph rows had no owner. They remain tenantless. Existing runs are
      # the only durable evidence that a tenant previously used a graph, so
      # clone exactly those referenced versions into each proven owner scope.
      # Bytes and publication timestamps are preserved.
      execute("""
      INSERT INTO #{graphs} (tenant_id, graph_id, graph_hash, graph, inserted_at)
      SELECT DISTINCT runs.tenant_id,
                      graphs.graph_id,
                      graphs.graph_hash,
                      graphs.graph,
                      graphs.inserted_at
      FROM #{runs} AS runs
      JOIN #{graphs} AS graphs
        ON graphs.tenant_id IS NULL
       AND graphs.graph_id = runs.graph_id
       AND graphs.graph_hash = runs.graph_hash
      WHERE runs.tenant_id IS NOT NULL
      """)

      execute("""
      CREATE UNIQUE INDEX docket_graph_versions_scope_graph_index
      ON #{graphs} (scope_key, graph_id, graph_hash)
      """)

      execute("""
      CREATE INDEX docket_graph_versions_scope_revision_order_index
      ON #{graphs} (scope_key, graph_id, inserted_at DESC, graph_hash DESC)
      """)

      execute("""
      ALTER TABLE #{runs}
      ADD CONSTRAINT docket_runs_graph_scope_fkey
      FOREIGN KEY (scope_key, graph_id, graph_hash)
      REFERENCES #{graphs} (scope_key, graph_id, graph_hash)
      ON DELETE RESTRICT
      """)
    end

    def down(%{prefix: prefix}) do
      graphs = Storage.qualified_table(prefix, "docket_graph_versions")
      runs = Storage.qualified_table(prefix, "docket_runs")

      execute("ALTER TABLE #{runs} DROP CONSTRAINT docket_runs_graph_scope_fkey")

      # V01 cannot represent scoped copies. Refuse to merge rows whose bytes
      # disagree, then retain the oldest publication for each content address.
      execute("""
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1
          FROM #{graphs}
          GROUP BY graph_id, graph_hash
          HAVING count(DISTINCT graph) > 1
        ) THEN
          RAISE EXCEPTION
            'cannot downgrade Docket V02: scoped graph bytes disagree for one content address';
        END IF;
      END
      $$
      """)

      execute(
        "DROP INDEX #{Storage.qualified_table(prefix, "docket_graph_versions_scope_revision_order_index")}"
      )

      execute(
        "DROP INDEX #{Storage.qualified_table(prefix, "docket_graph_versions_scope_graph_index")}"
      )

      execute("""
      DELETE FROM #{graphs} AS duplicate
      USING #{graphs} AS keeper
      WHERE duplicate.graph_id = keeper.graph_id
        AND duplicate.graph_hash = keeper.graph_hash
        AND (duplicate.inserted_at, duplicate.id) > (keeper.inserted_at, keeper.id)
      """)

      execute("UPDATE #{graphs} SET tenant_id = NULL WHERE tenant_id IS NOT NULL")

      execute("""
      CREATE UNIQUE INDEX docket_graph_versions_graph_id_graph_hash_index
      ON #{graphs} (graph_id, graph_hash)
      """)

      execute("""
      CREATE INDEX docket_graph_versions_revision_order_index
      ON #{graphs} (graph_id, inserted_at DESC, id DESC)
      """)

      execute("""
      ALTER TABLE #{runs}
      ADD CONSTRAINT docket_runs_graph_hash_fkey
      FOREIGN KEY (graph_id, graph_hash)
      REFERENCES #{graphs} (graph_id, graph_hash)
      ON DELETE RESTRICT
      """)

      execute("ALTER TABLE #{runs} DROP CONSTRAINT docket_runs_tenant_id_check")
      execute("ALTER TABLE #{runs} DROP COLUMN scope_key")
      execute("ALTER TABLE #{graphs} DROP CONSTRAINT docket_graph_versions_tenant_id_check")
      execute("ALTER TABLE #{graphs} ALTER COLUMN inserted_at DROP DEFAULT")
      execute("ALTER TABLE #{graphs} DROP COLUMN scope_key")
      execute("ALTER TABLE #{graphs} DROP COLUMN tenant_id")
    end
  end
end
