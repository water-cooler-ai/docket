if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V01 do
    @moduledoc false

    use Ecto.Migration

    def up(%{prefix: prefix}) do
      create_if_not_exists table(:docket_graph_versions, primary_key: false, prefix: prefix) do
        add(:id, :bigserial, primary_key: true)
        add(:graph_id, :text, null: false)
        add(:graph_hash, :text, null: false)
        add(:graph, :jsonb, null: false)
        add(:inserted_at, :timestamptz, null: false)
      end

      create_if_not_exists(
        unique_index(:docket_graph_versions, [:graph_id, :graph_hash], prefix: prefix)
      )

      create_if_not_exists table(:docket_runs, primary_key: false, prefix: prefix) do
        add(:id, :bigserial, primary_key: true)
        add(:run_id, :text, null: false)
        add(:tenant_id, :text)
        add(:graph_id, :text, null: false)
        add(:graph_hash, :text, null: false)
        add(:status, :text, null: false)
        add(:step, :integer, null: false, default: 0)
        add(:input, :jsonb, null: false)
        add(:output, :jsonb)
        add(:metadata, :jsonb, null: false, default: fragment("'{}'::jsonb"))
        add(:state, :jsonb, null: false)
        add(:checkpoint_seq, :bigint, null: false, default: 0)
        add(:latest_checkpoint_type, :text)
        add(:claim_token, :uuid)
        add(:claimed_at, :timestamptz)
        add(:wake_at, :timestamptz)
        add(:attempts, :integer, null: false, default: 0)
        add(:operational_status, :text, null: false, default: "active")
        add(:operational_error, :jsonb)
        add(:inserted_at, :timestamptz, null: false)
        add(:started_at, :timestamptz)
        add(:updated_at, :timestamptz, null: false)
        add(:finished_at, :timestamptz)
      end

      create_if_not_exists(unique_index(:docket_runs, [:run_id], prefix: prefix))

      execute("""
      ALTER TABLE #{prefix}.docket_runs
      ADD CONSTRAINT docket_runs_graph_version_fkey
      FOREIGN KEY (graph_id, graph_hash)
      REFERENCES #{prefix}.docket_graph_versions (graph_id, graph_hash)
      ON DELETE RESTRICT
      """)

      create_if_not_exists(
        index(:docket_runs, [:tenant_id, :status],
          where: "tenant_id IS NOT NULL",
          prefix: prefix
        )
      )

      create_if_not_exists(
        index(:docket_runs, [:tenant_id, :graph_id, :status],
          where: "tenant_id IS NOT NULL",
          prefix: prefix
        )
      )

      create_if_not_exists(
        index(:docket_runs, [:wake_at], where: "wake_at IS NOT NULL", prefix: prefix)
      )

      create_if_not_exists(
        index(:docket_runs, [:operational_status],
          where: "operational_status <> 'active'",
          prefix: prefix
        )
      )

      create_if_not_exists(index(:docket_runs, [:status, :updated_at], prefix: prefix))

      create_if_not_exists table(:docket_checkpoints, primary_key: false, prefix: prefix) do
        add(:id, :bigserial, primary_key: true)
        add(:run_id, :text, null: false)
        add(:seq, :bigint, null: false)
        add(:type, :text, null: false)
        add(:step, :integer, null: false)
        add(:park_action, :text)
        add(:created_at, :timestamptz, null: false)
        add(:inserted_at, :timestamptz, null: false)
      end

      create_if_not_exists(unique_index(:docket_checkpoints, [:run_id, :seq], prefix: prefix))

      create_if_not_exists table(:docket_events, primary_key: false, prefix: prefix) do
        add(:id, :bigserial, primary_key: true)
        add(:run_id, :text, null: false)
        add(:seq, :bigint, null: false)
        add(:type, :text, null: false)
        add(:step, :integer, null: false)
        add(:node_id, :text)
        add(:channel_id, :text)
        add(:task_id, :text)
        add(:payload, :jsonb, null: false, default: fragment("'{}'::jsonb"))
        add(:metadata, :jsonb, null: false, default: fragment("'{}'::jsonb"))
        add(:occurred_at, :timestamptz, null: false)
        add(:inserted_at, :timestamptz, null: false)
      end

      create_if_not_exists(unique_index(:docket_events, [:run_id, :seq], prefix: prefix))
    end

    def down(%{prefix: prefix}) do
      drop_if_exists(table(:docket_events, prefix: prefix))
      drop_if_exists(table(:docket_checkpoints, prefix: prefix))
      drop_if_exists(table(:docket_runs, prefix: prefix))
      drop_if_exists(table(:docket_graph_versions, prefix: prefix))
    end
  end
end
