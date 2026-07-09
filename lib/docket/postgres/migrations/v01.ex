if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V01 do
    @moduledoc false

    # Schema version 1: the four tables from the operational transition spec,
    # section 5 (rev 4).
    #
    # Layout invariants this version encodes:
    #
    #   * The row is the run: stable public `Docket.Run` fields are relational
    #     columns; only Docket-owned execution internals live in the `state`
    #     jsonb. No other table stores a run snapshot.
    #   * There is no `docket_graphs` parent table: every 0.1.0 operation is
    #     keyed by `(graph_id, graph_hash)` and touches only version rows.
    #   * `docket_checkpoints` is metadata-only: seq, type, step, park action,
    #     and timestamps.
    #   * `tenant_id` is nullable and appears only on `docket_runs`; tenancy
    #     is an optional scoping concept, never a requirement.
    #   * The dispatch scan index covers only rows with a non-null `wake_at`,
    #     structurally excluding terminal and externally-parked runs.

    use Ecto.Migration

    def up(%{prefix: prefix}) do
      # Content-addressed graph documents, keyed by graph_id + graph_hash.
      # Rows are immutable: publish-on-start upserts with ON CONFLICT DO
      # NOTHING, so there is no updated_at.
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

      # The row is the run: public fields are columns, execution internals
      # (channels, interrupts, pending nodes and writes, active tasks, timers,
      # internal counters) are the `state` jsonb — versioned internally by the
      # document's own `version` field, never interpreted by hosts.
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

      # The dispatch scan.
      create_if_not_exists(
        index(:docket_runs, [:wake_at], where: "wake_at IS NOT NULL", prefix: prefix)
      )

      create_if_not_exists(
        index(:docket_runs, [:operational_status],
          where: "operational_status <> 'active'",
          prefix: prefix
        )
      )

      # Ops introspection.
      create_if_not_exists(index(:docket_runs, [:status, :updated_at], prefix: prefix))

      # Checkpoint history is audit and observability metadata. created_at is
      # when the runtime built the checkpoint; inserted_at is when it was
      # persisted.
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

      # occurred_at is the event's own timestamp (Docket.Event.timestamp);
      # inserted_at is when it was persisted.
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
