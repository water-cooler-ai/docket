if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V01 do
    @moduledoc false

    use Ecto.Migration

    @durable_status_sql Docket.Run.durable_statuses()
                        |> Enum.map_join(", ", &"'#{&1}'")

    @terminal_status_sql Docket.Run.terminal_statuses()
                         |> Enum.map_join(", ", &"'#{&1}'")

    @run_checks [
      {"docket_runs_status_check", "status IN (#{@durable_status_sql})"},
      {"docket_runs_finished_at_check",
       "(status IN (#{@terminal_status_sql})) = (finished_at IS NOT NULL)"},
      {"docket_runs_claim_pair_check", "(claim_token IS NULL) = (claimed_at IS NULL)"},
      {"docket_runs_poison_pair_check", "(poisoned_at IS NULL) = (poison_reason IS NULL)"},
      {"docket_runs_waiting_terminal_idle_check",
       "status = 'running' OR (claim_token IS NULL AND wake_at IS NULL AND poisoned_at IS NULL)"},
      {"docket_runs_poisoned_shape_check",
       "poisoned_at IS NULL OR (status = 'running' AND claim_token IS NULL AND wake_at IS NULL)"},
      {"docket_runs_running_schedule_check",
       "status <> 'running' OR poisoned_at IS NOT NULL OR " <>
         "((wake_at IS NOT NULL) <> (claim_token IS NOT NULL))"},
      {"docket_runs_counters_check",
       "step >= 0 AND checkpoint_seq >= 0 AND claim_attempts >= 0 AND claim_abandons >= 0"}
    ]

    def up(%{prefix: prefix}) do
      create_if_not_exists table(:docket_graph_versions, primary_key: false, prefix: prefix) do
        add(:id, :bigserial, primary_key: true)
        add(:graph_id, :text, null: false)
        add(:graph_hash, :text, null: false)
        add(:graph, :binary, null: false)
        add(:inserted_at, :timestamptz, null: false)
      end

      create_if_not_exists(
        unique_index(:docket_graph_versions, [:graph_id, :graph_hash], prefix: prefix)
      )

      create_if_not_exists(
        index(:docket_graph_versions, [:graph_id, "inserted_at DESC", "id DESC"],
          name: :docket_graph_versions_revision_order_index,
          prefix: prefix
        )
      )

      create_if_not_exists table(:docket_runs, primary_key: false, prefix: prefix) do
        add(:id, :bigserial, primary_key: true)
        add(:run_id, :text, null: false)
        add(:tenant_id, :text)
        add(:graph_id, :text, null: false)

        add(
          :graph_hash,
          references(:docket_graph_versions,
            column: :graph_hash,
            with: [graph_id: :graph_id],
            type: :text,
            on_delete: :restrict,
            prefix: prefix
          ),
          null: false
        )

        add(:status, :text, null: false)
        add(:step, :integer, null: false, default: 0)
        add(:state, :binary, null: false)
        add(:checkpoint_seq, :bigint, null: false, default: 0)
        add(:latest_checkpoint_type, :text)
        add(:claim_token, :uuid)
        add(:claimed_at, :timestamptz)
        add(:wake_at, :timestamptz)
        add(:claim_attempts, :integer, null: false, default: 0)
        add(:claim_abandons, :integer, null: false, default: 0)
        add(:poisoned_at, :timestamptz)
        add(:poison_reason, :text)
        add(:inserted_at, :timestamptz, null: false)
        add(:started_at, :timestamptz, null: false)
        add(:updated_at, :timestamptz, null: false)
        add(:finished_at, :timestamptz)
      end

      for {name, check} <- @run_checks do
        create(constraint(:docket_runs, name, check: check, prefix: prefix))
      end

      create_if_not_exists(unique_index(:docket_runs, [:run_id], prefix: prefix))

      create_if_not_exists(
        index(:docket_runs, ["started_at DESC", "run_id DESC"],
          name: :docket_runs_list_order_index,
          prefix: prefix
        )
      )

      create_if_not_exists(
        index(:docket_runs, [:tenant_id, "started_at DESC", "run_id DESC"],
          name: :docket_runs_tenant_list_order_index,
          prefix: prefix
        )
      )

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
        index(:docket_runs, [:wake_at, :id],
          where:
            "status = 'running' AND poisoned_at IS NULL AND " <>
              "claim_token IS NULL AND wake_at IS NOT NULL",
          prefix: prefix
        )
      )

      create_if_not_exists(
        index(:docket_runs, [:claimed_at, :id],
          where: "status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL",
          prefix: prefix
        )
      )

      create_if_not_exists(
        index(:docket_runs, [:poisoned_at], where: "poisoned_at IS NOT NULL", prefix: prefix)
      )

      create_if_not_exists(index(:docket_runs, [:status, :updated_at], prefix: prefix))

      create_if_not_exists(
        index(:docket_runs, [:updated_at, :id],
          where: "status IN (#{@terminal_status_sql})",
          prefix: prefix
        )
      )

      create_if_not_exists(index(:docket_runs, [:graph_id, :graph_hash], prefix: prefix))

      create_if_not_exists table(:docket_events, primary_key: false, prefix: prefix) do
        add(:id, :bigserial, primary_key: true)

        add(
          :run_id,
          references(:docket_runs,
            column: :run_id,
            type: :text,
            on_delete: :delete_all,
            prefix: prefix
          ),
          null: false
        )

        add(:seq, :bigint, null: false)
        add(:type, :text, null: false)
        add(:step, :integer, null: false)
        add(:node_id, :text)
        add(:channel_id, :text)
        add(:task_id, :text)
        add(:payload, :binary, null: false)
        add(:metadata, :binary, null: false)
        add(:occurred_at, :timestamptz, null: false)
        add(:inserted_at, :timestamptz, null: false)
      end

      create_if_not_exists(unique_index(:docket_events, [:run_id, :seq], prefix: prefix))
      create_if_not_exists(index(:docket_events, [:inserted_at, :id], prefix: prefix))
    end

    def down(%{prefix: prefix}) do
      drop_if_exists(table(:docket_events, prefix: prefix))
      drop_if_exists(table(:docket_runs, prefix: prefix))
      drop_if_exists(table(:docket_graph_versions, prefix: prefix))
    end
  end
end
