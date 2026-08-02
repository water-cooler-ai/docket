if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V03 do
    @moduledoc false

    use Ecto.Migration

    # V01's running-schedule shape: a running unpoisoned run carries exactly
    # one of wake_at/claim_token. V03 relaxes it to at most one - a running
    # run parked on pending detached work with no live deadline carries
    # neither and is woken externally by a claim or a committed mutation.
    @schedule_check_name "docket_runs_running_schedule_check"
    @v01_schedule_check "status <> 'running' OR poisoned_at IS NOT NULL OR " <>
                          "((wake_at IS NOT NULL) <> (claim_token IS NOT NULL))"
    @v03_schedule_check "status <> 'running' OR poisoned_at IS NOT NULL OR " <>
                          "NOT (wake_at IS NOT NULL AND claim_token IS NOT NULL)"

    def up(%{prefix: prefix}) do
      create_if_not_exists table(:docket_detached_tasks, primary_key: false, prefix: prefix) do
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

        add(:tenant_id, :text)

        add(:scope_key, :text,
          generated: "ALWAYS AS (COALESCE(tenant_id, '')) STORED",
          null: false
        )

        add(:task_id, :text, null: false)
        add(:node_id, :text, null: false)
        add(:attempt, :integer, null: false)
        add(:state, :text, null: false)
        add(:scheduled_at, :timestamptz, null: false)
        add(:claimed_at, :timestamptz)
        add(:deadline_at, :timestamptz)
      end

      create(
        constraint(:docket_detached_tasks, :docket_detached_tasks_tenant_id_check,
          check: "tenant_id IS NULL OR tenant_id <> ''",
          prefix: prefix
        )
      )

      create(
        constraint(:docket_detached_tasks, :docket_detached_tasks_state_check,
          check: "state IN ('pending', 'claimed')",
          prefix: prefix
        )
      )

      create(
        constraint(:docket_detached_tasks, :docket_detached_tasks_claim_pair_check,
          check: "(state = 'claimed') = (claimed_at IS NOT NULL)",
          prefix: prefix
        )
      )

      create(
        constraint(:docket_detached_tasks, :docket_detached_tasks_claimed_deadline_check,
          check: "state = 'pending' OR deadline_at IS NOT NULL",
          prefix: prefix
        )
      )

      create(
        constraint(:docket_detached_tasks, :docket_detached_tasks_attempt_check,
          check: "attempt > 0",
          prefix: prefix
        )
      )

      create_if_not_exists(
        unique_index(:docket_detached_tasks, [:run_id, :task_id], prefix: prefix)
      )

      create_if_not_exists(
        index(:docket_detached_tasks, [:scope_key, :scheduled_at],
          where: "state = 'pending'",
          name: :docket_detached_tasks_pending_index,
          prefix: prefix
        )
      )

      drop_if_exists(constraint(:docket_runs, @schedule_check_name, prefix: prefix))

      create(
        constraint(:docket_runs, @schedule_check_name,
          check: @v03_schedule_check,
          prefix: prefix
        )
      )
    end

    def down(%{prefix: prefix}) do
      drop_if_exists(table(:docket_detached_tasks, prefix: prefix))

      drop_if_exists(constraint(:docket_runs, @schedule_check_name, prefix: prefix))

      # Runs in the V03-only shape (running, unpoisoned, neither wake nor
      # claim) violate the exact-one form the recreated constraint validates
      # against; they become immediately due instead.
      execute(
        "UPDATE #{Docket.Postgres.Storage.qualified_table(prefix, "docket_runs")} " <>
          "SET wake_at = clock_timestamp() WHERE status = 'running' " <>
          "AND poisoned_at IS NULL AND wake_at IS NULL AND claim_token IS NULL"
      )

      create(
        constraint(:docket_runs, @schedule_check_name,
          check: @v01_schedule_check,
          prefix: prefix
        )
      )
    end
  end
end
