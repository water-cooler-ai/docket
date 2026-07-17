if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.Budgets do
    @moduledoc false

    @scan_inspections 32
    @grant_outcomes 8
    @run_lock_attempts 16
    @ready_reconciliation_partitions 32
    @expired_reconciliation_partitions 32
    @reconciliation_cadence_scan_calls 32
    @expired_reconciliation_offset 16

    @spec scan_inspections() :: pos_integer()
    def scan_inspections, do: @scan_inspections

    @spec grant_outcomes() :: pos_integer()
    def grant_outcomes, do: @grant_outcomes

    @spec run_lock_attempts() :: pos_integer()
    def run_lock_attempts, do: @run_lock_attempts

    @spec ready_reconciliation_partitions() :: pos_integer()
    def ready_reconciliation_partitions, do: @ready_reconciliation_partitions

    @spec expired_reconciliation_partitions() :: pos_integer()
    def expired_reconciliation_partitions, do: @expired_reconciliation_partitions

    @spec reconciliation_cadence_scan_calls() :: pos_integer()
    def reconciliation_cadence_scan_calls, do: @reconciliation_cadence_scan_calls

    @spec ready_reconciliation_offset() :: 0
    def ready_reconciliation_offset, do: 0

    @spec expired_reconciliation_offset() :: non_neg_integer()
    def expired_reconciliation_offset, do: @expired_reconciliation_offset

    @spec max_grants_per_scan_call() :: pos_integer()
    def max_grants_per_scan_call, do: @scan_inspections

    @spec max_outcomes_per_scan_call() :: pos_integer()
    def max_outcomes_per_scan_call, do: @scan_inspections * @grant_outcomes

    @spec max_run_lock_attempts_per_scan_call() :: pos_integer()
    def max_run_lock_attempts_per_scan_call,
      do: @scan_inspections * @run_lock_attempts

    @spec max_run_rows_mutated_per_scan_call() :: pos_integer()
    def max_run_rows_mutated_per_scan_call,
      do: @scan_inspections * @grant_outcomes

    @spec as_map() :: map()
    def as_map do
      %{
        scan_inspections: @scan_inspections,
        grant_outcomes: @grant_outcomes,
        run_lock_attempts: @run_lock_attempts,
        ready_reconciliation_partitions: @ready_reconciliation_partitions,
        expired_reconciliation_partitions: @expired_reconciliation_partitions,
        reconciliation_cadence_scan_calls: @reconciliation_cadence_scan_calls,
        ready_reconciliation_offset: 0,
        expired_reconciliation_offset: @expired_reconciliation_offset,
        max_grants_per_scan_call: max_grants_per_scan_call(),
        max_outcomes_per_scan_call: max_outcomes_per_scan_call(),
        max_run_lock_attempts_per_scan_call: max_run_lock_attempts_per_scan_call(),
        max_run_rows_mutated_per_scan_call: max_run_rows_mutated_per_scan_call()
      }
    end
  end
end
