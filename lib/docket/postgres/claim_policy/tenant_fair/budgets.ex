if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.Budgets do
    @moduledoc false

    @scan_inspections 32
    @grant_outcomes 8
    @run_lock_attempts 16

    @spec scan_inspections() :: pos_integer()
    def scan_inspections, do: @scan_inspections

    @spec grant_outcomes() :: pos_integer()
    def grant_outcomes, do: @grant_outcomes

    @spec run_lock_attempts() :: pos_integer()
    def run_lock_attempts, do: @run_lock_attempts

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
        max_grants_per_scan_call: max_grants_per_scan_call(),
        max_outcomes_per_scan_call: max_outcomes_per_scan_call(),
        max_run_lock_attempts_per_scan_call: max_run_lock_attempts_per_scan_call(),
        max_run_rows_mutated_per_scan_call: max_run_rows_mutated_per_scan_call()
      }
    end
  end
end
