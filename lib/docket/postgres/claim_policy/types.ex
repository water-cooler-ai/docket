if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.Types do
    @moduledoc false

    @admin_states [:running, :hold_new, :drain]
    @target_kinds [:default, :partition, :bulk, :activation, :readiness, :audit]
    @outcomes [:applied, :unchanged, :demoted]
    @assertion_kinds [:dual_write, :old_binaries_absent]
    @backfill_phases [:not_started, :running, :reconciling, :complete]
    @fk_dispositions [:absent, :not_valid, :validated]
    @readiness_states [:not_ready, :ready]
    @admission_modes [:legacy, :tenant_fair]

    def admin_states, do: @admin_states
    def target_kinds, do: @target_kinds
    def outcomes, do: @outcomes
    def assertion_kinds, do: @assertion_kinds
    def backfill_phases, do: @backfill_phases
    def fk_dispositions, do: @fk_dispositions
    def readiness_states, do: @readiness_states
    def admission_modes, do: @admission_modes
  end
end
