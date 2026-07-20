if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.SQL do
    @moduledoc """
    Builds the production-facing call to the TenantFair ring function.

    The installed function returns a superset record used by raw trace tests.
    This wrapper fixes trace to `false`, filters out internal inspection rows,
    projects the stable fourteen decoder columns, and imposes deterministic
    visit/outcome ordering. It contains no discovery or scheduling policy; that
    work belongs to `RingFunction` inside the same database statement.
    """

    alias Docket.Postgres.ClaimPolicy.TenantFair.RingFunction

    def statement(function) when is_binary(function) do
      """
      SELECT
        #{RingFunction.public_projection()}
      FROM #{function}(
        $1,
        $2,
        $3,
        $4,
        $5,
        false
      ) AS claimed(
        #{RingFunction.result_definition()}
      )
      WHERE claimed.row_kind IN ('outcome', 'error')
      ORDER BY claimed.visit_ordinal NULLS FIRST,
               claimed.outcome_ordinal NULLS FIRST
      """
    end
  end
end
