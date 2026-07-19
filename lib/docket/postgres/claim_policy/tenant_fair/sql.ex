if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.SQL do
    @moduledoc """
    Builds the production-facing call to the TenantFair ring function.

    The installed function returns a superset record used by raw trace tests.
    This wrapper fixes trace to `false`, filters out internal inspection rows,
    projects the stable fourteen decoder columns, and imposes deterministic
    visit/outcome ordering. It contains no discovery or scheduling policy; that
    work belongs to `RingFunctionV3` inside the same database statement.
    """

    alias Docket.Postgres.ClaimPolicy.TenantFair.RingFunctionV3

    def statement(function) when is_binary(function) do
      """
      SELECT
        #{RingFunctionV3.public_projection()}
      FROM #{function}(
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        false,
        3
      ) AS claimed(
        #{RingFunctionV3.result_definition()}
      )
      WHERE claimed.row_kind IN ('outcome', 'error')
      ORDER BY claimed.visit_ordinal NULLS FIRST,
               claimed.outcome_ordinal NULLS FIRST
      """
    end
  end
end
