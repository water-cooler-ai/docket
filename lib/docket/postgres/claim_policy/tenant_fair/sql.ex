if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.SQL do
    @moduledoc """
    Canonical one-statement TenantFair discovery and function invocation.

    The outer discovery is deliberately tenant-blind and bounded. It supplies
    a non-authoritative key hint to exactly one function invocation; DCKT-49
    owns later rotation, cursors, and bounded-lag discovery.
    """

    alias Docket.Postgres.ClaimPolicy.TenantFair.Function

    def statement(runs, function) when is_binary(runs) and is_binary(function) do
      """
      WITH candidate_partitions AS MATERIALIZED (
        SELECT eligible.scope_key, min(eligible.eligible_at) AS eligible_at
        FROM (
          SELECT runs.scope_key, runs.wake_at AS eligible_at
          FROM #{runs} AS runs
          WHERE runs.status = 'running'
            AND runs.poisoned_at IS NULL
            AND runs.claim_token IS NULL
            AND runs.wake_at IS NOT NULL
            AND runs.wake_at <= $1
          UNION ALL
          SELECT runs.scope_key, runs.claimed_at AS eligible_at
          FROM #{runs} AS runs
          WHERE runs.status = 'running'
            AND runs.poisoned_at IS NULL
            AND runs.claim_token IS NOT NULL
            AND runs.claimed_at < $2
        ) AS eligible
        GROUP BY eligible.scope_key
        ORDER BY min(eligible.eligible_at), eligible.scope_key
        LIMIT LEAST($3, #{Function.partition_budget()})
      ),
      candidate_keys AS MATERIALIZED (
        SELECT COALESCE(
                 array_agg(scope_key ORDER BY scope_key),
                 ARRAY[]::text[]
               ) AS keys
        FROM candidate_partitions
      )
      SELECT claimed.*
      FROM #{function}(
        $1,
        $2,
        $3,
        $4,
        $5,
        (SELECT keys FROM candidate_keys)
      ) AS claimed(
        #{Function.result_definition()}
      )
      """
    end

    def sha256(runs, function), do: :crypto.hash(:sha256, statement(runs, function))
  end
end
