if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.FairRotationTargetWitness do
    @moduledoc false

    @fields [
      "eligible",
      "ready",
      "admitted_ready",
      "queued_promotion",
      "expired",
      "ready_poison",
      "admitted_ready_poison",
      "queued_poison_promotion",
      "expired_poison"
    ]

    @doc false
    def read!(repo, scope, now, cutoff, max_attempts) do
      boolean_projection =
        Enum.map_join(@fields, ",\n        ", fn field ->
          "(witness->>'#{field}')::boolean"
        end)

      [
        [
          eligible,
          ready,
          admitted_ready,
          queued_promotion,
          expired,
          ready_poison,
          admitted_ready_poison,
          queued_poison_promotion,
          expired_poison,
          cap,
          admitted_count,
          queued_head_id
        ]
      ] =
        repo.query!(
          """
          WITH result(witness) AS MATERIALIZED (
            #{query("$1", "$2", "$3", "$4")}
          )
          SELECT
            #{boolean_projection},
            (witness->>'cap')::bigint,
            (witness->>'admitted_count')::bigint,
            (witness->>'queued_head_id')::bigint
          FROM result
          """,
          [scope, now, cutoff, max_attempts]
        ).rows

      Map.new(
        Enum.zip(
          @fields ++ ["cap", "admitted_count", "queued_head_id"],
          [
            eligible,
            ready,
            admitted_ready,
            queued_promotion,
            expired,
            ready_poison,
            admitted_ready_poison,
            queued_poison_promotion,
            expired_poison,
            cap,
            admitted_count,
            queued_head_id
          ]
        )
      )
    end

    @doc false
    def query(scope, now, cutoff, max_attempts)
        when is_binary(scope) and is_binary(now) and is_binary(cutoff) and
               is_binary(max_attempts) do
      """
      WITH authority AS MATERIALIZED (
        SELECT COALESCE(partition.max_active, policy.max_active)::bigint AS cap,
               (SELECT count(*)::bigint
                FROM docket_runs AS admitted
                WHERE admitted.scope_key = #{scope}
                  AND admitted.status = 'running'
                  AND admitted.poisoned_at IS NULL
                  AND admitted.tenant_admitted_at IS NOT NULL) AS admitted_count
        FROM docket_claim_partitions AS partition
        CROSS JOIN docket_claim_policy AS policy
        WHERE partition.scope_key = #{scope} AND policy.id = 1
      ), queued_head AS MATERIALIZED (
        SELECT queued.id, queued.claim_attempts
        FROM docket_runs AS queued
        WHERE queued.scope_key = #{scope}
          AND queued.status = 'running'
          AND queued.poisoned_at IS NULL
          AND queued.tenant_admitted_at IS NULL
          AND queued.claim_token IS NULL
          AND queued.wake_at IS NOT NULL
          AND queued.wake_at <= #{now}
        ORDER BY queued.wake_at, queued.id
        LIMIT 1
      ), classes AS (
        SELECT
          EXISTS (
            SELECT 1
            FROM docket_runs AS ready
            WHERE ready.scope_key = #{scope}
              AND ready.status = 'running'
              AND ready.poisoned_at IS NULL
              AND ready.tenant_admitted_at IS NOT NULL
              AND ready.claim_token IS NULL
              AND ready.wake_at IS NOT NULL
              AND ready.wake_at <= #{now}
              AND ready.claim_attempts < #{max_attempts}
          ) AS admitted_ready,
          EXISTS (
            SELECT 1
            FROM queued_head, authority
            WHERE queued_head.claim_attempts < #{max_attempts}
              AND authority.admitted_count < authority.cap
          ) AS queued_promotion,
          EXISTS (
            SELECT 1
            FROM docket_runs AS expired
            WHERE expired.scope_key = #{scope}
              AND expired.status = 'running'
              AND expired.poisoned_at IS NULL
              AND expired.tenant_admitted_at IS NOT NULL
              AND expired.claim_token IS NOT NULL
              AND expired.claimed_at < #{cutoff}
              AND expired.claim_attempts < #{max_attempts}
          ) AS expired,
          EXISTS (
            SELECT 1
            FROM docket_runs AS ready_poison
            WHERE ready_poison.scope_key = #{scope}
              AND ready_poison.status = 'running'
              AND ready_poison.poisoned_at IS NULL
              AND ready_poison.tenant_admitted_at IS NOT NULL
              AND ready_poison.claim_token IS NULL
              AND ready_poison.wake_at IS NOT NULL
              AND ready_poison.wake_at <= #{now}
              AND ready_poison.claim_attempts >= #{max_attempts}
          ) AS admitted_ready_poison,
          EXISTS (
            SELECT 1
            FROM queued_head, authority
            WHERE queued_head.claim_attempts >= #{max_attempts}
              AND authority.admitted_count < authority.cap
          ) AS queued_poison_promotion,
          EXISTS (
            SELECT 1
            FROM docket_runs AS expired_poison
            WHERE expired_poison.scope_key = #{scope}
              AND expired_poison.status = 'running'
              AND expired_poison.poisoned_at IS NULL
              AND expired_poison.tenant_admitted_at IS NOT NULL
              AND expired_poison.claim_token IS NOT NULL
              AND expired_poison.claimed_at < #{cutoff}
              AND expired_poison.claim_attempts >= #{max_attempts}
          ) AS expired_poison,
          (SELECT cap FROM authority) AS cap,
          (SELECT admitted_count FROM authority) AS admitted_count,
          (SELECT id FROM queued_head) AS queued_head_id
      )
      SELECT jsonb_build_object(
        'eligible', admitted_ready OR queued_promotion OR expired OR
                    admitted_ready_poison OR queued_poison_promotion OR expired_poison,
        'ready', admitted_ready OR queued_promotion,
        'admitted_ready', admitted_ready,
        'queued_promotion', queued_promotion,
        'expired', expired,
        'ready_poison', admitted_ready_poison OR queued_poison_promotion,
        'admitted_ready_poison', admitted_ready_poison,
        'queued_poison_promotion', queued_poison_promotion,
        'expired_poison', expired_poison,
        'cap', cap,
        'admitted_count', admitted_count,
        'queued_head_id', queued_head_id
      )
      FROM classes
      """
    end
  end
end
