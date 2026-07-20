if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.QueryShapes do
    @moduledoc false

    alias Docket.Postgres.ClaimPolicy.TenantFair.Budgets

    @doc """
    Returns the fixed-budget circular active-tenant traversal.

    `$1` is the last committed ring position. The query reads scheduling rows
    without locking them. A future admission function must attempt partition
    authority separately so a lock skip remains one explicit inspection. The
    recursive seek intentionally revisits the ring after wrap when `H < S`.
    """
    def scan_positions(schedule) when is_binary(schedule) do
      budget = Budgets.scan_inspections()

      """
      WITH RECURSIVE inspected AS (
        SELECT next.ring_position,
               next.scope_key,
               next.unfinished_count,
               1::integer AS visit_ordinal,
               next.wrap_delta::bigint AS wrap_count
        FROM LATERAL (
          SELECT option.ring_position,
                 option.scope_key,
                 option.unfinished_count,
                 option.wrap_delta
          FROM (
            (SELECT ring_position, scope_key, unfinished_count,
                    0::integer AS wrap_delta
             FROM #{schedule}
             WHERE unfinished_count > 0 AND ring_position > $1
             ORDER BY ring_position
             LIMIT 1)
            UNION ALL
            (SELECT ring_position, scope_key, unfinished_count,
                    1::integer AS wrap_delta
             FROM #{schedule}
             WHERE unfinished_count > 0 AND ring_position <= $1
             ORDER BY ring_position
             LIMIT 1)
          ) AS option
          ORDER BY option.wrap_delta, option.ring_position
          LIMIT 1
        ) AS next

        UNION ALL

        SELECT next.ring_position,
               next.scope_key,
               next.unfinished_count,
               inspected.visit_ordinal + 1,
               inspected.wrap_count + next.wrap_delta
        FROM inspected
        CROSS JOIN LATERAL (
          SELECT option.ring_position,
                 option.scope_key,
                 option.unfinished_count,
                 option.wrap_delta
          FROM (
            (SELECT ring_position, scope_key, unfinished_count,
                    0::integer AS wrap_delta
             FROM #{schedule}
             WHERE unfinished_count > 0 AND ring_position > inspected.ring_position
             ORDER BY ring_position
             LIMIT 1)
            UNION ALL
            (SELECT ring_position, scope_key, unfinished_count,
                    1::integer AS wrap_delta
             FROM #{schedule}
             WHERE unfinished_count > 0 AND ring_position <= inspected.ring_position
             ORDER BY ring_position
             LIMIT 1)
          ) AS option
          ORDER BY option.wrap_delta, option.ring_position
          LIMIT 1
        ) AS next
        WHERE inspected.visit_ordinal < #{budget}
      )
      SELECT ring_position, scope_key, unfinished_count, visit_ordinal, wrap_count
      FROM inspected
      ORDER BY visit_ordinal
      """
      |> String.trim()
    end

    @doc """
    Returns an exact-partition structural candidate page bounded by `K`.

    The claim function combines the candidate pages on their bounded relation,
    applies class reservation and admission rules, then locks only the selected
    exact IDs. The authoritative rechecks remain under run locks.
    """
    def run_candidates(runs, :admitted_ready) when is_binary(runs) do
      candidate_page(runs, :admitted_ready)
    end

    def run_candidates(runs, :queued_ready) when is_binary(runs) do
      candidate_page(runs, :queued_ready)
    end

    def run_candidates(runs, :expired) when is_binary(runs) do
      candidate_page(runs, :expired)
    end

    @doc "Locks the one domain-global scan cursor without skipping it."
    def scan_cursor_lock(policy) when is_binary(policy) do
      """
      SELECT scan_ring_position AS ring_position
      FROM #{policy}
      WHERE id = 1
      FOR UPDATE
      """
      |> String.trim()
    end

    @doc "Attempts authority for exactly one inspected partition."
    def partition_lock_attempt(partitions) when is_binary(partitions) do
      """
      SELECT scope_key, max_active, partition_version, admission_epoch
      FROM #{partitions}
      WHERE scope_key = $1
      FOR NO KEY UPDATE SKIP LOCKED
      """
      |> String.trim()
    end

    @doc """
    Locks only an already-bounded exact-key attempt set.

    `$1` is a bigint array. The array is truncated before the primary-key
    join, so `SKIP LOCKED` cannot scan past an unbounded locked prefix.
    """
    def exact_run_lock_attempts(runs) when is_binary(runs) do
      attempts = Budgets.run_lock_attempts()

      """
      WITH requested AS MATERIALIZED (
        SELECT requested.id, requested.ordinality
        FROM unnest(($1::bigint[])[1:#{attempts}])
          WITH ORDINALITY AS requested(id, ordinality)
        ORDER BY requested.ordinality
        LIMIT #{attempts}
      )
      SELECT runs.id, requested.ordinality
      FROM requested
      JOIN #{runs} AS runs ON runs.id = requested.id
      ORDER BY requested.ordinality
      FOR UPDATE OF runs SKIP LOCKED
      """
      |> String.trim()
    end

    @doc """
    Bounds the exact mutation input to `Q` rows.

    The claim function performs the class, cap, and state recheck before its
    final update; this shape fixes the maximum row input for one grant.
    """
    def mutation_ids do
      outcomes = Budgets.grant_outcomes()

      """
      SELECT requested.id, requested.ordinality
      FROM unnest(($1::bigint[])[1:#{outcomes}])
        WITH ORDINALITY AS requested(id, ordinality)
      ORDER BY requested.ordinality
      LIMIT #{outcomes}
      """
      |> String.trim()
    end

    defp candidate_parts(:admitted_ready) do
      {"candidate.status = 'running' AND candidate.poisoned_at IS NULL AND " <>
         "candidate.tenant_admitted_at IS NOT NULL AND candidate.claim_token IS NULL AND " <>
         "candidate.wake_at IS NOT NULL", "wake_at", "<="}
    end

    defp candidate_parts(:queued_ready) do
      {"candidate.status = 'running' AND candidate.poisoned_at IS NULL AND " <>
         "candidate.tenant_admitted_at IS NULL AND candidate.claim_token IS NULL AND " <>
         "candidate.wake_at IS NOT NULL", "wake_at", "<="}
    end

    defp candidate_parts(:expired) do
      {"candidate.status = 'running' AND candidate.poisoned_at IS NULL AND " <>
         "candidate.tenant_admitted_at IS NOT NULL AND candidate.claim_token IS NOT NULL",
       "claimed_at", "<"}
    end

    defp candidate_page(runs, class) do
      budget = Budgets.run_lock_attempts()
      {predicate, eligible_column, due_operator} = candidate_parts(class)

      """
      SELECT candidate.id,
             candidate.#{eligible_column} AS eligible_at,
             candidate.claim_attempts,
             '#{class}'::text AS work_class
      FROM #{runs} AS candidate
      WHERE #{predicate}
        AND candidate.scope_key = $1
        AND candidate.#{eligible_column} #{due_operator} $2
      ORDER BY candidate.#{eligible_column}, candidate.id
      LIMIT #{budget}
      """
      |> String.trim()
    end
  end
end
