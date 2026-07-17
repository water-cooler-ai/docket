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
               next.ready_candidate_cursor_at,
               next.ready_candidate_cursor_id,
               1::integer AS visit_ordinal,
               next.wrap_delta::bigint AS wrap_count
        FROM LATERAL (
          SELECT option.ring_position,
                 option.scope_key,
                 option.unfinished_count,
                 option.ready_candidate_cursor_at,
                 option.ready_candidate_cursor_id,
                 option.wrap_delta
          FROM (
            (SELECT ring_position, scope_key, unfinished_count,
                    ready_candidate_cursor_at, ready_candidate_cursor_id,
                    0::integer AS wrap_delta
             FROM #{schedule}
             WHERE unfinished_count > 0 AND ring_position > $1
             ORDER BY ring_position
             LIMIT 1)
            UNION ALL
            (SELECT ring_position, scope_key, unfinished_count,
                    ready_candidate_cursor_at, ready_candidate_cursor_id,
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
               next.ready_candidate_cursor_at,
               next.ready_candidate_cursor_id,
               inspected.visit_ordinal + 1,
               inspected.wrap_count + next.wrap_delta
        FROM inspected
        CROSS JOIN LATERAL (
          SELECT option.ring_position,
                 option.scope_key,
                 option.unfinished_count,
                 option.ready_candidate_cursor_at,
                 option.ready_candidate_cursor_id,
                 option.wrap_delta
          FROM (
            (SELECT ring_position, scope_key, unfinished_count,
                    ready_candidate_cursor_at, ready_candidate_cursor_id,
                    0::integer AS wrap_delta
             FROM #{schedule}
             WHERE unfinished_count > 0 AND ring_position > inspected.ring_position
             ORDER BY ring_position
             LIMIT 1)
            UNION ALL
            (SELECT ring_position, scope_key, unfinished_count,
                    ready_candidate_cursor_at, ready_candidate_cursor_id,
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
      SELECT ring_position, scope_key, unfinished_count,
             ready_candidate_cursor_at, ready_candidate_cursor_id,
             visit_ordinal, wrap_count
      FROM inspected
      ORDER BY visit_ordinal
      """
      |> String.trim()
    end

    @doc """
    Returns an exact-partition structural candidate page bounded by `K`.

    DCKT-78 must combine the ready and expired pages on their at-most `2K`
    relation, apply class reservation and attempt/cap rules, then lock only the
    selected exact IDs. The authoritative rechecks remain under run locks.
    """
    def run_candidates(runs, :ready) when is_binary(runs) do
      candidate_page(runs, :ready)
    end

    def run_candidates(runs, :expired) when is_binary(runs) do
      candidate_page(runs, :expired)
    end

    @doc """
    Continues one exact-partition candidate walk after a zero-outcome page.

    `$1` is scope, `$2` is the class time threshold, and `$3/$4` are the last
    inspected eligible time and run ID. The two keyset halves return at most
    `K` rows across one wrap. DCKT-78 uses this continuation for capped ready
    work so ordinary rows cannot permanently hide a later poison row; normal
    below-cap selection still uses `run_candidates/2` from the oldest head.
    """
    def rotating_run_candidates(runs, :ready) when is_binary(runs) do
      budget = Budgets.run_lock_attempts()
      class = :ready
      {predicate, eligible_column, due_operator} = candidate_parts(:ready)

      """
      WITH after_cursor AS MATERIALIZED (
        SELECT candidate.id,
               candidate.#{eligible_column} AS eligible_at,
               candidate.claim_attempts,
               '#{class}'::text AS work_class,
               false AS wrapped
        FROM #{runs} AS candidate
        WHERE #{predicate}
          AND candidate.scope_key = $1
          AND candidate.#{eligible_column} #{due_operator} $2
          AND (candidate.#{eligible_column}, candidate.id) > ($3, $4)
        ORDER BY candidate.#{eligible_column}, candidate.id
        LIMIT #{budget}
      ),
      residual AS MATERIALIZED (
        SELECT GREATEST(#{budget} - count(*)::integer, 0) AS remaining
        FROM after_cursor
      ),
      wrapped AS MATERIALIZED (
        SELECT candidate.id,
               candidate.#{eligible_column} AS eligible_at,
               candidate.claim_attempts,
               '#{class}'::text AS work_class,
               true AS wrapped
        FROM #{runs} AS candidate
        WHERE #{predicate}
          AND candidate.scope_key = $1
          AND candidate.#{eligible_column} #{due_operator} $2
          AND (candidate.#{eligible_column}, candidate.id) <= ($3, $4)
        ORDER BY candidate.#{eligible_column}, candidate.id
        LIMIT (SELECT remaining FROM residual)
      )
      SELECT id, eligible_at, claim_attempts, work_class, wrapped
      FROM after_cursor
      UNION ALL
      SELECT id, eligible_at, claim_attempts, work_class, wrapped
      FROM wrapped
      ORDER BY wrapped, eligible_at, id
      """
      |> String.trim()
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

    DCKT-78 owns the authoritative class/cap/state recheck and final update;
    this shape fixes the maximum row input it may mutate for one grant.
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

    defp candidate_parts(:ready) do
      {"candidate.status = 'running' AND candidate.poisoned_at IS NULL AND " <>
         "candidate.claim_token IS NULL AND candidate.wake_at IS NOT NULL", "wake_at", "<="}
    end

    defp candidate_parts(:expired) do
      {"candidate.status = 'running' AND candidate.poisoned_at IS NULL AND " <>
         "candidate.claim_token IS NOT NULL", "claimed_at", "<"}
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
