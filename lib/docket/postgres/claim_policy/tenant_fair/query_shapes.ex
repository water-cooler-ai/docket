if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.QueryShapes do
    @moduledoc false

    alias Docket.Postgres.ClaimPolicy.TenantFair.Budgets

    @cohort_predicate "in_cohort"

    @doc """
    Returns the fixed-budget circular scheduling-cohort traversal.

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
               next.may_have_ready_at,
               next.may_have_claimed_at,
               next.ready_dirty,
               next.claimed_dirty,
               1::integer AS visit_ordinal,
               next.wrap_delta::bigint AS wrap_count
        FROM LATERAL (
          SELECT option.ring_position,
                 option.scope_key,
                 option.may_have_ready_at,
                 option.may_have_claimed_at,
                 option.ready_dirty,
                 option.claimed_dirty,
                 option.wrap_delta
          FROM (
            (SELECT ring_position, scope_key, may_have_ready_at,
                    may_have_claimed_at, ready_dirty, claimed_dirty,
                    0::integer AS wrap_delta
             FROM #{schedule}
             WHERE #{@cohort_predicate} AND ring_position > $1
             ORDER BY ring_position
             LIMIT 1)
            UNION ALL
            (SELECT ring_position, scope_key, may_have_ready_at,
                    may_have_claimed_at, ready_dirty, claimed_dirty,
                    1::integer AS wrap_delta
             FROM #{schedule}
             WHERE #{@cohort_predicate} AND ring_position <= $1
             ORDER BY ring_position
             LIMIT 1)
          ) AS option
          ORDER BY option.wrap_delta, option.ring_position
          LIMIT 1
        ) AS next

        UNION ALL

        SELECT next.ring_position,
               next.scope_key,
               next.may_have_ready_at,
               next.may_have_claimed_at,
               next.ready_dirty,
               next.claimed_dirty,
               inspected.visit_ordinal + 1,
               inspected.wrap_count + next.wrap_delta
        FROM inspected
        CROSS JOIN LATERAL (
          SELECT option.ring_position,
                 option.scope_key,
                 option.may_have_ready_at,
                 option.may_have_claimed_at,
                 option.ready_dirty,
                 option.claimed_dirty,
                 option.wrap_delta
          FROM (
            (SELECT ring_position, scope_key, may_have_ready_at,
                    may_have_claimed_at, ready_dirty, claimed_dirty,
                    0::integer AS wrap_delta
             FROM #{schedule}
             WHERE #{@cohort_predicate} AND ring_position > inspected.ring_position
             ORDER BY ring_position
             LIMIT 1)
            UNION ALL
            (SELECT ring_position, scope_key, may_have_ready_at,
                    may_have_claimed_at, ready_dirty, claimed_dirty,
                    1::integer AS wrap_delta
             FROM #{schedule}
             WHERE #{@cohort_predicate} AND ring_position <= inspected.ring_position
             ORDER BY ring_position
             LIMIT 1)
          ) AS option
          ORDER BY option.wrap_delta, option.ring_position
          LIMIT 1
        ) AS next
        WHERE inspected.visit_ordinal < #{budget}
      )
      SELECT ring_position, scope_key, may_have_ready_at, may_have_claimed_at,
             ready_dirty, claimed_dirty, visit_ordinal, wrap_count
      FROM inspected
      ORDER BY visit_ordinal
      """
      |> String.trim()
    end

    @doc """
    Returns one bounded recursive loose-index reconciliation page.

    `$1` is the class-local last scope key and `$2` is the ready time or
    expiration cutoff. Each recursive step seeks the next distinct scope head
    through the existing scope-first class index, then performs one exact-scope
    due probe. The two halves are disjoint and wrap at most once.
    """
    def reconciliation_heads(runs, class)
        when is_binary(runs) and class in [:ready, :expired] do
      budget = reconciliation_budget(class)
      {predicate, eligible_column, due_operator} = reconciliation_parts(class)

      """
      WITH RECURSIVE after_cursor AS (
        SELECT head.scope_key, 1::integer AS visit_ordinal
        FROM LATERAL (
          SELECT candidate.scope_key
          FROM #{runs} AS candidate
          WHERE #{predicate}
            AND candidate.scope_key > $1
          ORDER BY candidate.scope_key, candidate.#{eligible_column}, candidate.id
          LIMIT 1
        ) AS head

        UNION ALL

        SELECT head.scope_key, page.visit_ordinal + 1
        FROM after_cursor AS page
        CROSS JOIN LATERAL (
          SELECT candidate.scope_key
          FROM #{runs} AS candidate
          WHERE #{predicate}
            AND candidate.scope_key > page.scope_key
          ORDER BY candidate.scope_key, candidate.#{eligible_column}, candidate.id
          LIMIT 1
        ) AS head
        WHERE page.visit_ordinal < #{budget}
      ),
      after_count AS MATERIALIZED (
        SELECT count(*)::integer AS inspected
        FROM after_cursor
      ),
      wrapped AS (
        SELECT head.scope_key, after_count.inspected + 1 AS visit_ordinal
        FROM after_count
        CROSS JOIN LATERAL (
          SELECT candidate.scope_key
          FROM #{runs} AS candidate
          WHERE #{predicate}
            AND candidate.scope_key <= $1
          ORDER BY candidate.scope_key, candidate.#{eligible_column}, candidate.id
          LIMIT 1
        ) AS head
        WHERE after_count.inspected < #{budget}

        UNION ALL

        SELECT head.scope_key, page.visit_ordinal + 1
        FROM wrapped AS page
        CROSS JOIN LATERAL (
          SELECT candidate.scope_key
          FROM #{runs} AS candidate
          WHERE #{predicate}
            AND candidate.scope_key > page.scope_key
            AND candidate.scope_key <= $1
          ORDER BY candidate.scope_key, candidate.#{eligible_column}, candidate.id
          LIMIT 1
        ) AS head
        WHERE page.visit_ordinal < #{budget}
      ),
      heads AS MATERIALIZED (
        SELECT scope_key, visit_ordinal, false AS wrapped
        FROM after_cursor
        UNION ALL
        SELECT scope_key, visit_ordinal, true AS wrapped
        FROM wrapped
      )
      SELECT heads.scope_key,
             heads.visit_ordinal,
             heads.wrapped,
             due.id AS evidence_run_id,
             due.eligible_at
      FROM heads
      LEFT JOIN LATERAL (
        SELECT candidate.id, candidate.#{eligible_column} AS eligible_at
        FROM #{runs} AS candidate
        WHERE #{predicate}
          AND candidate.scope_key = heads.scope_key
          AND candidate.#{eligible_column} #{due_operator} $2
        ORDER BY candidate.#{eligible_column}, candidate.id
        LIMIT 1
      ) AS due ON true
      ORDER BY heads.visit_ordinal
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
    def rotating_run_candidates(runs, class)
        when is_binary(runs) and class in [:ready, :expired] do
      budget = Budgets.run_lock_attempts()
      {predicate, eligible_column, due_operator} = reconciliation_parts(class)

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
    def scan_cursor_lock(cursor) when is_binary(cursor) do
      """
      SELECT ring_position, scan_call_sequence
      FROM #{cursor}
      WHERE id = 1
      FOR UPDATE
      """
      |> String.trim()
    end

    @doc "Locks one class-local reconciliation cursor without cross-class serialization."
    def reconciliation_cursor_lock(cursor) when is_binary(cursor) do
      """
      SELECT last_scope_key, wrap_count, next_scan_call
      FROM #{cursor}
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

    defp reconciliation_budget(:ready), do: Budgets.ready_reconciliation_partitions()
    defp reconciliation_budget(:expired), do: Budgets.expired_reconciliation_partitions()

    defp reconciliation_parts(:ready) do
      {"candidate.status = 'running' AND candidate.poisoned_at IS NULL AND " <>
         "candidate.claim_token IS NULL AND candidate.wake_at IS NOT NULL", "wake_at", "<="}
    end

    defp reconciliation_parts(:expired) do
      {"candidate.status = 'running' AND candidate.poisoned_at IS NULL AND " <>
         "candidate.claim_token IS NOT NULL", "claimed_at", "<"}
    end

    defp candidate_page(runs, class) do
      budget = Budgets.run_lock_attempts()
      {predicate, eligible_column, due_operator} = reconciliation_parts(class)

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
