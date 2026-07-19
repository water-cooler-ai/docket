if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.RingFunction do
    @moduledoc """
    Defines `docket_tenant_fair_claim`, the database-side TenantFair
    admission engine.

    One call accepts a point in time, expired-claim cutoff, requested outcome
    count, poison-attempt threshold, optional ready/expired preference,
    bootstrap default cap, and trace flag. The function returns newly claimed
    leases, poisoned runs, or a normalized pre-admission error.

    ## Scheduling model

    `docket_claim_schedule` is a stable ring of claim partitions. A schedule
    row participates while `unfinished_count > 0`; membership therefore covers
    ready work, claimed work, future timers, and other nonterminal runs rather
    than only work that can produce an outcome immediately.

    The singleton policy row stores `scan_ring_position`, the position inspected
    by the last committed TenantFair call. A call starts at its absolute
    successor, wraps to the smallest positive position when necessary, and
    materializes exactly the next `S = 32` visits when demand remains. If the
    positive ring contains fewer than 32 partitions, traversal wraps repeatedly
    and may visit the same partition more than once. An empty ring produces no
    visits and leaves the cursor unchanged.

    Entering a visit advances the call-local cursor. The visit counts even when
    partition authority is unavailable, the partition is capped, its work is
    dormant, its bounded candidate page is empty, every exact run lock is
    skipped, or every locked row fails authoritative recheck. If demand becomes
    satisfied, prefetched but unentered visits do not advance the cursor.

    Each entered visit targets one partition and produces at most one grant. A
    grant contains between one and `Q = 8` outcomes, limited further by the
    caller's remaining demand. Only a nonempty grant increments that
    partition's `admission_epoch`, and it increments the epoch exactly once
    regardless of the number of outcomes in the grant.

    ## Policy and partition authority

    The function requires a writable Read Committed transaction. It preserves
    a lower caller `lock_timeout`; otherwise it limits lock waits throughout
    the function to 250 ms.

    It initializes the persisted default cap if necessary, switches the
    singleton policy to TenantFair, and holds that row `FOR UPDATE`. That lock
    serializes committed cursor movement and remains held until the surrounding
    transaction ends. A timeout during this narrow initialization/cursor phase
    rolls the phase back and returns `lock_contention` without inspecting a
    partition or changing policy, schedules, epochs, or runs. Later errors are
    raised so the entire statement aborts.

    For every entered visit, the function attempts the exact partition with
    `FOR NO KEY UPDATE SKIP LOCKED`. Under that authority it freshly counts
    healthy running rows carrying the durable `tenant_admitted_at` marker.
    The effective cap is the partition override or persisted default;
    promotion capacity is the nonnegative difference between that cap and the
    admitted count.

    ## Ready and expired candidates

    Ready work is an unclaimed running row whose `wake_at` is due. An admitted
    ready row already has `tenant_admitted_at` and may reacquire a transient
    claim at or above the cap. A queued ready row has no marker; its FIFO-head
    promotion atomically installs both the marker and a claim and requires a
    free logical-run slot. Cap debt blocks promotion, not reacquisition.

    Expired work is a claimed running row whose `claimed_at` precedes the
    cutoff. Replacing its claim token is a count-neutral steal, so ordinary
    expired work remains admissible at or above the cap.

    A ready or expired row at the attempt threshold becomes a poison outcome.
    Poisoning clears scheduling, claim, and admission state and returns no
    claim token. An admitted poison can therefore retire a cohort member at
    the cap. A queued poison must itself be the FIFO head and requires a free
    promotion slot; it never bypasses older ordinary queued work.

    The function reads a shared page of at most `K = 16` structural ready
    candidates: admitted-ready rows first, then the oldest queued-ready rows
    that fit the residual page. It separately reads at most 16 expired
    candidates. From their bounded union
    it ranks and freezes at most 16 exact run IDs. Demand-one calls put the
    requested class head first and retain the other class as fallback. Calls
    with demand of at least two reserve service for both ready and expired work
    until each class produces a committed outcome. If only one final demand
    slot remains while a class is still unserved, the other class cannot consume
    it; the call may therefore intentionally return one fewer outcome.

    The frozen attempt set first retains each unserved class head when demand
    is at least two, so either class cannot disappear at the `K` truncation,
    then prefers admitted work and applies demand-one preference. Mutation
    still orders admitted work before promotion. Queued rows retain exact
    `(wake_at, id)` FIFO order even when a later queued row is poison. The
    function locks only those exact IDs with `FOR UPDATE SKIP LOCKED`. A skipped
    or stale ID is not replaced by structural row 17 or any ID outside the
    frozen set during that visit.

    After locking, every row is rechecked against the frozen scope, ready or
    expired class, poison class, running/poisoned state, claim state, wake or
    cutoff time, and remaining promotion capacity. Only rows that still satisfy all
    authoritative predicates are mutated. Queue mutation also requires the
    next contiguous frozen queue ordinal and an authoritative absence check for
    an older currently visible unadmitted head. A locked or stale head therefore
    blocks every later promotion in that visit. TenantFair never rotates queued
    discovery.

    ## Lock and write order

    Authority is acquired in this order:

        policy cursor -> partition -> frozen run IDs

    The function never locks a schedule row and then seeks another run. Policy,
    run, epoch, and cursor changes are part of the caller's transaction and roll
    back together.

    ## Results and trace mode

    Outcome and error rows begin with the stable fourteen columns decoded by
    `TenantFair`. The raw record appends a per-call token, transaction ID, visit
    and outcome ordinals, demand, cursor transition, ring identity,
    disposition, outcome count, and epoch delta.

    Production calls the function with trace disabled. `TenantFair.SQL` removes
    the internal columns and explicitly orders public outcomes by visit and
    outcome ordinal. Trace-enabled calls additionally receive one inspection
    row per entered visit, including skips, denials, stale pages, empty pages,
    and grants. Trace rows are returned only to the caller and are never stored
    in a trace table or emitted as identity-bearing metric labels.

    This Elixir module generates the prefix-qualified function definition,
    exposes its SQL identity and record shape, and supplies its migration create
    and drop statements.
    """

    alias Docket.Postgres.ClaimPolicy.TenantFair.Budgets
    alias Docket.Postgres.Storage

    @name "docket_tenant_fair_claim"
    @identity_arguments "timestamp with time zone, timestamp with time zone, integer, integer, text, integer, boolean"
    @lock_timeout_ms 250

    @public_result_columns [
      row_kind: "text",
      error_reason: "text",
      run_id: "text",
      tenant_id: "text",
      graph_id: "text",
      graph_hash: "text",
      checkpoint_seq: "bigint",
      claim_token: "uuid",
      claimed_at: "timestamp with time zone",
      claim_attempt: "integer",
      poisoned_at: "timestamp with time zone",
      poison_reason: "text",
      work_class: "text",
      eligible_at: "timestamp with time zone"
    ]

    @internal_result_columns [
      call_token: "uuid",
      transaction_id: "bigint",
      visit_ordinal: "integer",
      outcome_ordinal: "integer",
      demand: "integer",
      cursor_before: "bigint",
      cursor_after: "bigint",
      ring_position: "bigint",
      scope_key: "text",
      disposition: "text",
      outcome_count: "integer",
      epoch_delta: "bigint"
    ]

    def name, do: @name
    def identity_arguments, do: @identity_arguments
    def lock_timeout_ms, do: @lock_timeout_ms
    def public_result_columns, do: @public_result_columns

    def result_definition do
      Enum.map_join(@public_result_columns ++ @internal_result_columns, ",\n        ", fn {name,
                                                                                           type} ->
        "#{name} #{type}"
      end)
    end

    def public_projection(alias_name \\ "claimed") when is_binary(alias_name) do
      Enum.map_join(@public_result_columns, ",\n        ", fn {name, _type} ->
        "#{alias_name}.#{name}"
      end)
    end

    def create_sql(prefix) when is_binary(prefix) do
      function = Storage.qualified_table(prefix, @name)

      """
      CREATE FUNCTION #{function}(
        timestamp with time zone,
        timestamp with time zone,
        integer,
        integer,
        text,
        integer,
        boolean
      )
      RETURNS SETOF record
      LANGUAGE plpgsql
      VOLATILE
      PARALLEL UNSAFE
      SECURITY INVOKER
      SET search_path TO pg_catalog, pg_temp
      AS $docket_tenant_fair$#{prosrc(prefix)}$docket_tenant_fair$
      """
    end

    def drop_sql(prefix) when is_binary(prefix) do
      function = Storage.qualified_table(prefix, @name)
      "DROP FUNCTION IF EXISTS #{function}(#{@identity_arguments})"
    end

    def prosrc(prefix) when is_binary(prefix) do
      policy = Storage.qualified_table(prefix, "docket_claim_policy")
      partitions = Storage.qualified_table(prefix, "docket_claim_partitions")
      schedule = Storage.qualified_table(prefix, "docket_claim_schedule")
      runs = Storage.qualified_table(prefix, "docket_runs")
      scan_budget = Budgets.scan_inspections()
      run_budget = Budgets.run_lock_attempts()
      grant_budget = Budgets.grant_outcomes()

      """
      DECLARE
        p_now ALIAS FOR $1;
        p_cutoff ALIAS FOR $2;
        p_demand ALIAS FOR $3;
        p_max_attempts ALIAS FOR $4;
        p_preference ALIAS FOR $5;
        p_default_max ALIAS FOR $6;
        p_trace ALIAS FOR $7;
        v_prior_lock_timeout text := current_setting('lock_timeout');
        v_call_token uuid := pg_catalog.gen_random_uuid();
        v_transaction_id bigint := pg_catalog.txid_current();
        v_default_max integer;
        v_cursor_before bigint;
        v_cursor bigint;
        v_visit_cursor_before bigint;
        v_remaining integer := p_demand;
        v_visit_outcome_ordinal integer := 0;
        v_inspections integer := 0;
        v_served_ready boolean := false;
        v_served_expired boolean := false;
        v_visit record;
        v_partition record;
        v_candidate record;
        v_updated record;
        v_admitted_count bigint;
        v_promotion_slots bigint;
        v_attempt_ids bigint[] := ARRAY[]::bigint[];
        v_attempt_classes text[] := ARRAY[]::text[];
        v_attempt_poisons boolean[] := ARRAY[]::boolean[];
        v_attempt_admitted boolean[] := ARRAY[]::boolean[];
        v_attempt_queue_ordinals integer[] := ARRAY[]::integer[];
        v_locked_ids bigint[] := ARRAY[]::bigint[];
        v_grant_limit integer;
        v_grant_count integer;
        v_rechecked_count integer;
        v_disposition text;
        v_next_queue_ordinal integer;
        v_candidate_counted boolean;
      BEGIN
        IF p_now IS NULL OR p_cutoff IS NULL OR p_cutoff > p_now OR
           p_demand IS NULL OR p_demand <= 0 OR
           p_max_attempts IS NULL OR p_max_attempts <= 0 OR
           p_default_max IS NULL OR p_default_max <= 0 OR
           p_trace IS NULL OR
           p_preference IS NOT NULL AND p_preference NOT IN ('ready', 'expired') THEN
          RAISE EXCEPTION 'invalid docket tenant-fair ring function arguments'
            USING ERRCODE = '22023';
        END IF;

        IF current_setting('transaction_read_only') = 'on' THEN
          RETURN QUERY SELECT 'error'::text, 'read_only_transaction'::text,
            NULL::text, NULL::text, NULL::text, NULL::text, NULL::bigint,
            NULL::uuid, NULL::timestamp with time zone, NULL::integer,
            NULL::timestamp with time zone, NULL::text, NULL::text,
            NULL::timestamp with time zone, v_call_token, v_transaction_id,
            NULL::integer, NULL::integer, p_demand, NULL::bigint, NULL::bigint,
            NULL::bigint, NULL::text, 'transaction_mode'::text, 0::integer, 0::bigint;
          RETURN;
        END IF;

        IF current_setting('transaction_isolation') <> 'read committed' THEN
          RETURN QUERY SELECT 'error'::text, 'unsupported_isolation'::text,
            NULL::text, NULL::text, NULL::text, NULL::text, NULL::bigint,
            NULL::uuid, NULL::timestamp with time zone, NULL::integer,
            NULL::timestamp with time zone, NULL::text, NULL::text,
            NULL::timestamp with time zone, v_call_token, v_transaction_id,
            NULL::integer, NULL::integer, p_demand, NULL::bigint, NULL::bigint,
            NULL::bigint, NULL::text, 'transaction_mode'::text, 0::integer, 0::bigint;
          RETURN;
        END IF;

        IF v_prior_lock_timeout = '0' OR
           v_prior_lock_timeout::interval > interval '#{@lock_timeout_ms} milliseconds' THEN
          PERFORM set_config('lock_timeout', '#{@lock_timeout_ms}ms', true);
        END IF;

        BEGIN
          UPDATE #{policy}
          SET max_active = p_default_max,
              admission_mode = 'tenant_fair',
              policy_version = policy_version + 1,
              initialized_at = COALESCE(initialized_at, p_now),
              updated_at = p_now
          WHERE id = 1 AND max_active IS NULL;

          UPDATE #{policy}
          SET admission_mode = 'tenant_fair',
              updated_at = p_now
          WHERE id = 1 AND admission_mode <> 'tenant_fair';

          SELECT max_active, scan_ring_position
          INTO v_default_max, v_cursor_before
          FROM #{policy}
          WHERE id = 1
          FOR UPDATE;
        EXCEPTION
          WHEN lock_not_available THEN
            PERFORM set_config('lock_timeout', v_prior_lock_timeout, true);
            RETURN QUERY SELECT 'error'::text, 'lock_contention'::text,
              NULL::text, NULL::text, NULL::text, NULL::text, NULL::bigint,
              NULL::uuid, NULL::timestamp with time zone, NULL::integer,
              NULL::timestamp with time zone, NULL::text, NULL::text,
              NULL::timestamp with time zone, v_call_token, v_transaction_id,
              NULL::integer, NULL::integer, p_demand, NULL::bigint, NULL::bigint,
              NULL::bigint, NULL::text, 'policy_cursor'::text, 0::integer, 0::bigint;
            RETURN;
        END;

        IF v_default_max IS NULL OR v_cursor_before IS NULL THEN
          RAISE EXCEPTION 'docket claim policy is not initialized'
            USING ERRCODE = '55000';
        END IF;

        v_cursor := v_cursor_before;

        FOR v_visit IN
          WITH RECURSIVE inspected AS MATERIALIZED (
            SELECT next.ring_position, next.scope_key, 1::integer AS visit_ordinal
            FROM LATERAL (
              SELECT option.ring_position, option.scope_key
              FROM (
                (SELECT ring_position, scope_key, 0::integer AS wrapped
                 FROM #{schedule}
                 WHERE unfinished_count > 0 AND ring_position > v_cursor_before
                 ORDER BY ring_position LIMIT 1)
                UNION ALL
                (SELECT ring_position, scope_key, 1::integer AS wrapped
                 FROM #{schedule}
                 WHERE unfinished_count > 0 AND ring_position <= v_cursor_before
                 ORDER BY ring_position LIMIT 1)
              ) AS option
              ORDER BY option.wrapped, option.ring_position
              LIMIT 1
            ) AS next

            UNION ALL

            SELECT next.ring_position, next.scope_key, inspected.visit_ordinal + 1
            FROM inspected
            CROSS JOIN LATERAL (
              SELECT option.ring_position, option.scope_key
              FROM (
                (SELECT ring_position, scope_key, 0::integer AS wrapped
                 FROM #{schedule}
                 WHERE unfinished_count > 0 AND ring_position > inspected.ring_position
                 ORDER BY ring_position LIMIT 1)
                UNION ALL
                (SELECT ring_position, scope_key, 1::integer AS wrapped
                 FROM #{schedule}
                 WHERE unfinished_count > 0 AND ring_position <= inspected.ring_position
                 ORDER BY ring_position LIMIT 1)
              ) AS option
              ORDER BY option.wrapped, option.ring_position
              LIMIT 1
            ) AS next
            WHERE inspected.visit_ordinal < #{scan_budget}
          )
          SELECT ring_position, scope_key, visit_ordinal
          FROM inspected
          ORDER BY visit_ordinal
        LOOP
          EXIT WHEN v_remaining = 0;

          v_inspections := v_inspections + 1;
          v_visit_cursor_before := v_cursor;
          v_cursor := v_visit.ring_position;
          v_visit_outcome_ordinal := 0;
          v_grant_count := 0;
          v_rechecked_count := 0;
          v_disposition := 'partition_lock_skip';

          SELECT scope_key, COALESCE(max_active, v_default_max) AS max_active
          INTO v_partition
          FROM #{partitions}
          WHERE scope_key = v_visit.scope_key
          FOR NO KEY UPDATE SKIP LOCKED;

          IF FOUND THEN
            SELECT count(*)::bigint
            INTO v_admitted_count
            FROM #{runs}
            WHERE scope_key = v_partition.scope_key
              AND status = 'running'
              AND poisoned_at IS NULL
              AND tenant_admitted_at IS NOT NULL;

            v_promotion_slots := greatest(v_partition.max_active::bigint - v_admitted_count, 0);

            WITH admitted_ready_page AS MATERIALIZED (
              SELECT candidate.id, candidate.wake_at AS eligible_at,
                     candidate.claim_attempts, 'ready'::text AS work_class,
                     true AS admitted, NULL::integer AS queue_ordinal
              FROM #{runs} AS candidate
              WHERE candidate.scope_key = v_partition.scope_key
                AND candidate.status = 'running'
                AND candidate.poisoned_at IS NULL
                AND candidate.tenant_admitted_at IS NOT NULL
                AND candidate.claim_token IS NULL
                AND candidate.wake_at IS NOT NULL AND candidate.wake_at <= p_now
              ORDER BY candidate.wake_at, candidate.id
              LIMIT #{run_budget}
            ),
            ready_residual AS MATERIALIZED (
              SELECT greatest(#{run_budget} - count(*)::integer, 0) AS remaining
              FROM admitted_ready_page
            ),
            queued_ready_page AS MATERIALIZED (
              SELECT candidate.id, candidate.wake_at AS eligible_at,
                     candidate.claim_attempts, 'ready'::text AS work_class,
                     false AS admitted,
                     row_number() OVER (ORDER BY candidate.wake_at, candidate.id)::integer
                       AS queue_ordinal
              FROM #{runs} AS candidate
              WHERE candidate.scope_key = v_partition.scope_key
                AND candidate.status = 'running'
                AND candidate.poisoned_at IS NULL
                AND candidate.tenant_admitted_at IS NULL
                AND candidate.claim_token IS NULL
                AND candidate.wake_at IS NOT NULL AND candidate.wake_at <= p_now
              ORDER BY candidate.wake_at, candidate.id
              LIMIT (SELECT remaining FROM ready_residual)
            ),
            ready_page AS MATERIALIZED (
              SELECT * FROM admitted_ready_page
              UNION ALL
              SELECT * FROM queued_ready_page
            ),
            expired_page AS MATERIALIZED (
              SELECT candidate.id, candidate.claimed_at AS eligible_at,
                     candidate.claim_attempts, 'expired'::text AS work_class,
                     true AS admitted, NULL::integer AS queue_ordinal
              FROM #{runs} AS candidate
              WHERE candidate.scope_key = v_partition.scope_key
                AND candidate.status = 'running'
                AND candidate.poisoned_at IS NULL
                AND candidate.tenant_admitted_at IS NOT NULL
                AND candidate.claim_token IS NOT NULL
                AND candidate.claimed_at < p_cutoff
              ORDER BY candidate.claimed_at, candidate.id
              LIMIT #{run_budget}
            ),
            candidates AS MATERIALIZED (
              SELECT * FROM ready_page
              UNION ALL
              SELECT * FROM expired_page
            ),
            ranked AS MATERIALIZED (
              SELECT candidates.*,
                     candidates.claim_attempts >= p_max_attempts AS poison,
                     row_number() OVER (
                       PARTITION BY candidates.work_class
                       ORDER BY
                         CASE WHEN candidates.work_class = 'ready' AND candidates.admitted
                           THEN 0
                           WHEN candidates.work_class = 'ready' THEN 1
                           ELSE 0 END,
                         candidates.queue_ordinal NULLS FIRST,
                         candidates.eligible_at, candidates.id
                     ) AS class_rank
              FROM candidates
            ),
            prioritized AS MATERIALIZED (
              SELECT ranked.*,
                     row_number() OVER (ORDER BY
                       CASE WHEN p_demand >= 2 AND class_rank = 1 AND
                           (work_class = 'ready' AND NOT v_served_ready OR
                            work_class = 'expired' AND NOT v_served_expired)
                         THEN 0 ELSE 1 END,
                       CASE WHEN admitted THEN 0 ELSE 1 END,
                       CASE WHEN p_demand = 1 AND class_rank = 1 THEN 0 ELSE 1 END,
                       CASE WHEN p_demand = 1 AND work_class = p_preference THEN 0 ELSE 1 END,
                       queue_ordinal NULLS FIRST,
                       CASE WHEN poison THEN 0 ELSE 1 END,
                       eligible_at, id
                     ) AS attempt_ordinal
              FROM ranked
            ),
            chosen AS MATERIALIZED (
              SELECT id, work_class, eligible_at, poison, admitted,
                     queue_ordinal, attempt_ordinal
              FROM prioritized
              ORDER BY attempt_ordinal
              LIMIT #{run_budget}
            )
            SELECT COALESCE(array_agg(chosen.id ORDER BY chosen.attempt_ordinal),
                            ARRAY[]::bigint[]),
                   COALESCE(array_agg(chosen.work_class ORDER BY chosen.attempt_ordinal),
                            ARRAY[]::text[]),
                   COALESCE(array_agg(chosen.poison ORDER BY chosen.attempt_ordinal),
                            ARRAY[]::boolean[]),
                   COALESCE(array_agg(chosen.admitted ORDER BY chosen.attempt_ordinal),
                            ARRAY[]::boolean[]),
                   COALESCE(array_agg(chosen.queue_ordinal ORDER BY chosen.attempt_ordinal),
                            ARRAY[]::integer[])
            INTO v_attempt_ids, v_attempt_classes, v_attempt_poisons,
                 v_attempt_admitted, v_attempt_queue_ordinals
            FROM chosen;

            SELECT COALESCE(array_agg(locked.id ORDER BY locked.ordinality), ARRAY[]::bigint[])
            INTO v_locked_ids
            FROM (
              SELECT candidate.id, requested.ordinality
              FROM unnest(v_attempt_ids[1:#{run_budget}])
                WITH ORDINALITY AS requested(id, ordinality)
              JOIN #{runs} AS candidate ON candidate.id = requested.id
              ORDER BY requested.ordinality
              FOR UPDATE OF candidate SKIP LOCKED
            ) AS locked;

            v_grant_limit := least(#{grant_budget}, v_remaining);
            v_next_queue_ordinal := 1;
            v_disposition := CASE
              WHEN cardinality(v_attempt_ids) = 0 THEN 'empty_page'
              WHEN cardinality(v_locked_ids) = 0 THEN 'lock_miss'
              ELSE 'denied'
            END;

            FOR v_candidate IN
              WITH locked AS MATERIALIZED (
                SELECT candidate.*,
                       attempted.work_class,
                       CASE WHEN attempted.work_class = 'ready'
                         THEN candidate.wake_at ELSE candidate.claimed_at END AS eligible_at,
                       attempted.poison, attempted.admitted,
                       attempted.queue_ordinal
                FROM unnest(v_attempt_ids, v_attempt_classes, v_attempt_poisons,
                            v_attempt_admitted, v_attempt_queue_ordinals)
                  WITH ORDINALITY AS attempted(
                    id, work_class, poison, admitted, queue_ordinal, ordinality
                  )
                JOIN #{runs} AS candidate ON candidate.id = attempted.id
                WHERE candidate.scope_key = v_partition.scope_key
                  AND candidate.id = ANY(v_locked_ids)
                  AND candidate.status = 'running'
                  AND candidate.poisoned_at IS NULL
                  AND attempted.poison = (candidate.claim_attempts >= p_max_attempts)
                  AND (
                    attempted.work_class = 'ready' AND
                    attempted.admitted = (candidate.tenant_admitted_at IS NOT NULL) AND
                    candidate.claim_token IS NULL AND
                    candidate.wake_at IS NOT NULL AND
                    candidate.wake_at <= p_now OR
                    attempted.work_class = 'expired' AND
                    candidate.tenant_admitted_at IS NOT NULL AND
                    candidate.claim_token IS NOT NULL AND
                    candidate.claimed_at < p_cutoff
                  )
              ),
              ranked AS (
                SELECT locked.*,
                       row_number() OVER (
                         PARTITION BY work_class ORDER BY
                           CASE WHEN work_class = 'ready' AND admitted THEN 0
                                WHEN work_class = 'ready' THEN 1 ELSE 0 END,
                           queue_ordinal NULLS FIRST, eligible_at, id
                       ) AS class_rank
                FROM locked
              )
              SELECT * FROM ranked
              ORDER BY
                CASE WHEN admitted THEN 0 ELSE 1 END,
                CASE WHEN p_demand >= 2 AND class_rank = 1 AND
                    (work_class = 'ready' AND NOT v_served_ready OR
                     work_class = 'expired' AND NOT v_served_expired)
                  THEN 0 ELSE 1 END,
                CASE WHEN p_demand = 1 AND class_rank = 1 THEN 0 ELSE 1 END,
                CASE WHEN p_demand = 1 AND work_class = p_preference THEN 0 ELSE 1 END,
                queue_ordinal NULLS FIRST,
                CASE WHEN poison THEN 0 ELSE 1 END,
                eligible_at, id
            LOOP
              v_rechecked_count := v_rechecked_count + 1;
              EXIT WHEN v_grant_count >= v_grant_limit;

              IF p_demand >= 2 AND v_remaining = 1 AND
                 ((NOT v_served_ready AND v_candidate.work_class <> 'ready') OR
                  (NOT v_served_expired AND v_candidate.work_class <> 'expired')) THEN
                CONTINUE;
              END IF;

              IF v_candidate.work_class = 'ready' AND NOT v_candidate.admitted AND
                 v_candidate.queue_ordinal <> v_next_queue_ordinal THEN
                CONTINUE;
              END IF;

              IF v_candidate.work_class = 'ready' AND NOT v_candidate.admitted AND
                 v_promotion_slots = 0 THEN
                CONTINUE;
              END IF;

              v_candidate_counted := v_candidate.tenant_admitted_at IS NOT NULL;

              UPDATE #{runs} AS admitted
              SET claim_token = CASE WHEN v_candidate.poison
                    THEN NULL ELSE pg_catalog.gen_random_uuid() END,
                  claimed_at = CASE WHEN v_candidate.poison THEN NULL ELSE p_now END,
                  tenant_admitted_at = CASE
                    WHEN v_candidate.poison THEN NULL
                    WHEN v_candidate.work_class = 'ready' AND NOT v_candidate.admitted
                      THEN p_now
                    ELSE admitted.tenant_admitted_at
                  END,
                  wake_at = NULL,
                  claim_attempts = CASE WHEN v_candidate.poison
                    THEN admitted.claim_attempts ELSE admitted.claim_attempts + 1 END,
                  poisoned_at = CASE WHEN v_candidate.poison THEN p_now ELSE NULL END,
                  poison_reason = CASE WHEN v_candidate.poison
                    THEN 'max_claim_attempts_exceeded' ELSE NULL END
              WHERE admitted.id = v_candidate.id
                AND admitted.scope_key = v_partition.scope_key
                AND admitted.status = 'running'
                AND admitted.poisoned_at IS NULL
                AND (
                  v_candidate.work_class = 'expired' AND
                    admitted.tenant_admitted_at IS NOT NULL OR
                  v_candidate.work_class = 'ready' AND v_candidate.admitted AND
                    admitted.tenant_admitted_at IS NOT NULL OR
                  v_candidate.work_class = 'ready' AND NOT v_candidate.admitted AND
                    admitted.tenant_admitted_at IS NULL
                )
                AND (
                  v_candidate.work_class = 'ready' AND admitted.claim_token IS NULL AND
                  admitted.wake_at IS NOT NULL AND admitted.wake_at <= p_now OR
                  v_candidate.work_class = 'expired' AND admitted.claim_token IS NOT NULL AND
                  admitted.claimed_at < p_cutoff
                )
                AND (
                  v_candidate.poison AND admitted.claim_attempts >= p_max_attempts OR
                  NOT v_candidate.poison AND admitted.claim_attempts < p_max_attempts
                )
                AND (
                  v_candidate.work_class <> 'ready' OR v_candidate.admitted OR
                  NOT EXISTS (
                    SELECT 1
                    FROM #{runs} AS earlier
                    WHERE earlier.scope_key = v_partition.scope_key
                      AND earlier.status = 'running'
                      AND earlier.poisoned_at IS NULL
                      AND earlier.tenant_admitted_at IS NULL
                      AND earlier.claim_token IS NULL
                      AND earlier.wake_at IS NOT NULL
                      AND earlier.wake_at <= p_now
                      AND (earlier.wake_at, earlier.id) <
                          (admitted.wake_at, admitted.id)
                  )
                )
              RETURNING admitted.run_id, admitted.tenant_id, admitted.graph_id,
                        admitted.graph_hash, admitted.checkpoint_seq, admitted.claim_token,
                        admitted.claimed_at, admitted.claim_attempts, admitted.poisoned_at,
                        admitted.poison_reason
              INTO v_updated;

              IF FOUND THEN
                v_grant_count := v_grant_count + 1;
                v_visit_outcome_ordinal := v_visit_outcome_ordinal + 1;
                v_remaining := v_remaining - 1;
                v_disposition := 'grant';

                IF v_candidate.work_class = 'ready' AND NOT v_candidate.admitted THEN
                  v_next_queue_ordinal := v_next_queue_ordinal + 1;
                END IF;

                IF v_candidate.poison AND v_candidate_counted THEN
                  v_admitted_count := greatest(v_admitted_count - 1, 0);
                ELSIF v_candidate.work_class = 'ready' AND
                      NOT v_candidate.admitted AND NOT v_candidate.poison THEN
                  v_admitted_count := v_admitted_count + 1;
                END IF;
                v_promotion_slots :=
                  greatest(v_partition.max_active::bigint - v_admitted_count, 0);

                IF v_candidate.work_class = 'ready' THEN
                  v_served_ready := true;
                ELSE
                  v_served_expired := true;
                END IF;

                RETURN QUERY SELECT 'outcome'::text, NULL::text,
                  v_updated.run_id::text, v_updated.tenant_id::text,
                  v_updated.graph_id::text, v_updated.graph_hash::text,
                  v_updated.checkpoint_seq::bigint, v_updated.claim_token::uuid,
                  v_updated.claimed_at::timestamp with time zone,
                  v_updated.claim_attempts::integer,
                  v_updated.poisoned_at::timestamp with time zone,
                  v_updated.poison_reason::text,
                  CASE
                    WHEN v_candidate.work_class = 'expired' THEN 'expired'
                    WHEN v_candidate.admitted THEN 'admitted_ready'
                    ELSE 'queued_ready'
                  END::text,
                  v_candidate.eligible_at::timestamp with time zone,
                  v_call_token, v_transaction_id, v_visit.visit_ordinal::integer,
                  v_visit_outcome_ordinal, p_demand, v_visit_cursor_before, v_cursor,
                  v_visit.ring_position::bigint, v_visit.scope_key::text,
                  CASE
                    WHEN v_candidate.work_class = 'expired' THEN 'expired_recovery'
                    WHEN v_candidate.admitted THEN 'admitted_reacquisition'
                    ELSE 'queued_promotion'
                  END::text, NULL::integer, NULL::bigint;
              END IF;
            END LOOP;

            IF v_grant_count = 0 THEN
              v_disposition := CASE
                WHEN cardinality(v_attempt_ids) = 0 THEN 'empty_page'
                WHEN cardinality(v_locked_ids) = 0 THEN 'lock_miss'
                WHEN v_rechecked_count = 0 THEN 'stale'
                WHEN v_promotion_slots = 0 AND false = ANY(v_attempt_admitted)
                  THEN 'cap_debt_denial'
                ELSE 'denied'
              END;
            END IF;

            IF v_grant_count > 0 THEN
              UPDATE #{partitions}
              SET admission_epoch = admission_epoch + 1,
                  updated_at = p_now
              WHERE scope_key = v_partition.scope_key;
            END IF;
          END IF;

          IF p_trace THEN
            RETURN QUERY SELECT 'inspection'::text, NULL::text,
              NULL::text, NULL::text, NULL::text, NULL::text, NULL::bigint,
              NULL::uuid, NULL::timestamp with time zone, NULL::integer,
              NULL::timestamp with time zone, NULL::text, NULL::text,
              NULL::timestamp with time zone, v_call_token, v_transaction_id,
              v_visit.visit_ordinal::integer, NULL::integer, p_demand,
              v_visit_cursor_before, v_cursor, v_visit.ring_position::bigint,
              v_visit.scope_key::text, v_disposition, v_grant_count,
              CASE WHEN v_grant_count > 0 THEN 1::bigint ELSE 0::bigint END;
          END IF;
        END LOOP;

        IF v_inspections > 0 THEN
          UPDATE #{policy}
          SET scan_ring_position = v_cursor,
              updated_at = p_now
          WHERE id = 1;
        END IF;

        PERFORM set_config('lock_timeout', v_prior_lock_timeout, true);
      END;
      """
      |> String.trim_leading("\n")
    end
  end
end
