if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.RingFunction do
    @moduledoc """
    Defines `docket_tenant_fair_claim`, the database-side TenantFair
    admission engine.

    One call accepts a point in time, expired-claim cutoff, requested outcome
    count, poison-attempt threshold, optional ready/expired preference,
    bootstrap default cap, and trace flag. It returns newly claimed leases,
    poisoned runs, or a normalized pre-admission error.

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
    a lower caller `lock_timeout`; otherwise it limits policy-cursor acquisition
    to 250 ms.

    It initializes the persisted default cap if necessary, switches the
    singleton policy to TenantFair, and holds that row `FOR UPDATE`. That lock
    serializes committed cursor movement and remains held until the surrounding
    transaction ends. A timeout during this narrow initialization/cursor phase
    rolls the phase back and returns `lock_contention` without inspecting a
    partition or changing policy, schedules, epochs, or runs. Later errors are
    raised so the entire statement aborts.

    For every entered visit, the function attempts the exact partition with
    `FOR NO KEY UPDATE SKIP LOCKED`. Under that authority it freshly counts
    running, unpoisoned rows with claim tokens. The effective cap is the
    partition override or persisted default; available ready capacity is the
    nonnegative difference between that cap and the live count.

    ## Ready and expired candidates

    Ready work is an unclaimed running row whose `wake_at` is due. Admitting an
    ordinary ready row creates a claim token and consumes one available cap
    slot. Cap debt blocks ordinary ready work until the live count falls below
    the effective cap.

    Expired work is a claimed running row whose `claimed_at` precedes the
    cutoff. Replacing its claim token is a count-neutral steal, so ordinary
    expired work remains admissible at or above the cap.

    A ready or expired row at the attempt threshold becomes a poison outcome.
    Poisoning clears scheduling and claim state, returns no claim token, is
    allowed at the cap, and counts as serving that row's ready or expired class.

    The function reads at most `K = 16` structural ready candidates and 16
    structural expired candidates for the partition. From their bounded union
    it ranks and freezes at most 16 exact run IDs. Demand-one calls put the
    requested class head first and retain the other class as fallback. Calls
    with demand of at least two reserve service for both ready and expired work
    until each class produces a committed outcome. If only one final demand
    slot remains while a class is still unserved, the other class cannot consume
    it; the call may therefore intentionally return one fewer outcome.

    The frozen attempt set is ordered by class reservation, demand-one
    preference, poison before ordinary work, eligible time, and run ID. The
    function locks only those exact IDs with `FOR UPDATE SKIP LOCKED`. A skipped
    or stale ID is not replaced by structural row 17 or any ID outside the
    frozen set during that visit.

    After locking, every row is rechecked against the frozen scope, ready or
    expired class, poison class, running/poisoned state, claim state, wake or
    cutoff time, and remaining ready capacity. Only rows that still satisfy all
    authoritative predicates are mutated.

    ## Ready continuation

    A capped partition can contain more than 16 ordinary ready rows before a
    later ready poison row. Its schedule row therefore stores a ready-candidate
    continuation tuple.

    Below the cap, ready discovery starts at the oldest row and clears the
    continuation. At the cap, discovery starts after the effective continuation
    and wraps once to fill the bounded page. If a capped ready page produces no
    ready outcome, the function stages its final structural tuple even when an
    expired outcome was granted. An empty ready page or a committed ready lease
    or poison outcome stages a clear.

    Continuation state is loaded on a scope's first actual visit and then held
    in function-local arrays. Repeated visits in the same call use the latest
    staged value. Only the final decision for each scope is written back.

    ## Lock and write order

    Authority is acquired in this order:

        policy cursor -> partition -> frozen run IDs

    The function never locks a schedule row and then seeks another run.
    Continuation writes are staged until all visits finish, then flushed before
    the final policy cursor is persisted. Policy, continuation, run, epoch, and
    cursor changes are part of the caller's transaction and roll back together.

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
        v_live_count bigint;
        v_ready_slots bigint;
        v_attempt_ids bigint[] := ARRAY[]::bigint[];
        v_attempt_classes text[] := ARRAY[]::text[];
        v_attempt_poisons boolean[] := ARRAY[]::boolean[];
        v_locked_ids bigint[] := ARRAY[]::bigint[];
        v_ready_page_count integer;
        v_ready_page_last_at timestamp with time zone;
        v_ready_page_last_id bigint;
        v_grant_limit integer;
        v_grant_count integer;
        v_rechecked_count integer;
        v_disposition text;
        v_stage_index integer;
        v_stage_scopes text[] := ARRAY[]::text[];
        v_stage_cursor_ats timestamp with time zone[] := ARRAY[]::timestamp with time zone[];
        v_stage_cursor_ids bigint[] := ARRAY[]::bigint[];
        v_effective_cursor_at timestamp with time zone;
        v_effective_cursor_id bigint;
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
            INTO v_live_count
            FROM #{runs}
            WHERE scope_key = v_partition.scope_key
              AND status = 'running'
              AND poisoned_at IS NULL
              AND claim_token IS NOT NULL;

            v_ready_slots := greatest(v_partition.max_active::bigint - v_live_count, 0);
            v_stage_index := array_position(v_stage_scopes, v_partition.scope_key);

            IF v_stage_index IS NULL THEN
              SELECT ready_candidate_cursor_at, ready_candidate_cursor_id
              INTO v_effective_cursor_at, v_effective_cursor_id
              FROM #{schedule}
              WHERE scope_key = v_partition.scope_key;

              v_stage_scopes := array_append(v_stage_scopes, v_partition.scope_key);
              v_stage_cursor_ats := array_append(v_stage_cursor_ats, v_effective_cursor_at);
              v_stage_cursor_ids := array_append(v_stage_cursor_ids, v_effective_cursor_id);
              v_stage_index := cardinality(v_stage_scopes);
            ELSE
              v_effective_cursor_at := v_stage_cursor_ats[v_stage_index];
              v_effective_cursor_id := v_stage_cursor_ids[v_stage_index];
            END IF;

            WITH ready_after AS MATERIALIZED (
              SELECT candidate.id, candidate.wake_at AS eligible_at,
                     candidate.claim_attempts, 'ready'::text AS work_class,
                     false AS wrapped
              FROM #{runs} AS candidate
              WHERE candidate.scope_key = v_partition.scope_key
                AND candidate.status = 'running'
                AND candidate.poisoned_at IS NULL
                AND candidate.claim_token IS NULL
                AND candidate.wake_at IS NOT NULL AND candidate.wake_at <= p_now
                AND (
                  v_ready_slots > 0 OR v_effective_cursor_at IS NULL OR
                  (candidate.wake_at, candidate.id) >
                    (v_effective_cursor_at, v_effective_cursor_id)
                )
              ORDER BY candidate.wake_at, candidate.id
              LIMIT #{run_budget}
            ),
            ready_residual AS MATERIALIZED (
              SELECT greatest(#{run_budget} - count(*)::integer, 0) AS remaining
              FROM ready_after
            ),
            ready_wrapped AS MATERIALIZED (
              SELECT candidate.id, candidate.wake_at AS eligible_at,
                     candidate.claim_attempts, 'ready'::text AS work_class,
                     true AS wrapped
              FROM #{runs} AS candidate
              WHERE v_ready_slots = 0 AND v_effective_cursor_at IS NOT NULL
                AND candidate.scope_key = v_partition.scope_key
                AND candidate.status = 'running'
                AND candidate.poisoned_at IS NULL
                AND candidate.claim_token IS NULL
                AND candidate.wake_at IS NOT NULL AND candidate.wake_at <= p_now
                AND (candidate.wake_at, candidate.id) <=
                    (v_effective_cursor_at, v_effective_cursor_id)
              ORDER BY candidate.wake_at, candidate.id
              LIMIT (SELECT remaining FROM ready_residual)
            ),
            ready_page AS MATERIALIZED (
              SELECT * FROM ready_after
              UNION ALL
              SELECT * FROM ready_wrapped
            ),
            expired_page AS MATERIALIZED (
              SELECT candidate.id, candidate.claimed_at AS eligible_at,
                     candidate.claim_attempts, 'expired'::text AS work_class
              FROM #{runs} AS candidate
              WHERE candidate.scope_key = v_partition.scope_key
                AND candidate.status = 'running'
                AND candidate.poisoned_at IS NULL
                AND candidate.claim_token IS NOT NULL
                AND candidate.claimed_at < p_cutoff
              ORDER BY candidate.claimed_at, candidate.id
              LIMIT #{run_budget}
            ),
            candidates AS MATERIALIZED (
              SELECT id, eligible_at, claim_attempts, work_class FROM ready_page
              UNION ALL
              SELECT * FROM expired_page
            ),
            ranked AS MATERIALIZED (
              SELECT candidates.*,
                     candidates.claim_attempts >= p_max_attempts AS poison,
                     row_number() OVER (
                       PARTITION BY candidates.work_class
                       ORDER BY candidates.eligible_at, candidates.id
                     ) AS class_rank
              FROM candidates
              WHERE candidates.work_class = 'expired'
                 OR candidates.claim_attempts >= p_max_attempts
                 OR v_ready_slots > 0
            ),
            prioritized AS MATERIALIZED (
              SELECT ranked.*,
                     row_number() OVER (ORDER BY
                       CASE WHEN p_demand >= 2 AND class_rank = 1 AND
                           (work_class = 'ready' AND NOT v_served_ready OR
                            work_class = 'expired' AND NOT v_served_expired)
                         THEN 0 ELSE 1 END,
                       CASE WHEN p_demand = 1 AND class_rank = 1 THEN 0 ELSE 1 END,
                       CASE WHEN p_demand = 1 AND work_class = p_preference THEN 0 ELSE 1 END,
                       CASE WHEN poison THEN 0 ELSE 1 END,
                       eligible_at, id
                     ) AS attempt_ordinal
              FROM ranked
            ),
            chosen AS MATERIALIZED (
              SELECT id, work_class, eligible_at, poison, attempt_ordinal
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
                   (SELECT count(*)::integer FROM ready_page),
                   (SELECT eligible_at FROM ready_page
                    ORDER BY wrapped DESC, eligible_at DESC, id DESC LIMIT 1),
                   (SELECT id FROM ready_page
                    ORDER BY wrapped DESC, eligible_at DESC, id DESC LIMIT 1)
            INTO v_attempt_ids, v_attempt_classes, v_attempt_poisons,
                 v_ready_page_count,
                 v_ready_page_last_at, v_ready_page_last_id
            FROM chosen;

            IF v_ready_slots > 0 OR v_ready_page_count = 0 THEN
              v_stage_cursor_ats[v_stage_index] := NULL;
              v_stage_cursor_ids[v_stage_index] := NULL;
            ELSE
              v_stage_cursor_ats[v_stage_index] := v_ready_page_last_at;
              v_stage_cursor_ids[v_stage_index] := v_ready_page_last_id;
            END IF;

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
            v_disposition := CASE
              WHEN cardinality(v_attempt_ids) = 0 THEN 'empty_page'
              WHEN cardinality(v_locked_ids) = 0 THEN 'lock_miss'
              ELSE 'denied'
            END;

            FOR v_candidate IN
              WITH locked AS MATERIALIZED (
                SELECT candidate.*,
                       attempted.work_class,
                       CASE WHEN candidate.claim_token IS NULL
                         THEN candidate.wake_at ELSE candidate.claimed_at END AS eligible_at,
                       attempted.poison
                FROM unnest(v_attempt_ids, v_attempt_classes, v_attempt_poisons)
                  WITH ORDINALITY AS attempted(id, work_class, poison, ordinality)
                JOIN #{runs} AS candidate ON candidate.id = attempted.id
                WHERE candidate.scope_key = v_partition.scope_key
                  AND candidate.id = ANY(v_locked_ids)
                  AND candidate.status = 'running'
                  AND candidate.poisoned_at IS NULL
                  AND attempted.poison = (candidate.claim_attempts >= p_max_attempts)
                  AND (
                    attempted.work_class = 'ready' AND candidate.claim_token IS NULL AND
                    candidate.wake_at IS NOT NULL AND
                    candidate.wake_at <= p_now OR
                    attempted.work_class = 'expired' AND candidate.claim_token IS NOT NULL AND
                    candidate.claimed_at < p_cutoff
                  )
              ),
              ranked AS (
                SELECT locked.*,
                       row_number() OVER (
                         PARTITION BY work_class ORDER BY eligible_at, id
                       ) AS class_rank
                FROM locked
              )
              SELECT * FROM ranked
              ORDER BY
                CASE WHEN p_demand >= 2 AND class_rank = 1 AND
                    (work_class = 'ready' AND NOT v_served_ready OR
                     work_class = 'expired' AND NOT v_served_expired)
                  THEN 0 ELSE 1 END,
                CASE WHEN p_demand = 1 AND class_rank = 1 THEN 0 ELSE 1 END,
                CASE WHEN p_demand = 1 AND work_class = p_preference THEN 0 ELSE 1 END,
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

              IF v_candidate.work_class = 'ready' AND NOT v_candidate.poison AND
                 v_ready_slots = 0 THEN
                CONTINUE;
              END IF;

              UPDATE #{runs} AS admitted
              SET claim_token = CASE WHEN v_candidate.poison
                    THEN NULL ELSE pg_catalog.gen_random_uuid() END,
                  claimed_at = CASE WHEN v_candidate.poison THEN NULL ELSE p_now END,
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
                  v_candidate.work_class = 'ready' AND admitted.claim_token IS NULL AND
                  admitted.wake_at IS NOT NULL AND admitted.wake_at <= p_now OR
                  v_candidate.work_class = 'expired' AND admitted.claim_token IS NOT NULL AND
                  admitted.claimed_at < p_cutoff
                )
                AND (
                  v_candidate.poison AND admitted.claim_attempts >= p_max_attempts OR
                  NOT v_candidate.poison AND admitted.claim_attempts < p_max_attempts
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

                IF v_candidate.work_class = 'ready' THEN
                  v_served_ready := true;
                  v_stage_cursor_ats[v_stage_index] := NULL;
                  v_stage_cursor_ids[v_stage_index] := NULL;
                  IF NOT v_candidate.poison THEN
                    v_ready_slots := v_ready_slots - 1;
                  END IF;
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
                  v_updated.poison_reason::text, v_candidate.work_class::text,
                  v_candidate.eligible_at::timestamp with time zone,
                  v_call_token, v_transaction_id, v_visit.visit_ordinal::integer,
                  v_visit_outcome_ordinal, p_demand, v_visit_cursor_before, v_cursor,
                  v_visit.ring_position::bigint, v_visit.scope_key::text,
                  'grant'::text, NULL::integer, NULL::bigint;
              END IF;
            END LOOP;

            IF v_grant_count = 0 THEN
              v_disposition := CASE
                WHEN cardinality(v_attempt_ids) = 0 THEN 'empty_page'
                WHEN cardinality(v_locked_ids) = 0 THEN 'lock_miss'
                WHEN v_rechecked_count = 0 THEN 'stale'
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

        IF cardinality(v_stage_scopes) > 0 THEN
          UPDATE #{schedule} AS persisted
          SET ready_candidate_cursor_at = staged.cursor_at,
              ready_candidate_cursor_id = staged.cursor_id,
              updated_at = p_now
          FROM unnest(v_stage_scopes, v_stage_cursor_ats, v_stage_cursor_ids)
            AS staged(scope_key, cursor_at, cursor_id)
          WHERE persisted.scope_key = staged.scope_key;
        END IF;

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
