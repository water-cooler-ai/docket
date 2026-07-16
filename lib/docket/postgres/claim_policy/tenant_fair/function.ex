if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.Function do
    @moduledoc """
    Canonical database-function contract for exact-cap admission.

    The prefix-specific `prosrc/1` bytes are the authority consumed by the
    migration and activation catalog checks. Formatting or body changes are a
    contract change even when PostgreSQL-visible attributes remain identical.
    """

    alias Docket.Postgres.Storage

    @version 1
    @name "docket_tenant_fair_claim_v1"
    @identity_arguments "timestamp with time zone, timestamp with time zone, integer, integer, text, text[]"
    @result "SETOF record"
    @search_path ["search_path=pg_catalog, pg_temp"]
    @lock_timeout_ms 100
    @partition_budget 1_024

    @result_columns [
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
      eligible_at: "timestamp with time zone",
      eligible_partitions: "bigint",
      locked_partitions: "bigint",
      skipped_partitions: "bigint",
      cap_denied_partitions: "bigint",
      below_preferred_partitions: "bigint",
      default_policy_partitions: "bigint",
      override_policy_partitions: "bigint",
      running_partitions: "bigint",
      hold_new_partitions: "bigint",
      drain_partitions: "bigint",
      preferred_admissions: "bigint",
      borrowed_admissions: "bigint",
      ready_leases: "bigint",
      ready_poisoned: "bigint",
      expired_leases: "bigint",
      expired_poisoned: "bigint",
      candidate_rows_examined: "bigint",
      under_claimed: "bigint",
      ready_claim_wait_ms_count: "bigint",
      ready_claim_wait_ms_sum: "bigint",
      ready_claim_wait_ms_max: "bigint",
      expired_recovery_wait_ms_count: "bigint",
      expired_recovery_wait_ms_sum: "bigint",
      expired_recovery_wait_ms_max: "bigint",
      mode_epoch: "bigint",
      function_contract: "integer",
      ready_candidates: "bigint",
      expired_candidates: "bigint"
    ]

    def version, do: @version
    def name, do: @name
    def identity_arguments, do: @identity_arguments
    def result, do: @result
    def search_path, do: @search_path
    def lock_timeout_ms, do: @lock_timeout_ms
    def partition_budget, do: @partition_budget
    def result_columns, do: @result_columns

    def catalog_contract do
      %{
        name: @name,
        identity_arguments: @identity_arguments,
        result: @result,
        volatility: :volatile,
        parallel: :unsafe,
        security: :invoker,
        search_path: @search_path,
        version: @version
      }
    end

    def result_definition do
      Enum.map_join(@result_columns, ",\n        ", fn {name, type} -> "#{name} #{type}" end)
    end

    def body_sha256(prefix), do: :crypto.hash(:sha256, prosrc(prefix))

    def create_sql(prefix) when is_binary(prefix) do
      function = Storage.qualified_table(prefix, @name)

      """
      CREATE FUNCTION #{function}(
        timestamp with time zone,
        timestamp with time zone,
        integer,
        integer,
        text,
        text[]
      )
      RETURNS SETOF record
      LANGUAGE plpgsql
      VOLATILE
      PARALLEL UNSAFE
      SECURITY INVOKER
      SET search_path TO pg_catalog, pg_temp
      AS $docket_tenant_fair_v1$#{prosrc(prefix)}$docket_tenant_fair_v1$
      """
    end

    def drop_sql(prefix) when is_binary(prefix) do
      function = Storage.qualified_table(prefix, @name)
      "DROP FUNCTION IF EXISTS #{function}(#{@identity_arguments})"
    end

    def prosrc(prefix) when is_binary(prefix) do
      gate = Storage.qualified_table(prefix, "docket_claim_admission_gate")
      policy = Storage.qualified_table(prefix, "docket_claim_policy")
      partitions = Storage.qualified_table(prefix, "docket_claim_partitions")
      runs = Storage.qualified_table(prefix, "docket_runs")
      error_tail = null_columns(2)

      """
      DECLARE
        p_now ALIAS FOR $1;
        p_cutoff ALIAS FOR $2;
        p_demand ALIAS FOR $3;
        p_max_attempts ALIAS FOR $4;
        p_preference ALIAS FOR $5;
        p_candidate_keys ALIAS FOR $6;
        v_prior_lock_timeout text := current_setting('lock_timeout');
        v_error_reason text;
        v_gate_mode text;
        v_gate_readiness text;
        v_gate_contract integer;
        v_mode_epoch bigint;
        v_readiness_epoch bigint;
        v_default_preferred integer;
        v_default_max integer;
        v_default_weight integer;
        v_default_borrowing boolean;
        v_default_initialized_at timestamp with time zone;
        v_keys text[];
        v_locked_keys text[] := ARRAY[]::text[];
        v_key_max integer[] := ARRAY[]::integer[];
        v_key_state text[] := ARRAY[]::text[];
        v_key_live bigint[] := ARRAY[]::bigint[];
        v_partition record;
        v_result record;
        v_planned_ids bigint[];
        v_planned_classes text[];
        v_planned_poison boolean[];
        v_omitted_count bigint;
        v_omitted_eligible bigint;
        v_live_count bigint;
        v_ready_slots bigint;
        v_remaining integer := p_demand;
        v_lock_budget bigint := p_demand::bigint * 2;
        v_page_ready boolean := false;
        v_page_expired boolean := false;
        v_last_ready_key text;
        v_last_expired_key text;
        v_need_ready boolean := false;
        v_need_expired boolean := false;
        v_key_acquired bigint;
        v_wait_ms bigint;

        v_eligible_partitions bigint := 0;
        v_locked_partitions bigint := 0;
        v_skipped_partitions bigint := 0;
        v_cap_denied_partitions bigint := 0;
        v_default_policy_partitions bigint := 0;
        v_override_policy_partitions bigint := 0;
        v_running_partitions bigint := 0;
        v_hold_new_partitions bigint := 0;
        v_drain_partitions bigint := 0;
        v_ready_leases bigint := 0;
        v_ready_poisoned bigint := 0;
        v_expired_leases bigint := 0;
        v_expired_poisoned bigint := 0;
        v_candidate_rows_examined bigint := 0;
        v_ready_wait_count bigint := 0;
        v_ready_wait_sum bigint := 0;
        v_ready_wait_max bigint := 0;
        v_expired_wait_count bigint := 0;
        v_expired_wait_sum bigint := 0;
        v_expired_wait_max bigint := 0;
        v_ready_candidates bigint := 0;
        v_expired_candidates bigint := 0;
        v_observed_for_lock bigint := 0;
        v_acquired_for_lock bigint := 0;
        v_recheck_invalidated bigint := 0;
        v_specifically_skipped bigint := 0;

        v_out_run_id text[] := ARRAY[]::text[];
        v_out_tenant_id text[] := ARRAY[]::text[];
        v_out_graph_id text[] := ARRAY[]::text[];
        v_out_graph_hash text[] := ARRAY[]::text[];
        v_out_checkpoint_seq bigint[] := ARRAY[]::bigint[];
        v_out_claim_token uuid[] := ARRAY[]::uuid[];
        v_out_claimed_at timestamp with time zone[] := ARRAY[]::timestamp with time zone[];
        v_out_claim_attempt integer[] := ARRAY[]::integer[];
        v_out_poisoned_at timestamp with time zone[] := ARRAY[]::timestamp with time zone[];
        v_out_poison_reason text[] := ARRAY[]::text[];
        v_out_work_class text[] := ARRAY[]::text[];
        v_out_eligible_at timestamp with time zone[] := ARRAY[]::timestamp with time zone[];
        v_index integer;
      BEGIN
        IF v_prior_lock_timeout = '0' OR
           v_prior_lock_timeout::interval > interval '#{@lock_timeout_ms} milliseconds' THEN
          PERFORM set_config('lock_timeout', '#{@lock_timeout_ms}ms', true);
        END IF;

        <<authority>>
        BEGIN
          IF current_setting('transaction_read_only') = 'on' THEN
            v_error_reason := 'read_only_transaction';
            EXIT authority;
          END IF;

          IF current_setting('transaction_isolation') <> 'read committed' THEN
            v_error_reason := 'unsupported_isolation';
            EXIT authority;
          END IF;

          IF p_now IS NULL OR p_cutoff IS NULL OR p_cutoff > p_now OR
             p_demand IS NULL OR p_demand <= 0 OR
             p_max_attempts IS NULL OR p_max_attempts <= 0 OR
             p_preference IS NOT NULL AND p_preference NOT IN ('ready', 'expired') THEN
            RAISE EXCEPTION 'invalid docket tenant-fair function arguments'
              USING ERRCODE = '22023';
          END IF;

          SELECT ARRAY(
            SELECT DISTINCT key
            FROM unnest(COALESCE(p_candidate_keys, ARRAY[]::text[])) AS hinted(key)
            WHERE key IS NOT NULL
            ORDER BY key
            LIMIT LEAST(p_demand, #{@partition_budget})
          )
          INTO v_keys;
          v_eligible_partitions := cardinality(v_keys);

          SELECT admission_mode, readiness, required_function_contract,
                 mode_epoch, readiness_epoch
          INTO v_gate_mode, v_gate_readiness, v_gate_contract,
               v_mode_epoch, v_readiness_epoch
          FROM #{gate}
          WHERE id = 1
          FOR SHARE SKIP LOCKED;

          IF NOT FOUND THEN
            v_error_reason := 'lock_contention';
            EXIT authority;
          END IF;

          IF v_gate_mode <> 'tenant_fair' THEN
            v_error_reason := 'inactive_engine';
            EXIT authority;
          END IF;

          IF v_gate_readiness <> 'ready' OR v_readiness_epoch <= 0 THEN
            v_error_reason := 'not_ready';
            EXIT authority;
          END IF;

          IF v_gate_contract <> #{@version} OR v_mode_epoch <= 0 THEN
            v_error_reason := 'function_contract_mismatch';
            EXIT authority;
          END IF;

          SELECT preferred_active, max_active, weight, borrowing, initialized_at
          INTO v_default_preferred, v_default_max, v_default_weight,
               v_default_borrowing, v_default_initialized_at
          FROM #{policy}
          WHERE id = 1
          FOR SHARE SKIP LOCKED;

          IF NOT FOUND THEN
            v_error_reason := CASE
              WHEN EXISTS (SELECT 1 FROM #{policy} WHERE id = 1)
                THEN 'lock_contention'
              ELSE 'not_initialized'
            END;
            EXIT authority;
          END IF;

          IF v_default_initialized_at IS NULL OR v_default_preferred IS NULL OR
             v_default_max IS NULL OR v_default_weight IS NULL OR
             v_default_borrowing IS NULL THEN
            v_error_reason := 'not_initialized';
            EXIT authority;
          END IF;

          FOR v_partition IN
            SELECT partitions.scope_key
            FROM #{partitions} AS partitions
            WHERE partitions.scope_key = ANY(v_keys)
            ORDER BY partitions.scope_key
            FOR NO KEY UPDATE SKIP LOCKED
          LOOP
            v_locked_keys := array_append(v_locked_keys, v_partition.scope_key::text);
          END LOOP;

          v_locked_partitions := cardinality(v_locked_keys);

          IF v_locked_partitions > 0 THEN
            FOR v_index IN 1..v_locked_partitions LOOP
              SELECT partitions.scope_key,
                     partitions.preferred_active,
                     partitions.max_active,
                     partitions.weight,
                     partitions.borrowing,
                     partitions.admin_state
              INTO v_partition
              FROM #{partitions} AS partitions
              WHERE partitions.scope_key = v_locked_keys[v_index];

              IF v_partition.max_active IS NULL THEN
                v_partition.preferred_active := v_default_preferred;
                v_partition.max_active := v_default_max;
                v_partition.weight := v_default_weight;
                v_partition.borrowing := v_default_borrowing;
                v_default_policy_partitions := v_default_policy_partitions + 1;
              ELSE
                v_override_policy_partitions := v_override_policy_partitions + 1;
              END IF;

              CASE v_partition.admin_state
                WHEN 'running' THEN v_running_partitions := v_running_partitions + 1;
                WHEN 'hold_new' THEN v_hold_new_partitions := v_hold_new_partitions + 1;
                WHEN 'drain' THEN v_drain_partitions := v_drain_partitions + 1;
              END CASE;

              IF v_partition.admin_state = 'running' AND EXISTS (
                SELECT 1
                FROM #{runs} AS runs
                WHERE runs.scope_key = v_partition.scope_key
                  AND runs.status = 'running'
                  AND runs.poisoned_at IS NULL
                  AND runs.claim_token IS NULL
                  AND runs.wake_at IS NOT NULL
                  AND runs.wake_at <= p_now
                  AND runs.claim_attempts < p_max_attempts
              ) THEN
                UPDATE #{partitions}
                SET admission_epoch = admission_epoch + 1
                WHERE scope_key = v_partition.scope_key;
              END IF;

              SELECT count(*)::bigint
              INTO v_live_count
              FROM #{runs} AS runs
              WHERE runs.scope_key = v_partition.scope_key
                AND runs.status = 'running'
                AND runs.poisoned_at IS NULL
                AND runs.claim_token IS NOT NULL;

              IF v_partition.admin_state = 'running' AND
                 v_live_count >= v_partition.max_active AND EXISTS (
                SELECT 1
                FROM #{runs} AS runs
                WHERE runs.scope_key = v_partition.scope_key
                  AND runs.status = 'running'
                  AND runs.poisoned_at IS NULL
                  AND runs.claim_token IS NULL
                  AND runs.wake_at IS NOT NULL
                  AND runs.wake_at <= p_now
                  AND runs.claim_attempts < p_max_attempts
              ) THEN
                v_cap_denied_partitions := v_cap_denied_partitions + 1;
              END IF;

              v_key_max := array_append(v_key_max, v_partition.max_active::integer);
              v_key_state := array_append(v_key_state, v_partition.admin_state::text);
              v_key_live := array_append(v_key_live, v_live_count::bigint);
            END LOOP;

            WITH key_config AS MATERIALIZED (
              SELECT config.scope_key, config.max_active,
                     config.admin_state, config.live_count
              FROM unnest(v_locked_keys, v_key_max, v_key_state, v_key_live)
                AS config(scope_key, max_active, admin_state, live_count)
            ),
            visibility AS MATERIALIZED (
              SELECT key_config.scope_key,
                     EXISTS (
                       SELECT 1 FROM #{runs} AS runs
                       WHERE runs.scope_key = key_config.scope_key
                         AND runs.status = 'running'
                         AND runs.poisoned_at IS NULL
                         AND runs.claim_token IS NULL
                         AND runs.wake_at IS NOT NULL AND runs.wake_at <= p_now
                         AND runs.claim_attempts >= p_max_attempts
                     ) OR (
                       key_config.admin_state = 'running' AND
                       key_config.live_count < key_config.max_active AND
                       EXISTS (
                         SELECT 1 FROM #{runs} AS runs
                         WHERE runs.scope_key = key_config.scope_key
                           AND runs.status = 'running'
                           AND runs.poisoned_at IS NULL
                           AND runs.claim_token IS NULL
                           AND runs.wake_at IS NOT NULL AND runs.wake_at <= p_now
                           AND runs.claim_attempts < p_max_attempts
                       )
                     ) AS ready_visible,
                     EXISTS (
                       SELECT 1 FROM #{runs} AS runs
                       WHERE runs.scope_key = key_config.scope_key
                         AND runs.status = 'running'
                         AND runs.poisoned_at IS NULL
                         AND runs.claim_token IS NOT NULL
                         AND runs.claimed_at < p_cutoff
                         AND runs.claim_attempts >= p_max_attempts
                     ) OR (
                       key_config.admin_state <> 'drain' AND
                       EXISTS (
                         SELECT 1 FROM #{runs} AS runs
                         WHERE runs.scope_key = key_config.scope_key
                           AND runs.status = 'running'
                           AND runs.poisoned_at IS NULL
                           AND runs.claim_token IS NOT NULL
                           AND runs.claimed_at < p_cutoff
                           AND runs.claim_attempts < p_max_attempts
                       )
                     ) AS expired_visible
              FROM key_config
            )
            SELECT COALESCE(bool_or(ready_visible), false),
                   COALESCE(bool_or(expired_visible), false),
                   max(scope_key) FILTER (WHERE ready_visible),
                   max(scope_key) FILTER (WHERE expired_visible)
            INTO v_page_ready, v_page_expired,
                 v_last_ready_key, v_last_expired_key
            FROM visibility;

            v_need_ready := p_demand >= 2 AND v_page_ready;
            v_need_expired := p_demand >= 2 AND v_page_expired;

            FOR v_index IN 1..v_locked_partitions LOOP
              EXIT WHEN v_remaining = 0 OR v_lock_budget = 0;

              v_partition.scope_key := v_locked_keys[v_index];
              v_partition.max_active := v_key_max[v_index];
              v_partition.admin_state := v_key_state[v_index];
              v_live_count := v_key_live[v_index];
              v_ready_slots := CASE
                WHEN v_partition.admin_state = 'running'
                  THEN greatest(v_partition.max_active::bigint - v_live_count, 0)
                ELSE 0
              END;

              v_planned_ids := ARRAY[]::bigint[];
              v_planned_classes := ARRAY[]::text[];
              v_planned_poison := ARRAY[]::boolean[];
              v_key_acquired := 0;

              FOR v_result IN
                WITH ready_poison_raw AS MATERIALIZED (
                  SELECT runs.id, runs.wake_at AS eligible_at,
                         'ready'::text AS work_class, true AS poison
                  FROM #{runs} AS runs
                  WHERE runs.scope_key = v_partition.scope_key
                    AND runs.status = 'running'
                    AND runs.poisoned_at IS NULL
                    AND runs.claim_token IS NULL
                    AND runs.wake_at IS NOT NULL AND runs.wake_at <= p_now
                    AND runs.claim_attempts >= p_max_attempts
                  ORDER BY runs.wake_at, runs.id
                  LIMIT v_remaining
                ),
                ready_ordinary_raw AS MATERIALIZED (
                  SELECT runs.id, runs.wake_at AS eligible_at,
                         'ready'::text AS work_class, false AS poison
                  FROM #{runs} AS runs
                  WHERE runs.scope_key = v_partition.scope_key
                    AND runs.status = 'running'
                    AND runs.poisoned_at IS NULL
                    AND runs.claim_token IS NULL
                    AND runs.wake_at IS NOT NULL AND runs.wake_at <= p_now
                    AND runs.claim_attempts < p_max_attempts
                  ORDER BY runs.wake_at, runs.id
                  LIMIT v_remaining
                ),
                expired_poison_raw AS MATERIALIZED (
                  SELECT runs.id, runs.claimed_at AS eligible_at,
                         'expired'::text AS work_class, true AS poison
                  FROM #{runs} AS runs
                  WHERE runs.scope_key = v_partition.scope_key
                    AND runs.status = 'running'
                    AND runs.poisoned_at IS NULL
                    AND runs.claim_token IS NOT NULL
                    AND runs.claimed_at < p_cutoff
                    AND runs.claim_attempts >= p_max_attempts
                  ORDER BY runs.claimed_at, runs.id
                  LIMIT v_remaining
                ),
                expired_ordinary_raw AS MATERIALIZED (
                  SELECT runs.id, runs.claimed_at AS eligible_at,
                         'expired'::text AS work_class, false AS poison
                  FROM #{runs} AS runs
                  WHERE runs.scope_key = v_partition.scope_key
                    AND runs.status = 'running'
                    AND runs.poisoned_at IS NULL
                    AND runs.claim_token IS NOT NULL
                    AND runs.claimed_at < p_cutoff
                    AND runs.claim_attempts < p_max_attempts
                  ORDER BY runs.claimed_at, runs.id
                  LIMIT v_remaining
                ),
                ready_ordinary_ranked AS MATERIALIZED (
                  SELECT raw.*,
                         row_number() OVER (ORDER BY eligible_at, id) AS slot_rank
                  FROM ready_ordinary_raw AS raw
                ),
                planned AS MATERIALIZED (
                  SELECT * FROM ready_poison_raw
                  UNION ALL
                  SELECT id, eligible_at, work_class, poison
                  FROM ready_ordinary_ranked
                  WHERE v_partition.admin_state = 'running'
                    AND slot_rank <= v_ready_slots
                  UNION ALL
                  SELECT * FROM expired_poison_raw
                  UNION ALL
                  SELECT * FROM expired_ordinary_raw
                  WHERE v_partition.admin_state <> 'drain'
                ),
                decision_ranked AS MATERIALIZED (
                  SELECT planned.*,
                         row_number() OVER (
                           PARTITION BY work_class
                           ORDER BY CASE WHEN poison THEN 0 ELSE 1 END,
                                    eligible_at, id
                         ) AS decision_rank
                  FROM planned
                ),
                decision_source AS MATERIALIZED (
                  SELECT id, eligible_at, work_class, poison
                  FROM decision_ranked
                  WHERE decision_rank <= v_remaining
                ),
                eligible_at_start AS MATERIALIZED (
                  SELECT decision.*
                  FROM decision_source AS decision
                  JOIN #{runs} AS runs ON runs.id = decision.id
                  WHERE runs.scope_key = v_partition.scope_key
                    AND runs.status = 'running'
                    AND runs.poisoned_at IS NULL
                    AND (
                      decision.work_class = 'ready' AND
                      runs.claim_token IS NULL AND
                      runs.wake_at IS NOT NULL AND runs.wake_at <= p_now AND
                      (decision.poison AND runs.claim_attempts >= p_max_attempts OR
                       NOT decision.poison AND runs.claim_attempts < p_max_attempts) OR
                      decision.work_class = 'expired' AND
                      runs.claim_token IS NOT NULL AND runs.claimed_at < p_cutoff AND
                      (decision.poison AND runs.claim_attempts >= p_max_attempts OR
                       NOT decision.poison AND runs.claim_attempts < p_max_attempts)
                    )
                ),
                locked AS MATERIALIZED (
                  SELECT runs.id, eligible.work_class,
                         eligible.eligible_at, eligible.poison
                  FROM eligible_at_start AS eligible
                  JOIN #{runs} AS runs ON runs.id = eligible.id
                  WHERE runs.scope_key = v_partition.scope_key
                    AND runs.status = 'running'
                    AND runs.poisoned_at IS NULL
                    AND (
                      eligible.work_class = 'ready' AND
                      runs.claim_token IS NULL AND
                      runs.wake_at IS NOT NULL AND runs.wake_at <= p_now AND
                      (eligible.poison AND runs.claim_attempts >= p_max_attempts OR
                       NOT eligible.poison AND runs.claim_attempts < p_max_attempts) OR
                      eligible.work_class = 'expired' AND
                      runs.claim_token IS NOT NULL AND runs.claimed_at < p_cutoff AND
                      (eligible.poison AND runs.claim_attempts >= p_max_attempts OR
                       NOT eligible.poison AND runs.claim_attempts < p_max_attempts)
                    )
                  ORDER BY runs.id
                  FOR UPDATE OF runs SKIP LOCKED
                ),
                lock_counts AS MATERIALIZED (
                  SELECT count(*)::bigint AS acquired,
                         count(*) FILTER (WHERE work_class = 'ready')::bigint AS ready,
                         count(*) FILTER (WHERE work_class = 'expired')::bigint AS expired
                  FROM locked
                ),
                locked_ranked AS MATERIALIZED (
                  SELECT locked.*,
                         row_number() OVER (
                           PARTITION BY work_class
                           ORDER BY CASE WHEN poison THEN 0 ELSE 1 END,
                                    eligible_at, id
                         ) AS class_rank
                  FROM locked
                ),
                choice_ranked AS MATERIALIZED (
                  SELECT ranked.*,
                         row_number() OVER (
                           ORDER BY
                             CASE
                               WHEN v_remaining >= 2 AND ranked.class_rank = 1 AND
                                    (v_need_ready AND ranked.work_class = 'ready' OR
                                     v_need_expired AND ranked.work_class = 'expired')
                                 THEN 0
                               ELSE 1
                             END,
                             CASE
                             WHEN v_remaining = 1 AND ranked.work_class = p_preference THEN 0
                               ELSE 1
                             END,
                             CASE WHEN ranked.poison THEN 0 ELSE 1 END,
                             ranked.eligible_at,
                             ranked.id
                         ) AS choice_rank,
                         greatest(
                           v_remaining -
                           CASE
                             WHEN v_remaining >= 2 AND v_need_ready AND lock_counts.ready = 0 AND
                                  v_partition.scope_key IS DISTINCT FROM v_last_ready_key
                               THEN 1 ELSE 0
                           END -
                           CASE
                             WHEN v_remaining >= 2 AND v_need_expired AND lock_counts.expired = 0 AND
                                  v_partition.scope_key IS DISTINCT FROM v_last_expired_key
                               THEN 1 ELSE 0
                           END,
                           0
                         ) AS allowed_count
                  FROM locked_ranked AS ranked
                  CROSS JOIN lock_counts
                ),
                chosen AS MATERIALIZED (
                  SELECT id, work_class, eligible_at, poison
                  FROM choice_ranked
                  WHERE choice_rank <= allowed_count
                ),
                updated AS (
                  UPDATE #{runs} AS runs
                  SET claim_token = CASE WHEN chosen.poison
                        THEN NULL ELSE pg_catalog.gen_random_uuid() END,
                      claimed_at = CASE WHEN chosen.poison THEN NULL ELSE p_now END,
                      wake_at = NULL,
                      claim_attempts = CASE WHEN chosen.poison
                        THEN runs.claim_attempts ELSE runs.claim_attempts + 1 END,
                      poisoned_at = CASE WHEN chosen.poison THEN p_now ELSE NULL END,
                      poison_reason = CASE WHEN chosen.poison
                        THEN 'max_claim_attempts_exceeded' ELSE NULL END
                  FROM chosen
                  WHERE runs.id = chosen.id
                    AND runs.status = 'running'
                    AND runs.poisoned_at IS NULL
                    AND (
                      chosen.work_class = 'ready' AND
                      runs.claim_token IS NULL AND
                      runs.wake_at IS NOT NULL AND runs.wake_at <= p_now AND
                      (chosen.poison AND runs.claim_attempts >= p_max_attempts OR
                       NOT chosen.poison AND runs.claim_attempts < p_max_attempts) OR
                      chosen.work_class = 'expired' AND
                      runs.claim_token IS NOT NULL AND runs.claimed_at < p_cutoff AND
                      (chosen.poison AND runs.claim_attempts >= p_max_attempts OR
                       NOT chosen.poison AND runs.claim_attempts < p_max_attempts)
                    )
                  RETURNING runs.id, runs.run_id, runs.tenant_id, runs.graph_id,
                            runs.graph_hash, runs.checkpoint_seq, runs.claim_token,
                            runs.claimed_at, runs.claim_attempts, runs.poisoned_at,
                            runs.poison_reason, chosen.work_class, chosen.eligible_at
                ),
                accounting AS MATERIALIZED (
                  SELECT
                    (SELECT count(*) FROM ready_poison_raw) +
                      (SELECT count(*) FROM ready_ordinary_raw) AS ready_raw,
                    (SELECT count(*) FROM expired_poison_raw) +
                      (SELECT count(*) FROM expired_ordinary_raw) AS expired_raw,
                    COALESCE(array_agg(decision.id ORDER BY decision.id), ARRAY[]::bigint[])
                      AS planned_ids,
                    COALESCE(array_agg(decision.work_class ORDER BY decision.id), ARRAY[]::text[])
                      AS planned_classes,
                    COALESCE(array_agg(decision.poison ORDER BY decision.id), ARRAY[]::boolean[])
                      AS planned_poison,
                    (SELECT acquired FROM lock_counts) AS acquired,
                    (SELECT ready FROM lock_counts) AS locked_ready,
                    (SELECT expired FROM lock_counts) AS locked_expired
                  FROM decision_source AS decision
                )
                SELECT result.*
                FROM (
                  SELECT 0 AS sort_group, updated.id AS sort_id, 'outcome'::text AS kind,
                         updated.run_id, updated.tenant_id, updated.graph_id,
                         updated.graph_hash, updated.checkpoint_seq,
                         updated.claim_token, updated.claimed_at,
                         updated.claim_attempts, updated.poisoned_at,
                         updated.poison_reason, updated.work_class,
                         updated.eligible_at, 0::bigint AS ready_raw,
                         0::bigint AS expired_raw, NULL::bigint[] AS planned_ids,
                         NULL::text[] AS planned_classes,
                         NULL::boolean[] AS planned_poison, 0::bigint AS acquired,
                         0::bigint AS locked_ready, 0::bigint AS locked_expired
                  FROM updated
                  UNION ALL
                  SELECT 1, 0::bigint, 'accounting', NULL::text, NULL::text,
                         NULL::text, NULL::text, NULL::bigint, NULL::uuid,
                         NULL::timestamp with time zone, NULL::integer,
                         NULL::timestamp with time zone, NULL::text, NULL::text,
                         NULL::timestamp with time zone, accounting.ready_raw,
                         accounting.expired_raw, accounting.planned_ids,
                         accounting.planned_classes, accounting.planned_poison,
                         accounting.acquired, accounting.locked_ready,
                         accounting.locked_expired
                  FROM accounting
                ) AS result
                ORDER BY result.sort_group, result.sort_id
              LOOP
                IF v_result.kind = 'accounting' THEN
                  v_ready_candidates := v_ready_candidates + v_result.ready_raw;
                  v_expired_candidates := v_expired_candidates + v_result.expired_raw;
                  v_candidate_rows_examined := v_candidate_rows_examined +
                    v_result.ready_raw + v_result.expired_raw;
                  v_planned_ids := v_result.planned_ids;
                  v_planned_classes := v_result.planned_classes;
                  v_planned_poison := v_result.planned_poison;
                  v_key_acquired := v_result.acquired;
                ELSE
                  v_remaining := v_remaining - 1;

                  IF v_result.work_class = 'ready' THEN
                    v_need_ready := false;

                    IF v_result.claim_token IS NULL THEN
                      v_ready_poisoned := v_ready_poisoned + 1;
                    ELSE
                      v_ready_leases := v_ready_leases + 1;
                      v_wait_ms := greatest(
                        floor(extract(epoch FROM (p_now - v_result.eligible_at)) * 1000)::bigint,
                        0
                      );
                      v_ready_wait_count := v_ready_wait_count + 1;
                      v_ready_wait_sum := v_ready_wait_sum + v_wait_ms;
                      v_ready_wait_max := greatest(v_ready_wait_max, v_wait_ms);
                    END IF;
                  ELSE
                    v_need_expired := false;
                    v_wait_ms := greatest(
                      floor(extract(epoch FROM (p_cutoff - v_result.eligible_at)) * 1000)::bigint,
                      0
                    );
                    v_expired_wait_count := v_expired_wait_count + 1;
                    v_expired_wait_sum := v_expired_wait_sum + v_wait_ms;
                    v_expired_wait_max := greatest(v_expired_wait_max, v_wait_ms);

                    IF v_result.claim_token IS NULL THEN
                      v_expired_poisoned := v_expired_poisoned + 1;
                    ELSE
                      v_expired_leases := v_expired_leases + 1;
                    END IF;
                  END IF;

                  v_out_run_id := array_append(v_out_run_id, v_result.run_id::text);
                  v_out_tenant_id := array_append(v_out_tenant_id, v_result.tenant_id::text);
                  v_out_graph_id := array_append(v_out_graph_id, v_result.graph_id::text);
                  v_out_graph_hash := array_append(v_out_graph_hash, v_result.graph_hash::text);
                  v_out_checkpoint_seq := array_append(v_out_checkpoint_seq, v_result.checkpoint_seq::bigint);
                  v_out_claim_token := array_append(v_out_claim_token, v_result.claim_token::uuid);
                  v_out_claimed_at := array_append(v_out_claimed_at, v_result.claimed_at::timestamp with time zone);
                  v_out_claim_attempt := array_append(v_out_claim_attempt, v_result.claim_attempts::integer);
                  v_out_poisoned_at := array_append(v_out_poisoned_at, v_result.poisoned_at::timestamp with time zone);
                  v_out_poison_reason := array_append(v_out_poison_reason, v_result.poison_reason::text);
                  v_out_work_class := array_append(v_out_work_class, v_result.work_class::text);
                  v_out_eligible_at := array_append(v_out_eligible_at, v_result.eligible_at::timestamp with time zone);
                END IF;
              END LOOP;

              v_acquired_for_lock := v_acquired_for_lock + v_key_acquired;
              v_lock_budget := v_lock_budget - v_key_acquired;

              IF v_key_acquired = 0 AND cardinality(v_planned_ids) > 0 THEN
                v_observed_for_lock := v_observed_for_lock + cardinality(v_planned_ids);

                SELECT count(*)::bigint,
                       count(*) FILTER (
                         WHERE runs.id IS NOT NULL AND
                           runs.status = 'running' AND
                           runs.poisoned_at IS NULL AND
                           (
                             omitted.work_class = 'ready' AND
                             runs.claim_token IS NULL AND
                             runs.wake_at IS NOT NULL AND runs.wake_at <= p_now AND
                             (omitted.poison AND runs.claim_attempts >= p_max_attempts OR
                              NOT omitted.poison AND runs.claim_attempts < p_max_attempts) OR
                             omitted.work_class = 'expired' AND
                             runs.claim_token IS NOT NULL AND runs.claimed_at < p_cutoff AND
                             (omitted.poison AND runs.claim_attempts >= p_max_attempts OR
                              NOT omitted.poison AND runs.claim_attempts < p_max_attempts)
                           )
                       )::bigint
                INTO v_omitted_count, v_omitted_eligible
                FROM unnest(v_planned_ids, v_planned_classes, v_planned_poison)
                  AS omitted(id, work_class, poison)
                LEFT JOIN #{runs} AS runs ON runs.id = omitted.id;

                v_recheck_invalidated := v_recheck_invalidated +
                  v_omitted_count - v_omitted_eligible;
                v_specifically_skipped := v_specifically_skipped + v_omitted_eligible;
              END IF;

              IF v_partition.scope_key IS NOT DISTINCT FROM v_last_ready_key THEN
                v_need_ready := false;
              END IF;

              IF v_partition.scope_key IS NOT DISTINCT FROM v_last_expired_key THEN
                v_need_expired := false;
              END IF;
            END LOOP;
          END IF;

          v_skipped_partitions := v_eligible_partitions - v_locked_partitions;

          IF v_eligible_partitions > 0 AND v_locked_partitions = 0 THEN
            v_error_reason := 'lock_contention';
            EXIT authority;
          END IF;

          IF v_observed_for_lock > 0 AND v_acquired_for_lock = 0 AND
             v_recheck_invalidated = 0 AND
             v_specifically_skipped = v_observed_for_lock THEN
            v_error_reason := 'lock_contention';
            EXIT authority;
          END IF;
        EXCEPTION
          WHEN SQLSTATE '55P03' THEN
            v_error_reason := 'lock_contention';
        END authority;

        PERFORM set_config('lock_timeout', v_prior_lock_timeout, true);

        IF v_error_reason IS NOT NULL THEN
          RETURN QUERY SELECT 'error'::text, v_error_reason::text,
          #{error_tail};
          RETURN;
        END IF;

        IF cardinality(v_out_run_id) > 0 THEN
          FOR v_index IN 1..cardinality(v_out_run_id) LOOP
            RETURN QUERY SELECT
              'outcome'::text,
              NULL::text,
              v_out_run_id[v_index],
              v_out_tenant_id[v_index],
              v_out_graph_id[v_index],
              v_out_graph_hash[v_index],
              v_out_checkpoint_seq[v_index],
              v_out_claim_token[v_index],
              v_out_claimed_at[v_index],
              v_out_claim_attempt[v_index],
              v_out_poisoned_at[v_index],
              v_out_poison_reason[v_index],
              v_out_work_class[v_index],
              v_out_eligible_at[v_index],
              #{null_columns(14)};
          END LOOP;
        END IF;

        RETURN QUERY SELECT
          'summary'::text,
          NULL::text,
          #{null_columns_range(2, 12)},
          v_eligible_partitions,
          v_locked_partitions,
          v_skipped_partitions,
          v_cap_denied_partitions,
          0::bigint,
          v_default_policy_partitions,
          v_override_policy_partitions,
          v_running_partitions,
          v_hold_new_partitions,
          v_drain_partitions,
          0::bigint,
          0::bigint,
          v_ready_leases,
          v_ready_poisoned,
          v_expired_leases,
          v_expired_poisoned,
          v_candidate_rows_examined,
          0::bigint,
          v_ready_wait_count,
          v_ready_wait_sum,
          v_ready_wait_max,
          v_expired_wait_count,
          v_expired_wait_sum,
          v_expired_wait_max,
          v_mode_epoch,
          v_gate_contract,
          v_ready_candidates,
          v_expired_candidates;
      END;
      """
      |> String.trim_leading("\n")
    end

    defp null_columns(drop), do: null_columns(length(@result_columns), drop)

    defp null_columns(total, drop) do
      @result_columns
      |> Enum.slice(drop, total - drop)
      |> Enum.map_join(",\n          ", fn {_name, type} -> "NULL::#{type}" end)
    end

    defp null_columns_range(start, count) do
      @result_columns
      |> Enum.slice(start, count)
      |> Enum.map_join(",\n          ", fn {_name, type} -> "NULL::#{type}" end)
    end
  end
end
