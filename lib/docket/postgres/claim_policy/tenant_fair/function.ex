if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair.Function do
    @moduledoc false

    alias Docket.Postgres.Storage

    @name "docket_tenant_fair_claim_v1"
    @identity_arguments "timestamp with time zone, timestamp with time zone, integer, integer, text, integer, text[]"
    @lock_timeout_ms 250
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
      eligible_at: "timestamp with time zone"
    ]

    def name, do: @name
    def identity_arguments, do: @identity_arguments
    def lock_timeout_ms, do: @lock_timeout_ms
    def partition_budget, do: @partition_budget

    def result_definition do
      Enum.map_join(@result_columns, ",\n        ", fn {name, type} -> "#{name} #{type}" end)
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
      policy = Storage.qualified_table(prefix, "docket_claim_policy")
      partitions = Storage.qualified_table(prefix, "docket_claim_partitions")
      runs = Storage.qualified_table(prefix, "docket_runs")

      """
      DECLARE
        p_now ALIAS FOR $1;
        p_cutoff ALIAS FOR $2;
        p_demand ALIAS FOR $3;
        p_max_attempts ALIAS FOR $4;
        p_preference ALIAS FOR $5;
        p_default_max ALIAS FOR $6;
        p_candidate_keys ALIAS FOR $7;
        v_prior_lock_timeout text := current_setting('lock_timeout');
        v_default_max integer;
        v_keys text[];
        v_locked_keys text[];
        v_scope_key text;
        v_partition record;
        v_live_count bigint;
        v_ready_slots bigint;
        v_remaining integer := p_demand;
        v_partitions_remaining integer;
        v_quota integer;
        v_claimed integer;
      BEGIN
        IF p_now IS NULL OR p_cutoff IS NULL OR p_cutoff > p_now OR
           p_demand IS NULL OR p_demand <= 0 OR
           p_max_attempts IS NULL OR p_max_attempts <= 0 OR
           p_default_max IS NULL OR p_default_max <= 0 OR
           p_preference IS NOT NULL AND p_preference NOT IN ('ready', 'expired') THEN
          RAISE EXCEPTION 'invalid docket tenant-fair function arguments'
            USING ERRCODE = '22023';
        END IF;

        IF current_setting('transaction_read_only') = 'on' THEN
          RETURN QUERY SELECT 'error'::text, 'read_only_transaction'::text,
            NULL::text, NULL::text, NULL::text, NULL::text, NULL::bigint,
            NULL::uuid, NULL::timestamp with time zone, NULL::integer,
            NULL::timestamp with time zone, NULL::text, NULL::text,
            NULL::timestamp with time zone;
          RETURN;
        END IF;

        IF current_setting('transaction_isolation') <> 'read committed' THEN
          RETURN QUERY SELECT 'error'::text, 'unsupported_isolation'::text,
            NULL::text, NULL::text, NULL::text, NULL::text, NULL::bigint,
            NULL::uuid, NULL::timestamp with time zone, NULL::integer,
            NULL::timestamp with time zone, NULL::text, NULL::text,
            NULL::timestamp with time zone;
          RETURN;
        END IF;

        IF v_prior_lock_timeout = '0' OR
           v_prior_lock_timeout::interval > interval '#{@lock_timeout_ms} milliseconds' THEN
          PERFORM set_config('lock_timeout', '#{@lock_timeout_ms}ms', true);
        END IF;

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

        SELECT max_active
        INTO v_default_max
        FROM #{policy}
        WHERE id = 1
        FOR SHARE;

        IF v_default_max IS NULL THEN
          RAISE EXCEPTION 'docket claim policy is not initialized'
            USING ERRCODE = '55000';
        END IF;

        SELECT ARRAY(
          SELECT hinted.key
          FROM unnest(COALESCE(p_candidate_keys, ARRAY[]::text[]))
            WITH ORDINALITY AS hinted(key, position)
          WHERE hinted.key IS NOT NULL
          GROUP BY hinted.key
          ORDER BY min(hinted.position)
          LIMIT #{@partition_budget}
        )
        INTO v_keys;

        SELECT COALESCE(
                 array_agg(locked.scope_key ORDER BY locked.position),
                 ARRAY[]::text[]
               )
        INTO v_locked_keys
        FROM (
          SELECT partitions.scope_key,
                 array_position(v_keys, partitions.scope_key) AS position
          FROM #{partitions} AS partitions
          WHERE partitions.scope_key = ANY(v_keys)
          ORDER BY array_position(v_keys, partitions.scope_key)
          LIMIT LEAST(cardinality(v_keys), p_demand + 1)
          FOR UPDATE OF partitions SKIP LOCKED
        ) AS locked;

        v_partitions_remaining := cardinality(v_locked_keys);

        FOREACH v_scope_key IN ARRAY v_locked_keys LOOP
          EXIT WHEN v_remaining = 0;

          SELECT scope_key, COALESCE(max_active, v_default_max) AS max_active
          INTO v_partition
          FROM #{partitions}
          WHERE scope_key = v_scope_key;

          SELECT count(*)::bigint
          INTO v_live_count
          FROM #{runs}
          WHERE scope_key = v_partition.scope_key
            AND status = 'running'
            AND poisoned_at IS NULL
            AND claim_token IS NOT NULL;

          v_ready_slots := greatest(v_partition.max_active::bigint - v_live_count, 0);
          v_quota := greatest(1, ceil(v_remaining::numeric / v_partitions_remaining)::integer);

          RETURN QUERY
          WITH ready_poison AS MATERIALIZED (
            SELECT id, wake_at AS eligible_at, 'ready'::text AS work_class,
                   true AS poison
            FROM #{runs}
            WHERE scope_key = v_partition.scope_key
              AND status = 'running'
              AND poisoned_at IS NULL
              AND claim_token IS NULL
              AND wake_at IS NOT NULL AND wake_at <= p_now
              AND claim_attempts >= p_max_attempts
            ORDER BY wake_at, id
            LIMIT v_quota
          ),
          ready_ordinary AS MATERIALIZED (
            SELECT id, wake_at AS eligible_at, 'ready'::text AS work_class,
                   false AS poison
            FROM #{runs}
            WHERE scope_key = v_partition.scope_key
              AND status = 'running'
              AND poisoned_at IS NULL
              AND claim_token IS NULL
              AND wake_at IS NOT NULL AND wake_at <= p_now
              AND claim_attempts < p_max_attempts
            ORDER BY wake_at, id
            LIMIT least(v_quota::bigint, v_ready_slots)
          ),
          expired_poison AS MATERIALIZED (
            SELECT id, claimed_at AS eligible_at, 'expired'::text AS work_class,
                   true AS poison
            FROM #{runs}
            WHERE scope_key = v_partition.scope_key
              AND status = 'running'
              AND poisoned_at IS NULL
              AND claim_token IS NOT NULL
              AND claimed_at < p_cutoff
              AND claim_attempts >= p_max_attempts
            ORDER BY claimed_at, id
            LIMIT v_quota
          ),
          expired_ordinary AS MATERIALIZED (
            SELECT id, claimed_at AS eligible_at, 'expired'::text AS work_class,
                   false AS poison
            FROM #{runs}
            WHERE scope_key = v_partition.scope_key
              AND status = 'running'
              AND poisoned_at IS NULL
              AND claim_token IS NOT NULL
              AND claimed_at < p_cutoff
              AND claim_attempts < p_max_attempts
            ORDER BY claimed_at, id
            LIMIT v_quota
          ),
          candidates AS MATERIALIZED (
            SELECT * FROM ready_poison
            UNION ALL SELECT * FROM ready_ordinary
            UNION ALL SELECT * FROM expired_poison
            UNION ALL SELECT * FROM expired_ordinary
          ),
          ranked AS MATERIALIZED (
            SELECT candidates.*,
                   row_number() OVER (
                     PARTITION BY work_class
                     ORDER BY CASE WHEN poison THEN 0 ELSE 1 END, eligible_at, id
                   ) AS class_rank
            FROM candidates
          ),
          chosen AS MATERIALIZED (
            SELECT id, eligible_at, work_class, poison
            FROM ranked
            ORDER BY
              CASE WHEN v_quota >= 2 AND class_rank = 1 THEN 0 ELSE 1 END,
              CASE WHEN v_quota = 1 AND work_class = p_preference THEN 0 ELSE 1 END,
              CASE WHEN poison THEN 0 ELSE 1 END,
              eligible_at,
              id
            LIMIT v_quota
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
                runs.wake_at IS NOT NULL AND runs.wake_at <= p_now OR
                chosen.work_class = 'expired' AND
                runs.claim_token IS NOT NULL AND runs.claimed_at < p_cutoff
              )
              AND (
                chosen.poison AND runs.claim_attempts >= p_max_attempts OR
                NOT chosen.poison AND runs.claim_attempts < p_max_attempts
              )
            RETURNING runs.run_id, runs.tenant_id, runs.graph_id, runs.graph_hash,
                      runs.checkpoint_seq, runs.claim_token, runs.claimed_at,
                      runs.claim_attempts, runs.poisoned_at, runs.poison_reason,
                      chosen.work_class, chosen.eligible_at
          )
          SELECT 'outcome'::text, NULL::text, updated.run_id, updated.tenant_id,
                 updated.graph_id, updated.graph_hash, updated.checkpoint_seq,
                 updated.claim_token, updated.claimed_at, updated.claim_attempts,
                 updated.poisoned_at, updated.poison_reason, updated.work_class,
                 updated.eligible_at
          FROM updated
          ORDER BY updated.eligible_at, updated.run_id;

          GET DIAGNOSTICS v_claimed = ROW_COUNT;

          -- Advance every considered partition, including cap-denied ones, so
          -- a full tenant at the head of discovery cannot pin the window.
          UPDATE #{partitions}
          SET admission_epoch = admission_epoch + 1,
              updated_at = p_now
          WHERE scope_key = v_partition.scope_key;

          v_remaining := v_remaining - v_claimed;
          v_partitions_remaining := v_partitions_remaining - 1;
        END LOOP;

        PERFORM set_config('lock_timeout', v_prior_lock_timeout, true);
      END;
      """
      |> String.trim_leading("\n")
    end
  end
end
