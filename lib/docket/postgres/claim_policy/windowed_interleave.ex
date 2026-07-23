if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.WindowedInterleave do
    @moduledoc """
    Experimental statistically-fair PostgreSQL admission engine.

    One set-based claim statement samples up to `limit` active scopes from
    `docket_claim_schedule` in random order, reads a bounded per-scope page of
    due ready work through the v2 partial indexes, and admits candidates
    breadth-first across scopes: every sampled scope's first-ranked run is
    considered before any scope's second-ranked run. Expired-claim recovery
    is scope-blind and identical to `Docket.Postgres.ClaimPolicy.Legacy`.

    Admission is sticky within a scope. A run's first claim installs
    `tenant_admitted_at`, and admitted due work ranks ahead of unadmitted work
    in that scope's page, so runs already in progress are re-acquired and
    driven to completion before new runs start. A scope's in-flight cohort
    therefore stays near its share of each claim batch, with no configured
    cap: new work is admitted only when a batch slot reaches the scope and no
    admitted run is due. Poisoning clears the marker. Unlike TenantFair,
    promotion order is near-FIFO rather than strict: a locked or stale queue
    head does not block later promotions.

    The engine runs under the `legacy` admission mode, holds no cursor, and
    takes no policy-row lock beyond the shared admission gate, so concurrent
    dispatchers admit in parallel. Startup configuration normalizes the
    persisted admission mode back to `legacy` when a previous configuration
    left it in another mode, so a rebooted instance always claims. Scope
    sampling considers only scopes that currently hold due ready work, so a
    single-claim call cannot land on a scope whose runs are all sleeping
    while due work exists elsewhere. Fairness across tenants is statistical —
    per fetch and across nodes — rather than the deterministic ring order and
    per-tenant `max_active` cap enforcement that
    `Docket.Postgres.ClaimPolicy.TenantFair` provides; per-tenant caps are
    not enforced. Requires schema version 2 for `docket_claim_schedule` and
    the scoped partial indexes.
    """

    @behaviour Docket.Postgres.ClaimPolicy

    alias Docket.Postgres.ClaimPolicy.Legacy
    alias Docket.Postgres.ClaimPolicy.Plan
    alias Docket.Postgres.Storage

    @impl true
    def init([], _context), do: {:ok, nil}
    def init(options, _context), do: {:error, {:unknown_options, Keyword.keys(options)}}

    @impl true
    def configure(%{prefix: prefix}, nil, query) do
      policy = Storage.qualified_table(prefix, "docket_claim_policy")

      statement = """
      INSERT INTO #{policy} (id, admission_mode, updated_at)
      VALUES (1, 'legacy', CURRENT_TIMESTAMP)
      ON CONFLICT (id) DO UPDATE
      SET admission_mode = 'legacy',
          updated_at = CURRENT_TIMESTAMP
      WHERE #{policy}.admission_mode IS DISTINCT FROM 'legacy'
      RETURNING admission_mode
      """

      case query.(statement, []) do
        {:ok, %{rows: rows}} when length(rows) in 0..1 -> :ok
        {:error, reason} -> {:error, reason}
        other -> {:error, {:unexpected_startup_configuration_result, other}}
      end
    end

    @impl true
    def build_plan(
          %{prefix: prefix, identifiers: %{runs: table, claim_policy: policy}},
          %{
            now: %DateTime{} = now,
            limit: limit,
            orphan_ttl_ms: ttl,
            max_claim_attempts: max,
            preference: preference
          },
          nil
        ) do
      now = normalize_database_datetime(now)
      cutoff = DateTime.add(now, -ttl, :millisecond)
      schedule = Storage.qualified_table(prefix, "docket_claim_schedule")

      %Plan{
        statement: claim_statement(table, policy, schedule),
        params: [now, cutoff, limit, max, preference && Atom.to_string(preference)],
        decoder: %{now: now, orphan_ttl_ms: ttl},
        observation: %{demand: limit, preference: preference}
      }
    end

    @impl true
    def decode(rows, decoder, nil), do: Legacy.decode(rows, decoder, nil)

    @impl true
    def observe(observation, stats, result, duration, nil),
      do: Legacy.observe(observation, stats, result, duration, nil)

    @doc false
    @spec claim_statement(String.t(), String.t(), String.t()) :: String.t()
    def claim_statement(table, policy, schedule)
        when is_binary(table) and is_binary(policy) and is_binary(schedule) do
      """
      WITH transaction_context AS MATERIALIZED (
        SELECT current_setting('transaction_isolation') AS isolation,
               current_setting('transaction_read_only') = 'on' AS read_only
      ),
      admission_gate AS MATERIALIZED (
        SELECT gate.admission_mode
        FROM transaction_context
        CROSS JOIN #{policy} AS gate
        WHERE transaction_context.isolation = 'read committed'
          AND NOT transaction_context.read_only
          AND gate.id = 1
        FOR SHARE SKIP LOCKED
      ),
      legacy_authority AS MATERIALIZED (
        SELECT admission_mode
        FROM admission_gate
        WHERE admission_mode = 'legacy'
      ),
      active_scopes AS MATERIALIZED (
        SELECT schedule.scope_key
        FROM legacy_authority
        CROSS JOIN #{schedule} AS schedule
        WHERE schedule.unfinished_count > 0
          AND (
            EXISTS (
              SELECT 1 FROM #{table} AS probe
              WHERE probe.scope_key = schedule.scope_key
                AND probe.status = 'running'
                AND probe.poisoned_at IS NULL
                AND probe.tenant_admitted_at IS NULL
                AND probe.claim_token IS NULL
                AND probe.wake_at IS NOT NULL AND probe.wake_at <= $1
            )
            OR EXISTS (
              SELECT 1 FROM #{table} AS probe
              WHERE probe.scope_key = schedule.scope_key
                AND probe.status = 'running'
                AND probe.poisoned_at IS NULL
                AND probe.tenant_admitted_at IS NOT NULL
                AND probe.claim_token IS NULL
                AND probe.wake_at IS NOT NULL AND probe.wake_at <= $1
            )
          )
        ORDER BY random()
        LIMIT $3
      ),
      ready_pool AS MATERIALIZED (
        SELECT scoped.id, scoped.eligible_at, scoped.scope_rank
        FROM active_scopes
        CROSS JOIN LATERAL (
          SELECT page.id, page.eligible_at,
                 ROW_NUMBER() OVER (
                   ORDER BY CASE WHEN page.admitted THEN 0 ELSE 1 END,
                            page.eligible_at, page.id
                 ) AS scope_rank
          FROM (
            (SELECT runs.id, runs.wake_at AS eligible_at, true AS admitted
             FROM #{table} AS runs
             WHERE runs.scope_key = active_scopes.scope_key
               AND runs.status = 'running'
               AND runs.poisoned_at IS NULL
               AND runs.tenant_admitted_at IS NOT NULL
               AND runs.claim_token IS NULL
               AND runs.wake_at IS NOT NULL AND runs.wake_at <= $1
             ORDER BY runs.wake_at, runs.id
             LIMIT $3)
            UNION ALL
            (SELECT runs.id, runs.wake_at AS eligible_at, false AS admitted
             FROM #{table} AS runs
             WHERE runs.scope_key = active_scopes.scope_key
               AND runs.status = 'running'
               AND runs.poisoned_at IS NULL
               AND runs.tenant_admitted_at IS NULL
               AND runs.claim_token IS NULL
               AND runs.wake_at IS NOT NULL AND runs.wake_at <= $1
             ORDER BY runs.wake_at, runs.id
             LIMIT $3)
          ) AS page
          ORDER BY scope_rank
          LIMIT $3
        ) AS scoped
      ),
      ready_candidates AS MATERIALIZED (
        SELECT candidate.id, ready_pool.eligible_at, ready_pool.scope_rank
        FROM ready_pool
        JOIN #{table} AS candidate ON candidate.id = ready_pool.id
        WHERE candidate.status = 'running'
          AND candidate.poisoned_at IS NULL
          AND candidate.claim_token IS NULL
          AND candidate.wake_at IS NOT NULL AND candidate.wake_at <= $1
        ORDER BY ready_pool.scope_rank, ready_pool.eligible_at, ready_pool.id
        LIMIT $3
        FOR UPDATE OF candidate SKIP LOCKED
      ),
      expired_candidates AS MATERIALIZED (
        SELECT runs.id, runs.claimed_at AS eligible_at
        FROM legacy_authority
        CROSS JOIN #{table} AS runs
        WHERE status = 'running'
          AND poisoned_at IS NULL
          AND claim_token IS NOT NULL
          AND claimed_at < $2
        ORDER BY claimed_at, id
        LIMIT $3
        FOR UPDATE SKIP LOCKED
      ),
      candidates AS MATERIALIZED (
        SELECT id, class, eligible_at
        FROM (
          SELECT id, eligible_at, 'ready' AS class,
                 ROW_NUMBER() OVER (ORDER BY scope_rank, eligible_at, id) AS class_rank
          FROM ready_candidates
          UNION ALL
          SELECT id, eligible_at, 'expired' AS class,
                 ROW_NUMBER() OVER (ORDER BY eligible_at, id) AS class_rank
          FROM expired_candidates
        ) AS eligible
        ORDER BY
          CASE WHEN $3 >= 2 AND class_rank = 1 THEN 0 ELSE 1 END,
          CASE WHEN $3 = 1 AND class = $5 THEN 0 ELSE 1 END,
          class_rank, eligible_at, id
        LIMIT $3
      ),
      updated AS (
        UPDATE #{table} AS runs
        SET claim_token =
              CASE WHEN runs.claim_attempts < $4 THEN gen_random_uuid() ELSE NULL END,
            claimed_at =
              CASE WHEN runs.claim_attempts < $4 THEN $1 ELSE NULL END,
            tenant_admitted_at =
              CASE
                WHEN runs.claim_attempts < $4 THEN COALESCE(runs.tenant_admitted_at, $1)
                ELSE NULL
              END,
            wake_at = NULL,
            claim_attempts =
              CASE
                WHEN runs.claim_attempts < $4 THEN runs.claim_attempts + 1
                ELSE runs.claim_attempts
              END,
            poisoned_at =
              CASE WHEN runs.claim_attempts < $4 THEN NULL ELSE $1 END,
            poison_reason =
              CASE
                WHEN runs.claim_attempts < $4 THEN NULL
                ELSE 'max_claim_attempts_exceeded'
              END
        FROM candidates
        WHERE runs.id = candidates.id
          AND runs.status = 'running'
          AND runs.poisoned_at IS NULL
        RETURNING
          runs.run_id,
          runs.tenant_id,
          runs.graph_id,
          runs.graph_hash,
          runs.checkpoint_seq,
          runs.claim_token,
          runs.claimed_at,
          runs.claim_attempts,
          runs.poisoned_at,
          runs.poison_reason,
          candidates.class,
          candidates.eligible_at
      )
      SELECT
        run_id,
        tenant_id,
        graph_id,
        graph_hash,
        checkpoint_seq,
        claim_token,
        claimed_at,
        claim_attempts,
        poisoned_at,
        poison_reason,
        class,
        eligible_at,
        (SELECT count(*) FROM ready_candidates),
        (SELECT count(*) FROM expired_candidates)
      FROM updated
      UNION ALL
      SELECT
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        '__docket_admission_gate__', $1,
        CASE
          WHEN transaction_context.read_only THEN -4
          WHEN transaction_context.isolation <> 'read committed' THEN -3
          WHEN EXISTS (SELECT 1 FROM admission_gate) THEN -2
          ELSE -1
        END,
        0
      FROM transaction_context
      WHERE NOT EXISTS (SELECT 1 FROM legacy_authority)
      ORDER BY run_id
      """
    end

    defp normalize_database_datetime(%DateTime{} = datetime) do
      datetime
      |> DateTime.to_unix(:microsecond)
      |> DateTime.from_unix!(:microsecond)
    end
  end
end
