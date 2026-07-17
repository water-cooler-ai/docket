if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.Legacy do
    @moduledoc """
    Current tenant-blind PostgreSQL admission engine.

    This module owns the complete legacy claim plan: ready and expired
    selection, class progress and preference, claim/steal/poison mutation,
    row decoding, selection statistics, and the established claim telemetry.
    """

    @behaviour Docket.Postgres.ClaimPolicy

    alias Docket.Postgres.ClaimPolicy.Plan

    @empty_claim_stats %{
      ready_candidates: 0,
      expired_candidates: 0,
      ready_selected: 0,
      expired_selected: 0,
      steals: 0,
      ready_oldest_age_ms: 0,
      expired_oldest_age_ms: 0
    }

    @impl true
    def init([], _context), do: {:ok, nil}
    def init(options, _context), do: {:error, {:unknown_options, Keyword.keys(options)}}

    @impl true
    def build_plan(
          %{identifiers: %{runs: table, claim_policy: policy}},
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

      %Plan{
        statement: claim_statement(table, policy),
        params: [now, cutoff, limit, max, preference && Atom.to_string(preference)],
        decoder: %{now: now, orphan_ttl_ms: ttl},
        observation: %{demand: limit, preference: preference}
      }
    end

    @impl true
    def decode(
          [
            [
              nil,
              nil,
              nil,
              nil,
              nil,
              nil,
              nil,
              nil,
              nil,
              nil,
              "__docket_admission_gate__",
              _now,
              gate_status,
              0
            ]
          ],
          _decoder,
          nil
        )
        when gate_status in [-4, -3, -2, -1] do
      reason =
        case gate_status do
          -4 -> :read_only_transaction
          -3 -> :unsupported_isolation
          -2 -> :inactive_engine
          -1 -> :lock_contention
        end

      {:error, {:claim_policy_unavailable, reason}, %{}}
    end

    def decode(rows, %{now: now, orphan_ttl_ms: orphan_ttl_ms}, nil) do
      {{leases, poisoned}, stats} =
        Enum.reduce(rows, {{[], []}, @empty_claim_stats}, fn
          [
            run_id,
            tenant_id,
            graph_id,
            graph_hash,
            checkpoint_seq,
            claim_token,
            claimed_at,
            claim_attempt,
            nil,
            nil,
            class,
            eligible_at,
            ready_candidates,
            expired_candidates
          ],
          {{leases, poisoned}, stats} ->
            lease = %{
              run_id: run_id,
              owner_scope: tenant_owner_scope(tenant_id),
              graph_id: graph_id,
              graph_hash: graph_hash,
              checkpoint_seq: checkpoint_seq,
              claim_token: load_uuid!(claim_token),
              claimed_at: claimed_at,
              claim_attempt: claim_attempt,
              orphan_ttl_ms: orphan_ttl_ms
            }

            stats =
              stats
              |> observe_outcome(class, eligible_at, ready_candidates, expired_candidates, now)
              |> observe_steal(class)

            {{[lease | leases], poisoned}, stats}

          [
            run_id,
            _tenant_id,
            _graph_id,
            _graph_hash,
            _checkpoint_seq,
            nil,
            nil,
            _claim_attempt,
            %DateTime{} = poisoned_at,
            poison_reason,
            class,
            eligible_at,
            ready_candidates,
            expired_candidates
          ],
          {{leases, poisoned}, stats} ->
            result = %{
              run_id: run_id,
              poisoned_at: poisoned_at,
              poison_reason: poison_reason
            }

            {{leases, [result | poisoned]},
             observe_outcome(stats, class, eligible_at, ready_candidates, expired_candidates, now)}
        end)

      {:ok, %{leases: Enum.reverse(leases), poisoned: Enum.reverse(poisoned)}, stats}
    end

    @impl true
    def observe(
          %{demand: demand, preference: preference},
          stats,
          {:ok, batch},
          duration,
          nil
        ) do
      fallback? =
        demand == 1 and preference != nil and
          ((preference == :ready and stats.expired_selected > 0) or
             (preference == :expired and stats.ready_selected > 0))

      :telemetry.execute(
        [:docket, :postgres, :run_store, :claim],
        Map.merge(stats, %{
          duration: duration,
          demand: demand,
          leases: length(batch.leases),
          poisoned: length(batch.poisoned),
          claim_attempts: Enum.sum(Enum.map(batch.leases, & &1.claim_attempt))
        }),
        %{preference: preference, fallback: fallback?, result: :ok}
      )

      Enum.each(batch.leases, fn lease ->
        :telemetry.execute(
          [:docket, :postgres, :claim, :attempt],
          %{count: 1, claim_attempts: lease.claim_attempt},
          %{result: if(lease.claim_attempt == 1, do: :acquired, else: :reacquired)}
        )
      end)

      Enum.each(batch.poisoned, fn poison ->
        :telemetry.execute(
          [:docket, :postgres, :claim, :poisoned],
          %{count: 1},
          %{reason: poison_reason(poison.poison_reason)}
        )
      end)

      :ok
    end

    def observe(
          %{demand: demand, preference: preference},
          _decoded_observation,
          {:error, _reason},
          duration,
          nil
        ) do
      :telemetry.execute(
        [:docket, :postgres, :run_store, :claim],
        %{
          duration: duration,
          demand: demand,
          leases: 0,
          poisoned: 0,
          steals: 0,
          claim_attempts: 0
        },
        %{preference: preference, fallback: false, result: :error}
      )

      :ok
    end

    @doc false
    @spec claim_statement(String.t(), String.t()) :: String.t()
    def claim_statement(table, policy) when is_binary(table) and is_binary(policy) do
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
      ready_candidates AS MATERIALIZED (
        SELECT runs.id, runs.wake_at AS eligible_at
        FROM legacy_authority
        CROSS JOIN #{table} AS runs
        WHERE status = 'running'
          AND poisoned_at IS NULL
          AND claim_token IS NULL
          AND wake_at <= $1
        ORDER BY wake_at, id
        LIMIT $3
        FOR UPDATE SKIP LOCKED
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
                 ROW_NUMBER() OVER (ORDER BY eligible_at, id) AS class_rank
          FROM ready_candidates
          UNION ALL
          SELECT id, eligible_at, 'expired' AS class,
                 ROW_NUMBER() OVER (ORDER BY eligible_at, id) AS class_rank
          FROM expired_candidates
        ) AS eligible
        ORDER BY
          CASE WHEN $3 >= 2 AND class_rank = 1 THEN 0 ELSE 1 END,
          CASE WHEN $3 = 1 AND class = $5 THEN 0 ELSE 1 END,
          eligible_at, id
        LIMIT $3
      ),
      updated AS (
        UPDATE #{table} AS runs
        SET claim_token =
              CASE WHEN runs.claim_attempts < $4 THEN gen_random_uuid() ELSE NULL END,
            claimed_at =
              CASE WHEN runs.claim_attempts < $4 THEN $1 ELSE NULL END,
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

    defp observe_outcome(stats, class, eligible_at, ready_candidates, expired_candidates, now) do
      age = max(DateTime.diff(now, eligible_at, :millisecond), 0)

      stats = %{
        stats
        | ready_candidates: ready_candidates,
          expired_candidates: expired_candidates
      }

      case class do
        "ready" ->
          %{
            stats
            | ready_selected: stats.ready_selected + 1,
              ready_oldest_age_ms: max(stats.ready_oldest_age_ms, age)
          }

        "expired" ->
          %{
            stats
            | expired_selected: stats.expired_selected + 1,
              expired_oldest_age_ms: max(stats.expired_oldest_age_ms, age)
          }
      end
    end

    defp observe_steal(stats, "expired"), do: %{stats | steals: stats.steals + 1}
    defp observe_steal(stats, _class), do: stats

    defp tenant_owner_scope(nil), do: :tenantless
    defp tenant_owner_scope(tenant_id) when is_binary(tenant_id), do: {:tenant, tenant_id}

    defp load_uuid!(token) do
      case Ecto.UUID.load(token) do
        {:ok, uuid} -> uuid
        :error -> raise "Postgres returned an invalid claim UUID"
      end
    end

    defp poison_reason("max_claim_attempts_exceeded"), do: :max_claim_attempts
    defp poison_reason("max_claim_abandons_exceeded"), do: :max_claim_abandons
    defp poison_reason(_), do: :other

    defp normalize_database_datetime(%DateTime{} = datetime) do
      datetime
      |> DateTime.to_unix(:microsecond)
      |> DateTime.from_unix!(:microsecond)
    end
  end
end
