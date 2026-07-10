if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunStore do
    @moduledoc """
    Postgres run-aggregate operations for claiming due work.

    A claim is the current authority to commit a run, not evidence that only
    one process exists. An expired holder can overlap a new holder after a
    steal, but only the freshly stored token can refresh, release, or satisfy
    the commit fence.

    Claim acquisition is one short database statement. It locks bounded,
    separately indexed ready and expired candidate sets with `SKIP LOCKED`,
    fences them as materialized CTEs, and applies one combined demand limit.
    Dispatchers call it outside any transaction that spans vehicle execution,
    so no connection is held while node code runs.

    This module implements the claim slice of `Docket.Storage.Runs`.
    Codec/read operations and durable commit operations land in their owning
    Postgres tickets, but share this module and its `current_claim/2` fence.
    """

    import Ecto.Query

    alias Docket.Postgres.Schemas.Run

    @type ctx :: module() | %{required(:repo) => module(), optional(:prefix) => String.t() | nil}

    @doc """
    Atomically claims at most `policy.limit` due runs.

    Ready and expired candidates are selected through separate partial-index
    paths. The oldest eligible rows across both fenced paths consume the
    shared demand budget. A candidate below the attempt limit receives a new
    token; an exhausted candidate is poisoned instead and is never returned
    as a lease.
    """
    @spec claim_due(ctx(), :system, Docket.Storage.Runs.claim_policy()) ::
            {:ok, Docket.Storage.Runs.claim_batch()} | {:error, term()}
    def claim_due(ctx, :system, policy) do
      {repo, prefix} = context!(ctx)

      %{now: now, limit: limit, orphan_ttl_ms: ttl, max_claim_attempts: max} =
        validate_policy!(policy)

      cutoff = DateTime.add(now, -ttl, :millisecond)

      case Ecto.Adapters.SQL.query(repo, claim_statement(prefix), [
             now,
             cutoff,
             limit,
             max,
             Docket.Runtime.Graph.Artifact.compiler_abi()
           ]) do
        {:ok, %{rows: rows}} -> {:ok, decode_claim_batch(rows)}
        {:error, reason} -> {:error, reason}
      end
    end

    def claim_due(_ctx, scope, _policy) do
      raise ArgumentError, "claim_due scope must be :system, got: #{inspect(scope)}"
    end

    @doc """
    Refreshes the claim timestamp when `claim_token` is still current.

    Expiry is deliberately absent from the predicate. A token remains valid
    after its TTL until another claimant actually replaces it.
    """
    @spec refresh_claim(
            ctx(),
            :system,
            String.t(),
            Docket.Storage.Runs.claim_token(),
            DateTime.t()
          ) ::
            :ok | {:error, :claim_lost}
    def refresh_claim(ctx, :system, run_id, claim_token, %DateTime{} = now) do
      {repo, prefix} = context!(ctx)

      {count, _} =
        run_id
        |> current_claim(claim_token)
        |> claim_query(prefix)
        |> repo.update_all(set: [claimed_at: now, updated_at: now])

      if count == 1, do: :ok, else: {:error, :claim_lost}
    end

    def refresh_claim(_ctx, :system, _run_id, _claim_token, now) do
      raise ArgumentError, "refresh_claim now must be a DateTime, got: #{inspect(now)}"
    end

    def refresh_claim(_ctx, scope, _run_id, _claim_token, _now) do
      raise ArgumentError, "refresh_claim scope must be :system, got: #{inspect(scope)}"
    end

    @doc """
    Idempotently releases the exact current token and records an immediate wake.

    A missing or stale token changes nothing. A matching release never changes
    `checkpoint_seq` or `claim_attempts`; it only clears claim authority,
    restores `wake_at`, and advances the row's update timestamp.
    """
    @spec release_claim(
            ctx(),
            :system,
            String.t(),
            Docket.Storage.Runs.claim_token(),
            DateTime.t()
          ) ::
            :ok
    def release_claim(ctx, :system, run_id, claim_token, %DateTime{} = now) do
      {repo, prefix} = context!(ctx)

      run_id
      |> current_claim(claim_token)
      |> claim_query(prefix)
      |> repo.update_all(set: [claim_token: nil, claimed_at: nil, wake_at: now, updated_at: now])

      :ok
    end

    def release_claim(_ctx, :system, _run_id, _claim_token, now) do
      raise ArgumentError, "release_claim now must be a DateTime, got: #{inspect(now)}"
    end

    def release_claim(_ctx, scope, _run_id, _claim_token, _now) do
      raise ArgumentError, "release_claim scope must be :system, got: #{inspect(scope)}"
    end

    @doc false
    @spec current_claim(String.t(), Docket.Storage.Runs.claim_token()) ::
            Ecto.Query.dynamic_expr()
    def current_claim(run_id, claim_token)
        when is_binary(run_id) and byte_size(run_id) > 0 and is_binary(claim_token) and
               byte_size(claim_token) > 0 do
      case Ecto.UUID.cast(claim_token) do
        {:ok, token} -> dynamic([run], run.run_id == ^run_id and run.claim_token == ^token)
        :error -> dynamic([_run], false)
      end
    end

    def current_claim(run_id, claim_token) do
      raise ArgumentError,
            "run id and claim token must be non-empty binaries, got: " <>
              "#{inspect(run_id)}, #{inspect(claim_token)}"
    end

    @doc false
    @spec claim_statement(String.t() | nil) :: String.t()
    def claim_statement(prefix \\ nil) do
      table = qualified_table(prefix)

      """
      WITH ready_candidates AS MATERIALIZED (
        SELECT id, wake_at AS eligible_at
        FROM #{table}
        WHERE status = 'running'
          AND graph_compiler_abi = $5
          AND poisoned_at IS NULL
          AND claim_token IS NULL
          AND wake_at <= $1
        ORDER BY wake_at, id
        LIMIT $3
        FOR UPDATE SKIP LOCKED
      ),
      expired_candidates AS MATERIALIZED (
        SELECT id, claimed_at AS eligible_at
        FROM #{table}
        WHERE status = 'running'
          AND graph_compiler_abi = $5
          AND poisoned_at IS NULL
          AND claim_token IS NOT NULL
          AND claimed_at < $2
        ORDER BY claimed_at, id
        LIMIT $3
        FOR UPDATE SKIP LOCKED
      ),
      candidates AS MATERIALIZED (
        SELECT id
        FROM (
          SELECT id, eligible_at FROM ready_candidates
          UNION ALL
          SELECT id, eligible_at FROM expired_candidates
        ) AS eligible
        ORDER BY eligible_at, id
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
                ELSE jsonb_build_object(
                  'type', 'max_claim_attempts_exceeded',
                  'max_claim_attempts', $4,
                  'claim_attempts', runs.claim_attempts
                )
              END,
            updated_at = $1
        FROM candidates
        WHERE runs.id = candidates.id
          AND runs.status = 'running'
          AND runs.graph_compiler_abi = $5
          AND runs.poisoned_at IS NULL
        RETURNING
          runs.run_id,
          runs.graph_id,
          runs.graph_hash,
          runs.graph_compiler_abi,
          runs.checkpoint_seq,
          runs.claim_token,
          runs.claimed_at,
          runs.claim_attempts,
          runs.poisoned_at,
          runs.poison_reason
      )
      SELECT
        run_id,
        graph_id,
        graph_hash,
        graph_compiler_abi,
        checkpoint_seq,
        claim_token,
        claimed_at,
        claim_attempts,
        poisoned_at,
        poison_reason
      FROM updated
      ORDER BY run_id
      """
    end

    defp claim_query(predicate, prefix) do
      Run
      |> where(^predicate)
      |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)
    end

    defp decode_claim_batch(rows) do
      {leases, poisoned} =
        Enum.reduce(rows, {[], []}, fn
          [
            run_id,
            graph_id,
            graph_hash,
            graph_compiler_abi,
            checkpoint_seq,
            claim_token,
            claimed_at,
            claim_attempt,
            nil,
            nil
          ],
          {leases, poisoned} ->
            lease = %{
              run_id: run_id,
              graph_id: graph_id,
              graph_hash: graph_hash,
              graph_compiler_abi: graph_compiler_abi,
              checkpoint_seq: checkpoint_seq,
              claim_token: load_uuid!(claim_token),
              claimed_at: claimed_at,
              claim_attempt: claim_attempt
            }

            {[lease | leases], poisoned}

          [
            run_id,
            _graph_id,
            _graph_hash,
            _graph_compiler_abi,
            _checkpoint_seq,
            nil,
            nil,
            _claim_attempt,
            %DateTime{} = poisoned_at,
            poison_reason
          ],
          {leases, poisoned} ->
            result = %{
              run_id: run_id,
              poisoned_at: poisoned_at,
              poison_reason: poison_reason
            }

            {leases, [result | poisoned]}
        end)

      %{leases: Enum.reverse(leases), poisoned: Enum.reverse(poisoned)}
    end

    defp load_uuid!(token) do
      case Ecto.UUID.load(token) do
        {:ok, uuid} -> uuid
        :error -> raise "Postgres returned an invalid claim UUID"
      end
    end

    defp validate_policy!(
           %{
             now: %DateTime{} = now,
             limit: limit,
             orphan_ttl_ms: ttl,
             max_claim_attempts: max
           } = policy
         )
         when is_integer(limit) and limit > 0 and is_integer(ttl) and ttl >= 0 and
                is_integer(max) and max > 0 do
      %{policy | now: DateTime.truncate(now, :microsecond)}
    end

    defp validate_policy!(policy) do
      raise ArgumentError,
            "claim policy requires DateTime now, positive limit/max_claim_attempts, " <>
              "and non-negative orphan_ttl_ms, got: #{inspect(policy)}"
    end

    defp context!(repo) when is_atom(repo), do: {repo, nil}

    defp context!(%{repo: repo} = ctx) when is_atom(repo) do
      prefix = Map.get(ctx, :prefix)

      if is_nil(prefix) or is_binary(prefix) do
        {repo, prefix}
      else
        raise ArgumentError, "Postgres context prefix must be a string or nil"
      end
    end

    defp context!(ctx) do
      raise ArgumentError,
            "Postgres context must be a Repo or contain :repo and optional :prefix, got: " <>
              inspect(ctx)
    end

    defp qualified_table(nil), do: ~s("docket_runs")

    defp qualified_table(prefix) do
      quoted_prefix = String.replace(prefix, ~s("), ~s(""))
      ~s("#{quoted_prefix}"."docket_runs")
    end
  end
end
