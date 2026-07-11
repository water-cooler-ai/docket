if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunStore do
    @moduledoc """
    Postgres persistence for the durable run aggregate and its operational
    delivery state.

    A claim is the current authority to commit a run, not evidence that only
    one process exists. An expired holder can overlap a new holder after a
    steal, but only the freshly stored token can refresh, release, or satisfy
    the commit fence.

    Claim acquisition is one short database statement. It locks bounded,
    separately indexed ready and expired candidate sets with `SKIP LOCKED`,
    fences them as materialized CTEs, and applies one combined demand limit.
    Dispatchers call it outside any transaction that spans vehicle execution,
    so no connection is held while node code runs.

    Committed `Docket.Run` fields and backend-owned operational fields share a
    row but have separate clocks. In particular, `updated_at` belongs to the
    last committed run document; claims, heartbeats, releases, and poison
    transitions never rewrite it.

    Claim tokens are redacted from schema inspection, and token-bearing
    refresh/release queries disable Ecto's ordinary SQL query log. Repo
    telemetry remains a trusted instrumentation boundary and may observe bind
    parameters, like any other database telemetry subscriber.
    """

    import Ecto.Query

    alias Docket.Postgres.{RunCodec, Storage}
    alias Docket.Postgres.Schemas.Run

    @type ctx :: module() | %{required(:repo) => module(), optional(:prefix) => String.t() | nil}

    @doc """
    Inserts one initialized running run under its explicit owner scope.

    The graph version must already exist. Initialization is the only insert
    shape: the committed run has a positive checkpoint sequence, start and
    update timestamps, `:run_initialized` checkpoint metadata, and an explicit
    first wake.
    """
    @spec insert_run(
            ctx(),
            Docket.Storage.owner_scope(),
            Docket.Run.t(),
            Docket.Checkpoint.type(),
            DateTime.t()
          ) :: {:ok, Docket.Run.t()} | {:error, term()}
    def insert_run(ctx, owner_scope, run, checkpoint_type, wake_at) do
      {repo, prefix} = Storage.context!(ctx)
      tenant_id = owner_tenant_id!(owner_scope)

      with :ok <- validate_initialized_run(run, checkpoint_type, wake_at),
           {:ok, attrs} <- RunCodec.dump(run) do
        wake_at = normalize_database_datetime(wake_at)

        attrs =
          Map.merge(attrs, %{
            tenant_id: tenant_id,
            latest_checkpoint_type: checkpoint_type,
            claim_token: nil,
            claimed_at: nil,
            wake_at: wake_at,
            claim_attempts: 0,
            poisoned_at: nil,
            poison_reason: nil
          })

        changeset = Run.changeset(attrs)

        if changeset.valid? do
          case repo.insert(changeset, prefix: prefix) do
            {:ok, _row} -> {:ok, run}
            {:error, %Ecto.Changeset{} = changeset} -> insert_error(changeset)
            {:error, reason} -> {:error, reason}
          end
        else
          {:error, :invalid_run}
        end
      else
        _invalid -> {:error, :invalid_run}
      end
    end

    @doc """
    Fetches the last committed run under an explicit SQL-enforced scope.
    """
    @spec fetch_run(ctx(), Docket.Storage.scope(), String.t()) ::
            {:ok, Docket.Run.t()} | {:error, :not_found}
    def fetch_run(ctx, scope, run_id) do
      {repo, prefix} = Storage.context!(ctx)

      with {:ok, row} <- fetch_scoped_row(repo, prefix, scope, run_id) do
        {:ok, RunCodec.load!(row)}
      end
    end

    @doc """
    Fetches the committed run plus token-free backend operational state.
    """
    @spec inspect_run(ctx(), Docket.Storage.scope(), String.t()) ::
            {:ok, Docket.RunInfo.t()} | {:error, :not_found}
    def inspect_run(ctx, scope, run_id) do
      {repo, prefix} = Storage.context!(ctx)

      with {:ok, row} <- fetch_scoped_row(repo, prefix, scope, run_id) do
        info =
          Docket.RunInfo.new!(
            run: RunCodec.load!(row),
            wake_at: row.wake_at,
            claimed_at: row.claimed_at,
            claim_attempts: row.claim_attempts,
            poisoned_at: row.poisoned_at,
            poison_reason: row.poison_reason
          )

        {:ok, info}
      end
    end

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
      {repo, prefix} = Storage.context!(ctx)

      %{now: now, limit: limit, orphan_ttl_ms: ttl, max_claim_attempts: max} =
        validate_policy!(policy)

      cutoff = DateTime.add(now, -ttl, :millisecond)

      case Ecto.Adapters.SQL.query(repo, claim_statement(prefix), [now, cutoff, limit, max]) do
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
      {repo, prefix} = Storage.context!(ctx)
      now = normalize_database_datetime(now)

      {count, _} =
        run_id
        |> current_claim(claim_token)
        |> claim_query(prefix)
        |> repo.update_all([set: [claimed_at: now]], log: false)

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
    `checkpoint_seq`, `claim_attempts`, or the committed run's `updated_at`;
    it only clears claim authority and restores `wake_at`.
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
      {repo, prefix} = Storage.context!(ctx)
      now = normalize_database_datetime(now)

      run_id
      |> current_claim(claim_token)
      |> claim_query(prefix)
      |> repo.update_all([set: [claim_token: nil, claimed_at: nil, wake_at: now]], log: false)

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
              END
        FROM candidates
        WHERE runs.id = candidates.id
          AND runs.status = 'running'
          AND runs.poisoned_at IS NULL
        RETURNING
          runs.run_id,
          runs.graph_id,
          runs.graph_hash,
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

    defp fetch_scoped_row(repo, prefix, scope, run_id) do
      query =
        Run
        |> where([run], run.run_id == ^run_id)
        |> scope_query(scope)
        |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)

      case repo.one(query) do
        nil -> {:error, :not_found}
        %Run{} = row -> {:ok, row}
      end
    end

    defp scope_query(query, :system), do: query
    defp scope_query(query, :tenantless), do: where(query, [run], is_nil(run.tenant_id))

    defp scope_query(query, {:tenant, tenant_id}) when is_binary(tenant_id) do
      where(query, [run], run.tenant_id == ^tenant_id)
    end

    defp scope_query(_query, scope) do
      raise ArgumentError,
            "scope must be :system, :tenantless, or {:tenant, tenant_id}, got: #{inspect(scope)}"
    end

    defp owner_tenant_id!(:tenantless), do: nil
    defp owner_tenant_id!({:tenant, tenant_id}) when is_binary(tenant_id), do: tenant_id

    defp owner_tenant_id!(scope) do
      raise ArgumentError,
            "run owner scope must be :tenantless or {:tenant, tenant_id}, got: " <>
              inspect(scope)
    end

    defp validate_initialized_run(
           %Docket.Run{
             id: run_id,
             graph_id: graph_id,
             graph_hash: graph_hash,
             status: :running,
             output: nil,
             failure: nil,
             checkpoint_seq: checkpoint_seq,
             started_at: %DateTime{},
             updated_at: %DateTime{},
             finished_at: nil
           },
           :run_initialized,
           %DateTime{}
         )
         when is_binary(run_id) and byte_size(run_id) > 0 and is_binary(graph_id) and
                byte_size(graph_id) > 0 and is_binary(graph_hash) and byte_size(graph_hash) > 0 and
                is_integer(checkpoint_seq) and checkpoint_seq >= 1,
         do: :ok

    defp validate_initialized_run(_run, _checkpoint_type, _wake_at), do: {:error, :invalid_run}

    defp insert_error(%Ecto.Changeset{} = changeset) do
      if Keyword.has_key?(changeset.errors, :run_id) do
        {:error, :already_exists}
      else
        {:error, changeset}
      end
    end

    defp decode_claim_batch(rows) do
      {leases, poisoned} =
        Enum.reduce(rows, {[], []}, fn
          [
            run_id,
            graph_id,
            graph_hash,
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
      %{policy | now: normalize_database_datetime(now)}
    end

    defp validate_policy!(policy) do
      raise ArgumentError,
            "claim policy requires DateTime now, positive limit/max_claim_attempts, " <>
              "and non-negative orphan_ttl_ms, got: #{inspect(policy)}"
    end

    defp normalize_database_datetime(%DateTime{} = datetime) do
      datetime
      |> DateTime.to_unix(:microsecond)
      |> DateTime.from_unix!(:microsecond)
    end

    defp qualified_table(nil), do: ~s("docket_runs")

    defp qualified_table(prefix) do
      quoted_prefix = String.replace(prefix, ~s("), ~s(""))
      ~s("#{quoted_prefix}"."docket_runs")
    end
  end
end
