if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunStore do
    @moduledoc """
    Postgres persistence for the durable run aggregate and its operational
    delivery state.

    A claim is the current authority to commit a run, not evidence that only
    one process exists. An expired holder can overlap a new holder after a
    steal, but only the freshly stored token can refresh, release, abandon,
    or satisfy the commit fence.

    Claim acquisition is one short database statement. It locks bounded,
    separately indexed ready and expired candidate sets with `SKIP LOCKED`,
    fences them as materialized CTEs, and applies one combined demand limit.
    Dispatchers call it outside any transaction that spans vehicle execution,
    so no connection is held while node code runs.

    Candidate selection keeps both continuously eligible classes making
    progress. With demand of at least two and both classes non-empty, at
    least one outcome (lease or poison) goes to each class before the
    remaining demand falls back to the oldest eligible rows under the stable
    `(eligible_at, id)` tie-break. With demand of exactly one, the policy's
    optional `:preference` names the class served first, falling through to
    the other class when the preferred one is empty; without a preference
    the oldest eligible row wins regardless of class. This is bounded aging,
    not fairness: no strict FIFO, no bounded queue wait, no tenant or
    workload-class fairness, and no starvation freedom under sustained
    arrivals or persistent row locks.

    Every claim scan emits `[:docket, :postgres, :run_store, :claim]` with
    per-class candidate counts (bounded by demand - the scan never counts
    eligible rows beyond its own limited candidate sets), selected counts,
    poison count, demand, oldest selected eligibility ages, and the
    preference it served. Run, task, token, tenant, and graph identities
    never appear in it.

    Committed `Docket.Run` fields and backend-owned operational fields share a
    row but have separate clocks. In particular, `updated_at` belongs to the
    last committed run document; claims, heartbeats, releases, and poison
    transitions never rewrite it.

    Claim tokens are redacted from schema inspection, and token-bearing
    refresh/release queries disable Ecto's ordinary SQL query log. Repo
    telemetry remains a trusted instrumentation boundary and may observe bind
    parameters, like any other database telemetry subscriber.

    Run insertion, moment commit, signal mutation, and poison recovery
    announce a wake due at or before the database clock with `pg_notify` on
    the `docket_wake` channel, carrying the context prefix (empty string when
    unprefixed) as payload. The notification runs on the write's connection
    and joins any transaction the write executes in, so PostgreSQL exposes it
    only after that transaction commits and drops it on rollback. Claim
    release and abandonment record their wakes without a notification, so the
    dispatcher's poll interval alone bounds their redispatch latency.
    """

    import Ecto.Query

    alias Docket.Postgres.{RunCodec, Storage}
    alias Docket.Postgres.Schemas.Run

    @type ctx :: module() | %{required(:repo) => module(), optional(:prefix) => String.t() | nil}

    @wake_channel "docket_wake"

    @doc false
    @spec wake_channel() :: String.t()
    def wake_channel, do: @wake_channel

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
            claim_abandons: 0,
            poisoned_at: nil,
            poison_reason: nil
          })

        changeset = Run.changeset(attrs)

        if changeset.valid? do
          case repo.insert(changeset, prefix: prefix) do
            {:ok, _row} ->
              notify_due_wake(repo, prefix, wake_at)
              {:ok, run}

            {:error, %Ecto.Changeset{} = changeset} ->
              insert_error(changeset)

            {:error, reason} ->
              {:error, reason}
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
            claim_abandons: row.claim_abandons,
            poisoned_at: row.poisoned_at,
            poison_reason: row.poison_reason
          )

        {:ok, info}
      end
    end

    @doc """
    Atomically claims at most `policy.limit` due runs.

    Ready and expired candidates are selected through separate partial-index
    paths and consume one shared demand budget under the class-progress
    policy described in the module documentation: at demand two or more each
    non-empty class receives at least one outcome before the oldest eligible
    rows take the remainder; at demand one the optional `policy.preference`
    class is served first with fallthrough. A candidate below the attempt
    limit receives a new token; an exhausted candidate is poisoned instead
    and is never returned as a lease.
    """
    @spec claim_due(ctx(), :system, Docket.Storage.Runs.claim_policy()) ::
            {:ok, Docket.Storage.Runs.claim_batch()} | {:error, term()}
    def claim_due(ctx, :system, policy) do
      {repo, prefix} = Storage.context!(ctx)

      %{now: now, limit: limit, orphan_ttl_ms: ttl, max_claim_attempts: max} =
        policy = validate_policy!(policy)

      preference = Map.get(policy, :preference)
      cutoff = DateTime.add(now, -ttl, :millisecond)
      params = [now, cutoff, limit, max, preference && Atom.to_string(preference)]

      case Ecto.Adapters.SQL.query(repo, claim_statement(prefix), params) do
        {:ok, %{rows: rows}} ->
          {batch, stats} = decode_claim_batch(rows, now)
          emit_claim_telemetry({batch, stats}, limit, preference)
          {:ok, batch}

        {:error, reason} ->
          {:error, reason}
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

    @doc """
    Hands back a pre-execution claim under a token-and-sequence fence.

    One conditional UPDATE reverses the acquisition's attempt increment,
    counts the abandon, and either records the policy's retry wake or — once
    the abandon count reached the policy maximum — poisons the run with
    reason `"max_claim_abandons_exceeded"`. A stale token or an advanced
    checkpoint sequence matches nothing and reports `{:ok, :stale}`.
    """
    @spec abandon_claim(
            ctx(),
            :system,
            String.t(),
            Docket.Storage.Runs.claim_token(),
            Docket.Storage.Runs.abandon_policy()
          ) :: Docket.Storage.Runs.abandon_result()
    def abandon_claim(ctx, :system, run_id, claim_token, policy) do
      {repo, prefix} = Storage.context!(ctx)

      %{expected_checkpoint_seq: seq, now: now, retry_at: retry_at, max_claim_abandons: max} =
        validate_abandon_policy!(policy)

      predicate =
        dynamic(
          [run],
          ^current_claim(run_id, claim_token) and run.checkpoint_seq == ^seq
        )

      updates = [
        set: [
          claim_token: nil,
          claimed_at: nil,
          claim_attempts: dynamic([run], fragment("GREATEST(? - 1, 0)", run.claim_attempts)),
          claim_abandons:
            dynamic(
              [run],
              fragment(
                "CASE WHEN ? < ? THEN ? + 1 ELSE ? END",
                run.claim_abandons,
                ^max,
                run.claim_abandons,
                run.claim_abandons
              )
            ),
          wake_at:
            dynamic(
              [run],
              fragment(
                "CASE WHEN ? < ? THEN ? ELSE NULL END",
                run.claim_abandons,
                ^max,
                type(^retry_at, :utc_datetime_usec)
              )
            ),
          poisoned_at:
            dynamic(
              [run],
              fragment(
                "CASE WHEN ? < ? THEN NULL ELSE ? END",
                run.claim_abandons,
                ^max,
                type(^now, :utc_datetime_usec)
              )
            ),
          poison_reason:
            dynamic(
              [run],
              fragment(
                "CASE WHEN ? < ? THEN NULL ELSE 'max_claim_abandons_exceeded' END",
                run.claim_abandons,
                ^max
              )
            )
        ]
      ]

      query =
        predicate
        |> claim_query(prefix)
        |> select([run], run.poisoned_at)

      case repo.update_all(query, updates, log: false) do
        {0, _} -> {:ok, :stale}
        {1, [nil]} -> {:ok, :rescheduled}
        {1, [%DateTime{}]} -> {:ok, :poisoned}
      end
    end

    def abandon_claim(_ctx, scope, _run_id, _claim_token, _policy) do
      raise ArgumentError, "abandon_claim scope must be :system, got: #{inspect(scope)}"
    end

    @doc "Commits an exact-next run document under the current claim fence."
    @spec commit(ctx(), Docket.Storage.scope(), Docket.Storage.Runs.commit_proposal()) ::
            {:ok, Docket.Run.t()} | {:error, :stale_fence | :invalid_commit | :not_found}
    def commit(ctx, scope, proposal) do
      {repo, prefix} = Storage.context!(ctx)

      with :ok <- validate_commit(proposal),
           {:ok, attrs} <- RunCodec.dump(proposal.run),
           {:ok, stored} <- fetch_scoped_row(repo, prefix, scope, proposal.run.id),
           :ok <- validate_immutable_binding(stored, proposal.run) do
        query =
          Run
          |> where([run], run.run_id == ^proposal.run.id)
          |> scope_query(scope)
          |> where(
            [run],
            run.checkpoint_seq == ^proposal.expected_checkpoint_seq and
              run.claim_token == ^proposal.claim_token
          )
          |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)

        updates = commit_updates(attrs, proposal.checkpoint_type, proposal.schedule)

        case repo.update_all(query, updates, log: false) do
          {1, _} ->
            notify_wake(repo, prefix, proposal.schedule)
            {:ok, proposal.run}

          {0, _} ->
            commit_miss(repo, prefix, scope, proposal.run.id)
        end
      else
        {:error, :not_found} -> {:error, :not_found}
        _ -> {:error, :invalid_commit}
      end
    end

    @doc "Serializes and applies one pure run mutation without requiring a claim fence."
    @spec mutate_run(ctx(), Docket.Storage.scope(), String.t(), Docket.Storage.Runs.mutation()) ::
            Docket.Storage.Runs.mutation_result()
    def mutate_run(ctx, scope, run_id, mutation) when is_function(mutation, 1) do
      {repo, prefix} = Storage.context!(ctx)
      validate_scope!(scope)

      case repo.transaction(fn ->
             with {:ok, row} <- fetch_locked_scoped_row(repo, prefix, scope, run_id) do
               run = RunCodec.load!(row)

               # Evaluate this pure, bounded decision while holding the row lock.
               # Moving it before the lock requires callback re-entry on a lost
               # compare-and-swap and breaks serialized signals.
               apply_mutation(repo, prefix, scope, row, run, mutation.(run))
             end
           end) do
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
    end

    @doc "Clears poison from a non-terminal run and records an immediate wake."
    @spec retry_poisoned_run(
            ctx(),
            Docket.Storage.scope(),
            String.t(),
            DateTime.t()
          ) :: {:ok, Docket.Run.t()} | {:error, :not_found | :inactive_run}
    def retry_poisoned_run(ctx, scope, run_id, %DateTime{} = now) do
      {repo, prefix} = Storage.context!(ctx)
      validate_scope!(scope)

      case repo.transaction(fn ->
             with {:ok, row} <- fetch_locked_scoped_row(repo, prefix, scope, run_id) do
               run = RunCodec.load!(row)

               cond do
                 Docket.Run.terminal?(run) ->
                   {:error, :inactive_run}

                 is_nil(row.poisoned_at) ->
                   {:ok, run}

                 true ->
                   updates = [
                     set: [
                       claim_token: nil,
                       claimed_at: nil,
                       wake_at: normalize_database_datetime(now),
                       claim_attempts: 0,
                       claim_abandons: 0,
                       poisoned_at: nil,
                       poison_reason: nil
                     ]
                   ]

                   case repo.update_all(scoped_row_query(row, scope, prefix), updates, log: false) do
                     {1, _} ->
                       notify_wake(repo, prefix)
                       {:ok, run}

                     {0, _} ->
                       {:error, :not_found}
                   end
               end
             end
           end) do
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
    end

    def retry_poisoned_run(_ctx, _scope, _run_id, now) do
      raise ArgumentError, "retry_poisoned_run now must be a DateTime, got: #{inspect(now)}"
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

    defp validate_commit(%{
           run: %Docket.Run{} = run,
           expected_checkpoint_seq: expected,
           claim_token: token,
           checkpoint_type: checkpoint_type,
           schedule: schedule
         })
         when is_integer(expected) and expected >= 0 and is_binary(token) and byte_size(token) > 0 and
                is_atom(checkpoint_type) do
      with {:ok, _token} <- Ecto.UUID.cast(token),
           true <- checkpoint_type in Docket.Checkpoint.types(),
           true <- run.checkpoint_seq == expected + 1,
           true <- valid_schedule?(schedule),
           true <- schedule_matches_status?(schedule, run.status),
           :ok <- Docket.Run.validate_failure(run) do
        :ok
      else
        _ -> {:error, :invalid_commit}
      end
    end

    defp validate_commit(_proposal), do: {:error, :invalid_commit}

    defp apply_mutation(
           repo,
           prefix,
           scope,
           row,
           stored_run,
           {:commit, proposed_run, checkpoint_type, schedule, opaque}
         ) do
      with :ok <- validate_mutation(stored_run, proposed_run, checkpoint_type, schedule),
           {:ok, attrs} <- RunCodec.dump(proposed_run) do
        case repo.update_all(
               scoped_row_query(row, scope, prefix),
               commit_updates(attrs, checkpoint_type, schedule),
               log: false
             ) do
          {1, _} ->
            notify_wake(repo, prefix, schedule)
            {:ok, {:committed, opaque}}

          {0, _} ->
            {:error, :stale_fence}
        end
      else
        _ -> {:error, :invalid_mutation}
      end
    end

    defp apply_mutation(_repo, _prefix, _scope, _row, _run, {:no_change, opaque}),
      do: {:ok, {:unchanged, opaque}}

    defp apply_mutation(_repo, _prefix, _scope, _row, _run, {:error, reason}),
      do: {:error, reason}

    defp apply_mutation(_repo, _prefix, _scope, _row, _run, _decision),
      do: {:error, :invalid_mutation}

    defp validate_mutation(stored, proposed, checkpoint_type, schedule) do
      cond do
        not is_struct(proposed, Docket.Run) -> {:error, :invalid_mutation}
        proposed.id != stored.id -> {:error, :invalid_mutation}
        proposed.graph_id != stored.graph_id -> {:error, :invalid_mutation}
        proposed.graph_hash != stored.graph_hash -> {:error, :invalid_mutation}
        proposed.checkpoint_seq != stored.checkpoint_seq + 1 -> {:error, :invalid_mutation}
        checkpoint_type not in Docket.Checkpoint.types() -> {:error, :invalid_mutation}
        schedule == :retain_claim -> {:error, :invalid_mutation}
        not valid_schedule?(schedule) -> {:error, :invalid_mutation}
        not schedule_matches_status?(schedule, proposed.status) -> {:error, :invalid_mutation}
        true -> Docket.Run.validate_failure(proposed)
      end
    end

    defp validate_immutable_binding(stored, proposed) do
      if stored.graph_id == proposed.graph_id and stored.graph_hash == proposed.graph_hash,
        do: :ok,
        else: {:error, :invalid_commit}
    end

    defp valid_schedule?(:retain_claim), do: true

    defp valid_schedule?({:release_claim, reason})
         when reason in [:immediate, :external, :terminal],
         do: true

    defp valid_schedule?({:release_claim, {:at, %DateTime{}}}), do: true
    defp valid_schedule?(_), do: false

    defp schedule_matches_status?(:retain_claim, :running), do: true
    defp schedule_matches_status?({:release_claim, :immediate}, :running), do: true
    defp schedule_matches_status?({:release_claim, {:at, %DateTime{}}}, :running), do: true
    defp schedule_matches_status?({:release_claim, :external}, :waiting), do: true

    defp schedule_matches_status?({:release_claim, :terminal}, status),
      do: status in [:done, :failed, :cancelled]

    defp schedule_matches_status?(_, _), do: false

    defp commit_updates(attrs, checkpoint_type, schedule) do
      base = [
        graph_id: attrs.graph_id,
        graph_hash: attrs.graph_hash,
        status: attrs.status,
        step: attrs.step,
        state: attrs.state,
        checkpoint_seq: attrs.checkpoint_seq,
        latest_checkpoint_type: checkpoint_type,
        claim_attempts: 0,
        claim_abandons: 0,
        poisoned_at: nil,
        poison_reason: nil,
        started_at: attrs.started_at,
        updated_at: attrs.updated_at,
        finished_at: attrs.finished_at
      ]

      schedule_updates =
        case schedule do
          :retain_claim ->
            [wake_at: nil, claimed_at: dynamic([_run], fragment("CURRENT_TIMESTAMP"))]

          {:release_claim, :immediate} ->
            [
              claim_token: nil,
              claimed_at: nil,
              wake_at: dynamic([_run], fragment("CURRENT_TIMESTAMP"))
            ]

          {:release_claim, {:at, at}} ->
            [claim_token: nil, claimed_at: nil, wake_at: normalize_database_datetime(at)]

          {:release_claim, reason} when reason in [:external, :terminal] ->
            [claim_token: nil, claimed_at: nil, wake_at: nil]
        end

      [set: base ++ schedule_updates]
    end

    defp commit_miss(repo, prefix, scope, run_id) do
      case fetch_scoped_row(repo, prefix, scope, run_id) do
        {:ok, _row} -> {:error, :stale_fence}
        {:error, :not_found} -> {:error, :not_found}
      end
    end

    defp fetch_locked_scoped_row(repo, prefix, scope, run_id) do
      query =
        Run
        |> where([run], run.run_id == ^run_id)
        |> scope_query(scope)
        |> lock("FOR UPDATE")
        |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)

      case repo.one(query) do
        nil -> {:error, :not_found}
        row -> {:ok, row}
      end
    end

    defp scoped_row_query(row, scope, prefix) do
      Run
      |> where([run], run.id == ^row.id)
      |> scope_query(scope)
      |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)
    end

    defp validate_scope!(:system), do: :ok
    defp validate_scope!(:tenantless), do: :ok
    defp validate_scope!({:tenant, tenant_id}) when is_binary(tenant_id), do: :ok

    defp validate_scope!(scope) do
      raise ArgumentError,
            "scope must be :system, :tenantless, or {:tenant, tenant_id}, got: #{inspect(scope)}"
    end

    defp scope_query(query, :system), do: query
    defp scope_query(query, :tenantless), do: where(query, [run], is_nil(run.tenant_id))

    defp scope_query(query, {:tenant, tenant_id}) when is_binary(tenant_id) do
      where(query, [run], run.tenant_id == ^tenant_id)
    end

    defp scope_query(_query, scope) do
      validate_scope!(scope)
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

    @empty_claim_stats %{
      ready_candidates: 0,
      expired_candidates: 0,
      ready_selected: 0,
      expired_selected: 0,
      ready_oldest_age_ms: 0,
      expired_oldest_age_ms: 0
    }

    defp decode_claim_batch(rows, now) do
      {{leases, poisoned}, stats} =
        Enum.reduce(rows, {{[], []}, @empty_claim_stats}, fn
          [
            run_id,
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
              graph_id: graph_id,
              graph_hash: graph_hash,
              checkpoint_seq: checkpoint_seq,
              claim_token: load_uuid!(claim_token),
              claimed_at: claimed_at,
              claim_attempt: claim_attempt
            }

            {{[lease | leases], poisoned},
             observe_outcome(stats, class, eligible_at, ready_candidates, expired_candidates, now)}

          [
            run_id,
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

      {%{leases: Enum.reverse(leases), poisoned: Enum.reverse(poisoned)}, stats}
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

    defp emit_claim_telemetry({batch, stats}, demand, preference) do
      fallback? =
        demand == 1 and preference != nil and
          ((preference == :ready and stats.expired_selected > 0) or
             (preference == :expired and stats.ready_selected > 0))

      :telemetry.execute(
        [:docket, :postgres, :run_store, :claim],
        Map.merge(stats, %{
          demand: demand,
          leases: length(batch.leases),
          poisoned: length(batch.poisoned)
        }),
        %{preference: preference, fallback: fallback?}
      )
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
      case Map.get(policy, :preference) do
        preference when preference in [nil, :ready, :expired] ->
          %{policy | now: normalize_database_datetime(now)}

        other ->
          raise ArgumentError,
                "claim policy preference must be :ready or :expired, got: #{inspect(other)}"
      end
    end

    defp validate_policy!(policy) do
      raise ArgumentError,
            "claim policy requires DateTime now, positive limit/max_claim_attempts, " <>
              "non-negative orphan_ttl_ms, and optional preference of :ready or :expired, " <>
              "got: #{inspect(policy)}"
    end

    defp validate_abandon_policy!(
           %{
             expected_checkpoint_seq: seq,
             now: %DateTime{} = now,
             retry_at: %DateTime{} = retry_at,
             max_claim_abandons: max
           } = policy
         )
         when is_integer(seq) and seq >= 0 and is_integer(max) and max > 0 do
      if DateTime.compare(retry_at, now) == :lt do
        raise ArgumentError,
              "abandon policy retry_at must not precede now, got: #{inspect(policy)}"
      end

      %{
        policy
        | now: normalize_database_datetime(now),
          retry_at: normalize_database_datetime(retry_at)
      }
    end

    defp validate_abandon_policy!(policy) do
      raise ArgumentError,
            "abandon policy requires non-negative expected_checkpoint_seq, DateTime now " <>
              "and retry_at, and positive max_claim_abandons, got: #{inspect(policy)}"
    end

    defp notify_wake(repo, prefix, {:release_claim, :immediate}), do: notify_wake(repo, prefix)

    defp notify_wake(repo, prefix, {:release_claim, {:at, %DateTime{} = at}}),
      do: notify_due_wake(repo, prefix, at)

    defp notify_wake(_repo, _prefix, _schedule), do: :ok

    defp notify_wake(repo, prefix) do
      _ =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT pg_notify($1, $2)",
          [@wake_channel, prefix || ""],
          log: false
        )

      :ok
    end

    defp notify_due_wake(repo, prefix, %DateTime{} = wake_at) do
      _ =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT pg_notify($1, $2) WHERE $3::timestamptz <= clock_timestamp()",
          [@wake_channel, prefix || "", normalize_database_datetime(wake_at)],
          log: false
        )

      :ok
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
