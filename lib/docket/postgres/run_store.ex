if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunStore do
    @moduledoc """
    Postgres persistence for the durable run aggregate and its operational
    delivery state.

    A claim is the current authority to commit a run, not evidence that only
    one process exists. An expired holder can overlap a new holder after a
    steal, but only the freshly stored token can refresh, release, abandon,
    or satisfy the commit fence.

    Claim acquisition dispatches through the ClaimPolicy resolved in the
    backend context. The selected engine builds and decodes one data-only
    admission plan; this module alone executes that plan as one PostgreSQL
    statement. Dispatchers call the entrypoint outside any transaction that
    spans vehicle execution, so no connection is held while node code runs.

    Committed `Docket.Run` fields and backend-owned operational fields share a
    row but have separate clocks. In particular, `updated_at` belongs to the
    last committed run document; claims, refreshes, releases, and poison
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

    @behaviour Docket.Backend.RunStore

    alias Docket.Postgres.{ClaimPolicy, RunCodec, Storage}
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
    @impl true
    @spec insert_run(
            ctx(),
            Docket.Backend.owner_scope(),
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
    @impl true
    @spec fetch_run(ctx(), Docket.Backend.scope(), String.t()) ::
            {:ok, Docket.Run.t()} | {:error, :not_found}
    def fetch_run(ctx, scope, run_id) do
      store_operation(:run_fetch, fn ->
        {repo, prefix} = Storage.context!(ctx)

        with {:ok, row} <- fetch_scoped_row(repo, prefix, scope, run_id) do
          {:ok, RunCodec.load!(row)}
        end
      end)
    end

    @doc """
    Lists lightweight run summaries under an explicit SQL-enforced scope.

    The query selects only summary columns, reads one row beyond the requested
    limit, and uses the immutable `(started_at, run_id)` key for stable
    newest-first pagination.
    """
    @impl Docket.Backend.RunStore
    @spec list_runs(ctx(), Docket.Backend.scope(), Docket.Backend.RunStore.list_query()) ::
            {:ok, Docket.RunPage.t()}
    def list_runs(ctx, scope, query) do
      store_operation(:run_list, fn ->
        query = validate_list_query!(query)
        {repo, prefix} = Storage.context!(ctx)

        rows =
          Run
          |> scope_query(scope)
          |> filter_graph_id(query.graph_id)
          |> filter_graph_hash(query.graph_hash)
          |> filter_statuses(query.statuses)
          |> filter_before(query.before)
          |> order_by([run], desc: run.started_at, desc: run.run_id)
          |> limit(^(query.limit + 1))
          |> select([run], %{
            id: run.run_id,
            tenant_id: run.tenant_id,
            graph_id: run.graph_id,
            graph_hash: run.graph_hash,
            status: run.status,
            step: run.step,
            checkpoint_seq: run.checkpoint_seq,
            started_at: run.started_at,
            updated_at: run.updated_at,
            finished_at: run.finished_at
          })
          |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)
          |> repo.all()

        summaries = Enum.map(rows, &Docket.RunSummary.new!/1)
        {:ok, Docket.RunPage.new(summaries, query.before, query.limit)}
      end)
    end

    @doc """
    Fetches the committed run plus token-free backend operational state.
    """
    @impl true
    @spec inspect_run(ctx(), Docket.Backend.scope(), String.t()) ::
            {:ok, Docket.RunInfo.t()} | {:error, :not_found}
    def inspect_run(ctx, scope, run_id) do
      store_operation(:run_inspect, fn ->
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
      end)
    end

    @doc """
    Executes one admission plan from the ClaimPolicy selected by `ctx`.

    A bare Repo or otherwise unconfigured direct context selects the Legacy
    policy for source compatibility. Configured root and transaction contexts
    carry one already-resolved policy value.
    """
    @impl true
    @spec claim_due(ctx(), :system, Docket.Backend.RunStore.claim_policy()) ::
            {:ok, Docket.Backend.RunStore.claim_batch()} | {:error, term()}
    def claim_due(ctx, :system, policy) do
      started = System.monotonic_time()
      {repo, _prefix} = Storage.context!(ctx)
      claim_policy = ClaimPolicy.resolve(ctx)
      effective_policy = ClaimPolicy.effective_policy!(policy)
      plan = ClaimPolicy.build_plan(claim_policy, ctx, effective_policy)

      {result, decoded_observation} =
        case Ecto.Adapters.SQL.query(repo, plan.statement, plan.params) do
          {:ok, %{rows: rows}} ->
            case ClaimPolicy.decode(claim_policy, plan, rows) do
              {:ok, batch, observation} -> {{:ok, batch}, observation}
              {:error, reason} -> {{:error, reason}, nil}
            end

          {:error, reason} ->
            {{:error, reason}, nil}
        end

      :ok = ClaimPolicy.observe(claim_policy, plan, decoded_observation, result, started)
      result
    end

    def claim_due(_ctx, scope, _policy) do
      raise ArgumentError, "claim_due scope must be :system, got: #{inspect(scope)}"
    end

    @doc """
    Refreshes the claim timestamp when `claim_token` is still current.

    Expiry is deliberately absent from the predicate. A token remains valid
    after its TTL until another claimant actually replaces it.

    The write uses the database clock and never regresses:
    `GREATEST(claimed_at, CURRENT_TIMESTAMP)`. A refresh that was delayed in
    the pool behind a `:retain_claim` commit therefore cannot move
    `claimed_at` backward past the commit's fresher stamp and re-expose the
    run to steal. The caller's `now` is validated but not written.
    """
    @impl true
    @spec refresh_claim(
            ctx(),
            :system,
            String.t(),
            Docket.Backend.RunStore.claim_token(),
            DateTime.t()
          ) ::
            :ok | {:error, :claim_lost}
    def refresh_claim(ctx, :system, run_id, claim_token, %DateTime{}) do
      started = System.monotonic_time()
      {repo, prefix} = Storage.context!(ctx)

      {count, _} =
        run_id
        |> current_claim(claim_token)
        |> claim_query(prefix)
        |> repo.update_all(
          [
            set: [
              claimed_at:
                dynamic([run], fragment("GREATEST(?, CURRENT_TIMESTAMP)", run.claimed_at))
            ]
          ],
          log: false
        )

      result = if count == 1, do: :ok, else: {:error, :claim_lost}
      emit_claim_operation(:refresh, started, result)
      result
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
    @impl true
    @spec release_claim(
            ctx(),
            :system,
            String.t(),
            Docket.Backend.RunStore.claim_token(),
            DateTime.t()
          ) ::
            :ok
    def release_claim(ctx, :system, run_id, claim_token, %DateTime{} = now) do
      started = System.monotonic_time()
      {repo, prefix} = Storage.context!(ctx)
      now = normalize_database_datetime(now)

      {matched, _} =
        run_id
        |> current_claim(claim_token)
        |> claim_query(prefix)
        |> repo.update_all([set: [claim_token: nil, claimed_at: nil, wake_at: now]], log: false)

      emit_claim_operation(
        :release,
        started,
        if(matched == 1, do: :ok, else: {:error, :claim_lost}),
        %{matched: matched}
      )

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

    A `:non_poisoning` policy never poisons; with `:backoff` the recorded
    wake grows exponentially with the durable abandon count up to the cap.
    """
    @impl true
    @spec abandon_claim(
            ctx(),
            :system,
            String.t(),
            Docket.Backend.RunStore.claim_token(),
            Docket.Backend.RunStore.abandon_policy()
          ) :: Docket.Backend.RunStore.abandon_result()
    def abandon_claim(ctx, :system, run_id, claim_token, policy) do
      started = System.monotonic_time()

      %{expected_checkpoint_seq: seq, now: now, retry_at: retry_at, max_claim_abandons: max} =
        policy = validate_abandon_policy!(policy)

      if Map.get(policy, :non_poisoning, false) do
        non_poisoning_abandon(ctx, run_id, claim_token, seq, retry_at, policy, started)
      else
        poisoning_abandon(ctx, run_id, claim_token, seq, now, retry_at, max, started)
      end
    end

    def abandon_claim(_ctx, scope, _run_id, _claim_token, _policy) do
      raise ArgumentError, "abandon_claim scope must be :system, got: #{inspect(scope)}"
    end

    defp non_poisoning_abandon(ctx, run_id, claim_token, seq, retry_at, policy, started) do
      {repo, prefix} = Storage.context!(ctx)

      {matched, _} =
        run_id
        |> current_claim(claim_token)
        |> where([run], run.checkpoint_seq == ^seq)
        |> claim_query(prefix)
        |> repo.update_all(
          [
            set: [
              claim_token: nil,
              claimed_at: nil,
              claim_attempts: dynamic([run], fragment("GREATEST(? - 1, 0)", run.claim_attempts)),
              claim_abandons: dynamic([run], run.claim_abandons + 1),
              wake_at: non_poisoning_wake(policy, retry_at)
            ]
          ],
          log: false
        )

      result = if matched == 1, do: {:ok, :rescheduled}, else: {:ok, :stale}
      emit_claim_operation(:abandon, started, result, %{reason: :host_incompatible})
      result
    end

    # The exponent is the durable abandon count, so consecutive handbacks
    # push the wake back geometrically until the cap; committed progress
    # resets the count. The exponent is clamped so the double-precision
    # product stays exact well past any practical cap.
    defp non_poisoning_wake(%{backoff: %{base_ms: base, cap_ms: cap}, now: now}, _retry_at) do
      dynamic(
        [run],
        fragment(
          "? + (LEAST(? * POWER(2, LEAST(?, 30)), ?) * interval '1 millisecond')",
          type(^now, :utc_datetime_usec),
          ^base,
          run.claim_abandons,
          ^cap
        )
      )
    end

    defp non_poisoning_wake(_policy, retry_at), do: retry_at

    defp poisoning_abandon(ctx, run_id, claim_token, seq, now, retry_at, max, started) do
      {repo, prefix} = Storage.context!(ctx)

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

      result =
        case repo.update_all(query, updates, log: false) do
          {0, _} -> {:ok, :stale}
          {1, [nil]} -> {:ok, :rescheduled}
          {1, [%DateTime{}]} -> {:ok, :poisoned}
        end

      emit_claim_operation(:abandon, started, result, %{})

      if result == {:ok, :poisoned} do
        :telemetry.execute(
          [:docket, :postgres, :claim, :poisoned],
          %{count: 1},
          %{reason: :max_claim_abandons}
        )
      end

      result
    end

    @doc "Commits an exact-next run document under the current claim fence."
    @impl true
    @spec commit(ctx(), Docket.Backend.scope(), Docket.Backend.RunStore.commit_proposal()) ::
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
    @impl true
    @spec mutate_run(
            ctx(),
            Docket.Backend.scope(),
            String.t(),
            Docket.Backend.RunStore.mutation()
          ) ::
            Docket.Backend.RunStore.mutation_result()
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
    @impl true
    @spec retry_poisoned_run(
            ctx(),
            Docket.Backend.scope(),
            String.t(),
            DateTime.t()
          ) :: {:ok, Docket.Run.t()} | {:error, :not_found | :inactive_run}
    def retry_poisoned_run(ctx, scope, run_id, %DateTime{} = now) do
      started = System.monotonic_time()
      {repo, prefix} = Storage.context!(ctx)
      validate_scope!(scope)

      result =
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

                     case repo.update_all(scoped_row_query(row, scope, prefix), updates,
                            log: false
                          ) do
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

      emit_claim_operation(:poison_recovery, started, result, %{})
      result
    end

    def retry_poisoned_run(_ctx, _scope, _run_id, now) do
      raise ArgumentError, "retry_poisoned_run now must be a DateTime, got: #{inspect(now)}"
    end

    @doc false
    @spec current_claim(String.t(), Docket.Backend.RunStore.claim_token()) ::
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

    defp filter_graph_id(query, nil), do: query
    defp filter_graph_id(query, graph_id), do: where(query, [run], run.graph_id == ^graph_id)

    defp filter_graph_hash(query, nil), do: query

    defp filter_graph_hash(query, graph_hash),
      do: where(query, [run], run.graph_hash == ^graph_hash)

    defp filter_statuses(query, nil), do: query
    defp filter_statuses(query, statuses), do: where(query, [run], run.status in ^statuses)

    defp filter_before(query, nil), do: query

    defp filter_before(query, {%DateTime{} = started_at, run_id}) do
      where(
        query,
        [run],
        run.started_at < ^started_at or
          (run.started_at == ^started_at and run.run_id < ^run_id)
      )
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
        proposed.started_at != stored.started_at -> {:error, :invalid_mutation}
        proposed.checkpoint_seq != stored.checkpoint_seq + 1 -> {:error, :invalid_mutation}
        checkpoint_type not in Docket.Checkpoint.types() -> {:error, :invalid_mutation}
        schedule == :retain_claim -> {:error, :invalid_mutation}
        not valid_schedule?(schedule) -> {:error, :invalid_mutation}
        not schedule_matches_status?(schedule, proposed.status) -> {:error, :invalid_mutation}
        true -> Docket.Run.validate_failure(proposed)
      end
    end

    defp validate_immutable_binding(stored, proposed) do
      if stored.graph_id == proposed.graph_id and stored.graph_hash == proposed.graph_hash and
           stored.started_at == proposed.started_at,
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

    defp validate_list_query!(
           %{
             limit: limit,
             before: before,
             graph_id: graph_id,
             graph_hash: graph_hash,
             statuses: statuses
           } = query
         )
         when is_integer(limit) and limit > 0 do
      validate_list_cursor!(before)
      validate_optional_filter!(graph_id, :graph_id)
      validate_optional_filter!(graph_hash, :graph_hash)

      unless is_nil(statuses) or
               (is_list(statuses) and statuses != [] and
                  Enum.all?(statuses, &Docket.Run.durable_status?/1)) do
        raise ArgumentError,
              "run list statuses must be nil or a non-empty list of durable statuses, got: " <>
                inspect(statuses)
      end

      query
    end

    defp validate_list_query!(query) do
      raise ArgumentError,
            "run list query requires positive limit and normalized before, graph_id, " <>
              "graph_hash, and statuses fields, got: #{inspect(query)}"
    end

    defp validate_list_cursor!(nil), do: :ok

    defp validate_list_cursor!({%DateTime{}, run_id})
         when is_binary(run_id) and byte_size(run_id) > 0,
         do: :ok

    defp validate_list_cursor!(before) do
      raise ArgumentError,
            "run list before cursor must be nil or {DateTime, non-empty run_id}, got: " <>
              inspect(before)
    end

    defp validate_optional_filter!(nil, _name), do: :ok

    defp validate_optional_filter!(value, _name)
         when is_binary(value) and byte_size(value) > 0,
         do: :ok

    defp validate_optional_filter!(value, name) do
      raise ArgumentError,
            "run list #{name} must be nil or a non-empty binary, got: #{inspect(value)}"
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

    defp owner_tenant_id!({:tenant, tenant_id})
         when is_binary(tenant_id) and byte_size(tenant_id) > 0,
         do: tenant_id

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
      cond do
        Keyword.has_key?(changeset.errors, :run_id) ->
          {:error, :already_exists}

        Enum.any?(changeset.errors, fn
          {:graph_hash, {_message, metadata}} ->
            metadata[:constraint] == :foreign and
                metadata[:constraint_name] == "docket_runs_graph_scope_fkey"

          _other ->
            false
        end) ->
          {:error, :not_found}

        true ->
          {:error, changeset}
      end
    end

    defp emit_claim_operation(operation, started, result, measurements \\ %{}) do
      :telemetry.execute(
        [:docket, :postgres, :claim, :operation],
        Map.put(measurements, :duration, System.monotonic_time() - started),
        %{operation: operation, result: claim_operation_result(result)}
      )
    end

    defp store_operation(operation, fun) do
      started = System.monotonic_time()
      result = fun.()

      :telemetry.execute(
        [:docket, :postgres, :store],
        %{
          duration: System.monotonic_time() - started,
          selected_rows: selected_rows(result)
        },
        Map.merge(Docket.Telemetry.correlation_metadata(), %{
          operation: operation,
          result: Docket.Telemetry.result_kind(result)
        })
      )

      result
    end

    defp selected_rows({:ok, %Docket.RunPage{runs: runs}}), do: length(runs)
    defp selected_rows({:ok, _result}), do: 1
    defp selected_rows(_result), do: 0

    defp claim_operation_result(:ok), do: :ok

    defp claim_operation_result({:ok, disposition})
         when disposition in [:stale, :rescheduled, :poisoned],
         do: disposition

    defp claim_operation_result({:error, :claim_lost}), do: :claim_lost
    defp claim_operation_result({:error, :inactive_run}), do: :inactive
    defp claim_operation_result({:error, _}), do: :error
    defp claim_operation_result(_), do: :ok

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

      validate_abandon_backoff!(policy)

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

    defp validate_abandon_backoff!(policy) do
      case Map.get(policy, :backoff) do
        nil ->
          :ok

        %{base_ms: base, cap_ms: cap}
        when is_integer(base) and base > 0 and is_integer(cap) and cap >= base ->
          :ok

        other ->
          raise ArgumentError,
                "abandon policy backoff requires positive base_ms and cap_ms >= base_ms, " <>
                  "got: #{inspect(other)}"
      end
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
  end
end
