if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.MomentStore do
    @moduledoc """
    PostgreSQL-native one-statement commitment for a claimed runtime moment.

    The checkpoint fence, immutable run binding, scoped run update, assigned
    event insert, detached claim-index sync, and optional wake notification
    execute as one PostgreSQL statement. PostgreSQL statement atomicity makes
    the run update, event insert, and index sync indivisible without an
    explicit transaction.

    A zero-row update pays one diagnostic read to preserve the public
    `:not_found` / `:invalid_commit` / `:stale_fence` distinction. The
    successful hot path performs exactly one database exchange.
    """

    alias Docket.Postgres.{EventStore, RunStore, Storage}

    @wake_channel "docket_wake"

    @doc false
    @spec commit(
            Docket.Backend.ctx(),
            Docket.Backend.scope(),
            Docket.Backend.TransitionStore.claimed_proposal(),
            [Docket.Event.t()]
          ) :: {:ok, Docket.Run.t()} | {:error, term()}
    def commit(ctx, scope, %{run: %Docket.Run{} = run} = proposal, events) do
      started = System.monotonic_time()
      {repo, prefix} = Storage.context!(ctx)

      result =
        with {:ok, attrs} <- RunStore.prepare_commit(proposal),
             {:ok, event_attrs} <- EventStore.prepare_events(run.id, events),
             {:ok, scope_system?, scope_key} <- scope_values(scope) do
          params =
            statement_params(attrs, proposal, event_attrs, scope_system?, scope_key, prefix)

          execute(repo, prefix, ctx, scope, proposal, event_attrs, params)
        end

      emit_store_telemetry(result, events, started)
      result
    end

    def commit(_ctx, scope, _proposal, _events) do
      _ = scope_values(scope)
      {:error, :invalid_commit}
    end

    defp execute(repo, prefix, ctx, scope, proposal, event_attrs, params) do
      case Ecto.Adapters.SQL.query!(repo, statement(prefix), params, log: false).rows do
        [[1, inserted, admitted_at, _guards, _detached]] when inserted == length(event_attrs) ->
          maybe_emit_admission_release(admitted_at, proposal.schedule)
          {:ok, proposal.run}

        [[0, 0, nil, _guards, 0]] ->
          RunStore.classify_commit_miss(ctx, scope, proposal)

        rows ->
          raise "fused Docket moment commit returned unexpected rows: #{inspect(rows)}"
      end
    rescue
      error in Postgrex.Error ->
        if unique_violation?(error) do
          {:error, :event_conflict}
        else
          reraise error, __STACKTRACE__
        end
    end

    defp statement_params(attrs, proposal, events, scope_system?, scope_key, prefix) do
      {schedule_code, schedule_at} = schedule_values(proposal.schedule)
      detached = Docket.Run.detached_tasks(proposal.run)

      [
        attrs.run_id,
        scope_system?,
        scope_key,
        attrs.graph_id,
        attrs.graph_hash,
        normalize_database_datetime(attrs.started_at),
        proposal.expected_checkpoint_seq,
        dump_uuid!(proposal.claim_token),
        Atom.to_string(attrs.status),
        attrs.step,
        attrs.state,
        attrs.checkpoint_seq,
        Atom.to_string(proposal.checkpoint_type),
        normalize_database_datetime(attrs.updated_at),
        normalize_optional_datetime(attrs.finished_at),
        schedule_code,
        schedule_at,
        @wake_channel,
        prefix || "",
        Enum.map(events, & &1.seq),
        Enum.map(events, &Atom.to_string(&1.type)),
        Enum.map(events, & &1.step),
        Enum.map(events, & &1.node_id),
        Enum.map(events, & &1.channel_id),
        Enum.map(events, & &1.task_id),
        Enum.map(events, & &1.payload),
        Enum.map(events, & &1.metadata),
        Enum.map(events, & &1.occurred_at),
        Enum.map(detached, & &1.task_id),
        Enum.map(detached, & &1.node_id),
        Enum.map(detached, & &1.attempt),
        Enum.map(detached, &Docket.Run.TaskState.index_state/1),
        Enum.map(detached, &normalize_database_datetime(&1.scheduled_at)),
        Enum.map(detached, &normalize_optional_datetime(&1.started_at)),
        Enum.map(detached, &normalize_optional_datetime(&1.deadline_at))
      ]
    end

    defp statement(prefix) do
      runs = Storage.qualified_table(prefix, "docket_runs")
      events = Storage.qualified_table(prefix, "docket_events")
      detached = Storage.qualified_table(prefix, "docket_detached_tasks")

      """
      WITH target AS MATERIALIZED (
        SELECT run.id, run.tenant_admitted_at
        FROM #{runs} AS run
        WHERE run.run_id = $1::text
          AND ($2::boolean OR run.scope_key = $3::text)
          AND run.graph_id = $4::text
          AND run.graph_hash = $5::text
          AND run.started_at = $6::timestamptz
          AND run.checkpoint_seq = $7::bigint
          AND run.claim_token = $8::uuid
      ),
      updated_run AS (
        UPDATE #{runs} AS run
        SET
          graph_id = $4::text,
          graph_hash = $5::text,
          status = $9::text,
          step = $10::integer,
          state = $11::bytea,
          checkpoint_seq = $12::bigint,
          latest_checkpoint_type = $13::text,
          claim_attempts = 0,
          claim_abandons = 0,
          poisoned_at = NULL,
          poison_reason = NULL,
          started_at = $6::timestamptz,
          updated_at = $14::timestamptz,
          finished_at = $15::timestamptz,
          claim_token =
            CASE WHEN $16::text = 'retain' THEN run.claim_token ELSE NULL END,
          claimed_at =
            CASE WHEN $16::text = 'retain' THEN CURRENT_TIMESTAMP ELSE NULL END,
          tenant_admitted_at =
            CASE
              WHEN $16::text IN ('retain', 'immediate')
              THEN run.tenant_admitted_at
              ELSE NULL
            END,
          wake_at =
            CASE
              WHEN $16::text = 'immediate' THEN CURRENT_TIMESTAMP
              WHEN $16::text = 'at' THEN $17::timestamptz
              ELSE NULL
            END
        FROM target
        WHERE run.id = target.id
          AND run.run_id = $1::text
          AND ($2::boolean OR run.scope_key = $3::text)
          AND run.graph_id = $4::text
          AND run.graph_hash = $5::text
          AND run.started_at = $6::timestamptz
          AND run.checkpoint_seq = $7::bigint
          AND run.claim_token = $8::uuid
        RETURNING run.run_id, run.tenant_id, target.tenant_admitted_at
      ),
      proposed_events AS MATERIALIZED (
        SELECT *
        FROM unnest(
          $20::bigint[],
          $21::text[],
          $22::integer[],
          $23::text[],
          $24::text[],
          $25::text[],
          $26::bytea[],
          $27::bytea[],
          $28::timestamptz[]
        ) AS event(
          seq,
          type,
          step,
          node_id,
          channel_id,
          task_id,
          payload,
          metadata,
          occurred_at
        )
      ),
      inserted_events AS (
        INSERT INTO #{events} AS stored_event (
          run_id,
          seq,
          type,
          step,
          node_id,
          channel_id,
          task_id,
          payload,
          metadata,
          occurred_at,
          inserted_at
        )
        SELECT
          updated_run.run_id,
          event.seq,
          event.type,
          event.step,
          event.node_id,
          event.channel_id,
          event.task_id,
          event.payload,
          event.metadata,
          event.occurred_at,
          clock_timestamp()
        FROM updated_run
        CROSS JOIN proposed_events AS event
        ON CONFLICT (run_id, seq) DO UPDATE
        SET run_id = EXCLUDED.run_id
        WHERE stored_event.type = EXCLUDED.type
          AND stored_event.step = EXCLUDED.step
          AND stored_event.node_id IS NOT DISTINCT FROM EXCLUDED.node_id
          AND stored_event.channel_id IS NOT DISTINCT FROM EXCLUDED.channel_id
          AND stored_event.task_id IS NOT DISTINCT FROM EXCLUDED.task_id
          AND stored_event.payload = EXCLUDED.payload
          AND stored_event.metadata = EXCLUDED.metadata
          AND stored_event.occurred_at = EXCLUDED.occurred_at
        RETURNING seq
      ),
      -- A mismatched conflict is not returned above. Re-inserting one proposed
      -- key raises the existing unique violation and rolls the statement back.
      event_conflict_guard AS (
        INSERT INTO #{events} (
          run_id,
          seq,
          type,
          step,
          node_id,
          channel_id,
          task_id,
          payload,
          metadata,
          occurred_at,
          inserted_at
        )
        SELECT
          updated_run.run_id,
          event.seq,
          event.type,
          event.step,
          event.node_id,
          event.channel_id,
          event.task_id,
          event.payload,
          event.metadata,
          event.occurred_at,
          clock_timestamp()
        FROM updated_run
        CROSS JOIN LATERAL (
          SELECT *
          FROM proposed_events
          LIMIT 1
        ) AS event
        WHERE (SELECT count(*) FROM inserted_events) <>
              (SELECT count(*) FROM proposed_events)
        RETURNING seq
      ),
      proposed_detached AS MATERIALIZED (
        SELECT *
        FROM unnest(
          $29::text[],
          $30::text[],
          $31::integer[],
          $32::text[],
          $33::timestamptz[],
          $34::timestamptz[],
          $35::timestamptz[]
        ) AS task(
          task_id,
          node_id,
          attempt,
          state,
          scheduled_at,
          claimed_at,
          deadline_at
        )
      ),
      -- The claim index is a projection of the committed run's detached
      -- tasks: rows outside the proposed set are stale and deleted, rows in
      -- it are upserted in place. The two row sets are disjoint, and an
      -- empty proposed set clears every row of the run. Both CTEs join
      -- updated_run, so a failed fence syncs nothing.
      deleted_detached AS (
        DELETE FROM #{detached} AS task
        USING updated_run
        WHERE task.run_id = updated_run.run_id
          AND task.task_id <> ALL ($29::text[])
        RETURNING task.id
      ),
      upserted_detached AS (
        INSERT INTO #{detached} AS stored_task (
          run_id,
          tenant_id,
          task_id,
          node_id,
          attempt,
          state,
          scheduled_at,
          claimed_at,
          deadline_at
        )
        SELECT
          updated_run.run_id,
          updated_run.tenant_id,
          task.task_id,
          task.node_id,
          task.attempt,
          task.state,
          task.scheduled_at,
          task.claimed_at,
          task.deadline_at
        FROM updated_run
        CROSS JOIN proposed_detached AS task
        ON CONFLICT (run_id, task_id) DO UPDATE
        SET attempt = EXCLUDED.attempt,
            state = EXCLUDED.state,
            scheduled_at = EXCLUDED.scheduled_at,
            claimed_at = EXCLUDED.claimed_at,
            deadline_at = EXCLUDED.deadline_at
        RETURNING stored_task.id
      ),
      notification AS (
        SELECT pg_notify($18::text, $19::text)
        FROM updated_run
        WHERE $16::text = 'immediate'
           OR (
             $16::text = 'at'
             AND $17::timestamptz <= clock_timestamp()
           )
      )
      SELECT
        (SELECT count(*)::bigint FROM updated_run),
        (SELECT count(*)::bigint FROM inserted_events),
        (SELECT tenant_admitted_at FROM updated_run LIMIT 1),
        (SELECT count(*)::bigint FROM notification) +
          (SELECT count(*)::bigint FROM event_conflict_guard),
        (SELECT count(*)::bigint FROM deleted_detached) +
          (SELECT count(*)::bigint FROM upserted_detached)
      """
    end

    defp unique_violation?(%Postgrex.Error{postgres: %{code: :unique_violation}}), do: true
    defp unique_violation?(_error), do: false

    defp dump_uuid!(token) do
      case Ecto.UUID.dump(token) do
        {:ok, dumped} -> dumped
        :error -> raise ArgumentError, "claim token must be a valid UUID"
      end
    end

    defp schedule_values(:retain_claim), do: {"retain", nil}
    defp schedule_values({:release_claim, :immediate}), do: {"immediate", nil}

    defp schedule_values({:release_claim, {:at, %DateTime{} = at}}),
      do: {"at", normalize_database_datetime(at)}

    defp schedule_values({:release_claim, :external}), do: {"external", nil}
    defp schedule_values({:release_claim, :terminal}), do: {"terminal", nil}

    defp scope_values(:system), do: {:ok, true, ""}
    defp scope_values(:tenantless), do: {:ok, false, ""}

    defp scope_values({:tenant, tenant_id})
         when is_binary(tenant_id) and byte_size(tenant_id) > 0,
         do: {:ok, false, tenant_id}

    defp scope_values(scope) do
      raise ArgumentError,
            "scope must be :system, :tenantless, or {:tenant, tenant_id}, got: #{inspect(scope)}"
    end

    defp maybe_emit_admission_release(nil, _schedule), do: :ok
    defp maybe_emit_admission_release(_admitted_at, :retain_claim), do: :ok
    defp maybe_emit_admission_release(_admitted_at, {:release_claim, :immediate}), do: :ok

    defp maybe_emit_admission_release(_admitted_at, {:release_claim, {:at, _at}}),
      do: emit_admission_release(:future)

    defp maybe_emit_admission_release(_admitted_at, {:release_claim, :external}),
      do: emit_admission_release(:external)

    defp maybe_emit_admission_release(_admitted_at, {:release_claim, :terminal}),
      do: emit_admission_release(:terminal)

    defp emit_admission_release(reason) do
      :telemetry.execute(
        [:docket, :postgres, :admission, :release],
        %{count: 1},
        %{reason: reason}
      )
    end

    defp emit_store_telemetry(result, events, started) do
      encoded_bytes =
        Enum.reduce(events, 0, fn
          %Docket.Event{payload: payload, metadata: metadata}, bytes ->
            bytes +
              byte_size(Docket.DurableCodec.encode!(:event, payload)) +
              byte_size(Docket.DurableCodec.encode!(:event, metadata))

          _event, bytes ->
            bytes
        end)

      :telemetry.execute(
        [:docket, :postgres, :store],
        %{
          duration: System.monotonic_time() - started,
          attempted_rows: length(events),
          encoded_bytes: encoded_bytes
        },
        Map.merge(Docket.Telemetry.correlation_metadata(), %{
          operation: :transition_commit,
          result: Docket.Telemetry.result_kind(result)
        })
      )
    rescue
      _error -> :ok
    end

    defp normalize_optional_datetime(nil), do: nil

    defp normalize_optional_datetime(%DateTime{} = datetime),
      do: normalize_database_datetime(datetime)

    defp normalize_database_datetime(%DateTime{} = datetime) do
      datetime
      |> DateTime.to_unix(:microsecond)
      |> DateTime.from_unix!(:microsecond)
    end
  end
end
