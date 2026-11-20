if Code.ensure_loaded?(Exqlite.Sqlite3) do
  defmodule Docket.SQLite.Store do
    @moduledoc """
    Graph, run, event, and transition stores for `Docket.SQLite`.

    Every operation is one call into `Docket.SQLite.Connection`, which runs it
    serialized against every other Docket operation on the database. Writes
    run inside `BEGIN IMMEDIATE`; because nothing else can interleave, each
    store reads the row, decides in Elixir, and writes it back, with no
    compare-and-swap SQL.

    Storage layout mirrors `Docket.Postgres`: the columns needed for identity,
    scheduling, claiming, and listing are columns, and every other run field
    is one opaque `Docket.DurableCodec` blob. Timestamps are stored as integer
    microseconds since the Unix epoch. The tenantless owner scope is stored as
    the empty tenant ID, which a real tenant ID can never be.
    """

    @behaviour Docket.Backend.GraphStore
    @behaviour Docket.Backend.RunStore
    @behaviour Docket.Backend.EventStore
    @behaviour Docket.Backend.TransitionStore

    import Docket.SQLite.Connection, only: [query!: 3, one!: 3, execute!: 3]

    alias Docket.{DurableCodec, Run}
    alias Docket.Backend.TransitionStore
    alias Docket.SQLite.Connection

    @tenantless ""

    @state_fields [
      :input,
      :output,
      :failure,
      :channels,
      :changed_channels,
      :pending_nodes,
      :active_tasks,
      :pending_writes,
      :interrupts,
      :timers,
      :event_seq,
      :metadata
    ]

    @run_columns "run_id, tenant_id, graph_id, graph_hash, status, step, checkpoint_seq, " <>
                   "started_at, updated_at, finished_at, state, wake_at, claim_token, " <>
                   "claimed_at, claim_attempts, claim_abandons, poisoned_at, poison_reason"

    @event_columns "seq, type, step, node_id, channel_id, task_id, occurred_at, payload, metadata"

    # =================================================================
    # TransitionStore
    # =================================================================

    @impl Docket.Backend.TransitionStore
    def initialize(ctx, owner_scope, proposal, events) do
      tenant_id = owner_tenant_id!(owner_scope)

      with :ok <- TransitionStore.validate(:initialize, 0, proposal, events),
           {:ok, attrs} <- dump_run(proposal.run),
           {:ok, encoded_events} <- encode_events(events, proposal.run.id) do
        wake_at = to_us(proposal.wake_at)

        result =
          Connection.transaction(ctx.conn, fn db ->
            run = proposal.run

            cond do
              is_nil(
                one!(
                  db,
                  "SELECT 1 FROM docket_graph_versions WHERE tenant_id = ?1 AND graph_id = ?2 AND graph_hash = ?3",
                  [tenant_id, run.graph_id, run.graph_hash]
                )
              ) ->
                {:error, :not_found}

              one!(db, "SELECT 1 FROM docket_runs WHERE run_id = ?1", [run.id]) ->
                {:error, :conflict}

              true ->
                insert_run!(db, tenant_id, attrs, proposal.checkpoint_type, wake_at)

                with :ok <- merge_events(db, run.id, encoded_events) do
                  {:ok, run}
                end
            end
          end)

        notify_if_due(ctx, result, proposal.wake_at)
      end
    end

    @impl Docket.Backend.TransitionStore
    def commit_claimed(ctx, scope, proposal, events) do
      validate_scope!(scope)
      expected = Map.get(proposal, :expected_checkpoint_seq, -1)

      with :ok <- TransitionStore.validate(:claimed, expected, proposal, events),
           {:ok, attrs} <- dump_run(proposal.run),
           {:ok, encoded_events} <- encode_events(events, proposal.run.id) do
        ctx.conn
        |> Connection.transaction(fn db ->
          commit(ctx, db, scope, proposal, attrs, encoded_events, fn row ->
            row.checkpoint_seq == expected and row.claim_token == proposal.claim_token
          end)
        end)
        |> notify_after_commit(ctx, proposal.schedule)
      end
    end

    @impl Docket.Backend.TransitionStore
    def commit_unclaimed(ctx, scope, expected, proposal, events)
        when is_integer(expected) and expected >= 0 do
      validate_scope!(scope)

      with :ok <- TransitionStore.validate(:unclaimed, expected, proposal, events),
           {:ok, attrs} <- dump_run(proposal.run),
           {:ok, encoded_events} <- encode_events(events, proposal.run.id) do
        ctx.conn
        |> Connection.transaction(fn db ->
          commit(ctx, db, scope, proposal, attrs, encoded_events, fn row ->
            row.checkpoint_seq == expected
          end)
        end)
        |> notify_after_commit(ctx, proposal.schedule)
      end
    end

    def commit_unclaimed(_ctx, scope, _expected, _proposal, _events) do
      validate_scope!(scope)
      {:error, :invalid_transition}
    end

    defp commit(ctx, db, scope, proposal, attrs, encoded_events, fence?) do
      run = proposal.run

      with {:ok, row} <- fetch_row(db, scope, run.id),
           :ok <- immutable_binding(row, run),
           true <- fence?.(row) || {:error, :stale_checkpoint},
           :ok <- merge_events(db, run.id, encoded_events) do
        {wake_at, claim_token, claimed_at} =
          schedule_columns(row, proposal.schedule, now_us(ctx))

        execute!(
          db,
          """
          UPDATE docket_runs SET
            status = ?2, step = ?3, checkpoint_seq = ?4, checkpoint_type = ?5, event_seq = ?6,
            updated_at = ?7, finished_at = ?8, state = ?9,
            wake_at = ?10, claim_token = ?11, claimed_at = ?12,
            claim_attempts = 0, claim_abandons = 0, poisoned_at = NULL, poison_reason = NULL
          WHERE run_id = ?1
          """,
          [
            run.id,
            attrs.status,
            attrs.step,
            attrs.checkpoint_seq,
            Atom.to_string(proposal.checkpoint_type),
            attrs.event_seq,
            attrs.updated_at,
            attrs.finished_at,
            {:blob, attrs.state},
            wake_at,
            claim_token,
            claimed_at
          ]
        )

        {:ok, run}
      end
    end

    defp schedule_columns(row, :retain_claim, now), do: {nil, row.claim_token, now}
    defp schedule_columns(_row, {:release_claim, :immediate}, now), do: {now, nil, nil}

    defp schedule_columns(_row, {:release_claim, {:at, %DateTime{} = at}}, _now),
      do: {to_us(at), nil, nil}

    defp schedule_columns(_row, {:release_claim, reason}, _now)
         when reason in [:external, :terminal],
         do: {nil, nil, nil}

    defp immutable_binding(row, run) do
      if row.run_id == run.id and row.graph_id == run.graph_id and
           row.graph_hash == run.graph_hash and row.started_at == to_us(run.started_at),
         do: :ok,
         else: {:error, :invalid_transition}
    end

    defp insert_run!(db, tenant_id, attrs, checkpoint_type, wake_at) do
      execute!(
        db,
        """
        INSERT INTO docket_runs (
          run_id, tenant_id, graph_id, graph_hash, status, step, checkpoint_seq,
          checkpoint_type, event_seq, started_at, updated_at, finished_at, state, wake_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
        """,
        [
          attrs.run_id,
          tenant_id,
          attrs.graph_id,
          attrs.graph_hash,
          attrs.status,
          attrs.step,
          attrs.checkpoint_seq,
          Atom.to_string(checkpoint_type),
          attrs.event_seq,
          attrs.started_at,
          attrs.updated_at,
          attrs.finished_at,
          {:blob, attrs.state},
          wake_at
        ]
      )
    end

    # Events are idempotent by canonical content at {run_id, seq}.
    defp merge_events(db, run_id, encoded_events) do
      Enum.reduce_while(encoded_events, :ok, fn event, :ok ->
        case one!(
               db,
               "SELECT #{@event_columns} FROM docket_events WHERE run_id = ?1 AND seq = ?2",
               [
                 run_id,
                 event.seq
               ]
             ) do
          nil ->
            execute!(
              db,
              """
              INSERT INTO docket_events (run_id, seq, type, step, node_id, channel_id, task_id,
                occurred_at, payload, metadata)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
              """,
              [
                run_id,
                event.seq,
                event.type,
                event.step,
                event.node_id,
                event.channel_id,
                event.task_id,
                event.occurred_at,
                {:blob, event.payload},
                {:blob, event.metadata}
              ]
            )

            {:cont, :ok}

          existing ->
            if same_event?(existing, event),
              do: {:cont, :ok},
              else: {:halt, {:error, :event_conflict}}
        end
      end)
    end

    # Columns compare directly; payload and metadata compare as decoded
    # terms, because equal maps need not encode to identical bytes.
    defp same_event?(
           [seq, type, step, node_id, channel_id, task_id, occurred_at, payload, metadata],
           event
         ) do
      [seq, type, step, node_id, channel_id, task_id, occurred_at] ==
        [
          event.seq,
          event.type,
          event.step,
          event.node_id,
          event.channel_id,
          event.task_id,
          event.occurred_at
        ] and same_term?(payload, event.payload, event.raw_payload) and
        same_term?(metadata, event.metadata, event.raw_metadata)
    end

    defp same_term?(bytes, bytes, _raw), do: true

    defp same_term?(stored, _encoded, raw) do
      case DurableCodec.decode(stored, :event) do
        {:ok, decoded} -> decoded === raw
        {:error, _corrupt} -> false
      end
    end

    defp encode_events(events, run_id) do
      events
      |> Enum.uniq_by(& &1.seq)
      |> Enum.reduce_while({:ok, []}, fn
        %Docket.Event{run_id: ^run_id} = event, {:ok, acc} ->
          encoded = %{
            seq: event.seq,
            type: Atom.to_string(event.type),
            step: event.step,
            node_id: event.node_id,
            channel_id: event.channel_id,
            task_id: event.task_id,
            occurred_at: to_us(event.timestamp),
            payload: DurableCodec.encode!(:event, event.payload),
            metadata: DurableCodec.encode!(:event, event.metadata),
            raw_payload: event.payload,
            raw_metadata: event.metadata
          }

          {:cont, {:ok, [encoded | acc]}}

        _event, _acc ->
          {:halt, {:error, :invalid_transition}}
      end)
      |> case do
        {:ok, encoded} -> {:ok, Enum.reverse(encoded)}
        error -> error
      end
    rescue
      _error in Docket.Error -> {:error, :invalid_transition}
    end

    defp notify_after_commit({:ok, _run} = result, ctx, {:release_claim, :immediate}) do
      notify(ctx)
      result
    end

    defp notify_after_commit({:ok, _run} = result, ctx, {:release_claim, {:at, at}}) do
      notify_if_due(ctx, result, at)
    end

    defp notify_after_commit(result, _ctx, _schedule), do: result

    defp notify_if_due(ctx, {:ok, _run} = result, %DateTime{} = wake_at) do
      if DateTime.compare(wake_at, now(ctx)) != :gt, do: notify(ctx), else: notify(ctx, wake_at)
      result
    end

    defp notify_if_due(_ctx, result, _wake_at), do: result

    # In-process replacement for LISTEN/NOTIFY: tell the runner, if one is
    # running, that work is due now or at a known future instant.
    defp notify(ctx, at \\ nil) do
      case Map.get(ctx, :runner) do
        nil ->
          :ok

        runner ->
          case GenServer.whereis(runner) do
            nil -> :ok
            pid -> send(pid, {:docket_sqlite_wake, at})
          end
      end
    end

    # =================================================================
    # GraphStore
    # =================================================================

    @impl Docket.Backend.GraphStore
    def save_graph(
          ctx,
          owner_scope,
          graph_id,
          graph_hash,
          %Docket.Graph{id: id, diagnostics: []} = graph
        )
        when id == graph_id do
      tenant_id = owner_tenant_id!(owner_scope)
      bytes = DurableCodec.encode!(:graph, graph)

      if hash(bytes) == graph_hash do
        Connection.transaction(ctx.conn, fn db ->
          case one!(
                 db,
                 "SELECT document FROM docket_graph_versions WHERE tenant_id = ?1 AND graph_id = ?2 AND graph_hash = ?3",
                 [tenant_id, graph_id, graph_hash]
               ) do
            nil ->
              execute!(
                db,
                """
                INSERT INTO docket_graph_versions (tenant_id, graph_id, graph_hash, document, published_at)
                VALUES (?1, ?2, ?3, ?4, ?5)
                """,
                [tenant_id, graph_id, graph_hash, {:blob, bytes}, now_us(ctx)]
              )

              :ok

            [^bytes] ->
              :ok

            [_other] ->
              {:error, :graph_content_conflict}
          end
        end)
      else
        {:error, :invalid_graph_hash}
      end
    rescue
      _error in Docket.Error -> {:error, :invalid_graph_document}
    end

    def save_graph(_ctx, _owner_scope, _graph_id, _graph_hash, _graph),
      do: {:error, :invalid_graph_document}

    @impl Docket.Backend.GraphStore
    def fetch_graph(ctx, owner_scope, graph_id, graph_hash) do
      tenant_id = owner_tenant_id!(owner_scope)

      ctx.conn
      |> Connection.read(fn db ->
        one!(
          db,
          "SELECT document FROM docket_graph_versions WHERE tenant_id = ?1 AND graph_id = ?2 AND graph_hash = ?3",
          [tenant_id, graph_id, graph_hash]
        )
      end)
      |> case do
        nil -> {:error, :not_found}
        [bytes] -> load_graph(bytes, graph_id, graph_hash)
      end
    end

    defp load_graph(bytes, graph_id, graph_hash) do
      with true <- hash(bytes) == graph_hash,
           {:ok, %Docket.Graph{id: ^graph_id} = graph} <- DurableCodec.decode(bytes, :graph) do
        {:ok, graph}
      else
        _corrupt -> {:error, :corrupt_graph}
      end
    end

    @impl Docket.Backend.GraphStore
    def fetch_latest_graph_ref(ctx, owner_scope, graph_id) do
      tenant_id = owner_tenant_id!(owner_scope)

      ctx.conn
      |> Connection.read(fn db ->
        one!(
          db,
          """
          SELECT graph_hash FROM docket_graph_versions
          WHERE tenant_id = ?1 AND graph_id = ?2
          ORDER BY published_at DESC, graph_hash DESC LIMIT 1
          """,
          [tenant_id, graph_id]
        )
      end)
      |> case do
        nil -> {:error, :not_found}
        [graph_hash] -> {:ok, %Docket.GraphRef{graph_id: graph_id, graph_hash: graph_hash}}
      end
    end

    @impl Docket.Backend.GraphStore
    def list_graph_versions(ctx, owner_scope, graph_id, %{limit: limit, before: before}) do
      tenant_id = owner_tenant_id!(owner_scope)

      unless is_integer(limit) and limit > 0 do
        raise ArgumentError, "graph version list limit must be positive"
      end

      {cursor_sql, cursor_params} =
        case before do
          nil ->
            {"", []}

          {%DateTime{} = published_at, before_hash} ->
            {"AND (published_at < ?4 OR (published_at = ?4 AND graph_hash < ?5))",
             [to_us(published_at), before_hash]}
        end

      rows =
        Connection.read(ctx.conn, fn db ->
          query!(
            db,
            """
            SELECT graph_hash, published_at FROM docket_graph_versions
            WHERE tenant_id = ?1 AND graph_id = ?2 #{cursor_sql}
            ORDER BY published_at DESC, graph_hash DESC LIMIT ?3
            """,
            [tenant_id, graph_id, limit + 1] ++ cursor_params
          )
        end)

      versions =
        Enum.map(rows, fn [graph_hash, published_at] ->
          %Docket.GraphVersion{
            ref: %Docket.GraphRef{graph_id: graph_id, graph_hash: graph_hash},
            published_at: from_us(published_at)
          }
        end)

      {:ok, Docket.GraphVersionPage.new(versions, before, limit)}
    end

    # =================================================================
    # RunStore
    # =================================================================

    @impl Docket.Backend.RunStore
    def fetch_run(ctx, scope, run_id) do
      validate_scope!(scope)

      with {:ok, row} <- Connection.read(ctx.conn, &fetch_row(&1, scope, run_id)) do
        {:ok, load_run!(row)}
      end
    end

    @impl Docket.Backend.RunStore
    def inspect_run(ctx, scope, run_id) do
      validate_scope!(scope)

      with {:ok, row} <- Connection.read(ctx.conn, &fetch_row(&1, scope, run_id)) do
        {:ok, run_info(row)}
      end
    end

    @impl Docket.Backend.RunStore
    def list_runs(ctx, scope, %{
          limit: limit,
          before: before,
          graph_id: graph_id,
          graph_hash: graph_hash,
          statuses: statuses
        }) do
      validate_scope!(scope)

      unless is_integer(limit) and limit > 0 do
        raise ArgumentError, "run list limit must be positive"
      end

      {clauses, params} =
        [
          scope_clause(scope),
          graph_id && {"graph_id = ?", [graph_id]},
          graph_hash && {"graph_hash = ?", [graph_hash]},
          statuses &&
            {"status IN (#{Enum.map_join(statuses, ", ", fn _ -> "?" end)})",
             Enum.map(statuses, &Atom.to_string/1)},
          case before do
            nil ->
              nil

            {%DateTime{} = started_at, run_id} ->
              us = to_us(started_at)
              {"(started_at < ? OR (started_at = ? AND run_id < ?))", [us, us, run_id]}
          end
        ]
        |> Enum.reject(&is_nil/1)
        |> Enum.unzip()

      where = if clauses == [], do: "", else: "WHERE " <> Enum.join(clauses, " AND ")

      rows =
        Connection.read(ctx.conn, fn db ->
          query!(
            db,
            """
            SELECT run_id, tenant_id, graph_id, graph_hash, status, step, checkpoint_seq,
                   started_at, updated_at, finished_at
            FROM docket_runs #{where}
            ORDER BY started_at DESC, run_id DESC LIMIT ?
            """,
            List.flatten(params) ++ [limit + 1]
          )
        end)

      summaries =
        Enum.map(rows, fn [
                            id,
                            tenant_id,
                            gid,
                            ghash,
                            status,
                            step,
                            seq,
                            started,
                            updated,
                            finished
                          ] ->
          Docket.RunSummary.new!(
            id: id,
            tenant_id: if(tenant_id == @tenantless, do: nil, else: tenant_id),
            graph_id: gid,
            graph_hash: ghash,
            status: String.to_existing_atom(status),
            step: step,
            checkpoint_seq: seq,
            started_at: from_us(started),
            updated_at: from_us(updated),
            finished_at: from_us(finished)
          )
        end)

      {:ok, Docket.RunPage.new(summaries, before, limit)}
    end

    @impl Docket.Backend.RunStore
    def claim_due(ctx, :system, policy) do
      validate_claim_policy!(policy)
      now = to_us(policy.now)
      cutoff = now - policy.orphan_ttl_ms * 1_000

      Connection.transaction(ctx.conn, fn db ->
        ready =
          query!(
            db,
            """
            SELECT run_id, wake_at, claim_attempts FROM docket_runs
            WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NULL
              AND wake_at IS NOT NULL AND wake_at <= ?1
            ORDER BY wake_at, run_id LIMIT ?2
            """,
            [now, policy.limit]
          )
          |> Enum.map(fn [id, at, attempts] -> {{at, 0, id}, :ready, attempts} end)

        expired =
          query!(
            db,
            """
            SELECT run_id, claimed_at, claim_attempts FROM docket_runs
            WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL
              AND claimed_at < ?1
            ORDER BY claimed_at, run_id LIMIT ?2
            """,
            [cutoff, policy.limit]
          )
          |> Enum.map(fn [id, at, attempts] -> {{at, 1, id}, :expired, attempts} end)

        selected =
          (ready ++ expired)
          |> Enum.sort_by(&elem(&1, 0))
          |> select_candidates(policy.limit, Map.get(policy, :preference))

        {leases, poisoned} =
          Enum.reduce(selected, {[], []}, fn {{_at, _class, run_id}, _class_name, attempts},
                                             {leases, poisoned} ->
            if attempts < policy.max_claim_attempts do
              {[claim!(ctx, db, run_id, attempts + 1, policy) | leases], poisoned}
            else
              reason = "max_claim_attempts_exceeded"

              execute!(
                db,
                """
                UPDATE docket_runs SET wake_at = NULL, claim_token = NULL, claimed_at = NULL,
                  poisoned_at = ?2, poison_reason = ?3
                WHERE run_id = ?1
                """,
                [run_id, now, reason]
              )

              {leases,
               [%{run_id: run_id, poisoned_at: policy.now, poison_reason: reason} | poisoned]}
            end
          end)

        {:ok, %{leases: Enum.reverse(leases), poisoned: Enum.reverse(poisoned)}}
      end)
    end

    def claim_due(_ctx, scope, _policy) do
      raise ArgumentError, "claim_due scope must be :system, got: #{inspect(scope)}"
    end

    defp claim!(ctx, db, run_id, claim_attempt, policy) do
      token = generate_token(ctx)

      [tenant_id, graph_id, graph_hash, checkpoint_seq] =
        one!(
          db,
          """
          UPDATE docket_runs SET wake_at = NULL, claim_token = ?2, claimed_at = ?3,
            claim_attempts = ?4
          WHERE run_id = ?1
          RETURNING tenant_id, graph_id, graph_hash, checkpoint_seq
          """,
          [run_id, token, to_us(policy.now), claim_attempt]
        )

      %{
        run_id: run_id,
        owner_scope: owner_scope(tenant_id),
        graph_id: graph_id,
        graph_hash: graph_hash,
        checkpoint_seq: checkpoint_seq,
        claim_token: token,
        claimed_at: policy.now,
        claim_attempt: claim_attempt,
        orphan_ttl_ms: policy.orphan_ttl_ms
      }
    end

    # With demand of at least two and both classes present, the oldest
    # candidate of each class is selected before the remaining demand takes
    # the sorted (oldest-eligible) order; with demand of exactly one an
    # optional preferred class is served first, falling through when empty.
    defp select_candidates(sorted, limit, preference) do
      cond do
        limit >= 2 ->
          heads =
            [:ready, :expired]
            |> Enum.map(fn class -> Enum.find(sorted, &(elem(&1, 1) == class)) end)
            |> Enum.reject(&is_nil/1)

          heads ++ Enum.take(Enum.reject(sorted, &(&1 in heads)), limit - length(heads))

        limit == 1 and preference in [:ready, :expired] ->
          case Enum.find(sorted, &(elem(&1, 1) == preference)) do
            nil -> Enum.take(sorted, 1)
            candidate -> [candidate]
          end

        true ->
          Enum.take(sorted, limit)
      end
    end

    @impl Docket.Backend.RunStore
    def refresh_claim(ctx, :system, run_id, claim_token, now) do
      changed =
        Connection.transaction(ctx.conn, fn db ->
          execute!(
            db,
            """
            UPDATE docket_runs SET claimed_at = MAX(COALESCE(claimed_at, ?3), ?3)
            WHERE run_id = ?1 AND claim_token = ?2
            """,
            [run_id, claim_token, to_us(now)]
          )
        end)

      if changed == 1, do: :ok, else: {:error, :claim_lost}
    end

    def refresh_claim(_ctx, scope, _run_id, _claim_token, _now) do
      raise ArgumentError, "refresh_claim scope must be :system, got: #{inspect(scope)}"
    end

    @impl Docket.Backend.RunStore
    def release_claim(ctx, :system, run_id, claim_token, now) do
      changed =
        Connection.transaction(ctx.conn, fn db ->
          execute!(
            db,
            """
            UPDATE docket_runs SET claim_token = NULL, claimed_at = NULL, wake_at = ?3
            WHERE run_id = ?1 AND claim_token = ?2
            """,
            [run_id, claim_token, to_us(now)]
          )
        end)

      if changed == 1, do: notify(ctx)
      :ok
    end

    def release_claim(_ctx, scope, _run_id, _claim_token, _now) do
      raise ArgumentError, "release_claim scope must be :system, got: #{inspect(scope)}"
    end

    @impl Docket.Backend.RunStore
    def abandon_claim(ctx, :system, run_id, claim_token, policy) do
      validate_abandon_policy!(policy)

      Connection.transaction(ctx.conn, fn db ->
        case one!(
               db,
               """
               SELECT checkpoint_seq, claim_attempts, claim_abandons FROM docket_runs
               WHERE run_id = ?1 AND claim_token = ?2
               """,
               [run_id, claim_token]
             ) do
          [seq, attempts, abandons] when seq == policy.expected_checkpoint_seq ->
            apply_abandon(db, run_id, max(attempts - 1, 0), abandons, policy)

          _stale ->
            {:ok, :stale}
        end
      end)
    end

    def abandon_claim(_ctx, scope, _run_id, _claim_token, _policy) do
      raise ArgumentError, "abandon_claim scope must be :system, got: #{inspect(scope)}"
    end

    defp apply_abandon(db, run_id, attempts, abandons, policy) do
      cond do
        Map.get(policy, :non_poisoning, false) ->
          wake = non_poisoning_wake(policy, abandons)
          reschedule_abandon!(db, run_id, attempts, abandons + 1, wake)

        abandons < policy.max_claim_abandons ->
          reschedule_abandon!(db, run_id, attempts, abandons + 1, policy.retry_at)

        true ->
          execute!(
            db,
            """
            UPDATE docket_runs SET claim_token = NULL, claimed_at = NULL, claim_attempts = ?2,
              wake_at = NULL, poisoned_at = ?3, poison_reason = 'max_claim_abandons_exceeded'
            WHERE run_id = ?1
            """,
            [run_id, attempts, to_us(policy.now)]
          )

          {:ok, :poisoned}
      end
    end

    defp reschedule_abandon!(db, run_id, attempts, abandons, wake_at) do
      execute!(
        db,
        """
        UPDATE docket_runs SET claim_token = NULL, claimed_at = NULL, claim_attempts = ?2,
          claim_abandons = ?3, wake_at = ?4
        WHERE run_id = ?1
        """,
        [run_id, attempts, abandons, to_us(wake_at)]
      )

      {:ok, :rescheduled}
    end

    defp non_poisoning_wake(%{backoff: %{base_ms: base, cap_ms: cap}, now: now}, abandons),
      do: DateTime.add(now, min(base * Integer.pow(2, min(abandons, 30)), cap), :millisecond)

    defp non_poisoning_wake(policy, _abandons), do: policy.retry_at

    @impl Docket.Backend.RunStore
    def retry_poisoned_run(ctx, scope, run_id, now) do
      validate_scope!(scope)

      result =
        Connection.transaction(ctx.conn, fn db ->
          with {:ok, row} <- fetch_row(db, scope, run_id) do
            cond do
              row.status in ["done", "failed", "cancelled"] ->
                {:error, :inactive_run}

              is_nil(row.poisoned_at) ->
                {:ok, load_run!(row)}

              true ->
                execute!(
                  db,
                  """
                  UPDATE docket_runs SET claim_token = NULL, claimed_at = NULL, wake_at = ?2,
                    claim_attempts = 0, claim_abandons = 0, poisoned_at = NULL, poison_reason = NULL
                  WHERE run_id = ?1
                  """,
                  [run_id, to_us(now)]
                )

                {:ok, {:recovered, load_run!(row)}}
            end
          end
        end)

      case result do
        {:ok, {:recovered, run}} ->
          notify(ctx)
          {:ok, run}

        other ->
          other
      end
    end

    # =================================================================
    # EventStore
    # =================================================================

    @impl Docket.Backend.EventStore
    def fetch_event(ctx, scope, run_id, seq) do
      validate_scope!(scope)

      unless is_integer(seq) and seq > 0 do
        raise ArgumentError, "event sequence must be a positive integer"
      end

      Connection.read(ctx.conn, fn db ->
        with :ok <- run_visible(db, scope, run_id) do
          case one!(
                 db,
                 "SELECT #{@event_columns} FROM docket_events WHERE run_id = ?1 AND seq = ?2",
                 [
                   run_id,
                   seq
                 ]
               ) do
            nil -> {:error, :not_found}
            row -> decode_event(row, run_id)
          end
        end
      end)
    end

    @impl Docket.Backend.EventStore
    def fetch_latest_event(ctx, scope, run_id) do
      validate_scope!(scope)

      Connection.read(ctx.conn, fn db ->
        with :ok <- run_visible(db, scope, run_id) do
          case one!(
                 db,
                 "SELECT #{@event_columns} FROM docket_events WHERE run_id = ?1 ORDER BY seq DESC LIMIT 1",
                 [run_id]
               ) do
            nil -> {:ok, nil}
            row -> decode_event(row, run_id)
          end
        end
      end)
    end

    @impl Docket.Backend.EventStore
    def list_events(ctx, scope, run_id, %{after_seq: after_seq, limit: limit}) do
      validate_scope!(scope)

      unless is_integer(after_seq) and after_seq >= 0 and is_integer(limit) and limit > 0 do
        raise ArgumentError, "event cursor must be non-negative and limit must be positive"
      end

      Connection.read(ctx.conn, fn db ->
        {scope_sql, scope_params} = scope_clause(scope, 2)

        case one!(
               db,
               "SELECT event_seq FROM docket_runs WHERE run_id = ?1 #{scope_sql}",
               [run_id | scope_params]
             ) do
          nil ->
            {:error, :not_found}

          [latest_seq] ->
            [oldest, latest] =
              one!(db, "SELECT MIN(seq), MAX(seq) FROM docket_events WHERE run_id = ?1", [run_id])

            rows =
              query!(
                db,
                """
                SELECT #{@event_columns} FROM docket_events
                WHERE run_id = ?1 AND seq > ?2 ORDER BY seq LIMIT ?3
                """,
                [run_id, after_seq, limit]
              )

            rows
            |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
              case decode_event(row, run_id) do
                {:ok, event} -> {:cont, {:ok, [event | acc]}}
                error -> {:halt, error}
              end
            end)
            |> case do
              {:ok, events} ->
                {:ok,
                 Docket.EventPage.new(Enum.reverse(events), after_seq, oldest, latest, latest_seq)}

              error ->
                error
            end
        end
      end)
    end

    defp run_visible(db, scope, run_id) do
      {scope_sql, scope_params} = scope_clause(scope, 2)

      if one!(db, "SELECT 1 FROM docket_runs WHERE run_id = ?1 #{scope_sql}", [
           run_id | scope_params
         ]),
         do: :ok,
         else: {:error, :not_found}
    end

    defp decode_event(
           [seq, type, step, node_id, channel_id, task_id, occurred_at, payload, metadata],
           run_id
         ) do
      {:ok,
       %Docket.Event{
         run_id: run_id,
         seq: seq,
         type: event_type!(type),
         step: step,
         node_id: node_id,
         channel_id: channel_id,
         task_id: task_id,
         timestamp: from_us(occurred_at),
         payload: DurableCodec.decode!(payload, :event),
         metadata: DurableCodec.decode!(metadata, :event)
       }}
    rescue
      error in Docket.Error ->
        {:error,
         Docket.Error.new(:corrupt_event_row, "SQLite event row is corrupt",
           details: %{seq: seq, cause_type: error.type},
           reason: error
         )}
    end

    defp event_type!(type) do
      Enum.find(Docket.Event.types(), &(Atom.to_string(&1) == type)) ||
        raise Docket.Error.new(:invalid_durable_state, "unknown event type #{inspect(type)}")
    end

    # =================================================================
    # Backend-owned maintenance used by Docket.SQLite.Runner
    # =================================================================

    @doc false
    # The runner owns the database. On boot, any claim still held belongs to
    # a vehicle from a previous process that can no longer commit, so it is
    # released for immediate re-claim instead of waiting out the orphan TTL.
    # The claim attempt it consumed still counts toward poisoning.
    @spec release_stale_claims(map()) :: non_neg_integer()
    def release_stale_claims(ctx) do
      Connection.transaction(ctx.conn, fn db ->
        execute!(
          db,
          """
          UPDATE docket_runs SET claim_token = NULL, claimed_at = NULL, wake_at = ?1
          WHERE claim_token IS NOT NULL
          """,
          [now_us(ctx)]
        )
      end)
    end

    @doc false
    @spec next_wake(map()) :: DateTime.t() | nil
    def next_wake(ctx) do
      Connection.read(ctx.conn, fn db ->
        one!(
          db,
          """
          SELECT MIN(wake_at) FROM docket_runs
          WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NULL
            AND wake_at IS NOT NULL
          """,
          []
        )
      end)
      |> case do
        [nil] -> nil
        [us] -> from_us(us)
      end
    end

    # =================================================================
    # Rows and codecs
    # =================================================================

    defp fetch_row(db, scope, run_id) do
      {scope_sql, scope_params} = scope_clause(scope, 2)

      case one!(
             db,
             "SELECT #{@run_columns} FROM docket_runs WHERE run_id = ?1 #{scope_sql}",
             [run_id | scope_params]
           ) do
        nil ->
          {:error, :not_found}

        [
          id,
          tenant_id,
          gid,
          ghash,
          status,
          step,
          seq,
          started,
          updated,
          finished,
          state,
          wake,
          token,
          claimed,
          attempts,
          abandons,
          poisoned,
          reason
        ] ->
          {:ok,
           %{
             run_id: id,
             tenant_id: tenant_id,
             graph_id: gid,
             graph_hash: ghash,
             status: status,
             step: step,
             checkpoint_seq: seq,
             started_at: started,
             updated_at: updated,
             finished_at: finished,
             state: state,
             wake_at: wake,
             claim_token: token,
             claimed_at: claimed,
             claim_attempts: attempts,
             claim_abandons: abandons,
             poisoned_at: poisoned,
             poison_reason: reason
           }}
      end
    end

    defp dump_run(%Run{} = run) do
      {:ok,
       %{
         run_id: run.id,
         graph_id: run.graph_id,
         graph_hash: run.graph_hash,
         status: Atom.to_string(run.status),
         step: run.step,
         checkpoint_seq: run.checkpoint_seq,
         event_seq: run.event_seq,
         started_at: to_us(run.started_at),
         updated_at: to_us(run.updated_at),
         finished_at: to_us(run.finished_at),
         state: DurableCodec.encode!(:run, Map.take(run, @state_fields))
       }}
    rescue
      _error in Docket.Error -> {:error, :invalid_transition}
    end

    defp load_run!(row) do
      state = DurableCodec.decode!(row.state, :run)

      unless is_map(state) and Enum.sort(Map.keys(state)) == Enum.sort(@state_fields) do
        raise Docket.Error.new(:invalid_durable_state, "SQLite run state has the wrong fields")
      end

      run =
        struct!(
          Run,
          Map.merge(state, %{
            id: row.run_id,
            graph_id: row.graph_id,
            graph_hash: row.graph_hash,
            status: String.to_existing_atom(row.status),
            step: row.step,
            checkpoint_seq: row.checkpoint_seq,
            started_at: from_us(row.started_at),
            updated_at: from_us(row.updated_at),
            finished_at: from_us(row.finished_at)
          })
        )

      case Run.validate_durable(run) do
        :ok -> run
        {:error, error} -> raise error
      end
    end

    defp run_info(row) do
      Docket.RunInfo.new!(
        run: load_run!(row),
        wake_at: from_us(row.wake_at),
        claimed_at: from_us(row.claimed_at),
        claim_attempts: row.claim_attempts,
        claim_abandons: row.claim_abandons,
        poisoned_at: from_us(row.poisoned_at),
        poison_reason: row.poison_reason
      )
    end

    # =================================================================
    # Scope, time, validation
    # =================================================================

    defp scope_clause(:system), do: nil
    defp scope_clause(:tenantless), do: {"tenant_id = ?", [@tenantless]}
    defp scope_clause({:tenant, tenant_id}), do: {"tenant_id = ?", [tenant_id]}

    defp scope_clause(:system, _index), do: {"", []}
    defp scope_clause(:tenantless, index), do: {"AND tenant_id = ?#{index}", [@tenantless]}
    defp scope_clause({:tenant, tenant_id}, index), do: {"AND tenant_id = ?#{index}", [tenant_id]}

    defp owner_tenant_id!(:tenantless), do: @tenantless

    defp owner_tenant_id!({:tenant, tenant_id})
         when is_binary(tenant_id) and byte_size(tenant_id) > 0,
         do: tenant_id

    defp owner_tenant_id!(scope) do
      raise ArgumentError,
            "run owner scope must be :tenantless or {:tenant, tenant_id}, got: #{inspect(scope)}"
    end

    defp owner_scope(@tenantless), do: :tenantless
    defp owner_scope(tenant_id), do: {:tenant, tenant_id}

    defp validate_scope!(:system), do: :ok
    defp validate_scope!(:tenantless), do: :ok
    defp validate_scope!({:tenant, tenant_id}) when is_binary(tenant_id), do: :ok

    defp validate_scope!(scope) do
      raise ArgumentError,
            "scope must be :system, :tenantless, or {:tenant, tenant_id}, got: #{inspect(scope)}"
    end

    defp validate_claim_policy!(%{
           now: %DateTime{},
           limit: limit,
           orphan_ttl_ms: ttl,
           max_claim_attempts: max
         })
         when is_integer(limit) and limit > 0 and is_integer(ttl) and ttl >= 0 and
                is_integer(max) and max > 0,
         do: :ok

    defp validate_claim_policy!(policy),
      do: raise(ArgumentError, "invalid claim policy: #{inspect(policy)}")

    defp validate_abandon_policy!(%{
           expected_checkpoint_seq: seq,
           now: %DateTime{} = now,
           retry_at: %DateTime{} = retry_at,
           max_claim_abandons: max
         })
         when is_integer(seq) and seq >= 0 and is_integer(max) and max > 0 do
      if DateTime.compare(retry_at, now) == :lt do
        raise ArgumentError, "abandon policy retry_at must not precede now"
      end

      :ok
    end

    defp validate_abandon_policy!(policy),
      do: raise(ArgumentError, "invalid abandon policy: #{inspect(policy)}")

    defp hash(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    defp generate_token(ctx) do
      token = ctx.token_generator.()

      unless is_binary(token) and byte_size(token) > 0 do
        raise ArgumentError, "claim token generator must return a non-empty binary"
      end

      token
    end

    defp now(ctx), do: ctx.clock.()
    defp now_us(ctx), do: to_us(now(ctx))

    defp to_us(nil), do: nil
    defp to_us(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :microsecond)

    defp from_us(nil), do: nil
    defp from_us(us) when is_integer(us), do: DateTime.from_unix!(us, :microsecond)
  end
end
