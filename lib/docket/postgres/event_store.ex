if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.EventStore do
    @moduledoc "Postgres append-only persistence for already-assigned runtime events."

    import Ecto.Query

    alias Docket.Postgres.{RunCodec, Storage}
    alias Docket.Postgres.Schemas.{Event, Run}

    @behaviour Docket.Backend.EventStore

    @impl true
    def append_events(_ctx, scope, _run_id, []) do
      validate_scope!(scope)
      :ok
    end

    def append_events(ctx, scope, run_id, events) when is_list(events) do
      started = System.monotonic_time()
      {repo, prefix} = Storage.context!(ctx)

      result =
        with {:ok, attrs} <- encode_events(events, run_id) do
          transactional_append(repo, prefix, scope, run_id, attrs)
        end

      bytes = Enum.reduce(events, 0, fn event, acc -> acc + event_bytes(event) end)

      :telemetry.execute(
        [:docket, :postgres, :store],
        %{
          duration: System.monotonic_time() - started,
          attempted_rows: length(events),
          encoded_bytes: bytes
        },
        Map.merge(Docket.Telemetry.correlation_metadata(), %{
          operation: :event_append,
          result: Docket.Telemetry.result_kind(result)
        })
      )

      result
    end

    def append_events(_ctx, scope, _run_id, _events) do
      validate_scope!(scope)
      {:error, :invalid_events}
    end

    @impl Docket.Backend.EventStore
    def fetch_event(ctx, scope, run_id, seq) when is_integer(seq) and seq > 0 do
      started = System.monotonic_time()
      {repo, prefix} = Storage.context!(ctx)
      {scope_sql, scope_params} = scope_filter(scope, 3)
      params = [run_id, seq] ++ scope_params

      result =
        case Ecto.Adapters.SQL.query(repo, fetch_statement(prefix, scope_sql), params) do
          {:ok, %{rows: []}} -> {:error, :not_found}
          {:ok, %{rows: [[nil | _rest]]}} -> {:error, :not_found}
          {:ok, %{rows: [row]}} -> decode_event_row(row, run_id, 0)
          {:error, reason} -> {:error, reason}
        end

      emit_read_telemetry(:event_fetch, result, started)
      result
    end

    def fetch_event(_ctx, scope, _run_id, _seq) do
      validate_scope!(scope)
      raise ArgumentError, "event sequence must be a positive integer"
    end

    @impl Docket.Backend.EventStore
    def fetch_latest_event(ctx, scope, run_id) do
      started = System.monotonic_time()
      {repo, prefix} = Storage.context!(ctx)
      {scope_sql, scope_params} = scope_filter(scope, 2)
      params = [run_id] ++ scope_params

      result =
        case Ecto.Adapters.SQL.query(repo, latest_statement(prefix, scope_sql), params) do
          {:ok, %{rows: []}} -> {:error, :not_found}
          {:ok, %{rows: [[nil | _rest]]}} -> {:ok, nil}
          {:ok, %{rows: [row]}} -> decode_event_row(row, run_id, 0)
          {:error, reason} -> {:error, reason}
        end

      emit_read_telemetry(:event_fetch_latest, result, started)
      result
    end

    @impl true
    def list_events(ctx, scope, run_id, %{after_seq: after_seq, limit: limit})
        when is_integer(after_seq) and after_seq >= 0 and is_integer(limit) and limit > 0 do
      started = System.monotonic_time()
      {repo, prefix} = Storage.context!(ctx)
      {scope_sql, scope_params} = scope_filter(scope, 4)
      params = [run_id, after_seq, limit] ++ scope_params

      result =
        case Ecto.Adapters.SQL.query(repo, list_statement(prefix, scope_sql), params) do
          {:ok, %{rows: []}} -> {:error, :not_found}
          {:ok, %{rows: rows}} -> decode_page(rows, run_id, after_seq)
          {:error, reason} -> {:error, reason}
        end

      emit_list_telemetry(result, started)
      result
    end

    defp fetch_statement(prefix, scope_sql) do
      runs = Storage.qualified_table(prefix, "docket_runs")
      events = Storage.qualified_table(prefix, "docket_events")

      """
      SELECT
        event.seq,
        event.type,
        event.step,
        event.node_id,
        event.channel_id,
        event.task_id,
        event.payload,
        event.metadata,
        event.occurred_at
      FROM #{runs} AS runs
      LEFT JOIN #{events} AS event
        ON event.run_id = runs.run_id AND event.seq = $2
      WHERE runs.run_id = $1#{scope_sql}
      """
    end

    defp latest_statement(prefix, scope_sql) do
      runs = Storage.qualified_table(prefix, "docket_runs")
      events = Storage.qualified_table(prefix, "docket_events")

      """
      SELECT
        latest.seq,
        latest.type,
        latest.step,
        latest.node_id,
        latest.channel_id,
        latest.task_id,
        latest.payload,
        latest.metadata,
        latest.occurred_at
      FROM #{runs} AS runs
      LEFT JOIN LATERAL (
        SELECT
          event.seq,
          event.type,
          event.step,
          event.node_id,
          event.channel_id,
          event.task_id,
          event.payload,
          event.metadata,
          event.occurred_at
        FROM #{events} AS event
        WHERE event.run_id = runs.run_id
        ORDER BY event.seq DESC
        LIMIT 1
      ) AS latest ON TRUE
      WHERE runs.run_id = $1#{scope_sql}
      """
    end

    defp list_statement(prefix, scope_sql) do
      runs = Storage.qualified_table(prefix, "docket_runs")
      events = Storage.qualified_table(prefix, "docket_events")

      """
      SELECT
        0 AS kind,
        runs.run_id,
        runs.graph_id,
        runs.graph_hash,
        runs.status,
        runs.step,
        runs.checkpoint_seq,
        runs.started_at,
        runs.updated_at,
        runs.finished_at,
        runs.state,
        bounds.oldest,
        bounds.latest,
        NULL AS seq,
        NULL AS type,
        NULL AS event_step,
        NULL AS node_id,
        NULL AS channel_id,
        NULL AS task_id,
        NULL AS payload,
        NULL AS metadata,
        NULL AS occurred_at
      FROM #{runs} AS runs
      LEFT JOIN LATERAL (
        SELECT MIN(e.seq) AS oldest, MAX(e.seq) AS latest
        FROM #{events} AS e
        WHERE e.run_id = runs.run_id
      ) AS bounds ON TRUE
      WHERE runs.run_id = $1#{scope_sql}
      UNION ALL
      SELECT
        1 AS kind,
        NULL AS run_id,
        NULL AS graph_id,
        NULL AS graph_hash,
        NULL AS status,
        NULL AS run_step,
        NULL AS checkpoint_seq,
        NULL AS started_at,
        NULL AS updated_at,
        NULL AS finished_at,
        NULL AS state,
        NULL AS oldest,
        NULL AS latest,
        page.seq,
        page.type,
        page.step,
        page.node_id,
        page.channel_id,
        page.task_id,
        page.payload,
        page.metadata,
        page.occurred_at
      FROM #{runs} AS runs
      JOIN LATERAL (
        SELECT e.seq, e.type, e.step, e.node_id, e.channel_id, e.task_id,
               e.payload, e.metadata, e.occurred_at
        FROM #{events} AS e
        WHERE e.run_id = runs.run_id AND e.seq > $2
        ORDER BY e.seq
        LIMIT $3
      ) AS page ON TRUE
      WHERE runs.run_id = $1#{scope_sql}
      ORDER BY kind, seq
      """
    end

    defp decode_page([header | detail_rows], run_id, after_seq) do
      latest_seq = run_event_seq!(header)
      oldest = Enum.at(header, 11)
      latest = Enum.at(header, 12)

      with {:ok, events} <- decode_events(detail_rows, run_id) do
        {:ok, Docket.EventPage.new(events, after_seq, oldest, latest, latest_seq)}
      end
    end

    defp run_event_seq!(row) do
      attrs = %{
        run_id: Enum.at(row, 1),
        graph_id: Enum.at(row, 2),
        graph_hash: Enum.at(row, 3),
        status: load_status(Enum.at(row, 4)),
        step: Enum.at(row, 5),
        checkpoint_seq: Enum.at(row, 6),
        started_at: Enum.at(row, 7),
        updated_at: Enum.at(row, 8),
        finished_at: Enum.at(row, 9),
        state: Enum.at(row, 10)
      }

      RunCodec.load!(attrs).event_seq
    end

    defp decode_events(rows, run_id) do
      rows
      |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
        case decode_event_row(row, run_id) do
          {:ok, event} -> {:cont, {:ok, [event | acc]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, events} -> {:ok, Enum.reverse(events)}
        error -> error
      end
    end

    defp decode_event_row(row, run_id, offset \\ 13) do
      seq = Enum.at(row, offset)

      try do
        event = %Docket.Event{
          run_id: run_id,
          seq: seq,
          type: load_event_type!(Enum.at(row, offset + 1)),
          step: Enum.at(row, offset + 2),
          node_id: Enum.at(row, offset + 3),
          channel_id: Enum.at(row, offset + 4),
          task_id: Enum.at(row, offset + 5),
          timestamp: Enum.at(row, offset + 8),
          payload: Docket.DurableCodec.decode!(Enum.at(row, offset + 6), :event),
          metadata: Docket.DurableCodec.decode!(Enum.at(row, offset + 7), :event)
        }

        {:ok, validate_decoded_event!(event)}
      rescue
        error in Docket.Error -> {:error, event_corruption(seq, error)}
      end
    end

    defp validate_decoded_event!(%Docket.Event{} = event) do
      valid? =
        is_binary(event.run_id) and is_integer(event.seq) and event.seq > 0 and
          event.type in Docket.Event.types() and is_integer(event.step) and event.step >= 0 and
          optional_binary?(event.node_id) and optional_binary?(event.channel_id) and
          optional_binary?(event.task_id) and database_timestamp?(event.timestamp) and
          is_map(event.payload) and not is_struct(event.payload) and is_map(event.metadata) and
          not is_struct(event.metadata)

      unless valid? do
        raise Docket.Error,
          type: :invalid_durable_state,
          message: "event row fields do not form a valid Docket.Event"
      end

      event
    end

    defp optional_binary?(value), do: is_nil(value) or is_binary(value)

    defp database_timestamp?(
           %DateTime{
             calendar: Calendar.ISO,
             time_zone: "Etc/UTC",
             zone_abbr: "UTC",
             utc_offset: 0,
             std_offset: 0,
             microsecond: {_value, 6}
           } = datetime
         ),
         do: Docket.DurableCodec.valid_datetime?(datetime)

    defp database_timestamp?(_other), do: false

    defp load_status(text) when is_binary(text) do
      Enum.find(Docket.Run.durable_statuses(), fn status -> Atom.to_string(status) == text end)
    end

    defp load_status(other), do: other

    defp load_event_type!(text) when is_binary(text) do
      Enum.find(Docket.Event.types(), fn type -> Atom.to_string(type) == text end) ||
        raise Docket.Error, type: :invalid_durable_state, message: "unknown event type #{text}"
    end

    defp load_event_type!(other) do
      raise Docket.Error,
        type: :invalid_durable_state,
        message: "event type must be text, got: #{inspect(other)}"
    end

    defp event_corruption(seq, %Docket.Error{} = reason) do
      Docket.Error.new(:corrupt_event_row, "Postgres event row is corrupt",
        details: %{seq: seq, cause_type: reason.type},
        reason: reason
      )
    end

    defp scope_filter(:system, _parameter), do: {"", []}
    defp scope_filter(:tenantless, _parameter), do: {" AND runs.tenant_id IS NULL", []}

    defp scope_filter({:tenant, tenant}, parameter) when is_binary(tenant),
      do: {" AND runs.tenant_id = $#{parameter}", [tenant]}

    defp scope_filter(scope, _parameter),
      do: raise(ArgumentError, "invalid storage scope: #{inspect(scope)}")

    defp emit_read_telemetry(operation, result, started) do
      {rows, bytes} =
        case result do
          {:ok, %Docket.Event{} = event} -> {1, event_bytes(event)}
          _other -> {0, 0}
        end

      :telemetry.execute(
        [:docket, :postgres, :store],
        %{
          duration: System.monotonic_time() - started,
          selected_rows: rows,
          encoded_bytes: bytes
        },
        Map.merge(Docket.Telemetry.correlation_metadata(), %{
          operation: operation,
          result: Docket.Telemetry.result_kind(result)
        })
      )
    end

    defp emit_list_telemetry(result, started) do
      {rows, bytes} =
        case result do
          {:ok, page} ->
            {length(page.events),
             Enum.reduce(page.events, 0, fn event, acc -> acc + event_bytes(event) end)}

          _other ->
            {0, 0}
        end

      :telemetry.execute(
        [:docket, :postgres, :store],
        %{
          duration: System.monotonic_time() - started,
          selected_rows: rows,
          encoded_bytes: bytes
        },
        Map.merge(Docket.Telemetry.correlation_metadata(), %{
          operation: :event_list,
          result: Docket.Telemetry.result_kind(result)
        })
      )
    end

    defp transactional_append(repo, prefix, scope, run_id, attrs) do
      case repo.transaction(fn ->
             with :ok <- ensure_visible(repo, prefix, scope, run_id),
                  :ok <- append_batch(repo, prefix, run_id, attrs) do
               :ok
             else
               {:error, reason} -> repo.rollback(reason)
             end
           end) do
        {:ok, :ok} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end

    defp append_batch(repo, prefix, run_id, attrs) do
      inserted_at = normalize_database_datetime(DateTime.utc_now())
      rows = Enum.map(attrs, &Map.put(&1, :inserted_at, inserted_at))

      repo.insert_all(Event, rows,
        prefix: prefix,
        on_conflict: :nothing,
        conflict_target: [:run_id, :seq]
      )

      stored = existing(repo, prefix, run_id, Enum.map(attrs, & &1.seq))
      stored_by_seq = Map.new(stored, &{&1.seq, &1})

      Enum.reduce_while(attrs, :ok, fn event_attrs, :ok ->
        case Map.fetch(stored_by_seq, event_attrs.seq) do
          {:ok, row} ->
            if same_event?(row, event_attrs),
              do: {:cont, :ok},
              else: {:halt, {:error, :event_conflict}}

          :error ->
            {:halt, {:error, :event_insert_failed}}
        end
      end)
    end

    defp existing(repo, prefix, run_id, seqs) do
      Event
      |> where([event], event.run_id == ^run_id and event.seq in ^seqs)
      |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)
      |> repo.all()
    end

    defp consolidate_events(attrs) do
      Enum.reduce_while(attrs, {:ok, %{}}, fn attrs, {:ok, by_seq} ->
        case Map.fetch(by_seq, attrs.seq) do
          :error -> {:cont, {:ok, Map.put(by_seq, attrs.seq, attrs)}}
          {:ok, ^attrs} -> {:cont, {:ok, by_seq}}
          {:ok, _different} -> {:halt, {:error, :event_conflict}}
        end
      end)
      |> case do
        {:ok, by_seq} -> {:ok, by_seq |> Map.values() |> Enum.sort_by(& &1.seq)}
        error -> error
      end
    end

    defp same_event?(row, attrs) do
      Enum.all?(
        [
          :run_id,
          :seq,
          :type,
          :step,
          :node_id,
          :channel_id,
          :task_id,
          :payload,
          :metadata,
          :occurred_at
        ],
        &(Map.fetch!(row, &1) == Map.fetch!(attrs, &1))
      )
    end

    defp encode_events(events, run_id) do
      Enum.reduce_while(events, {:ok, []}, fn
        %Docket.Event{run_id: ^run_id, seq: seq, timestamp: %DateTime{} = timestamp} = event,
        {:ok, acc}
        when is_integer(seq) and seq > 0 ->
          case encode_event(event, run_id, seq, timestamp) do
            {:ok, attrs} -> {:cont, {:ok, [attrs | acc]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        %Docket.Event{run_id: ^run_id, seq: seq}, _
        when not is_integer(seq) or seq <= 0 ->
          {:halt, {:error, :invalid_event_sequence}}

        %Docket.Event{run_id: ^run_id}, _ ->
          {:halt, {:error, :invalid_events}}

        %Docket.Event{}, _ ->
          {:halt, {:error, :event_run_mismatch}}

        _, _ ->
          {:halt, {:error, :invalid_events}}
      end)
      |> case do
        {:ok, attrs} -> attrs |> Enum.reverse() |> consolidate_events()
        error -> error
      end
    end

    defp encode_event(event, run_id, seq, timestamp) do
      attrs = %{
        run_id: run_id,
        seq: seq,
        type: event.type,
        step: event.step,
        node_id: event.node_id,
        channel_id: event.channel_id,
        task_id: event.task_id,
        payload: Docket.DurableCodec.encode!(:event, event.payload),
        metadata: Docket.DurableCodec.encode!(:event, event.metadata),
        occurred_at: normalize_database_datetime(timestamp)
      }

      if Event.changeset(attrs).valid?, do: {:ok, attrs}, else: {:error, :invalid_events}
    rescue
      _error in Docket.Error -> {:error, :invalid_events}
    end

    defp event_bytes(%Docket.Event{} = event) do
      byte_size(Docket.DurableCodec.encode!(:event, event.payload)) +
        byte_size(Docket.DurableCodec.encode!(:event, event.metadata))
    rescue
      _ -> 0
    end

    defp event_bytes(_), do: 0

    defp ensure_visible(repo, prefix, scope, run_id) do
      query =
        Run
        |> where([run], run.run_id == ^run_id)
        |> scope_query(scope)
        |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)

      if repo.exists?(query), do: :ok, else: {:error, :not_found}
    end

    defp scope_query(query, :system), do: query
    defp scope_query(query, :tenantless), do: where(query, [run], is_nil(run.tenant_id))

    defp scope_query(query, {:tenant, tenant}) when is_binary(tenant),
      do: where(query, [run], run.tenant_id == ^tenant)

    defp scope_query(_query, scope),
      do: raise(ArgumentError, "invalid storage scope: #{inspect(scope)}")

    defp validate_scope!(:system), do: :ok
    defp validate_scope!(:tenantless), do: :ok
    defp validate_scope!({:tenant, tenant}) when is_binary(tenant), do: :ok

    defp validate_scope!(scope),
      do: raise(ArgumentError, "invalid storage scope: #{inspect(scope)}")

    defp normalize_database_datetime(%DateTime{} = datetime) do
      datetime
      |> DateTime.to_unix(:microsecond)
      |> DateTime.from_unix!(:microsecond)
    end
  end
end
