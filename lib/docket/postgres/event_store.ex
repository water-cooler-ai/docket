if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.EventStore do
    @moduledoc "Postgres append-only persistence for already-assigned runtime events."

    import Ecto.Query

    alias Docket.Postgres.Storage
    alias Docket.Postgres.Schemas.{Event, Run}

    @behaviour Docket.Storage.Events

    @impl true
    def append_events(_ctx, scope, _run_id, []) do
      validate_scope!(scope)
      :ok
    end

    def append_events(ctx, scope, run_id, events) when is_list(events) do
      {repo, prefix} = Storage.context!(ctx)

      with :ok <- ensure_visible(repo, prefix, scope, run_id),
           {:ok, attrs} <- encode_events(events, run_id) do
        Enum.reduce_while(attrs, :ok, fn event_attrs, :ok ->
          case append_one(repo, prefix, event_attrs) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
      end
    end

    def append_events(_ctx, scope, _run_id, _events) do
      validate_scope!(scope)
      {:error, :invalid_events}
    end

    defp append_one(repo, prefix, attrs) do
      {count, _rows} =
        repo.insert_all(Event, [Map.put(attrs, :inserted_at, DateTime.utc_now())],
          prefix: prefix,
          on_conflict: :nothing,
          conflict_target: [:run_id, :seq]
        )

      case count do
        1 ->
          :ok

        0 ->
          case existing(repo, prefix, attrs.run_id, attrs.seq) do
            nil -> {:error, :event_insert_failed}
            row -> if same_event?(row, attrs), do: :ok, else: {:error, :event_conflict}
          end
      end
    end

    defp existing(repo, prefix, run_id, seq) do
      Event
      |> where([event], event.run_id == ^run_id and event.seq == ^seq)
      |> then(fn query -> if prefix, do: put_query_prefix(query, prefix), else: query end)
      |> repo.one()
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

          if Event.changeset(attrs).valid?,
            do: {:cont, {:ok, [attrs | acc]}},
            else: {:halt, {:error, :invalid_events}}

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
        {:ok, attrs} -> {:ok, Enum.reverse(attrs)}
        error -> error
      end
    end

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
