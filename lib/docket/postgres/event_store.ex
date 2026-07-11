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
        append_batch(repo, prefix, run_id, attrs)
      end
    end

    def append_events(_ctx, scope, _run_id, _events) do
      validate_scope!(scope)
      {:error, :invalid_events}
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
        {:ok, attrs} -> attrs |> Enum.reverse() |> consolidate_events()
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
