if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Pruner do
    @moduledoc """
    Periodic, bounded retention for Postgres events, terminal runs, and graph versions.

    A pass deletes persisted events before their run when event retention is
    shorter, then deletes terminal runs and relies on the event foreign key to
    cascade any remaining events. It deletes graph versions only when no run
    references them and they rank older than the newest ten publications for
    their owner scope and `graph_id`. Revision order is immutable
    `(inserted_at, graph_hash)`, matching the public graph-version API. The
    newest ten revisions per owner therefore survive even when they have no
    runs, and one tenant's publications cannot evict another tenant's history.

    Passes are serialized per database schema with a transaction-scoped
    advisory lock. A competing node reports a skipped pass instead of waiting.
    Candidate selection is also bounded and uses `SKIP LOCKED`, so application
    row locks do not stall retention.

    The batch size bounds explicitly selected event and run rows. A run may
    own more than one batch of events, so foreign-key cascade work is
    deliberately not claimed to be bounded by the batch size.

    Each pass emits `[:docket, :postgres, :pruner, :pass]`. Its numeric
    measurements are `events_deleted`, `runs_deleted`,
    `cascade_events_deleted`, `graphs_deleted`, and `duration`; `duration` is
    in native `System.monotonic_time/0` units, following the `:telemetry` span
    convention. Metadata contains only the bounded `result` dimension.
    """

    use GenServer

    alias Docket.Postgres.Storage

    @telemetry_event [:docket, :postgres, :pruner, :pass]
    @terminal_status_sql Docket.Run.terminal_statuses()
                         |> Enum.map_join(", ", &"'#{&1}'")

    @retained_graph_revisions 10
    @type ctx :: Docket.Backend.ctx()
    @type policy :: %{
            required(:now) => DateTime.t() | :database,
            required(:event_retention_ms) => non_neg_integer(),
            required(:run_retention_ms) => non_neg_integer(),
            required(:batch_size) => pos_integer()
          }
    @type counts :: %{
            required(:events_deleted) => non_neg_integer(),
            required(:runs_deleted) => non_neg_integer(),
            required(:cascade_events_deleted) => non_neg_integer(),
            required(:graphs_deleted) => non_neg_integer()
          }
    @type result :: {:ok, counts()} | {:skipped, :locked} | {:error, term()}

    @type option ::
            {:name, GenServer.name()}
            | {:context, ctx()}
            | {:interval_ms, pos_integer()}
            | {:event_retention_ms, non_neg_integer()}
            | {:run_retention_ms, non_neg_integer()}
            | {:batch_size, pos_integer()}
            | {:clock, (-> DateTime.t() | :database)}

    @spec start_link([option()]) :: GenServer.on_start()
    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
    end

    def child_spec(opts) do
      %{
        id: Keyword.get(opts, :name, __MODULE__),
        start: {__MODULE__, :start_link, [opts]}
      }
    end

    @doc "Runs one bounded retention pass and emits committed count telemetry."
    @spec prune(ctx(), policy()) :: result()
    def prune(ctx, policy) do
      policy = validate_policy!(policy)
      started = System.monotonic_time()

      result =
        Storage.transaction(ctx, fn tx ->
          {repo, prefix} = Storage.context!(tx)

          case acquire_lock(repo, prefix) do
            :locked -> {:ok, {:skipped, :locked}}
            :acquired -> prune_locked(repo, prefix, policy)
          end
        end)
        |> case do
          {:ok, {:skipped, :locked}} -> {:skipped, :locked}
          {:ok, counts} -> {:ok, counts}
          {:error, reason} -> {:error, reason}
        end

      emit_telemetry(result, System.monotonic_time() - started)
      result
    end

    @impl true
    def init(opts) do
      state = %{
        context: Keyword.fetch!(opts, :context),
        interval_ms: positive!(opts, :interval_ms),
        clock: Keyword.get(opts, :clock, fn -> :database end),
        policy: %{
          event_retention_ms: non_negative!(opts, :event_retention_ms),
          run_retention_ms: non_negative!(opts, :run_retention_ms),
          batch_size: positive!(opts, :batch_size)
        },
        timer: nil
      }

      unless is_function(state.clock, 0) do
        raise ArgumentError, "pruner clock must be a zero-argument function"
      end

      _ = validate_policy!(Map.put(state.policy, :now, clock_now!(state.clock)))
      {:ok, state, {:continue, :prune}}
    end

    @impl true
    def handle_continue(:prune, state), do: {:noreply, run_and_schedule(state)}

    @impl true
    def handle_info({:prune, token}, %{timer: {_timer, token}} = state),
      do: {:noreply, run_and_schedule(%{state | timer: nil})}

    def handle_info({:prune, _stale_token}, state), do: {:noreply, state}

    def handle_info(_message, state), do: {:noreply, state}

    @impl true
    def terminate(_reason, %{timer: timer}) do
      if timer, do: timer |> elem(0) |> Process.cancel_timer()
      :ok
    end

    defp run_and_schedule(state) do
      _ = prune(state.context, Map.put(state.policy, :now, clock_now!(state.clock)))
      token = make_ref()
      timer = Process.send_after(self(), {:prune, token}, state.interval_ms)
      %{state | timer: {timer, token}}
    end

    defp clock_now!(clock) do
      case clock.() do
        :database -> :database
        %DateTime{} = now -> now
        other -> raise ArgumentError, "pruner clock must return a DateTime or :database, got: #{inspect(other)}"
      end
    end

    defp prune_locked(repo, prefix, policy) do
      with {:ok, now} <- resolve_now(repo, policy.now),
           event_cutoff = DateTime.add(now, -policy.event_retention_ms, :millisecond),
           run_cutoff = DateTime.add(now, -policy.run_retention_ms, :millisecond),
           {:ok, events_deleted} <- delete_events(repo, prefix, event_cutoff, policy.batch_size),
           {:ok, run_candidates} <-
             select_runs(repo, prefix, run_cutoff, policy.batch_size),
           {:ok, run_candidates} <- count_run_events(repo, prefix, run_candidates),
           {:ok, runs_deleted} <- delete_runs(repo, prefix, run_candidates),
           {:ok, graph_candidates} <- select_graphs(repo, prefix, policy.batch_size),
           {:ok, graphs_deleted} <- delete_graphs(repo, prefix, graph_candidates) do
        cascade_events_deleted =
          run_candidates
          |> Enum.filter(&MapSet.member?(runs_deleted.ids, &1.id))
          |> Enum.map(& &1.event_count)
          |> Enum.sum()

        {:ok,
         %{
           events_deleted: events_deleted,
           runs_deleted: runs_deleted.count,
           cascade_events_deleted: cascade_events_deleted,
           graphs_deleted: graphs_deleted
         }}
      end
    end

    defp acquire_lock(repo, prefix) do
      key = "docket:pruner:" <> lock_prefix(repo, prefix)

      case Ecto.Adapters.SQL.query(
             repo,
             "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
             [key]
           ) do
        {:ok, %{rows: [[true]]}} -> :acquired
        {:ok, %{rows: [[false]]}} -> :locked
        {:error, reason} -> repo.rollback(reason)
      end
    end

    defp delete_events(repo, prefix, cutoff, batch_size) do
      sql = """
      WITH candidates AS MATERIALIZED (
        SELECT id
        FROM #{Storage.qualified_table(prefix, "docket_events")}
        WHERE inserted_at < $1
        ORDER BY inserted_at, id
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      DELETE FROM #{Storage.qualified_table(prefix, "docket_events")} AS events
      USING candidates
      WHERE events.id = candidates.id
      """

      case Ecto.Adapters.SQL.query(repo, sql, [cutoff, batch_size]) do
        {:ok, %{num_rows: count}} -> {:ok, count}
        {:error, reason} -> {:error, reason}
      end
    end

    defp select_runs(repo, prefix, cutoff, batch_size) do
      sql = """
      SELECT runs.id, runs.run_id, runs.graph_id, runs.graph_hash
      FROM #{Storage.qualified_table(prefix, "docket_runs")} AS runs
      WHERE runs.status IN (#{@terminal_status_sql})
        AND runs.updated_at < $1
      ORDER BY runs.updated_at, runs.id
      LIMIT $2
      FOR UPDATE OF runs SKIP LOCKED
      """

      case Ecto.Adapters.SQL.query(repo, sql, [cutoff, batch_size]) do
        {:ok, %{rows: rows}} ->
          {:ok,
           Enum.map(rows, fn [id, run_id, graph_id, graph_hash] ->
             %{id: id, run_id: run_id, graph_id: graph_id, graph_hash: graph_hash}
           end)}

        {:error, reason} ->
          {:error, reason}
      end
    end

    defp count_run_events(_repo, _prefix, []), do: {:ok, []}

    defp count_run_events(repo, prefix, candidates) do
      run_ids = Enum.map(candidates, & &1.run_id)

      sql = """
      SELECT run_id, count(*)::bigint
      FROM #{Storage.qualified_table(prefix, "docket_events")}
      WHERE run_id = ANY($1::text[])
      GROUP BY run_id
      """

      case Ecto.Adapters.SQL.query(repo, sql, [run_ids]) do
        {:ok, %{rows: rows}} ->
          counts = Map.new(rows, fn [run_id, count] -> {run_id, count} end)
          {:ok, Enum.map(candidates, &Map.put(&1, :event_count, Map.get(counts, &1.run_id, 0)))}

        {:error, reason} ->
          {:error, reason}
      end
    end

    defp delete_runs(_repo, _prefix, []), do: {:ok, %{count: 0, ids: MapSet.new()}}

    defp delete_runs(repo, prefix, candidates) do
      ids = Enum.map(candidates, & &1.id)

      sql = """
      DELETE FROM #{Storage.qualified_table(prefix, "docket_runs")}
      WHERE id = ANY($1::bigint[])
      RETURNING id
      """

      case Ecto.Adapters.SQL.query(repo, sql, [ids]) do
        {:ok, %{rows: rows, num_rows: count}} ->
          {:ok, %{count: count, ids: rows |> Enum.map(&hd/1) |> MapSet.new()}}

        {:error, reason} ->
          {:error, reason}
      end
    end

    defp select_graphs(repo, prefix, batch_size) do
      sql = """
      WITH ranked AS MATERIALIZED (
        SELECT id,
               row_number() OVER (
                 PARTITION BY scope_key, graph_id
                 ORDER BY inserted_at DESC, graph_hash DESC
               ) AS revision_rank
        FROM #{Storage.qualified_table(prefix, "docket_graph_versions")}
      ),
      candidates AS MATERIALIZED (
        SELECT graphs.id
        FROM #{Storage.qualified_table(prefix, "docket_graph_versions")} AS graphs
        JOIN ranked ON ranked.id = graphs.id
        WHERE ranked.revision_rank > #{@retained_graph_revisions}
          AND NOT EXISTS (
            SELECT 1
            FROM #{Storage.qualified_table(prefix, "docket_runs")} AS runs
            WHERE runs.scope_key = graphs.scope_key
              AND runs.graph_id = graphs.graph_id
              AND runs.graph_hash = graphs.graph_hash
          )
        ORDER BY graphs.inserted_at, graphs.graph_hash, graphs.id
        LIMIT $1
        FOR UPDATE OF graphs SKIP LOCKED
      )
      SELECT id FROM candidates
      """

      case Ecto.Adapters.SQL.query(repo, sql, [batch_size]) do
        {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &hd/1)}
        {:error, reason} -> {:error, reason}
      end
    end

    defp delete_graphs(_repo, _prefix, []), do: {:ok, 0}

    defp delete_graphs(repo, prefix, ids) do
      sql = """
      DELETE FROM #{Storage.qualified_table(prefix, "docket_graph_versions")} AS graphs
      WHERE graphs.id = ANY($1::bigint[])
        AND NOT EXISTS (
          SELECT 1
          FROM #{Storage.qualified_table(prefix, "docket_runs")} AS runs
          WHERE runs.scope_key = graphs.scope_key
            AND runs.graph_id = graphs.graph_id
            AND runs.graph_hash = graphs.graph_hash
        )
      """

      case Ecto.Adapters.SQL.query(repo, sql, [ids]) do
        {:ok, %{num_rows: count}} -> {:ok, count}
        {:error, reason} -> {:error, reason}
      end
    end

    defp validate_policy!(
           %{
             now: %DateTime{} = now,
             event_retention_ms: event_retention_ms,
             run_retention_ms: run_retention_ms,
             batch_size: batch_size
           } = policy
         )
         when is_integer(event_retention_ms) and event_retention_ms >= 0 and
                is_integer(run_retention_ms) and run_retention_ms >= 0 and
                is_integer(batch_size) and batch_size > 0 do
      if event_retention_ms > run_retention_ms do
        raise ArgumentError,
              "event retention must not exceed run retention, got: #{inspect(policy)}"
      end

      %{policy | now: normalize_datetime(now)}
    end

    defp validate_policy!(%{now: :database} = policy) do
      validate_policy!(Map.put(policy, :now, ~U[2000-01-01 00:00:00Z]))
      policy
    end

    defp validate_policy!(policy) do
      raise ArgumentError,
            "prune policy requires DateTime now, non-negative event/run retention, " <>
              "and a positive batch size, got: #{inspect(policy)}"
    end

    defp emit_telemetry(result, duration) do
      {measurements, metadata} =
        case result do
          {:ok, counts} ->
            {Map.put(counts, :duration, duration), %{result: :ok}}

          {:skipped, :locked} ->
            {Map.put(empty_counts(), :duration, duration), %{result: :locked}}

          {:error, _reason} ->
            {Map.put(empty_counts(), :duration, duration), %{result: :error}}
        end

      :telemetry.execute(@telemetry_event, measurements, metadata)
    end

    defp empty_counts do
      %{events_deleted: 0, runs_deleted: 0, cascade_events_deleted: 0, graphs_deleted: 0}
    end

    defp positive!(opts, key) do
      case Keyword.fetch!(opts, key) do
        value when is_integer(value) and value > 0 -> value
        value -> raise ArgumentError, "#{key} must be a positive integer, got: #{inspect(value)}"
      end
    end

    defp non_negative!(opts, key) do
      case Keyword.fetch!(opts, key) do
        value when is_integer(value) and value >= 0 ->
          value

        value ->
          raise ArgumentError, "#{key} must be a non-negative integer, got: #{inspect(value)}"
      end
    end

    defp normalize_datetime(datetime) do
      datetime |> DateTime.to_unix(:microsecond) |> DateTime.from_unix!(:microsecond)
    end

    defp resolve_now(_repo, %DateTime{} = now), do: {:ok, now}

    defp resolve_now(repo, :database) do
      case Ecto.Adapters.SQL.query(repo, "SELECT transaction_timestamp()") do
        {:ok, %{rows: [[%DateTime{} = now]]}} -> {:ok, normalize_datetime(now)}
        {:error, reason} -> {:error, reason}
      end
    end

    defp lock_prefix(_repo, prefix) when is_binary(prefix), do: prefix

    defp lock_prefix(repo, nil) do
      case Ecto.Adapters.SQL.query(repo, "SELECT current_schema()") do
        {:ok, %{rows: [[prefix]]}} when is_binary(prefix) -> prefix
        {:error, reason} -> repo.rollback(reason)
      end
    end
  end
end
