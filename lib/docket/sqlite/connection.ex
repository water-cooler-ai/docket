if Code.ensure_loaded?(Exqlite.Sqlite3) do
  defmodule Docket.SQLite.Connection do
    @moduledoc """
    The single SQLite connection behind one `Docket.SQLite` backend.

    One process owns the database handle, and every read and write runs inside
    that process. Operations are therefore serialized: a transaction can never
    interleave with another Docket transaction, so the stores need no row locks,
    no `SKIP LOCKED`, no advisory locks, and no busy-retry loops. Claim tokens
    and checkpoint fences are still enforced, because a vehicle can crash or be
    slower than its claim, but they never race another writer.

    The process is the database's only Docket writer. Other OS processes (an
    operator's `sqlite3` shell, say) may read the file; they must not write to
    Docket's tables.

    Prepared statements are cached per SQL string for the life of the
    connection.
    """

    use GenServer

    alias Exqlite.Sqlite3

    @schema_version 1
    @default_call_timeout 15_000

    @schema [
      """
      CREATE TABLE docket_graph_versions (
        tenant_id TEXT NOT NULL,
        graph_id TEXT NOT NULL,
        graph_hash TEXT NOT NULL,
        document BLOB NOT NULL,
        published_at INTEGER NOT NULL,
        PRIMARY KEY (tenant_id, graph_id, graph_hash)
      ) WITHOUT ROWID
      """,
      """
      CREATE INDEX docket_graph_versions_latest
        ON docket_graph_versions (tenant_id, graph_id, published_at DESC, graph_hash DESC)
      """,
      """
      CREATE TABLE docket_runs (
        run_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        graph_id TEXT NOT NULL,
        graph_hash TEXT NOT NULL,
        status TEXT NOT NULL,
        step INTEGER NOT NULL,
        checkpoint_seq INTEGER NOT NULL,
        checkpoint_type TEXT NOT NULL,
        event_seq INTEGER NOT NULL,
        started_at INTEGER,
        updated_at INTEGER,
        finished_at INTEGER,
        state BLOB NOT NULL,
        wake_at INTEGER,
        claim_token TEXT,
        claimed_at INTEGER,
        claim_attempts INTEGER NOT NULL DEFAULT 0,
        claim_abandons INTEGER NOT NULL DEFAULT 0,
        poisoned_at INTEGER,
        poison_reason TEXT
      )
      """,
      """
      CREATE INDEX docket_runs_ready ON docket_runs (wake_at, run_id)
        WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NULL
          AND wake_at IS NOT NULL
      """,
      """
      CREATE INDEX docket_runs_claimed ON docket_runs (claimed_at, run_id)
        WHERE claim_token IS NOT NULL
      """,
      """
      CREATE INDEX docket_runs_list ON docket_runs (tenant_id, started_at DESC, run_id DESC)
      """,
      """
      CREATE TABLE docket_events (
        run_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        type TEXT NOT NULL,
        step INTEGER NOT NULL,
        node_id TEXT,
        channel_id TEXT,
        task_id TEXT,
        occurred_at INTEGER NOT NULL,
        payload BLOB NOT NULL,
        metadata BLOB NOT NULL,
        PRIMARY KEY (run_id, seq)
      ) WITHOUT ROWID
      """
    ]

    @type db :: Sqlite3.db()

    @doc "The schema version this module creates and requires."
    @spec schema_version() :: pos_integer()
    def schema_version, do: @schema_version

    @spec start_link(keyword()) :: GenServer.on_start()
    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
    end

    def child_spec(opts) do
      %{id: Keyword.get(opts, :name, __MODULE__), start: {__MODULE__, :start_link, [opts]}}
    end

    @doc """
    Runs `fun` with the database handle inside one immediate transaction.

    The transaction commits unless `fun` returns `{:error, _}` or raises; an
    exception is re-raised in the caller with its original stacktrace.
    """
    @spec transaction(GenServer.server(), (db() -> result)) :: result when result: term()
    def transaction(conn, fun) when is_function(fun, 1), do: call(conn, {:transaction, fun})

    @doc "Runs `fun` with the database handle, outside an explicit transaction."
    @spec read(GenServer.server(), (db() -> result)) :: result when result: term()
    def read(conn, fun) when is_function(fun, 1), do: call(conn, {:read, fun})

    defp call(conn, request) do
      case GenServer.call(conn, request, @default_call_timeout) do
        {:ok, result} -> result
        {:raise, kind, reason, stacktrace} -> :erlang.raise(kind, reason, stacktrace)
      end
    end

    # -------------------------------------------------------------------
    # Statement helpers. These run only inside the connection process.
    # -------------------------------------------------------------------

    @doc false
    @spec query!(db(), String.t(), list()) :: [list()]
    def query!(db, sql, params \\ []) do
      stmt = statement!(db, sql)

      try do
        :ok = Sqlite3.bind(stmt, params)

        case Sqlite3.fetch_all(db, stmt) do
          {:ok, rows} -> rows
          {:error, reason} -> raise_sqlite!(reason, sql)
        end
      after
        Sqlite3.reset(stmt)
      end
    end

    @doc false
    @spec one!(db(), String.t(), list()) :: list() | nil
    def one!(db, sql, params \\ []) do
      case query!(db, sql, params) do
        [] -> nil
        [row] -> row
        [row | _more] -> row
      end
    end

    @doc false
    @spec execute!(db(), String.t(), list()) :: non_neg_integer()
    def execute!(db, sql, params \\ []) do
      _rows = query!(db, sql, params)
      {:ok, changes} = Sqlite3.changes(db)
      changes
    end

    defp statement!(db, sql) do
      key = {__MODULE__, :stmt, sql}

      case Process.get(key) do
        nil ->
          case Sqlite3.prepare(db, sql) do
            {:ok, stmt} ->
              Process.put(key, stmt)
              stmt

            {:error, reason} ->
              raise_sqlite!(reason, sql)
          end

        stmt ->
          stmt
      end
    end

    defp raise_sqlite!(reason, sql) do
      raise Docket.Error.new(:sqlite_error, "SQLite statement failed: #{inspect(reason)}",
              details: %{sql: sql}
            )
    end

    # -------------------------------------------------------------------
    # Server
    # -------------------------------------------------------------------

    @impl true
    def init(opts) do
      path = Keyword.fetch!(opts, :database)

      {:ok, db} =
        case Sqlite3.open(path) do
          {:ok, db} -> {:ok, db}
          {:error, reason} -> raise ArgumentError, "could not open #{path}: #{inspect(reason)}"
        end

      configure!(db, path, opts)
      migrate!(db)
      {:ok, %{db: db}}
    end

    @impl true
    def handle_call({:read, fun}, _from, %{db: db} = state) do
      {:reply, capture(fn -> fun.(db) end), state}
    end

    def handle_call({:transaction, fun}, _from, %{db: db} = state) do
      :ok = Sqlite3.execute(db, "BEGIN IMMEDIATE")

      reply =
        case capture(fn -> fun.(db) end) do
          {:ok, {:error, _reason}} = reply ->
            :ok = Sqlite3.execute(db, "ROLLBACK")
            reply

          {:ok, _result} = reply ->
            :ok = Sqlite3.execute(db, "COMMIT")
            reply

          {:raise, _kind, _reason, _stacktrace} = reply ->
            _ = Sqlite3.execute(db, "ROLLBACK")
            reply
        end

      {:reply, reply, state}
    end

    @impl true
    def terminate(_reason, %{db: db}) do
      Process.get_keys()
      |> Enum.filter(&match?({__MODULE__, :stmt, _sql}, &1))
      |> Enum.each(&Sqlite3.release(db, Process.delete(&1)))

      Sqlite3.close(db)
    end

    defp capture(fun) do
      {:ok, fun.()}
    catch
      kind, reason -> {:raise, kind, reason, __STACKTRACE__}
    end

    defp configure!(db, path, opts) do
      if path != ":memory:" do
        [["wal"]] = query!(db, "PRAGMA journal_mode = WAL")
      end

      synchronous =
        case Keyword.get(opts, :synchronous, :normal) do
          mode when mode in [:off, :normal, :full, :extra] ->
            mode

          other ->
            raise ArgumentError,
                  ":synchronous must be :off, :normal, :full, or :extra, got: #{inspect(other)}"
        end

      :ok = Sqlite3.execute(db, "PRAGMA synchronous = #{String.upcase(to_string(synchronous))}")
      :ok = Sqlite3.execute(db, "PRAGMA foreign_keys = OFF")
      :ok = Sqlite3.set_busy_timeout(db, Keyword.get(opts, :busy_timeout_ms, 5_000))
    end

    defp migrate!(db) do
      case query!(db, "PRAGMA user_version") do
        [[0]] ->
          :ok = Sqlite3.execute(db, "BEGIN IMMEDIATE")
          Enum.each(@schema, &(:ok = Sqlite3.execute(db, &1)))
          :ok = Sqlite3.execute(db, "PRAGMA user_version = #{@schema_version}")
          :ok = Sqlite3.execute(db, "COMMIT")

        [[@schema_version]] ->
          :ok

        [[other]] ->
          raise ArgumentError,
                "Docket.SQLite requires schema version #{@schema_version}, found #{other}"
      end
    end
  end
end
