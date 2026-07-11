if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Notifier do
    @moduledoc """
    LISTEN fast path turning committed immediate-wake notifications into
    dispatcher polls.

    The Postgres RunStore announces every committed due wake with `pg_notify`
    on the `docket_wake` channel, carrying the schema prefix (empty string
    when unprefixed) as payload. The notifier holds one dedicated LISTEN
    connection outside the Repo pool, and each notification whose payload
    matches its own context prefix requests one immediate
    `Docket.Postgres.Dispatcher` poll. Notification bursts collapse inside
    the dispatcher.

    Polling remains the correctness mechanism. A lost notification, a dead
    listener, or an absent notifier only delays claiming until the
    dispatcher's next scheduled poll; omitting this child entirely is
    poll-only operation, where the dispatcher's poll interval alone bounds
    immediate-wake latency.

    The LISTEN connection derives its options from the configured Repo,
    reconnects on its own, and re-subscribes after reconnecting.
    Notifications sent while it is disconnected are dropped. `pg_notify`
    emission is ordinary SQL inside the writer's transaction and works
    through any pooler, but LISTEN requires a session-scoped connection:
    behind PgBouncer in transaction or statement pooling mode, point
    `:connection` at a direct or session-pooled endpoint, or run poll-only.
    """

    use GenServer

    alias Docket.Postgres.{Dispatcher, RunStore, Storage}

    @type option ::
            {:name, GenServer.name()}
            | {:context, Storage.ctx()}
            | {:dispatcher, GenServer.server()}
            | {:connection, keyword()}

    @repo_only_options [
      :adapter,
      :log,
      :loggers,
      :name,
      :otp_app,
      :pool,
      :pool_count,
      :pool_size,
      :priv,
      :stacktrace,
      :telemetry_prefix
    ]

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

    @impl true
    def init(opts) do
      {repo, prefix} = Storage.context!(Keyword.fetch!(opts, :context))
      dispatcher = Keyword.fetch!(opts, :dispatcher)
      connection = connection_opts(repo, Keyword.get(opts, :connection, []))

      {:ok, listener} = Postgrex.Notifications.start_link(connection)

      case Postgrex.Notifications.listen(listener, RunStore.wake_channel()) do
        {:ok, _reference} -> :ok
        {:eventually, _reference} -> :ok
      end

      {:ok, %{payload: prefix || "", dispatcher: dispatcher, listener: listener}}
    end

    @impl true
    def handle_info({:notification, _pid, _ref, channel, payload}, state) do
      if channel == RunStore.wake_channel() and payload == state.payload do
        Dispatcher.request_poll(state.dispatcher)
      end

      {:noreply, state}
    end

    def handle_info(_message, state), do: {:noreply, state}

    defp connection_opts(repo, overrides) do
      repo.config()
      |> Keyword.drop(@repo_only_options)
      |> Keyword.put(:auto_reconnect, true)
      |> Keyword.merge(overrides)
    end
  end
end
