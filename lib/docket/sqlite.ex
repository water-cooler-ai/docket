if Code.ensure_loaded?(Exqlite.Sqlite3) do
  defmodule Docket.SQLite do
    @moduledoc """
    A single-node SQLite backend for a durable Docket runtime.

        use Docket,
          backend: {Docket.SQLite, database: "priv/docket.db"}

    The host declares `{:exqlite, "~> 0.27"}`; nothing else is required. The
    backend creates its tables on first start (versioned with SQLite's
    `user_version`), so there is no migration to generate. Give Docket its own
    database file rather than sharing one with an Ecto repo.

    ## Why this is simpler than `Docket.Postgres`

    One process owns one SQLite connection, and every store operation runs in
    it (`Docket.SQLite.Connection`). Writes are serialized, so:

      * claiming is a plain select-then-update inside `BEGIN IMMEDIATE`, with no
        `SKIP LOCKED`, advisory locks, or claim-policy admission phases;
      * wakeups are in-process messages to `Docket.SQLite.Runner` instead of
        LISTEN/NOTIFY;
      * on boot the runner releases claims left by a previous process instead
        of waiting for the orphan TTL.

    Claim tokens and checkpoint fences still guard every commit, because a
    vehicle can crash or outlive its claim.

    ## Limits

      * One BEAM node writes the file. Running two runtimes against one file
        is unsupported.
      * Throughput is bounded by one connection; node execution still runs
        concurrently, only the commits queue.
      * There is no tenant-fair claim policy: `tenant_mode: :required` scopes
        data by tenant, but claims are oldest-first across tenants.
      * There is no retention pruner yet; events and terminal runs are kept.

    ## Options

      * `:database` (required) - a file path, or `":memory:"` for tests.
      * `:synchronous` - `:normal` (default, WAL-safe) or `:full`, `:extra`,
        `:off`.
      * `:busy_timeout_ms` - for external readers holding the file (5000).
      * `:runner` - `:concurrency` (10), `:poll_interval_ms` (1000),
        `:orphan_ttl_ms` (60000), `:max_claim_attempts` (5), `:on_poisoned`.
      * `:vehicle` - `:drain_budget` (`[max_moments: 100,
        max_elapsed_ms: 3000]`), `:abandon_backoff_ms`,
        `:abandon_backoff_cap_ms`, `:max_claim_abandons`.

    `testing: :inline | :manual` starts only the connection; runs advance
    through `drain_runs/2`, as with `Docket.Postgres`.
    """

    use Supervisor

    @behaviour Docket.Backend

    alias Docket.SQLite.{Connection, Runner, Store, Vehicle}

    @default_runner [
      concurrency: 10,
      poll_interval_ms: 1_000,
      orphan_ttl_ms: 60_000,
      max_claim_attempts: 5
    ]
    @default_execution [max_attempt_elapsed_ms: 2_000]
    @default_vehicle [drain_budget: [max_moments: 100, max_elapsed_ms: 3_000]]
    @runner_keys [
      :concurrency,
      :poll_interval_ms,
      :orphan_ttl_ms,
      :max_claim_attempts,
      :on_poisoned
    ]
    @vehicle_keys [
      :drain_budget,
      :abandon_backoff_ms,
      :abandon_backoff_cap_ms,
      :max_claim_abandons,
      :monotonic_clock,
      :jitter
    ]
    @testing_modes [:inline, :manual]

    @impl Docket.Backend
    def capabilities do
      %{contract_version: 2, transitions: %{version: Docket.Backend.TransitionStore.version()}}
    end

    @impl Docket.Backend
    def transitions, do: Store

    @impl Docket.Backend
    def graphs, do: Store

    @impl Docket.Backend
    def runs, do: Store

    @impl Docket.Backend
    def events, do: Store

    @impl Docket.Backend
    def context(opts) do
      name = Keyword.fetch!(opts, :name)

      %{
        conn: connection_name(name),
        runner:
          if(Keyword.get(opts, :testing) in @testing_modes, do: nil, else: runner_name(name)),
        clock: clock(opts),
        token_generator: &generate_token/0
      }
    end

    @impl Docket.Backend
    def child_spec(opts, context) do
      %{
        id: Keyword.fetch!(opts, :name),
        start: {__MODULE__, :start_link, [opts, context]},
        type: :supervisor
      }
    end

    @doc false
    def start_link(opts, context) do
      Supervisor.start_link(__MODULE__, {opts, context}, name: Keyword.fetch!(opts, :name))
    end

    @impl Supervisor
    def init({opts, context}) do
      Docket.Runtime.Config.validate_instance!(opts)
      validate!(opts)
      name = Keyword.fetch!(opts, :name)

      connection =
        {Connection,
         Keyword.take(opts, [:database, :synchronous, :busy_timeout_ms])
         |> Keyword.put(:name, context.conn)}

      children =
        if Keyword.get(opts, :testing) in @testing_modes do
          [connection]
        else
          vehicle_supervisor = Module.concat(name, VehicleSupervisor)
          runner = Keyword.merge(@default_runner, Keyword.get(opts, :runner, []))

          [
            connection,
            {Task.Supervisor, name: vehicle_supervisor},
            {Runner,
             runner
             |> Keyword.merge(
               name: context.runner,
               context: context,
               clock: context.clock,
               task_supervisor: vehicle_supervisor,
               vehicle_opts: vehicle_opts(opts, context, vehicle_supervisor)
             )}
          ]
        end

      # The runner, its vehicles, and the connection fail together: a lost
      # connection invalidates every in-flight vehicle's commit path.
      Supervisor.init(children, strategy: :one_for_all)
    end

    @doc "Synchronously claims and drains due runs in the calling process (testing modes only)."
    @impl Docket.Backend
    def drain_runs(context, opts) when is_list(opts) do
      max_runs = Keyword.get(opts, :max_runs, 100)
      runner = Keyword.merge(@default_runner, Keyword.get(opts, :runner, []))

      cond do
        opts[:testing] not in @testing_modes ->
          {:error, :testing_mode_required}

        not (is_integer(max_runs) and max_runs > 0) ->
          {:error, :invalid_max_runs}

        true ->
          vehicle = vehicle_opts(opts, context, Keyword.fetch!(opts, :task_supervisor))

          drain_due(context, runner, vehicle, max_runs, %{
            drained: 0,
            poisoned: [],
            outcomes: [],
            limit_reached: false
          })
      end
    end

    defp drain_due(_context, _runner, _vehicle, 0, summary),
      do: {:ok, %{summary | limit_reached: true}}

    defp drain_due(context, runner, vehicle, remaining, summary) do
      result =
        Store.claim_due(context, :system, %{
          now: context.clock.(),
          limit: 1,
          orphan_ttl_ms: Keyword.fetch!(runner, :orphan_ttl_ms),
          max_claim_attempts: Keyword.fetch!(runner, :max_claim_attempts)
        })

      case result do
        {:ok, %{leases: [], poisoned: []}} ->
          {:ok, summary}

        {:ok, %{leases: [lease], poisoned: poisoned}} ->
          outcome = Vehicle.drain(lease, vehicle)

          summary = %{
            summary
            | drained: summary.drained + 1,
              poisoned: summary.poisoned ++ poisoned,
              outcomes: summary.outcomes ++ [outcome]
          }

          case outcome do
            {:ok, {:parked, _kind}} -> drain_due(context, runner, vehicle, remaining - 1, summary)
            {:ok, reason} -> {:error, {:drain_stopped, reason, summary}}
          end

        {:ok, %{leases: [], poisoned: poisoned}} ->
          drain_due(context, runner, vehicle, remaining - 1, %{
            summary
            | poisoned: summary.poisoned ++ poisoned
          })
      end
    end

    defp vehicle_opts(opts, context, task_supervisor) do
      vehicle = Keyword.merge(@default_vehicle, Keyword.get(opts, :vehicle, []))

      vehicle_opts =
        @default_execution
        |> Keyword.merge(Keyword.take(opts, Docket.Runtime.Config.instance_keys()))
        |> Keyword.merge(vehicle)
        |> Keyword.merge(backend: {__MODULE__, context}, task_supervisor: task_supervisor)

      executor_opts =
        vehicle_opts
        |> Keyword.get(:executor_opts, [])
        |> Keyword.put_new(:task_supervisor, task_supervisor)

      Keyword.put(vehicle_opts, :executor_opts, executor_opts)
    end

    defp validate!(opts) do
      database = Keyword.get(opts, :database)

      unless is_binary(database) and database != "" do
        raise ArgumentError, "Docket.SQLite requires a :database path (or \":memory:\")"
      end

      case Keyword.get(opts, :tenant_mode, :none) do
        mode when mode in [:none, :required] ->
          :ok

        mode ->
          raise ArgumentError, ":tenant_mode must be :none or :required, got: #{inspect(mode)}"
      end

      if Keyword.has_key?(opts, :clock) and Keyword.get(opts, :testing) not in @testing_modes do
        raise ArgumentError,
              ":clock is a testing-only option and requires testing: :inline or :manual"
      end

      known!(Keyword.get(opts, :runner, []), @runner_keys, :runner)
      known!(Keyword.get(opts, :vehicle, []), @vehicle_keys, :vehicle)

      runner = Keyword.merge(@default_runner, Keyword.get(opts, :runner, []))

      for key <- [:concurrency, :poll_interval_ms, :max_claim_attempts],
          not (is_integer(runner[key]) and runner[key] > 0) do
        raise ArgumentError, ":runner #{key} must be a positive integer"
      end

      budget = Keyword.merge(@default_vehicle, Keyword.get(opts, :vehicle, []))[:drain_budget]
      elapsed = Keyword.get(budget, :max_elapsed_ms, :infinity)

      attempt =
        Keyword.get(opts, :max_attempt_elapsed_ms, @default_execution[:max_attempt_elapsed_ms])

      unless is_integer(elapsed) and attempt <= elapsed and elapsed < runner[:orphan_ttl_ms] do
        raise ArgumentError,
              ":vehicle drain_budget max_elapsed_ms must be finite, at least " <>
                "max_attempt_elapsed_ms, and below :runner orphan_ttl_ms"
      end

      :ok
    end

    defp known!(opts, allowed, key) do
      unless Keyword.keyword?(opts) do
        raise ArgumentError, "#{inspect(key)} must be a keyword list, got: #{inspect(opts)}"
      end

      case Keyword.keys(opts) -- allowed do
        [] -> :ok
        unknown -> raise ArgumentError, "#{inspect(key)} has unknown keys: #{inspect(unknown)}"
      end
    end

    defp clock(opts) do
      wall = Docket.Runtime.Clock.wall_clock(opts)
      fn -> Docket.Runtime.Clock.normalize!(wall.()) end
    end

    defp generate_token,
      do: 18 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)

    @doc false
    def connection_name(name), do: Module.concat(name, Connection)

    @doc false
    def runner_name(name), do: Module.concat(name, Runner)
  end
end
