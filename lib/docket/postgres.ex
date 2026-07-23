if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres do
    @moduledoc """
    The complete PostgreSQL backend bundle for a durable Docket runtime.

    Configure the bundle as the single durable substitution boundary. The
    application owns and supervises its Repo; Docket owns the dispatcher,
    claimed-run vehicles, optional LISTEN/NOTIFY fast path, and retention
    pruner:

        use Docket,
          backend: Docket.Postgres,
          repo: MyApp.Repo,
          pruner: [
            interval_ms: :timer.hours(1),
            event_retention_ms: :timer.hours(24 * 30),
            run_retention_ms: :timer.hours(24 * 90),
            batch_size: 1_000
          ]

    `notifier: :none` selects poll-only operation. Retention is deliberately
    explicit: starting the bundle without a complete `:pruner` policy fails
    instead of silently choosing when durable records are deleted.

    `:clock` is a testing-only, instance-owned wall-clock seam. It requires
    `testing: :inline` or `testing: :manual`, is configured only at the top
    level, and cannot be replaced by individual calls. Production dispatch,
    execution, and retention use their authoritative default clocks.

    Store modules are fixed by this bundle and are not public mix-and-match
    configuration. Public operations resolve tenant scope before calling the
    stores; only the supervised dispatcher and vehicles use `:system` scope.
    """

    use Supervisor

    @behaviour Docket.Backend

    alias Docket.Postgres.{
      AdmissionPhase,
      ClaimPolicy,
      EventStore,
      GraphStore,
      Migration,
      Notifier,
      Pruner,
      RunStore,
      Storage
    }

    @default_dispatcher [
      concurrency: 10,
      poll_interval_ms: 1_000,
      orphan_ttl_ms: 60_000,
      max_claim_attempts: 5,
      drain_timeout_ms: 30_000
    ]
    @default_execution [max_attempt_elapsed_ms: 2_000]
    @default_vehicle [drain_budget: [max_moments: 100, max_elapsed_ms: 3_000]]

    @dispatcher_keys [
      :concurrency,
      :poll_interval_ms,
      :orphan_ttl_ms,
      :max_claim_attempts,
      :drain_timeout_ms,
      :on_poisoned,
      :jitter
    ]
    @vehicle_keys [
      :monotonic_clock,
      :drain_budget,
      :jitter,
      :abandon_backoff_ms,
      :abandon_backoff_cap_ms,
      :max_claim_abandons
    ]
    @pruner_keys [
      :interval_ms,
      :event_retention_ms,
      :run_retention_ms,
      :batch_size,
      :clock
    ]
    @testing_modes [:inline, :manual]

    @impl Docket.Backend
    defdelegate transaction(ctx, fun), to: Storage

    @impl Docket.Backend
    def graphs, do: GraphStore

    @impl Docket.Backend
    def runs, do: RunStore

    @impl Docket.Backend
    def events, do: EventStore

    @impl Docket.Backend
    def context(opts) do
      repo = Keyword.fetch!(opts, :repo)
      validate_repo!(repo)
      context = %{repo: repo, prefix: Keyword.get(opts, :prefix)}
      {^repo, configured_prefix} = Storage.context!(context)
      prefix = Storage.physical_prefix!(repo, configured_prefix)
      name = Keyword.get(opts, :name)

      resolved = %{
        repo: repo,
        prefix: prefix,
        claim_policy:
          ClaimPolicy.new(Keyword.get(opts, :claim_policy, []), %{repo: repo, prefix: prefix})
      }

      if name, do: Map.put(resolved, :admission_phase, admission_phase_name(name)), else: resolved
    end

    @impl Docket.Backend
    def child_spec(opts, context) do
      name = Keyword.fetch!(opts, :name)

      %{
        id: name,
        start: {__MODULE__, :start_link, [opts, context]},
        type: :supervisor
      }
    end

    @doc false
    def child_spec(_opts) do
      raise ArgumentError,
            "Docket.Postgres requires a resolved backend context; " <>
              "start it through Docket.Runtime.Supervisor"
    end

    @doc false
    @spec start_link(keyword(), Docket.Backend.ctx()) :: Supervisor.on_start()
    def start_link(opts, context) do
      Supervisor.start_link(__MODULE__, {opts, context}, name: Keyword.fetch!(opts, :name))
    end

    @impl true
    def init({opts, context}) when is_list(opts) do
      init_with_context(opts, context)
    end

    defp init_with_context(opts, context) do
      name = Keyword.fetch!(opts, :name)
      Docket.Runtime.Config.validate_instance!(opts)
      reject_top_level_vehicle!(opts)
      validate_tenant_mode!(opts)
      validate_tenant_claim_policy!(opts, context)
      validate_testing!(opts)
      validate_wall_clock!(opts)
      reject_nested_wall_clocks!(opts)
      validate_nested!(Keyword.get(opts, :dispatcher, []), :dispatcher)
      validate_nested!(Keyword.get(opts, :vehicle, []), :vehicle)
      dispatcher = effective_dispatcher(opts)
      validate_dispatcher!(dispatcher)
      resolved_claim_policy = ClaimPolicy.resolve(context)
      validate_schema_version!(context, opts)
      execution = effective_execution(opts)
      vehicle = effective_vehicle(Keyword.get(opts, :vehicle, []))
      validate_vehicle!(vehicle)
      validate_runtime_limits!(dispatcher, execution, vehicle)

      children = children(opts, name, context)
      configure_claim_policy!(context, resolved_claim_policy)

      Supervisor.init(children, strategy: :one_for_one)
    end

    defp validate_schema_version!(%{repo: repo} = context, opts) do
      if Keyword.get(opts, :testing) in @testing_modes and
           repo.config()[:pool] == Ecto.Adapters.SQL.Sandbox do
        validate_sandbox_schema_version!(context)
      else
        validate_schema_version!(context)
      end
    end

    defp validate_schema_version!(context) do
      {repo, prefix} = Storage.context!(context)
      expected = Migration.current_version()
      actual = Migration.migrated_version(repo: repo, prefix: prefix)

      validate_schema_version!(expected, actual, prefix)
      validate_schema_shape!(Migration.current_shape?(repo, prefix), prefix)
    end

    # Inline/manual work runs through the test process's checked-out Sandbox
    # connection, which the backend child cannot borrow during its own init.
    # Schema identity is global rather than transaction-local, so validate it
    # through one short independent connection instead of weakening startup.
    defp validate_sandbox_schema_version!(%{repo: repo, prefix: prefix}) do
      connection_options =
        repo.config()
        |> Keyword.drop([:name, :pool, :pool_size])
        |> Keyword.put(:sync_connect, true)

      case Postgrex.start_link(connection_options) do
        {:ok, connection} ->
          try do
            query = """
            SELECT obj_description(pg_class.oid, 'pg_class')
            FROM pg_class
            LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE pg_class.relname = 'docket_runs' AND pg_namespace.nspname = $1
            """

            actual =
              case Postgrex.query(connection, query, [prefix]) do
                {:ok, %{rows: [[version]]}} when is_binary(version) ->
                  String.to_integer(version)

                _missing_or_uncommented ->
                  0
              end

            validate_schema_version!(Migration.current_version(), actual, prefix)

            current_shape =
              case Postgrex.query(connection, Migration.current_shape_query(), [prefix]) do
                {:ok, %{rows: [[true]]}} -> true
                _missing_or_unexpected -> false
              end

            validate_schema_shape!(current_shape, prefix)
          after
            GenServer.stop(connection)
          end

        {:error, reason} ->
          raise ArgumentError,
                "Docket.Postgres could not validate its SQL Sandbox schema: #{inspect(reason)}"
      end
    end

    defp validate_schema_version!(expected, actual, prefix) do
      if actual != expected do
        raise ArgumentError,
              "Docket.Postgres requires schema version #{expected}, found #{actual} in prefix " <>
                "#{inspect(prefix)}; stop all Docket writers, run the generated migration, " <>
                "and restart one homogeneous application version"
      end
    end

    defp validate_schema_shape!(true, _prefix), do: :ok

    defp validate_schema_shape!(false, prefix) do
      raise ArgumentError,
            "Docket.Postgres found schema version #{Migration.current_version()} in prefix " <>
              "#{inspect(prefix)}, but its structure does not match the current Docket schema; " <>
              "recreate the unreleased development schema from the current generated migration"
    end

    defp configure_claim_policy!(context, claim_policy) do
      if ClaimPolicy.configures_on_startup?(claim_policy) do
        %{repo: repo} = context

        result =
          ClaimPolicy.configure(claim_policy, context, fn statement, params ->
            repo.query(statement, params, log: false)
          end)

        case result do
          :ok ->
            :ok

          {:error, reason} ->
            raise ArgumentError,
                  "Docket.Postgres could not persist its configured claim policy before " <>
                    "startup: #{inspect(reason)}"
        end
      end
    end

    defp children(opts, name, context) do
      if Keyword.get(opts, :testing) in @testing_modes do
        [{AdmissionPhase, name: admission_phase_name(name)}]
      else
        dispatcher = dispatcher_name(name)

        [
          {Docket.Postgres.Runner,
           name: runner_name(name),
           dispatcher: dispatcher,
           vehicle_supervisor: vehicle_supervisor_name(name),
           context: context,
           host_opts: opts}
        ] ++
          notifier_children(opts, name, context, dispatcher) ++
          [pruner_child(opts, name, context)]
      end
    end

    @doc "Synchronously claims and drains due runs in the calling process."
    @impl Docket.Backend
    def drain_runs(context, opts) when is_list(opts) do
      reject_top_level_vehicle!(opts)
      validate_testing!(opts)
      validate_wall_clock!(opts)
      reject_nested_wall_clocks!(opts)
      validate_nested!(Keyword.get(opts, :dispatcher, []), :dispatcher)
      validate_nested!(Keyword.get(opts, :vehicle, []), :vehicle)
      dispatcher_config = effective_dispatcher(opts)
      execution_config = effective_execution(opts)
      vehicle_config = effective_vehicle(Keyword.get(opts, :vehicle, []))
      validate_dispatcher!(dispatcher_config)
      validate_vehicle!(vehicle_config)
      validate_runtime_limits!(dispatcher_config, execution_config, vehicle_config)

      max_runs = Keyword.get(opts, :max_runs, 100)

      cond do
        opts[:testing] not in @testing_modes ->
          {:error, :testing_mode_required}

        not (is_integer(max_runs) and max_runs > 0) ->
          {:error, :invalid_max_runs}

        true ->
          dispatcher = dispatcher_config
          vehicle = testing_vehicle_opts(opts, context)

          drain_due(
            context,
            Map.fetch!(context, :admission_phase),
            dispatcher,
            vehicle,
            max_runs,
            %{
              drained: 0,
              poisoned: [],
              outcomes: [],
              limit_reached: false
            }
          )
      end
    end

    defp drain_due(_context, _phase, _dispatcher, _vehicle, 0, summary),
      do: {:ok, %{summary | limit_reached: true}}

    defp drain_due(context, phase, dispatcher, vehicle, remaining, summary) do
      now = Keyword.fetch!(dispatcher, :clock).()

      result =
        AdmissionPhase.run(phase, 1, fn preference ->
          RunStore.claim_due(context, :system, %{
            now: now,
            limit: 1,
            orphan_ttl_ms: Keyword.fetch!(dispatcher, :orphan_ttl_ms),
            max_claim_attempts: Keyword.fetch!(dispatcher, :max_claim_attempts),
            preference: preference
          })
        end)

      case result do
        {:ok, %{leases: [], poisoned: []}} ->
          {:ok, summary}

        {:ok, %{leases: [lease], poisoned: poisoned}} ->
          outcome = Docket.Postgres.Vehicle.drain(lease, vehicle)

          next = %{
            summary
            | drained: summary.drained + 1,
              poisoned: summary.poisoned ++ poisoned,
              outcomes: summary.outcomes ++ [outcome]
          }

          case outcome do
            {:ok, {:parked, _kind}} ->
              drain_due(context, phase, dispatcher, vehicle, remaining - 1, next)

            {:ok, reason} ->
              {:error, {:drain_stopped, reason, next}}
          end

        {:ok, %{leases: [], poisoned: poisoned}} ->
          drain_due(
            context,
            phase,
            dispatcher,
            vehicle,
            remaining - 1,
            %{summary | poisoned: summary.poisoned ++ poisoned}
          )

        {:error, reason} ->
          {:error, reason}
      end
    end

    defp testing_vehicle_opts(opts, context) do
      effective_execution(opts)
      |> Keyword.merge(effective_vehicle(Keyword.get(opts, :vehicle, [])))
      |> Keyword.put(:graph_cache, false)
      |> Keyword.put(:task_supervisor, Keyword.fetch!(opts, :task_supervisor))
      |> Keyword.put(:backend, {__MODULE__, context})
    end

    @doc false
    def runner_name(name), do: Module.concat(name, "Runner")

    @doc false
    def dispatcher_name(name), do: Module.concat(name, "Dispatcher")

    @doc false
    def vehicle_supervisor_name(name), do: Module.concat(name, "VehicleSupervisor")

    @doc false
    def admission_phase_name(name), do: Module.concat(name, "AdmissionPhase")

    @doc false
    def notifier_name(name), do: Module.concat(name, "Notifier")

    @doc false
    def pruner_name(name), do: Module.concat(name, "Pruner")

    defp notifier_children(opts, name, context, dispatcher) do
      case Keyword.get(opts, :notifier, []) do
        :none ->
          []

        notifier_opts when is_list(notifier_opts) ->
          assert_keyword!(notifier_opts, :notifier)
          assert_known_keys!(notifier_opts, [:connection], :notifier)
          connection = Keyword.get(notifier_opts, :connection, [])
          assert_keyword!(connection, :connection)

          [
            {Notifier,
             name: notifier_name(name),
             context: context,
             dispatcher: dispatcher,
             connection: connection}
          ]

        other ->
          raise ArgumentError,
                ":notifier must be :none or a keyword list, got: #{inspect(other)}"
      end
    end

    defp pruner_child(opts, name, context) do
      pruner_opts = Keyword.get(opts, :pruner)
      assert_keyword!(pruner_opts, :pruner)
      assert_known_keys!(pruner_opts, @pruner_keys, :pruner)

      required = [:interval_ms, :event_retention_ms, :run_retention_ms, :batch_size]
      missing = Enum.reject(required, &Keyword.has_key?(pruner_opts, &1))

      if missing != [] do
        raise ArgumentError,
              ":pruner requires #{Enum.map_join(missing, ", ", &inspect/1)}"
      end

      validate_pruner!(pruner_opts)

      {Pruner, Keyword.merge(pruner_opts, name: pruner_name(name), context: context)}
    end

    defp validate_dispatcher!(opts) do
      Enum.each([:concurrency, :poll_interval_ms, :max_claim_attempts], fn key ->
        value = Keyword.fetch!(opts, key)

        unless is_integer(value) and value > 0 do
          raise ArgumentError, ":dispatcher #{key} must be a positive integer"
        end
      end)

      Enum.each([:orphan_ttl_ms, :drain_timeout_ms], fn key ->
        value = Keyword.fetch!(opts, key)

        unless is_integer(value) and value >= 0 do
          raise ArgumentError, ":dispatcher #{key} must be a non-negative integer"
        end
      end)

      for {key, arity} <- [on_poisoned: 1, clock: 0, jitter: 1],
          callback = Keyword.get(opts, key),
          callback != nil and not is_function(callback, arity) do
        raise ArgumentError, ":dispatcher #{key} must be a function of arity #{arity}"
      end
    end

    defp validate_pruner!(opts) do
      Enum.each([:interval_ms, :batch_size], fn key ->
        value = Keyword.fetch!(opts, key)

        unless is_integer(value) and value > 0 do
          raise ArgumentError, ":pruner #{key} must be a positive integer"
        end
      end)

      Enum.each([:event_retention_ms, :run_retention_ms], fn key ->
        value = Keyword.fetch!(opts, key)

        unless is_integer(value) and value >= 0 do
          raise ArgumentError, ":pruner #{key} must be a non-negative integer"
        end
      end)

      if opts[:event_retention_ms] > opts[:run_retention_ms] do
        raise ArgumentError, ":pruner event retention must not exceed run retention"
      end

      case Keyword.get(opts, :clock) do
        nil -> :ok
        clock when is_function(clock, 0) -> :ok
        _ -> raise ArgumentError, ":pruner clock must be a zero-argument function"
      end
    end

    defp validate_tenant_mode!(opts) do
      case Keyword.get(opts, :tenant_mode, :none) do
        mode when mode in [:none, :required] ->
          :ok

        mode ->
          raise ArgumentError, ":tenant_mode must be :none or :required, got: #{inspect(mode)}"
      end
    end

    defp validate_tenant_claim_policy!(opts, context) do
      if Keyword.get(opts, :tenant_mode, :none) == :required do
        implementation = context |> ClaimPolicy.resolve() |> ClaimPolicy.implementation()

        unless implementation == Docket.Postgres.ClaimPolicy.WindowedInterleave do
          raise ArgumentError,
                ":tenant_mode :required requires the WindowedInterleave claim policy"
        end
      end

      :ok
    end

    defp validate_testing!(opts) do
      case Keyword.get(opts, :testing) do
        nil -> :ok
        mode when mode in @testing_modes -> :ok
        mode -> raise ArgumentError, ":testing must be :inline or :manual, got: #{inspect(mode)}"
      end
    end

    defp validate_wall_clock!(opts) do
      case Keyword.get(opts, :clock) do
        nil ->
          :ok

        clock when is_function(clock, 0) ->
          if Keyword.get(opts, :testing) in @testing_modes do
            :ok
          else
            raise ArgumentError,
                  ":clock is a testing-only option and requires testing: :inline or :manual"
          end

        other ->
          raise ArgumentError,
                ":clock must be a zero-argument function, got: #{inspect(other)}"
      end
    end

    defp reject_nested_wall_clocks!(opts) do
      nested =
        for key <- [:dispatcher, :vehicle, :pruner],
            value = Keyword.get(opts, key),
            Keyword.keyword?(value),
            Keyword.has_key?(value, :clock),
            do: key

      if nested != [] do
        raise ArgumentError,
              ":clock is instance-owned; configure it once at the top level, not under " <>
                Enum.map_join(nested, ", ", &inspect/1)
      end
    end

    defp validate_repo!(repo) do
      valid? =
        is_atom(repo) and Code.ensure_loaded?(repo) and function_exported?(repo, :__adapter__, 0) and
          function_exported?(repo, :config, 0) and repo.__adapter__() == Ecto.Adapters.Postgres

      unless valid? do
        raise ArgumentError, ":repo must be an Ecto PostgreSQL Repo, got: #{inspect(repo)}"
      end
    end

    defp validate_vehicle!(vehicle) do
      for {key, arity} <- [monotonic_clock: 0, jitter: 1],
          callback = Keyword.get(vehicle, key),
          callback != nil and not is_function(callback, arity) do
        raise ArgumentError, ":vehicle #{key} must be a function of arity #{arity}"
      end

      for key <- [
            :abandon_backoff_ms,
            :abandon_backoff_cap_ms,
            :max_claim_abandons
          ],
          value = Keyword.get(vehicle, key),
          value != nil and not (is_integer(value) and value > 0) do
        raise ArgumentError, ":vehicle #{key} must be a positive integer"
      end
    end

    defp validate_runtime_limits!(dispatcher, execution, vehicle) do
      maximum = Keyword.fetch!(execution, :max_attempt_elapsed_ms)
      budget = Docket.Postgres.Vehicle.drain_budget!(vehicle)
      elapsed = budget.max_elapsed_ms
      orphan_ttl = Keyword.fetch!(dispatcher, :orphan_ttl_ms)

      unless is_integer(maximum) and maximum > 0 do
        raise ArgumentError, ":vehicle max_attempt_elapsed_ms must be a positive finite integer"
      end

      unless is_integer(elapsed) and maximum <= elapsed do
        raise ArgumentError,
              ":vehicle drain_budget max_elapsed_ms must be finite and at least max_attempt_elapsed_ms"
      end

      unless elapsed < orphan_ttl do
        raise ArgumentError,
              ":vehicle drain_budget max_elapsed_ms must leave headroom below dispatcher orphan_ttl_ms"
      end

      _abandon_backoff = Docket.Postgres.Vehicle.abandon_backoff!(vehicle)
    end

    defp effective_execution(host_opts),
      do:
        @default_execution
        |> Keyword.merge(Keyword.take(host_opts, Docket.Runtime.Config.instance_keys()))

    @doc false
    def effective_dispatcher(host_opts) do
      @default_dispatcher
      |> Keyword.merge(Keyword.get(host_opts, :dispatcher, []))
      |> Keyword.put(:clock, Docket.Runtime.Clock.wall_clock(host_opts))
    end

    defp effective_vehicle(nested), do: Keyword.merge(@default_vehicle, nested)

    defp reject_top_level_vehicle!(opts) do
      case Enum.filter(@vehicle_keys, &Keyword.has_key?(opts, &1)) do
        [] ->
          :ok

        keys ->
          raise ArgumentError,
                "Postgres vehicle mechanics must be configured under :vehicle, not at the " <>
                  "top level: #{Enum.map_join(keys, ", ", &inspect/1)}"
      end
    end

    defp assert_keyword!(value, key) do
      unless Keyword.keyword?(value) do
        raise ArgumentError, "#{inspect(key)} must be a keyword list, got: #{inspect(value)}"
      end
    end

    defp assert_known_keys!(opts, allowed, key) do
      case Keyword.keys(opts) -- allowed do
        [] -> :ok
        unknown -> raise ArgumentError, "#{inspect(key)} has unknown keys: #{inspect(unknown)}"
      end
    end

    defmodule Runner do
      @moduledoc false

      use Supervisor

      def start_link(opts) do
        Supervisor.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
      end

      @impl true
      def init(opts) do
        host_opts = Keyword.fetch!(opts, :host_opts)
        context = Keyword.fetch!(opts, :context)
        dispatcher_name = Keyword.fetch!(opts, :dispatcher)
        vehicle_supervisor = Keyword.fetch!(opts, :vehicle_supervisor)

        dispatcher_opts = Docket.Postgres.effective_dispatcher(host_opts)

        vehicle_opts =
          Docket.Postgres.default_execution()
          |> Keyword.merge(Keyword.take(host_opts, Docket.Runtime.Config.instance_keys()))
          |> Keyword.merge(nested_opts!(host_opts, :vehicle, Docket.Postgres.default_vehicle()))
          |> Keyword.merge(
            backend: {Docket.Postgres, context},
            task_supervisor: vehicle_supervisor
          )

        executor_opts =
          vehicle_opts
          |> Keyword.get(:executor_opts, [])
          |> Keyword.put_new(:task_supervisor, vehicle_supervisor)

        vehicle_opts = Keyword.put(vehicle_opts, :executor_opts, executor_opts)

        launch = Docket.Postgres.Vehicle.launcher(vehicle_opts)

        dispatcher_opts =
          Keyword.merge(dispatcher_opts,
            name: dispatcher_name,
            context: context,
            launch: launch
          )

        children = [
          {Docket.Postgres.AdmissionPhase, name: Map.fetch!(context, :admission_phase)},
          {Task.Supervisor, name: vehicle_supervisor},
          {Docket.Postgres.Dispatcher, dispatcher_opts}
        ]

        # Admission phase, dispatcher, and vehicles are one accounting unit.
        # If any boundary fails, replace the unit so claim preference and
        # in-flight vehicle accounting cannot diverge.
        Supervisor.init(children, strategy: :one_for_all)
      end

      defp nested_opts!(host_opts, key, defaults) do
        value = Keyword.get(host_opts, key, [])
        Docket.Postgres.validate_nested!(value, key)
        Keyword.merge(defaults, value)
      end
    end

    @doc false
    def default_dispatcher, do: @default_dispatcher

    @doc false
    def default_execution, do: @default_execution

    @doc false
    def default_vehicle, do: @default_vehicle

    @doc false
    def vehicle_keys, do: @vehicle_keys

    @doc false
    def validate_nested!(opts, :dispatcher) do
      assert_keyword!(opts, :dispatcher)
      assert_known_keys!(opts, @dispatcher_keys, :dispatcher)
    end

    def validate_nested!(opts, :vehicle) do
      assert_keyword!(opts, :vehicle)
      assert_known_keys!(opts, @vehicle_keys, :vehicle)
    end
  end
end
