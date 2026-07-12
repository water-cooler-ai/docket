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

    Store modules are fixed by this bundle and are not public mix-and-match
    configuration. Public operations resolve tenant scope before calling the
    stores; only the supervised dispatcher and vehicles use `:system` scope.
    """

    use Supervisor

    @behaviour Docket.Backend

    alias Docket.Postgres.{EventStore, GraphStore, Notifier, Pruner, RunStore, Storage}

    @default_dispatcher [
      concurrency: 10,
      poll_interval_ms: 1_000,
      orphan_ttl_ms: 60_000,
      max_claim_attempts: 5,
      drain_timeout_ms: 30_000
    ]
    @default_vehicle [drain_budget: [max_moments: 100, max_elapsed_ms: 1_000]]

    @dispatcher_keys [
      :concurrency,
      :poll_interval_ms,
      :orphan_ttl_ms,
      :max_claim_attempts,
      :drain_timeout_ms,
      :on_poisoned,
      :clock,
      :jitter
    ]
    @vehicle_keys [
      :clock,
      :monotonic_clock,
      :drain_budget,
      :heartbeat,
      :jitter,
      :abandon_backoff_ms,
      :max_claim_abandons,
      :executor,
      :executor_opts,
      :max_supersteps,
      :context,
      :id_generator,
      :checkpoint_observers
    ]
    @pruner_keys [
      :interval_ms,
      :event_retention_ms,
      :run_retention_ms,
      :batch_size,
      :clock
    ]

    @impl Docket.Backend
    def storage, do: Storage

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
      {^repo, prefix} = Storage.context!(context)
      %{repo: repo, prefix: prefix}
    end

    @impl Docket.Backend
    def child_spec(opts) do
      name = Keyword.fetch!(opts, :name)

      %{
        id: name,
        start: {__MODULE__, :start_link, [opts]},
        type: :supervisor
      }
    end

    @spec start_link(keyword()) :: Supervisor.on_start()
    def start_link(opts) do
      Supervisor.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
    end

    @impl true
    def init(opts) do
      name = Keyword.fetch!(opts, :name)
      validate_tenant_mode!(opts)
      validate_observers!(opts)
      validate_nested!(Keyword.get(opts, :dispatcher, []), :dispatcher)
      validate_nested!(Keyword.get(opts, :vehicle, []), :vehicle)
      validate_dispatcher!(Keyword.merge(@default_dispatcher, Keyword.get(opts, :dispatcher, [])))
      validate_vehicle!(opts, Keyword.get(opts, :vehicle, []))

      context = context(opts)
      runner = runner_name(name)
      dispatcher = dispatcher_name(name)

      children =
        [
          {Docket.Postgres.Runner,
           name: runner,
           dispatcher: dispatcher,
           vehicle_supervisor: vehicle_supervisor_name(name),
           context: context,
           host_opts: opts}
        ] ++
          notifier_children(opts, name, context, dispatcher) ++
          [pruner_child(opts, name, context)]

      Supervisor.init(children, strategy: :one_for_one)
    end

    @doc false
    def runner_name(name), do: Module.concat(name, "Runner")

    @doc false
    def dispatcher_name(name), do: Module.concat(name, "Dispatcher")

    @doc false
    def vehicle_supervisor_name(name), do: Module.concat(name, "VehicleSupervisor")

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

    defp validate_repo!(repo) do
      valid? =
        is_atom(repo) and Code.ensure_loaded?(repo) and function_exported?(repo, :__adapter__, 0) and
          function_exported?(repo, :config, 0) and repo.__adapter__() == Ecto.Adapters.Postgres

      unless valid? do
        raise ArgumentError, ":repo must be an Ecto PostgreSQL Repo, got: #{inspect(repo)}"
      end
    end

    defp validate_observers!(opts) do
      validate_observer_modules!(Keyword.get(opts, :checkpoint_observers, []))
    end

    defp validate_observer_modules!(observers) do
      observers = List.wrap(observers)

      Enum.each(observers, fn observer ->
        unless is_atom(observer) and Code.ensure_loaded?(observer) and
                 function_exported?(observer, :observe, 2) do
          raise ArgumentError,
                ":checkpoint_observers must implement observe/2, got: #{inspect(observer)}"
        end
      end)
    end

    defp validate_vehicle!(host_opts, vehicle_opts) do
      effective =
        host_opts
        |> Keyword.take(@vehicle_keys)
        |> Keyword.merge(vehicle_opts)

      for {key, arity} <- [clock: 0, monotonic_clock: 0, jitter: 1, id_generator: 1],
          callback = Keyword.get(effective, key),
          callback != nil and not is_function(callback, arity) do
        raise ArgumentError, ":vehicle #{key} must be a function of arity #{arity}"
      end

      for key <- [:max_supersteps, :abandon_backoff_ms, :max_claim_abandons],
          value = Keyword.get(effective, key),
          value != nil and not (is_integer(value) and value > 0) do
        raise ArgumentError, ":vehicle #{key} must be a positive integer"
      end

      case Keyword.get(effective, :context) do
        nil -> :ok
        context when is_map(context) -> :ok
        other -> raise ArgumentError, ":vehicle context must be a map, got: #{inspect(other)}"
      end

      case Keyword.get(effective, :executor) do
        nil ->
          :ok

        executor when is_atom(executor) ->
          unless Code.ensure_loaded?(executor) and function_exported?(executor, :execute, 6) do
            raise ArgumentError, ":vehicle executor must implement execute/6"
          end

        _ ->
          raise ArgumentError, ":vehicle executor must implement execute/6"
      end

      validate_observer_modules!(Keyword.get(effective, :checkpoint_observers, []))
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

        dispatcher_opts =
          nested_opts!(host_opts, :dispatcher, Docket.Postgres.default_dispatcher())

        vehicle_opts = nested_opts!(host_opts, :vehicle, Docket.Postgres.default_vehicle())

        vehicle_opts =
          host_opts
          |> Keyword.take(Docket.Postgres.vehicle_keys())
          |> Keyword.merge(vehicle_opts)
          |> Keyword.merge(
            backend: {Docket.Postgres, context},
            task_supervisor: vehicle_supervisor
          )

        launch = Docket.Postgres.Vehicle.launcher(vehicle_opts)

        dispatcher_opts =
          Keyword.merge(dispatcher_opts,
            name: dispatcher_name,
            context: context,
            launch: launch
          )

        children = [
          {Task.Supervisor, name: vehicle_supervisor},
          {Docket.Postgres.Dispatcher, dispatcher_opts}
        ]

        # Dispatcher and its vehicles are one accounting unit. If either
        # supervisor boundary fails, terminate both so a replacement
        # dispatcher never loses track of still-running vehicles.
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
