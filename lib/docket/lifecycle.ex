defmodule Docket.Lifecycle do
  @moduledoc false

  require Logger

  alias Docket.Runtime.Moment

  @type backend :: {module(), Docket.Storage.ctx()}

  @spec start(backend(), Docket.Storage.owner_scope(), Moment.t()) ::
          {:ok, Moment.t()} | {:error, term()}
  def start({backend, context}, scope, %Moment{} = moment) do
    storage = backend.storage()
    runs = backend.runs()
    events = backend.events()

    Docket.Telemetry.lifecycle_span(:start, fn ->
      storage.transaction(context, fn tx ->
        with {:ok, _run} <-
               store_span(:run_insert, fn ->
                 runs.insert_run(
                   tx,
                   scope,
                   moment.run,
                   moment.checkpoint_type,
                   start_wake_at(moment)
                 )
               end),
             :ok <-
               store_span(
                 :event_append,
                 fn ->
                   events.append_events(tx, scope, moment.run.id, moment.events)
                 end
               ) do
          {:ok, moment}
        end
      end)
    end)
  end

  @spec commit_moment(
          backend(),
          Docket.Storage.scope(),
          Moment.t(),
          non_neg_integer(),
          Docket.Storage.Runs.claim_token()
        ) :: {:ok, Moment.t()} | {:error, term()}
  def commit_moment(
        {backend, context},
        scope,
        %Moment{} = moment,
        expected_checkpoint_seq,
        claim_token
      ) do
    storage = backend.storage()
    runs = backend.runs()
    events = backend.events()

    proposal = %{
      run: moment.run,
      expected_checkpoint_seq: expected_checkpoint_seq,
      claim_token: claim_token,
      checkpoint_type: moment.checkpoint_type,
      schedule: schedule(moment.disposition, :claimed)
    }

    Docket.Telemetry.lifecycle_span(:moment, fn ->
      storage.transaction(context, fn tx ->
        with {:ok, _run} <- store_span(:run_commit, fn -> runs.commit(tx, scope, proposal) end),
             :ok <-
               store_span(
                 :event_append,
                 fn ->
                   events.append_events(tx, scope, moment.run.id, moment.events)
                 end
               ) do
          {:ok, moment}
        end
      end)
    end)
  end

  @spec signal(
          backend(),
          Docket.Storage.scope(),
          String.t(),
          (Docket.Run.t() ->
             {:ok, Moment.t()} | {:unchanged, Docket.Run.t()} | {:error, term()})
        ) :: {:ok, Moment.t() | Docket.Run.t()} | {:error, term()}
  def signal({backend, context}, scope, run_id, mutation) when is_function(mutation, 1) do
    storage = backend.storage()
    runs = backend.runs()
    events = backend.events()

    Docket.Telemetry.lifecycle_span(:signal, fn ->
      storage.transaction(context, fn tx ->
        case store_span(:run_mutation, fn ->
               runs.mutate_run(tx, scope, run_id, fn run ->
                 mutation_decision(mutation.(run))
               end)
             end) do
          {:ok, {:committed, %Moment{} = moment}} ->
            with :ok <-
                   store_span(
                     :event_append,
                     fn ->
                       events.append_events(tx, scope, run_id, moment.events)
                     end
                   ) do
              {:ok, moment}
            end

          {:ok, {:unchanged, %Docket.Run{} = run}} ->
            {:ok, run}

          {:error, reason} ->
            {:error, reason}
        end
      end)
    end)
  end

  @doc false
  @spec schedule(Moment.disposition(), :claimed | :unclaimed) :: Docket.Storage.Runs.schedule()
  def schedule(:continue, :claimed), do: :retain_claim
  def schedule(:continue, :unclaimed), do: {:release_claim, :immediate}
  def schedule({:park, :immediate, _reason}, _claim), do: {:release_claim, :immediate}
  def schedule({:park, :external, _reason}, _claim), do: {:release_claim, :external}

  def schedule({:park, {:at, %DateTime{} = at}, _reason}, _claim),
    do: {:release_claim, {:at, at}}

  def schedule({:park, :terminal, _reason}, _claim), do: {:release_claim, :terminal}

  @spec after_commit(Moment.t(), keyword()) :: :ok
  def after_commit(%Moment{} = moment, opts) do
    checkpoint = Moment.checkpoint(moment, :observer)
    context = Moment.context(moment, Keyword.get(opts, :context, %{}))

    :telemetry.execute(
      [:docket, :lifecycle, :committed],
      Map.merge(
        %{count: 1, checkpoint_seq: moment.run.checkpoint_seq, step: moment.run.step},
        retry_measurements(moment)
      ),
      %{
        checkpoint_type: moment.checkpoint_type,
        disposition: disposition_kind(moment),
        result: :committed
      }
    )

    Docket.Telemetry.emit_events(moment.run, moment.events)

    opts
    |> Keyword.get(:checkpoint_observers, [])
    |> List.wrap()
    |> Enum.each(
      &start_observer(
        Keyword.get(opts, :task_supervisor),
        &1,
        checkpoint,
        context
      )
    )

    :ok
  end

  defp start_wake_at(%Moment{disposition: disposition, proposed_at: proposed_at}) do
    case schedule(disposition, :unclaimed) do
      {:release_claim, :immediate} -> proposed_at
      {:release_claim, {:at, at}} -> at
      {:release_claim, park} when park in [:external, :terminal] -> nil
    end
  end

  defp mutation_decision({:ok, %Moment{} = moment}) do
    {:commit, moment.run, moment.checkpoint_type, schedule(moment.disposition, :unclaimed),
     moment}
  end

  defp mutation_decision({:unchanged, %Docket.Run{} = run}), do: {:no_change, run}
  defp mutation_decision({:error, reason}), do: {:error, reason}
  defp mutation_decision(other), do: {:error, {:invalid_lifecycle_mutation, other}}

  defp disposition_kind(%Moment{checkpoint_type: :retry_scheduled}), do: :retry
  defp disposition_kind(%Moment{disposition: :continue}), do: :continue
  defp disposition_kind(%Moment{disposition: {:park, :terminal, _}}), do: :terminal
  defp disposition_kind(%Moment{disposition: {:park, :external, _}}), do: :external
  defp disposition_kind(%Moment{disposition: {:park, :immediate, :drain_budget}}), do: :budget
  defp disposition_kind(%Moment{disposition: {:park, :immediate, _}}), do: :immediate
  defp disposition_kind(%Moment{disposition: {:park, {:at, _}, _}}), do: :timer

  defp retry_measurements(%Moment{
         checkpoint_type: :retry_scheduled,
         disposition: {:park, {:at, at}, _},
         proposed_at: proposed_at
       }),
       do: %{retry_delay_ms: max(DateTime.diff(at, proposed_at, :millisecond), 0)}

  defp retry_measurements(_), do: %{}

  defp store_span(operation, fun) do
    metadata = Map.put(Docket.Telemetry.correlation_metadata(), :operation, operation)

    Docket.Telemetry.span([:docket, :store, :operation], metadata, fn ->
      result = fun.()
      {result, %{result: Docket.Telemetry.result_kind(result)}}
    end)
  end

  defp start_observer(nil, observer, checkpoint, _context) do
    emit_observer_failure(:supervisor_unavailable, checkpoint)
    log_observer_failure(observer, checkpoint, :observer_supervisor_unavailable)
  end

  defp start_observer(task_supervisor, observer, checkpoint, context) do
    case Task.Supervisor.start_child(task_supervisor, fn ->
           deliver_observer(observer, checkpoint, context)
         end) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        emit_observer_failure(:not_started, checkpoint)
        log_observer_failure(observer, checkpoint, {:not_started, reason})
    end
  catch
    :exit, reason ->
      emit_observer_failure(:not_started, checkpoint)
      log_observer_failure(observer, checkpoint, {:not_started, reason})
  end

  defp deliver_observer(observer, checkpoint, context) when is_atom(observer) do
    Docket.Telemetry.span(
      [:docket, :checkpoint, :observer],
      %{checkpoint_type: checkpoint.type},
      fn ->
        case observe(observer, checkpoint, context) do
          :ok ->
            {:ok, %{result: :callback_completed, durable_success: true}}

          {:error, class, reason} ->
            emit_observer_failure(class, checkpoint)
            log_observer_failure(observer, checkpoint, reason)
            {:ok, %{result: class, durable_success: true}}
        end
      end
    )
  end

  defp deliver_observer(observer, checkpoint, _context) do
    emit_observer_failure(:invalid_observer, checkpoint)
    log_observer_failure(observer, checkpoint, :invalid_observer)
  end

  defp observe(observer, checkpoint, context) do
    case observer.observe(checkpoint, context) do
      :ok -> :ok
      {:error, reason} -> {:error, :callback_error, reason}
      other -> {:error, :invalid_return, {:invalid_return, other}}
    end
  rescue
    error -> {:error, :exception, {:exception, error}}
  catch
    kind, reason -> {:error, :throw, {kind, reason}}
  end

  defp emit_observer_failure(result, checkpoint) do
    :telemetry.execute(
      [:docket, :checkpoint, :observer, :failure],
      %{count: 1},
      %{checkpoint_type: checkpoint.type, result: result, durable_success: true}
    )
  end

  defp log_observer_failure(observer, checkpoint, reason) do
    Logger.warning(
      "Docket checkpoint observer failed after commit",
      observer: inspect(observer),
      run_id: checkpoint.run.id,
      checkpoint_type: checkpoint.type,
      reason: inspect(reason)
    )
  end
end
