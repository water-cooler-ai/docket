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

    storage.transaction(context, fn tx ->
      with {:ok, _run} <-
             runs.insert_run(
               tx,
               scope,
               moment.run,
               moment.checkpoint_type,
               start_wake_at(moment)
             ),
           :ok <- events.append_events(tx, scope, moment.run.id, moment.events) do
        {:ok, moment}
      end
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

    storage.transaction(context, fn tx ->
      with {:ok, _run} <- runs.commit(tx, scope, proposal),
           :ok <- events.append_events(tx, scope, moment.run.id, moment.events) do
        {:ok, moment}
      end
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

    storage.transaction(context, fn tx ->
      case runs.mutate_run(tx, scope, run_id, fn run ->
             mutation_decision(mutation.(run))
           end) do
        {:ok, {:committed, %Moment{} = moment}} ->
          with :ok <- events.append_events(tx, scope, run_id, moment.events) do
            {:ok, moment}
          end

        {:ok, {:unchanged, %Docket.Run{} = run}} ->
          {:ok, run}

        {:error, reason} ->
          {:error, reason}
      end
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

  defp start_observer(nil, observer, checkpoint, _context) do
    log_observer_failure(observer, checkpoint, :observer_supervisor_unavailable)
  end

  defp start_observer(task_supervisor, observer, checkpoint, context) do
    case Task.Supervisor.start_child(task_supervisor, fn ->
           deliver_observer(observer, checkpoint, context)
         end) do
      {:ok, _pid} -> :ok
      {:error, reason} -> log_observer_failure(observer, checkpoint, {:not_started, reason})
    end
  catch
    :exit, reason -> log_observer_failure(observer, checkpoint, {:not_started, reason})
  end

  defp deliver_observer(observer, checkpoint, context) when is_atom(observer) do
    case observer.observe(checkpoint, context) do
      :ok -> :ok
      {:error, reason} -> log_observer_failure(observer, checkpoint, reason)
      other -> log_observer_failure(observer, checkpoint, {:invalid_return, other})
    end
  rescue
    error -> log_observer_failure(observer, checkpoint, {:exception, error})
  catch
    kind, reason -> log_observer_failure(observer, checkpoint, {kind, reason})
  end

  defp deliver_observer(observer, checkpoint, _context) do
    log_observer_failure(observer, checkpoint, :invalid_observer)
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
