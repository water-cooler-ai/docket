defmodule Docket.Runtime.Loop do
  @moduledoc false

  # Processless transition functions over `Docket.Runtime.Graph` and
  # `Docket.Run`, shared by the supervised Runtime and `Docket.Test`.
  #
  # Every transition calculates exactly one pre-commit
  # `Docket.Runtime.Moment`: the proposed run, its assigned events, the
  # checkpoint type, and an explicit disposition. Calculation delivers no
  # checkpoint and emits no telemetry. `propose_init/3` and
  # `propose_advance/3` expose the raw moments to drivers that commit
  # externally; the legacy entrypoints (`init/3`, `plan/3`,
  # `apply_results/5`, `resolve_interrupt/5`) adapt the same moments through
  # the host-owned sync committer and return a new committed run plus
  # checkpoint effects - or a typed error with the previous run untouched.
  # Deterministic execution logic lives in `Docket.Runtime.Algorithm`.
  #
  # Checkpoint effects are `{:checkpoint, checkpoint, context, :accepted}`
  # for sync checkpoints already delivered inside the transition, and
  # `{:checkpoint, checkpoint, context, :pending}` for async checkpoints the
  # shell must deliver. A processless module cannot own async execution, so
  # async delivery belongs to the shell (inline: drained synchronously;
  # supervised Runtime: background task).

  alias Docket.{Checkpoint, Error, Event, Run, Schema, Wire}
  alias Docket.Run.{ChannelState, Failure, InterruptState, PendingWrite, TaskState, TimerState}
  alias Docket.Runtime.{Algorithm, Config, Dispatcher, Moment, TaskResult}

  @doc false
  # Builds the fresh `:created` run document consumed by `init/3`. Shared by
  # `Docket.run/4` and `Docket.Test.run_inline/3` so both entry points create
  # byte-identical initial runs.
  def build_initial_run(rtg, input, opts) do
    config = Config.resolve(opts)

    %Run{
      id: Keyword.get(opts, :run_id) || config.id_generator.(:run),
      graph_id: rtg.graph_id,
      graph_hash: rtg.graph_hash,
      status: :created,
      input: input || %{},
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  # ---------------------------------------------------------------------------
  # init/3
  # ---------------------------------------------------------------------------

  @doc """
  Single loop entrypoint for a live run.

  Infers fresh-versus-saved execution from the supplied run document: a
  `:created` run is initialized (inputs validated and written, `$start`
  edges evaluated); a `:running`/`:waiting` run continues as-is; a terminal
  run is returned unchanged with no checkpoint and no execution restart.

  For any run it is going to execute, emits the required sync
  `:run_initialized` checkpoint before returning.
  """
  def init(rtg, %Run{} = run, opts) do
    config = Config.resolve(opts)

    case do_propose_init(rtg, run, config) do
      {:ok, %Moment{} = moment} -> accept(moment, config)
      {:terminal, run} -> {:ok, run, []}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Calculates the initialization moment without delivering anything.

  Same run-status inference as `init/3`, but the transition is returned as
  one pre-commit `Docket.Runtime.Moment`: no checkpoint handler is invoked
  and no telemetry is emitted. Returns `{:ok, moment}`, `{:terminal, run}`
  for an already-terminal run (nothing to commit), or `{:error, error}`.
  """
  def propose_init(rtg, %Run{} = run, opts) do
    do_propose_init(rtg, run, Config.resolve(opts))
  end

  defp do_propose_init(rtg, run, config) do
    cond do
      run.graph_id != rtg.graph_id or run.graph_hash != rtg.graph_hash ->
        {:error,
         Error.new(:graph_mismatch, "run #{inspect(run.id)} does not match the supplied graph",
           details: %{
             run_graph_id: run.graph_id,
             run_graph_hash: run.graph_hash,
             graph_id: rtg.graph_id,
             graph_hash: rtg.graph_hash
           }
         )}

      run.status == :created ->
        init_fresh(rtg, run, config)

      Run.terminal?(run) ->
        {:terminal, run}

      run.status in [:running, :waiting] ->
        init_saved(rtg, run, config)

      true ->
        {:error,
         Error.new(
           :invalid_run,
           "run #{inspect(run.id)} has unknown status #{inspect(run.status)}"
         )}
    end
  end

  defp init_fresh(rtg, run, config) do
    with {:ok, input} <- validate_input(rtg, run.input),
         {:ok, %{channels: channels, triggered: triggered}} <- initial_channels(rtg, input) do
      now = config.clock.()
      input_channel_ids = input |> Map.keys() |> Enum.sort() |> Enum.map(&("input:" <> &1))
      edge_channel_ids = Enum.map(triggered, &Map.fetch!(rtg.edges, &1).channel_id)

      run = %{
        run
        | status: :running,
          input: input,
          channels: channels,
          changed_channels: MapSet.new(input_channel_ids ++ edge_channel_ids),
          started_at: now,
          updated_at: now
      }

      entries =
        [entry(:run_initialized, run.step, payload: %{"inputs" => Enum.sort(Map.keys(input))})] ++
          Enum.map(input_channel_ids, fn channel_id ->
            entry(:channel_updated, run.step, channel_id: channel_id, payload: %{"version" => 1})
          end) ++
          Enum.map(triggered, fn edge_id ->
            entry(:edge_triggered, run.step,
              channel_id: Map.fetch!(rtg.edges, edge_id).channel_id,
              payload: %{"edge_id" => edge_id}
            )
          end)

      {:ok, propose(run, :run_initialized, entries, :continue, config)}
    end
  end

  defp init_saved(rtg, run, config) do
    _ = rtg
    run = %{run | updated_at: config.clock.()}
    entries = [entry(:run_initialized, run.step, payload: %{"resumed" => true})]
    {:ok, propose(run, :run_initialized, entries, run_disposition(run), config)}
  end

  # The disposition a committed non-terminal run needs next: a `:waiting`
  # run is parked on its open interrupts; a `:running` run has dispatchable
  # work (planning decides what, including re-parking on retry deadlines).
  defp run_disposition(%Run{status: :waiting}), do: {:park, :external, :awaiting_interrupts}
  defp run_disposition(%Run{status: :running}), do: :continue

  defp validate_input(rtg, input) when is_nil(input) or input == %{} do
    validate_input_map(rtg, %{})
  end

  defp validate_input(rtg, input) when is_map(input) and not is_struct(input) do
    case Wire.dump_value(input) do
      {:ok, coerced} ->
        validate_input_map(rtg, coerced)

      {:error, reason} ->
        {:error, Error.new(:invalid_input, "run input is not durable: #{reason}", phase: :init)}
    end
  end

  defp validate_input(_rtg, other) do
    {:error,
     Error.new(:invalid_input, "run input must be a map, got #{inspect(other)}", phase: :init)}
  end

  defp validate_input_map(rtg, input) do
    declared = rtg.lowering.public_to_runtime.inputs

    unknown =
      for key <- Enum.sort(Map.keys(input)), not Map.has_key?(declared, key) do
        "unknown input #{inspect(key)}"
      end

    missing =
      for {input_id, channel_id} <- Enum.sort(declared),
          channel = Map.fetch!(rtg.channels, channel_id),
          channel.required,
          not Map.has_key?(input, input_id),
          is_nil(channel.default) do
        "required input #{inspect(input_id)} is missing"
      end

    invalid =
      for {input_id, value} <- Enum.sort(input),
          channel_id = Map.get(declared, input_id),
          channel_id != nil,
          schema = Map.fetch!(rtg.channels, channel_id).value_schema,
          schema != nil,
          {:error, reasons} <- [Schema.validate(schema, value)],
          reason <- reasons do
        "input #{inspect(input_id)}: #{reason}"
      end

    case unknown ++ missing ++ invalid do
      [] ->
        {:ok, input}

      reasons ->
        {:error,
         Error.new(:invalid_input, "run input is invalid",
           phase: :init,
           details: %{reasons: reasons}
         )}
    end
  end

  defp initial_channels(rtg, input) do
    channels =
      for {input_id, value} <- input, into: %{} do
        channel_id = "input:" <> input_id
        {channel_id, %ChannelState{channel_id: channel_id, value: value, version: 1}}
      end

    case Algorithm.evaluate_start_edges(rtg, channels, Map.keys(input)) do
      {:ok, result} ->
        {:ok, result}

      {:error, {edge_id, reasons}} ->
        {:error, guard_error(edge_id, reasons, :init)}
    end
  end

  # ---------------------------------------------------------------------------
  # plan/3
  # ---------------------------------------------------------------------------

  @doc """
  Plans the next node attempts from committed state.

  A run carrying durable active-superstep state resumes that superstep: the
  parked attempts whose retry deadlines have arrived are rebuilt with their
  committed identity. Otherwise a fresh superstep is planned.

  Returns:

  - `{:execute, run, activations}` - dispatch these, then call
    `apply_results/5`; the run is unchanged (planning commits nothing)
  - `{:park, run, park}` - the active superstep has no attempt due yet;
    `park` is `%{resume_at: DateTime.t(), wait_ms: non_neg_integer()}` and
    the run is unchanged
  - `{:wait, run, interrupt_ids}` - blocked on open interrupts
  - `{:terminal, run, effects}` - the run just completed or failed (the
    terminal checkpoint has been emitted), or was already terminal
  - `{:error, error}` - sync checkpoint failure or uninitialized run; the
    caller keeps the previous run

  Shells that have already served a park's wait pass the park's `resume_at`
  as `:resume_floor` in `opts`, so deadline checks do not depend on the
  wall clock having advanced (deterministic inline tests inject `:sleeper`
  instead of sleeping). The floor must be the served park's `resume_at` and
  never a later instant: flooring at the wake deadline makes exactly the
  due attempts eligible while later deadlines stay parked.
  """
  def plan(rtg, %Run{} = run, opts) do
    config = Config.resolve(opts)

    case do_propose_plan(rtg, run, opts, config) do
      {:moment, %Moment{} = moment} ->
        case accept(moment, config) do
          {:ok, run, effects} -> {:terminal, run, effects}
          {:error, error} -> {:error, error}
        end

      {:already_terminal, run} ->
        {:terminal, run, []}

      {:execute, run, activations} ->
        {:execute, run, activations}

      {:wait, run, interrupt_ids} ->
        {:wait, run, interrupt_ids}

      {:park, run, park} ->
        {:park, run, park}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Calculates the next commit-boundary moment for one advancement.

  Plans, dispatches, and applies exactly one superstep attempt, returning
  its commit boundary as one pre-commit `Docket.Runtime.Moment` - a
  barrier, retry park, or terminal commit. Calculation never delivers a
  checkpoint, never emits telemetry, and never speculatively drains a
  second uncommitted step: the caller commits each moment before asking
  for the next.

  Returns:

  - `{:ok, moment}` - one commit-boundary moment; `moment.disposition`
    says what the run needs after the commit
  - `{:wait, run, interrupt_ids}` - blocked on open interrupts; nothing
    to commit (the `:waiting` status was committed at its barrier)
  - `{:park, run, park}` - the active superstep has no attempt due yet;
    nothing to commit
  - `{:terminal, run}` - the run is already terminal; nothing to commit
  - `{:error, error}` - the run cannot advance (uninitialized or unknown
    status); nothing was calculated

  Accepts the same `:resume_floor` option as `plan/3`.
  """
  def propose_advance(rtg, %Run{} = run, opts) do
    config = Config.resolve(opts)

    case do_propose_plan(rtg, run, opts, config) do
      {:execute, run, activations} ->
        results = Dispatcher.dispatch(activations, rtg, run, config)

        case do_propose_results(rtg, run, config, activations, results) do
          {:moment, moment} -> {:ok, moment}
          {:moment, moment, _park} -> {:ok, moment}
        end

      {:moment, moment} ->
        {:ok, moment}

      {:wait, run, interrupt_ids} ->
        {:wait, run, interrupt_ids}

      {:park, run, park} ->
        {:park, run, park}

      {:already_terminal, run} ->
        {:terminal, run}

      {:error, error} ->
        {:error, error}
    end
  end

  defp do_propose_plan(rtg, run, opts, config) do
    cond do
      run.status == :created ->
        {:error,
         Error.new(:invalid_run, "run #{inspect(run.id)} must be initialized before planning")}

      Run.terminal?(run) ->
        {:already_terminal, run}

      map_size(run.active_tasks) > 0 ->
        resume_superstep(rtg, run, opts, config)

      true ->
        case Algorithm.plan(rtg, run, config) do
          :done ->
            complete(rtg, run, config)

          {:wait, interrupt_ids} ->
            {:wait, run, interrupt_ids}

          {:failed, :max_supersteps_exceeded} ->
            limit = Algorithm.max_supersteps(rtg, config)

            fail(run, config, [],
              failure:
                Failure.new(
                  "max_supersteps_exceeded",
                  "run exceeded the superstep limit of #{limit}",
                  details: %{"limit" => limit}
                ),
              payload: %{"reason" => "max_supersteps_exceeded", "limit" => limit}
            )

          {:execute, node_ids} ->
            case Algorithm.prepare_activations(rtg, run, node_ids, config) do
              {:ok, activations} ->
                {:execute, run, activations}

              {:error, %Error{} = error} ->
                fail(run, config, [],
                  failure: Failure.new(Atom.to_string(error.type), error.message),
                  payload: %{"reason" => Atom.to_string(error.type), "message" => error.message}
                )
            end
        end
    end
  end

  # Resumes the durable active superstep: parked attempts whose retry
  # deadline has arrived are rebuilt with their committed identity; when no
  # attempt is due yet the shell is told when to wake. Rebuilding failures
  # (unknown node, invalid policy) fail the run the same way fresh planning
  # does.
  defp resume_superstep(rtg, run, opts, config) do
    now = resume_now(config, opts)
    due_ids = run.active_tasks |> Map.keys() |> Enum.filter(&due?(run.timers, &1, now))

    if due_ids == [] do
      {:park, run, park_info(run.timers, now)}
    else
      case Algorithm.resume_activations(rtg, run) do
        {:ok, activations} ->
          due_set = MapSet.new(due_ids)
          {:execute, run, Enum.filter(activations, &MapSet.member?(due_set, &1.task_id))}

        {:error, %Error{} = error} ->
          fail(run, config, [],
            failure: Failure.new(Atom.to_string(error.type), error.message),
            payload: %{"reason" => Atom.to_string(error.type), "message" => error.message}
          )
      end
    end
  end

  # The instant deadline checks compare against: the injected clock, floored
  # by the park deadline the shell reports it has already waited out.
  defp resume_now(config, opts) do
    now = config.clock.()

    case Keyword.get(opts, :resume_floor) do
      %DateTime{} = floor -> if DateTime.compare(floor, now) == :gt, do: floor, else: now
      nil -> now
    end
  end

  # An active task without a timer cannot wait on anything, so it is due;
  # committed parks always write one timer per active task.
  defp due?(timers, task_id, now) do
    case Map.fetch(timers, task_id) do
      {:ok, %TimerState{fires_at: fires_at}} -> DateTime.compare(fires_at, now) != :gt
      :error -> true
    end
  end

  defp park_info(timers, now) do
    resume_at =
      timers
      |> Enum.map(fn {_task_id, timer} -> timer.fires_at end)
      |> Enum.min(DateTime)

    %{resume_at: resume_at, wait_ms: max(DateTime.diff(resume_at, now, :millisecond), 0)}
  end

  defp complete(rtg, run, config) do
    now = config.clock.()
    output = Algorithm.project_output(rtg, run.channels)
    run = %{run | status: :done, output: output, finished_at: now, updated_at: now}

    entries = [
      entry(:run_completed, run.step, payload: %{"outputs" => Enum.sort(Map.keys(output))})
    ]

    {:moment, propose(run, :run_completed, entries, {:park, :terminal, :run_completed}, config)}
  end

  # Terminal failure absorbs the active superstep: no parked attempt, pending
  # write, or timer survives a failed run.
  defp fail(run, config, extra_entries, opts) do
    now = config.clock.()
    failure = Keyword.fetch!(opts, :failure)

    run = %{
      run
      | status: :failed,
        failure: failure,
        finished_at: now,
        updated_at: now,
        active_tasks: %{},
        pending_writes: [],
        timers: %{}
    }

    entries =
      extra_entries ++ [entry(:run_failed, run.step, payload: Keyword.fetch!(opts, :payload))]

    {:moment, propose(run, :run_failed, entries, {:park, :terminal, :run_failed}, config)}
  end

  # ---------------------------------------------------------------------------
  # apply_results/5
  # ---------------------------------------------------------------------------

  @doc """
  Commits the outcome of one dispatch of the active superstep.

  When every task of the superstep has a final result, this is the update
  barrier: pending sibling results parked by earlier retry commits are
  merged with this dispatch's results, reducers apply, edge triggers
  evaluate, interrupts register, and the barrier checkpoint commits with
  the active-superstep state cleared.

  When any dispatched attempt failed retryably with budget remaining - or
  parked attempts not yet due remain active - the superstep parks instead:
  completed results become pending writes (invisible until the barrier),
  each failed task's next attempt and retry deadline commit through a sync
  `:retry_scheduled` checkpoint without advancing the graph step, and
  `{:park, run, park, effects}` tells the shell when to wake.

  Any permanent node failure fails the superstep: no writes from the
  superstep commit and the run commits as `:failed` through a sync
  `:run_failed` checkpoint. Returns `{:ok, run, effects}` (the run may be
  terminal), `{:park, run, park, effects}`, or `{:error, error}` on sync
  checkpoint failure, keeping the previous committed run.
  """
  def apply_results(rtg, %Run{} = run, activations, results, opts) do
    config = Config.resolve(opts)

    case do_propose_results(rtg, run, config, activations, results) do
      {:moment, %Moment{} = moment} ->
        accept(moment, config)

      {:moment, %Moment{} = moment, park} ->
        case accept(moment, config) do
          {:ok, run, effects} -> {:park, run, park, effects}
          {:error, error} -> {:error, error}
        end
    end
  end

  defp do_propose_results(rtg, run, config, activations, results) do
    results = Enum.sort_by(results, & &1.node_id)

    {oks, interrupt_results, retries, errors} = partition_results(results)
    {validated_writes, write_errors} = validate_writes(rtg, oks)
    {interrupt_specs, interrupt_errors} = validate_interrupts(rtg, run, interrupt_results)
    permanent = errors ++ write_errors ++ interrupt_errors

    cond do
      permanent != [] ->
        fail_superstep(run, config, results, permanent)

      retries != [] or remaining_active?(run, results) ->
        park(run, config, activations, retries, validated_writes, interrupt_specs)

      true ->
        barrier(rtg, run, config, results, validated_writes, interrupt_specs)
    end
  end

  # The update barrier: pending sibling results parked by earlier retry
  # commits pass through the same validators as this dispatch's results.
  # Validation is deterministic, so runtime-produced pending state validates
  # identically to when it was parked; only corrupted durable state can
  # fail here, and it fails the run through the typed permanent path
  # instead of crashing the commit.
  defp barrier(rtg, run, config, results, validated_writes, interrupt_specs) do
    pending_results = rehydrate_pending(run)
    {pending_oks, pending_interrupts, [], []} = partition_results(pending_results)
    {pending_writes, pending_write_errors} = validate_writes(rtg, pending_oks)
    {pending_specs, pending_interrupt_errors} = validate_interrupts(rtg, run, pending_interrupts)

    case pending_write_errors ++ pending_interrupt_errors do
      [] ->
        validated_writes =
          Enum.sort_by(pending_writes ++ validated_writes, fn {result, _} -> result.node_id end)

        interrupt_specs =
          Enum.sort_by(pending_specs ++ interrupt_specs, fn {result, _} -> result.node_id end)

        commit(rtg, run, config, results, validated_writes, interrupt_specs)

      permanent ->
        fail_superstep(run, config, results, permanent)
    end
  end

  defp fail_superstep(run, config, results, permanent) do
    entries =
      attempt_failure_entries(run.step, results) ++
        Enum.map(permanent, fn failure ->
          entry(:node_failed, run.step,
            node_id: failure.node_id,
            task_id: failure.task_id,
            payload: %{
              "attempt" => failure.attempt,
              "reason" => inspect(failure.reason),
              "permanent" => true
            }
          )
        end)

    failed_nodes = permanent |> Enum.map(& &1.node_id) |> Enum.uniq() |> Enum.sort()

    fail(run, config, entries,
      failure: node_failure(permanent, failed_nodes),
      payload: %{"reason" => "node_failed", "nodes" => failed_nodes}
    )
  end

  # True when active tasks beyond this dispatch's results remain parked
  # (their retry deadlines were not due), so the barrier cannot commit yet.
  defp remaining_active?(run, results) do
    dispatched = MapSet.new(results, & &1.task_id)
    run.active_tasks |> Map.keys() |> Enum.any?(&(not MapSet.member?(dispatched, &1)))
  end

  # Commits the retry park: completed results move to pending writes, each
  # retrying task's next attempt (with its full activation identity and
  # accumulated failures) and retry deadline become durable, and the graph
  # step does not advance. The checkpoint is sync so a crash during backoff
  # resumes from this state instead of resetting the attempt position.
  defp park(run, config, activations, retries, validated_writes, interrupt_specs) do
    now = config.clock.()
    activations_by_task = Map.new(activations, &{&1.task_id, &1})

    new_pending =
      Enum.sort_by(
        Enum.map(validated_writes, fn {result, update} ->
          pending_write(result, :update, update)
        end) ++
          Enum.map(interrupt_specs, fn {result, interrupt} ->
            pending_write(result, :interrupt, %{interrupt | node_id: result.node_id})
          end),
        & &1.node_id
      )

    finalized_ids = Enum.map(new_pending, & &1.task_id)

    {parked_tasks, parked_timers} =
      Enum.reduce(retries, {%{}, %{}}, fn result, {tasks, timers} ->
        activation = Map.fetch!(activations_by_task, result.task_id)
        next_attempt = activation.attempt + 1

        prior_failures =
          case Map.fetch(run.active_tasks, result.task_id) do
            {:ok, %TaskState{failures: failures}} -> failures
            :error -> []
          end

        task = %TaskState{
          task_id: result.task_id,
          node_id: result.node_id,
          step: activation.step,
          attempt: next_attempt,
          status: :retry_scheduled,
          input_hash: activation.input_hash,
          idempotency_key: TaskState.idempotency_key(result.task_id, next_attempt),
          snapshot: activation.snapshot,
          source_versions: activation.source_versions,
          failures: prior_failures ++ [%{attempt: result.attempt, reason: inspect(result.value)}]
        }

        timer = %TimerState{
          kind: :retry,
          fires_at: DateTime.add(now, activation.retry.backoff_ms, :millisecond)
        }

        {Map.put(tasks, result.task_id, task), Map.put(timers, result.task_id, timer)}
      end)

    parked = %{
      run
      | active_tasks: run.active_tasks |> Map.drop(finalized_ids) |> Map.merge(parked_tasks),
        pending_writes: run.pending_writes ++ new_pending,
        timers: run.timers |> Map.drop(finalized_ids) |> Map.merge(parked_timers),
        updated_at: now
    }

    park = park_info(parked.timers, now)
    entries = attempt_failure_entries(parked.step, retries)
    disposition = {:park, {:at, park.resume_at}, :retry_backoff}

    {:moment,
     propose(parked, :retry_scheduled, entries, disposition, config,
       pending_attempts: new_pending
     ), park}
  end

  defp pending_write(result, kind, value) do
    %PendingWrite{
      task_id: result.task_id,
      node_id: result.node_id,
      attempt: result.attempt,
      kind: kind,
      value: value
    }
  end

  # Rehydrates pending sibling results committed by earlier retry parks back
  # into task results. Their retried attempts' failure events were emitted by
  # the parks that recorded them, so they contribute no failure entries at
  # the barrier.
  defp rehydrate_pending(run) do
    Enum.map(run.pending_writes, fn %PendingWrite{} = pending ->
      %TaskResult{
        task_id: pending.task_id,
        node_id: pending.node_id,
        attempt: pending.attempt,
        status: if(pending.kind == :update, do: :ok, else: :interrupt),
        value: pending.value
      }
    end)
  end

  defp failure(result, reason) do
    %{
      node_id: result.node_id,
      task_id: result.task_id,
      attempt: result.attempt,
      reason: reason
    }
  end

  # The durable cause for a permanent node failure. Per-node reasons ride in
  # details so a failed run keeps them even when event persistence is off.
  defp node_failure(permanent, failed_nodes) do
    errors = Map.new(permanent, fn failure -> {failure.node_id, inspect(failure.reason)} end)

    node_id =
      case failed_nodes do
        [node_id] -> node_id
        _multiple -> nil
      end

    Failure.new("node_failed", "node(s) #{Enum.join(failed_nodes, ", ")} failed permanently",
      node_id: node_id,
      details: %{"nodes" => failed_nodes, "errors" => errors}
    )
  end

  defp partition_results(results) do
    Enum.reduce(results, {[], [], [], []}, fn result, {oks, interrupts, retries, errors} ->
      case result.status do
        :ok ->
          {[result | oks], interrupts, retries, errors}

        :interrupt ->
          {oks, [result | interrupts], retries, errors}

        :retry ->
          {oks, interrupts, [result | retries], errors}

        :error ->
          {oks, interrupts, retries, [failure(result, result.value) | errors]}
      end
    end)
    |> then(fn {oks, interrupts, retries, errors} ->
      {Enum.reverse(oks), Enum.reverse(interrupts), Enum.reverse(retries), Enum.reverse(errors)}
    end)
  end

  defp validate_writes(rtg, oks) do
    Enum.reduce(oks, {[], []}, fn result, {writes, errors} ->
      case Algorithm.validate_state_update(rtg, result.node_id, result.value) do
        {:ok, update} ->
          {[{result, update} | writes], errors}

        {:error, reasons} ->
          {writes, [failure(result, {:invalid_state_update, reasons}) | errors]}
      end
    end)
    |> then(fn {writes, errors} -> {Enum.reverse(writes), Enum.reverse(errors)} end)
  end

  defp validate_interrupts(rtg, run, interrupt_results) do
    Enum.reduce(interrupt_results, {[], []}, fn result, {specs, errors} ->
      interrupt = result.value

      case interrupt_errors(rtg, run, interrupt) do
        [] ->
          {[{result, interrupt} | specs], errors}

        reasons ->
          {specs, [failure(result, {:invalid_interrupt, reasons}) | errors]}
      end
    end)
    |> then(fn {specs, errors} -> {Enum.reverse(specs), Enum.reverse(errors)} end)
  end

  defp interrupt_errors(rtg, run, interrupt) do
    check_resume_channel(rtg, interrupt) ++
      check_schema(interrupt) ++
      check_id_unused(run, interrupt)
  end

  defp check_resume_channel(rtg, interrupt) do
    if Map.has_key?(rtg.lowering.public_to_runtime.fields, interrupt.resume_channel || "") do
      []
    else
      [
        "interrupt resume_channel #{inspect(interrupt.resume_channel)} is not a declared state field"
      ]
    end
  end

  defp check_schema(interrupt) do
    case interrupt.schema do
      nil -> []
      %Schema{} -> []
      other -> ["interrupt schema must be a Docket.Schema or nil, got #{inspect(other)}"]
    end
  end

  defp check_id_unused(run, interrupt) do
    if interrupt.id != nil and Map.has_key?(run.interrupts, interrupt.id) do
      ["interrupt id #{inspect(interrupt.id)} already exists on this run"]
    else
      []
    end
  end

  defp commit(rtg, run, config, results, validated_writes, interrupt_specs) do
    writes = Enum.map(validated_writes, fn {result, update} -> {result.node_id, update} end)
    {channels, changed_fields, writers} = Algorithm.apply_state_writes(rtg, run.channels, writes)
    ok_node_ids = Enum.map(validated_writes, fn {result, _update} -> result.node_id end)

    case Algorithm.evaluate_edge_triggers(rtg, channels, ok_node_ids, changed_fields) do
      {:error, {edge_id, reasons}} ->
        fail(run, config, [],
          failure:
            Failure.new(
              "guard_evaluation_failed",
              "edge #{edge_id} guard evaluation failed",
              details: %{"edge_id" => edge_id, "reasons" => reasons}
            ),
          payload: %{
            "reason" => "guard_evaluation_failed",
            "edge_id" => edge_id,
            "details" => reasons
          }
        )

      {:ok, %{channels: channels, triggered: triggered, finish: finish}} ->
        now = config.clock.()
        channels = clear_consumed_activations(rtg, channels, run.changed_channels)

        {interrupts, interrupt_node_ids, interrupt_results} =
          build_interrupts(config, interrupt_specs, now)

        changed_channels =
          MapSet.new(
            Enum.map(changed_fields, &("state:" <> &1)) ++
              Enum.map(triggered, &Map.fetch!(rtg.edges, &1).channel_id)
          )

        pending_nodes =
          run.pending_nodes
          |> MapSet.difference(MapSet.new(ok_node_ids))
          |> MapSet.union(MapSet.new(interrupt_node_ids))

        committed = %{
          run
          | channels: channels,
            changed_channels: changed_channels,
            pending_nodes: pending_nodes,
            interrupts: Map.merge(run.interrupts, interrupts),
            active_tasks: %{},
            pending_writes: [],
            timers: %{},
            step: run.step + 1,
            status: :running,
            updated_at: now
        }

        committed = %{committed | status: eager_status(rtg, committed, config)}

        entries =
          commit_entries(rtg, run.step, results, validated_writes, changed_fields, writers,
            triggered: triggered,
            finish: finish,
            interrupts: interrupts,
            interrupt_results: interrupt_results
          )

        type = if map_size(interrupts) == 0, do: :step_committed, else: :interrupt_requested
        {:moment, propose(committed, type, entries, run_disposition(committed), config)}
    end
  end

  # The run must never durably claim :running when nothing can proceed: when
  # the barrier leaves open interrupts and no activations, commit :waiting in
  # the same checkpoint.
  defp eager_status(rtg, committed, config) do
    case Algorithm.plan(rtg, committed, config) do
      {:wait, _interrupt_ids} -> :waiting
      _other -> :running
    end
  end

  defp build_interrupts(config, interrupt_specs, now) do
    Enum.reduce(interrupt_specs, {%{}, [], %{}}, fn {result, interrupt},
                                                    {states, node_ids, results} ->
      id = interrupt.id || config.id_generator.(:interrupt)

      state = %InterruptState{
        id: id,
        node_id: result.node_id,
        status: :open,
        resume_channel: interrupt.resume_channel,
        prompt: interrupt.prompt,
        schema: interrupt.schema,
        created_at: now,
        metadata: interrupt.metadata || %{}
      }

      {
        Map.put(states, id, state),
        [result.node_id | node_ids],
        Map.put(results, id, result)
      }
    end)
    |> then(fn {states, node_ids, results} ->
      {states, Enum.reverse(node_ids), results}
    end)
  end

  # Edge activation channels are visible for one step: values consumed by
  # this superstep's plan are cleared at its barrier. Versions stay monotonic
  # for observability; activation is driven by the changed set, never by
  # stored activation values.
  defp clear_consumed_activations(rtg, channels, previously_changed) do
    Enum.reduce(previously_changed, channels, fn channel_id, channels ->
      case Map.fetch(rtg.channels, channel_id) do
        {:ok, %{type: :ephemeral}} ->
          Map.update(channels, channel_id, nil, fn
            nil -> nil
            state -> %{state | value: nil}
          end)
          |> Map.reject(fn {_id, state} -> is_nil(state) end)

        _other ->
          channels
      end
    end)
  end

  defp commit_entries(rtg, step, results, validated_writes, changed_fields, writers, extra) do
    attempt_failure_entries(step, results) ++
      Enum.map(validated_writes, fn {result, _update} ->
        entry(:node_completed, step,
          node_id: result.node_id,
          task_id: result.task_id,
          payload: %{"attempt" => result.attempt}
        )
      end) ++
      Enum.map(Enum.sort(changed_fields), fn field_id ->
        entry(:channel_updated, step,
          channel_id: "state:" <> field_id,
          payload: %{"writers" => Map.fetch!(writers, field_id)}
        )
      end) ++
      Enum.map(Keyword.fetch!(extra, :triggered) ++ Keyword.fetch!(extra, :finish), fn edge_id ->
        entry(:edge_triggered, step,
          channel_id: Map.fetch!(rtg.edges, edge_id).channel_id,
          payload: %{"edge_id" => edge_id, "to" => Map.fetch!(rtg.edges, edge_id).to}
        )
      end) ++
      Enum.map(Enum.sort(Keyword.fetch!(extra, :interrupts)), fn {id, state} ->
        result = Map.fetch!(Keyword.fetch!(extra, :interrupt_results), id)

        entry(:interrupt_requested, step,
          node_id: state.node_id,
          task_id: result.task_id,
          payload: %{
            "attempt" => result.attempt,
            "interrupt_id" => id,
            "resume_channel" => state.resume_channel
          }
        )
      end)
  end

  # One dispatch executes at most one attempt per task, so the only
  # non-permanent failure to record is a :retry result's; an :error result's
  # permanent failure is reported separately by the caller.
  defp attempt_failure_entries(step, results) do
    for result <- results, result.status == :retry do
      entry(:node_failed, step,
        node_id: result.node_id,
        task_id: result.task_id,
        payload: %{
          "attempt" => result.attempt,
          "reason" => inspect(result.value),
          "permanent" => false
        }
      )
    end
  end

  # ---------------------------------------------------------------------------
  # resolve_interrupt/5
  # ---------------------------------------------------------------------------

  @doc """
  Resolves an open interrupt: validates the value, writes it to the resume
  field, marks the interrupt resolved, and emits the sync
  `:interrupt_resolved` checkpoint.

  The interrupted node stays in `pending_nodes` and re-executes in the next
  superstep with the resolved value in its snapshot.
  """
  def resolve_interrupt(rtg, %Run{} = run, interrupt_id, value, opts) do
    config = Config.resolve(opts)

    cond do
      Run.terminal?(run) ->
        {:error,
         Error.new(:inactive_run, "run #{inspect(run.id)} is #{run.status} and cannot be resumed")}

      not match?({:ok, %InterruptState{status: :open}}, Map.fetch(run.interrupts, interrupt_id)) ->
        {:error, Error.new(:not_found, "no open interrupt #{inspect(interrupt_id)}")}

      true ->
        interrupt = Map.fetch!(run.interrupts, interrupt_id)

        case do_resolve_interrupt(rtg, run, interrupt, value, config) do
          {:moment, %Moment{} = moment} -> accept(moment, config)
          {:error, error} -> {:error, error}
        end
    end
  end

  defp do_resolve_interrupt(rtg, run, interrupt, value, config) do
    with {:ok, value} <- durable_resolution(value),
         :ok <- validate_resolution_schema(interrupt, value),
         {:ok, update} <- validate_resolution_write(rtg, interrupt, value) do
      now = config.clock.()

      {channels, changed_fields, _writers} =
        Algorithm.apply_state_writes(rtg, run.channels, [{interrupt.node_id, update}])

      resolved = %{interrupt | status: :resolved, resolved_at: now}
      changed_channel_ids = Enum.map(changed_fields, &("state:" <> &1))

      run = %{
        run
        | channels: channels,
          changed_channels:
            Enum.reduce(changed_channel_ids, run.changed_channels, &MapSet.put(&2, &1)),
          interrupts: Map.put(run.interrupts, interrupt.id, resolved),
          status: :running,
          updated_at: now
      }

      entries =
        [
          entry(:interrupt_resolved, run.step,
            node_id: interrupt.node_id,
            payload: %{
              "interrupt_id" => interrupt.id,
              "resume_channel" => interrupt.resume_channel
            }
          )
        ] ++
          Enum.map(changed_channel_ids, fn channel_id ->
            entry(:channel_updated, run.step,
              channel_id: channel_id,
              payload: %{"writers" => [interrupt.node_id]}
            )
          end)

      {:moment, propose(run, :interrupt_resolved, entries, :continue, config)}
    end
  end

  defp durable_resolution(value) do
    case Wire.dump_value(value) do
      {:ok, coerced} ->
        {:ok, coerced}

      {:error, reason} ->
        {:error,
         Error.new(:invalid_input, "interrupt resolution value is not durable: #{reason}")}
    end
  end

  defp validate_resolution_schema(%InterruptState{schema: nil}, _value), do: :ok

  defp validate_resolution_schema(%InterruptState{schema: schema}, value) do
    case Schema.validate(schema, value) do
      :ok ->
        :ok

      {:error, reasons} ->
        {:error, invalid_resolution(reasons)}
    end
  end

  defp validate_resolution_write(rtg, interrupt, value) do
    case Algorithm.validate_state_update(rtg, interrupt.node_id, %{
           interrupt.resume_channel => value
         }) do
      {:ok, update} ->
        {:ok, update}

      {:error, reasons} ->
        {:error, invalid_resolution(reasons)}
    end
  end

  defp invalid_resolution(reasons) do
    Error.new(:invalid_input, "interrupt resolution value is invalid",
      details: %{reasons: reasons}
    )
  end

  # ---------------------------------------------------------------------------
  # Moment production and legacy adaptation
  # ---------------------------------------------------------------------------

  defp entry(type, step, opts) do
    %{
      type: type,
      step: step,
      node_id: Keyword.get(opts, :node_id),
      channel_id: Keyword.get(opts, :channel_id),
      task_id: Keyword.get(opts, :task_id),
      payload: Keyword.get(opts, :payload, %{})
    }
  end

  defp guard_error(edge_id, reasons, phase) do
    Error.new(:guard_evaluation_failed, "guard on edge #{inspect(edge_id)} failed to evaluate",
      phase: phase,
      details: %{edge_id: edge_id, reasons: reasons}
    )
  end

  # Assigns event identities from the run's sequences, bumps its counters,
  # and builds the one pre-commit moment for the transition. Pure
  # calculation: no storage write, no checkpoint delivery, no telemetry;
  # no executor work is ever in flight when it runs.
  defp propose(run, type, entries, disposition, config, identity_opts \\ []) do
    now = config.clock.()
    run = %{run | checkpoint_seq: run.checkpoint_seq + 1}

    {runtime_events, event_seq} =
      Enum.map_reduce(entries, run.event_seq, fn entry, seq ->
        seq = seq + 1

        {%Event{
           run_id: run.id,
           seq: seq,
           type: entry.type,
           step: entry.step,
           node_id: entry.node_id,
           channel_id: entry.channel_id,
           task_id: entry.task_id,
           timestamp: now,
           payload: entry.payload
         }, seq}
      end)

    pending_attempts = Keyword.get(identity_opts, :pending_attempts, [])

    checkpoint_metadata =
      Moment.checkpoint_metadata(run, runtime_events, type, disposition, pending_attempts)

    checkpoint_event_seq = event_seq + 1

    checkpoint_event = %Event{
      run_id: run.id,
      seq: checkpoint_event_seq,
      type: :checkpoint_committed,
      step: run.step,
      timestamp: now,
      metadata: checkpoint_metadata
    }

    events = runtime_events ++ [checkpoint_event]
    run = %{run | event_seq: checkpoint_event_seq}

    %Moment{
      run: run,
      events: events,
      checkpoint_type: type,
      checkpoint_metadata: checkpoint_metadata,
      pending_attempts: pending_attempts,
      disposition: disposition,
      proposed_at: now
    }
  end

  # Adapts one moment through the host-owned committer: a sync checkpoint
  # must be accepted before the proposed run becomes the shell's truth; an
  # async checkpoint is returned as a pending effect alongside the already
  # accepted run. Only this function turns moments into committed
  # checkpoints and telemetry.
  defp accept(%Moment{} = moment, config) do
    delivery = Checkpoint.delivery(moment.checkpoint_type, config.checkpoint_overrides)
    checkpoint = Moment.checkpoint(moment, delivery)
    context = Moment.context(moment, config.context)

    case delivery do
      :sync ->
        case deliver_checkpoint(config.checkpoint, checkpoint, context) do
          :ok ->
            Docket.Telemetry.emit_events(moment.run, moment.events)
            {:ok, moment.run, [{:checkpoint, checkpoint, context, :accepted}]}

          {:error, reason} ->
            {:error,
             Error.new(
               :checkpoint_failed,
               "sync #{moment.checkpoint_type} checkpoint was not accepted",
               phase: moment.checkpoint_type,
               reason: reason
             )}
        end

      :async ->
        Docket.Telemetry.emit_events(moment.run, moment.events)
        {:ok, moment.run, [{:checkpoint, checkpoint, context, :pending}]}
    end
  end

  @doc false
  def deliver_checkpoint(module, checkpoint, context) do
    case module.handle(checkpoint, context) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_checkpoint_return, other}}
    end
  rescue
    exception -> {:error, {:raised, exception}}
  end
end
