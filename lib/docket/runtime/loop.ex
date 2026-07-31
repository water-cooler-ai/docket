defmodule Docket.Runtime.Loop do
  @moduledoc false

  # Processless transition functions over `Docket.Runtime.Graph` and
  # `Docket.Run`, shared by backend vehicles and `Docket.Test`.
  #
  # Every transition calculates exactly one pre-commit
  # `Docket.Runtime.Moment`: the proposed run, its assigned events, the
  # checkpoint type, and an explicit disposition. Calculation delivers no
  # checkpoint and emits no telemetry. `propose_init/3` and
  # `propose_advance/3` expose the raw moments to durable and processless
  # drivers alike.
  # Deterministic execution logic lives in `Docket.Runtime.Algorithm`;
  # dispatch results settle through `Docket.Runtime.Superstep`, which
  # partitions and validates each result batch exactly once before the loop
  # picks the commit boundary.

  alias Docket.{Error, Run, Schema, Wire}
  alias Docket.Run.{ChannelState, Failure, InterruptState, TaskState, TimerState}
  alias Docket.Runtime.{Algorithm, Config, Dispatcher, ExecutionPolicy, Moment, Superstep}

  @doc false
  # Builds the fresh `:created` run document consumed by `init/3`. Shared by
  # backend lifecycle starts and `Docket.Test.run_inline/3` so both entry points create
  # byte-identical initial runs.
  def build_initial_run(rtg, input, opts) do
    config = Config.resolve_moment(opts)

    %Run{
      id: Keyword.get(opts, :run_id) || config.id_generator.(:run),
      graph_id: rtg.graph_id,
      graph_hash: rtg.graph_hash,
      status: :created,
      input: input || %{},
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  @doc """
  Calculates the initialization moment without delivering anything.

  Same run-status inference as `init/3`, but the transition is returned as
  one pre-commit `Docket.Runtime.Moment`: no observer is invoked and no
  telemetry is emitted. Returns `{:ok, moment}`, `{:terminal, run}`
  for an already-terminal run (nothing to commit), or `{:error, error}`.
  """
  def propose_init(rtg, %Run{} = run, opts) do
    do_propose_init(rtg, run, Config.resolve_moment(opts))
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
    config = Config.resolve_moment(opts)

    case do_propose_plan(rtg, run, opts, config) do
      {:execute, run, activations, pre_results} ->
        skip = MapSet.new(pre_results, & &1.task_id)
        dispatchable = Enum.reject(activations, &MapSet.member?(skip, &1.task_id))
        results = pre_results ++ Dispatcher.dispatch(dispatchable, rtg, run, config)
        {:ok, settle(Superstep.new(rtg, run, config, activations, results))}

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

      run.pending_writes != [] ->
        {:execute, run, [], []}

      true ->
        case Algorithm.plan(rtg, run, config) do
          :done ->
            {:moment, complete(rtg, run, config)}

          {:wait, interrupt_ids} ->
            {:wait, run, interrupt_ids}

          {:failed, :max_supersteps_exceeded} ->
            limit = Algorithm.max_supersteps(rtg, config)

            failure =
              Failure.new(
                "max_supersteps_exceeded",
                "run exceeded the superstep limit of #{limit}",
                details: %{"limit" => limit}
              )

            {:moment, fail(run, config, [], failure)}

          {:execute, node_ids} ->
            case Algorithm.prepare_activations(rtg, run, node_ids, config) do
              {:ok, activations} ->
                {:execute, run, activations, []}

              {:error, %Error{} = error} ->
                {:moment, fail(run, config, [], Failure.from_error(error))}
            end
        end
    end
  end

  # Resumes the durable active superstep: parked attempts whose retry
  # deadline has arrived are rebuilt with their committed identity; when no
  # attempt is due yet the shell is told when to wake. A due detached task is
  # never re-dispatched — its outstanding attempt may still be executing
  # elsewhere under the same idempotency key — so its deadline expiry is
  # synthesized as a failure result and settled through the ordinary retry
  # budget. Rebuilding failures (unknown node, invalid policy) fail the run
  # the same way fresh planning does.
  defp resume_superstep(rtg, run, opts, config) do
    now = resume_now(config, opts)
    due_ids = run.active_tasks |> Map.keys() |> Enum.filter(&due?(run.timers, &1, now))

    if due_ids == [] do
      {:park, run, park_info(run.timers, now)}
    else
      case Algorithm.resume_activations(rtg, run) do
        {:ok, activations} ->
          due_set = MapSet.new(due_ids)
          due = Enum.filter(activations, &MapSet.member?(due_set, &1.task_id))
          expired = Enum.filter(due, &detached?(run, &1.task_id))
          {:execute, run, due, Enum.map(expired, &expired_result(run, &1))}

        {:error, %Error{} = error} ->
          {:moment, fail(run, config, [], Failure.from_error(error))}
      end
    end
  end

  defp detached?(run, task_id) do
    match?(%TaskState{status: :detached}, Map.get(run.active_tasks, task_id))
  end

  # A worker-reported failure stored by the early-expire completion wins over
  # the generic deadline reason; the disposition policy decides retry vs
  # permanent through the same budget rule as dispatched failures.
  defp expired_result(run, activation) do
    task = Map.fetch!(run.active_tasks, activation.task_id)
    reason = Map.get(task.metadata, "detach_error", :detach_deadline_expired)

    Dispatcher.result(
      activation,
      {:failure, activation.detach.on_deadline == :reschedule, reason}
    )
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

  # ---------------------------------------------------------------------------
  # Superstep settlement
  # ---------------------------------------------------------------------------

  # One settlement decision per validated superstep: any permanent failure
  # fails the run; a retrying, detaching, or partially-dispatched superstep
  # parks; otherwise the update barrier commits.
  defp settle(%Superstep{failures: [_ | _]} = superstep), do: fail_superstep(superstep)
  defp settle(%Superstep{retries: [_ | _]} = superstep), do: park(superstep)
  defp settle(%Superstep{detached: [_ | _]} = superstep), do: park(superstep)
  defp settle(%Superstep{remaining_active?: true} = superstep), do: park(superstep)
  defp settle(%Superstep{} = superstep), do: barrier(superstep)

  # The update barrier: pending sibling results parked by earlier retry
  # commits pass through the same validators as this dispatch's results
  # before the barrier commits (see `Superstep.absorb_pending/1`).
  defp barrier(%Superstep{} = superstep) do
    case Superstep.absorb_pending(superstep) do
      {:ok, superstep} -> commit(superstep)
      {:error, superstep} -> fail_superstep(superstep)
    end
  end

  defp fail_superstep(%Superstep{run: run, config: config} = superstep) do
    entries =
      attempt_failure_entries(run.step, superstep.results) ++
        Enum.map(superstep.failures, fn %Superstep.Failure{} = failure ->
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

    failed_nodes = superstep.failures |> Enum.map(& &1.node_id) |> Enum.uniq() |> Enum.sort()
    fail(run, config, entries, node_failure(superstep.failures, failed_nodes))
  end

  # Commits the retry/detach park: completed results move to pending writes,
  # each retrying task's next attempt (with its full activation identity and
  # accumulated failures) and retry deadline become durable, each detached
  # task's outstanding identity and recovery deadline become durable, and the
  # graph step does not advance. The checkpoint is sync so a crash during the
  # park resumes from this state instead of resetting the attempt position.
  defp park(%Superstep{run: run, config: config} = superstep) do
    now = config.clock.()
    activations_by_task = Map.new(superstep.activations, &{&1.task_id, &1})
    new_pending = Superstep.to_pending_writes(superstep)
    finalized_ids = Enum.map(new_pending, & &1.task_id)

    {parked_tasks, parked_timers} =
      Enum.reduce(superstep.retries ++ superstep.detached, {%{}, %{}}, fn result,
                                                                          {tasks, timers} ->
        activation = Map.fetch!(activations_by_task, result.task_id)
        {task, timer} = parked_attempt(run, activation, result, now, config)
        {Map.put(tasks, result.task_id, task), Map.put(timers, result.task_id, timer)}
      end)

    parked = %{
      run
      | active_tasks: run.active_tasks |> Map.drop(finalized_ids) |> Map.merge(parked_tasks),
        pending_writes: run.pending_writes ++ new_pending,
        timers: run.timers |> Map.drop(finalized_ids) |> Map.merge(parked_timers),
        updated_at: now
    }

    entries =
      attempt_failure_entries(parked.step, superstep.retries) ++
        detach_entries(parked.step, parked_tasks, superstep.detached)

    {type, reason} =
      if superstep.detached == [],
        do: {:retry_scheduled, :retry_backoff},
        else: {:detach_scheduled, :awaiting_detached}

    disposition = {:park, {:at, park_info(parked.timers, now).resume_at}, reason}

    propose(parked, type, entries, disposition, config,
      pending_attempts: new_pending,
      detached_workers: detached_workers(run, parked_tasks, superstep.detached)
    )
  end

  # Workers ride the moment as transient process-local state; the lifecycle
  # starts them only after this park durably commits, so a completion can
  # never precede its own detach park.
  defp detached_workers(run, parked_tasks, detached) do
    for result <- detached, worker = result.value.worker, not is_nil(worker) do
      %{
        ref: Docket.Detached.from_task(run.id, Map.fetch!(parked_tasks, result.task_id)),
        worker: worker
      }
    end
  end

  defp parked_attempt(run, activation, %{status: :retry} = result, now, _config) do
    {retry_task(run, activation, result), retry_timer(activation, now)}
  end

  defp parked_attempt(run, activation, %{status: :detached} = result, now, config) do
    deadline_ms =
      ExecutionPolicy.effective_detach_deadline_ms(
        activation.detach.deadline_ms,
        config.detach_deadline_ms
      )

    deadline = DateTime.add(now, deadline_ms, :millisecond)

    {detached_task(run, activation, result, now, deadline),
     %TimerState{kind: :detached_deadline, fires_at: deadline}}
  end

  # The parked next attempt keeps the committed activation identity and
  # appends this attempt's failure to the task's accumulated history.
  defp retry_task(run, activation, result) do
    next_attempt = activation.attempt + 1

    %TaskState{
      task_id: result.task_id,
      node_id: result.node_id,
      step: activation.step,
      attempt: next_attempt,
      status: :retry_scheduled,
      input_hash: activation.input_hash,
      idempotency_key: TaskState.idempotency_key(result.task_id, next_attempt),
      snapshot: activation.snapshot,
      source_versions: activation.source_versions,
      failures:
        prior_failures(run, result.task_id) ++
          [%{attempt: result.attempt, reason: inspect(result.value)}]
    }
  end

  # The detached attempt keeps its own identity outstanding: same attempt
  # number, no failure recorded, the detach instant and mandatory recovery
  # deadline durable, and the node's token retained for correlation.
  defp detached_task(run, activation, result, now, deadline) do
    %TaskState{
      task_id: result.task_id,
      node_id: result.node_id,
      step: activation.step,
      attempt: activation.attempt,
      status: :detached,
      input_hash: activation.input_hash,
      idempotency_key: activation.idempotency_key,
      snapshot: activation.snapshot,
      source_versions: activation.source_versions,
      started_at: now,
      deadline_at: deadline,
      failures: prior_failures(run, result.task_id),
      metadata: %{"detach_token" => result.value.token}
    }
  end

  defp prior_failures(run, task_id) do
    case Map.fetch(run.active_tasks, task_id) do
      {:ok, %TaskState{failures: failures}} -> failures
      :error -> []
    end
  end

  defp retry_timer(activation, now) do
    %TimerState{
      kind: :retry,
      fires_at: DateTime.add(now, activation.retry.backoff_ms, :millisecond)
    }
  end

  defp detach_entries(step, parked_tasks, detached) do
    for result <- detached do
      task = Map.fetch!(parked_tasks, result.task_id)

      entry(:node_detached, step,
        node_id: result.node_id,
        task_id: result.task_id,
        payload: %{
          "attempt" => result.attempt,
          "deadline_at" => DateTime.to_iso8601(task.deadline_at)
        }
      )
    end
  end

  defp commit(%Superstep{rtg: rtg, run: run, config: config} = superstep) do
    writes = Enum.map(superstep.writes, fn write -> {write.node_id, write.value} end)
    {channels, changed_fields, writers} = Algorithm.apply_state_writes(rtg, run.channels, writes)
    ok_node_ids = Enum.map(superstep.writes, & &1.node_id)

    case Algorithm.evaluate_edge_triggers(rtg, channels, ok_node_ids, changed_fields) do
      {:ok, triggers} ->
        commit_step(superstep, triggers, ok_node_ids, changed_fields, writers)

      {:error, {edge_id, reasons}} ->
        failure =
          Failure.new(
            "guard_evaluation_failed",
            "edge #{edge_id} guard evaluation failed",
            details: %{"edge_id" => edge_id, "reasons" => reasons}
          )

        fail(run, config, [], failure)
    end
  end

  defp commit_step(%Superstep{} = superstep, triggers, ok_node_ids, changed_fields, writers) do
    %Superstep{rtg: rtg, run: run, config: config} = superstep
    %{channels: channels, triggered: triggered, finish: finish} = triggers
    now = config.clock.()
    channels = clear_consumed_activations(rtg, channels, run.changed_channels)

    {interrupts, interrupt_node_ids, interrupt_writes} =
      build_interrupts(config, superstep.interrupts, now)

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
      commit_entries(rtg, run.step, superstep.writes, changed_fields, writers,
        triggered: triggered,
        finish: finish,
        interrupts: interrupts,
        interrupt_writes: interrupt_writes
      )

    type = if map_size(interrupts) == 0, do: :step_committed, else: :interrupt_requested
    propose(committed, type, entries, run_disposition(committed), config)
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

  defp build_interrupts(config, interrupt_writes, now) do
    Enum.reduce(interrupt_writes, {%{}, [], %{}}, fn write, {states, node_ids, writes} ->
      interrupt = write.value
      id = interrupt.id || config.id_generator.(:interrupt)

      state = %InterruptState{
        id: id,
        node_id: write.node_id,
        status: :open,
        resume_channel: interrupt.resume_channel,
        schema: interrupt.schema,
        created_at: now,
        metadata: interrupt.metadata || %{}
      }

      {
        Map.put(states, id, state),
        [write.node_id | node_ids],
        Map.put(writes, id, write)
      }
    end)
    |> then(fn {states, node_ids, writes} ->
      {states, Enum.reverse(node_ids), writes}
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

  # A committing superstep has no retry results, so unlike the retry park
  # and superstep failure paths, the barrier emits no attempt-failure
  # entries of its own.
  defp commit_entries(rtg, step, validated_writes, changed_fields, writers, extra) do
    Enum.map(validated_writes, fn write ->
      entry(:node_completed, step,
        node_id: write.node_id,
        task_id: write.task_id,
        payload: %{"attempt" => write.attempt}
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
        write = Map.fetch!(Keyword.fetch!(extra, :interrupt_writes), id)

        entry(:interrupt_requested, step,
          node_id: state.node_id,
          task_id: write.task_id,
          payload: %{
            "attempt" => write.attempt,
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

  # The durable cause for a permanent node failure. Per-node reasons remain in
  # the run's details independently of retained event history.
  defp node_failure(failures, failed_nodes) do
    errors = Map.new(failures, fn failure -> {failure.node_id, inspect(failure.reason)} end)

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

  defp complete(rtg, run, config) do
    now = config.clock.()
    output = Algorithm.project_output(rtg, run.channels)
    run = %{run | status: :done, output: output, finished_at: now, updated_at: now}

    entries = [
      entry(:run_completed, run.step, payload: %{"outputs" => Enum.sort(Map.keys(output))})
    ]

    propose(run, :run_completed, entries, {:park, :terminal, :run_completed}, config)
  end

  # Terminal failure absorbs the active superstep: no parked attempt, pending
  # write, or timer survives a failed run. The durable failure is the single
  # source of truth: the `:run_failed` event's node ID and payload are
  # projections of it, never assembled separately.
  defp fail(run, config, extra_entries, %Failure{} = failure) do
    now = config.clock.()

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

    failed_entry =
      entry(:run_failed, run.step, node_id: failure.node_id, payload: failure_payload(failure))

    entries = extra_entries ++ [failed_entry]
    propose(run, :run_failed, entries, {:park, :terminal, :run_failed}, config)
  end

  defp failure_payload(%Failure{} = failure) do
    failure.details
    |> Map.put("reason", failure.code)
    |> Map.put("message", failure.message)
  end

  # ---------------------------------------------------------------------------
  # Moment production
  # ---------------------------------------------------------------------------

  defp entry(type, step, opts) do
    Moment.event_entry(type, step, opts)
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
    Moment.propose(run, type, entries, disposition, config.clock.(), identity_opts)
  end
end
