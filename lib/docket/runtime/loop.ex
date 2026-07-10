defmodule Docket.Runtime.Loop do
  @moduledoc false

  # Processless transition functions over `Docket.Runtime.Graph` and
  # `Docket.Run`, shared by the supervised Runtime and `Docket.Test`.
  #
  # Every transition takes the runtime graph, the current committed run, and
  # options, and returns a new committed run plus checkpoint effects - or a
  # typed error with the previous run untouched. The loop owns checkpoint
  # emission and barrier semantics; deterministic execution logic lives in
  # `Docket.Runtime.Algorithm`.
  #
  # Checkpoint effects are `{:checkpoint, checkpoint, context, :accepted}`
  # for sync checkpoints already delivered inside the transition, and
  # `{:checkpoint, checkpoint, context, :pending}` for async checkpoints the
  # shell must deliver. A processless module cannot own async execution, so
  # async delivery belongs to the shell (inline: drained synchronously;
  # supervised Runtime: background task).

  alias Docket.{Checkpoint, Error, Event, Run, Schema, Wire}
  alias Docket.Run.{ChannelState, Failure, InterruptState}
  alias Docket.Runtime.{Algorithm, Config}

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
        {:ok, run, []}

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

      emit(run, :run_initialized, entries, config)
    end
  end

  defp init_saved(rtg, run, config) do
    _ = rtg
    run = %{run | updated_at: config.clock.()}
    entries = [entry(:run_initialized, run.step, payload: %{"resumed" => true})]
    emit(run, :run_initialized, entries, config)
  end

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
  Plans the next superstep from committed state.

  Returns:

  - `{:execute, run, activations}` - dispatch these, then call
    `apply_results/4`; the run is unchanged (planning commits nothing)
  - `{:wait, run, interrupt_ids}` - blocked on open interrupts
  - `{:terminal, run, effects}` - the run just completed or failed (the
    terminal checkpoint has been emitted), or was already terminal
  - `{:error, error}` - sync checkpoint failure or uninitialized run; the
    caller keeps the previous run
  """
  def plan(rtg, %Run{} = run, opts) do
    config = Config.resolve(opts)

    cond do
      run.status == :created ->
        {:error,
         Error.new(:invalid_run, "run #{inspect(run.id)} must be initialized before planning")}

      Run.terminal?(run) ->
        {:terminal, run, []}

      true ->
        case Algorithm.plan(rtg, run, config) do
          :done ->
            complete(rtg, run, config)

          {:wait, interrupt_ids} ->
            {:wait, run, interrupt_ids}

          {:failed, :max_supersteps_exceeded} ->
            limit = Algorithm.max_supersteps(rtg, config)

            terminal_fail(run, config, [],
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
                terminal_fail(run, config, [],
                  failure: Failure.new(Atom.to_string(error.type), error.message),
                  payload: %{"reason" => Atom.to_string(error.type), "message" => error.message}
                )
            end
        end
    end
  end

  defp complete(rtg, run, config) do
    now = config.clock.()
    output = Algorithm.project_output(rtg, run.channels)
    run = %{run | status: :done, output: output, finished_at: now, updated_at: now}

    entries = [
      entry(:run_completed, run.step, payload: %{"outputs" => Enum.sort(Map.keys(output))})
    ]

    case emit(run, :run_completed, entries, config) do
      {:ok, run, effects} -> {:terminal, run, effects}
      {:error, error} -> {:error, error}
    end
  end

  defp terminal_fail(run, config, extra_entries, opts) do
    case fail(run, config, extra_entries, opts) do
      {:ok, run, effects} -> {:terminal, run, effects}
      {:error, error} -> {:error, error}
    end
  end

  defp fail(run, config, extra_entries, opts) do
    now = config.clock.()
    failure = Keyword.fetch!(opts, :failure)
    run = %{run | status: :failed, failure: failure, finished_at: now, updated_at: now}

    entries =
      extra_entries ++ [entry(:run_failed, run.step, payload: Keyword.fetch!(opts, :payload))]

    emit(run, :run_failed, entries, config)
  end

  # ---------------------------------------------------------------------------
  # apply_results/4
  # ---------------------------------------------------------------------------

  @doc """
  The update barrier: validates task results, applies reducers, evaluates
  edge triggers, registers interrupts, and emits the barrier checkpoint.

  Any permanent node failure fails the superstep: no writes from the
  superstep commit and the run commits as `:failed` through a sync
  `:run_failed` checkpoint. Returns `{:ok, run, effects}` (the run may be
  terminal) or `{:error, error}` on sync checkpoint failure, keeping the
  previous committed run.
  """
  def apply_results(rtg, %Run{} = run, results, opts) do
    config = Config.resolve(opts)
    results = Enum.sort_by(results, & &1.node_id)

    {oks, interrupt_results, errors} = partition_results(results)
    {validated_writes, write_errors} = validate_writes(rtg, oks)
    {interrupt_specs, interrupt_errors} = validate_interrupts(rtg, run, interrupt_results)

    case errors ++ write_errors ++ interrupt_errors do
      [] ->
        commit(rtg, run, config, results, validated_writes, interrupt_specs)

      permanent ->
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
    Enum.reduce(results, {[], [], []}, fn result, {oks, interrupts, errors} ->
      case result.status do
        :ok ->
          {[result | oks], interrupts, errors}

        :interrupt ->
          {oks, [result | interrupts], errors}

        :error ->
          {oks, interrupts, [failure(result, result.value) | errors]}
      end
    end)
    |> then(fn {oks, interrupts, errors} ->
      {Enum.reverse(oks), Enum.reverse(interrupts), Enum.reverse(errors)}
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
        {interrupts, interrupt_node_ids} = build_interrupts(run, config, interrupt_specs, now)

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
            step: run.step + 1,
            status: :running,
            updated_at: now
        }

        committed = %{committed | status: eager_status(rtg, committed, config)}

        entries =
          commit_entries(rtg, run.step, results, validated_writes, changed_fields, writers,
            triggered: triggered,
            finish: finish,
            interrupts: interrupts
          )

        type = if map_size(interrupts) == 0, do: :step_committed, else: :interrupt_requested
        emit(committed, type, entries, config)
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

  defp build_interrupts(run, config, interrupt_specs, now) do
    Enum.reduce(interrupt_specs, {%{}, []}, fn {result, interrupt}, {states, node_ids} ->
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

      _ = run
      {Map.put(states, id, state), [result.node_id | node_ids]}
    end)
    |> then(fn {states, node_ids} -> {states, Enum.reverse(node_ids)} end)
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
        entry(:interrupt_requested, step,
          node_id: state.node_id,
          payload: %{"interrupt_id" => id, "resume_channel" => state.resume_channel}
        )
      end)
  end

  defp attempt_failure_entries(step, results) do
    Enum.flat_map(results, fn result ->
      retried =
        case result.status do
          # The final permanent failure is reported separately by the caller.
          :error -> Enum.drop(result.failures, -1)
          _other -> result.failures
        end

      Enum.map(retried, fn failure ->
        entry(:node_failed, step,
          node_id: result.node_id,
          task_id: result.task_id,
          payload: %{
            "attempt" => failure.attempt,
            "reason" => inspect(failure.reason),
            "permanent" => false
          }
        )
      end)
    end)
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
        do_resolve_interrupt(rtg, run, interrupt, value, config)
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

      emit(run, :interrupt_resolved, entries, config)
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
  # Events and checkpoint emission
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

  # Builds events with sequential seqs, bumps the run's counters, and emits
  # the checkpoint. Only this function constructs and delivers checkpoints;
  # no executor work is ever in flight when it runs.
  defp emit(run, type, entries, config) do
    now = config.clock.()

    {events, event_seq} =
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

    run = %{run | event_seq: event_seq, checkpoint_seq: run.checkpoint_seq + 1}
    delivery = Checkpoint.delivery(type, config.checkpoint_overrides)

    checkpoint = %Checkpoint{
      type: type,
      delivery: delivery,
      seq: run.checkpoint_seq,
      run: run,
      events: events,
      created_at: now
    }

    context = %Checkpoint.Context{
      run_id: run.id,
      graph_id: run.graph_id,
      graph_hash: run.graph_hash,
      application: config.context
    }

    case delivery do
      :sync ->
        case deliver_checkpoint(config.checkpoint, checkpoint, context) do
          :ok ->
            Docket.Telemetry.emit_events(run, events)
            {:ok, run, [{:checkpoint, checkpoint, context, :accepted}]}

          {:error, reason} ->
            {:error,
             Error.new(:checkpoint_failed, "sync #{type} checkpoint was not accepted",
               phase: type,
               reason: reason
             )}
        end

      :async ->
        Docket.Telemetry.emit_events(run, events)
        {:ok, run, [{:checkpoint, checkpoint, context, :pending}]}
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
