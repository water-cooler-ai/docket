defmodule Docket.Run.Serializer do
  @moduledoc false

  # Internal implementation of the canonical wire serialization for
  # `Docket.Run` documents. The only public entry/exit points are
  # `Docket.Run.to_map/2` and `Docket.Run.from_map/2` (and `from_map!/2`).
  #
  # The wire format mirrors the graph document conventions: string keys only,
  # empty collections and nil optionals omitted, open content restricted to
  # durable JSON-safe terms, "$"-prefixed map keys reserved, strict load
  # validation that never creates atoms.
  #
  # Version 3 adds `graph_compiler_abi`, binding durable operational runs to
  # the exact published execution-artifact ABI. It remains optional for the
  # storage-free legacy driver.
  #
  # Version 2 added the terminal `failure` payload and admitted only the five
  # durable statuses: the private `:created` sentinel is rejected on dump
  # and load. Version-1 documents are not loadable; there is no released
  # userbase to migrate.
  #
  # Version 2 also carries the active superstep: `active_tasks` (parked
  # attempts with their activation identity), `pending_writes` (completed
  # sibling results held until the barrier), and `timers` (retry deadlines).
  # The three keys share the failure bump's version rather than allocating
  # another one; documents dumped without an active superstep omit them and
  # load with the empty defaults, in both directions of the transition.

  alias Docket.Interrupt
  alias Docket.Run
  alias Docket.Run.{ChannelState, Failure, InterruptState, PendingWrite, TaskState, TimerState}
  alias Docket.Wire

  @version 3

  @statuses %{
    "running" => :running,
    "waiting" => :waiting,
    "done" => :done,
    "failed" => :failed,
    "cancelled" => :cancelled
  }
  @statuses_out Map.new(@statuses, fn {string, atom} -> {atom, string} end)

  @interrupt_statuses %{"open" => :open, "resolved" => :resolved}
  @interrupt_statuses_out Map.new(@interrupt_statuses, fn {string, atom} -> {atom, string} end)

  @run_keys ~w(version id graph_id graph_hash graph_compiler_abi status step input output failure
               started_at updated_at finished_at channels changed_channels
               pending_nodes interrupts active_tasks pending_writes timers
               checkpoint_seq event_seq metadata)
  @channel_keys ~w(value version barrier_seen)
  @interrupt_keys ~w(node_id status resume_channel prompt schema created_at
                     resolved_at metadata)
  @failure_keys ~w(code message node_id details)
  @active_task_keys ~w(node_id attempt input_hash snapshot source_versions
                       failures)
  @task_failure_keys ~w(attempt reason)
  @pending_update_keys ~w(task_id node_id attempt kind update)
  @pending_interrupt_keys ~w(task_id node_id attempt kind interrupt)
  @pending_interrupt_value_keys ~w(id node_id resume_channel prompt schema metadata)
  @pending_kinds %{"update" => :update, "interrupt" => :interrupt}
  @pending_kinds_out Map.new(@pending_kinds, fn {string, atom} -> {atom, string} end)
  @timer_keys ~w(kind fires_at)
  @timer_kinds %{"retry" => :retry}
  @timer_kinds_out Map.new(@timer_kinds, fn {string, atom} -> {atom, string} end)

  # ---------------------------------------------------------------------------
  # Dump
  # ---------------------------------------------------------------------------

  @spec dump(Run.t(), keyword()) :: map()
  def dump(%Run{} = run, _opts \\ []) do
    validate_failure!(run)
    validate_active_superstep!(run, :invalid_run)

    %{
      "version" => @version,
      "id" => required_string!(run.id, "run id"),
      "graph_id" => required_string!(run.graph_id, "run graph_id"),
      "status" => dump_status(run.status),
      "step" => run.step,
      "checkpoint_seq" => run.checkpoint_seq,
      "event_seq" => run.event_seq
    }
    |> put_present("graph_hash", run.graph_hash)
    |> put_present("graph_compiler_abi", run.graph_compiler_abi)
    |> put_open_map("input", run.input || %{}, "run input")
    |> put_present("output", dump_output(run.output))
    |> put_present("failure", dump_failure(run.failure))
    |> put_present("started_at", dump_timestamp(run.started_at))
    |> put_present("updated_at", dump_timestamp(run.updated_at))
    |> put_present("finished_at", dump_timestamp(run.finished_at))
    |> put_collection("channels", run.channels, &dump_channel/1)
    |> put_id_set("changed_channels", run.changed_channels)
    |> put_id_set("pending_nodes", run.pending_nodes)
    |> put_collection("interrupts", run.interrupts, &dump_interrupt/1)
    |> put_active_tasks(run)
    |> put_pending_writes(run)
    |> put_timers(run)
    |> put_open_map("metadata", run.metadata, "run metadata")
  end

  # ---------------------------------------------------------------------------
  # Active superstep
  # ---------------------------------------------------------------------------

  # Shared dump/load invariants for retry-parked execution state: only a
  # `:running` run carries it, pending results require active tasks, every
  # active task parks with exactly one retry timer, and a node contributes
  # at most one result or parked attempt per superstep. The retry-timer
  # coverage check is scoped by kind so future non-retry timers are not
  # bound to active tasks.
  defp validate_active_superstep!(%Run{} = run, error_type) do
    retry_timer_ids = for {timer_id, %TimerState{kind: :retry}} <- run.timers, do: timer_id

    cond do
      map_size(run.active_tasks) == 0 and run.pending_writes == [] and retry_timer_ids == [] ->
        :ok

      map_size(run.active_tasks) == 0 ->
        invalid!(
          error_type,
          "pending writes and retry timers are only durable while tasks are active"
        )

      run.status != :running ->
        invalid!(
          error_type,
          "an active superstep is only durable on a running run, got status " <>
            inspect(run.status)
        )

      Enum.sort(retry_timer_ids) != Enum.sort(Map.keys(run.active_tasks)) ->
        invalid!(
          error_type,
          "active tasks and retry timers must cover the same task IDs",
          %{
            active_tasks: Enum.sort(Map.keys(run.active_tasks)),
            timers: Enum.sort(retry_timer_ids)
          }
        )

      true ->
        node_ids = active_node_ids!(run, error_type) ++ pending_node_ids!(run, error_type)

        case node_ids -- Enum.uniq(node_ids) do
          [] ->
            :ok

          duplicated ->
            invalid!(
              error_type,
              "a node has at most one result or parked attempt per superstep, got " <>
                "duplicates for #{inspect(Enum.sort(Enum.uniq(duplicated)))}"
            )
        end
    end
  end

  defp active_node_ids!(run, error_type) do
    Enum.map(run.active_tasks, fn
      {_task_id, %TaskState{node_id: node_id}} ->
        node_id

      {task_id, other} ->
        invalid!(
          error_type,
          "active task #{inspect(task_id)} must be a Docket.Run.TaskState, got #{inspect(other)}"
        )
    end)
  end

  defp pending_node_ids!(run, error_type) do
    Enum.map(run.pending_writes, fn
      %PendingWrite{node_id: node_id} ->
        node_id

      other ->
        invalid!(
          error_type,
          "pending write must be a Docket.Run.PendingWrite, got #{inspect(other)}"
        )
    end)
  end

  defp put_active_tasks(map, %Run{active_tasks: tasks}) when map_size(tasks) == 0, do: map

  defp put_active_tasks(map, run) do
    Map.put(
      map,
      "active_tasks",
      Map.new(run.active_tasks, fn {task_id, task} ->
        {task_id, dump_active_task(run, task_id, task)}
      end)
    )
  end

  # Dump enforces the same task-state consistency as load, so a checkpoint
  # the host persists is always reloadable: an inconsistent in-memory task
  # fails at the write boundary, never at recovery.
  defp dump_active_task(run, task_id, %TaskState{} = task) do
    location = "active task #{inspect(task_id)}"
    failures = dump_task_failures(task.failures, location)

    cond do
      task.task_id != task_id ->
        invalid!(
          :invalid_run,
          "#{location} is keyed by #{inspect(task_id)} but carries task_id " <>
            inspect(task.task_id)
        )

      task.step != run.step ->
        invalid!(
          :invalid_run,
          "#{location} step #{inspect(task.step)} does not match run step #{run.step}"
        )

      task_id != TaskState.task_id(run.id, run.step, task.node_id) ->
        invalid!(
          :invalid_run,
          "#{location} does not carry the stable task identity for node " <>
            inspect(task.node_id)
        )

      Enum.map(failures, & &1["attempt"]) != Enum.to_list(1..length(failures)) ->
        invalid!(
          :invalid_run,
          "#{location} failures must record attempts 1..n in order"
        )

      task.attempt != length(failures) + 1 ->
        invalid!(
          :invalid_run,
          "#{location} attempt #{inspect(task.attempt)} does not follow its " <>
            "#{length(failures)} recorded failed attempt(s)"
        )

      TaskState.snapshot_hash(task.snapshot || %{}) !=
          required_string!(task.input_hash, "#{location} input_hash") ->
        invalid!(
          :invalid_run,
          "#{location} snapshot does not match its recorded input_hash"
        )

      true ->
        %{
          "node_id" => required_string!(task.node_id, "#{location} node_id"),
          "attempt" => task.attempt,
          "input_hash" => task.input_hash,
          "failures" => failures
        }
        |> put_open_map("snapshot", task.snapshot || %{}, "#{location} snapshot")
        |> put_source_versions(task.source_versions || %{}, location)
    end
  end

  defp dump_active_task(_run, task_id, other) do
    invalid!(
      :invalid_run,
      "active task #{inspect(task_id)} must be a Docket.Run.TaskState, got #{inspect(other)}"
    )
  end

  defp dump_task_failures(failures, location) when is_list(failures) and failures != [] do
    Enum.map(failures, fn
      %{attempt: attempt, reason: reason} when is_integer(attempt) and attempt >= 1 ->
        %{
          "attempt" => attempt,
          "reason" => required_string!(reason, "#{location} failure reason")
        }

      other ->
        invalid!(
          :invalid_run,
          "#{location} failures must be %{attempt, reason} entries, got #{inspect(other)}"
        )
    end)
  end

  defp dump_task_failures(other, location) do
    invalid!(
      :invalid_run,
      "#{location} must record at least one failed attempt, got #{inspect(other)}"
    )
  end

  defp put_source_versions(map, versions, location)
       when is_map(versions) and not is_struct(versions) do
    case map_size(versions) do
      0 ->
        map

      _present ->
        Map.put(
          map,
          "source_versions",
          Map.new(versions, fn
            {key, value} when is_binary(key) and is_integer(value) and value >= 0 ->
              {key, value}

            {key, value} ->
              invalid!(
                :invalid_run,
                "#{location} source_versions entries must map channel IDs to " <>
                  "non-negative integers, got #{inspect({key, value})}"
              )
          end)
        )
    end
  end

  defp put_source_versions(_map, other, location) do
    invalid!(:invalid_run, "#{location} source_versions must be a map, got #{inspect(other)}")
  end

  defp put_pending_writes(map, %Run{pending_writes: []}), do: map

  defp put_pending_writes(map, run) do
    Map.put(map, "pending_writes", Enum.map(run.pending_writes, &dump_pending_write(run, &1)))
  end

  defp dump_pending_write(run, %PendingWrite{} = pending) do
    location = "pending write #{inspect(pending.node_id)}"

    unless pending.task_id == TaskState.task_id(run.id, run.step, pending.node_id) do
      invalid!(
        :invalid_run,
        "#{location} does not carry the stable task identity, got #{inspect(pending.task_id)}"
      )
    end

    unless is_integer(pending.attempt) and pending.attempt >= 1 do
      invalid!(
        :invalid_run,
        "#{location} attempt must be a positive integer, got #{inspect(pending.attempt)}"
      )
    end

    base = %{
      "task_id" => pending.task_id,
      "node_id" => required_string!(pending.node_id, "#{location} node_id"),
      "attempt" => pending.attempt
    }

    case {pending.kind, pending.value} do
      {:update, value} when is_map(value) and not is_struct(value) ->
        base
        |> Map.put("kind", Map.fetch!(@pending_kinds_out, :update))
        |> Map.put("update", Wire.dump_value!(value, "#{location} update"))

      {:interrupt, %Interrupt{} = interrupt} ->
        validate_pending_interrupt_node!(
          interrupt.node_id,
          pending.node_id,
          location,
          :invalid_run
        )

        base
        |> Map.put("kind", Map.fetch!(@pending_kinds_out, :interrupt))
        |> Map.put("interrupt", dump_pending_interrupt(interrupt, location))

      {kind, value} ->
        invalid!(
          :invalid_run,
          "#{location} must pair kind :update with an update map or kind :interrupt " <>
            "with a Docket.Interrupt, got #{inspect({kind, value})}"
        )
    end
  end

  defp dump_pending_write(_run, other) do
    invalid!(
      :invalid_run,
      "pending write must be a Docket.Run.PendingWrite, got #{inspect(other)}"
    )
  end

  defp validate_pending_interrupt_node!(interrupt_node_id, node_id, location, error_type) do
    unless interrupt_node_id in [nil, node_id] do
      invalid!(
        error_type,
        "#{location} interrupt node_id #{inspect(interrupt_node_id)} does not match " <>
          "the pending write's node"
      )
    end
  end

  defp dump_pending_interrupt(%Interrupt{} = interrupt, location) do
    %{
      "resume_channel" =>
        required_string!(interrupt.resume_channel, "#{location} interrupt resume_channel")
    }
    |> put_present("id", optional_string!(interrupt.id, "#{location} interrupt id"))
    |> put_present(
      "node_id",
      optional_string!(interrupt.node_id, "#{location} interrupt node_id")
    )
    |> put_present("prompt", optional_string!(interrupt.prompt, "#{location} interrupt prompt"))
    |> put_present("schema", dump_schema(interrupt.schema))
    |> put_open_map("metadata", interrupt.metadata || %{}, "#{location} interrupt metadata")
  end

  defp put_timers(map, %Run{timers: timers}) when map_size(timers) == 0, do: map

  defp put_timers(map, run) do
    Map.put(
      map,
      "timers",
      Map.new(run.timers, fn {timer_id, timer} -> {timer_id, dump_timer(timer_id, timer)} end)
    )
  end

  defp dump_timer(_timer_id, %TimerState{kind: :retry, fires_at: %DateTime{} = fires_at}) do
    %{
      "kind" => Map.fetch!(@timer_kinds_out, :retry),
      "fires_at" => DateTime.to_iso8601(fires_at)
    }
  end

  defp dump_timer(timer_id, other) do
    invalid!(
      :invalid_run,
      "timer #{inspect(timer_id)} must be a retry Docket.Run.TimerState with a " <>
        "DateTime deadline, got #{inspect(other)}"
    )
  end

  defp dump_channel(%ChannelState{} = channel) do
    %{"version" => channel.version}
    |> put_present("value", Wire.dump_value!(channel.value, "channel value"))
    |> then(fn map ->
      case channel.barrier_seen do
        [] -> map
        seen -> Map.put(map, "barrier_seen", Enum.sort(seen))
      end
    end)
  end

  defp dump_interrupt(%InterruptState{} = interrupt) do
    %{
      "node_id" => required_string!(interrupt.node_id, "interrupt node_id"),
      "status" => dump_interrupt_status(interrupt.status),
      "resume_channel" => required_string!(interrupt.resume_channel, "interrupt resume_channel")
    }
    |> put_present("prompt", interrupt.prompt)
    |> put_present("schema", dump_schema(interrupt.schema))
    |> put_present("created_at", dump_timestamp(interrupt.created_at))
    |> put_present("resolved_at", dump_timestamp(interrupt.resolved_at))
    |> put_open_map("metadata", interrupt.metadata, "interrupt metadata")
  end

  defp dump_status(:created) do
    invalid!(
      :invalid_run,
      "run status :created is a private initialization sentinel and cannot be " <>
        "written durably; initialize the run first"
    )
  end

  defp dump_status(status) do
    Map.get(@statuses_out, status) ||
      invalid!(
        :invalid_run,
        "run status must be one of #{inspect(Map.keys(@statuses_out))}, got #{inspect(status)}"
      )
  end

  defp validate_failure!(%Run{} = run) do
    case Run.validate_failure(run) do
      :ok -> :ok
      {:error, %Docket.Error{} = error} -> raise error
    end
  end

  defp dump_failure(nil), do: nil

  defp dump_failure(%Failure{} = failure) do
    %{
      "code" => required_string!(failure.code, "failure code"),
      "message" => required_string!(failure.message, "failure message")
    }
    |> put_present("node_id", dump_failure_node_id(failure.node_id))
    |> put_open_map("details", failure.details, "failure details")
  end

  defp dump_failure_node_id(nil), do: nil
  defp dump_failure_node_id(node_id), do: required_string!(node_id, "failure node_id")

  defp dump_interrupt_status(status) do
    Map.get(@interrupt_statuses_out, status) ||
      invalid!(
        :invalid_run,
        "interrupt status must be :open or :resolved, got #{inspect(status)}"
      )
  end

  defp dump_output(nil), do: nil

  defp dump_output(output) when is_map(output) and not is_struct(output) do
    Wire.dump_value!(output, "run output")
  end

  defp dump_output(other) do
    invalid!(:invalid_run, "run output must be a map or nil, got #{inspect(other)}")
  end

  defp dump_timestamp(nil), do: nil
  defp dump_timestamp(%DateTime{} = timestamp), do: DateTime.to_iso8601(timestamp)

  defp dump_timestamp(other) do
    invalid!(:invalid_run, "run timestamps must be DateTime or nil, got #{inspect(other)}")
  end

  defp dump_schema(schema) do
    Docket.Graph.Serializer.dump_schema(schema)
  rescue
    error in Docket.Graph.Error ->
      invalid!(:invalid_run, "interrupt schema is not durable: #{error.message}")
  end

  defp required_string!(value, _label) when is_binary(value), do: value

  defp required_string!(value, label) do
    invalid!(:invalid_run, "#{label} must be a string, got #{inspect(value)}")
  end

  defp optional_string!(nil, _label), do: nil
  defp optional_string!(value, label), do: required_string!(value, label)

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)

  defp put_open_map(map, _key, value, _location) when value == %{}, do: map

  defp put_open_map(map, key, value, location) when is_map(value) and not is_struct(value) do
    Map.put(map, key, Wire.dump_value!(value, location))
  end

  defp put_open_map(_map, key, other, _location) do
    invalid!(:invalid_run, "#{key} must be a string-keyed map, got #{inspect(other)}")
  end

  defp put_id_set(map, key, %MapSet{} = set) do
    case MapSet.to_list(set) do
      [] -> map
      ids -> Map.put(map, key, Enum.sort(ids))
    end
  end

  defp put_collection(map, _key, collection, _fun) when map_size(collection) == 0, do: map

  defp put_collection(map, key, collection, fun) do
    Map.put(map, key, Map.new(collection, fn {id, record} -> {id, fun.(record)} end))
  end

  # ---------------------------------------------------------------------------
  # Load
  # ---------------------------------------------------------------------------

  @spec load!(map(), keyword()) :: Run.t()
  def load!(map, _opts \\ []) do
    unless is_map(map) and not is_struct(map) do
      invalid!(:invalid_document, "run document must be a plain map, got #{inspect(map)}")
    end

    assert_string_keys!(map, "run document")
    load_version!(map)
    assert_known_keys!(map, @run_keys, "run")

    id = load_required_string!(map, "id", "run")
    step = load_non_neg_integer!(map, "step", "run")

    run = %Run{
      id: id,
      graph_id: load_required_string!(map, "graph_id", "run"),
      graph_hash: load_optional_string!(map, "graph_hash", "run"),
      graph_compiler_abi: load_optional_string!(map, "graph_compiler_abi", "run"),
      status: load_enum!(map, "status", @statuses, "run status"),
      step: step,
      input: load_open_map!(map, "input", "run input"),
      output: load_output!(map),
      started_at: load_timestamp!(map, "started_at"),
      updated_at: load_timestamp!(map, "updated_at"),
      finished_at: load_timestamp!(map, "finished_at"),
      channels: load_collection!(map, "channels", &load_channel!/2),
      changed_channels: load_id_set!(map, "changed_channels"),
      pending_nodes: load_id_set!(map, "pending_nodes"),
      interrupts: load_collection!(map, "interrupts", &load_interrupt!/2),
      active_tasks: load_active_tasks!(map, id, step),
      pending_writes: load_pending_writes!(map, id, step),
      timers: load_timers!(map),
      checkpoint_seq: load_non_neg_integer!(map, "checkpoint_seq", "run"),
      event_seq: load_non_neg_integer!(map, "event_seq", "run"),
      metadata: load_open_map!(map, "metadata", "run metadata")
    }

    run = %{run | failure: load_failure!(map)}
    validate_active_superstep!(run, :invalid_document)

    case Run.validate_failure(run) do
      :ok -> run
      {:error, %Docket.Error{message: message}} -> invalid!(:invalid_document, message)
    end
  end

  defp load_active_tasks!(map, run_id, run_step) do
    load_collection!(map, "active_tasks", fn task_id, task_map ->
      load_active_task!(task_id, task_map, run_id, run_step)
    end)
  end

  defp load_active_task!(task_id, map, run_id, run_step) do
    location = "active task #{inspect(task_id)}"
    assert_string_keys!(map, location)
    assert_known_keys!(map, @active_task_keys, location)

    node_id = load_required_string!(map, "node_id", location)
    attempt = load_pos_integer!(map, "attempt", location)
    input_hash = load_required_string!(map, "input_hash", location)
    snapshot = load_open_map!(map, "snapshot", "#{location} snapshot")
    source_versions = load_source_versions!(map, location)
    failures = load_task_failures!(map, location)

    cond do
      task_id != TaskState.task_id(run_id, run_step, node_id) ->
        invalid!(
          :invalid_document,
          "#{location} is not the stable task identity for node #{inspect(node_id)}"
        )

      attempt != length(failures) + 1 ->
        invalid!(
          :invalid_document,
          "#{location} attempt #{attempt} does not follow its #{length(failures)} " <>
            "recorded failed attempt(s)"
        )

      TaskState.snapshot_hash(snapshot) != input_hash ->
        invalid!(
          :invalid_document,
          "#{location} snapshot does not match its recorded input_hash"
        )

      true ->
        %TaskState{
          task_id: task_id,
          node_id: node_id,
          step: run_step,
          attempt: attempt,
          status: :retry_scheduled,
          input_hash: input_hash,
          idempotency_key: TaskState.idempotency_key(task_id, attempt),
          snapshot: snapshot,
          source_versions: source_versions,
          failures: failures
        }
    end
  end

  defp load_task_failures!(map, location) do
    case Map.get(map, "failures") do
      failures when is_list(failures) and failures != [] ->
        loaded =
          Enum.map(failures, fn entry ->
            unless is_map(entry) and not is_struct(entry) do
              invalid!(
                :invalid_document,
                "#{location} failures entries must be maps, got #{inspect(entry)}"
              )
            end

            assert_string_keys!(entry, "#{location} failure")
            assert_known_keys!(entry, @task_failure_keys, "#{location} failure")

            %{
              attempt: load_pos_integer!(entry, "attempt", "#{location} failure"),
              reason: load_required_string!(entry, "reason", "#{location} failure")
            }
          end)

        unless Enum.map(loaded, & &1.attempt) == Enum.to_list(1..length(loaded)) do
          invalid!(
            :invalid_document,
            "#{location} failures must record attempts 1..n in order"
          )
        end

        loaded

      other ->
        invalid!(
          :invalid_document,
          "#{location} must record at least one failed attempt, got #{inspect(other)}"
        )
    end
  end

  defp load_source_versions!(map, location) do
    case Map.get(map, "source_versions") do
      nil ->
        %{}

      versions when is_map(versions) and not is_struct(versions) ->
        Map.new(versions, fn
          {key, value} when is_binary(key) and is_integer(value) and value >= 0 ->
            {key, value}

          {key, value} ->
            invalid!(
              :invalid_document,
              "#{location} source_versions entries must map channel IDs to " <>
                "non-negative integers, got #{inspect({key, value})}"
            )
        end)

      other ->
        invalid!(
          :invalid_document,
          "#{location} source_versions must be a map, got #{inspect(other)}"
        )
    end
  end

  defp load_pending_writes!(map, run_id, run_step) do
    case Map.get(map, "pending_writes") do
      nil ->
        []

      pending when is_list(pending) ->
        Enum.map(pending, &load_pending_write!(&1, run_id, run_step))

      other ->
        invalid!(:invalid_document, "pending_writes must be a list, got #{inspect(other)}")
    end
  end

  defp load_pending_write!(entry, run_id, run_step)
       when is_map(entry) and not is_struct(entry) do
    assert_string_keys!(entry, "pending write")

    kind = load_enum!(entry, "kind", @pending_kinds, "pending write kind")
    node_id = load_required_string!(entry, "node_id", "pending write")
    location = "pending write #{inspect(node_id)}"
    task_id = load_required_string!(entry, "task_id", location)
    attempt = load_pos_integer!(entry, "attempt", location)

    unless task_id == TaskState.task_id(run_id, run_step, node_id) do
      invalid!(
        :invalid_document,
        "#{location} task_id is not the stable task identity for this run and step"
      )
    end

    value =
      case kind do
        :update ->
          assert_known_keys!(entry, @pending_update_keys, location)

          case Map.fetch(entry, "update") do
            {:ok, update} when is_map(update) and not is_struct(update) ->
              Wire.load_value!(update, "#{location} update")

            {:ok, other} ->
              invalid!(
                :invalid_document,
                "#{location} update must be a map, got #{inspect(other)}"
              )

            :error ->
              invalid!(:invalid_document, "#{location} is missing required key \"update\"")
          end

        :interrupt ->
          assert_known_keys!(entry, @pending_interrupt_keys, location)
          interrupt = load_pending_interrupt!(Map.get(entry, "interrupt"), location)

          validate_pending_interrupt_node!(
            interrupt.node_id,
            node_id,
            location,
            :invalid_document
          )

          interrupt
      end

    %PendingWrite{task_id: task_id, node_id: node_id, attempt: attempt, kind: kind, value: value}
  end

  defp load_pending_write!(other, _run_id, _run_step) do
    invalid!(:invalid_document, "pending write entries must be maps, got #{inspect(other)}")
  end

  defp load_pending_interrupt!(value, location) when is_map(value) and not is_struct(value) do
    interrupt_location = "#{location} interrupt"
    assert_string_keys!(value, interrupt_location)
    assert_known_keys!(value, @pending_interrupt_value_keys, interrupt_location)

    %Interrupt{
      id: load_optional_string!(value, "id", interrupt_location),
      node_id: load_optional_string!(value, "node_id", interrupt_location),
      resume_channel: load_required_string!(value, "resume_channel", interrupt_location),
      prompt: load_optional_string!(value, "prompt", interrupt_location),
      schema: load_schema!(Map.get(value, "schema"), interrupt_location),
      metadata: load_open_map!(value, "metadata", "#{interrupt_location} metadata")
    }
  end

  defp load_pending_interrupt!(other, location) do
    invalid!(:invalid_document, "#{location} interrupt must be a map, got #{inspect(other)}")
  end

  defp load_timers!(map) do
    load_collection!(map, "timers", fn timer_id, timer_map ->
      location = "timer #{inspect(timer_id)}"
      assert_string_keys!(timer_map, location)
      assert_known_keys!(timer_map, @timer_keys, location)

      kind = load_enum!(timer_map, "kind", @timer_kinds, "#{location} kind")

      fires_at =
        case load_timestamp!(timer_map, "fires_at", location) do
          %DateTime{} = fires_at ->
            fires_at

          nil ->
            invalid!(:invalid_document, "#{location} is missing required key \"fires_at\"")
        end

      %TimerState{kind: kind, fires_at: fires_at}
    end)
  end

  defp load_version!(map) do
    case Map.fetch(map, "version") do
      {:ok, @version} ->
        :ok

      {:ok, version} when is_integer(version) and version >= 1 ->
        invalid!(
          :unsupported_schema_version,
          "run document version #{version} is not the supported version #{@version}",
          %{version: version, supported: @version}
        )

      {:ok, other} ->
        invalid!(
          :invalid_document,
          "run version must be a positive integer, got #{inspect(other)}"
        )

      :error ->
        invalid!(:invalid_document, "run document is missing required key \"version\"")
    end
  end

  defp load_failure!(map) do
    case Map.get(map, "failure") do
      nil ->
        nil

      value when is_map(value) and not is_struct(value) ->
        assert_string_keys!(value, "run failure")
        assert_known_keys!(value, @failure_keys, "run failure")

        %Failure{
          code: load_required_string!(value, "code", "run failure"),
          message: load_required_string!(value, "message", "run failure"),
          node_id: load_optional_string!(value, "node_id", "run failure"),
          details: load_open_map!(value, "details", "run failure details")
        }

      other ->
        invalid!(:invalid_document, "run failure must be a map, got #{inspect(other)}")
    end
  end

  defp load_channel!(id, map) do
    assert_string_keys!(map, "channel #{inspect(id)}")
    assert_known_keys!(map, @channel_keys, "channel #{inspect(id)}")

    version = load_non_neg_integer!(map, "version", "channel #{inspect(id)}")

    unless version >= 1 do
      invalid!(
        :invalid_document,
        "channel #{inspect(id)} version must be at least 1, got #{version}"
      )
    end

    %ChannelState{
      channel_id: id,
      value: Wire.load_value!(Map.get(map, "value"), "channel #{inspect(id)} value"),
      version: version,
      barrier_seen:
        load_string_list!(Map.get(map, "barrier_seen"), "channel #{inspect(id)} barrier_seen")
    }
  end

  defp load_interrupt!(id, map) do
    assert_string_keys!(map, "interrupt #{inspect(id)}")
    assert_known_keys!(map, @interrupt_keys, "interrupt #{inspect(id)}")

    %InterruptState{
      id: id,
      node_id: load_required_string!(map, "node_id", "interrupt #{inspect(id)}"),
      status: load_enum!(map, "status", @interrupt_statuses, "interrupt #{inspect(id)} status"),
      resume_channel: load_required_string!(map, "resume_channel", "interrupt #{inspect(id)}"),
      prompt: load_optional_string!(map, "prompt", "interrupt #{inspect(id)}"),
      schema: load_schema!(Map.get(map, "schema"), "interrupt #{inspect(id)}"),
      created_at: load_timestamp!(map, "created_at"),
      resolved_at: load_timestamp!(map, "resolved_at"),
      metadata: load_open_map!(map, "metadata", "interrupt #{inspect(id)} metadata")
    }
  end

  defp load_schema!(nil, _location), do: nil

  defp load_schema!(value, location) do
    Docket.Graph.Serializer.load_schema!(value, location)
  rescue
    error in Docket.Graph.Error ->
      invalid!(:invalid_document, error.message, error.details)
  end

  defp load_output!(map) do
    case Map.get(map, "output") do
      nil ->
        nil

      value when is_map(value) and not is_struct(value) ->
        Wire.load_value!(value, "run output")

      other ->
        invalid!(:invalid_document, "run output must be a map, got #{inspect(other)}")
    end
  end

  defp load_timestamp!(map, key, location \\ "run") do
    case Map.get(map, key) do
      nil ->
        nil

      value when is_binary(value) ->
        case DateTime.from_iso8601(value) do
          {:ok, timestamp, _offset} ->
            timestamp

          {:error, reason} ->
            invalid!(
              :invalid_document,
              "#{location} #{key} is not a valid ISO8601 timestamp: #{inspect(reason)}",
              %{key: key, value: value}
            )
        end

      other ->
        invalid!(:invalid_document, "#{location} #{key} must be a string, got #{inspect(other)}")
    end
  end

  defp load_enum!(map, key, table, label) do
    value = load_required_string!(map, key, label)

    Map.get(table, value) ||
      invalid!(
        :invalid_document,
        "unknown #{label} #{inspect(value)}; expected one of #{inspect(Map.keys(table))}"
      )
  end

  defp load_collection!(map, key, fun) do
    case Map.get(map, key) do
      nil ->
        %{}

      collection when is_map(collection) and not is_struct(collection) ->
        assert_string_keys!(collection, key)

        Map.new(collection, fn {id, record} ->
          unless is_map(record) and not is_struct(record) do
            invalid!(
              :invalid_document,
              "#{key} entry #{inspect(id)} must be a map, got #{inspect(record)}"
            )
          end

          {id, fun.(id, record)}
        end)

      other ->
        invalid!(:invalid_document, "#{key} must be a map, got #{inspect(other)}")
    end
  end

  defp load_id_set!(map, key) do
    map
    |> Map.get(key)
    |> load_string_list!("run #{key}")
    |> MapSet.new()
  end

  defp load_string_list!(nil, _location), do: []

  defp load_string_list!(list, location) when is_list(list) do
    Enum.each(list, fn
      value when is_binary(value) ->
        :ok

      other ->
        invalid!(:invalid_document, "#{location} entries must be strings, got #{inspect(other)}")
    end)

    list
  end

  defp load_string_list!(other, location) do
    invalid!(:invalid_document, "#{location} must be a list, got #{inspect(other)}")
  end

  defp load_open_map!(map, key, location) do
    case Map.get(map, key) do
      nil ->
        %{}

      value when is_map(value) and not is_struct(value) ->
        Wire.load_value!(value, location)

      other ->
        invalid!(:invalid_document, "#{location} must be a map, got #{inspect(other)}")
    end
  end

  defp load_required_string!(map, key, location) do
    case Map.fetch(map, key) do
      {:ok, value} when is_binary(value) ->
        value

      {:ok, other} ->
        invalid!(:invalid_document, "#{location} #{key} must be a string, got #{inspect(other)}")

      :error ->
        invalid!(
          :invalid_document,
          "#{location} document is missing required key #{inspect(key)}"
        )
    end
  end

  defp load_optional_string!(map, key, location) do
    case Map.get(map, key) do
      nil ->
        nil

      value when is_binary(value) ->
        value

      other ->
        invalid!(:invalid_document, "#{location} #{key} must be a string, got #{inspect(other)}")
    end
  end

  defp load_non_neg_integer!(map, key, location) do
    case Map.fetch(map, key) do
      {:ok, value} when is_integer(value) and value >= 0 ->
        value

      {:ok, other} ->
        invalid!(
          :invalid_document,
          "#{location} #{key} must be a non-negative integer, got #{inspect(other)}"
        )

      :error ->
        invalid!(
          :invalid_document,
          "#{location} document is missing required key #{inspect(key)}"
        )
    end
  end

  defp load_pos_integer!(map, key, location) do
    case load_non_neg_integer!(map, key, location) do
      0 ->
        invalid!(:invalid_document, "#{location} #{key} must be a positive integer, got 0")

      value ->
        value
    end
  end

  defp assert_string_keys!(map, location) do
    Enum.each(Map.keys(map), fn
      key when is_binary(key) ->
        :ok

      other ->
        invalid!(:invalid_document, "#{location} keys must be strings, got #{inspect(other)}", %{
          location: location
        })
    end)
  end

  defp assert_known_keys!(map, allowed, location) do
    allowed_set = MapSet.new(allowed)

    Enum.each(Map.keys(map), fn key ->
      unless MapSet.member?(allowed_set, key) do
        invalid!(:invalid_document, "unknown #{location} key #{inspect(key)}", %{
          location: location,
          key: key
        })
      end
    end)
  end

  defp invalid!(type, message, details \\ %{}) do
    raise Docket.Error, type: type, message: message, details: details
  end
end
