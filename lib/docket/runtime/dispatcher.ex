defmodule Docket.Runtime.Dispatcher do
  @moduledoc false

  alias Docket.Run.TaskState
  alias Docket.Runtime.{ExecutionPolicy, TaskResult}

  def dispatch([], _rtg, _run, _config), do: []

  def dispatch(activations, rtg, run, config) do
    case ExecutionPolicy.validate_graph(rtg, config.max_attempt_elapsed_ms) do
      :ok -> collect(start_workers(activations, rtg, run, config), %{})
      {:error, error} -> raise error
    end
  end

  defp start_workers(activations, rtg, run, config) do
    dispatch_ref = make_ref()
    parent = self()

    workers =
      activations
      |> Enum.with_index()
      |> Map.new(fn {activation, index} ->
        node = Map.fetch!(rtg.nodes, activation.runtime_node_id)

        timeout =
          ExecutionPolicy.effective_timeout(activation.timeout_ms, config.max_attempt_elapsed_ms)

        worker_ref = make_ref()
        started = System.monotonic_time()

        {pid, monitor} =
          spawn_monitor(fn ->
            result = attempt(activation, node, run, config, timeout, started)
            send(parent, {dispatch_ref, worker_ref, result})
          end)

        _guardian = guard_worker(parent, pid)
        timer = Process.send_after(parent, {dispatch_ref, :timeout, worker_ref}, timeout)

        {worker_ref,
         %{
           index: index,
           activation: activation,
           pid: pid,
           monitor: monitor,
           timer: timer,
           started: started
         }}
      end)

    %{ref: dispatch_ref, workers: workers, count: length(activations)}
  end

  defp collect(%{workers: workers, count: count}, results) when map_size(workers) == 0 do
    for index <- 0..(count - 1), do: Map.fetch!(results, index)
  end

  defp collect(%{ref: ref, workers: workers} = state, results) do
    receive do
      {^ref, worker_ref, result} when is_map_key(workers, worker_ref) ->
        worker = Map.fetch!(workers, worker_ref)
        cancel_timer(worker.timer, ref, worker_ref)
        await_down(worker.monitor, worker.pid)
        finish_worker(state, results, worker_ref, worker, result)

      {^ref, :timeout, worker_ref} when is_map_key(workers, worker_ref) ->
        worker = Map.fetch!(workers, worker_ref)
        Process.exit(worker.pid, :kill)
        await_down(worker.monitor, worker.pid)
        flush_result(ref, worker_ref)

        finish_worker(
          state,
          results,
          worker_ref,
          worker,
          timeout_result(worker.activation, worker.started)
        )

      {:DOWN, monitor, :process, pid, reason} ->
        case Enum.find(workers, fn {_ref, worker} ->
               worker.monitor == monitor and worker.pid == pid
             end) do
          {worker_ref, worker} ->
            cancel_timer(worker.timer, ref, worker_ref)
            result = failure_result(worker.activation, {:exited, reason}, worker.started)
            finish_worker(state, results, worker_ref, worker, result)

          nil ->
            collect(state, results)
        end
    end
  end

  defp finish_worker(%{workers: workers} = state, results, worker_ref, worker, result) do
    collect(
      %{state | workers: Map.delete(workers, worker_ref)},
      Map.put(results, worker.index, result)
    )
  end

  # A guardian couples an unlinked monitored worker to the caller lifetime.
  # It prevents orphaned node work without allowing an abnormal node exit to
  # take down the dispatcher before the DOWN can be normalized.
  defp guard_worker(parent, worker) do
    spawn(fn ->
      parent_ref = Process.monitor(parent)
      worker_ref = Process.monitor(worker)

      receive do
        {:DOWN, ^parent_ref, :process, ^parent, _reason} -> Process.exit(worker, :kill)
        {:DOWN, ^worker_ref, :process, ^worker, _reason} -> :ok
      end
    end)
  end

  defp await_down(monitor, pid) do
    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    end
  end

  defp cancel_timer(timer, ref, worker_ref) do
    if Process.cancel_timer(timer, async: false, info: false) == false do
      receive do
        {^ref, :timeout, ^worker_ref} -> :ok
      after
        0 -> :ok
      end
    end
  end

  defp flush_result(ref, worker_ref) do
    receive do
      {^ref, ^worker_ref, _result} -> :ok
    after
      0 -> :ok
    end
  end

  defp attempt(activation, node, run, config, timeout, started) do
    result = result(activation, classify(execute(activation, node, run, config, timeout)))
    emit(result, activation, started)
    result
  end

  defp timeout_result(activation, started) do
    result = result(activation, {:failure, true, :timeout})
    emit(result, activation, started)
    result
  end

  defp failure_result(activation, reason, started) do
    result = result(activation, {:failure, true, reason})
    emit(result, activation, started)
    result
  end

  defp result(activation, {:final, status, value}),
    do: %TaskResult{
      task_id: activation.task_id,
      node_id: activation.node_id,
      attempt: activation.attempt,
      status: status,
      value: value
    }

  defp result(activation, {:failure, retryable?, reason}) do
    status =
      if retryable? and activation.attempt < activation.retry.max_attempts,
        do: :retry,
        else: :error

    %TaskResult{
      task_id: activation.task_id,
      node_id: activation.node_id,
      attempt: activation.attempt,
      status: status,
      value: reason
    }
  end

  defp emit(result, activation, started) do
    :telemetry.execute(
      [:docket, :node, :execution],
      %{duration: System.monotonic_time() - started, attempt: activation.attempt},
      %{result: result.status}
    )
  end

  defp execute(activation, node, run, config, timeout) do
    task = %TaskState{
      task_id: activation.task_id,
      node_id: activation.node_id,
      step: activation.step,
      attempt: activation.attempt,
      status: :running,
      input_hash: activation.input_hash,
      idempotency_key: activation.idempotency_key
    }

    context = %{
      run_id: run.id,
      node_id: activation.node_id,
      step: activation.step,
      attempt: activation.attempt,
      source_versions: activation.source_versions,
      idempotency_key: task.idempotency_key,
      application: config.context
    }

    config.executor.execute(
      task,
      node,
      activation.snapshot,
      activation.config,
      context,
      Keyword.put(config.executor_opts, :timeout_ms, timeout)
    )
  rescue
    exception -> {:raised, exception, __STACKTRACE__}
  catch
    :exit, reason -> {:exited, reason}
    :throw, value -> {:thrown, value}
  end

  defp classify({:ok, update}), do: {:final, :ok, update}

  defp classify({:interrupt, %Docket.Interrupt{} = interrupt}),
    do: {:final, :interrupt, interrupt}

  defp classify({:interrupt, other}), do: {:failure, false, {:invalid_interrupt, other}}
  defp classify({:await, _}), do: {:failure, false, :unsupported_await}
  defp classify({:command, _}), do: {:failure, false, :unsupported_command}
  defp classify({:error, reason}), do: {:failure, true, reason}

  defp classify({:raised, exception, stacktrace}),
    do: {:failure, true, {:raised, exception, stacktrace}}

  defp classify({:exited, reason}), do: {:failure, true, {:exited, reason}}
  defp classify({:thrown, value}), do: {:failure, true, {:thrown, value}}
  defp classify(other), do: {:failure, false, {:invalid_node_return, other}}
end
