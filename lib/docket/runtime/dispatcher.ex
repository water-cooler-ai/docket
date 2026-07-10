defmodule Docket.Runtime.Dispatcher do
  @moduledoc false

  # Internal task dispatch mechanics: builds the node context, calls the
  # configured executor for each activation, and normalizes every outcome
  # shape into a `TaskResult`. One dispatch executes exactly one attempt per
  # activation - retry waiting is durable retry parking owned by the loop
  # and its shells, never a sleep in here. The dispatcher does not evaluate
  # guards, apply reducers, commit checkpoints, or make retry policy
  # decisions beyond classifying whether attempt budget remains.
  #
  # Activations run serially in the calling process in v1; semantic
  # parallelism is guaranteed by barrier visibility, not scheduling.

  alias Docket.Run.TaskState
  alias Docket.Runtime.{Activation, TaskResult}

  @spec dispatch([Activation.t()], Docket.Runtime.Graph.t(), Docket.Run.t(), map()) ::
          [TaskResult.t()]
  def dispatch(activations, rtg, run, config) do
    Enum.map(activations, fn activation ->
      node = Map.fetch!(rtg.nodes, activation.runtime_node_id)
      attempt(activation, node, run, config)
    end)
  end

  defp attempt(activation, node, run, config) do
    outcome = execute(activation, node, run, config)

    case classify(outcome) do
      {:final, status, value} ->
        %TaskResult{
          task_id: activation.task_id,
          node_id: activation.node_id,
          attempt: activation.attempt,
          status: status,
          value: value
        }

      {:failure, retryable?, reason} ->
        status =
          if retryable? and activation.attempt < activation.retry.max_attempts,
            do: :retry,
            else: :error

        %TaskResult{
          task_id: activation.task_id,
          node_id: activation.node_id,
          attempt: activation.attempt,
          status: status,
          value: reason,
          failures: [%{attempt: activation.attempt, reason: reason}]
        }
    end
  end

  defp execute(activation, node, run, config) do
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

    executor_opts = Keyword.put(config.executor_opts, :timeout_ms, activation.timeout_ms)

    config.executor.execute(
      task,
      node,
      activation.snapshot,
      activation.config,
      context,
      executor_opts
    )
  rescue
    exception -> {:raised, exception, __STACKTRACE__}
  catch
    :exit, reason -> {:exited, reason}
    :throw, value -> {:thrown, value}
  end

  # Node returns of {:error, reason}, raises, exits, and throws are retryable
  # attempt failures. Reserved/invalid return shapes are deterministic and
  # never retried. Update-map validation happens at the barrier, not here.
  defp classify({:ok, update}), do: {:final, :ok, update}

  defp classify({:interrupt, %Docket.Interrupt{} = interrupt}),
    do: {:final, :interrupt, interrupt}

  defp classify({:interrupt, other}),
    do: {:failure, false, {:invalid_interrupt, other}}

  defp classify({:await, _await}), do: {:failure, false, :unsupported_await}
  defp classify({:command, _command}), do: {:failure, false, :unsupported_command}
  defp classify({:error, reason}), do: {:failure, true, reason}

  defp classify({:raised, exception, stacktrace}),
    do: {:failure, true, {:raised, exception, stacktrace}}

  defp classify({:exited, reason}), do: {:failure, true, {:exited, reason}}
  defp classify({:thrown, value}), do: {:failure, true, {:thrown, value}}
  defp classify(other), do: {:failure, false, {:invalid_node_return, other}}
end
