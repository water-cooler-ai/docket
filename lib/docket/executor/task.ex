defmodule Docket.Executor.Task do
  @moduledoc """
  Process-isolated executor: runs the node in its own task so `timeout_ms`
  can be enforced and node crashes cannot take the dispatching process down.

  The superstep contract stays barrier-synchronous: `execute/6` awaits the
  task before returning, so the runtime still collects all selected results
  before the update barrier.

  ## Options

  - `:task_supervisor` - a `Task.Supervisor` name or pid to run node code
    under. The supervised Runtime injects its own task supervisor
    automatically; without one the node runs in an unsupervised monitored
    process.
  - `:timeout_ms` - injected by the dispatcher from the node's resolved
    `"timeout_ms"` policy; `nil` waits indefinitely.

  Timeouts and task crashes normalize to `{:error, :timeout}` and
  `{:error, {:exited, reason}}`, which the dispatcher treats as retryable
  node attempt failures.
  """

  @behaviour Docket.Executor

  @impl true
  def execute(_task, node, state, config, context, opts) do
    timeout = Keyword.get(opts, :timeout_ms) || :infinity
    fun = fn -> apply(node.module, node.function, [state, config, context]) end

    case Keyword.get(opts, :task_supervisor) do
      nil -> run_monitored(fun, timeout)
      supervisor -> run_supervised(supervisor, fun, timeout)
    end
  end

  defp run_supervised(supervisor, fun, timeout) do
    task = Task.Supervisor.async_nolink(supervisor, fun)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:exited, reason}}
      nil -> {:error, :timeout}
    end
  end

  defp run_monitored(fun, timeout) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        send(parent, {__MODULE__, self(), fun.()})
      end)

    receive do
      {__MODULE__, ^pid, result} ->
        Process.demonitor(ref, [:flush])
        result

      {:DOWN, ^ref, :process, ^pid, reason} ->
        {:error, {:exited, reason}}
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        Process.exit(pid, :kill)

        # The node may have replied in the same instant it was killed; never
        # leave its result in the dispatcher's mailbox.
        receive do
          {__MODULE__, ^pid, _result} -> :ok
        after
          0 -> :ok
        end

        {:error, :timeout}
    end
  end
end
