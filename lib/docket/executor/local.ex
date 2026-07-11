defmodule Docket.Executor.Local do
  @moduledoc """
  Synchronous local executor: calls the node module directly in the
  activation's dispatcher task.

  The dispatcher gives concurrent activations separate tasks, but `Local`
  does not add a child process around an individual node call and therefore
  cannot enforce `timeout_ms`. Per-node timeouts become real with
  `Docket.Executor.Task`.
  """

  @behaviour Docket.Executor

  @impl true
  def execute(_task, node, state, config, context, _opts) do
    apply(node.module, node.function, [state, config, context])
  end
end
