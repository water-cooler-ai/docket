defmodule Docket.Executor.Local do
  @moduledoc """
  Synchronous local executor: calls the node module directly in the
  dispatching process.

  `Local` cannot enforce `timeout_ms` (there is no process boundary); node
  timeouts become real with `Docket.Executor.Task`.
  """

  @behaviour Docket.Executor

  @impl true
  def execute(_task, node, state, config, context, _opts) do
    apply(node.module, node.function, [state, config, context])
  end
end
