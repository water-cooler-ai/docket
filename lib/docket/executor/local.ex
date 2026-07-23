defmodule Docket.Executor.Local do
  @moduledoc """
  Synchronous local executor: calls the node module directly in the
  activation's dispatcher task.

  The dispatcher gives concurrent activations separate tasks, but `Local`
  does not add a nested node task. The runtime dispatcher owns the activation
  process and enforces its effective attempt timeout.
  """

  @behaviour Docket.Executor

  @impl true
  def execute(_task, node, state, config, context, _opts) do
    apply(node.module, node.function, [state, config, context])
  end
end
