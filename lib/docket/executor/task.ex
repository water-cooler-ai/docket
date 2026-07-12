defmodule Docket.Executor.Task do
  @moduledoc """
  Compatibility executor retained for callers that select it explicitly.
  Process isolation and timeout enforcement are owned by the runtime's
  per-activation dispatcher process, so this executor adds no nested task.

  It deliberately executes synchronously inside that activation process;
  adding an unlinked nested task could survive the runtime-owned deadline.
  """

  @behaviour Docket.Executor

  @impl true
  def execute(_task, node, state, config, context, _opts) do
    apply(node.module, node.function, [state, config, context])
  end
end
