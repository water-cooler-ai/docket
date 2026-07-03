defmodule Docket.Test.Checkpoint.Recording do
  @moduledoc """
  Checkpoint sink that sends every checkpoint to a test process, optionally
  rejecting configured types.

  Supervised tests use it as their synchronization point instead of
  `Process.sleep/1`:

      Docket.run(runtime, graph, input,
        checkpoint: Recording,
        context: %{notify: self(), fail_on: [:step_committed]}
      )

      assert_receive {:checkpoint, %Docket.Checkpoint{type: :run_completed}}

  Accepted checkpoints arrive as `{:checkpoint, checkpoint}`; rejected ones
  as `{:checkpoint_rejected, checkpoint}` with `{:error, {:forced_failure,
  type}}` returned to the runtime.
  """

  @behaviour Docket.Checkpoint

  @impl true
  def handle(checkpoint, %Docket.Checkpoint.Context{application: application}) do
    notify = Map.fetch!(application, :notify)

    if checkpoint.type in Map.get(application, :fail_on, []) do
      send(notify, {:checkpoint_rejected, checkpoint})
      {:error, {:forced_failure, checkpoint.type}}
    else
      send(notify, {:checkpoint, checkpoint})
      :ok
    end
  end
end
