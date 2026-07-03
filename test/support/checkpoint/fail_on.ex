defmodule Docket.Test.Checkpoint.FailOn do
  @moduledoc """
  Checkpoint sink that rejects configured checkpoint types and accepts the
  rest, recording every accepted checkpoint to an optional memory sink.

      {:ok, sink} = MemorySink.start_link()
      Docket.Test.run_inline(graph, input,
        checkpoint: FailOn,
        context: %{fail_on: [:step_committed], memory_sink: sink}
      )
  """

  @behaviour Docket.Checkpoint

  @impl true
  def handle(checkpoint, %Docket.Checkpoint.Context{application: application} = context) do
    if checkpoint.type in Map.get(application, :fail_on, []) do
      {:error, {:forced_failure, checkpoint.type}}
    else
      case Map.fetch(application, :memory_sink) do
        {:ok, _sink} -> Docket.Test.Checkpoint.MemorySink.handle(checkpoint, context)
        :error -> :ok
      end
    end
  end
end
