defmodule Docket.Test.Checkpoint.MemorySink do
  @moduledoc """
  Agent-backed checkpoint sink that stores accepted checkpoints in order.

  Tests start one agent per test and pass its pid through the application
  context:

      {:ok, sink} = MemorySink.start_link()
      Docket.Test.run_inline(graph, input,
        checkpoint: MemorySink,
        context: %{memory_sink: sink}
      )

      MemorySink.checkpoints(sink)
  """

  @behaviour Docket.Checkpoint

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> [] end)
  end

  @impl true
  def handle(checkpoint, %Docket.Checkpoint.Context{application: application}) do
    application
    |> Map.fetch!(:memory_sink)
    |> Agent.update(&[checkpoint | &1])

    :ok
  end

  def checkpoints(sink) do
    sink |> Agent.get(& &1) |> Enum.reverse()
  end
end
