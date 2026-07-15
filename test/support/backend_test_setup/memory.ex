defmodule Docket.Test.BackendTestSetup.Memory do
  @moduledoc false

  alias Docket.Test.MemoryBackend

  @now ~U[2026-07-15 12:00:00.000000Z]

  def setup_suite, do: {:ok, []}

  def setup(_context) do
    {:ok, backend} = MemoryBackend.start_link(clock: fn -> @now end)
    ExUnit.Callbacks.on_exit(fn -> if Process.alive?(backend), do: Agent.stop(backend) end)

    subject = %{
      backend: MemoryBackend,
      context: backend,
      namespace: "memory-#{System.unique_integer([:positive, :monotonic])}",
      now: @now
    }

    {:ok, backend_test: subject}
  end
end
