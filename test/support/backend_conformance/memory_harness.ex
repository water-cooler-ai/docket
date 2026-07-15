defmodule Docket.Test.BackendConformance.MemoryHarness do
  @moduledoc false
  @behaviour Docket.Backend.Conformance.Harness

  alias Docket.Backend.Conformance.Instance
  alias Docket.Test.MemoryBackend

  @now ~U[2026-07-15 12:00:00.000000Z]

  @impl true
  def setup_case(_suite_state, _context) do
    {:ok, backend} = MemoryBackend.start_link(clock: fn -> @now end)

    {:ok,
     %Instance{
       backend: MemoryBackend,
       context: backend,
       namespace: "memory-#{System.unique_integer([:positive, :monotonic])}",
       now: @now
     }}
  end

  @impl true
  def teardown_case(%Instance{context: backend}) do
    if Process.alive?(backend), do: Agent.stop(backend)
  end
end
