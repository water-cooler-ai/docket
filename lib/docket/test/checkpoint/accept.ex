defmodule Docket.Test.Checkpoint.Accept do
  @moduledoc """
  Default checkpoint sink for `Docket.Test` helpers: accepts every
  checkpoint without storing it.

  Inline helpers return accepted checkpoints directly, so tests that only
  assert on the returned list do not need a real sink.
  """

  @behaviour Docket.Checkpoint

  @impl true
  def handle(_checkpoint, _context), do: :ok
end
