defmodule Docket.RunPage do
  @moduledoc """
  One newest-first page of lightweight durable run summaries.

  Run pagination uses the immutable `(started_at, run_id)` pair as a stable
  keyset. A backend reads at most one row beyond the requested limit and passes
  those already ordered candidates to `new/3`, which centralizes trimming and
  cursor derivation across storage implementations. Ordering and cursor
  exclusivity are storage-behaviour contracts; this value does not revalidate
  trusted backend output.
  """

  @typedoc "Cursor selecting runs strictly older than this immutable key."
  @type cursor :: {DateTime.t(), String.t()}

  @type t :: %__MODULE__{
          runs: [Docket.RunSummary.t()],
          next_before: cursor() | nil,
          has_more?: boolean()
        }

  @enforce_keys [:runs, :next_before, :has_more?]
  defstruct [:runs, :next_before, :has_more?]

  @doc """
  Builds a page from newest-first candidates containing at most `limit + 1` rows.

  The extra candidate determines `has_more?` and is not returned. The next
  cursor is the last returned run's `(started_at, id)` key. An empty page keeps
  the supplied cursor, matching the cursor-preserving semantics of
  `Docket.EventPage`.
  """
  @spec new([Docket.RunSummary.t()], cursor() | nil, pos_integer()) :: t()
  def new(candidates, before, limit) do
    {runs, lookahead} = Enum.split(candidates, limit)

    next_before =
      case List.last(runs) do
        nil -> before
        %Docket.RunSummary{id: id, started_at: started_at} -> {started_at, id}
      end

    %__MODULE__{runs: runs, next_before: next_before, has_more?: lookahead != []}
  end
end
