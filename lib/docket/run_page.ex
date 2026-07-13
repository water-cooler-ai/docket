defmodule Docket.RunPage do
  @moduledoc """
  One newest-first page of lightweight durable run summaries.

  Run pagination uses the immutable `(started_at, run_id)` pair as a stable
  keyset. A backend reads at most one row beyond the requested limit and passes
  those already ordered candidates to `new/3`, which centralizes trimming and
  cursor derivation across storage implementations.
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
  def new(candidates, before, limit)
      when is_list(candidates) and is_integer(limit) and limit > 0 do
    validate_cursor!(before)

    if length(candidates) > limit + 1 do
      raise ArgumentError,
            "run page candidates must contain at most limit + 1 rows, got: " <>
              "#{length(candidates)} for limit #{limit}"
    end

    unless Enum.all?(candidates, &is_struct(&1, Docket.RunSummary)) do
      raise ArgumentError, "run page candidates must all be Docket.RunSummary values"
    end

    has_more? = length(candidates) > limit
    runs = Enum.take(candidates, limit)

    next_before =
      case List.last(runs) do
        nil -> before
        %Docket.RunSummary{id: id, started_at: started_at} -> {started_at, id}
      end

    %__MODULE__{runs: runs, next_before: next_before, has_more?: has_more?}
  end

  def new(candidates, before, limit) do
    raise ArgumentError,
          "run page requires a candidate list, valid cursor, and positive limit, got: " <>
            inspect(candidates: candidates, before: before, limit: limit)
  end

  defp validate_cursor!(nil), do: :ok

  defp validate_cursor!({%DateTime{}, id}) when is_binary(id) and byte_size(id) > 0, do: :ok

  defp validate_cursor!(cursor) do
    raise ArgumentError,
          "run page cursor must be nil or {DateTime, non-empty run_id}, got: #{inspect(cursor)}"
  end
end
