defmodule Docket.GraphVersionPage do
  @moduledoc """
  One newest-first page of retained graph-version metadata.

  Pagination uses the immutable `(published_at, graph_hash)` pair. Backends
  read at most one row beyond the requested limit and pass those already
  ordered candidates to `new/3`, which centralizes trimming, order validation,
  and cursor derivation.
  """

  @typedoc "Cursor selecting versions strictly older than this immutable key."
  @type cursor :: {DateTime.t(), String.t()}

  @type t :: %__MODULE__{
          versions: [Docket.GraphVersionSummary.t()],
          next_before: cursor() | nil,
          has_more?: boolean()
        }

  @enforce_keys [:versions, :next_before, :has_more?]
  defstruct [:versions, :next_before, :has_more?]

  @doc """
  Builds a page from newest-first candidates containing at most `limit + 1` rows.

  The extra candidate determines `has_more?` and is not returned. The next
  cursor is the last returned version's `(published_at, graph_hash)` key. An
  empty page preserves the supplied cursor.
  """
  @spec new([Docket.GraphVersionSummary.t()], cursor() | nil, pos_integer()) :: t()
  def new(candidates, before, limit)
      when is_list(candidates) and is_integer(limit) and limit > 0 do
    validate_cursor!(before)
    validate_candidate_count!(candidates, limit)
    validate_candidates!(candidates)
    validate_order!(candidates)
    validate_before!(candidates, before)

    has_more? = length(candidates) > limit
    versions = Enum.take(candidates, limit)

    next_before =
      case List.last(versions) do
        nil -> before
        version -> Docket.GraphVersionSummary.cursor(version)
      end

    %__MODULE__{versions: versions, next_before: next_before, has_more?: has_more?}
  end

  def new(candidates, before, limit) do
    raise ArgumentError,
          "graph version page requires a candidate list, valid cursor, and positive limit, got: " <>
            inspect(candidates: candidates, before: before, limit: limit)
  end

  defp validate_candidate_count!(candidates, limit) do
    if length(candidates) > limit + 1 do
      raise ArgumentError,
            "graph version page candidates must contain at most limit + 1 rows, got: " <>
              "#{length(candidates)} for limit #{limit}"
    end
  end

  defp validate_candidates!(candidates) do
    unless Enum.all?(candidates, &is_struct(&1, Docket.GraphVersionSummary)) do
      raise ArgumentError,
            "graph version page candidates must all be Docket.GraphVersionSummary values"
    end

    graph_ids = candidates |> Enum.map(& &1.ref.graph_id) |> Enum.uniq()

    if length(graph_ids) > 1 do
      raise ArgumentError, "graph version page candidates must belong to one graph ID"
    end
  end

  defp validate_order!(candidates) do
    ordered? =
      candidates
      |> Enum.map(&Docket.GraphVersionSummary.cursor/1)
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.all?(fn [newer, older] -> compare_cursor(newer, older) == :gt end)

    unless ordered? do
      raise ArgumentError,
            "graph version page candidates must be strictly newest-first by " <>
              "{published_at, graph_hash}"
    end
  end

  defp validate_before!([], _before), do: :ok
  defp validate_before!(_candidates, nil), do: :ok

  defp validate_before!([first | _], before) do
    if compare_cursor(Docket.GraphVersionSummary.cursor(first), before) != :lt do
      raise ArgumentError,
            "graph version page candidates must be strictly older than the before cursor"
    end
  end

  defp validate_cursor!(nil), do: :ok

  defp validate_cursor!({%DateTime{}, graph_hash})
       when is_binary(graph_hash) and byte_size(graph_hash) > 0,
       do: :ok

  defp validate_cursor!(cursor) do
    raise ArgumentError,
          "graph version page cursor must be nil or {DateTime, non-empty graph_hash}, got: " <>
            inspect(cursor)
  end

  defp compare_cursor({left_at, left_hash}, {right_at, right_hash}) do
    case DateTime.compare(left_at, right_at) do
      :eq when left_hash > right_hash -> :gt
      :eq when left_hash < right_hash -> :lt
      :eq -> :eq
      order -> order
    end
  end
end
