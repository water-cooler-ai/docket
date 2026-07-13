defmodule Docket.GraphVersionPage do
  @moduledoc """
  One newest-first page of retained graph-version metadata.

  Pagination uses the immutable `(published_at, graph_hash)` pair. Backends
  read at most one row beyond the requested limit and pass those already
  ordered candidates to `new/3`, which centralizes trimming and cursor
  derivation. Ordering and cursor exclusivity are storage-behaviour contracts;
  this value does not revalidate trusted backend output.
  """

  @typedoc "Cursor selecting versions strictly older than this immutable key."
  @type cursor :: {DateTime.t(), String.t()}

  @type t :: %__MODULE__{
          versions: [Docket.GraphVersion.t()],
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
  @spec new([Docket.GraphVersion.t()], cursor() | nil, pos_integer()) :: t()
  def new(candidates, before, limit) do
    {versions, lookahead} = Enum.split(candidates, limit)

    next_before =
      case List.last(versions) do
        nil ->
          before

        %Docket.GraphVersion{
          ref: %Docket.GraphRef{graph_hash: graph_hash},
          published_at: published_at
        } ->
          {published_at, graph_hash}
      end

    %__MODULE__{
      versions: versions,
      next_before: next_before,
      has_more?: lookahead != []
    }
  end
end
