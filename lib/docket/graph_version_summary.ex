defmodule Docket.GraphVersionSummary do
  @moduledoc """
  Lightweight metadata for one retained saved graph version.

  The reference is an exact content address within the owner scope used for
  the read. Owner scope is intentionally absent from this value: public graph
  APIs resolve it independently, and a reference is not an authorization
  credential. `published_at` records the first successful publication of this
  distinct version; an idempotent save does not change it.
  """

  @enforce_keys [:ref, :published_at]
  defstruct [:ref, :published_at]

  @type t :: %__MODULE__{
          ref: Docket.GraphRef.t(),
          published_at: DateTime.t()
        }

  @doc "Builds and validates graph-version metadata from a map or keyword list."
  @spec new!(map() | keyword()) :: t()
  def new!(fields) when is_list(fields), do: fields |> Map.new() |> new!()

  def new!(fields) when is_map(fields) and not is_struct(fields) do
    summary = struct!(__MODULE__, fields)

    validate_ref!(summary.ref)
    validate_published_at!(summary.published_at)
    summary
  end

  def new!(fields) do
    raise ArgumentError,
          "graph version summary fields must be a map or keyword list, got: #{inspect(fields)}"
  end

  @doc "Returns this version's stable newest-first pagination key."
  @spec cursor(t()) :: Docket.GraphVersionPage.cursor()
  def cursor(%__MODULE__{
        ref: %Docket.GraphRef{graph_hash: graph_hash},
        published_at: published_at
      }) do
    {published_at, graph_hash}
  end

  defp validate_ref!(%Docket.GraphRef{graph_id: graph_id, graph_hash: graph_hash})
       when is_binary(graph_id) and byte_size(graph_id) > 0 and is_binary(graph_hash) and
              byte_size(graph_hash) > 0,
       do: :ok

  defp validate_ref!(ref) do
    raise ArgumentError,
          "graph version summary ref must contain non-empty graph_id and graph_hash, got: " <>
            inspect(ref)
  end

  defp validate_published_at!(%DateTime{}), do: :ok

  defp validate_published_at!(published_at) do
    raise ArgumentError,
          "graph version summary published_at must be a DateTime, got: #{inspect(published_at)}"
  end
end
