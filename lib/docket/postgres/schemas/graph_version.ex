if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.GraphVersion do
    @moduledoc """
    Row schema for `docket_graph_versions` — content-addressed compiled
    graph documents.

    Keyed by `graph_id` + `graph_hash` so a worker recovering a run on
    another node can load the exact graph content with no host call in the
    loop. Rows are immutable: publish-on-start upserts with `ON CONFLICT DO
    NOTHING`, and content addressing makes racing publishes byte-identical.
    `graph` holds the JSON-safe wire map produced by
    `Docket.Graph.Serializer`.
    """

    use Ecto.Schema

    import Ecto.Changeset

    @type t :: %__MODULE__{
            id: integer() | nil,
            graph_id: String.t() | nil,
            graph_hash: String.t() | nil,
            graph: map() | nil,
            inserted_at: DateTime.t() | nil
          }

    schema "docket_graph_versions" do
      field(:graph_id, :string)
      field(:graph_hash, :string)
      field(:graph, :map)

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    @doc "Builds a changeset for inserting a graph version row."
    @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
    def changeset(version \\ %__MODULE__{}, attrs) do
      version
      |> cast(attrs, [:graph_id, :graph_hash, :graph])
      |> validate_required([:graph_id, :graph_hash, :graph])
      |> unique_constraint([:graph_id, :graph_hash])
    end
  end
end
