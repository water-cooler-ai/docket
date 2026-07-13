if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.GraphVersion do
    @moduledoc """
    Row schema for `docket_graph_versions` — effective durable graphs,
    content-addressed by tenant scope + `graph_id` + `graph_hash`.

    `graph` holds the private versioned ETF encoding. Compiled runtime graphs
    stay node-local and are never persisted here. Rows are immutable once
    inserted.
    """

    use Ecto.Schema

    import Ecto.Changeset

    @type t :: %__MODULE__{
            id: integer() | nil,
            tenant_id: String.t() | nil,
            scope_key: String.t() | nil,
            graph_id: String.t() | nil,
            graph_hash: String.t() | nil,
            graph: binary() | nil,
            inserted_at: DateTime.t() | nil
          }

    schema "docket_graph_versions" do
      field(:tenant_id, :string)
      field(:scope_key, :string, read_after_writes: true)
      field(:graph_id, :string)
      field(:graph_hash, :string)
      field(:graph, :binary, redact: true)

      field(:inserted_at, :utc_datetime_usec, read_after_writes: true)
    end

    @doc "Builds a changeset for inserting a graph version row."
    @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
    def changeset(version \\ %__MODULE__{}, attrs) do
      version
      |> cast(attrs, [:tenant_id, :graph_id, :graph_hash, :graph], empty_values: [])
      |> validate_required([:graph_id, :graph_hash, :graph])
      |> validate_length(:tenant_id, min: 1)
      |> unique_constraint([:scope_key, :graph_id, :graph_hash],
        name: :docket_graph_versions_scope_graph_index
      )
    end
  end
end
