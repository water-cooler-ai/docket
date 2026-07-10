if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.GraphArtifact do
    @moduledoc """
    Immutable compiled graph artifact selected by graph version and compiler ABI.

    `artifact` is the strict JSON-safe envelope produced by
    `Docket.Runtime.Graph.Artifact`; it is derived execution data, not the
    canonical editable graph document.
    """

    use Ecto.Schema

    import Ecto.Changeset

    schema "docket_graph_artifacts" do
      field(:graph_id, :string)
      field(:graph_hash, :string)
      field(:compiler_abi, :string)
      field(:artifact_hash, :string)
      field(:artifact, :map)

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    @doc "Builds a changeset for inserting one immutable compiled artifact."
    def changeset(artifact \\ %__MODULE__{}, attrs) do
      artifact
      |> cast(attrs, [:graph_id, :graph_hash, :compiler_abi, :artifact_hash, :artifact])
      |> validate_required([:graph_id, :graph_hash, :compiler_abi, :artifact_hash, :artifact])
      |> unique_constraint([:graph_id, :graph_hash, :compiler_abi])
    end
  end
end
