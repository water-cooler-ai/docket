if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.Checkpoint do
    @moduledoc """
    Row schema for `docket_checkpoints` — checkpoint metadata, and only
    metadata.

    A checkpoint row records seq, type, step, park action, and timestamps —
    never the run document. The latest committed document lives solely in
    `docket_runs.docket_run`; storing O(supersteps × state size) snapshots
    here would be the hidden cost that melts an adopter's database
    (operational transition spec, section 5).

    `park_action` records how the committing vehicle parked the run (spec
    section 9: terminal, waiting on interrupt, timer, remote await, drain
    budget yield, or retry backoff). It is `nil` for checkpoints committed
    mid-drain. The exact value vocabulary is pinned by the coordinator
    (DCKT-15), not by this schema.

    `created_at` is when the runtime built the checkpoint
    (`Docket.Checkpoint.created_at`); `inserted_at` is when it was
    persisted.
    """

    use Ecto.Schema

    import Ecto.Changeset

    @type t :: %__MODULE__{
            id: integer() | nil,
            run_id: String.t() | nil,
            seq: pos_integer() | nil,
            type: Docket.Checkpoint.type() | nil,
            step: non_neg_integer() | nil,
            park_action: String.t() | nil,
            created_at: DateTime.t() | nil,
            inserted_at: DateTime.t() | nil
          }

    schema "docket_checkpoints" do
      field(:run_id, :string)
      field(:seq, :integer)
      field(:type, Ecto.Enum, values: Docket.Checkpoint.types())
      field(:step, :integer)
      field(:park_action, :string)
      field(:created_at, :utc_datetime_usec)

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    @doc "Builds a changeset for inserting a checkpoint metadata row."
    @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
    def changeset(checkpoint \\ %__MODULE__{}, attrs) do
      checkpoint
      |> cast(attrs, [:run_id, :seq, :type, :step, :park_action, :created_at])
      |> validate_required([:run_id, :seq, :type, :step, :created_at])
      |> validate_number(:seq, greater_than: 0)
      |> validate_number(:step, greater_than_or_equal_to: 0)
      |> unique_constraint([:run_id, :seq])
    end
  end
end
