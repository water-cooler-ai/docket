if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.Event do
    @moduledoc """
    Row schema for `docket_events` — append-only run facts, mirroring
    `Docket.Event`.

    `occurred_at` is the event's own timestamp (`Docket.Event.timestamp`);
    `inserted_at` is when it was persisted.
    """

    use Ecto.Schema

    import Ecto.Changeset

    @type t :: %__MODULE__{
            id: integer() | nil,
            run_id: String.t() | nil,
            seq: pos_integer() | nil,
            type: Docket.Event.type() | nil,
            step: non_neg_integer() | nil,
            node_id: String.t() | nil,
            channel_id: String.t() | nil,
            task_id: String.t() | nil,
            payload: map(),
            metadata: map(),
            occurred_at: DateTime.t() | nil,
            inserted_at: DateTime.t() | nil
          }

    schema "docket_events" do
      field(:run_id, :string)
      field(:seq, :integer)
      field(:type, Ecto.Enum, values: Docket.Event.types())
      field(:step, :integer)
      field(:node_id, :string)
      field(:channel_id, :string)
      field(:task_id, :string)
      field(:payload, :map, default: %{})
      field(:metadata, :map, default: %{})
      field(:occurred_at, :utc_datetime_usec)

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    @doc "Builds a changeset for inserting an event row."
    @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
    def changeset(event \\ %__MODULE__{}, attrs) do
      event
      |> cast(attrs, [
        :run_id,
        :seq,
        :type,
        :step,
        :node_id,
        :channel_id,
        :task_id,
        :payload,
        :metadata,
        :occurred_at
      ])
      |> validate_required([:run_id, :seq, :type, :step, :occurred_at])
      |> validate_number(:seq, greater_than: 0)
      |> validate_number(:step, greater_than_or_equal_to: 0)
      |> unique_constraint([:run_id, :seq])
    end
  end
end
