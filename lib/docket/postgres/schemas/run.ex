if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.Run do
    @moduledoc """
    Row schema for `docket_runs`.

    The public `Docket.Run` fields (`run_id`, `graph_id`, `graph_hash`,
    `status`, `step`, `input`, `output`, `metadata`, timestamps) are
    columns; `state` holds the Docket-owned execution internals and must
    not be interpreted by hosts.

    Operational columns:

      * `wake_at` — when the run next advances: `now` means runnable, a
        future instant means a timer or retry backoff, and `nil` means
        externally parked or terminal.
      * `claim_token` / `claimed_at` — execution ownership.
      * `checkpoint_seq` — the optimistic commit fence.
      * `attempts`, `operational_status`, `operational_error` — operational
        health, separate from the graph-run `status`.

    `tenant_id` is a nullable scoping column; nothing requires it.
    """

    use Ecto.Schema

    import Ecto.Changeset

    @type t :: %__MODULE__{
            id: integer() | nil,
            run_id: String.t() | nil,
            tenant_id: String.t() | nil,
            graph_id: String.t() | nil,
            graph_hash: String.t() | nil,
            status: Docket.Run.status() | nil,
            step: non_neg_integer(),
            input: map() | nil,
            output: map() | nil,
            metadata: map(),
            state: map() | nil,
            checkpoint_seq: non_neg_integer(),
            latest_checkpoint_type: Docket.Checkpoint.type() | nil,
            claim_token: Ecto.UUID.t() | nil,
            claimed_at: DateTime.t() | nil,
            wake_at: DateTime.t() | nil,
            attempts: non_neg_integer(),
            operational_status: :active | :blocked | :poisoned,
            operational_error: map() | nil,
            inserted_at: DateTime.t() | nil,
            started_at: DateTime.t() | nil,
            updated_at: DateTime.t() | nil,
            finished_at: DateTime.t() | nil
          }

    @operational_statuses [:active, :blocked, :poisoned]

    schema "docket_runs" do
      field(:run_id, :string)
      field(:tenant_id, :string)
      field(:graph_id, :string)
      field(:graph_hash, :string)
      field(:status, Ecto.Enum, values: Docket.Run.statuses())
      field(:step, :integer, default: 0)
      field(:input, :map)
      field(:output, :map)
      field(:metadata, :map, default: %{})
      field(:state, :map)
      field(:checkpoint_seq, :integer, default: 0)
      field(:latest_checkpoint_type, Ecto.Enum, values: Docket.Checkpoint.types())
      field(:claim_token, Ecto.UUID)
      field(:claimed_at, :utc_datetime_usec)
      field(:wake_at, :utc_datetime_usec)
      field(:attempts, :integer, default: 0)
      field(:operational_status, Ecto.Enum, values: @operational_statuses, default: :active)
      field(:operational_error, :map)
      field(:started_at, :utc_datetime_usec)
      field(:finished_at, :utc_datetime_usec)

      timestamps(type: :utc_datetime_usec)
    end

    @permitted [
      :run_id,
      :tenant_id,
      :graph_id,
      :graph_hash,
      :status,
      :step,
      :input,
      :output,
      :metadata,
      :state,
      :checkpoint_seq,
      :latest_checkpoint_type,
      :claim_token,
      :claimed_at,
      :wake_at,
      :attempts,
      :operational_status,
      :operational_error,
      :started_at,
      :finished_at
    ]

    @required [:run_id, :graph_id, :graph_hash, :status, :input, :state]

    @doc """
    Builds a changeset for inserting or updating a run row.

    `tenant_id` is never required.
    """
    @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
    def changeset(run \\ %__MODULE__{}, attrs) do
      run
      |> cast(attrs, @permitted)
      |> validate_required(@required)
      |> validate_number(:step, greater_than_or_equal_to: 0)
      |> validate_number(:checkpoint_seq, greater_than_or_equal_to: 0)
      |> validate_number(:attempts, greater_than_or_equal_to: 0)
      |> unique_constraint(:run_id)
    end

    @doc false
    @spec operational_statuses() :: [:active | :blocked | :poisoned]
    def operational_statuses, do: @operational_statuses
  end
end
