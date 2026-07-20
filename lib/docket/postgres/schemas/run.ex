if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.Run do
    @moduledoc """
    Row schema for `docket_runs`.

    Columns expose only the fields Postgres needs for identity, claiming,
    scheduling, inspection, and constraints. `state` holds all remaining
    durable run fields and must not be interpreted by hosts.

    Operational columns:

      * `wake_at` — when the run next advances: `now` means runnable, a
        future instant means a timer or retry backoff, and `nil` means
        claimed, externally parked, poisoned, or terminal.
      * `claim_token` / `claimed_at` — execution ownership.
      * `tenant_admitted_at` — durable TenantFair cohort residency. It is
        independent of the transient claim token and is never set by Legacy.
      * `checkpoint_seq` — the optimistic commit fence.
      * `claim_attempts` — consecutive claims consumed by launched execution
        without committed progress.
      * `claim_abandons` — consecutive pre-execution claim abandons without
        committed progress.
      * `poisoned_at` / `poison_reason` — paired poison facts, both `nil`
        for a healthy run.

    `tenant_id` is a nullable scoping column; nothing requires it.

    The changeset is a row codec only. Lifecycle tuple validity (claim
    pairing, schedule shape, terminal columns) is enforced by the database
    CHECK constraints, which also bind raw SQL that bypasses changesets.
    """

    use Ecto.Schema

    import Ecto.Changeset

    @type t :: %__MODULE__{
            id: integer() | nil,
            run_id: String.t() | nil,
            tenant_id: String.t() | nil,
            scope_key: String.t() | nil,
            graph_id: String.t() | nil,
            graph_hash: String.t() | nil,
            status: Docket.Run.durable_status() | nil,
            step: non_neg_integer(),
            state: binary() | nil,
            checkpoint_seq: non_neg_integer(),
            latest_checkpoint_type: Docket.Checkpoint.type() | nil,
            claim_token: Ecto.UUID.t() | nil,
            claimed_at: DateTime.t() | nil,
            tenant_admitted_at: DateTime.t() | nil,
            wake_at: DateTime.t() | nil,
            claim_attempts: non_neg_integer(),
            claim_abandons: non_neg_integer(),
            poisoned_at: DateTime.t() | nil,
            poison_reason: String.t() | nil,
            inserted_at: DateTime.t() | nil,
            started_at: DateTime.t() | nil,
            updated_at: DateTime.t() | nil,
            finished_at: DateTime.t() | nil
          }

    schema "docket_runs" do
      field(:run_id, :string)
      field(:tenant_id, :string)
      field(:scope_key, :string, read_after_writes: true)
      field(:graph_id, :string)
      field(:graph_hash, :string)
      field(:status, Ecto.Enum, values: Docket.Run.durable_statuses())
      field(:step, :integer, default: 0)
      field(:state, :binary, redact: true)
      field(:checkpoint_seq, :integer, default: 0)
      field(:latest_checkpoint_type, Ecto.Enum, values: Docket.Checkpoint.types())
      field(:claim_token, Ecto.UUID, redact: true)
      field(:claimed_at, :utc_datetime_usec)
      field(:tenant_admitted_at, :utc_datetime_usec)
      field(:wake_at, :utc_datetime_usec)
      field(:claim_attempts, :integer, default: 0)
      field(:claim_abandons, :integer, default: 0)
      field(:poisoned_at, :utc_datetime_usec)
      field(:poison_reason, :string)
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
      :state,
      :checkpoint_seq,
      :latest_checkpoint_type,
      :claim_token,
      :claimed_at,
      :wake_at,
      :claim_attempts,
      :claim_abandons,
      :poisoned_at,
      :poison_reason,
      :started_at,
      :updated_at,
      :finished_at
    ]

    @required [:run_id, :graph_id, :graph_hash, :status, :state, :started_at]

    @doc """
    Builds a changeset for inserting or updating a run row.

    `tenant_id` is never required.
    """
    @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
    def changeset(run \\ %__MODULE__{}, attrs) do
      run
      |> cast(attrs, @permitted, empty_values: [])
      |> validate_required(@required)
      |> validate_length(:tenant_id, min: 1)
      |> validate_number(:step, greater_than_or_equal_to: 0)
      |> validate_number(:checkpoint_seq, greater_than_or_equal_to: 0)
      |> validate_number(:claim_attempts, greater_than_or_equal_to: 0)
      |> validate_number(:claim_abandons, greater_than_or_equal_to: 0)
      |> unique_constraint(:run_id)
      |> foreign_key_constraint(:graph_hash, name: :docket_runs_graph_scope_fkey)
    end
  end
end
