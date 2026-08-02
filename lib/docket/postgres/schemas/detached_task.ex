if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.DetachedTask do
    @moduledoc """
    Row schema for `docket_detached_tasks` — the claim index over parked
    detached tasks, one row per pending or claimed task.

    Rows are a projection of `Docket.Run.active_tasks` maintained inside the
    same commit as every run mutation; they are never written independently.
    `claimed_at` mirrors the claimed task's `started_at`; `deadline_at` is
    the schedule-to-start deadline while pending (nil when unbounded) and
    the start-to-close deadline once claimed. No token material is stored
    here.
    """

    use Ecto.Schema

    @type t :: %__MODULE__{
            id: integer() | nil,
            run_id: String.t() | nil,
            tenant_id: String.t() | nil,
            scope_key: String.t() | nil,
            task_id: String.t() | nil,
            node_id: String.t() | nil,
            attempt: pos_integer() | nil,
            state: String.t() | nil,
            scheduled_at: DateTime.t() | nil,
            claimed_at: DateTime.t() | nil,
            deadline_at: DateTime.t() | nil
          }

    schema "docket_detached_tasks" do
      field(:run_id, :string)
      field(:tenant_id, :string)
      field(:scope_key, :string, read_after_writes: true)
      field(:task_id, :string)
      field(:node_id, :string)
      field(:attempt, :integer)
      field(:state, :string)
      field(:scheduled_at, :utc_datetime_usec)
      field(:claimed_at, :utc_datetime_usec)
      field(:deadline_at, :utc_datetime_usec)
    end
  end
end
