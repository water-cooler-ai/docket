if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.ClaimPolicy do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "docket_claim_policy" do
      field(:admission_mode, Ecto.Enum, values: [:legacy, :tenant_fair], default: :legacy)
      field(:max_active, :integer)
      field(:policy_version, :integer, default: 0)
      field(:initialized_at, :utc_datetime_usec)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimPartition do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:scope_key, :string, autogenerate: false}
    schema "docket_claim_partitions" do
      field(:max_active, :integer)
      field(:partition_version, :integer, default: 0)
      field(:admission_epoch, :integer, default: 0)
      field(:inserted_at, :utc_datetime_usec, read_after_writes: true)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimSchedule do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:scope_key, :string, autogenerate: false}
    schema "docket_claim_schedule" do
      field(:ring_position, :integer, read_after_writes: true)
      field(:may_have_ready_at, :utc_datetime_usec)
      field(:may_have_claimed_at, :utc_datetime_usec)
      field(:ready_candidate_cursor_at, :utc_datetime_usec)
      field(:ready_candidate_cursor_id, :integer)
      field(:expired_candidate_cursor_at, :utc_datetime_usec)
      field(:expired_candidate_cursor_id, :integer)
      field(:ready_dirty, :boolean, default: true)
      field(:claimed_dirty, :boolean, default: true)
      field(:in_cohort, :boolean, read_after_writes: true)
      field(:inserted_at, :utc_datetime_usec, read_after_writes: true)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimScanCursor do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "docket_claim_scan_cursor" do
      field(:ring_position, :integer, default: 0)
      field(:scan_call_sequence, :integer, default: 0)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimReadyReconciliation do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "docket_claim_ready_reconciliation" do
      field(:last_scope_key, :string, default: "")
      field(:wrap_count, :integer, default: 0)
      field(:next_scan_call, :integer, default: 0)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimExpiredReconciliation do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "docket_claim_expired_reconciliation" do
      field(:last_scope_key, :string, default: "")
      field(:wrap_count, :integer, default: 0)
      field(:next_scan_call, :integer, default: 16)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end
end
