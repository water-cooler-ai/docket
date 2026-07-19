if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.ClaimPolicy do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "docket_claim_policy" do
      field(:admission_mode, Ecto.Enum, values: [:legacy, :tenant_fair], default: :legacy)
      field(:max_active, :integer)
      field(:policy_version, :integer, default: 0)
      field(:scan_ring_position, :integer, default: 0)
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
      field(:unfinished_count, :integer, default: 0)
      field(:inserted_at, :utc_datetime_usec, read_after_writes: true)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end
end
