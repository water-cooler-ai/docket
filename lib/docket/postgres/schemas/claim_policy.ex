if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Schemas.ClaimPolicy do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "docket_claim_policy" do
      field(:preferred_active, :integer)
      field(:max_active, :integer)
      field(:weight, :integer)
      field(:borrowing, :boolean)
      field(:policy_version, :integer, default: 0)
      field(:initialized_at, :utc_datetime_usec)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimPartition do
    @moduledoc false

    use Ecto.Schema

    alias Docket.Postgres.ClaimPolicy.Types

    @primary_key {:scope_key, :string, autogenerate: false}
    schema "docket_claim_partitions" do
      field(:preferred_active, :integer)
      field(:max_active, :integer)
      field(:weight, :integer)
      field(:borrowing, :boolean)
      field(:admin_state, Ecto.Enum, values: Types.admin_states(), default: :running)
      field(:partition_version, :integer, default: 0)
      field(:admission_epoch, :integer, default: 0)
      field(:inserted_at, :utc_datetime_usec, read_after_writes: true)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimPolicyReceipt do
    @moduledoc false

    use Ecto.Schema

    alias Docket.Postgres.ClaimPolicy.Types

    @primary_key false
    schema "docket_claim_policy_receipts" do
      field(:source, :string, primary_key: true)
      field(:event_id, :string, primary_key: true)
      field(:request_fingerprint, :binary, redact: true)
      field(:target_kind, Ecto.Enum, values: Types.target_kinds())
      field(:target_fingerprints, {:array, :binary}, redact: true)
      field(:outcome, Ecto.Enum, values: Types.outcomes())
      field(:previous_versions, {:array, :integer})
      field(:versions, {:array, :integer})
      field(:audit_id, :integer)
      field(:created_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimPolicyEvent do
    @moduledoc false

    use Ecto.Schema

    alias Docket.Postgres.ClaimPolicy.Types

    @primary_key {:audit_id, :id, autogenerate: true}
    schema "docket_claim_policy_events" do
      field(:target_kind, Ecto.Enum, values: Types.target_kinds())
      field(:target_keys, {:array, :string}, redact: true)
      field(:operation, :string)
      field(:actor, :string)
      field(:source, :string)
      field(:event_id, :string)
      field(:request_fingerprint, :binary, redact: true)
      field(:before_value, :map, redact: true)
      field(:after_value, :map, redact: true)
      field(:before_versions, {:array, :integer})
      field(:after_versions, {:array, :integer})
      field(:mode_epoch, :integer)
      field(:occurred_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimPolicyHold do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:hold_id, Ecto.UUID, autogenerate: false}
    schema "docket_claim_policy_holds" do
      field(:first_audit_id, :integer)
      field(:last_audit_id, :integer)
      field(:reason, :string)
      field(:actor, :string)
      field(:source, :string)
      field(:event_id, :string)
      field(:created_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimAuditExport do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:export_id, Ecto.UUID, autogenerate: false}
    schema "docket_claim_audit_exports" do
      field(:through_audit_id, :integer)
      field(:location_fingerprint, :binary, redact: true)
      field(:actor, :string)
      field(:source, :string)
      field(:event_id, :string)
      field(:completed_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimAssertion do
    @moduledoc false

    use Ecto.Schema

    alias Docket.Postgres.ClaimPolicy.Types

    @primary_key {:assertion_id, Ecto.UUID, autogenerate: false}
    schema "docket_claim_assertions" do
      field(:assertion_kind, Ecto.Enum, values: Types.assertion_kinds())
      field(:evidence_fingerprint, :binary, redact: true)
      field(:actor, :string)
      field(:source, :string)
      field(:event_id, :string)
      field(:asserted_at, :utc_datetime_usec, read_after_writes: true)
      field(:expires_at, :utc_datetime_usec)
      field(:audit_id, :integer)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimRollout do
    @moduledoc false

    use Ecto.Schema

    alias Docket.Postgres.ClaimPolicy.Types

    @primary_key {:id, :integer, autogenerate: false}
    schema "docket_claim_rollout" do
      field(:schema_generation, :integer, default: 2)
      field(:dual_write_assertion_id, Ecto.UUID)
      field(:backfill_phase, Ecto.Enum, values: Types.backfill_phases(), default: :not_started)
      field(:backfill_cursor, :integer)
      field(:backfill_batches, :integer, default: 0)
      field(:backfill_rows, :integer, default: 0)
      field(:backfill_completed_at, :utc_datetime_usec)
      field(:backfill_last_error, :string)
      field(:ready_index_valid, :boolean, default: false)
      field(:live_index_valid, :boolean, default: false)
      field(:fk_disposition, Ecto.Enum, values: Types.fk_dispositions(), default: :absent)
      field(:missing_partition_count, :integer)
      field(:verified_default_fingerprint, :binary, redact: true)
      field(:verified_at, :utc_datetime_usec)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimAdmissionGate do
    @moduledoc false

    use Ecto.Schema

    alias Docket.Postgres.ClaimPolicy.Types

    @primary_key {:id, :integer, autogenerate: false}
    schema "docket_claim_admission_gate" do
      field(:readiness, Ecto.Enum, values: Types.readiness_states(), default: :not_ready)
      field(:readiness_epoch, :integer, default: 0)
      field(:admission_mode, Ecto.Enum, values: Types.admission_modes(), default: :legacy)
      field(:mode_epoch, :integer, default: 0)
      field(:required_function_contract, :integer, default: 1)
      field(:updated_at, :utc_datetime_usec, read_after_writes: true)
    end
  end

  defmodule Docket.Postgres.Schemas.ClaimCapability do
    @moduledoc false

    use Ecto.Schema

    @primary_key {:instance_id, Ecto.UUID, autogenerate: false}
    schema "docket_claim_capabilities" do
      field(:binary_fingerprint, :binary, redact: true)
      field(:writer_contract, :integer)
      field(:gate_contract, :integer)
      field(:function_contract, :integer)
      field(:last_seen_at, :utc_datetime_usec)
      field(:expires_at, :utc_datetime_usec)
    end
  end
end
