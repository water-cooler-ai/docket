if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.SchemasTest do
    use ExUnit.Case, async: true

    alias Docket.Postgres.Schemas.Event
    alias Docket.Postgres.Schemas.GraphVersion
    alias Docket.Postgres.Schemas.Run

    alias Docket.Postgres.Schemas.{
      ClaimAdmissionGate,
      ClaimAssertion,
      ClaimAuditExport,
      ClaimCapability,
      ClaimPartition,
      ClaimPolicy,
      ClaimPolicyEvent,
      ClaimPolicyHold,
      ClaimPolicyReceipt,
      ClaimRollout
    }

    @valid_run %{
      run_id: "run_1",
      graph_id: "g1",
      graph_hash: "abc123",
      status: :running,
      state: <<131, 106>>,
      started_at: ~U[2026-07-09 00:00:00.000000Z]
    }

    describe "Run.changeset/2" do
      test "valid with the required public fields and state" do
        changeset = Run.changeset(@valid_run)

        assert changeset.valid?
      end

      test "tenant_id is never required" do
        changeset = Run.changeset(@valid_run)

        assert changeset.valid?
        assert Ecto.Changeset.get_field(changeset, :tenant_id) == nil

        with_tenant = Run.changeset(Map.put(@valid_run, :tenant_id, "acme"))

        assert with_tenant.valid?
      end

      test "rejects an empty tenant and keeps generated scope_key read-only" do
        refute Run.changeset(Map.put(@valid_run, :tenant_id, "")).valid?

        run =
          @valid_run
          |> Map.put(:scope_key, "forged")
          |> Run.changeset()
          |> Ecto.Changeset.apply_changes()

        assert run.scope_key == nil
      end

      test "applies operational defaults" do
        run = Ecto.Changeset.apply_changes(Run.changeset(@valid_run))

        assert run.step == 0
        assert run.checkpoint_seq == 0
        assert run.claim_attempts == 0
        assert run.poisoned_at == nil
        assert run.poison_reason == nil
        assert :state in Run.__schema__(:redact_fields)
      end

      test "requires run identity, status, state, and started_at" do
        changeset = Run.changeset(%{})

        for field <- [:run_id, :graph_id, :graph_hash, :status, :state, :started_at] do
          assert {_msg, [validation: :required]} = changeset.errors[field]
        end
      end

      test "carries operational poison facts" do
        poisoned =
          Map.merge(@valid_run, %{
            poisoned_at: ~U[2026-07-09 00:00:01.000000Z],
            poison_reason: "max_claim_attempts_exceeded"
          })

        poisoned_run = Ecto.Changeset.apply_changes(Run.changeset(poisoned))

        assert poisoned_run.poisoned_at == ~U[2026-07-09 00:00:01.000000Z]
        assert poisoned_run.poison_reason == "max_claim_attempts_exceeded"
      end

      test "status values mirror Docket.Run.durable_statuses/0" do
        assert Ecto.Enum.values(Run, :status) == Docket.Run.durable_statuses()

        for status <- [:sideways, :created] do
          changeset = Run.changeset(Map.put(@valid_run, :status, status))

          refute changeset.valid?
        end
      end

      test "latest_checkpoint_type values mirror runtime checkpoint types" do
        assert Ecto.Enum.values(Run, :latest_checkpoint_type) == Docket.Checkpoint.types()
      end

      test "rejects negative counters" do
        for field <- [:step, :checkpoint_seq, :claim_attempts, :claim_abandons] do
          changeset = Run.changeset(Map.put(@valid_run, field, -1))

          refute changeset.valid?
        end
      end
    end

    describe "GraphVersion.changeset/2" do
      test "requires the content address and the document" do
        changeset = GraphVersion.changeset(%{})

        for field <- [:graph_id, :graph_hash, :graph] do
          assert {_msg, [validation: :required]} = changeset.errors[field]
        end

        assert GraphVersion.changeset(%{
                 graph_id: "g1",
                 graph_hash: "abc123",
                 graph: <<131, 106>>
               }).valid?
      end

      test "accepts a tenant owner, rejects empty tenants, and does not cast scope_key" do
        attrs = %{
          tenant_id: "acme",
          scope_key: "forged",
          graph_id: "g1",
          graph_hash: "abc123",
          graph: <<131, 106>>
        }

        graph = attrs |> GraphVersion.changeset() |> Ecto.Changeset.apply_changes()

        assert graph.tenant_id == "acme"
        assert graph.scope_key == nil
        refute GraphVersion.changeset(%{attrs | tenant_id: ""}).valid?
      end
    end

    describe "Event.changeset/2" do
      @valid_event %{
        run_id: "run_1",
        seq: 1,
        type: :node_completed,
        step: 3,
        payload: <<131, 116, 0, 0, 0, 0>>,
        metadata: <<131, 116, 0, 0, 0, 0>>,
        occurred_at: ~U[2026-07-09 00:00:00.000000Z]
      }

      test "valid without optional origin columns" do
        changeset = Event.changeset(@valid_event)

        assert changeset.valid?

        event = Ecto.Changeset.apply_changes(changeset)

        assert is_binary(event.payload)
        assert is_binary(event.metadata)
        assert event.node_id == nil
        assert Enum.sort(Event.__schema__(:redact_fields)) == [:metadata, :payload]
      end

      test "type values mirror Docket.Event.types/0" do
        assert Ecto.Enum.values(Event, :type) == Docket.Event.types()
      end
    end

    describe "v2 source-owned row codecs" do
      test "bind every schema to its prefix-local source" do
        assert %{
                 ClaimPolicy => "docket_claim_policy",
                 ClaimPartition => "docket_claim_partitions",
                 ClaimPolicyReceipt => "docket_claim_policy_receipts",
                 ClaimPolicyEvent => "docket_claim_policy_events",
                 ClaimPolicyHold => "docket_claim_policy_holds",
                 ClaimAuditExport => "docket_claim_audit_exports",
                 ClaimAssertion => "docket_claim_assertions",
                 ClaimRollout => "docket_claim_rollout",
                 ClaimAdmissionGate => "docket_claim_admission_gate",
                 ClaimCapability => "docket_claim_capabilities"
               } ==
                 Map.new(
                   [
                     ClaimPolicy,
                     ClaimPartition,
                     ClaimPolicyReceipt,
                     ClaimPolicyEvent,
                     ClaimPolicyHold,
                     ClaimAuditExport,
                     ClaimAssertion,
                     ClaimRollout,
                     ClaimAdmissionGate,
                     ClaimCapability
                   ],
                   &{&1, &1.__schema__(:source)}
                 )
      end

      test "marks both durable receipt identity columns as the composite primary key" do
        assert ClaimPolicyReceipt.__schema__(:primary_key) == [:source, :event_id]
      end

      test "own the exact durable enum string mappings" do
        assert Ecto.Enum.mappings(ClaimPartition, :admin_state) ==
                 [running: "running", hold_new: "hold_new", drain: "drain"]

        assert Ecto.Enum.mappings(ClaimPolicyReceipt, :target_kind) ==
                 [
                   default: "default",
                   partition: "partition",
                   bulk: "bulk",
                   activation: "activation",
                   readiness: "readiness",
                   audit: "audit"
                 ]

        assert Ecto.Enum.mappings(ClaimPolicyReceipt, :outcome) ==
                 [applied: "applied", unchanged: "unchanged", demoted: "demoted"]

        assert Ecto.Enum.mappings(ClaimAssertion, :assertion_kind) ==
                 [dual_write: "dual_write", old_binaries_absent: "old_binaries_absent"]

        assert Ecto.Enum.mappings(ClaimRollout, :backfill_phase) ==
                 [
                   not_started: "not_started",
                   running: "running",
                   reconciling: "reconciling",
                   complete: "complete"
                 ]

        assert Ecto.Enum.mappings(ClaimRollout, :fk_disposition) ==
                 [absent: "absent", not_valid: "not_valid", validated: "validated"]

        assert Ecto.Enum.mappings(ClaimAdmissionGate, :readiness) ==
                 [not_ready: "not_ready", ready: "ready"]

        assert Ecto.Enum.mappings(ClaimAdmissionGate, :admission_mode) ==
                 [legacy: "legacy", tenant_fair: "tenant_fair"]
      end

      test "redact policy fingerprints and target identity" do
        assert Enum.sort(ClaimPolicyReceipt.__schema__(:redact_fields)) ==
                 [:request_fingerprint, :target_fingerprints]

        assert Enum.sort(ClaimPolicyEvent.__schema__(:redact_fields)) ==
                 [:after_value, :before_value, :request_fingerprint, :target_keys]

        assert ClaimCapability.__schema__(:redact_fields) == [:binary_fingerprint]
      end
    end
  end
end
