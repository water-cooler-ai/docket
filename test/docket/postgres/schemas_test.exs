if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.SchemasTest do
    use ExUnit.Case, async: true

    alias Docket.Postgres.Schemas.Event
    alias Docket.Postgres.Schemas.GraphVersion
    alias Docket.Postgres.Schemas.Run

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
  end
end
