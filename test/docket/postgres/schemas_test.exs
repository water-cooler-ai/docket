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
      input: %{"prompt" => "hello"},
      state: %{"channels" => %{}, "version" => 1},
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
        assert run.metadata == %{}
        assert run.failure == nil
        assert run.poisoned_at == nil
        assert run.poison_reason == nil
      end

      test "requires run identity, status, input, state, and started_at" do
        changeset = Run.changeset(%{})

        for field <- [:run_id, :graph_id, :graph_hash, :status, :input, :state, :started_at] do
          assert {_msg, [validation: :required]} = changeset.errors[field]
        end
      end

      test "carries failure and poison facts" do
        attrs =
          Map.merge(@valid_run, %{
            status: :failed,
            failure: %{"code" => "node_failed", "message" => "boom"},
            finished_at: ~U[2026-07-09 00:00:01.000000Z]
          })

        run = Ecto.Changeset.apply_changes(Run.changeset(attrs))

        assert run.failure == %{"code" => "node_failed", "message" => "boom"}

        poisoned =
          Map.merge(@valid_run, %{
            poisoned_at: ~U[2026-07-09 00:00:01.000000Z],
            poison_reason: %{"kind" => "max_claim_attempts"}
          })

        poisoned_run = Ecto.Changeset.apply_changes(Run.changeset(poisoned))

        assert poisoned_run.poisoned_at == ~U[2026-07-09 00:00:01.000000Z]
        assert poisoned_run.poison_reason == %{"kind" => "max_claim_attempts"}
      end

      test "status values mirror Docket.Run.durable_statuses/0" do
        assert Ecto.Enum.values(Run, :status) == Docket.Run.durable_statuses()

        for status <- [:sideways, :created] do
          changeset = Run.changeset(Map.put(@valid_run, :status, status))

          refute changeset.valid?
        end
      end

      test "latest_checkpoint_type values mirror Docket.Checkpoint.types/0" do
        assert Ecto.Enum.values(Run, :latest_checkpoint_type) == Docket.Checkpoint.types()
      end

      test "rejects negative counters" do
        for field <- [:step, :checkpoint_seq, :claim_attempts] do
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
                 graph: %{"nodes" => []}
               }).valid?
      end
    end

    describe "Event.changeset/2" do
      @valid_event %{
        run_id: "run_1",
        seq: 1,
        type: :node_completed,
        step: 3,
        occurred_at: ~U[2026-07-09 00:00:00.000000Z]
      }

      test "valid without optional origin columns; payload and metadata default" do
        changeset = Event.changeset(@valid_event)

        assert changeset.valid?

        event = Ecto.Changeset.apply_changes(changeset)

        assert event.payload == %{}
        assert event.metadata == %{}
        assert event.node_id == nil
      end

      test "type values mirror Docket.Event.types/0" do
        assert Ecto.Enum.values(Event, :type) == Docket.Event.types()
      end
    end
  end
end
