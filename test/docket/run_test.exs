defmodule Docket.RunTest do
  use Docket.Test.Case, async: true

  alias Docket.Run
  alias Docket.Run.{ChannelState, InterruptState}
  alias Docket.Schema

  defp rich_run do
    %Run{
      id: "run_1",
      graph_id: "essay-review",
      graph_hash: String.duplicate("ab", 32),
      status: :waiting,
      input: %{"topic" => "durable graphs"},
      output: nil,
      started_at: ~U[2026-07-03 10:00:00.000000Z],
      updated_at: ~U[2026-07-03 10:00:01.000000Z],
      finished_at: nil,
      step: 2,
      channels: %{
        "input:topic" => %ChannelState{
          channel_id: "input:topic",
          value: "durable graphs",
          version: 1
        },
        "state:draft" => %ChannelState{channel_id: "state:draft", value: "text", version: 2},
        "edge:edge_join" => %ChannelState{
          channel_id: "edge:edge_join",
          value: nil,
          version: 1,
          barrier_seen: ["left"]
        }
      },
      changed_channels: MapSet.new(["state:draft"]),
      pending_nodes: MapSet.new(["gate"]),
      interrupts: %{
        "interrupt_1" => %InterruptState{
          id: "interrupt_1",
          node_id: "gate",
          status: :open,
          resume_channel: "decision",
          prompt: "approve?",
          schema: Schema.string(),
          created_at: ~U[2026-07-03 10:00:01.000000Z]
        }
      },
      checkpoint_seq: 3,
      event_seq: 9,
      metadata: %{"tenant" => "acme"}
    }
  end

  describe "to_map/from_map" do
    test "round trips a rich run on struct equality" do
      run = rich_run()

      assert {:ok, loaded} = Run.from_map(Run.to_map(run))
      assert loaded == run
    end

    test "wire map is JSON-safe and omits empty collections" do
      map = Run.to_map(%Run{id: "run_2", graph_id: "g", status: :created, input: %{}})

      assert map["version"] == 1
      assert map["status"] == "created"
      refute Map.has_key?(map, "channels")
      refute Map.has_key?(map, "changed_channels")
      refute Map.has_key?(map, "interrupts")
      refute Map.has_key?(map, "output")
      refute Map.has_key?(map, "metadata")
    end

    test "id sets serialize as sorted lists" do
      map = Run.to_map(%{rich_run() | changed_channels: MapSet.new(["b", "a"])})

      assert map["changed_channels"] == ["a", "b"]
    end

    test "rejects unknown document keys" do
      map = Map.put(Run.to_map(rich_run()), "bogus", 1)

      assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(map)
    end

    test "rejects unknown statuses" do
      map = Map.put(Run.to_map(rich_run()), "status", "sideways")

      assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(map)
    end

    test "rejects missing version" do
      map = Map.delete(Run.to_map(rich_run()), "version")

      assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(map)
    end

    test "rejects newer document versions" do
      map = Map.put(Run.to_map(rich_run()), "version", 99)

      assert {:error, %Docket.Error{type: :unsupported_schema_version}} = Run.from_map(map)
    end

    test "from_map! raises on invalid documents" do
      assert_raise Docket.Error, fn -> Run.from_map!(%{"version" => 1}) end
    end

    test "non-durable channel values are rejected at dump" do
      run = %{
        rich_run()
        | channels: %{
            "state:draft" => %ChannelState{channel_id: "state:draft", value: self(), version: 1}
          }
      }

      assert_raise Docket.Error, fn -> Run.to_map(run) end
    end
  end

  describe "terminal?/1" do
    test "only done, failed, and cancelled are terminal" do
      for status <- [:done, :failed, :cancelled] do
        assert Run.terminal?(%Run{status: status})
      end

      for status <- [:created, :running, :waiting] do
        refute Run.terminal?(%Run{status: status})
      end
    end
  end
end
