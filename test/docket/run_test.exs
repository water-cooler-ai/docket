defmodule Docket.RunTest do
  use Docket.Test.Case, async: true

  alias Docket.Run
  alias Docket.Run.{ChannelState, Failure, InterruptState}
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

  defp failed_run do
    %Run{
      rich_run()
      | status: :failed,
        interrupts: %{},
        finished_at: ~U[2026-07-03 10:00:02.000000Z],
        failure:
          Failure.new("node_failed", "node(s) reviewer failed permanently",
            node_id: "reviewer",
            details: %{"nodes" => ["reviewer"], "errors" => %{"reviewer" => ":boom"}}
          )
    }
  end

  describe "to_map/from_map" do
    test "round trips a rich run on struct equality" do
      run = rich_run()

      assert {:ok, loaded} = Run.from_map(Run.to_map(run))
      assert loaded == run
    end

    test "round trips a failed run with its failure payload" do
      run = failed_run()
      map = Run.to_map(run)

      assert map["status"] == "failed"

      assert map["failure"] == %{
               "code" => "node_failed",
               "message" => "node(s) reviewer failed permanently",
               "node_id" => "reviewer",
               "details" => %{"nodes" => ["reviewer"], "errors" => %{"reviewer" => ":boom"}}
             }

      assert {:ok, loaded} = Run.from_map(map)
      assert loaded == run
    end

    test "failure wire map omits absent node_id and empty details" do
      run = %{
        failed_run()
        | failure: Failure.new("max_supersteps_exceeded", "run exceeded the superstep limit")
      }

      map = Run.to_map(run)

      assert map["failure"] == %{
               "code" => "max_supersteps_exceeded",
               "message" => "run exceeded the superstep limit"
             }

      assert {:ok, loaded} = Run.from_map(map)
      assert loaded.failure == run.failure
    end

    test "wire map is version 2, JSON-safe, and omits empty collections" do
      map = Run.to_map(%Run{id: "run_2", graph_id: "g", status: :running, input: %{}})

      assert map["version"] == 2
      assert map["status"] == "running"
      refute Map.has_key?(map, "channels")
      refute Map.has_key?(map, "changed_channels")
      refute Map.has_key?(map, "interrupts")
      refute Map.has_key?(map, "output")
      refute Map.has_key?(map, "failure")
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

    test "rejects any version other than the current one" do
      for version <- [1, 99] do
        map = Map.put(Run.to_map(rich_run()), "version", version)

        assert {:error, %Docket.Error{type: :unsupported_schema_version}} = Run.from_map(map)
      end

      map = Map.put(Run.to_map(rich_run()), "version", "2")

      assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(map)
    end

    test "from_map! raises on invalid documents" do
      assert_raise Docket.Error, fn -> Run.from_map!(%{"version" => 2}) end
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

  describe "created sentinel rejection" do
    test "dump rejects a :created run" do
      run = %Run{id: "run_2", graph_id: "g", status: :created, input: %{}}

      assert_raise Docket.Error, ~r/private initialization sentinel/, fn -> Run.to_map(run) end
    end

    test "load rejects the created status" do
      map = Map.put(Run.to_map(rich_run()), "status", "created")

      assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(map)
    end
  end

  describe "failed-iff-failure enforcement at the wire boundary" do
    test "dump rejects a failed run without a failure" do
      run = %{failed_run() | failure: nil}

      assert_raise Docket.Error, ~r/must carry a Docket.Run.Failure/, fn -> Run.to_map(run) end
    end

    test "dump rejects a non-failed run carrying a failure" do
      run = %{rich_run() | failure: Failure.new("node_failed", "boom")}

      assert_raise Docket.Error, ~r/only present on a failed run/, fn -> Run.to_map(run) end
    end

    test "load rejects a failed document without a failure" do
      map = Map.delete(Run.to_map(failed_run()), "failure")

      assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(map)
    end

    test "load rejects a non-failed document carrying a failure" do
      map =
        rich_run()
        |> Run.to_map()
        |> Map.put("failure", %{"code" => "node_failed", "message" => "boom"})

      assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(map)
    end

    test "load rejects malformed failure payloads" do
      base = Run.to_map(failed_run())

      for failure <- [
            %{"message" => "no code"},
            %{"code" => 1, "message" => "bad code"},
            %{"code" => "x", "message" => "y", "bogus" => true},
            %{"code" => "x", "message" => "y", "details" => "not a map"},
            "not a map"
          ] do
        map = Map.put(base, "failure", failure)

        assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(map),
               "expected rejection for #{inspect(failure)}"
      end
    end
  end

  describe "status helpers" do
    test "durable_statuses/0 exposes exactly the five durable values" do
      assert Run.durable_statuses() == [:running, :waiting, :done, :failed, :cancelled]
    end

    test "durable_status?/1 rejects the created sentinel and unknown values" do
      for status <- Run.durable_statuses() do
        assert Run.durable_status?(status)
      end

      refute Run.durable_status?(:created)
      refute Run.durable_status?(:blocked)
      refute Run.durable_status?("running")
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

  describe "valid_transition?/2" do
    @all_statuses [:created, :running, :waiting, :done, :failed, :cancelled]

    @allowed [
      {:created, :running},
      {:running, :running},
      {:running, :waiting},
      {:running, :done},
      {:running, :failed},
      {:running, :cancelled},
      {:waiting, :running},
      {:waiting, :cancelled}
    ]

    test "allows exactly the transition matrix over every status pair" do
      for from <- @all_statuses, to <- @all_statuses do
        expected = {from, to} in @allowed

        assert Run.valid_transition?(from, to) == expected,
               "expected valid_transition?(#{inspect(from)}, #{inspect(to)}) " <>
                 "to be #{expected}"
      end
    end

    test "terminal statuses are absorbing" do
      for from <- [:done, :failed, :cancelled], to <- @all_statuses do
        refute Run.valid_transition?(from, to)
      end
    end
  end

  describe "validate_failure/1" do
    test "accepts a failed run with a failure and non-failed runs without one" do
      assert Run.validate_failure(failed_run()) == :ok
      assert Run.validate_failure(rich_run()) == :ok
      assert Run.validate_failure(%Run{id: "r", graph_id: "g", status: :created}) == :ok
    end

    test "rejects a failed run without a failure" do
      assert {:error, %Docket.Error{type: :invalid_run}} =
               Run.validate_failure(%{failed_run() | failure: nil})
    end

    test "rejects a failed run whose failure has the wrong type" do
      assert {:error, %Docket.Error{type: :invalid_run}} =
               Run.validate_failure(%{failed_run() | failure: %{"code" => "x"}})
    end

    test "rejects any non-failed status carrying a failure" do
      failure = Failure.new("node_failed", "boom")

      for status <- [:created, :running, :waiting, :done, :cancelled] do
        assert {:error, %Docket.Error{type: :invalid_run}} =
                 Run.validate_failure(%{rich_run() | status: status, failure: failure})
      end
    end
  end

  describe "Failure.new/3" do
    test "builds a failure with optional node and details" do
      failure = Failure.new("node_failed", "boom", node_id: "n1", details: %{"k" => "v"})

      assert failure == %Failure{
               code: "node_failed",
               message: "boom",
               node_id: "n1",
               details: %{"k" => "v"}
             }
    end

    test "defaults node_id to nil and details to an empty map" do
      assert Failure.new("x", "y") == %Failure{code: "x", message: "y", details: %{}}
    end

    test "rejects malformed fields" do
      assert_raise ArgumentError, fn -> Failure.new("", "y") end
      assert_raise ArgumentError, fn -> Failure.new(:code, "y") end
      assert_raise ArgumentError, fn -> Failure.new("x", nil) end
      assert_raise ArgumentError, fn -> Failure.new("x", "y", node_id: "") end
      assert_raise ArgumentError, fn -> Failure.new("x", "y", details: [1]) end
    end
  end
end
