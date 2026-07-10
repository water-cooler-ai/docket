defmodule Docket.RunTest do
  use Docket.Test.Case, async: true

  alias Docket.Run
  alias Docket.Run.{ChannelState, Failure, InterruptState, PendingWrite, TaskState, TimerState}
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

  # A mid-superstep retry park: flaky is waiting out its backoff while its
  # completed siblings (a state write and an interrupt request) are parked
  # as pending writes.
  defp parked_run do
    snapshot = %{"topic" => "durable graphs", "draft" => "text"}
    task_id = "run_1:2:flaky"

    %Run{
      rich_run()
      | status: :running,
        interrupts: %{},
        active_tasks: %{
          task_id => %TaskState{
            task_id: task_id,
            node_id: "flaky",
            step: 2,
            attempt: 2,
            status: :retry_scheduled,
            input_hash: TaskState.snapshot_hash(snapshot),
            idempotency_key: "#{task_id}:2",
            snapshot: snapshot,
            source_versions: %{"topic" => 1, "draft" => 2},
            failures: [%{attempt: 1, reason: "{:flaky, 1}"}]
          }
        },
        pending_writes: [
          %PendingWrite{
            task_id: "run_1:2:steady",
            node_id: "steady",
            attempt: 1,
            kind: :update,
            value: %{"draft" => "revised"}
          },
          %PendingWrite{
            task_id: "run_1:2:asker",
            node_id: "asker",
            attempt: 1,
            kind: :interrupt,
            value: %Docket.Interrupt{
              node_id: "asker",
              prompt: "approve?",
              schema: Schema.string(),
              resume_channel: "decision"
            }
          }
        ],
        timers: %{
          task_id => %TimerState{kind: :retry, fires_at: ~U[2026-07-03 10:00:02.000000Z]}
        }
    }
  end

  describe "active superstep wire format" do
    test "round trips a parked run on struct equality" do
      run = parked_run()

      assert {:ok, loaded} = Run.from_map(Run.to_map(run))
      assert loaded == run
    end

    test "wire shape carries active tasks, pending writes, and timers" do
      map = Run.to_map(parked_run())

      assert %{
               "node_id" => "flaky",
               "attempt" => 2,
               "failures" => [%{"attempt" => 1, "reason" => "{:flaky, 1}"}]
             } = map["active_tasks"]["run_1:2:flaky"]

      assert [
               %{"kind" => "update", "node_id" => "steady", "update" => %{"draft" => "revised"}},
               %{"kind" => "interrupt", "node_id" => "asker", "interrupt" => interrupt}
             ] = map["pending_writes"]

      assert interrupt["resume_channel"] == "decision"

      assert %{"kind" => "retry", "fires_at" => "2026-07-03T10:00:02.000000Z"} =
               map["timers"]["run_1:2:flaky"]

      # Runs without an active superstep omit the keys entirely.
      plain = Run.to_map(rich_run())
      refute Map.has_key?(plain, "active_tasks")
      refute Map.has_key?(plain, "pending_writes")
      refute Map.has_key?(plain, "timers")
    end

    test "dump rejects pending writes or timers without active tasks" do
      run = %{parked_run() | active_tasks: %{}, timers: %{}}

      assert_raise Docket.Error, ~r/only durable while tasks are active/, fn ->
        Run.to_map(run)
      end
    end

    test "dump rejects an active superstep on a non-running run" do
      run = %{parked_run() | status: :waiting}

      assert_raise Docket.Error, ~r/only durable on a running run/, fn ->
        Run.to_map(run)
      end
    end

    test "load rejects an active superstep on a non-running status" do
      map = Map.put(Run.to_map(parked_run()), "status", "waiting")

      assert {:error, %Docket.Error{type: :invalid_document, message: message}} =
               Run.from_map(map)

      assert message =~ "only durable on a running run"
    end

    test "load rejects active tasks and timers that do not cover the same task IDs" do
      base = Run.to_map(parked_run())

      missing_timer = Map.delete(base, "timers")
      assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(missing_timer)

      extra_timer =
        put_in(base, ["timers", "run_1:2:ghost"], %{
          "kind" => "retry",
          "fires_at" => "2026-07-03T10:00:02.000000Z"
        })

      assert {:error, %Docket.Error{type: :invalid_document}} = Run.from_map(extra_timer)
    end

    test "load rejects pending writes without active tasks" do
      map =
        Run.to_map(parked_run())
        |> Map.delete("active_tasks")
        |> Map.delete("timers")

      assert {:error, %Docket.Error{type: :invalid_document, message: message}} =
               Run.from_map(map)

      assert message =~ "only durable while tasks are active"
    end

    test "load rejects a tampered snapshot" do
      map =
        put_in(
          Run.to_map(parked_run()),
          ["active_tasks", "run_1:2:flaky", "snapshot", "draft"],
          "tampered"
        )

      assert {:error, %Docket.Error{type: :invalid_document, message: message}} =
               Run.from_map(map)

      assert message =~ "snapshot does not match its recorded input_hash"
    end

    test "dump rejects inconsistent parked task state" do
      run = parked_run()
      [task_id] = Map.keys(run.active_tasks)
      task = run.active_tasks[task_id]

      tampered_snapshot = %{task | snapshot: Map.put(task.snapshot, "draft", "tampered")}
      tampered = %{run | active_tasks: %{task_id => tampered_snapshot}}

      assert_raise Docket.Error, ~r/snapshot does not match its recorded input_hash/, fn ->
        Run.to_map(tampered)
      end

      skipped = %{run | active_tasks: %{task_id => %{task | attempt: 4}}}

      assert_raise Docket.Error, ~r/does not follow its 1 recorded failed attempt/, fn ->
        Run.to_map(skipped)
      end

      [write, pending_interrupt] = run.pending_writes
      mismatched_value = %{pending_interrupt.value | node_id: "someone_else"}

      mismatched = %{
        run
        | pending_writes: [write, %{pending_interrupt | value: mismatched_value}]
      }

      assert_raise Docket.Error, ~r/does not match the pending write's node/, fn ->
        Run.to_map(mismatched)
      end
    end

    test "load rejects a task identity that does not match run, step, and node" do
      base = Run.to_map(parked_run())

      {entry, active} = Map.pop(base["active_tasks"], "run_1:2:flaky")

      renamed =
        base
        |> Map.put("active_tasks", Map.put(active, "run_9:2:flaky", entry))
        |> Map.put("timers", %{"run_9:2:flaky" => base["timers"]["run_1:2:flaky"]})

      assert {:error, %Docket.Error{message: message}} = Run.from_map(renamed)
      assert message =~ "stable task identity"

      bad_pending =
        update_in(base, ["pending_writes"], fn [write, interrupt] ->
          [Map.put(write, "task_id", "run_1:1:steady"), interrupt]
        end)

      assert {:error, %Docket.Error{message: message}} = Run.from_map(bad_pending)
      assert message =~ "stable task identity"
    end

    test "load rejects attempt and failure records that disagree" do
      base = Run.to_map(parked_run())

      skipped_attempt = put_in(base, ["active_tasks", "run_1:2:flaky", "attempt"], 3)
      assert {:error, %Docket.Error{message: message}} = Run.from_map(skipped_attempt)
      assert message =~ "does not follow its 1 recorded failed attempt"

      out_of_order =
        put_in(base, ["active_tasks", "run_1:2:flaky", "failures"], [
          %{"attempt" => 2, "reason" => "boom"}
        ])

      assert {:error, %Docket.Error{message: message}} = Run.from_map(out_of_order)
      assert message =~ "attempts 1..n in order"

      no_failures = put_in(base, ["active_tasks", "run_1:2:flaky", "failures"], [])
      assert {:error, %Docket.Error{message: message}} = Run.from_map(no_failures)
      assert message =~ "at least one failed attempt"
    end

    test "load rejects more than one result or attempt per node" do
      base = Run.to_map(parked_run())

      duplicated =
        update_in(base, ["pending_writes"], fn [write, interrupt] ->
          [write, write, interrupt]
        end)

      assert {:error, %Docket.Error{message: message}} = Run.from_map(duplicated)
      assert message =~ "at most one result or parked attempt per superstep"
    end

    test "load rejects malformed pending writes and timers" do
      base = Run.to_map(parked_run())

      bad_kind =
        update_in(base, ["pending_writes"], fn [write, interrupt] ->
          [Map.put(write, "kind", "sideways"), interrupt]
        end)

      assert {:error, %Docket.Error{message: message}} = Run.from_map(bad_kind)
      assert message =~ "unknown pending write kind"

      missing_update =
        update_in(base, ["pending_writes"], fn [write, interrupt] ->
          [Map.delete(write, "update"), interrupt]
        end)

      assert {:error, %Docket.Error{message: message}} = Run.from_map(missing_update)
      assert message =~ ~s(missing required key "update")

      bad_timer_kind = put_in(base, ["timers", "run_1:2:flaky", "kind"], "cron")
      assert {:error, %Docket.Error{message: message}} = Run.from_map(bad_timer_kind)
      assert message =~ "unknown timer"

      timerless =
        put_in(base, ["timers", "run_1:2:flaky"], %{"kind" => "retry"})

      assert {:error, %Docket.Error{message: message}} = Run.from_map(timerless)
      assert message =~ ~s(missing required key "fires_at")
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
