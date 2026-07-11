defmodule Docket.RunTest do
  use Docket.Test.Case, async: true

  alias Docket.Run
  alias Docket.Run.{ChannelState, Failure, PendingWrite, TaskState, TimerState}

  defp durable_run do
    %Run{
      id: "run_1",
      graph_id: "essay-review",
      graph_hash: String.duplicate("ab", 32),
      status: :waiting,
      input: %{"topic" => "durable graphs"},
      started_at: ~U[2026-07-03 10:00:00Z],
      updated_at: ~U[2026-07-03 10:00:01Z],
      metadata: %{"tenant" => "acme"}
    }
  end

  defp failed_run do
    %{
      durable_run()
      | status: :failed,
        finished_at: ~U[2026-07-03 10:00:02Z],
        failure: Failure.new("node_failed", "reviewer failed", node_id: "reviewer")
    }
  end

  describe "status helpers" do
    test "exposes exactly the five durable values" do
      assert Run.durable_statuses() == [:running, :waiting, :done, :failed, :cancelled]

      for status <- Run.durable_statuses(), do: assert(Run.durable_status?(status))
      refute Run.durable_status?(:created)
      refute Run.durable_status?(:blocked)
      refute Run.durable_status?("running")
    end

    test "only done, failed, and cancelled are terminal" do
      for status <- [:done, :failed, :cancelled], do: assert(Run.terminal?(%Run{status: status}))

      for status <- [:created, :running, :waiting],
          do: refute(Run.terminal?(%Run{status: status}))
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

    test "allows exactly the transition matrix" do
      for from <- @all_statuses, to <- @all_statuses do
        assert Run.valid_transition?(from, to) == {from, to} in @allowed
      end
    end
  end

  describe "validate_failure/1" do
    test "accepts failure exactly on failed runs" do
      assert :ok = Run.validate_failure(failed_run())
      assert :ok = Run.validate_failure(durable_run())

      assert {:error, %Docket.Error{type: :invalid_run}} =
               Run.validate_failure(%{failed_run() | failure: nil})

      failure = Failure.new("node_failed", "boom")

      assert {:error, %Docket.Error{type: :invalid_run}} =
               Run.validate_failure(%{durable_run() | failure: failure})
    end
  end

  describe "validate_durable/1" do
    test "accepts valid nonterminal and terminal runs" do
      assert :ok = Run.validate_durable(durable_run())
      assert :ok = Run.validate_durable(failed_run())
    end

    test "returns a typed error for failure and status mismatches" do
      assert {:error, %Docket.Error{type: :invalid_run}} =
               Run.validate_durable(%{failed_run() | failure: nil})

      assert {:error, %Docket.Error{type: :invalid_run}} =
               Run.validate_durable(%{durable_run() | failure: failed_run().failure})
    end

    test "rejects private status, malformed identity, counters, and document shapes" do
      invalid = [
        %{durable_run() | status: :created},
        %{durable_run() | graph_hash: nil},
        %{durable_run() | step: -1},
        %{durable_run() | input: []},
        %{durable_run() | output: %{}},
        %{durable_run() | channels: []},
        %{durable_run() | changed_channels: ["x"]},
        %{durable_run() | changed_channels: %MapSet{map: :corrupt}}
      ]

      for run <- invalid do
        assert {:error, %Docket.Error{type: :invalid_run}} = Run.validate_durable(run)
      end
    end

    test "requires terminal runs to have a UTC finish and nonterminal runs not to" do
      done = %{durable_run() | status: :done}
      waiting = %{durable_run() | finished_at: ~U[2026-07-03 10:00:02Z]}

      assert {:error, %Docket.Error{}} = Run.validate_durable(done)
      assert {:error, %Docket.Error{}} = Run.validate_durable(waiting)
    end

    test "checks active-superstep relationships without a wire schema" do
      snapshot = %{}
      task_id = TaskState.task_id("run_1", 0, "n")

      task = %TaskState{
        task_id: task_id,
        node_id: "n",
        step: 0,
        attempt: 2,
        status: :retry_scheduled,
        input_hash: TaskState.snapshot_hash(snapshot),
        idempotency_key: TaskState.idempotency_key(task_id, 2),
        snapshot: snapshot,
        source_versions: %{},
        failures: [%{attempt: 1, reason: "retry"}]
      }

      timer = %TimerState{kind: :retry, fires_at: ~U[2026-07-03 10:00:02Z]}

      run = %{
        durable_run()
        | status: :running,
          active_tasks: %{task.task_id => task},
          timers: %{task.task_id => timer}
      }

      assert :ok = Run.validate_durable(run)

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(%{run | timers: %{}})

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(%{
                 run
                 | timers: %{task_id => %TimerState{kind: :retry, fires_at: :tomorrow}}
               })

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(%{
                 run
                 | active_tasks: %{task_id => %{task | attempt: 0}}
               })

      duplicate = %PendingWrite{
        task_id: "run_1:1:n-result",
        node_id: "n",
        attempt: 1,
        kind: :update,
        value: %{}
      }

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(%{run | pending_writes: [duplicate]})

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(%{run | status: :waiting})
    end

    test "rejects malformed durable representations without raising" do
      cold = String.to_atom("docket_run_shape_#{System.unique_integer([:positive])}")
      started_at = durable_run().started_at
      malformed_set = %MapSet{map: %{"x" => cold}}
      malformed_channel = Map.delete(%ChannelState{channel_id: "x"}, :version)
      malformed_datetime = %{started_at | zone_abbr: cold}
      invalid_time = %{started_at | hour: 25}

      invalid_runs = [
        %{durable_run() | input: %{"bad" => [1 | 2]}},
        %{durable_run() | changed_channels: malformed_set},
        %{durable_run() | channels: %{"x" => malformed_channel}},
        %{durable_run() | started_at: malformed_datetime},
        %{durable_run() | started_at: invalid_time},
        Map.put(durable_run(), :unexpected, true)
      ]

      for run <- invalid_runs do
        assert {:error, %Docket.Error{type: :invalid_run}} = Run.validate_durable(run)
      end
    end
  end

  describe "Failure.new/3" do
    test "builds a failure and validates its fields" do
      assert Failure.new("node_failed", "boom", node_id: "n1", details: %{"k" => "v"}) ==
               %Failure{
                 code: "node_failed",
                 message: "boom",
                 node_id: "n1",
                 details: %{"k" => "v"}
               }

      assert_raise ArgumentError, fn -> Failure.new("", "y") end
      assert_raise ArgumentError, fn -> Failure.new(:code, "y") end
      assert_raise ArgumentError, fn -> Failure.new("x", nil) end
      assert_raise ArgumentError, fn -> Failure.new("x", "y", details: [1]) end
    end
  end
end
