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
      assert Run.terminal_statuses() == [:done, :failed, :cancelled]

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

  describe "validate_durable/1 detached task shapes" do
    @scheduled ~U[2026-07-03 10:00:02.000000Z]
    @deadline ~U[2026-07-03 10:00:32.000000Z]

    defp pending_task(overrides) do
      snapshot = %{}
      task_id = TaskState.task_id("run_1", 0, "n")

      struct!(
        %TaskState{
          task_id: task_id,
          node_id: "n",
          step: 0,
          attempt: 1,
          status: :detached_pending,
          input_hash: TaskState.snapshot_hash(snapshot),
          idempotency_key: TaskState.idempotency_key(task_id, 1),
          snapshot: snapshot,
          source_versions: %{},
          scheduled_at: @scheduled,
          failures: []
        },
        overrides
      )
    end

    defp running_with(task, timers) do
      %{
        durable_run()
        | status: :running,
          active_tasks: %{task.task_id => task},
          timers: timers
      }
    end

    test "an unbounded pending task is legal with no timer and no deadline" do
      task = pending_task([])
      assert :ok = Run.validate_durable(running_with(task, %{}))
    end

    test "a bounded pending task requires its schedule-to-start timer to match" do
      task = pending_task(deadline_at: @deadline)
      timer = %TimerState{kind: :schedule_to_start, fires_at: @deadline}

      assert :ok = Run.validate_durable(running_with(task, %{task.task_id => timer}))

      # Deadline without a timer, timer without a deadline, and a mismatched
      # or wrong-kind timer are all unrepresentable.
      assert {:error, %Docket.Error{}} = Run.validate_durable(running_with(task, %{}))

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(running_with(pending_task([]), %{task.task_id => timer}))

      late = %TimerState{kind: :schedule_to_start, fires_at: DateTime.add(@deadline, 1)}

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(running_with(task, %{task.task_id => late}))

      retry = %TimerState{kind: :retry, fires_at: @deadline}

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(running_with(task, %{task.task_id => retry}))
    end

    test "a pending task never carries a claim instant or token material" do
      claimed_at = pending_task(started_at: @scheduled)
      assert {:error, %Docket.Error{}} = Run.validate_durable(running_with(claimed_at, %{}))
    end

    test "a claimed task without a start-to-close deadline is unrepresentable" do
      claimed =
        pending_task(status: :detached_claimed, started_at: @scheduled, deadline_at: @deadline)

      assert :ok = Run.validate_durable(running_with(claimed, %{}))

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(running_with(%{claimed | deadline_at: nil}, %{}))

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(running_with(%{claimed | started_at: nil}, %{}))

      # A claimed task carries no timer in this schema; a stray one has no
      # owning semantics.
      stray = %TimerState{kind: :schedule_to_start, fires_at: @deadline}

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(running_with(claimed, %{claimed.task_id => stray}))
    end

    test "a fresh pending attempt has no failures; the attempt count still binds" do
      # Attempt 2 with an empty history breaks attempt == failures + 1.
      task_id = TaskState.task_id("run_1", 0, "n")

      stale =
        pending_task(attempt: 2, idempotency_key: TaskState.idempotency_key(task_id, 2))

      assert {:error, %Docket.Error{}} = Run.validate_durable(running_with(stale, %{}))

      # A retry-scheduled attempt still requires a non-empty history.
      retry_task =
        pending_task(status: :retry_scheduled, scheduled_at: nil, failures: [])

      timer = %TimerState{kind: :retry, fires_at: @deadline}

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(running_with(retry_task, %{retry_task.task_id => timer}))
    end

    test "a retry-scheduled attempt rejects detached lifetimes" do
      task_id = TaskState.task_id("run_1", 0, "n")

      retry_task =
        pending_task(
          status: :retry_scheduled,
          attempt: 2,
          idempotency_key: TaskState.idempotency_key(task_id, 2),
          failures: [%{attempt: 1, reason: "boom"}]
        )

      timer = %TimerState{kind: :retry, fires_at: @deadline}
      timers = %{retry_task.task_id => timer}

      assert {:error, %Docket.Error{}} =
               Run.validate_durable(running_with(retry_task, timers))

      assert :ok =
               Run.validate_durable(running_with(%{retry_task | scheduled_at: nil}, timers))
    end

    test "unknown task statuses and stray timers stay invalid" do
      unknown = pending_task(status: :detached)
      assert {:error, %Docket.Error{}} = Run.validate_durable(running_with(unknown, %{}))

      task = pending_task([])
      stray = %{"other_task" => %TimerState{kind: :retry, fires_at: @deadline}}
      assert {:error, %Docket.Error{}} = Run.validate_durable(running_with(task, stray))
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
