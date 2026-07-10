defmodule Docket.Runtime.RetryParkingTest do
  use Docket.Test.Case, async: true

  alias Docket.Run.{PendingWrite, TaskState, TimerState}
  alias Docket.Test.Checkpoint.FailOn

  # start fans out to flaky and steady in one superstep; flaky fails
  # retryably while steady commits its result at the first retry park.
  defp parallel_retry_graph(opts \\ []) do
    failures = Keyword.get(opts, :failures, 1.0)
    backoff_ms = Keyword.get(opts, :backoff_ms, 0)
    max_attempts = Keyword.get(opts, :max_attempts, 3)

    Graph.new!(id: "parallel-retry")
    |> Graph.put_field!("flaky_out", schema: Docket.Schema.string())
    |> Graph.put_field!("steady_out", schema: Docket.Schema.string())
    |> Graph.put_node!("flaky",
      implementation: Nodes.NotifyingFlaky,
      config: %{failures: failures, field: "flaky_out", value: "flaky done"},
      policies: %{"retry" => %{"max_attempts" => max_attempts, "backoff_ms" => backoff_ms}}
    )
    |> Graph.put_node!("steady",
      implementation: Nodes.NotifyingWrite,
      config: %{field: "steady_out", value: "steady done"}
    )
    |> Graph.put_edge!("edge_start_flaky", from: "$start", to: "flaky")
    |> Graph.put_edge!("edge_start_steady", from: "$start", to: "steady")
    |> Graph.put_edge!("edge_flaky_finish", from: "flaky", to: "$finish")
    |> Graph.put_edge!("edge_steady_finish", from: "steady", to: "$finish")
    |> Graph.put_output!("flaky_out", [])
    |> Graph.put_output!("steady_out", [])
  end

  defp collect_attempts(collected) do
    receive do
      {:attempted, node_id, attempt, _key, _state} ->
        collect_attempts([{node_id, attempt} | collected])
    after
      0 -> Enum.reverse(collected)
    end
  end

  describe "retry park control state" do
    test "completed sibling results park as pending writes, invisible until the barrier" do
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(parallel_retry_graph(), %{}, context: %{notify: self()})

      assert run.status == :done
      assert run.output == %{"flaky_out" => "flaky done", "steady_out" => "steady done"}

      # The sibling executed exactly once even though the superstep spanned
      # a retry park.
      assert_received {:executed, "steady", 1}
      refute_received {:executed, "steady", _}

      park = Enum.find(checkpoints, &(&1.type == :retry_scheduled))
      assert park.run.status == :running
      assert park.run.step == 0

      assert [
               %PendingWrite{
                 node_id: "steady",
                 attempt: 1,
                 kind: :update,
                 value: %{"steady_out" => "steady done"}
               }
             ] = park.run.pending_writes

      # The parked result stays invisible to channels until the barrier.
      assert field_value(park.run, "steady_out") == :unwritten
      assert field_value(park.run, "flaky_out") == :unwritten

      assert [{task_id, %TaskState{node_id: "flaky", attempt: 2}}] =
               Map.to_list(park.run.active_tasks)

      assert %TimerState{kind: :retry, fires_at: %DateTime{}} = park.run.timers[task_id]

      # The barrier commits the whole superstep in one step.
      step = Enum.find(checkpoints, &(&1.type == :step_committed))
      assert step.run.step == 1

      completed =
        for event <- step.events, event.type == :node_completed do
          {event.node_id, event.payload["attempt"]}
        end

      assert completed == [{"flaky", 2}, {"steady", 1}]
    end

    test "a crash during backoff resumes the persisted attempt without rerunning siblings" do
      graph = parallel_retry_graph()
      opts = [graph: graph, context: %{notify: self()}]

      {:ok, initialized, _} =
        Docket.Test.run_inline(graph, %{}, max_steps: 0, context: %{notify: self()})

      # One committed transition: the retry park.
      assert {:ok, parked, park_checkpoints} = Docket.Test.step_inline(initialized, opts)
      assert checkpoint_types(park_checkpoints) == [:retry_scheduled]

      task_id = "#{initialized.id}:0:flaky"
      assert_received {:attempted, "flaky", 1, key1, _state}
      assert key1 == "#{task_id}:1"
      assert_received {:executed, "steady", 1}

      # Crash during backoff: the wire round trip is the cold resume path.
      restored = Docket.Run.from_map!(Docket.Run.to_map(parked))
      assert restored == parked

      assert {:ok, resumed, _} =
               Docket.Test.resume_inline(graph, restored, context: %{notify: self()})

      assert resumed.status == :done
      assert resumed.output == %{"flaky_out" => "flaky done", "steady_out" => "steady done"}

      # The retry budget is not reset: execution resumes at the persisted
      # attempt 2 with the stable idempotency identity, and the committed
      # sibling result is not re-executed.
      assert_received {:attempted, "flaky", 2, key2, _state}
      assert key2 == "#{task_id}:2"
      refute_received {:attempted, "flaky", _, _, _}
      refute_received {:executed, "steady", _}
    end

    test "a park rejected by its sync checkpoint repeats the same attempt identity" do
      graph = parallel_retry_graph()

      assert {:error, error, checkpoints} =
               Docket.Test.run_inline(graph, %{},
                 checkpoint: FailOn,
                 context: %{notify: self(), fail_on: [:retry_scheduled]}
               )

      assert error.type == :checkpoint_failed
      assert_received {:attempted, "flaky", 1, key1, _state}
      assert_received {:executed, "steady", 1}

      # The initialized run stays the durable truth: nothing of the failed
      # superstep was committed.
      committed = List.last(checkpoints).run
      assert committed.active_tasks == %{}
      assert committed.pending_writes == []

      # Re-execution repeats attempt 1 - and the uncommitted sibling - with
      # byte-identical identity.
      assert {:ok, resumed, _} =
               Docket.Test.resume_inline(graph, committed, context: %{notify: self()})

      assert resumed.status == :done
      assert_received {:attempted, "flaky", 1, ^key1, _state}
      assert_received {:executed, "steady", 1}
    end

    test "retry backoff consumes the injected sleeper only after the park commits" do
      test_pid = self()

      sleeper = fn ms ->
        send(test_pid, {:slept, ms})
        :ok
      end

      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(parallel_retry_graph(backoff_ms: 25), %{},
                 sleeper: sleeper,
                 context: %{notify: self()}
               )

      assert run.status == :done

      # One park, one sleep, of exactly the configured backoff.
      assert Enum.count(checkpoints, &(&1.type == :retry_scheduled)) == 1
      assert_received {:slept, 25}
      refute_received {:slept, _}
    end
  end

  describe "heterogeneous retry deadlines" do
    test "the superstep parks at the earliest deadline and honors each task's backoff" do
      clock = fn -> ~U[2026-07-09 12:00:00.000000Z] end
      test_pid = self()

      sleeper = fn ms ->
        send(test_pid, {:slept, ms})
        :ok
      end

      graph =
        Graph.new!(id: "two-backoffs")
        |> Graph.put_field!("fast_out", schema: Docket.Schema.string())
        |> Graph.put_field!("slow_out", schema: Docket.Schema.string())
        |> Graph.put_node!("fast",
          implementation: Nodes.NotifyingFlaky,
          config: %{failures: 1.0, field: "fast_out", value: "fast done"},
          policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 10}}
        )
        |> Graph.put_node!("slow",
          implementation: Nodes.NotifyingFlaky,
          config: %{failures: 1.0, field: "slow_out", value: "slow done"},
          policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 50}}
        )
        |> Graph.put_edge!("edge_start_fast", from: "$start", to: "fast")
        |> Graph.put_edge!("edge_start_slow", from: "$start", to: "slow")
        |> Graph.put_edge!("edge_fast_finish", from: "fast", to: "$finish")
        |> Graph.put_edge!("edge_slow_finish", from: "slow", to: "$finish")

      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(graph, %{},
                 clock: clock,
                 sleeper: sleeper,
                 context: %{notify: self()}
               )

      assert run.status == :done
      assert field_value(run, "fast_out") == "fast done"
      assert field_value(run, "slow_out") == "slow done"

      # Wake at the earliest deadline, retry only what is due, then park
      # again until the later deadline.
      assert_received {:slept, 10}
      assert_received {:slept, 50}
      refute_received {:slept, _}

      assert collect_attempts([]) == [{"fast", 1}, {"slow", 1}, {"fast", 2}, {"slow", 2}]

      assert [park1, park2] = Enum.filter(checkpoints, &(&1.type == :retry_scheduled))

      # First park: both attempts failed, both parked.
      assert map_size(park1.run.active_tasks) == 2
      assert park1.run.pending_writes == []

      # Second park commits fast's completed retry as a pending write while
      # slow's deadline is still outstanding; no attempt failed, so it
      # carries no events.
      assert park2.events == []
      assert [%PendingWrite{node_id: "fast", attempt: 2}] = park2.run.pending_writes
      assert [%TaskState{node_id: "slow", attempt: 2}] = Map.values(park2.run.active_tasks)
    end
  end

  describe "interrupts during a retry park" do
    test "an open interrupt resolves mid-park and the parked snapshot stays stable" do
      graph =
        Graph.new!(id: "interrupt-during-park")
        |> Graph.put_field!("answer", schema: Docket.Schema.string())
        |> Graph.put_field!("asked", schema: Docket.Schema.string())
        |> Graph.put_field!("trigger", schema: Docket.Schema.string())
        |> Graph.put_field!("out", schema: Docket.Schema.string())
        |> Graph.put_node!("asker",
          implementation: Nodes.InterruptOnce,
          config: %{resume_field: "answer", write_field: "asked"}
        )
        |> Graph.put_node!("prep",
          implementation: Nodes.WriteStatic,
          config: %{field: "trigger", value: "go"}
        )
        |> Graph.put_node!("flaky",
          implementation: Nodes.NotifyingFlaky,
          config: %{failures: 1.0, field: "out", value: "done"},
          policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 0}}
        )
        |> Graph.put_edge!("edge_start_asker", from: "$start", to: "asker")
        |> Graph.put_edge!("edge_start_prep", from: "$start", to: "prep")
        |> Graph.put_edge!("edge_prep_flaky", from: "prep", to: "flaky")
        |> Graph.put_edge!("edge_asker_finish", from: "asker", to: "$finish")
        |> Graph.put_edge!("edge_flaky_finish", from: "flaky", to: "$finish")

      opts = [graph: graph, context: %{notify: self()}]

      # Superstep 0 commits the open interrupt and the trigger write.
      {:ok, run0, _} =
        Docket.Test.run_inline(graph, %{}, max_steps: 1, context: %{notify: self()})

      assert [interrupt_id] = Map.keys(run0.interrupts)
      assert run0.status == :running

      # Superstep 1 parks on flaky's first failure while the interrupt is
      # still open.
      assert {:ok, parked, park_checkpoints} = Docket.Test.step_inline(run0, opts)
      assert checkpoint_types(park_checkpoints) == [:retry_scheduled]
      assert map_size(parked.active_tasks) == 1
      assert_received {:attempted, "flaky", 1, _key, state1}

      # Resolving mid-park works, and the parked attempt re-executes with
      # the snapshot its superstep planned against - the resolution is not
      # visible to it.
      assert {:ok, done, _} =
               Docket.Test.resolve_interrupt_inline(parked, interrupt_id, "42", opts)

      assert done.status == :done
      assert_received {:attempted, "flaky", 2, _key, state2}
      assert state2 == state1
      refute Map.has_key?(state2, "answer")

      # The resolved value reached the interrupted node in its own later
      # superstep.
      assert field_value(done, "asked") == "42"
      assert field_value(done, "out") == "done"
    end
  end
end
