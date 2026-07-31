defmodule Docket.Runtime.DetachedParkingTest do
  use Docket.Test.Case, async: true

  alias Docket.Run.{PendingWrite, TaskState, TimerState}

  # start fans out to waits and steady in one superstep; waits detaches with
  # a durable token while steady commits its result at the detach park.
  defp parallel_detach_graph(opts \\ []) do
    detach = Keyword.get(opts, :detach, %{"deadline_ms" => 50})
    max_attempts = Keyword.get(opts, :max_attempts, 1)

    policies =
      %{"retry" => %{"max_attempts" => max_attempts, "backoff_ms" => 0}}
      |> then(fn policies ->
        if detach, do: Map.put(policies, "detach", detach), else: policies
      end)

    Graph.new!(id: "parallel-detach")
    |> Graph.put_field!("waits_out", schema: Docket.Schema.string())
    |> Graph.put_field!("steady_out", schema: Docket.Schema.string())
    |> Graph.put_node!("waits",
      implementation: Nodes.Detaches,
      config: %{token: "job-123"},
      policies: policies
    )
    |> Graph.put_node!("steady",
      implementation: Nodes.NotifyingWrite,
      config: %{field: "steady_out", value: "steady done"}
    )
    |> Graph.put_edge!("edge_start_waits", from: "$start", to: "waits")
    |> Graph.put_edge!("edge_start_steady", from: "$start", to: "steady")
    |> Graph.put_edge!("edge_waits_finish", from: "waits", to: "$finish")
    |> Graph.put_edge!("edge_steady_finish", from: "steady", to: "$finish")
    |> Graph.put_output!("waits_out", [])
    |> Graph.put_output!("steady_out", [])
  end

  # The inline shell serves a committed park's wait through the sleeper, so
  # parking to inspect the detach state must not wait out the real deadline.
  defp park_inline(graph, opts) do
    opts = Keyword.put_new(opts, :sleeper, fn _ms -> :ok end)
    {:ok, initialized, _} = Docket.Test.run_inline(graph, %{}, Keyword.put(opts, :max_steps, 0))
    {:ok, parked, checkpoints} = Docket.Test.step_inline(initialized, [{:graph, graph} | opts])
    {initialized, parked, checkpoints}
  end

  describe "detach park control state" do
    test "a detach commits durably without consuming the attempt as a failure" do
      clock = fn -> ~U[2026-07-30 12:00:00.000000Z] end
      graph = parallel_detach_graph()
      opts = [clock: clock, context: %{notify: self()}]

      {initialized, parked, checkpoints} = park_inline(graph, opts)

      assert checkpoint_types(checkpoints) == [:detach_scheduled]
      [park] = checkpoints

      # AC1: the attempt is not consumed and no failure is recorded.
      assert park.run.status == :running
      assert park.run.step == 0
      refute Enum.any?(park.events, &(&1.type == :node_failed))

      task_id = "#{initialized.id}:0:waits"

      assert %TaskState{
               node_id: "waits",
               step: 0,
               attempt: 1,
               status: :detached,
               failures: [],
               idempotency_key: idempotency_key,
               input_hash: input_hash,
               started_at: ~U[2026-07-30 12:00:00.000000Z],
               deadline_at: deadline_at,
               metadata: %{"detach_token" => "job-123"}
             } = parked.active_tasks[task_id]

      # AC2: the park records the stable task identity and the mandatory
      # deadline, equal to its timer.
      assert idempotency_key == "#{task_id}:1"
      assert is_binary(input_hash)
      assert deadline_at == ~U[2026-07-30 12:00:00.050000Z]

      assert %TimerState{kind: :detached_deadline, fires_at: ^deadline_at} =
               parked.timers[task_id]

      # The completed sibling parks as a pending write, invisible until the
      # barrier.
      assert [%PendingWrite{node_id: "steady", attempt: 1, kind: :update}] = parked.pending_writes
      assert field_value(parked, "steady_out") == :unwritten

      assert [detach_event] = Enum.filter(park.events, &(&1.type == :node_detached))
      assert detach_event.node_id == "waits"
      assert detach_event.task_id == task_id

      assert detach_event.payload == %{
               "attempt" => 1,
               "deadline_at" => "2026-07-30T12:00:00.050000Z"
             }

      assert park.metadata["park_reason"] == "awaiting_detached"
      assert park.metadata["wake_disposition"] == "at"

      assert Enum.map(park.metadata["node_attempts"], &{&1["node_id"], &1["outcome"]}) ==
               [{"waits", "detached"}, {"steady", "pending_update"}]

      assert :ok = Docket.Run.validate_durable(parked)
    end

    test "a missing detach policy inherits the runtime default deadline" do
      clock = fn -> ~U[2026-07-30 12:00:00.000000Z] end
      graph = parallel_detach_graph(detach: nil)

      {initialized, parked, _checkpoints} =
        park_inline(graph, clock: clock, detach_deadline_ms: 120_000, context: %{notify: self()})

      task_id = "#{initialized.id}:0:waits"
      assert parked.active_tasks[task_id].deadline_at == ~U[2026-07-30 12:02:00.000000Z]
    end

    test "detach_deadline_ms must be a positive finite integer" do
      for bad <- [0, -1, :infinity, "soon"] do
        assert_raise ArgumentError, ~r/:detach_deadline_ms/, fn ->
          Docket.Test.run_inline(parallel_detach_graph(), %{}, detach_deadline_ms: bad)
        end
      end
    end

    test "a durable :detached task cannot exist without its deadline timer" do
      graph = parallel_detach_graph()
      {initialized, parked, _} = park_inline(graph, context: %{notify: self()})
      task_id = "#{initialized.id}:0:waits"

      no_deadline =
        update_in(parked.active_tasks[task_id], &%{&1 | deadline_at: nil})

      assert {:error, %Docket.Error{type: :invalid_run}} =
               Docket.Run.validate_durable(no_deadline)

      retry_timer =
        put_in(parked.timers[task_id], %TimerState{kind: :retry, fires_at: DateTime.utc_now()})

      assert {:error, %Docket.Error{type: :invalid_run}} =
               Docket.Run.validate_durable(retry_timer)
    end
  end

  describe "post-commit workers" do
    defp worker_graph do
      Graph.new!(id: "detach-worker")
      |> Graph.put_field!("waits_out", schema: Docket.Schema.string())
      |> Graph.put_node!("waits",
        implementation: Nodes.DetachesWithWorker,
        policies: %{"detach" => %{"deadline_ms" => 60_000}}
      )
      |> Graph.put_edge!("edge_start_waits", from: "$start", to: "waits")
      |> Graph.put_edge!("edge_waits_finish", from: "waits", to: "$finish")
      |> Graph.put_output!("waits_out", [])
    end

    test "the worker starts only after the detach park commits" do
      graph = worker_graph()
      rtg = compile!(graph)

      {:ok, initialized, _} =
        Docket.Test.run_inline(graph, %{}, max_steps: 0, context: %{notify: self()})

      assert {:ok, moment} =
               Docket.Runtime.Loop.propose_advance(rtg, initialized, context: %{notify: self()})

      assert moment.checkpoint_type == :detach_scheduled
      assert [%{ref: %Docket.Detached{} = ref, worker: worker}] = moment.detached_workers
      assert ref.task_id == "#{initialized.id}:0:waits"
      assert ref.attempt == 1
      assert ref.idempotency_key == "#{ref.task_id}:1"
      assert is_function(worker, 1)

      # The node executed but its worker did not: proposing commits nothing
      # and starts nothing.
      assert_received {:detaching, _context}
      refute_received {:worker_ran, _}

      # The lifecycle starts the worker only after the commit succeeds,
      # handing it the identity of the durably parked task.
      supervisor = start_supervised!(Task.Supervisor)
      assert :ok = Docket.Lifecycle.after_commit(moment, task_supervisor: supervisor)
      assert_receive {:worker_ran, ^ref}
    end

    test "an invalid worker fails classification permanently" do
      graph =
        Graph.new!(id: "detach-bad-worker")
        |> Graph.put_node!("waits", implementation: Nodes.BadDetachWorker)
        |> Graph.put_edge!("edge_start_waits", from: "$start", to: "waits")
        |> Graph.put_edge!("edge_waits_finish", from: "waits", to: "$finish")

      assert {:ok, run, _} = Docket.Test.run_inline(graph, %{})
      assert run.status == :failed
      assert run.failure.details["errors"]["waits"] =~ "invalid_detach_worker"
    end
  end

  describe "late results" do
    test "a late result applies exactly once and stale or duplicate results change nothing" do
      graph = parallel_detach_graph()
      opts = [graph: graph, context: %{notify: self()}]

      {initialized, parked, _} = park_inline(graph, context: %{notify: self()})
      task_id = "#{initialized.id}:0:waits"

      # AC3: the completion commits :detach_resolved, then the barrier
      # absorbs both results in one step.
      assert {:ok, done, checkpoints} =
               Docket.Test.complete_detached_inline(
                 parked,
                 task_id,
                 1,
                 {:ok, %{"waits_out" => "late done"}},
                 opts
               )

      assert done.status == :done
      assert done.output == %{"waits_out" => "late done", "steady_out" => "steady done"}
      assert checkpoint_types(checkpoints) == [:detach_resolved, :step_committed, :run_completed]

      step = Enum.find(checkpoints, &(&1.type == :step_committed))

      completed =
        for event <- step.events, event.type == :node_completed do
          {event.node_id, event.payload["attempt"]}
        end

      assert completed == [{"steady", 1}, {"waits", 1}]

      # The node body executed exactly once; the barrier applied the late
      # result rather than re-dispatching.
      assert_received {:detaching, _context}
      refute_received {:detaching, _}
      assert_received {:executed, "steady", 1}
      refute_received {:executed, "steady", _}

      # Duplicate and superseded completions are no-ops.
      assert {:ok, unchanged, []} =
               Docket.Test.complete_detached_inline(
                 done,
                 task_id,
                 1,
                 {:ok, %{"waits_out" => "duplicate"}},
                 opts
               )

      assert unchanged == done
    end

    test "a completion before the detach park commits is retryable, not a silent no-op" do
      graph = parallel_detach_graph()
      opts = [graph: graph, context: %{notify: self()}]

      {:ok, initialized, _} =
        Docket.Test.run_inline(graph, %{}, max_steps: 0, context: %{notify: self()})

      task_id = "#{initialized.id}:0:waits"

      assert {:error, %Docket.Error{type: :detach_pending}, []} =
               Docket.Test.complete_detached_inline(
                 initialized,
                 task_id,
                 1,
                 {:ok, %{"waits_out" => "too early"}},
                 opts
               )
    end

    test "a duplicate completion on a still-parked superstep stays a silent no-op" do
      graph = parallel_detach_graph()
      rtg = compile!(graph)

      {initialized, parked, _} = park_inline(graph, context: %{notify: self()})
      task_id = "#{initialized.id}:0:waits"
      now = ~U[2026-07-31 12:00:00.000000Z]

      assert {:ok, moment} =
               Docket.Runtime.RunMutation.complete_detached(
                 rtg,
                 parked,
                 task_id,
                 1,
                 {:ok, %{"waits_out" => "first"}},
                 now
               )

      # The applied result sits in pending_writes at the same step; a
      # duplicate is stale (not premature) and changes nothing.
      applied = moment.run

      assert {:unchanged, ^applied} =
               Docket.Runtime.RunMutation.complete_detached(
                 rtg,
                 applied,
                 task_id,
                 1,
                 {:ok, %{"waits_out" => "second"}},
                 now
               )
    end

    test "a completion at the wrong attempt is a no-op" do
      graph = parallel_detach_graph()
      opts = [graph: graph, context: %{notify: self()}]

      {initialized, parked, _} = park_inline(graph, context: %{notify: self()})
      task_id = "#{initialized.id}:0:waits"

      assert {:ok, unchanged, []} =
               Docket.Test.complete_detached_inline(
                 parked,
                 task_id,
                 2,
                 {:ok, %{"waits_out" => "wrong attempt"}},
                 opts
               )

      assert unchanged == parked
    end

    test "an invalid late result is an API error, not a run failure" do
      graph = parallel_detach_graph()
      opts = [graph: graph, context: %{notify: self()}]

      {initialized, parked, _} = park_inline(graph, context: %{notify: self()})
      task_id = "#{initialized.id}:0:waits"

      assert {:error, %Docket.Error{type: :invalid_input}, []} =
               Docket.Test.complete_detached_inline(
                 parked,
                 task_id,
                 1,
                 {:ok, %{"ghost" => "x"}},
                 opts
               )

      assert :ok = Docket.Run.validate_durable(parked)
    end

    test "a reported failure expires the deadline and settles per policy" do
      graph = parallel_detach_graph()
      opts = [graph: graph, context: %{notify: self()}]

      {initialized, parked, _} = park_inline(graph, context: %{notify: self()})
      task_id = "#{initialized.id}:0:waits"

      assert {:ok, failed, checkpoints} =
               Docket.Test.complete_detached_inline(
                 parked,
                 task_id,
                 1,
                 {:error, "worker exploded"},
                 opts
               )

      # max_attempts is 1: the expired attempt has no retry budget, so the
      # run fails carrying the worker's reason.
      assert failed.status == :failed
      assert List.first(checkpoint_types(checkpoints)) == :detach_resolved
      assert List.last(checkpoint_types(checkpoints)) == :run_failed
      assert failed.failure.details["errors"]["waits"] =~ "worker exploded"
    end
  end

  describe "deadline expiry" do
    test "expiry reschedules the attempt through the normal retry budget" do
      test_pid = self()

      sleeper = fn ms ->
        send(test_pid, {:slept, ms})
        :ok
      end

      graph = parallel_detach_graph(detach: %{"deadline_ms" => 25}, max_attempts: 2)

      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(graph, %{}, sleeper: sleeper, context: %{notify: self()})

      # AC4/AC5: nothing ever completed the detached work; recovery is the
      # durable deadline alone. Attempt 1 detaches, expires, reschedules as
      # attempt 2; attempt 2 detaches and its expiry exhausts the budget.
      assert run.status == :failed
      assert run.failure.details["errors"]["waits"] =~ "detach_deadline_expired"

      assert checkpoint_types(checkpoints) ==
               [
                 :run_initialized,
                 :detach_scheduled,
                 :retry_scheduled,
                 :detach_scheduled,
                 :run_failed
               ]

      # Detach wait, zero-backoff retry park, second detach wait.
      assert_received {:slept, 25}
      assert_received {:slept, 0}
      assert_received {:slept, 25}
      refute_received {:slept, _}

      retry = Enum.find(checkpoints, &(&1.type == :retry_scheduled))

      assert [failed_event] = Enum.filter(retry.events, &(&1.type == :node_failed))
      assert failed_event.payload["attempt"] == 1
      assert failed_event.payload["permanent"] == false
      assert failed_event.payload["reason"] =~ "detach_deadline_expired"

      [%TaskState{attempt: 2, status: :retry_scheduled, failures: [%{attempt: 1}]}] =
        Map.values(retry.run.active_tasks)

      # The sibling's parked result survived both parks without re-execution.
      assert_received {:executed, "steady", 1}
      refute_received {:executed, "steady", _}
    end

    test "an on_deadline fail policy fails the run at the first expiry" do
      graph =
        parallel_detach_graph(
          detach: %{"deadline_ms" => 10, "on_deadline" => "fail"},
          max_attempts: 3
        )

      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(graph, %{}, context: %{notify: self()})

      assert run.status == :failed
      assert run.active_tasks == %{}
      assert run.pending_writes == []
      assert run.timers == %{}

      assert checkpoint_types(checkpoints) ==
               [:run_initialized, :detach_scheduled, :run_failed]
    end
  end

  describe "mixed and failing supersteps" do
    test "a retrying sibling and a detached task park together at the earliest deadline" do
      clock = fn -> ~U[2026-07-30 12:00:00.000000Z] end

      graph =
        Graph.new!(id: "mixed-park")
        |> Graph.put_field!("flaky_out", schema: Docket.Schema.string())
        |> Graph.put_field!("waits_out", schema: Docket.Schema.string())
        |> Graph.put_node!("flaky",
          implementation: Nodes.NotifyingFlaky,
          config: %{failures: 1.0, field: "flaky_out", value: "flaky done"},
          policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 10}}
        )
        |> Graph.put_node!("waits",
          implementation: Nodes.Detaches,
          config: %{token: "mixed"},
          policies: %{"detach" => %{"deadline_ms" => 50}}
        )
        |> Graph.put_edge!("edge_start_flaky", from: "$start", to: "flaky")
        |> Graph.put_edge!("edge_start_waits", from: "$start", to: "waits")
        |> Graph.put_edge!("edge_flaky_finish", from: "flaky", to: "$finish")
        |> Graph.put_edge!("edge_waits_finish", from: "waits", to: "$finish")

      opts = [graph: graph, clock: clock, context: %{notify: self()}]

      {:ok, initialized, _} =
        Docket.Test.run_inline(graph, %{}, max_steps: 0, clock: clock, context: %{notify: self()})

      assert {:ok, parked, checkpoints} = Docket.Test.step_inline(initialized, opts)

      # One park holds both shapes: the retry attempt and the detached task,
      # waking at the earliest deadline (the retry backoff).
      assert checkpoint_types(checkpoints) == [:detach_scheduled]
      [park] = checkpoints
      assert park.metadata["park_reason"] == "awaiting_detached"

      flaky_task = "#{initialized.id}:0:flaky"
      waits_task = "#{initialized.id}:0:waits"

      assert %TaskState{status: :retry_scheduled, attempt: 2} = parked.active_tasks[flaky_task]
      assert %TaskState{status: :detached, attempt: 1} = parked.active_tasks[waits_task]
      assert %TimerState{kind: :retry} = parked.timers[flaky_task]
      assert %TimerState{kind: :detached_deadline} = parked.timers[waits_task]

      assert [failed_event] = Enum.filter(park.events, &(&1.type == :node_failed))
      assert failed_event.node_id == "flaky"
      assert [detach_event] = Enum.filter(park.events, &(&1.type == :node_detached))
      assert detach_event.node_id == "waits"

      # Completing the detached task lets the retry fire and the barrier
      # commit both results.
      assert {:ok, done, _} =
               Docket.Test.complete_detached_inline(
                 parked,
                 waits_task,
                 1,
                 {:ok, %{"waits_out" => "late done"}},
                 opts
               )

      assert done.status == :done
      assert field_value(done, "flaky_out") == "flaky done"
      assert field_value(done, "waits_out") == "late done"
    end

    test "a sibling's permanent failure orphans the detached work and its result stays a no-op" do
      graph =
        Graph.new!(id: "detach-orphaned")
        |> Graph.put_field!("waits_out", schema: Docket.Schema.string())
        |> Graph.put_node!("boom", implementation: Nodes.Raises)
        |> Graph.put_node!("waits",
          implementation: Nodes.Detaches,
          config: %{token: "orphaned"},
          policies: %{"detach" => %{"deadline_ms" => 60_000}}
        )
        |> Graph.put_edge!("edge_start_boom", from: "$start", to: "boom")
        |> Graph.put_edge!("edge_start_waits", from: "$start", to: "waits")
        |> Graph.put_edge!("edge_boom_finish", from: "boom", to: "$finish")
        |> Graph.put_edge!("edge_waits_finish", from: "waits", to: "$finish")

      opts = [graph: graph, context: %{notify: self()}]

      assert {:ok, failed, _} = Docket.Test.run_inline(graph, %{}, context: %{notify: self()})
      assert failed.status == :failed
      assert failed.active_tasks == %{}
      assert failed.timers == %{}

      task_id = "#{failed.id}:0:waits"

      assert {:ok, unchanged, []} =
               Docket.Test.complete_detached_inline(
                 failed,
                 task_id,
                 1,
                 {:ok, %{"waits_out" => "too late"}},
                 opts
               )

      assert unchanged == failed
    end
  end
end
