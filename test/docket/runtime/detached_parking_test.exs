defmodule Docket.Runtime.DetachedParkingTest do
  use Docket.Test.Case, async: true

  alias Docket.Run
  alias Docket.Run.{PendingWrite, TaskState, TimerState}
  alias Docket.Runtime.Loop

  defmodule SpyExecutor do
    @behaviour Docket.Executor

    @impl true
    def execute(task, node, state, config, context, opts) do
      send(context.application.notify, {:dispatched, task.node_id})
      Docket.Executor.Local.execute(task, node, state, config, context, opts)
    end
  end

  defp detached_graph(opts) do
    policies =
      case Keyword.get(opts, :detach) do
        nil -> %{}
        detach -> %{"detach" => detach}
      end

    Graph.new!(id: Keyword.get(opts, :id, "detached-park"))
    |> Graph.put_field!("summary", schema: Docket.Schema.string())
    |> Graph.put_node!("summarize",
      implementation: :detached,
      config: %{"endpoint" => "summarize"},
      policies: policies
    )
    |> Graph.put_edge!("edge_start_summarize", from: "$start", to: "summarize")
    |> Graph.put_edge!("edge_summarize_finish", from: "summarize", to: "$finish")
    |> Graph.put_output!("summary", [])
  end

  # start fans out to the detached node and a module sibling in one superstep.
  defp mixed_graph(sibling_implementation, sibling_opts) do
    detached_graph(id: "detached-mixed")
    |> Graph.put_field!("sibling_out", schema: Docket.Schema.string())
    |> Graph.put_node!(
      "sibling",
      [implementation: sibling_implementation] ++ sibling_opts
    )
    |> Graph.put_edge!("edge_start_sibling", from: "$start", to: "sibling")
    |> Graph.put_edge!("edge_sibling_finish", from: "sibling", to: "$finish")
    |> Graph.put_output!("sibling_out", [])
  end

  defp two_detached_graph(a_detach, b_detach) do
    Graph.new!(id: "detached-two")
    |> Graph.put_field!("a_out", schema: Docket.Schema.string())
    |> Graph.put_field!("b_out", schema: Docket.Schema.string())
    |> Graph.put_node!("a", implementation: :detached, policies: %{"detach" => a_detach})
    |> Graph.put_node!("b", implementation: :detached, policies: %{"detach" => b_detach})
    |> Graph.put_edge!("edge_start_a", from: "$start", to: "a")
    |> Graph.put_edge!("edge_start_b", from: "$start", to: "b")
    |> Graph.put_edge!("edge_a_finish", from: "a", to: "$finish")
    |> Graph.put_edge!("edge_b_finish", from: "b", to: "$finish")
    |> Graph.put_output!("a_out", [])
    |> Graph.put_output!("b_out", [])
  end

  defp parked_moment(rtg, opts \\ []) do
    run = Loop.build_initial_run(rtg, %{}, opts)
    {:ok, init} = Loop.propose_init(rtg, run, opts)
    {:ok, moment} = Loop.propose_advance(rtg, init.run, opts)
    moment
  end

  describe "plan-time parking" do
    test "parks the detached node pending; the dispatcher never receives it" do
      graph = mixed_graph(Nodes.NotifyingWrite, config: %{field: "sibling_out", value: "done"})

      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(graph, %{},
                 executor: SpyExecutor,
                 context: %{notify: self()}
               )

      assert run.status == :running
      assert_received {:dispatched, "sibling"}
      refute_received {:dispatched, "summarize"}

      assert [park] = Enum.filter(checkpoints, &(&1.type == :detach_scheduled))
      assert park.run.step == 0
      assert park.metadata["wake_disposition"] == "external"
      assert park.metadata["park_reason"] == "awaiting_detached"

      # The sibling's completed result parks as a pending write, invisible
      # until the barrier the detached completion will eventually reach.
      assert [%PendingWrite{node_id: "sibling", attempt: 1, kind: :update}] =
               park.run.pending_writes

      assert field_value(park.run, "sibling_out") == :unwritten

      task_id = TaskState.task_id(run.id, 0, "summarize")

      assert %TaskState{
               status: :detached_pending,
               node_id: "summarize",
               attempt: 1,
               failures: [],
               scheduled_at: %DateTime{},
               started_at: nil,
               deadline_at: nil,
               metadata: %{}
             } = park.run.active_tasks[task_id]

      # Unbounded schedule-to-start: no timer, no wake.
      assert park.run.timers == %{}

      assert [detached_event] = Enum.filter(park.events, &(&1.type == :node_detached))
      assert detached_event.node_id == "summarize"
      assert detached_event.task_id == task_id
      assert %{"attempt" => 1, "scheduled_at" => scheduled_at} = detached_event.payload
      assert detached_event.payload["deadline_at"] == nil
      assert {:ok, _, _} = DateTime.from_iso8601(scheduled_at)

      assert [%TaskState{task_id: ^task_id}] = Run.detached_tasks(run)
    end

    test "bounded schedule-to-start stamps the deadline and its timer durably" do
      graph = detached_graph(detach: %{"schedule_to_start_ms" => 30_000})

      assert {:ok, run, checkpoints} = Docket.Test.run_inline(graph, %{})

      assert run.status == :running

      assert [park] = Enum.filter(checkpoints, &(&1.type == :detach_scheduled))

      task_id = TaskState.task_id(run.id, 0, "summarize")
      task = park.run.active_tasks[task_id]
      assert task.deadline_at == DateTime.add(task.scheduled_at, 30_000, :millisecond)

      assert %TimerState{kind: :schedule_to_start, fires_at: fires_at} =
               park.run.timers[task_id]

      assert fires_at == task.deadline_at

      # The deadline is durable but never wakes the run: a wake nothing can
      # act on is claim churn, so the park is external until the expiry
      # machinery consumes these timers.
      assert park.metadata["wake_disposition"] == "external"
    end

    test "schedule-to-start deadlines never wake the run; detached parks are external" do
      bounded = %{"schedule_to_start_ms" => 30_000}
      later = %{"schedule_to_start_ms" => 90_000}

      moment = parked_moment(compile!(two_detached_graph(bounded, later)))
      assert {:park, :external, :awaiting_detached} = moment.disposition

      a = moment.run.active_tasks[TaskState.task_id(moment.run.id, 0, "a")]
      b = moment.run.active_tasks[TaskState.task_id(moment.run.id, 0, "b")]
      assert a.deadline_at == DateTime.add(a.scheduled_at, 30_000, :millisecond)
      assert b.deadline_at == DateTime.add(b.scheduled_at, 90_000, :millisecond)

      moment = parked_moment(compile!(two_detached_graph(bounded, %{})))
      assert {:park, :external, :awaiting_detached} = moment.disposition

      moment = parked_moment(compile!(two_detached_graph(%{}, %{})))
      assert {:park, :external, :awaiting_detached} = moment.disposition
    end
  end

  defp retry_and_detached_rtg do
    mixed_graph(Nodes.NotifyingFlaky,
      config: %{failures: 1.0, field: "sibling_out", value: "recovered"},
      policies: %{"retry" => %{"max_attempts" => 3, "backoff_ms" => 60_000}}
    )
    |> compile!()
  end

  describe "mixed retry and detached parks" do
    test "one commit parks the retry and the detached task; the retry timer wakes it" do
      moment = parked_moment(retry_and_detached_rtg(), context: %{notify: self()})

      assert moment.checkpoint_type == :detach_scheduled

      flaky_id = TaskState.task_id(moment.run.id, 0, "sibling")
      summarize_id = TaskState.task_id(moment.run.id, 0, "summarize")

      assert %TaskState{status: :retry_scheduled, attempt: 2} =
               moment.run.active_tasks[flaky_id]

      assert %TaskState{status: :detached_pending, attempt: 1} =
               moment.run.active_tasks[summarize_id]

      assert %TimerState{kind: :retry, fires_at: fires_at} = moment.run.timers[flaky_id]
      refute Map.has_key?(moment.run.timers, summarize_id)

      assert {:park, {:at, ^fires_at}, :awaiting_detached} = moment.disposition
    end

    test "resume dispatches the due retry and preserves the parked detached task" do
      rtg = retry_and_detached_rtg()
      parked = parked_moment(rtg, context: %{notify: self()})
      flaky_id = TaskState.task_id(parked.run.id, 0, "sibling")
      summarize_id = TaskState.task_id(parked.run.id, 0, "summarize")
      %TimerState{fires_at: due_at} = parked.run.timers[flaky_id]

      assert {:ok, resumed} =
               Loop.propose_advance(rtg, parked.run,
                 resume_floor: due_at,
                 context: %{notify: self()}
               )

      # The recovered sibling's write parks pending; only the detached task
      # remains outstanding, so the park is external and typed by it.
      assert resumed.checkpoint_type == :detach_scheduled
      assert {:park, :external, :awaiting_detached} = resumed.disposition

      assert [%PendingWrite{node_id: "sibling", attempt: 2, kind: :update}] =
               resumed.run.pending_writes

      assert [{^summarize_id, %TaskState{status: :detached_pending}}] =
               Map.to_list(resumed.run.active_tasks)

      assert resumed.run.timers == %{}
    end

    test "a permanently failing sibling absorbs the parked detached task" do
      graph =
        mixed_graph(Nodes.NotifyingFlaky,
          config: %{failures: 2.0, field: "sibling_out", value: "never"},
          policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 0}}
        )

      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(graph, %{}, context: %{notify: self()})

      assert run.status == :failed
      assert run.active_tasks == %{}
      assert run.timers == %{}
      assert Run.detached_tasks(run) == []
      assert Enum.any?(checkpoints, &(&1.type == :detach_scheduled))
      assert Enum.any?(checkpoints, &(&1.type == :run_failed))
    end
  end
end
