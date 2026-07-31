defmodule Docket.Runtime.RunMutationTest do
  use Docket.Test.Case, async: true

  alias Docket.Run.{TaskState, TimerState}
  alias Docket.Runtime.{Moment, RunMutation}

  @now ~U[2026-07-10 18:00:00.000000Z]

  defp waiting_run do
    {:ok, run, _checkpoints} = Docket.Test.run_inline(Graphs.interrupt_review(), %{})
    {compile!(Graphs.interrupt_review()), run}
  end

  defp with_active_tasks(run, timers_by_task_id) do
    active_tasks =
      Map.new(timers_by_task_id, fn {task_id, _timer} ->
        {task_id, %TaskState{task_id: task_id, node_id: task_id, step: run.step, attempt: 2}}
      end)

    timers =
      for {task_id, %DateTime{} = fires_at} <- timers_by_task_id, into: %{} do
        {task_id, %TimerState{kind: :retry, fires_at: fires_at}}
      end

    %{run | active_tasks: active_tasks, timers: timers}
  end

  describe "resolve_interrupt/5" do
    test "deterministically produces one immediate-wake moment" do
      {rtg, run} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      assert {:ok, %Moment{} = first} =
               RunMutation.resolve_interrupt(rtg, run, interrupt_id, "approved", @now)

      assert RunMutation.resolve_interrupt(rtg, run, interrupt_id, "approved", @now) ==
               {:ok, first}

      assert first.checkpoint_type == :interrupt_resolved
      assert first.disposition == {:park, :immediate, :interrupt_resolved}
      assert first.proposed_at == @now
      assert first.run.status == :running
      assert first.run.updated_at == @now
      assert first.run.interrupts[interrupt_id].status == :resolved
      assert first.run.interrupts[interrupt_id].resolved_at == @now

      assert Enum.map(first.events, & &1.type) == [
               :interrupt_resolved,
               :channel_updated,
               :checkpoint_committed
             ]

      assert Enum.all?(first.events, &(&1.timestamp == @now))
      assert first.run.checkpoint_seq == run.checkpoint_seq + 1
      assert first.run.event_seq == run.event_seq + 3
      assert first.checkpoint_metadata["wake_disposition"] == "immediate"
    end

    test "returns distinct unknown and repeated-resolution errors" do
      {rtg, run} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      assert {:error, %Docket.Error{type: :not_found}} =
               RunMutation.resolve_interrupt(rtg, run, "unknown", "value", @now)

      resolved = put_in(run.interrupts[interrupt_id].status, :resolved)

      assert {:error, %Docket.Error{type: :already_resolved}} =
               RunMutation.resolve_interrupt(rtg, resolved, interrupt_id, "value", @now)
    end

    test "checks terminal status before looking up even a still-open interrupt" do
      {rtg, run} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      for status <- [:done, :failed, :cancelled] do
        terminal = %{run | status: status}

        assert {:error, %Docket.Error{type: :inactive_run}} =
                 RunMutation.resolve_interrupt(rtg, terminal, interrupt_id, "value", @now)
      end
    end

    test "rejects the private created sentinel before interrupt lookup" do
      {rtg, run} = waiting_run()
      created = %{run | status: :created, interrupts: %{}}

      assert {:error, %Docket.Error{type: :invalid_run}} =
               RunMutation.resolve_interrupt(rtg, created, "unknown", "value", @now)
    end

    test "parks at the earliest deadline when every active attempt has a future retry timer" do
      {rtg, run} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)
      earliest = DateTime.add(@now, 30, :second)

      parked =
        with_active_tasks(run, %{
          "near" => earliest,
          "far" => DateTime.add(@now, 90, :second)
        })

      assert {:ok, %Moment{} = moment} =
               RunMutation.resolve_interrupt(rtg, parked, interrupt_id, "approved", @now)

      assert moment.disposition == {:park, {:at, earliest}, :interrupt_resolved}
      assert moment.checkpoint_metadata["wake_disposition"] == "at"
    end

    test "proposes an immediate wake when any active attempt's deadline is due" do
      {rtg, run} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      parked =
        with_active_tasks(run, %{
          "due" => @now,
          "far" => DateTime.add(@now, 90, :second)
        })

      assert {:ok, %Moment{} = moment} =
               RunMutation.resolve_interrupt(rtg, parked, interrupt_id, "approved", @now)

      assert moment.disposition == {:park, :immediate, :interrupt_resolved}
      assert moment.checkpoint_metadata["wake_disposition"] == "immediate"
    end

    test "proposes an immediate wake when an active attempt has no timer" do
      {rtg, run} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      untimed = %TaskState{task_id: "untimed", node_id: "untimed", step: run.step, attempt: 2}

      parked = with_active_tasks(run, %{"far" => DateTime.add(@now, 90, :second)})
      parked = put_in(parked.active_tasks["untimed"], untimed)

      assert {:ok, %Moment{} = moment} =
               RunMutation.resolve_interrupt(rtg, parked, interrupt_id, "approved", @now)

      assert moment.disposition == {:park, :immediate, :interrupt_resolved}
      assert moment.checkpoint_metadata["wake_disposition"] == "immediate"
    end

    test "retains interrupt schema and durable-value validation" do
      {rtg, run} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      assert {:error, %Docket.Error{type: :invalid_input}} =
               RunMutation.resolve_interrupt(rtg, run, interrupt_id, 42, @now)

      assert {:error, %Docket.Error{type: :invalid_input}} =
               RunMutation.resolve_interrupt(rtg, run, interrupt_id, self(), @now)
    end
  end

  describe "complete_detached/6" do
    defp detach_graph do
      Graph.new!(id: "mutation-detach")
      |> Graph.put_field!("out", schema: Docket.Schema.string())
      |> Graph.put_node!("waits",
        implementation: Nodes.Detaches,
        config: %{token: "t"},
        policies: %{"detach" => %{"deadline_ms" => 60_000}}
      )
      |> Graph.put_edge!("edge_start_waits", from: "$start", to: "waits")
      |> Graph.put_edge!("edge_waits_finish", from: "waits", to: "$finish")
      |> Graph.put_output!("out", [])
    end

    defp detached_run do
      graph = detach_graph()
      sleeper = fn _ms -> :ok end
      {:ok, initialized, _} = Docket.Test.run_inline(graph, %{}, max_steps: 0, sleeper: sleeper)

      {:ok, parked, _} =
        Docket.Test.step_inline(initialized, graph: graph, sleeper: sleeper)

      {compile!(graph), parked, "#{initialized.id}:0:waits"}
    end

    test "deterministically applies a success result as one pending write" do
      {rtg, run, task_id} = detached_run()
      result = {:ok, %{"out" => "late"}}

      assert {:ok, %Moment{} = first} =
               RunMutation.complete_detached(rtg, run, task_id, 1, result, @now)

      assert RunMutation.complete_detached(rtg, run, task_id, 1, result, @now) == {:ok, first}

      assert first.checkpoint_type == :detach_resolved
      assert first.disposition == {:park, :immediate, :detach_resolved}
      assert first.run.status == :running
      assert first.run.active_tasks == %{}
      assert first.run.timers == %{}

      assert [%Docket.Run.PendingWrite{task_id: ^task_id, attempt: 1, kind: :update}] =
               first.run.pending_writes

      # No runtime event: the applied attempt's :node_completed is emitted by
      # the barrier that absorbs the write.
      assert Enum.map(first.events, & &1.type) == [:checkpoint_committed]
      assert first.run.checkpoint_seq == run.checkpoint_seq + 1
      assert :ok = Docket.Run.validate_durable(first.run)
    end

    test "a failure result expires the deadline in place" do
      {rtg, run, task_id} = detached_run()

      assert {:ok, %Moment{} = moment} =
               RunMutation.complete_detached(rtg, run, task_id, 1, {:error, :boom}, @now)

      assert moment.checkpoint_type == :detach_resolved
      assert moment.disposition == {:park, :immediate, :detach_resolved}

      task = moment.run.active_tasks[task_id]
      assert task.status == :detached
      assert task.attempt == 1
      assert task.deadline_at == @now
      assert task.metadata["detach_error"] == ":boom"
      assert moment.run.timers[task_id].fires_at == @now
      assert :ok = Docket.Run.validate_durable(moment.run)
    end

    test "stale, superseded, and terminal completions are no-ops" do
      {rtg, run, task_id} = detached_run()

      # Wrong attempt and unknown task change nothing.
      assert {:unchanged, ^run} =
               RunMutation.complete_detached(rtg, run, task_id, 2, {:ok, %{"out" => "x"}}, @now)

      assert {:unchanged, ^run} =
               RunMutation.complete_detached(rtg, run, "ghost", 1, {:ok, %{"out" => "x"}}, @now)

      # A cancelled run is a silent no-op, not an error.
      assert {:ok, %Moment{run: cancelled}} = RunMutation.cancel_run(run, @now)

      assert {:unchanged, ^cancelled} =
               RunMutation.complete_detached(
                 rtg,
                 cancelled,
                 task_id,
                 1,
                 {:ok, %{"out" => "x"}},
                 @now
               )
    end

    test "invalid result values are API errors" do
      {rtg, run, task_id} = detached_run()

      assert {:error, %Docket.Error{type: :invalid_input}} =
               RunMutation.complete_detached(rtg, run, task_id, 1, {:ok, %{"ghost" => "x"}}, @now)

      assert {:error, %Docket.Error{type: :invalid_input}} =
               RunMutation.complete_detached(rtg, run, task_id, 1, {:ok, "not a map"}, @now)

      assert {:error, %Docket.Error{type: :invalid_input}} =
               RunMutation.complete_detached(rtg, run, task_id, 1, :done, @now)
    end
  end

  describe "cancel_run/2" do
    test "deterministically cancels running and waiting runs with a terminal moment" do
      {_rtg, waiting} = waiting_run()

      for run <- [waiting, %{waiting | status: :running}] do
        assert {:ok, %Moment{} = first} = RunMutation.cancel_run(run, @now)
        assert RunMutation.cancel_run(run, @now) == {:ok, first}

        assert first.checkpoint_type == :run_cancelled
        assert first.disposition == {:park, :terminal, :run_cancelled}
        assert first.proposed_at == @now
        assert first.run.status == :cancelled
        assert first.run.finished_at == @now
        assert first.run.updated_at == @now
        assert Enum.map(first.events, & &1.type) == [:run_cancelled, :checkpoint_committed]
        assert Enum.all?(first.events, &(&1.timestamp == @now))
        assert first.checkpoint_metadata["wake_disposition"] == "terminal"
      end
    end

    test "absorbs a parked active superstep" do
      {_rtg, run} = waiting_run()
      task = %TaskState{task_id: "task", node_id: "node", step: run.step, attempt: 2}
      timer = %TimerState{kind: :retry, fires_at: DateTime.add(@now, 60, :second)}

      parked = %{
        run
        | status: :running,
          active_tasks: %{"task" => task},
          pending_writes: [%Docket.Run.PendingWrite{task_id: "sibling", node_id: "other"}],
          timers: %{"task" => timer}
      }

      assert {:ok, moment} = RunMutation.cancel_run(parked, @now)
      assert moment.run.active_tasks == %{}
      assert moment.run.pending_writes == []
      assert moment.run.timers == %{}
    end

    test "returns an already-cancelled run byte-for-byte without a new moment" do
      {_rtg, run} = waiting_run()
      cancelled = %{run | status: :cancelled, finished_at: @now, updated_at: @now}

      assert {:unchanged, ^cancelled} =
               RunMutation.cancel_run(cancelled, DateTime.add(@now, 1, :hour))
    end

    test "rejects done, failed, and the private created sentinel" do
      {_rtg, run} = waiting_run()

      for status <- [:done, :failed] do
        assert {:error, %Docket.Error{type: :inactive_run}} =
                 RunMutation.cancel_run(%{run | status: status}, @now)
      end

      assert {:error, %Docket.Error{type: :invalid_run}} =
               RunMutation.cancel_run(%{run | status: :created}, @now)
    end
  end

  test "the new cancellation fact is registered as checkpoint vocabulary" do
    assert :run_cancelled in Docket.Event.types()
    assert :run_cancelled in Docket.Checkpoint.types()
  end
end
