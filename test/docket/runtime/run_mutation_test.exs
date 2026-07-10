defmodule Docket.Runtime.RunMutationTest do
  use Docket.Test.Case, async: true

  alias Docket.Run.{TaskState, TimerState}
  alias Docket.Runtime.{Moment, RunMutation}

  @now ~U[2026-07-10 18:00:00.000000Z]

  defp waiting_run do
    {:ok, run, _checkpoints} = Docket.Test.run_inline(Graphs.interrupt_review(), %{})
    {compile!(Graphs.interrupt_review()), run}
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

    test "retains interrupt schema and durable-value validation" do
      {rtg, run} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      assert {:error, %Docket.Error{type: :invalid_input}} =
               RunMutation.resolve_interrupt(rtg, run, interrupt_id, 42, @now)

      assert {:error, %Docket.Error{type: :invalid_input}} =
               RunMutation.resolve_interrupt(rtg, run, interrupt_id, self(), @now)
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

  test "the new cancellation fact is registered as sync checkpoint vocabulary" do
    assert :run_cancelled in Docket.Event.types()
    assert :run_cancelled in Docket.Checkpoint.types()
    assert Docket.Checkpoint.delivery(:run_cancelled) == :sync
  end
end
