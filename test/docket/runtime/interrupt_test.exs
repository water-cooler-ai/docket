defmodule Docket.Runtime.InterruptTest do
  use Docket.Test.Case, async: true

  alias Docket.Run.InterruptState

  defp waiting_run do
    {:ok, run, checkpoints} = Docket.Test.run_inline(Graphs.interrupt_review(), %{})
    {run, checkpoints}
  end

  describe "interrupt request" do
    test "an interrupting node pauses the run as :waiting" do
      {run, checkpoints} = waiting_run()

      assert run.status == :waiting
      assert checkpoint_types(checkpoints) == [:run_initialized, :interrupt_requested]

      assert [{interrupt_id, %InterruptState{} = interrupt}] = Map.to_list(run.interrupts)
      assert interrupt.status == :open
      assert interrupt.node_id == "gate"
      assert interrupt.resume_channel == "decision"
      assert interrupt.id == interrupt_id
      assert MapSet.member?(run.pending_nodes, "gate")
    end

    test "the interrupt barrier commits :waiting durably in the same checkpoint" do
      {_run, checkpoints} = waiting_run()

      interrupt_checkpoint = List.last(checkpoints)
      assert interrupt_checkpoint.type == :interrupt_requested
      assert interrupt_checkpoint.delivery == :sync
      assert interrupt_checkpoint.run.status == :waiting
      assert Enum.any?(interrupt_checkpoint.events, &(&1.type == :interrupt_requested))
    end
  end

  describe "interrupt resolution" do
    test "resolution writes the resume channel and re-executes the node" do
      {run, _} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      assert {:ok, run, checkpoints} =
               Docket.Test.resolve_interrupt_inline(run, interrupt_id, "approved",
                 graph: Graphs.interrupt_review()
               )

      assert run.status == :done
      assert field_value(run, "decision") == "approved"
      assert field_value(run, "applied") == "approved"
      assert run.interrupts[interrupt_id].status == :resolved
      refute MapSet.member?(run.pending_nodes, "gate")

      assert checkpoint_types(checkpoints) ==
               [:interrupt_resolved, :step_committed, :run_completed]
    end

    test "resolution values are validated against the interrupt schema" do
      {run, _} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      assert {:error, %Docket.Error{type: :invalid_input}, []} =
               Docket.Test.resolve_interrupt_inline(run, interrupt_id, 42,
                 graph: Graphs.interrupt_review()
               )
    end

    test "unknown and already-resolved interrupts return :not_found" do
      {run, _} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      assert {:error, %Docket.Error{type: :not_found}, []} =
               Docket.Test.resolve_interrupt_inline(run, "nope", "x",
                 graph: Graphs.interrupt_review()
               )

      {:ok, resolved_run, _} =
        Docket.Test.resolve_interrupt_inline(run, interrupt_id, "approved",
          graph: Graphs.interrupt_review()
        )

      assert {:error, %Docket.Error{type: :inactive_run}, []} =
               Docket.Test.resolve_interrupt_inline(resolved_run, interrupt_id, "again",
                 graph: Graphs.interrupt_review()
               )
    end

    test "a waiting durable run resumes" do
      {run, _} = waiting_run()
      [interrupt_id] = Map.keys(run.interrupts)

      assert {:ok, resumed, _} =
               Docket.Test.resume_inline(Graphs.interrupt_review(), run)

      # Resume of a waiting run re-emits :run_initialized and waits again.
      assert resumed.status == :waiting

      assert {:ok, done, _} =
               Docket.Test.resolve_interrupt_inline(resumed, interrupt_id, "yes",
                 graph: Graphs.interrupt_review()
               )

      assert done.status == :done
      assert field_value(done, "applied") == "yes"
    end
  end
end
