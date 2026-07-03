defmodule Docket.Supervised.RuntimeLifecycleTest do
  use Docket.Test.Case, async: true

  @moduletag :capture_log

  alias Docket.Checkpoint
  alias Docket.Test.Checkpoint.Recording

  @runtime Module.concat(__MODULE__, Runtime)

  setup do
    start_supervised!({Docket.Runtime.Supervisor, name: @runtime, checkpoint: Recording})
    :ok
  end

  test "supervised execution matches the inline checkpoint sequence" do
    # Forcing :step_committed to :sync makes arrival order deterministic
    # across the sync/async boundary.
    assert {:ok, _run} =
             Docket.run(@runtime, Graphs.simple_edge(), %{"topic" => "supervision"},
               context: %{notify: self()},
               checkpoint_overrides: %{step_committed: :sync}
             )

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = completed}

    types = Enum.map(drain_checkpoints() ++ [completed], & &1.type)
    assert types == [:run_initialized, :step_committed, :step_committed, :run_completed]
  end

  test "a permanently failing superstep commits the run as failed" do
    assert {:ok, _run} =
             Docket.run(@runtime, Graphs.parallel_failure(), %{}, context: %{notify: self()})

    assert_receive {:checkpoint, %Checkpoint{type: :run_failed} = failed}
    assert failed.run.status == :failed
    assert failed.run.step == 0
  end

  test "interrupts park the run and resolve through the public API" do
    assert {:ok, run} =
             Docket.run(@runtime, Graphs.interrupt_review(), %{},
               run_id: "review-run",
               context: %{notify: self()}
             )

    assert_receive {:checkpoint, %Checkpoint{type: :interrupt_requested} = requested}
    assert requested.run.status == :waiting

    [interrupt_id] = Map.keys(requested.run.interrupts)

    assert {:ok, resolved} =
             Docket.resolve_interrupt(@runtime, run.id, interrupt_id, "ship it")

    assert resolved.status == :running
    assert_receive {:checkpoint, %Checkpoint{type: :interrupt_resolved}}
    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = final}
    assert field_value(final.run, "applied") == "ship it"
  end

  test "unknown interrupts are typed errors" do
    assert {:ok, run} =
             Docket.run(@runtime, Graphs.interrupt_review(), %{}, context: %{notify: self()})

    assert_receive {:checkpoint, %Checkpoint{type: :interrupt_requested}}

    assert {:error, %Docket.Error{type: :not_found}} =
             Docket.resolve_interrupt(@runtime, run.id, "no-such-interrupt", "value")
  end

  test "async checkpoint delivery failures are observable and never block the run" do
    assert {:ok, _run} =
             Docket.run(@runtime, Graphs.simple_edge(), %{"topic" => "resilience"},
               context: %{notify: self(), fail_on: [:step_committed]}
             )

    # Every async step checkpoint is rejected, yet the run finishes.
    assert_receive {:checkpoint, %Checkpoint{type: :run_completed}}
    assert_receive {:checkpoint_rejected, %Checkpoint{type: :step_committed}}
  end

  test "stale and unknown messages are ignored" do
    assert {:ok, run} =
             Docket.run(@runtime, Graphs.interrupt_review(), %{}, context: %{notify: self()})

    assert_receive {:checkpoint, %Checkpoint{type: :interrupt_requested} = requested}

    {:ok, pid} = Docket.Runtime.Registry.whereis(@runtime, run.id)
    send(pid, {make_ref(), {:ok, %{"applied" => "stale task completion"}}})
    send(pid, :unexpected)

    [interrupt_id] = Map.keys(requested.run.interrupts)
    assert {:ok, _} = Docket.resolve_interrupt(@runtime, run.id, interrupt_id, "real value")

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = final}
    assert field_value(final.run, "applied") == "real value"
  end

  defp drain_checkpoints(acc \\ []) do
    receive do
      {:checkpoint, checkpoint} -> drain_checkpoints([checkpoint | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
