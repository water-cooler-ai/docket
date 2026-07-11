defmodule Docket.Supervised.CrashRecoveryTest do
  use Docket.Test.Case, async: true

  alias Docket.Checkpoint
  alias Docket.Test.Checkpoint.EtsSink

  @runtime Module.concat(__MODULE__, Runtime)

  setup do
    start_supervised!({Docket.Runtime.Supervisor, name: @runtime, checkpoint: EtsSink})
    table = EtsSink.new_table()

    on_exit(fn ->
      if :ets.info(table) != :undefined do
        :ets.delete(table)
      end
    end)

    {:ok, table: table}
  end

  defp context(table, extra \\ %{}) do
    Map.merge(%{checkpoint_table: table, notify: self()}, extra)
  end

  defp kill_runtime(run_id) do
    {:ok, pid} = Docket.Runtime.Registry.whereis(@runtime, run_id)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
    pid
  end

  test "a killed waiting run resumes from the latest ETS checkpoint", %{table: table} do
    graph = Graphs.interrupt_review()

    assert {:ok, _run} =
             Docket.run(@runtime, graph, %{}, run_id: "crashed-run", context: context(table))

    assert_receive {:checkpoint, %Checkpoint{type: :interrupt_requested}}

    kill_runtime("crashed-run")
    assert {:error, %Docket.Error{type: :not_found}} = Docket.get_run(@runtime, "crashed-run")

    # The host recovers from durable state only: the latest accepted
    # checkpoint's run document plus the original graph.
    saved = EtsSink.latest_run(table, "crashed-run")
    assert saved.status == :waiting

    drain_messages()
    assert {:ok, resumed} = Docket.resume(@runtime, graph, saved, context: context(table))
    assert resumed.status == :waiting

    # Resume passes through the same durable barrier: the host upserts the
    # run again by ID.
    assert_receive {:checkpoint, %Checkpoint{type: :run_initialized}}

    [interrupt_id] = Map.keys(saved.interrupts)

    assert {:ok, _run} =
             Docket.resolve_interrupt(@runtime, "crashed-run", interrupt_id, "recovered")

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed}}
    assert EtsSink.latest_run(table, "crashed-run").status == :done
  end

  test "resume accepts the checkpointed durable run state", %{table: table} do
    graph = Graphs.interrupt_review()

    assert {:ok, _run} =
             Docket.run(@runtime, graph, %{}, run_id: "round-trip", context: context(table))

    assert_receive {:checkpoint, %Checkpoint{type: :interrupt_requested}}
    kill_runtime("round-trip")

    saved = EtsSink.latest_run(table, "round-trip")

    assert {:ok, _resumed} = Docket.resume(@runtime, graph, saved, context: context(table))

    [interrupt_id] = Map.keys(saved.interrupts)

    assert {:ok, _run} =
             Docket.resolve_interrupt(@runtime, "round-trip", interrupt_id, "still works")

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = final}
    assert field_value(final.run, "applied") == "still works"
  end

  test "a crash mid-superstep re-executes the uncommitted superstep with the same attempt",
       %{table: table} do
    graph = Graphs.blocking()

    assert {:ok, _run} =
             Docket.run(@runtime, graph, %{},
               run_id: "mid-flight",
               context: context(table, %{coordinator: self()})
             )

    # The Local executor runs the node inside the Runtime process, so the
    # kill lands while the superstep is in flight and uncommitted.
    assert_receive {:blocked, _node, "blocker", 1}
    kill_runtime("mid-flight")

    saved = EtsSink.latest_run(table, "mid-flight")
    assert saved.status == :running
    assert saved.step == 0

    assert {:ok, _resumed} =
             Docket.resume(@runtime, graph, saved,
               context: context(table, %{coordinator: self()})
             )

    # The re-executed superstep plans from committed state only, so the
    # attempt counter (and idempotency key) is unchanged.
    assert_receive {:blocked, node, "blocker", 1}
    send(node, :release)

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = final}
    assert final.run.output == %{"out" => "released"}
  end

  test "terminal runs never restart on resume", %{table: table} do
    graph = Graphs.minimal_linear()

    # Forcing :step_committed sync keeps every first-run checkpoint ahead of
    # the drain below, so the refute after resume observes an empty mailbox.
    assert {:ok, _run} =
             Docket.run(@runtime, graph, %{"value" => "hi"},
               run_id: "finished",
               context: context(table),
               checkpoint_overrides: %{step_committed: :sync}
             )

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed}}
    done = EtsSink.latest_run(table, "finished")
    assert done.status == :done

    drain_messages()
    assert {:ok, ^done} = Docket.resume(@runtime, graph, done, context: context(table))

    refute_receive {:checkpoint, _}, 50
    assert {:error, %Docket.Error{type: :not_found}} = Docket.get_run(@runtime, "finished")
  end

  defp drain_messages do
    receive do
      _message -> drain_messages()
    after
      0 -> :ok
    end
  end

  test "resume rejects a graph whose hash does not match", %{table: table} do
    graph = Graphs.interrupt_review()

    assert {:ok, _run} =
             Docket.run(@runtime, graph, %{}, run_id: "mismatched", context: context(table))

    assert_receive {:checkpoint, %Checkpoint{type: :interrupt_requested}}
    kill_runtime("mismatched")

    saved = EtsSink.latest_run(table, "mismatched")
    edited = Graph.metadata!(graph, "note", "content changed after publish")

    assert {:error, %Docket.Error{type: :graph_mismatch}} =
             Docket.resume(@runtime, edited, saved, context: context(table))
  end
end
