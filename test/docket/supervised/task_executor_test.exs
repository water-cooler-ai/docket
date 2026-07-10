defmodule Docket.Supervised.TaskExecutorTest do
  use Docket.Test.Case, async: true

  # Nodes crashing inside supervised tasks legitimately produce task crash
  # reports; keep them out of the test output.
  @moduletag :capture_log

  alias Docket.Checkpoint
  alias Docket.Test.Checkpoint.Recording

  @runtime Module.concat(__MODULE__, Runtime)

  setup do
    start_supervised!(
      {Docket.Runtime.Supervisor,
       name: @runtime, checkpoint: Recording, executor: Docket.Executor.Task}
    )

    :ok
  end

  test "nodes execute in supervised tasks under the runtime instance" do
    assert {:ok, _run} =
             Docket.run(@runtime, Graphs.minimal_linear(), %{"value" => "task"},
               context: %{notify: self()}
             )

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = final}
    assert final.run.output == %{"result" => "task"}
  end

  test "timeouts become node attempt failures and retry policy dispatches again" do
    graph =
      Graphs.blocking(%{
        "timeout_ms" => 50,
        "retry" => %{"max_attempts" => 2, "backoff_ms" => 0}
      })

    assert {:ok, _run} =
             Docket.run(@runtime, graph, %{}, context: %{notify: self(), coordinator: self()})

    # Never release attempt 1: it times out; attempt 2 is a fresh dispatch.
    assert_receive {:blocked, _attempt1, "blocker", 1}
    assert_receive {:blocked, attempt2, "blocker", 2}, 500
    send(attempt2, :release)

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = final}, 500
    assert final.run.output == %{"out" => "released"}

    # The retried timeout committed a retry park: the non-permanent failure
    # event rides the sync :retry_scheduled checkpoint, whose run stays
    # :running without advancing the graph step.
    assert_receive {:checkpoint, %Checkpoint{type: :retry_scheduled} = park}
    timeout_failures = for event <- park.events, event.type == :node_failed, do: event.payload
    assert [%{"attempt" => 1, "permanent" => false, "reason" => ":timeout"}] = timeout_failures
    assert park.run.status == :running
    assert park.run.step == 0

    assert_receive {:checkpoint, %Checkpoint{type: :step_committed} = step}
    refute Enum.any?(step.events, &(&1.type == :node_failed))
  end

  test "exhausted timeouts fail the run permanently" do
    graph = Graphs.blocking(%{"timeout_ms" => 25})

    assert {:ok, _run} =
             Docket.run(@runtime, graph, %{}, context: %{notify: self(), coordinator: self()})

    assert_receive {:blocked, _pid, "blocker", 1}
    assert_receive {:checkpoint, %Checkpoint{type: :run_failed} = failed}, 500

    assert failed.run.status == :failed

    assert Enum.any?(
             failed.events,
             &(&1.type == :node_failed and &1.payload["reason"] == ":timeout" and
                 &1.payload["permanent"])
           )
  end

  test "node crashes are isolated in the task and the tree stays up" do
    graph =
      Graph.new!(id: "crashing")
      |> Graph.put_field!("out", schema: Docket.Schema.string())
      |> Graph.put_node!("boom", implementation: Nodes.Raises)
      |> Graph.put_edge!("edge_start_boom", from: "$start", to: "boom")
      |> Graph.put_edge!("edge_boom_finish", from: "boom", to: "$finish")

    assert {:ok, _run} = Docket.run(@runtime, graph, %{}, context: %{notify: self()})

    assert_receive {:checkpoint, %Checkpoint{type: :run_failed} = failed}
    assert failed.run.status == :failed

    # The instance survives the node crash and accepts new runs.
    assert {:ok, _run} =
             Docket.run(@runtime, Graphs.minimal_linear(), %{"value" => "after crash"},
               context: %{notify: self()}
             )

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed}}
  end

  test "Executor.Task runs inline without a task supervisor" do
    assert {:ok, run, _checkpoints} =
             Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "monitored"},
               executor: Docket.Executor.Task
             )

    assert run.status == :done
    assert run.output == %{"result" => "monitored"}
  end

  test "Executor.Task enforces timeouts inline without a task supervisor" do
    graph = Graphs.blocking(%{"timeout_ms" => 25})

    assert {:ok, run, _checkpoints} =
             Docket.Test.run_inline(graph, %{},
               executor: Docket.Executor.Task,
               context: %{coordinator: self()}
             )

    assert run.status == :failed
    assert_received {:blocked, _pid, "blocker", 1}
  end
end
