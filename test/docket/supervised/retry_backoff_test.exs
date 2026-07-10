defmodule Docket.Supervised.RetryBackoffTest do
  use Docket.Test.Case, async: true

  alias Docket.Checkpoint
  alias Docket.Test.Checkpoint.Recording

  @runtime Module.concat(__MODULE__, Runtime)

  setup do
    start_supervised!({Docket.Runtime.Supervisor, name: @runtime, checkpoint: Recording})
    :ok
  end

  test "retry backoff parks the run without blocking the runtime process" do
    graph =
      Graph.new!(id: "supervised-retry")
      |> Graph.put_field!("out", schema: Docket.Schema.string())
      |> Graph.put_node!("flaky",
        implementation: Nodes.FlakyThenSucceeds,
        config: %{failures: 1.0, field: "out", value: "done"},
        policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 300}}
      )
      |> Graph.put_edge!("edge_start_flaky", from: "$start", to: "flaky")
      |> Graph.put_edge!("edge_flaky_finish", from: "flaky", to: "$finish")
      |> Graph.put_output!("out", [])

    assert {:ok, run} = Docket.run(@runtime, graph, %{}, context: %{notify: self()})

    # The park commits durably (sync) before any waiting happens.
    assert_receive {:checkpoint, %Checkpoint{type: :retry_scheduled} = park}, 500
    assert park.run.status == :running
    assert park.run.step == 0
    assert map_size(park.run.active_tasks) == 1

    # During backoff the Runtime is parked, not blocked: it still serves
    # reads of the committed parked run.
    assert {:ok, live} = Docket.get_run(@runtime, run.id)
    assert live.status == :running
    assert map_size(live.active_tasks) == 1

    # The scheduled wake dispatches the persisted attempt and completes.
    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = final}, 1_500
    assert final.run.output == %{"out" => "done"}
    assert final.run.active_tasks == %{}

    assert_receive {:checkpoint, %Checkpoint{type: :step_committed} = step}
    assert Enum.any?(step.events, &(&1.type == :node_completed and &1.payload["attempt"] == 2))
  end
end
