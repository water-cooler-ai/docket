defmodule Docket.Runtime.DispatcherConcurrencyTest do
  use Docket.Test.Case, async: true

  defmodule BlockingNode do
    @moduledoc false
    @behaviour Docket.Node

    @impl true
    def config_schema, do: Docket.Schema.object(%{})

    @impl true
    def call(state, _config, context) do
      send(context.application.test_pid, {:node_started, context.node_id, self(), state})

      receive do
        :release -> {:ok, %{}}
      end
    end
  end

  test "all nodes in a superstep execute concurrently against the same snapshot" do
    graph =
      Docket.Graph.new!(id: "concurrent-dispatch")
      |> Docket.Graph.put_input!("value", schema: Docket.Schema.string(required: true))
      |> Docket.Graph.put_node!("a", implementation: BlockingNode)
      |> Docket.Graph.put_node!("b", implementation: BlockingNode)
      |> Docket.Graph.put_node!("c", implementation: BlockingNode)
      |> Docket.Graph.put_edge!("start_a", from: "$start", to: "a")
      |> Docket.Graph.put_edge!("start_b", from: "$start", to: "b")
      |> Docket.Graph.put_edge!("start_c", from: "$start", to: "c")
      |> Docket.Graph.put_edge!("finish", from: ["a", "b", "c"], to: "$finish")

    test_pid = self()

    run =
      Task.async(fn ->
        Docket.Test.run_inline(graph, %{"value" => "shared"}, context: %{test_pid: test_pid})
      end)

    started =
      for _ <- 1..3 do
        assert_receive {:node_started, node_id, pid, snapshot}, 500
        {node_id, pid, snapshot}
      end

    assert started |> Enum.map(&elem(&1, 0)) |> Enum.sort() == ["a", "b", "c"]

    assert Enum.all?(started, fn {_node_id, _pid, snapshot} ->
             snapshot == %{"value" => "shared"}
           end)

    Enum.each(started, fn {_node_id, pid, _snapshot} -> send(pid, :release) end)

    assert {:ok, completed, _checkpoints} = Task.await(run, 500)
    assert completed.status == :done
    assert completed.step == 1
  end
end
