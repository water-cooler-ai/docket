defmodule Docket.Runtime.AttemptTimeoutTest do
  use Docket.Test.Case, async: true

  defmodule PassthroughExecutor do
    @behaviour Docket.Executor
    def execute(_task, node, state, config, context, _opts),
      do: apply(node.module, node.function, [state, config, context])
  end

  defmodule BlockingNode do
    @behaviour Docket.Node
    def config_schema, do: Docket.Schema.object(%{})

    def call(_state, _config, context) do
      send(context.application.coordinator, {:blocked, self(), context.node_id, context.attempt})
      receive do: (:release -> {:ok, %{}})
    end
  end

  defmodule OkNode do
    @behaviour Docket.Node
    def config_schema, do: Docket.Schema.object(%{})
    def call(_state, _config, _context), do: {:ok, %{}}
  end

  defmodule KillNode do
    @behaviour Docket.Node
    def config_schema, do: Docket.Schema.object(%{})
    def call(_state, _config, _context), do: Process.exit(self(), :kill)
  end

  test "a missing timeout inherits the finite runtime maximum for every executor" do
    for executor <- [Docket.Executor.Local, PassthroughExecutor] do
      task =
        Task.async(fn ->
          Docket.Test.run_inline(blocking_graph(), %{},
            executor: executor,
            max_attempt_elapsed_ms: 20,
            context: %{coordinator: self()}
          )
        end)

      assert {:ok, run, _checkpoints} = Task.await(task, 500)
      assert run.status == :failed
      assert run.failure.details["errors"]["slow"] =~ "timeout"
    end
  end

  test "an explicit timeout wins when it is at or below the host maximum" do
    assert {:ok, %{status: :failed} = run, _} =
             Docket.Test.run_inline(blocking_graph(15), %{},
               max_attempt_elapsed_ms: 100,
               context: %{coordinator: self()}
             )

    assert run.failure.details["errors"]["slow"] =~ "timeout"
  end

  test "a timeout above the host maximum rejects the full graph before any node executes" do
    graph =
      Docket.Graph.new!(id: "incompatible-timeout")
      |> Docket.Graph.put_node!("first", implementation: BlockingNode)
      |> Docket.Graph.put_node!("dormant",
        implementation: BlockingNode,
        policies: %{"timeout_ms" => 101}
      )
      |> Docket.Graph.put_edge!("start", from: "$start", to: "first")
      |> Docket.Graph.put_edge!("dormant_start", from: "$start", to: "dormant")
      |> Docket.Graph.put_edge!("finish", from: ["first", "dormant"], to: "$finish")

    assert {:error, %Docket.Error{type: :incompatible_execution_policy}, []} =
             Docket.Test.run_inline(graph, %{},
               max_attempt_elapsed_ms: 100,
               context: %{coordinator: self()}
             )

    refute_received {:blocked, _, _, _}
  end

  test "heterogeneous sibling timeouts complete the barrier without mailbox debris" do
    graph =
      Docket.Graph.new!(id: "heterogeneous-timeouts")
      |> Docket.Graph.put_field!("fast_out", schema: Docket.Schema.string())
      |> Docket.Graph.put_field!("slow_out", schema: Docket.Schema.string())
      |> Docket.Graph.put_node!("fast",
        implementation: OkNode,
        policies: %{"timeout_ms" => 100}
      )
      |> Docket.Graph.put_node!("slow",
        implementation: BlockingNode,
        policies: %{"timeout_ms" => 15}
      )
      |> Docket.Graph.put_edge!("fast_start", from: "$start", to: "fast")
      |> Docket.Graph.put_edge!("slow_start", from: "$start", to: "slow")
      |> Docket.Graph.put_edge!("finish", from: ["fast", "slow"], to: "$finish")

    assert {:ok, %{status: :failed} = run, _} =
             Docket.Test.run_inline(graph, %{},
               max_attempt_elapsed_ms: 100,
               context: %{coordinator: self()}
             )

    assert run.failure.details["errors"]["slow"] =~ "timeout"
    refute_received {_, _, _}
    refute_received {:DOWN, _, :process, _, _}
  end

  test "an untrappable node exit is normalized without killing the caller" do
    graph =
      Docket.Graph.new!(id: "self-kill")
      |> Docket.Graph.put_node!("ok", implementation: OkNode)
      |> Docket.Graph.put_node!("kill", implementation: KillNode)
      |> Docket.Graph.put_edge!("ok_start", from: "$start", to: "ok")
      |> Docket.Graph.put_edge!("kill_start", from: "$start", to: "kill")
      |> Docket.Graph.put_edge!("finish", from: ["ok", "kill"], to: "$finish")

    assert {:ok, %{status: :failed} = run, _} =
             Docket.Test.run_inline(graph, %{}, max_attempt_elapsed_ms: 100)

    assert run.failure.details["errors"]["kill"] =~ "killed"
    refute_received {:DOWN, _, :process, _, _}
  end

  test "dispatch leaves foreign DOWN messages in the caller's mailbox" do
    {dead, dead_ref} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^dead_ref, :process, ^dead, :normal}

    # Monitoring a dead process queues a :noproc DOWN that sits in the
    # mailbox while the whole run dispatches inline in this process.
    foreign_ref = Process.monitor(dead)

    graph =
      Docket.Graph.new!(id: "foreign-down")
      |> Docket.Graph.put_node!("ok", implementation: OkNode)
      |> Docket.Graph.put_edge!("start", from: "$start", to: "ok")
      |> Docket.Graph.put_edge!("finish", from: "ok", to: "$finish")

    assert {:ok, %{status: :done}, _} = Docket.Test.run_inline(graph, %{})

    assert_received {:DOWN, ^foreign_ref, :process, ^dead, :noproc}
  end

  test "a dying dispatch caller kills its in-flight node work" do
    test_pid = self()

    runner =
      spawn(fn ->
        Docket.Test.run_inline(blocking_graph(), %{},
          max_attempt_elapsed_ms: 60_000,
          context: %{coordinator: test_pid}
        )
      end)

    assert_receive {:blocked, worker, "slow", 1}, 1_000
    worker_ref = Process.monitor(worker)

    Process.exit(runner, :kill)
    assert_receive {:DOWN, ^worker_ref, :process, ^worker, :killed}, 1_000
  end

  defp blocking_graph(timeout \\ nil) do
    policies = if timeout, do: %{"timeout_ms" => timeout}, else: %{}

    Docket.Graph.new!(id: "blocking-timeout-#{inspect(timeout)}")
    |> Docket.Graph.put_field!("out", schema: Docket.Schema.string())
    |> Docket.Graph.put_node!("slow",
      implementation: BlockingNode,
      policies: policies
    )
    |> Docket.Graph.put_edge!("start", from: "$start", to: "slow")
    |> Docket.Graph.put_edge!("finish", from: "slow", to: "$finish")
  end
end
