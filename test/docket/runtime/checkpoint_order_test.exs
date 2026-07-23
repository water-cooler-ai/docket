defmodule Docket.Runtime.CheckpointOrderTest do
  use Docket.Test.Case, async: true

  describe "checkpoint ordering and delivery" do
    test ":run_initialized is emitted before any node execution" do
      {:ok, _run, checkpoints} =
        Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"})

      [first | _] = checkpoints
      assert first.type == :run_initialized
      assert first.run.status == :running
      assert first.run.step == 0
      assert event_types([first]) |> hd() == :run_initialized
    end

    test "returns checkpoints in committed transition order" do
      {:ok, _run, checkpoints} =
        Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"})

      assert checkpoint_types(checkpoints) == [
               :run_initialized,
               :step_committed,
               :run_completed
             ]
    end

    test ":run_completed is emitted only after terminal detection" do
      {:ok, _run, checkpoints} =
        Docket.Test.run_inline(Graphs.simple_edge(), %{"topic" => "t"})

      assert List.last(checkpoint_types(checkpoints)) == :run_completed
      completed = List.last(checkpoints)
      assert completed.run.status == :done
      assert completed.run.finished_at != nil
    end
  end

  describe "idempotency key stability" do
    test "re-planning a superstep after a failed checkpoint produces byte-identical keys" do
      graph = Graphs.minimal_linear()
      runtime_graph = compile!(graph)

      {:ok, run, _} = Docket.Test.run_inline(runtime_graph, %{"value" => "x"}, max_steps: 0)

      opts = []

      config = Docket.Runtime.Config.resolve(opts)
      {:execute, node_ids} = Docket.Runtime.Algorithm.plan(runtime_graph, run, config)

      {:ok, first} =
        Docket.Runtime.Algorithm.prepare_activations(runtime_graph, run, node_ids, config)

      {:ok, second} =
        Docket.Runtime.Algorithm.prepare_activations(runtime_graph, run, node_ids, config)

      assert Enum.map(first, & &1.task_id) == Enum.map(second, & &1.task_id)
      assert Enum.map(first, & &1.idempotency_key) == Enum.map(second, & &1.idempotency_key)
      assert Enum.map(first, & &1.input_hash) == Enum.map(second, & &1.input_hash)
      assert Enum.map(first, & &1.attempt) == Enum.map(second, & &1.attempt)
    end
  end
end
