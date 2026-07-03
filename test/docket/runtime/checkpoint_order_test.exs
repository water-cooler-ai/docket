defmodule Docket.Runtime.CheckpointOrderTest do
  use Docket.Test.Case, async: true

  alias Docket.Test.Checkpoint.{FailOn, MemorySink}

  describe "checkpoint ordering and delivery" do
    test ":run_initialized is emitted before any node execution" do
      {:ok, sink} = MemorySink.start_link()

      {:ok, _run, _} =
        Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"},
          checkpoint: MemorySink,
          context: %{memory_sink: sink}
        )

      [first | _] = MemorySink.checkpoints(sink)
      assert first.type == :run_initialized
      assert first.run.status == :running
      assert first.run.step == 0
      assert event_types([first]) |> hd() == :run_initialized
    end

    test "step checkpoints are async and lifecycle checkpoints are sync by default" do
      {:ok, _run, checkpoints} =
        Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"})

      deliveries = Enum.map(checkpoints, &{&1.type, &1.delivery})

      assert deliveries == [
               {:run_initialized, :sync},
               {:step_committed, :async},
               {:run_completed, :sync}
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

  describe "sync checkpoint failure" do
    test "a failed :run_initialized checkpoint means execution never started" do
      {:ok, sink} = MemorySink.start_link()

      assert {:error, error, []} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"},
                 checkpoint: FailOn,
                 context: %{fail_on: [:run_initialized], memory_sink: sink}
               )

      assert %Docket.Error{type: :checkpoint_failed, phase: :run_initialized} = error
      assert error.reason == {:forced_failure, :run_initialized}
      assert MemorySink.checkpoints(sink) == []
    end

    test "a failed :run_completed checkpoint keeps earlier accepted checkpoints" do
      {:ok, sink} = MemorySink.start_link()

      assert {:error, error, checkpoints} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"},
                 checkpoint: FailOn,
                 context: %{fail_on: [:run_completed], memory_sink: sink}
               )

      assert %Docket.Error{type: :checkpoint_failed, phase: :run_completed} = error
      assert checkpoint_types(checkpoints) == [:run_initialized, :step_committed]

      # The durable state remains the last accepted checkpoint.
      latest = List.last(MemorySink.checkpoints(sink))
      assert latest.type == :step_committed
      assert latest.run.status == :running
    end

    test "a raising sync checkpoint handler is a checkpoint failure, not a crash" do
      defmodule RaisingSink do
        @behaviour Docket.Checkpoint
        def handle(_checkpoint, _context), do: raise("sink exploded")
      end

      assert {:error, %Docket.Error{type: :checkpoint_failed, reason: {:raised, _}}, []} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"},
                 checkpoint: RaisingSink
               )
    end
  end

  describe "async checkpoint failure" do
    test "a failed async :step_committed does not block the run" do
      {:ok, sink} = MemorySink.start_link()

      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"},
                 checkpoint: FailOn,
                 context: %{fail_on: [:step_committed], memory_sink: sink}
               )

      assert run.status == :done
      assert checkpoint_types(checkpoints) == [:run_initialized, :run_completed]

      assert MemorySink.checkpoints(sink) |> checkpoint_types() == [
               :run_initialized,
               :run_completed
             ]
    end

    test "checkpoint_overrides can force :step_committed to sync" do
      assert {:error, %Docket.Error{type: :checkpoint_failed, phase: :step_committed},
              checkpoints} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"},
                 checkpoint: FailOn,
                 checkpoint_overrides: %{step_committed: :sync},
                 context: %{fail_on: [:step_committed]}
               )

      assert checkpoint_types(checkpoints) == [:run_initialized]
    end
  end

  describe "idempotency key stability" do
    test "re-planning a superstep after a failed checkpoint produces byte-identical keys" do
      graph = Graphs.minimal_linear()
      runtime_graph = compile!(graph)

      {:ok, run, _} = Docket.Test.run_inline(runtime_graph, %{"value" => "x"}, max_steps: 0)

      opts = [checkpoint: Docket.Test.Checkpoint.Accept]

      {:execute, _run, first} = Docket.Runtime.Loop.plan(runtime_graph, run, opts)
      {:execute, _run, second} = Docket.Runtime.Loop.plan(runtime_graph, run, opts)

      assert Enum.map(first, & &1.task_id) == Enum.map(second, & &1.task_id)
      assert Enum.map(first, & &1.idempotency_key) == Enum.map(second, & &1.idempotency_key)
      assert Enum.map(first, & &1.input_hash) == Enum.map(second, & &1.input_hash)
      assert Enum.map(first, & &1.attempt) == Enum.map(second, & &1.attempt)
    end
  end
end
