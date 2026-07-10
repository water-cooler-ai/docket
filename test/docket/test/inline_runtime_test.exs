defmodule Docket.Test.InlineRuntimeTest do
  use Docket.Test.Case, async: true

  alias Docket.Test.Checkpoint.MemorySink

  describe "run_inline/3" do
    test "runs minimal_linear to completion in the calling process" do
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "hello"})

      assert run.status == :done
      assert run.step == 1
      assert run.output == %{"result" => "hello"}
      assert checkpoint_types(checkpoints) == [:run_initialized, :step_committed, :run_completed]
    end

    test "accepts a precompiled runtime graph" do
      runtime_graph = compile!(Graphs.minimal_linear())

      assert {:ok, run, _checkpoints} =
               Docket.Test.run_inline(runtime_graph, %{"value" => "hi"})

      assert run.status == :done
      assert run.output == %{"result" => "hi"}
    end

    test "the checkpoint sink receives the same checkpoints the helper returns" do
      {:ok, sink} = MemorySink.start_link()

      assert {:ok, _run, checkpoints} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"},
                 checkpoint: MemorySink,
                 context: %{memory_sink: sink}
               )

      assert MemorySink.checkpoints(sink) == checkpoints
    end

    test "every checkpoint carries a restorable run and monotonic seqs" do
      assert {:ok, _run, checkpoints} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"})

      assert Enum.map(checkpoints, & &1.seq) == [1, 2, 3]

      event_seqs = Enum.flat_map(checkpoints, fn cp -> Enum.map(cp.events, & &1.seq) end)
      assert event_seqs == Enum.to_list(1..length(event_seqs))

      for checkpoint <- checkpoints do
        assert %Docket.Run{} = checkpoint.run
        assert checkpoint.run.checkpoint_seq == checkpoint.seq

        assert [%Docket.Event{type: :checkpoint_committed} = fact] =
                 Enum.filter(checkpoint.events, &(&1.type == :checkpoint_committed))

        assert fact == List.last(checkpoint.events)
        assert fact.metadata == checkpoint.metadata
      end
    end

    test "run IDs and metadata are honored" do
      assert {:ok, run, _} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"},
                 run_id: "run_custom",
                 metadata: %{"tenant" => "acme"}
               )

      assert run.id == "run_custom"
      assert run.metadata == %{"tenant" => "acme"}
    end

    test "max_steps stops driving after the configured superstep count" do
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.simple_edge(), %{"topic" => "graphs"}, max_steps: 1)

      assert run.status == :running
      assert run.step == 1
      assert checkpoint_types(checkpoints) == [:run_initialized, :step_committed]
    end

    test "an invalid graph returns a typed error with diagnostics" do
      assert {:error, %Docket.Error{type: :invalid_graph} = error, []} =
               Docket.Test.run_inline(Graphs.invalid_unknown_target(), %{"value" => "x"})

      assert [%Docket.Graph.Diagnostic{} | _] = error.details.diagnostics
    end

    test "missing required input fails before any checkpoint" do
      {:ok, sink} = MemorySink.start_link()

      assert {:error, %Docket.Error{type: :invalid_input} = error, []} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{},
                 checkpoint: MemorySink,
                 context: %{memory_sink: sink}
               )

      assert error.details.reasons == ["required input \"value\" is missing"]
      assert MemorySink.checkpoints(sink) == []
    end

    test "unknown and non-durable inputs fail with typed errors" do
      assert {:error, %Docket.Error{type: :invalid_input}, []} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{
                 "value" => "x",
                 "bogus" => 1
               })

      assert {:error, %Docket.Error{type: :invalid_input}, []} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => self()})
    end

    test "input values are validated against input schemas" do
      assert {:error, %Docket.Error{type: :invalid_input}, []} =
               Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => 42})
    end
  end

  describe "step_inline/2" do
    test "advances exactly one committed superstep" do
      graph = Graphs.simple_edge()

      {:ok, run, _} = Docket.Test.run_inline(graph, %{"topic" => "graphs"}, max_steps: 0)
      assert run.status == :running
      assert run.step == 0

      assert {:ok, run, checkpoints} = Docket.Test.step_inline(run, graph: graph)
      assert run.step == 1
      assert checkpoint_types(checkpoints) == [:step_committed]

      assert {:ok, run, _} = Docket.Test.step_inline(run, graph: graph)
      assert run.step == 2

      assert {:ok, run, checkpoints} = Docket.Test.step_inline(run, graph: graph)
      assert run.status == :done
      assert checkpoint_types(checkpoints) == [:run_completed]

      assert {:ok, ^run, []} = Docket.Test.step_inline(run, graph: graph)
    end

    test "requires the graph in opts" do
      {:ok, run, _} = Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"})

      assert {:error, %Docket.Error{type: :invalid_graph}, []} = Docket.Test.step_inline(run)
    end
  end
end
