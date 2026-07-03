defmodule Docket.Runtime.ResumeTest do
  use Docket.Test.Case, async: true

  describe "resume_inline/3" do
    test "resumes a mid-flight run from its persisted document" do
      graph = Graphs.simple_edge()

      {:ok, run, _} = Docket.Test.run_inline(graph, %{"topic" => "graphs"}, max_steps: 1)
      assert run.status == :running
      assert run.step == 1

      restored = Docket.Run.from_map!(Docket.Run.to_map(run))

      assert {:ok, resumed, checkpoints} = Docket.Test.resume_inline(graph, restored)

      assert resumed.status == :done
      assert resumed.step == 2

      assert checkpoint_types(checkpoints) ==
               [:run_initialized, :step_committed, :run_completed]

      [init | _] = checkpoints
      assert Enum.any?(init.events, &(&1.payload["resumed"] == true))
    end

    test "resuming the run persisted by a checkpoint re-executes the uncommitted superstep" do
      graph = Graphs.simple_edge()

      {:ok, _run, checkpoints} = Docket.Test.run_inline(graph, %{"topic" => "graphs"})

      # Resume from the first step checkpoint: superstep 2 was never durable.
      step_one = Enum.find(checkpoints, &(&1.type == :step_committed))

      assert {:ok, resumed, _} = Docket.Test.resume_inline(graph, step_one.run)
      assert resumed.status == :done
      assert resumed.step == 2
    end

    test "a mismatched graph is rejected" do
      {:ok, run, _} =
        Docket.Test.run_inline(Graphs.simple_edge(), %{"topic" => "t"}, max_steps: 1)

      assert {:error, %Docket.Error{type: :graph_mismatch}, []} =
               Docket.Test.resume_inline(Graphs.minimal_linear(), run)

      changed =
        Docket.Graph.put_field!(Graphs.simple_edge(), "extra", schema: Docket.Schema.string())

      assert {:error, %Docket.Error{type: :graph_mismatch}, []} =
               Docket.Test.resume_inline(changed, run)
    end

    test "a terminal run is returned unchanged without restarting execution" do
      graph = Graphs.minimal_linear()
      {:ok, run, _} = Docket.Test.run_inline(graph, %{"value" => "x"})
      assert run.status == :done

      assert {:ok, ^run, []} = Docket.Test.resume_inline(graph, run)
    end

    test "resume does not treat process startup as graph progress" do
      graph = Graphs.simple_edge()
      {:ok, run, _} = Docket.Test.run_inline(graph, %{"topic" => "t"}, max_steps: 1)

      {:ok, resumed, _} = Docket.Test.resume_inline(graph, run, max_steps: 0)

      # Only the initialization checkpoint ran; graph execution state is
      # untouched.
      assert resumed.status == run.status
      assert resumed.step == run.step
      assert resumed.channels == run.channels
      assert resumed.changed_channels == run.changed_channels
    end
  end
end
