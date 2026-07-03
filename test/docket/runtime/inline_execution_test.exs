defmodule Docket.Runtime.InlineExecutionTest do
  use Docket.Test.Case, async: true

  describe "sequential activation" do
    test "a two-node chain runs in two supersteps" do
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.simple_edge(), %{"topic" => "graphs"})

      assert run.status == :done
      assert run.step == 2

      assert checkpoint_types(checkpoints) ==
               [:run_initialized, :step_committed, :step_committed, :run_completed]

      [_init, step_one, step_two, _completed] = checkpoints
      assert Enum.any?(step_one.events, &(&1.type == :node_completed and &1.node_id == "writer"))

      assert Enum.any?(
               step_two.events,
               &(&1.type == :node_completed and &1.node_id == "reviewer")
             )
    end

    test "declared outputs whose source was never written project as nil" do
      assert {:ok, run, _} = Docket.Test.run_inline(Graphs.simple_edge(), %{"topic" => "graphs"})

      assert run.output == %{"draft" => nil}
    end
  end

  describe "fan-out and fan-in" do
    test "fan-out activates both targets in one superstep" do
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.fanout(), %{"value" => "x"})

      assert run.status == :done
      assert run.step == 2

      [_init, _source_step, fanout_step, _completed] = checkpoints

      completed =
        for event <- fanout_step.events, event.type == :node_completed, do: event.node_id

      assert completed == ["left", "right"]
    end

    test "a multi-source edge waits for all sources before activating the join" do
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.multi_source_edge(), %{"value" => "x"})

      assert run.status == :done
      assert run.step == 3

      [_init, _s1, s2, s3, _completed] = checkpoints

      assert Enum.any?(
               s2.events,
               &(&1.type == :edge_triggered and &1.payload["edge_id"] == "edge_combine_ready")
             )

      assert Enum.any?(s3.events, &(&1.type == :node_completed and &1.node_id == "combine"))

      # The fired barrier resets its seen set.
      assert run.channels["edge:edge_combine_ready"].barrier_seen == []
    end

    test "barrier completions are sticky across supersteps" do
      # "a" completes in superstep 1, "b" only in superstep 2 (extra hop);
      # the join must remember a's completion across the barrier.
      graph =
        Graph.new!(id: "sticky-barrier")
        |> Graph.put_node!("a", implementation: Nodes.Echo)
        |> Graph.put_node!("b0", implementation: Nodes.Echo)
        |> Graph.put_node!("b", implementation: Nodes.Echo)
        |> Graph.put_node!("join", implementation: Nodes.Echo)
        |> Graph.put_edge!("edge_start_a", from: "$start", to: "a")
        |> Graph.put_edge!("edge_start_b0", from: "$start", to: "b0")
        |> Graph.put_edge!("edge_b0_b", from: "b0", to: "b")
        |> Graph.put_edge!("edge_join", from: ["a", "b"], to: "join")
        |> Graph.put_edge!("edge_join_finish", from: "join", to: "$finish")

      {:ok, run, _} = Docket.Test.run_inline(graph, %{}, max_steps: 1)
      assert run.channels["edge:edge_join"].barrier_seen == ["a"]

      {:ok, run, checkpoints} = Docket.Test.step_inline(run, graph: graph)
      assert run.channels["edge:edge_join"].barrier_seen == []
      assert Enum.any?(hd(checkpoints).events, &(&1.payload["edge_id"] == "edge_join"))

      {:ok, run, checkpoints} = Docket.Test.step_inline(run, graph: graph)

      assert Enum.any?(
               hd(checkpoints).events,
               &(&1.type == :node_completed and &1.node_id == "join")
             )

      assert {:ok, %{status: :done}, _} = Docket.Test.step_inline(run, graph: graph)
    end
  end

  describe "barrier visibility" do
    test "writes from one node are invisible to other nodes in the same superstep" do
      assert {:ok, run, _} = Docket.Test.run_inline(Graphs.same_step_isolation(), %{})

      assert field_value(run, "x") == "new"
      assert field_value(run, "y") == "old"
    end

    test "same-step writes to a last_value field resolve to the last writer in sorted node order" do
      assert {:ok, run, _} = Docket.Test.run_inline(Graphs.write_conflict(), %{})

      assert run.status == :done
      assert run.output == %{"out" => "from_b"}
    end

    test "field change tracking is write-based" do
      # The writer commits the same value the field already holds; the write
      # still bumps the version and marks the field changed.
      graph = Graphs.same_step_isolation()

      {:ok, run, _} = Docket.Test.run_inline(graph, %{}, max_steps: 1)
      assert run.channels["state:x"].version == 1
      assert MapSet.member?(run.changed_channels, "state:x")
    end
  end

  describe "guarded edges" do
    test "guards choose the premium path from committed input state" do
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.guarded_edge(), %{
                 "user" => %{"premium_user" => true}
               })

      assert run.status == :done
      completed = for cp <- checkpoints, e <- cp.events, e.type == :node_completed, do: e.node_id
      assert "premium_step" in completed
      refute "standard_step" in completed
    end

    test "guards choose the standard path when the predicate is false" do
      assert {:ok, _run, checkpoints} =
               Docket.Test.run_inline(Graphs.guarded_edge(), %{
                 "user" => %{"premium_user" => false}
               })

      completed = for cp <- checkpoints, e <- cp.events, e.type == :node_completed, do: e.node_id
      assert "standard_step" in completed
      refute "premium_step" in completed
    end

    test "missing path segments make guards false instead of raising" do
      # premium guard reads user.premium_user; an empty user map matches
      # neither equals(true) nor... not(equals(true)) is true, so the
      # standard path runs.
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.guarded_edge(), %{"user" => %{}})

      assert run.status == :done
      completed = for cp <- checkpoints, e <- cp.events, e.type == :node_completed, do: e.node_id
      assert "standard_step" in completed
    end
  end

  describe "cycles" do
    test "a guarded cycle increments to its limit and terminates" do
      assert {:ok, run, _checkpoints} = Docket.Test.run_inline(Graphs.cycle_counter(), %{})

      assert run.status == :done
      assert field_value(run, "count") == 10.0
    end

    test "exceeding max_supersteps fails the run with a typed limit failure" do
      graph = Graph.policy!(Graphs.cycle_counter(), "max_supersteps", 3)

      assert {:ok, run, checkpoints} = Docket.Test.run_inline(graph, %{})

      assert run.status == :failed
      assert run.step == 3
      assert List.last(checkpoint_types(checkpoints)) == :run_failed

      run_failed = List.last(checkpoints)
      assert Enum.any?(run_failed.events, &(&1.payload["reason"] == "max_supersteps_exceeded"))
    end
  end

  describe "output projection and state" do
    test "committed state survives on the run document" do
      assert {:ok, run, _} = Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "vv"})

      assert run.channels["input:value"].value == "vv"
      assert run.channels["input:value"].version == 1
      assert field_value(run, "result") == "vv"
    end

    test "atom content in node writes is coerced to durable form at the barrier" do
      graph =
        Graph.new!(id: "atom-writes")
        |> Graph.put_field!("out", schema: Docket.Schema.map())
        |> Graph.put_node!("writer", implementation: Nodes.AtomWriter)
        |> Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
        |> Graph.put_edge!("edge_writer_finish", from: "writer", to: "$finish")

      assert {:ok, run, _} = Docket.Test.run_inline(graph, %{})

      assert field_value(run, "out") == %{"status" => "ok"}
    end
  end
end
