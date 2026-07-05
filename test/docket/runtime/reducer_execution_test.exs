defmodule Docket.Runtime.ReducerExecutionTest do
  use Docket.Test.Case, async: true

  alias Docket.{Reducer, Schema}

  # End-to-end reducer semantics: committed values fold the prior value with
  # each superstep's writes, snapshots expose accumulating zeros, write
  # validation is reducer-aware, and interrupt resolutions write through the
  # resume field's reducer.

  defp append_chain do
    Graph.new!(id: "append-chain")
    |> Graph.put_field!("messages",
      schema: Schema.list(Schema.string()),
      reducer: Reducer.append()
    )
    |> Graph.put_node!("first",
      implementation: Nodes.WriteValue,
      config: %{"field" => "messages", "value" => "one"}
    )
    |> Graph.put_node!("second",
      implementation: Nodes.WriteValue,
      config: %{"field" => "messages", "value" => ["two", "three"]}
    )
    |> Graph.put_edge!("edge_start_first", from: "$start", to: "first")
    |> Graph.put_edge!("edge_first_second", from: "first", to: "second")
    |> Graph.put_edge!("edge_second_finish", from: "second", to: "$finish")
    |> Graph.put_output!("messages", [])
  end

  test "append accumulates across supersteps; list writes concatenate" do
    assert {:ok, run, _checkpoints} = Docket.Test.run_inline(append_chain(), %{})

    assert run.status == :done
    assert run.output == %{"messages" => ["one", "two", "three"]}
  end

  test "same-step writes fold in sorted node order after the prior value" do
    graph =
      Graph.new!(id: "append-conflict")
      |> Graph.put_field!("log",
        schema: Schema.list(Schema.string()),
        default: ["seed"],
        reducer: Reducer.append()
      )
      |> Graph.put_node!("b_writer",
        implementation: Nodes.WriteValue,
        config: %{"field" => "log", "value" => "from_b"}
      )
      |> Graph.put_node!("a_writer",
        implementation: Nodes.WriteValue,
        config: %{"field" => "log", "value" => "from_a"}
      )
      |> Graph.put_edge!("edge_start_a", from: "$start", to: "a_writer")
      |> Graph.put_edge!("edge_start_b", from: "$start", to: "b_writer")
      |> Graph.put_output!("log", [])

    assert {:ok, run, _checkpoints} = Docket.Test.run_inline(graph, %{})

    assert run.output == %{"log" => ["seed", "from_a", "from_b"]}
  end

  test "accumulating fields expose their zero in node snapshots" do
    graph =
      Graph.new!(id: "zero-snapshot")
      |> Graph.put_field!("messages",
        schema: Schema.list(Schema.string()),
        reducer: Reducer.append()
      )
      |> Graph.put_field!("copy", schema: Schema.list(Schema.string()))
      |> Graph.put_node!("reader",
        implementation: Nodes.CopyInput,
        config: %{"from" => "messages", "to" => "copy"}
      )
      |> Graph.put_edge!("edge_start_reader", from: "$start", to: "reader")
      |> Graph.put_edge!("edge_reader_finish", from: "reader", to: "$finish")
      |> Graph.put_output!("copy", [])

    assert {:ok, run, _checkpoints} = Docket.Test.run_inline(graph, %{})

    assert run.output == %{"copy" => []}
  end

  test "sum accumulates numeric writes; merge folds map fragments" do
    graph =
      Graph.new!(id: "sum-merge")
      |> Graph.put_field!("total", schema: Schema.integer(), reducer: Reducer.sum())
      |> Graph.put_field!("meta", schema: Schema.map(), reducer: Reducer.merge())
      |> Graph.put_node!("a_step",
        implementation: Nodes.WriteValue,
        config: %{"field" => "total", "value" => 2}
      )
      |> Graph.put_node!("b_step",
        implementation: Nodes.WriteValue,
        config: %{"field" => "total", "value" => 3}
      )
      |> Graph.put_node!("meta_a",
        implementation: Nodes.WriteValue,
        config: %{"field" => "meta", "value" => %{"a" => 1}}
      )
      |> Graph.put_node!("meta_b",
        implementation: Nodes.WriteValue,
        config: %{"field" => "meta", "value" => %{"b" => 2}}
      )
      |> Graph.put_edge!("edge_start_a", from: "$start", to: "a_step")
      |> Graph.put_edge!("edge_start_b", from: "$start", to: "b_step")
      |> Graph.put_edge!("edge_start_ma", from: "$start", to: "meta_a")
      |> Graph.put_edge!("edge_start_mb", from: "$start", to: "meta_b")
      |> Graph.put_output!("total", [])
      |> Graph.put_output!("meta", [])

    assert {:ok, run, _checkpoints} = Docket.Test.run_inline(graph, %{})

    assert run.output == %{"total" => 5, "meta" => %{"a" => 1, "b" => 2}}
  end

  test "append writes validate against the item schema" do
    graph =
      Graph.new!(id: "append-invalid-item")
      |> Graph.put_field!("messages",
        schema: Schema.list(Schema.string()),
        reducer: Reducer.append()
      )
      |> Graph.put_node!("writer",
        implementation: Nodes.WriteValue,
        config: %{"field" => "messages", "value" => 42}
      )
      |> Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
      |> Graph.put_edge!("edge_writer_finish", from: "writer", to: "$finish")

    assert {:ok, run, _checkpoints} = Docket.Test.run_inline(graph, %{})

    assert run.status == :failed
    refute Map.has_key?(run.channels, "state:messages")
  end

  test "interrupt resolutions write through the resume field's reducer" do
    graph =
      Graph.new!(id: "append-interrupt")
      |> Graph.put_field!("answers",
        schema: Schema.list(Schema.string()),
        reducer: Reducer.append()
      )
      |> Graph.put_field!("applied", schema: Schema.list(Schema.string()))
      |> Graph.put_node!("gate",
        implementation: Nodes.InterruptWhileEmpty,
        config: %{"resume_field" => "answers", "write_field" => "applied"}
      )
      |> Graph.put_edge!("edge_start_gate", from: "$start", to: "gate")
      |> Graph.put_edge!("edge_gate_finish", from: "gate", to: "$finish")
      |> Graph.put_output!("applied", [])

    assert {:ok, run, _checkpoints} = Docket.Test.run_inline(graph, %{})
    assert run.status == :waiting

    [interrupt_id] = Map.keys(run.interrupts)

    assert {:ok, run, _checkpoints} =
             Docket.Test.resolve_interrupt_inline(run, interrupt_id, "yes", graph: graph)

    assert run.status == :done
    assert run.output == %{"applied" => ["yes"]}
    assert run.channels["state:answers"].value == ["yes"]
  end

  test "first_value keeps the first committed write" do
    graph =
      Graph.new!(id: "first-value")
      |> Graph.put_field!("winner", schema: Schema.string(), reducer: Reducer.first_value())
      |> Graph.put_node!("early",
        implementation: Nodes.WriteValue,
        config: %{"field" => "winner", "value" => "first"}
      )
      |> Graph.put_node!("late",
        implementation: Nodes.WriteValue,
        config: %{"field" => "winner", "value" => "second"}
      )
      |> Graph.put_edge!("edge_start_early", from: "$start", to: "early")
      |> Graph.put_edge!("edge_early_late", from: "early", to: "late")
      |> Graph.put_edge!("edge_late_finish", from: "late", to: "$finish")
      |> Graph.put_output!("winner", [])

    assert {:ok, run, _checkpoints} = Docket.Test.run_inline(graph, %{})

    assert run.output == %{"winner" => "first"}
  end
end
