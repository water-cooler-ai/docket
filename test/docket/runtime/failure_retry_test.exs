defmodule Docket.Runtime.FailureRetryTest do
  use Docket.Test.Case, async: true

  describe "failure policy" do
    test "a permanent node failure commits no writes from the superstep" do
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.parallel_failure(), %{})

      assert run.status == :failed
      assert run.step == 0
      assert field_value(run, "ok_out") == :unwritten
      assert checkpoint_types(checkpoints) == [:run_initialized, :run_failed]

      assert %Docket.Run.Failure{code: "node_failed", node_id: "failing_node"} = run.failure
      assert run.failure.details["nodes"] == ["failing_node"]
      assert run.failure.details["errors"]["failing_node"] =~ "always_fails"

      run_failed = List.last(checkpoints)
      assert run_failed.delivery == :sync

      failed_nodes =
        for event <- run_failed.events,
            event.type == :node_failed,
            event.payload["permanent"],
            do: event.node_id

      assert failed_nodes == ["failing_node"]
      refute Enum.any?(run_failed.events, &(&1.type == :node_completed))
    end

    test "raises and throws are normalized into node attempt failures" do
      for implementation <- [Nodes.Raises, Nodes.Throws] do
        graph =
          Graph.new!(id: "crashing")
          |> Graph.put_node!("boom", implementation: implementation)
          |> Graph.put_edge!("edge_start_boom", from: "$start", to: "boom")
          |> Graph.put_edge!("edge_boom_finish", from: "boom", to: "$finish")

        assert {:ok, run, checkpoints} = Docket.Test.run_inline(graph, %{})
        assert run.status == :failed
        assert List.last(checkpoint_types(checkpoints)) == :run_failed
      end
    end

    test "reserved return shapes fail permanently without retry" do
      for {implementation, marker} <- [
            {Nodes.Awaits, ":unsupported_await"},
            {Nodes.BadReturn, ":invalid_node_return"}
          ] do
        graph =
          Graph.new!(id: "reserved-return")
          |> Graph.put_node!("node",
            implementation: implementation,
            policies: %{"retry" => %{"max_attempts" => 3}}
          )
          |> Graph.put_edge!("edge_start_node", from: "$start", to: "node")
          |> Graph.put_edge!("edge_node_finish", from: "node", to: "$finish")

        assert {:ok, run, checkpoints} = Docket.Test.run_inline(graph, %{})
        assert run.status == :failed

        run_failed = List.last(checkpoints)
        permanent = Enum.filter(run_failed.events, &(&1.type == :node_failed))
        # Deterministic failures are not retried even with retry budget left.
        assert [event] = permanent
        assert event.payload["attempt"] == 1
        assert event.payload["reason"] =~ marker
      end
    end

    test "writes to unknown or read-only fields fail the superstep" do
      graph =
        Graph.new!(id: "bad-write")
        |> Graph.put_field!("out", schema: Docket.Schema.string())
        |> Graph.put_node!("writer",
          implementation: Nodes.WriteStatic,
          config: %{field: "nope", value: "x"}
        )
        |> Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
        |> Graph.put_edge!("edge_writer_finish", from: "writer", to: "$finish")

      assert {:ok, run, checkpoints} = Docket.Test.run_inline(graph, %{})
      assert run.status == :failed

      run_failed = List.last(checkpoints)
      assert Enum.any?(run_failed.events, &(&1.payload["reason"] =~ "unknown field"))
    end

    test "schema-invalid writes fail the superstep" do
      graph =
        Graph.new!(id: "schema-violation")
        |> Graph.put_field!("out", schema: Docket.Schema.float())
        |> Graph.put_node!("writer",
          implementation: Nodes.WriteStatic,
          config: %{field: "out", value: "not a number"}
        )
        |> Graph.put_edge!("edge_start_writer", from: "$start", to: "writer")
        |> Graph.put_edge!("edge_writer_finish", from: "writer", to: "$finish")

      assert {:ok, run, _} = Docket.Test.run_inline(graph, %{})
      assert run.status == :failed
      assert field_value(run, "out") == :unwritten
    end
  end

  describe "retry policy" do
    test "a flaky node succeeds within its retry budget" do
      assert {:ok, run, checkpoints} =
               Docket.Test.run_inline(Graphs.retry_then_continue(), %{})

      assert run.status == :done
      assert run.output == %{"out" => "done"}
      assert run.active_tasks == %{}
      assert run.pending_writes == []
      assert run.timers == %{}

      # Each retryable failure yields exactly one retry park; the barrier
      # then commits the whole superstep in one step.
      assert checkpoint_types(checkpoints) ==
               [
                 :run_initialized,
                 :retry_scheduled,
                 :retry_scheduled,
                 :step_committed,
                 :run_completed
               ]

      retried =
        for checkpoint <- checkpoints,
            checkpoint.type == :retry_scheduled,
            event <- checkpoint.events,
            event.type == :node_failed do
          {event.payload["attempt"], event.payload["permanent"]}
        end

      assert retried == [{1, false}, {2, false}]

      # A retry park is sync, keeps graph status :running, and does not
      # advance the graph step; the parked control state carries the next
      # attempt and the accumulated failures.
      for {park, index} <-
            checkpoints |> Enum.filter(&(&1.type == :retry_scheduled)) |> Enum.with_index(1) do
        assert park.delivery == :sync
        assert park.run.status == :running
        assert park.run.step == 0

        assert [{task_id, task}] = Map.to_list(park.run.active_tasks)
        assert task_id == "#{park.run.id}:0:flaky"
        assert task.attempt == index + 1
        assert task.idempotency_key == "#{task_id}:#{index + 1}"
        assert Enum.map(task.failures, & &1.attempt) == Enum.to_list(1..index)
        assert %Docket.Run.TimerState{kind: :retry} = park.run.timers[task_id]
      end

      step = Enum.find(checkpoints, &(&1.type == :step_committed))
      refute Enum.any?(step.events, &(&1.type == :node_failed))

      assert Enum.any?(
               step.events,
               &(&1.type == :node_completed and &1.payload["attempt"] == 3)
             )
    end

    test "exhausting the retry budget makes the failure permanent" do
      graph =
        Graphs.retry_then_continue()
        |> Graph.update_node!("flaky",
          policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 0}}
        )

      assert {:ok, run, checkpoints} = Docket.Test.run_inline(graph, %{})
      assert run.status == :failed
      assert %Docket.Run.Failure{code: "node_failed", node_id: "flaky"} = run.failure

      # Terminal failure absorbs the active superstep.
      assert run.active_tasks == %{}
      assert run.pending_writes == []
      assert run.timers == %{}

      assert checkpoint_types(checkpoints) == [:run_initialized, :retry_scheduled, :run_failed]

      park = Enum.find(checkpoints, &(&1.type == :retry_scheduled))

      parked_attempts =
        for event <- park.events, event.type == :node_failed do
          {event.payload["attempt"], event.payload["permanent"]}
        end

      assert parked_attempts == [{1, false}]

      run_failed = List.last(checkpoints)

      attempts =
        for event <- run_failed.events, event.type == :node_failed do
          {event.payload["attempt"], event.payload["permanent"]}
        end

      assert attempts == [{2, true}]
    end

    test "retry backoff uses the injected sleeper" do
      test_pid = self()

      sleeper = fn ms ->
        send(test_pid, {:slept, ms})
        :ok
      end

      graph =
        Graphs.retry_then_continue()
        |> Graph.update_node!("flaky",
          policies: %{"retry" => %{"max_attempts" => 3, "backoff_ms" => 25}}
        )

      assert {:ok, %{status: :done}, _} = Docket.Test.run_inline(graph, %{}, sleeper: sleeper)

      assert_received {:slept, 25}
      assert_received {:slept, 25}
      refute_received {:slept, _}
    end

    # The compiler rejects invalid node policies at verify time; these two
    # tests bypass it with a mutated precompiled runtime graph to prove the
    # plan-time defense holds for hand-built or stale runtime graphs.
    test "invalid node policies fail the run with a typed failure" do
      rtg =
        Graphs.retry_then_continue()
        |> compile!()
        |> put_in([Access.key!(:nodes), "node:flaky", Access.key!(:policies)], %{
          "retry" => %{"max_attempts" => 0}
        })

      assert {:ok, run, checkpoints} = Docket.Test.run_inline(rtg, %{})

      assert run.status == :failed
      assert %Docket.Run.Failure{code: "invalid_policy"} = run.failure

      run_failed = List.last(checkpoints)
      assert Enum.any?(run_failed.events, &(&1.payload["reason"] == "invalid_policy"))
    end

    test "the reserved on_error policy is rejected at plan time" do
      rtg =
        Graphs.retry_then_continue()
        |> compile!()
        |> put_in([Access.key!(:nodes), "node:flaky", Access.key!(:policies)], %{
          "on_error" => "route"
        })

      assert {:ok, run, _} = Docket.Test.run_inline(rtg, %{})
      assert run.status == :failed
    end
  end
end
