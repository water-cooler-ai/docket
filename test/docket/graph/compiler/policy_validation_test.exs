defmodule Docket.Graph.Compiler.PolicyValidationTest do
  use Docket.Test.Case, async: true

  # Compiler validation of the v0.1 node policy surface defined by the runtime
  # ("timeout_ms", "retry" => %{"max_attempts", "backoff_ms"}, reserved
  # "on_error"). Invalid policies are compile errors instead of plan-time run
  # failures.

  defp with_policies(policies) do
    Graph.update_node!(Graphs.retry_then_continue(), "flaky", policies: policies)
  end

  describe "valid node policies" do
    test "the full v0.1 surface compiles" do
      graph =
        with_policies(%{
          "timeout_ms" => 5_000,
          "retry" => %{"max_attempts" => 3, "backoff_ms" => 10}
        })

      assert {:ok, _rtg} = Compiler.compile(graph)
    end

    test "absent and partial policies compile" do
      assert {:ok, _rtg} = Compiler.compile(with_policies(%{}))
      assert {:ok, _rtg} = Compiler.compile(with_policies(%{"retry" => %{}}))
    end

    test "unknown open policy keys are ignored" do
      assert {:ok, _rtg} = Compiler.compile(with_policies(%{"custom" => "host-owned"}))
    end
  end

  describe "invalid node policies" do
    test "timeout_ms must be a positive integer" do
      for bad <- [0, -5, "fast"] do
        with_policies(%{"timeout_ms" => bad})
        |> verify_error!()
        |> assert_diagnostic(:invalid_policy,
          path: [:nodes, "flaky", :policies, "timeout_ms"],
          public_id: "flaky"
        )
      end
    end

    test "retry must be a map" do
      with_policies(%{"retry" => 3})
      |> verify_error!()
      |> assert_diagnostic(:invalid_policy,
        path: [:nodes, "flaky", :policies, "retry"],
        public_id: "flaky"
      )
    end

    test "retry fields are range- and type-checked" do
      for retry <- [
            %{"max_attempts" => 0},
            %{"max_attempts" => "three"},
            %{"backoff_ms" => -1}
          ] do
        with_policies(%{"retry" => retry})
        |> verify_error!()
        |> assert_diagnostic(:invalid_policy, path: [:nodes, "flaky", :policies, "retry"])
      end
    end

    test "unknown retry keys are rejected" do
      diagnostic =
        with_policies(%{"retry" => %{"max_attempts" => 2, "jitter" => true}})
        |> verify_error!()
        |> assert_diagnostic(:invalid_policy, path: [:nodes, "flaky", :policies, "retry"])

      assert diagnostic.message =~ "jitter"
    end

    test "the reserved on_error key is rejected" do
      with_policies(%{"on_error" => "fallback"})
      |> verify_error!()
      |> assert_diagnostic(:invalid_policy,
        path: [:nodes, "flaky", :policies, "on_error"],
        public_id: "flaky"
      )
    end

    test "every offending key gets its own diagnostic" do
      diagnostics =
        with_policies(%{
          "timeout_ms" => "slow",
          "retry" => %{"max_attempts" => 0},
          "on_error" => "route"
        })
        |> verify_error!()

      for key <- ["timeout_ms", "retry", "on_error"] do
        assert_diagnostic(diagnostics, :invalid_policy, path: [:nodes, "flaky", :policies, key])
      end
    end

    test "compile rejects the graph, matching verify" do
      graph = with_policies(%{"on_error" => "route"})

      assert {:error, %Graph{} = failed} = Compiler.compile(graph)
      assert_diagnostic(failed, :invalid_policy, path: [:nodes, "flaky", :policies, "on_error"])
    end
  end

  describe "detach policies" do
    defp with_detach_policies(policies) do
      Graph.update_node!(Graphs.detached_linear(), "summarize", policies: policies)
    end

    test "the full detach surface compiles on a detached node" do
      graph =
        with_detach_policies(%{
          "retry" => %{"max_attempts" => 3, "backoff_ms" => 10},
          "timeout_ms" => 5_000,
          "detach" => %{
            "start_to_close_ms" => 600_000,
            "schedule_to_start_ms" => 60_000,
            "on_deadline" => "fail"
          }
        })

      assert {:ok, _rtg} = Compiler.compile(graph)
    end

    test "absent, empty, and explicit-nil detach values compile" do
      assert {:ok, _rtg} = Compiler.compile(with_detach_policies(%{}))
      assert {:ok, _rtg} = Compiler.compile(with_detach_policies(%{"detach" => nil}))
      assert {:ok, _rtg} = Compiler.compile(with_detach_policies(%{"detach" => %{}}))

      assert {:ok, _rtg} =
               Compiler.compile(
                 with_detach_policies(%{
                   "detach" => %{
                     "start_to_close_ms" => nil,
                     "schedule_to_start_ms" => nil,
                     "on_deadline" => nil
                   }
                 })
               )
    end

    test "unknown open policy keys are ignored on detached nodes" do
      assert {:ok, _rtg} = Compiler.compile(with_detach_policies(%{"custom" => "host-owned"}))
    end

    test "detach policies on a module node are rejected" do
      Graph.update_node!(Graphs.retry_then_continue(), "flaky",
        policies: %{"detach" => %{"start_to_close_ms" => 600_000}}
      )
      |> verify_error!()
      |> assert_diagnostic(:invalid_policy,
        path: [:nodes, "flaky", :policies, "detach"],
        public_id: "flaky"
      )
    end

    test "an explicit-nil detach block means unset on module nodes too" do
      graph =
        Graph.update_node!(Graphs.retry_then_continue(), "flaky", policies: %{"detach" => nil})

      assert {:ok, _rtg} = Compiler.compile(graph)
    end

    test "deadline budgets must be positive integers or nil" do
      for key <- ["start_to_close_ms", "schedule_to_start_ms"],
          bad <- [0, -5, "fast", false, true] do
        with_detach_policies(%{"detach" => %{key => bad}})
        |> verify_error!()
        |> assert_diagnostic(:invalid_policy,
          path: [:nodes, "summarize", :policies, "detach"],
          public_id: "summarize"
        )
      end
    end

    test "on_deadline accepts only reschedule and fail" do
      for bad <- ["retry", 5, true] do
        with_detach_policies(%{"detach" => %{"on_deadline" => bad}})
        |> verify_error!()
        |> assert_diagnostic(:invalid_policy,
          path: [:nodes, "summarize", :policies, "detach"],
          public_id: "summarize"
        )
      end
    end

    test "unknown detach keys are rejected" do
      diagnostic =
        with_detach_policies(%{"detach" => %{"deadline_ms" => 1_000}})
        |> verify_error!()
        |> assert_diagnostic(:invalid_policy,
          path: [:nodes, "summarize", :policies, "detach"]
        )

      assert diagnostic.message =~ "deadline_ms"
    end

    test "detach must be a map" do
      with_detach_policies(%{"detach" => 600_000})
      |> verify_error!()
      |> assert_diagnostic(:invalid_policy,
        path: [:nodes, "summarize", :policies, "detach"],
        public_id: "summarize"
      )
    end
  end
end
