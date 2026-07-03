defmodule Docket.Graph.Compiler.PolicyValidationTest do
  use Docket.Test.Case, async: true

  # Compiler validation of the v1 node policy surface defined by the runtime
  # ("timeout_ms", "retry" => %{"max_attempts", "backoff_ms"}, reserved
  # "on_error"). Closes runtime attempt-1 clarification C4 / compiler phase
  # 9.5: invalid policies are now compile errors instead of plan-time run
  # failures.

  defp with_policies(policies) do
    Graph.update_node!(Graphs.retry_then_continue(), "flaky", policies: policies)
  end

  describe "valid node policies" do
    test "the full v1 surface compiles" do
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
end
