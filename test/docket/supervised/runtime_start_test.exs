defmodule Docket.Supervised.RuntimeStartTest do
  use Docket.Test.Case, async: true

  alias Docket.Checkpoint
  alias Docket.Test.Checkpoint.Recording

  @runtime Module.concat(__MODULE__, Runtime)

  defmodule HostDocket do
    use Docket, checkpoint: Docket.Test.Checkpoint.Recording
  end

  setup do
    start_supervised!({Docket.Runtime.Supervisor, name: @runtime, checkpoint: Recording})
    :ok
  end

  defp run_opts(overrides \\ []) do
    Keyword.merge([context: %{notify: self()}], overrides)
  end

  test "run/4 is a start barrier: it returns the initialized run before completion" do
    assert {:ok, run} =
             Docket.run(@runtime, Graphs.minimal_linear(), %{"value" => "hi"}, run_opts())

    # The returned snapshot is the durably initialized run; execution
    # continues in the Runtime process.
    assert run.status == :running
    assert run.step == 0
    assert run.output == nil

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = final}
    assert final.run.output == %{"result" => "hi"}
  end

  test ":run_initialized is emitted before any node execution" do
    assert {:ok, _run} =
             Docket.run(@runtime, Graphs.minimal_linear(), %{"value" => "hi"}, run_opts())

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed}}

    # Drain in arrival order: the initialization checkpoint must be first and
    # must carry no node events.
    assert_received {:checkpoint, %Checkpoint{type: :run_initialized} = first}
    refute Enum.any?(first.events, &(&1.type in [:node_completed, :node_failed]))
  end

  test "use Docket host wrappers drive the same path" do
    start_supervised!(HostDocket)

    assert {:ok, run} =
             HostDocket.run(Graphs.minimal_linear(), %{"value" => "wrapped"}, run_opts())

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed} = final}
    assert final.run.id == run.id
    assert final.run.output == %{"result" => "wrapped"}
  end

  test "invalid input fails before anything durable is written" do
    assert {:error, %Docket.Error{type: :invalid_input}} =
             Docket.run(@runtime, Graphs.minimal_linear(), %{}, run_opts(run_id: "bad-input"))

    refute_received {:checkpoint, _}
    assert {:error, %Docket.Error{type: :not_found}} = Docket.get_run(@runtime, "bad-input")
  end

  test "initial checkpoint failure leaves no runtime registered" do
    assert {:error, %Docket.Error{type: :checkpoint_failed, phase: :run_initialized}} =
             Docket.run(
               @runtime,
               Graphs.minimal_linear(),
               %{"value" => "hi"},
               run_opts(
                 run_id: "cp-fail",
                 context: %{notify: self(), fail_on: [:run_initialized]}
               )
             )

    assert_receive {:checkpoint_rejected, %Checkpoint{type: :run_initialized}}
    assert {:error, %Docket.Error{type: :not_found}} = Docket.get_run(@runtime, "cp-fail")
  end

  test "unverifiable graphs are rejected with diagnostics" do
    assert {:error, %Docket.Error{type: :invalid_graph} = error} =
             Docket.run(@runtime, Graphs.invalid_unknown_target(), %{}, run_opts())

    assert error.details.diagnostics != []
  end

  test "an unstarted runtime instance is a typed error" do
    assert {:error, %Docket.Error{type: :runtime_unavailable}} =
             Docket.run(NotStarted.Docket, Graphs.minimal_linear(), %{"value" => "hi"}, [])
  end
end
