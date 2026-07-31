defmodule Docket.DetachedTest do
  use ExUnit.Case, async: true

  alias Docket.Detached

  @context %{
    run_id: "run_abc",
    node_id: "fetch",
    step: 3,
    attempt: 2,
    source_versions: %{},
    idempotency_key: "run_abc:3:fetch:2",
    application: %{}
  }

  describe "from_context/1" do
    test "derives the completion identity from the execution context" do
      ref = Detached.from_context(@context)

      assert %Detached{
               run_id: "run_abc",
               node_id: "fetch",
               step: 3,
               attempt: 2,
               task_id: "run_abc:3:fetch",
               idempotency_key: "run_abc:3:fetch:2"
             } = ref
    end
  end

  describe "start/2" do
    test "starts detached work under the runtime task supervisor" do
      supervisor = start_supervised!({Task.Supervisor, name: nil})
      context = Map.put(@context, :task_supervisor, supervisor)
      parent = self()

      assert {:ok, pid} = Detached.start(context, fn -> send(parent, {:worked, self()}) end)
      assert_receive {:worked, ^pid}
    end

    test "returns an error when no runtime task supervisor exists" do
      assert {:error, :no_task_supervisor} = Detached.start(@context, fn -> :ok end)

      assert {:error, :no_task_supervisor} =
               Detached.start(Map.put(@context, :task_supervisor, nil), fn -> :ok end)
    end
  end
end
