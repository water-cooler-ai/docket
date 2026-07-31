defmodule Docket.DetachedTest do
  use ExUnit.Case, async: true

  alias Docket.Detached
  alias Docket.Run.TaskState

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

  describe "from_task/2" do
    test "carries the durably parked task's identity verbatim" do
      task = %TaskState{
        task_id: "run_abc:3:fetch",
        node_id: "fetch",
        step: 3,
        attempt: 2,
        status: :detached,
        idempotency_key: "run_abc:3:fetch:2"
      }

      assert Detached.from_task("run_abc", task) == Detached.from_context(@context)
    end
  end
end
