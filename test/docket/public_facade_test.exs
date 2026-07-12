defmodule Docket.PublicFacadeTest do
  use ExUnit.Case, async: true

  defmodule Host do
    use Docket, backend: Docket.Test.MemoryBackend
  end

  test "the 0.0.1 production facade is not exported" do
    assert Code.ensure_loaded?(Docket)
    assert Code.ensure_loaded?(Host)

    for {name, arity} <- [run: 3, run: 4, resume: 3, resume: 4, get_run: 2, get_run: 3] do
      refute function_exported?(Docket, name, arity)
    end

    for {name, arity} <- [run: 2, run: 3, resume: 2, resume: 3, get_run: 1, get_run: 2] do
      refute function_exported?(Host, name, arity)
    end
  end

  test "the durable facade and processless helpers remain exported" do
    assert Code.ensure_loaded?(Docket)
    assert Code.ensure_loaded?(Docket.Test)

    for {name, arity} <- [
          save_graph: 3,
          start_run: 4,
          fetch_run: 3,
          inspect_run: 3,
          await_run: 3,
          resolve_interrupt: 5,
          cancel_run: 3,
          retry_poisoned_run: 3
        ] do
      assert function_exported?(Docket, name, arity)
    end

    for {name, arity} <- [run_inline: 3, resume_inline: 3, step_inline: 2] do
      assert function_exported?(Docket.Test, name, arity)
    end
  end
end
