defmodule Docket.PublicFacadeTest do
  use ExUnit.Case, async: false

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
          list_events: 3,
          await_run: 3,
          resolve_interrupt: 5,
          cancel_run: 3,
          retry_poisoned_run: 3
        ] do
      assert function_exported?(Docket, name, arity)
    end

    assert function_exported?(Host, :list_events, 1)
    assert function_exported?(Host, :list_events, 2)

    for {name, arity} <- [run_inline: 3, resume_inline: 3, step_inline: 2] do
      assert function_exported?(Docket.Test, name, arity)
    end
  end

  describe "list_events through a configured memory-backend runtime" do
    setup do
      start_supervised!(Host)
      {:ok, defaults} = Docket.Runtime.Instance.defaults(Host)

      %{
        backend: Keyword.fetch!(defaults, :backend),
        context: Keyword.fetch!(defaults, :backend_context)
      }
    end

    test "rejects invalid options before reaching storage" do
      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.list_events("run", after_seq: -1)

      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.list_events("run", limit: 0)

      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.list_events("run", limit: 1001)

      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.list_events("run", after_seq: "0")
    end

    test "reads a page of retained events", %{backend: backend, context: context} do
      run = %Docket.Run{
        id: "run",
        graph_id: "g",
        graph_hash: String.duplicate("ab", 32),
        status: :running,
        input: %{},
        checkpoint_seq: 1,
        event_seq: 2,
        started_at: ~U[2026-07-12 00:00:00Z],
        updated_at: ~U[2026-07-12 00:00:00Z]
      }

      events =
        for seq <- 1..2 do
          %Docket.Event{
            run_id: "run",
            seq: seq,
            type: :node_completed,
            step: seq,
            timestamp: ~U[2026-07-12 00:00:00Z],
            payload: %{},
            metadata: %{}
          }
        end

      assert {:ok, :done} =
               backend.transaction(context, fn tx ->
                 with {:ok, _} <-
                        backend.insert_run(
                          tx,
                          :tenantless,
                          run,
                          :run_initialized,
                          ~U[2026-07-12 00:00:00Z]
                        ),
                      :ok <- backend.append_events(tx, :tenantless, "run", events) do
                   {:ok, :done}
                 end
               end)

      assert {:ok, %Docket.EventPage{} = page} = Host.list_events("run", after_seq: 0, limit: 1)
      assert [%Docket.Event{seq: 1}] = page.events
      assert page.next_after_seq == 1
      assert page.has_more?
      assert page.oldest_available_seq == 1
      assert page.latest_available_seq == 2
      assert page.latest_seq == 2

      assert {:error, :not_found} = Host.list_events("missing")
    end
  end
end
