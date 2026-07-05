defmodule Docket.Test.EtsCheckpointSinkTest do
  use Docket.Test.Case, async: true

  alias Docket.Test.Checkpoint.EtsSink

  setup do
    table = EtsSink.new_table()

    on_exit(fn ->
      if :ets.info(table) != :undefined do
        :ets.delete(table)
      end
    end)

    {:ok, table: table}
  end

  defp run_with_sink(table, graph, input, opts \\ []) do
    Docket.Test.run_inline(
      graph,
      input,
      [checkpoint: EtsSink, context: %{checkpoint_table: table}] ++ opts
    )
  end

  test "accepted checkpoints are stored in order and fetched by run ID", %{table: table} do
    assert {:ok, run, returned} =
             run_with_sink(table, Graphs.minimal_linear(), %{"value" => "hi"})

    stored = EtsSink.list_checkpoints(table, run.id)

    assert checkpoint_types(stored) == [:run_initialized, :step_committed, :run_completed]
    assert stored == returned
    assert Enum.map(stored, & &1.seq) == [1, 2, 3]
  end

  test "latest_run returns the run from the latest accepted checkpoint", %{table: table} do
    assert {:ok, run, _} = run_with_sink(table, Graphs.minimal_linear(), %{"value" => "hi"})

    latest = EtsSink.latest_run(table, run.id)
    assert latest.status == :done
    assert latest == run

    assert EtsSink.latest_run(table, "unknown-run") == nil
  end

  test "rows are isolated by run ID and deleted per run", %{table: table} do
    assert {:ok, run_a, _} =
             run_with_sink(table, Graphs.minimal_linear(), %{"value" => "a"}, run_id: "run-a")

    assert {:ok, run_b, _} =
             run_with_sink(table, Graphs.minimal_linear(), %{"value" => "b"}, run_id: "run-b")

    assert length(EtsSink.list_checkpoints(table, run_a.id)) == 3
    assert length(EtsSink.list_checkpoints(table, run_b.id)) == 3

    :ok = EtsSink.delete_run(table, run_a.id)

    assert EtsSink.list_checkpoints(table, run_a.id) == []
    assert length(EtsSink.list_checkpoints(table, run_b.id)) == 3
  end

  test "handling is idempotent by run ID and checkpoint seq", %{table: table} do
    assert {:ok, run, [first | _]} =
             run_with_sink(table, Graphs.minimal_linear(), %{"value" => "hi"})

    context = %Docket.Checkpoint.Context{
      run_id: run.id,
      graph_id: run.graph_id,
      graph_hash: run.graph_hash,
      application: %{checkpoint_table: table}
    }

    assert :ok = EtsSink.handle(first, context)

    assert length(EtsSink.list_checkpoints(table, run.id)) == 3
  end

  test "notify sends accepted checkpoints to the test process", %{table: table} do
    assert {:ok, _run, _} =
             Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "hi"},
               checkpoint: EtsSink,
               context: %{checkpoint_table: table, notify: self()}
             )

    assert_received {:checkpoint, %Docket.Checkpoint{type: :run_initialized}}
    assert_received {:checkpoint, %Docket.Checkpoint{type: :step_committed}}
    assert_received {:checkpoint, %Docket.Checkpoint{type: :run_completed}}
  end
end
