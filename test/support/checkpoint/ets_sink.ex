defmodule Docket.Test.Checkpoint.EtsSink do
  @moduledoc """
  ETS-backed checkpoint sink for recovery and host-like lookup tests.

  Each test creates its own anonymous table and passes it through the
  application context; rows contain public checkpoint documents only:

      table = EtsSink.new_table()
      Docket.run(runtime, graph, input,
        checkpoint: EtsSink,
        context: %{checkpoint_table: table}
      )

      EtsSink.latest_run(table, run_id)

  Handling is idempotent by `{run_id, checkpoint_seq}`: redelivering a
  checkpoint overwrites the same row. When the context carries `:notify`,
  every accepted checkpoint is also sent to that pid as
  `{:checkpoint, checkpoint}` so supervised tests can synchronize without
  sleeps.
  """

  @behaviour Docket.Checkpoint

  @doc "Creates an anonymous public table owned by the calling test."
  def new_table do
    :ets.new(:docket_checkpoint, [:public, :ordered_set])
  end

  @impl true
  def handle(checkpoint, %Docket.Checkpoint.Context{application: application}) do
    table = Map.fetch!(application, :checkpoint_table)

    row = %{
      checkpoint: checkpoint,
      inserted_at: :erlang.unique_integer([:monotonic])
    }

    true = :ets.insert(table, {{checkpoint.run.id, checkpoint.seq}, row})

    case Map.fetch(application, :notify) do
      {:ok, pid} -> send(pid, {:checkpoint, checkpoint})
      :error -> :ok
    end

    :ok
  end

  @doc "Returns the run document from the latest accepted checkpoint, or nil."
  def latest_run(table, run_id) do
    case list_checkpoints(table, run_id) do
      [] -> nil
      checkpoints -> List.last(checkpoints).run
    end
  end

  @doc "Lists accepted checkpoints for a run in checkpoint-seq order."
  def list_checkpoints(table, run_id) do
    table
    |> :ets.match_object({{run_id, :_}, :_})
    |> Enum.map(fn {{_run_id, _seq}, row} -> row.checkpoint end)
  end

  @doc "Deletes only this run's rows."
  def delete_run(table, run_id) do
    :ets.match_delete(table, {{run_id, :_}, :_})
    :ok
  end
end
