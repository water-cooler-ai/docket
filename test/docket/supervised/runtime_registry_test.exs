defmodule Docket.Supervised.RuntimeRegistryTest do
  use Docket.Test.Case, async: true

  alias Docket.Checkpoint
  alias Docket.Test.Checkpoint.Recording

  @runtime Module.concat(__MODULE__, Runtime)

  setup do
    start_supervised!({Docket.Runtime.Supervisor, name: @runtime, checkpoint: Recording})
    :ok
  end

  # Starts a run that deterministically parks in :waiting on an open
  # interrupt, so registry state is stable for assertions.
  defp start_waiting_run(run_id) do
    {:ok, run} =
      Docket.run(@runtime, Graphs.interrupt_review(), %{},
        run_id: run_id,
        context: %{notify: self()}
      )

    assert_receive {:checkpoint, %Checkpoint{type: :interrupt_requested} = checkpoint}
    {run, checkpoint.run}
  end

  test "only one active Runtime owns a run ID" do
    {run, _waiting} = start_waiting_run("owned-run")

    assert {:error, %Docket.Error{type: :already_active}} =
             Docket.run(@runtime, Graphs.interrupt_review(), %{},
               run_id: "owned-run",
               context: %{notify: self()}
             )

    assert {:error, %Docket.Error{type: :already_active}} =
             Docket.resume(@runtime, Graphs.interrupt_review(), run, context: %{notify: self()})
  end

  test "get_run/3 reads only active Runtime memory" do
    {_initial, waiting} = start_waiting_run("readable-run")

    assert {:ok, current} = Docket.get_run(@runtime, "readable-run")
    assert current.status == :waiting
    assert current == waiting
  end

  test "never-started runs are not found" do
    assert {:error, %Docket.Error{type: :not_found}} = Docket.get_run(@runtime, "nope")
  end

  test "finished runs are deregistered and no longer readable" do
    {_run, waiting} = start_waiting_run("finished-run")

    {:ok, pid} = Docket.Runtime.Registry.whereis(@runtime, "finished-run")
    ref = Process.monitor(pid)

    [interrupt_id] = Map.keys(waiting.interrupts)

    assert {:ok, _run} =
             Docket.resolve_interrupt(@runtime, "finished-run", interrupt_id, "approved")

    assert_receive {:checkpoint, %Checkpoint{type: :run_completed}}
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

    assert {:error, %Docket.Error{type: :not_found}} = Docket.get_run(@runtime, "finished-run")
  end
end
