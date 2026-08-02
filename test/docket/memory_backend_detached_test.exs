defmodule Docket.Test.MemoryBackendDetachedTest do
  use ExUnit.Case, async: true

  alias Docket.BackendTests.Fixture
  alias Docket.Run.TaskState
  alias Docket.Runtime.RunMutation
  alias Docket.Test.MemoryBackend

  setup context do
    {:ok, backend_test: subject} = Docket.Test.BackendTestSetup.Memory.setup(context)
    %{subject: subject}
  end

  defp pending_detached(run, node_id, now) do
    snapshot = %{}
    task_id = TaskState.task_id(run.id, run.step, node_id)

    %TaskState{
      task_id: task_id,
      node_id: node_id,
      step: run.step,
      attempt: 1,
      status: :detached_pending,
      input_hash: TaskState.snapshot_hash(snapshot),
      idempotency_key: TaskState.idempotency_key(task_id, 1),
      snapshot: snapshot,
      source_versions: %{},
      scheduled_at: now,
      failures: []
    }
  end

  test "park writes the pending index entry; cancel clears it in the terminal commit",
       %{subject: subject} do
    {graph, graph_hash} = Fixture.publish_graph(subject, :tenantless, "detached")
    run = Fixture.run(subject, "detached-run", graph, graph_hash)
    {:ok, _} = Fixture.initialize(subject, :tenantless, run, [])

    task = pending_detached(run, "summarize", subject.now)

    parked = %{
      run
      | checkpoint_seq: run.checkpoint_seq + 1,
        active_tasks: %{task.task_id => task}
    }

    assert {:ok, _} =
             MemoryBackend.commit_unclaimed(
               subject.context,
               :system,
               run.checkpoint_seq,
               %{
                 run: parked,
                 checkpoint_type: :detach_scheduled,
                 schedule: {:release_claim, :external}
               },
               []
             )

    assert [row] = MemoryBackend.detached_index(subject.context)
    assert row.run_id == run.id
    assert row.task_id == task.task_id
    assert row.node_id == "summarize"
    assert row.attempt == 1
    assert row.state == "pending"
    assert row.tenant_id == nil
    assert row.scheduled_at == subject.now
    assert row.claimed_at == nil
    assert row.deadline_at == nil

    {:ok, moment} = RunMutation.cancel_run(parked, DateTime.add(subject.now, 5, :second))

    assert {:ok, _} =
             MemoryBackend.commit_unclaimed(
               subject.context,
               :system,
               parked.checkpoint_seq,
               %{
                 run: moment.run,
                 checkpoint_type: :run_cancelled,
                 schedule: {:release_claim, :terminal}
               },
               moment.events
             )

    assert MemoryBackend.detached_index(subject.context) == []
  end

  test "a rejected commit leaves the index untouched", %{subject: subject} do
    {graph, graph_hash} = Fixture.publish_graph(subject, :tenantless, "detached-stale")
    run = Fixture.run(subject, "stale-run", graph, graph_hash)
    {:ok, _} = Fixture.initialize(subject, :tenantless, run, [])

    task = pending_detached(run, "summarize", subject.now)

    parked = %{
      run
      | checkpoint_seq: run.checkpoint_seq + 2,
        active_tasks: %{task.task_id => task}
    }

    assert {:error, :stale_checkpoint} =
             MemoryBackend.commit_unclaimed(
               subject.context,
               :system,
               run.checkpoint_seq + 1,
               %{
                 run: parked,
                 checkpoint_type: :detach_scheduled,
                 schedule: {:release_claim, :external}
               },
               []
             )

    assert MemoryBackend.detached_index(subject.context) == []
  end
end
