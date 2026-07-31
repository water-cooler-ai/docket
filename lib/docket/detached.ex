defmodule Docket.Detached do
  @moduledoc """
  Completion identity for detached node work.

  A node that returns `{:detach, token, worker}` hands its long-running work
  back to the runtime: the run parks durably with the task's identity and a
  mandatory deadline, the claim releases, and the runtime starts `worker`
  under its task supervisor **only after that park commits** — the worker
  receives this identity and completes through `Docket.complete_detached/4`
  (or the `use Docket` delegate). Because the worker cannot exist before the
  detach is durable, a completion can never race a park that was not yet
  persisted, and a park whose commit loses its fence never starts a worker.

  A node that hands work to an external system instead returns
  `{:detach, token}` and gives that system the identity (build it with
  `from_context/1` inside the node). An external completion that arrives
  before the detach park commits receives
  `{:error, %Docket.Error{type: :detach_pending}}` and must retry.

  A worker that crashes or never completes is recovered by the detach
  deadline; nothing tracks the worker process itself.
  """

  alias Docket.Run.TaskState

  @enforce_keys [:run_id, :node_id, :step, :attempt, :task_id, :idempotency_key]
  defstruct [:run_id, :node_id, :step, :attempt, :task_id, :idempotency_key]

  @type t :: %__MODULE__{
          run_id: String.t(),
          node_id: String.t(),
          step: non_neg_integer(),
          attempt: pos_integer(),
          task_id: String.t(),
          idempotency_key: String.t()
        }

  @doc """
  Builds the completion identity from a node's execution context.

  The identity names one attempt of one task in one superstep; the
  completion mutation is fenced on the run still holding exactly that task
  detached at exactly that attempt. `idempotency_key` is the same attempt
  identity the execution contract designates for external-effect
  deduplication.
  """
  @spec from_context(map()) :: t()
  def from_context(%{run_id: run_id, node_id: node_id, step: step, attempt: attempt}) do
    task_id = TaskState.task_id(run_id, step, node_id)

    %__MODULE__{
      run_id: run_id,
      node_id: node_id,
      step: step,
      attempt: attempt,
      task_id: task_id,
      idempotency_key: TaskState.idempotency_key(task_id, attempt)
    }
  end

  @doc false
  # Builds the identity handed to a post-commit worker from the durably
  # parked task itself.
  @spec from_task(String.t(), TaskState.t()) :: t()
  def from_task(run_id, %TaskState{} = task) do
    %__MODULE__{
      run_id: run_id,
      node_id: task.node_id,
      step: task.step,
      attempt: task.attempt,
      task_id: task.task_id,
      idempotency_key: task.idempotency_key
    }
  end
end
