defmodule Docket.Detached do
  @moduledoc """
  Identity and supervision helpers for detached node work.

  A node that returns `{:detach, token}` hands its in-flight work back to
  the runtime: the run parks durably with the task's identity and a
  mandatory deadline, and the claim releases while the work finishes outside
  the runtime. Work must not live in the activation process — the dispatcher
  kills that process at its attempt timeout — so `start/2` starts it under
  the runtime's task supervisor, and `from_context/1` captures the
  completion identity from the node's execution context before the node
  returns.

  The worker completes through `Docket.complete_detached/4` (or the
  `use Docket` delegate) with the captured identity. A worker that crashes
  or never completes is recovered by the detach deadline; nothing tracks the
  worker process itself.
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

  @doc """
  Starts detached work under the runtime's task supervisor.

  The supervisor comes from the node's execution context; work started here
  survives the activation process, the vehicle, and the claim, and is
  terminated with the runtime's supervision tree. Returns
  `{:error, :no_task_supervisor}` in shells that run without a runtime tree,
  such as the processless `Docket.Test`.
  """
  @spec start(map(), (-> any())) :: {:ok, pid()} | {:error, term()}
  def start(%{task_supervisor: supervisor}, fun)
      when not is_nil(supervisor) and is_function(fun, 0) do
    Task.Supervisor.start_child(supervisor, fun)
  end

  def start(context, fun) when is_map(context) and is_function(fun, 0),
    do: {:error, :no_task_supervisor}
end
