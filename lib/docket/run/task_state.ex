defmodule Docket.Run.TaskState do
  @moduledoc """
  Durable description of one node execution attempt.

  Task state appears in event payloads, checkpoint metadata, and — while a
  superstep is parked between attempts — in `Docket.Run.active_tasks`. An
  active entry pins the activation identity for the next attempt: the stable
  `task_id`, the attempt number to execute, the committed state snapshot and
  source versions the superstep planned against, and every prior failed
  attempt. Re-executing from this state preserves the task and idempotency
  identity of the superstep that parked it.
  """

  defstruct [
    :task_id,
    :node_id,
    :step,
    :attempt,
    :status,
    :input_hash,
    :idempotency_key,
    :snapshot,
    :source_versions,
    :started_at,
    :deadline_at,
    failures: [],
    metadata: %{}
  ]

  @typedoc "One durably recorded failed attempt: the attempt number and reason text."
  @type failure :: %{attempt: pos_integer(), reason: String.t()}

  @type t :: %__MODULE__{
          task_id: String.t(),
          node_id: String.t(),
          step: non_neg_integer(),
          attempt: pos_integer(),
          status: atom() | nil,
          input_hash: String.t() | nil,
          idempotency_key: String.t() | nil,
          snapshot: map() | nil,
          source_versions: %{optional(String.t()) => non_neg_integer()} | nil,
          started_at: DateTime.t() | nil,
          deadline_at: DateTime.t() | nil,
          failures: [failure()],
          metadata: map()
        }

  @doc """
  Builds the stable task identity for one node execution in one superstep.

  The same committed run always yields the same task ID, so a superstep that
  never commits re-plans with byte-identical identities.
  """
  @spec task_id(String.t(), non_neg_integer(), String.t()) :: String.t()
  def task_id(run_id, step, node_id), do: "#{run_id}:#{step}:#{node_id}"

  @doc """
  Builds the idempotency key for one attempt of a task.
  """
  @spec idempotency_key(String.t(), pos_integer()) :: String.t()
  def idempotency_key(task_id, attempt), do: "#{task_id}:#{attempt}"

  @doc """
  Hashes a committed state snapshot into the activation's input identity.

  The hash is over deterministic external term format bytes, so equal durable
  snapshots hash equally regardless of map insertion order.
  """
  @spec snapshot_hash(map()) :: String.t()
  def snapshot_hash(snapshot) do
    snapshot
    |> :erlang.term_to_binary([:deterministic, minor_version: 2])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
