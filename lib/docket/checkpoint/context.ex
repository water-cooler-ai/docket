defmodule Docket.Checkpoint.Context do
  @moduledoc """
  Context passed to `c:Docket.Checkpoint.handle/2` alongside each checkpoint.

  `checkpoint_seq` is the committed run fence and `graph_step` is the
  proposed run's committed graph position. `active_superstep`, when present,
  exposes the stable identities of scheduled retry tasks and results awaiting
  the barrier. `node_attempts` describes only the attempt outcomes newly
  represented by this moment. These fields let durable drivers and observers
  identify retry work without interpreting Docket-owned run state or parsing
  task IDs.

  `application` carries the caller-supplied application context from the run
  options; Docket does not interpret it.
  """

  defstruct [
    :run_id,
    :graph_id,
    :graph_hash,
    :checkpoint_seq,
    :graph_step,
    active_superstep: nil,
    node_attempts: [],
    application: %{}
  ]

  @type superstep_task :: %{
          task_id: String.t(),
          node_id: String.t(),
          scheduled_attempt: pos_integer(),
          idempotency_key: String.t()
        }

  @type pending_attempt :: %{
          task_id: String.t(),
          node_id: String.t(),
          attempted: pos_integer(),
          kind: :update | :interrupt,
          idempotency_key: String.t()
        }

  @type active_superstep :: %{
          step: non_neg_integer(),
          tasks: [superstep_task()],
          pending_attempts: [pending_attempt()]
        }

  @type node_attempt :: %{
          task_id: String.t(),
          node_id: String.t(),
          attempted: pos_integer(),
          outcome: :completed | :failed | :interrupted | :pending_update | :pending_interrupt,
          next_scheduled_attempt: pos_integer() | nil
        }

  @type t :: %__MODULE__{
          run_id: String.t(),
          graph_id: String.t(),
          graph_hash: String.t() | nil,
          checkpoint_seq: pos_integer() | nil,
          graph_step: non_neg_integer() | nil,
          active_superstep: active_superstep() | nil,
          node_attempts: [node_attempt()],
          application: map()
        }
end
