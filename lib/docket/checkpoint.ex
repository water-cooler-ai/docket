defmodule Docket.Checkpoint do
  @moduledoc """
  Public notification that a runtime moment committed, carrying the latest
  restorable `Docket.Run` document.

  Host applications implement the `handle/2` callback and persist
  `checkpoint.run` (and optionally `checkpoint.events`). Handlers upsert by
  `Docket.Run.id`, must not require a run row to exist before the first
  checkpoint, and must be safe to receive the same checkpoint more than once.

  v1 checkpoint types and default delivery:

  | type | delivery |
  | --- | --- |
  | `:run_initialized` | `:sync` |
  | `:step_committed` | `:async` |
  | `:interrupt_requested` | `:sync` |
  | `:interrupt_resolved` | `:sync` |
  | `:run_completed` | `:sync` |
  | `:run_failed` | `:sync` |
  | `:run_cancelled` | `:sync` |

  In the host-owned driver, sync checkpoints must be accepted (`:ok`) before
  the in-memory transition commits or the related public API reports success.

  In a Docket-owned durable driver, the storage backend is the committer.
  Handlers run only after the fenced storage commit succeeds and are
  notifications: handler failure is observable but cannot roll back durable
  state. A proposal that loses its fence is never delivered as committed.

  ## Relation to storage backends

  The `handle/2` callback is the host-facing notification contract: hosts
  persist runs from checkpoints exactly as before in the host-owned driver.
  `Docket.Storage` and `Docket.Coordinator` are the deeper seam a durable
  backend implements. In that driver this callback is post-commit observation,
  because an arbitrary callback cannot participate in the backend transaction.
  """

  defstruct [:type, :delivery, :seq, :run, :created_at, events: [], metadata: %{}]

  @type type ::
          :run_initialized
          | :step_committed
          | :interrupt_requested
          | :interrupt_resolved
          | :run_completed
          | :run_failed
          | :run_cancelled

  @type delivery :: :sync | :async

  @type t :: %__MODULE__{
          type: type(),
          delivery: delivery(),
          seq: pos_integer(),
          run: Docket.Run.t(),
          events: [Docket.Event.t()],
          created_at: DateTime.t(),
          metadata: map()
        }

  @callback handle(checkpoint :: t(), context :: Docket.Checkpoint.Context.t()) ::
              :ok | {:error, term()}

  @types [
    :run_initialized,
    :step_committed,
    :interrupt_requested,
    :interrupt_resolved,
    :run_completed,
    :run_failed,
    :run_cancelled
  ]

  @default_deliveries %{
    run_initialized: :sync,
    step_committed: :async,
    interrupt_requested: :sync,
    interrupt_resolved: :sync,
    run_completed: :sync,
    run_failed: :sync,
    run_cancelled: :sync
  }

  @doc false
  @spec types() :: [type()]
  def types, do: @types

  @doc """
  Resolves the delivery mode for a checkpoint type.

  `overrides` may force additional types to `:sync` when the host wants
  stronger crash-resume guarantees; forcing a sync type to `:async` is not
  supported and is ignored.
  """
  @spec delivery(type(), %{optional(type()) => delivery()}) :: delivery()
  def delivery(type, overrides \\ %{}) when type in @types do
    default = Map.fetch!(@default_deliveries, type)

    case {default, Map.get(overrides, type)} do
      {:async, :sync} -> :sync
      {default, _other} -> default
    end
  end
end
