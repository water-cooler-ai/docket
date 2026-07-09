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

  Sync checkpoints must be accepted (`:ok`) before the state transition is
  committed or the related public API reports success. Async checkpoints are
  delivered after the in-memory transition commits; their failure is
  observable but does not roll back the active run.
  """

  defstruct [:type, :delivery, :seq, :run, :created_at, events: [], metadata: %{}]

  @type type ::
          :run_initialized
          | :step_committed
          | :interrupt_requested
          | :interrupt_resolved
          | :run_completed
          | :run_failed

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
    :run_failed
  ]

  @default_deliveries %{
    run_initialized: :sync,
    step_committed: :async,
    interrupt_requested: :sync,
    interrupt_resolved: :sync,
    run_completed: :sync,
    run_failed: :sync
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
