defmodule Docket.Runtime.Moment do
  @moduledoc """
  Substrate-neutral pre-commit value for exactly one runtime transition.

  Initialization, advancement, and graph signals each calculate one moment:
  the proposed `Docket.Run`, the runtime events already assigned from the
  run's sequences, the checkpoint type/metadata for the commit boundary, and
  an explicit disposition telling the driver what the run needs next.
  Calculating a moment performs no storage write, no checkpoint delivery,
  and no telemetry emission.

  A moment is not a committed `Docket.Checkpoint` and carries no storage
  vocabulary. Drivers own commitment:

  - A durable driver persists the proposed run and assigned events inside
    its outer storage transaction and, only after transaction success,
    builds the committed checkpoint with `checkpoint/2`/`context/2` and
    delivers observers and telemetry. A lost fence or failed event append
    discards the moment; no committed checkpoint value ever exists for a
    discarded moment, and observer failure after commit cannot change
    durable state.
  - The host-owned drivers (supervised runtime and inline test shell)
    present the moment to their configured sync checkpoint committer, which
    may veto the transition before it becomes the run's in-memory truth.

  Dispositions:

  | disposition | meaning |
  | --- | --- |
  | `:continue` | the run is advanceable now; propose the next moment |
  | `{:park, :immediate, reason}` | commit, then wake immediately |
  | `{:park, :external, reason}` | nothing dispatchable until an external signal (open interrupts) |
  | `{:park, {:at, timestamp}, reason}` | nothing dispatchable before `timestamp` (earliest retry deadline) |
  | `{:park, :terminal, reason}` | the run is terminal; it never wakes again |

  Disposition is decided by the runtime core; storage contracts receive
  only the schedule effect a lifecycle composer derives from it.
  """

  alias Docket.Checkpoint

  @enforce_keys [:run, :events, :checkpoint_type, :disposition, :proposed_at]
  defstruct [
    :run,
    :events,
    :checkpoint_type,
    :disposition,
    :proposed_at,
    checkpoint_metadata: %{}
  ]

  @type park_kind :: :immediate | :external | {:at, DateTime.t()} | :terminal

  @type disposition :: :continue | {:park, park_kind(), term()}

  @type t :: %__MODULE__{
          run: Docket.Run.t(),
          events: [Docket.Event.t()],
          checkpoint_type: Checkpoint.type(),
          checkpoint_metadata: map(),
          disposition: disposition(),
          proposed_at: DateTime.t()
        }

  @doc """
  Builds the committed `Docket.Checkpoint` value for a moment.

  Call only after the moment has durably committed (or, for host-owned
  drivers, as the value presented to the sync committer). `delivery` is the
  resolved delivery mode for the checkpoint type.
  """
  @spec checkpoint(t(), Checkpoint.delivery()) :: Checkpoint.t()
  def checkpoint(%__MODULE__{} = moment, delivery) do
    %Checkpoint{
      type: moment.checkpoint_type,
      delivery: delivery,
      seq: moment.run.checkpoint_seq,
      run: moment.run,
      events: moment.events,
      created_at: moment.proposed_at,
      metadata: moment.checkpoint_metadata
    }
  end

  @doc """
  Builds the `Docket.Checkpoint.Context` for a moment's checkpoint.

  `application` is the host application context configured on the runtime.
  """
  @spec context(t(), map()) :: Checkpoint.Context.t()
  def context(%__MODULE__{} = moment, application \\ %{}) do
    %Checkpoint.Context{
      run_id: moment.run.id,
      graph_id: moment.run.graph_id,
      graph_hash: moment.run.graph_hash,
      application: application
    }
  end
end
