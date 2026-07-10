defmodule Docket.Run do
  @moduledoc """
  Canonical durable execution state document for one graph run.

  A run is created by Docket when a run starts, advanced through
  `Docket.Checkpoint` emissions, stored by the host application, and passed
  back to Docket to resume. Hosts may inspect the top-level fields (`id`,
  `graph_id`, `graph_hash`, `status`, `step`, `input`, `output`, and the
  timestamps) but should not interpret, pattern match, mutate, or rebuild
  Docket-owned execution internals such as channels, interrupts, the
  changed-channel set, or the active-superstep state (`active_tasks`,
  `pending_writes`, and `timers`).

  Code that needs an external storage format should use `to_map/1` and
  `from_map/1` rather than treating the run as a public map contract.

  ## Status

  The durable/public status vocabulary is exactly five values:

  - `:running` - autonomous graph execution can proceed. This one value
    covers ready, claimed, timer-scheduled, budget-yielded, and retry-backoff
    positions; queue position is derived from schedule, claim, and
    active-superstep facts, never stored as extra statuses.
  - `:waiting` - open interrupts and nothing else can proceed; only an
    external graph mutation resumes the run.
  - `:done` / `:failed` / `:cancelled` - terminal and absorbing.

  `:created` is a private initialization sentinel: a built-but-never
  initialized run consumed by the runtime's init barrier. It never appears
  in a checkpoint, is not cancellable, and is rejected by the wire format
  and durable storage.

  Status describes graph execution state, not Runtime process liveness and
  not operational health - see `Docket.RunInfo` for the latter.

  ## Transitions

      created -> running                                    (initialization)
      running -> running | waiting | done | failed | cancelled
      waiting -> running | cancelled

  Terminal statuses are absorbing. A retryable node failure stays `:running`
  with a future wake; only permanent or exhausted graph failure becomes
  `:failed`.

  ## Active superstep

  Between a retryable node failure and the superstep's update barrier, the
  run durably encodes the superstep in flight: `active_tasks` holds the
  parked next attempt of each still-executing task (stable identity,
  snapshot, accumulated failures), `pending_writes` holds completed sibling
  results that stay invisible to channels until the barrier, and `timers`
  holds each parked task's retry deadline. These fields are non-empty only
  on a `:running` run and are cleared by the barrier or a terminal commit.

  ## Failure

  `failure` carries the durable, JSON-safe `Docket.Run.Failure` cause of a
  terminal graph failure. It is present exactly when `status` is `:failed`
  (see `validate_failure/1`), so a failed run retains its cause even when
  event persistence is disabled.
  """

  alias Docket.Run.{Failure, Serializer}

  defstruct [
    :id,
    :graph_id,
    :graph_hash,
    :status,
    :input,
    :output,
    :failure,
    :started_at,
    :updated_at,
    :finished_at,
    step: 0,
    channels: %{},
    changed_channels: MapSet.new(),
    pending_nodes: MapSet.new(),
    active_tasks: %{},
    pending_writes: [],
    interrupts: %{},
    timers: %{},
    checkpoint_seq: 0,
    event_seq: 0,
    version: 1,
    metadata: %{}
  ]

  @typedoc "Durable/public graph status."
  @type durable_status :: :running | :waiting | :done | :failed | :cancelled

  @typedoc "Any graph status, including the private `:created` sentinel."
  @type status :: :created | durable_status()

  @type t :: %__MODULE__{
          id: String.t(),
          graph_id: String.t(),
          graph_hash: String.t() | nil,
          status: status(),
          input: map(),
          output: map() | nil,
          failure: Failure.t() | nil,
          started_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          step: non_neg_integer(),
          channels: %{optional(String.t()) => Docket.Run.ChannelState.t()},
          changed_channels: MapSet.t(String.t()),
          pending_nodes: MapSet.t(String.t()),
          active_tasks: %{optional(String.t()) => Docket.Run.TaskState.t()},
          pending_writes: [Docket.Run.PendingWrite.t()],
          interrupts: %{optional(String.t()) => Docket.Run.InterruptState.t()},
          timers: %{optional(String.t()) => Docket.Run.TimerState.t()},
          checkpoint_seq: non_neg_integer(),
          event_seq: non_neg_integer(),
          version: pos_integer(),
          metadata: map()
        }

  @durable_statuses [:running, :waiting, :done, :failed, :cancelled]
  @terminal_statuses [:done, :failed, :cancelled]

  @doc """
  Returns the five durable/public graph statuses.

  The private `:created` sentinel is deliberately excluded: it must never be
  written to storage or the wire format.
  """
  @spec durable_statuses() :: [durable_status()]
  def durable_statuses, do: @durable_statuses

  @doc """
  Returns true for the five durable/public graph statuses and false for the
  private `:created` sentinel (and anything else).
  """
  @spec durable_status?(term()) :: boolean()
  def durable_status?(status), do: status in @durable_statuses

  @doc """
  Returns true when the run has reached a terminal status.
  """
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{status: status}), do: status in @terminal_statuses

  @doc """
  Returns true when a run may move from `from` to `to` in one committed
  transition.

  Encodes the transition matrix from the module documentation:
  `:created -> :running` is the initialization edge, `:running` may recommit
  itself or reach any other durable status, `:waiting` may only resume to
  `:running` or be cancelled, and terminal statuses are absorbing.
  """
  @spec valid_transition?(status(), status()) :: boolean()
  def valid_transition?(:created, :running), do: true
  def valid_transition?(:running, to) when to in @durable_statuses, do: true
  def valid_transition?(:waiting, to) when to in [:running, :cancelled], do: true
  def valid_transition?(_from, _to), do: false

  @doc """
  Validates that `failure` is present exactly when the run is `:failed`.

  Returns `{:error, Docket.Error.t()}` for a failed run without a
  `Docket.Run.Failure`, any other status carrying one, or a failure value of
  the wrong type.
  """
  @spec validate_failure(t()) :: :ok | {:error, Docket.Error.t()}
  def validate_failure(%__MODULE__{status: :failed, failure: %Failure{}}), do: :ok
  def validate_failure(%__MODULE__{status: status, failure: nil}) when status != :failed, do: :ok

  def validate_failure(%__MODULE__{status: :failed, failure: failure}) do
    {:error,
     Docket.Error.new(
       :invalid_run,
       "a failed run must carry a Docket.Run.Failure, got: #{inspect(failure)}"
     )}
  end

  def validate_failure(%__MODULE__{status: status, failure: failure}) do
    {:error,
     Docket.Error.new(
       :invalid_run,
       "failure is only present on a failed run, got status #{inspect(status)} " <>
         "with failure #{inspect(failure)}"
     )}
  end

  @doc """
  Dumps the run to the plain, JSON-safe wire map.

  The wire map is the storage boundary for hosts that cannot persist Elixir
  structs directly. Raises `Docket.Error` for a `:created` run, a run whose
  status and failure disagree, or non-durable content - the latter indicates
  a Docket bug: the runtime coerces all open content to durable form at
  write barriers.
  """
  @spec to_map(t(), keyword()) :: map()
  def to_map(%__MODULE__{} = run, opts \\ []), do: Serializer.dump(run, opts)

  @doc """
  Loads a run from a wire map produced by `to_map/1`.

  Validates the document strictly and never creates atoms.
  """
  @spec from_map(map(), keyword()) :: {:ok, t()} | {:error, Docket.Error.t()}
  def from_map(map, opts \\ []) do
    {:ok, Serializer.load!(map, opts)}
  rescue
    error in Docket.Error -> {:error, error}
  end

  @doc """
  Same as `from_map/2` but raises `Docket.Error` on invalid documents.
  """
  @spec from_map!(map(), keyword()) :: t()
  def from_map!(map, opts \\ []), do: Serializer.load!(map, opts)
end
