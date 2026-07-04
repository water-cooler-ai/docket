defmodule Docket.Run do
  @moduledoc """
  Canonical durable execution state document for one graph run.

  A run is created by Docket when a run starts, advanced through
  `Docket.Checkpoint` emissions, stored by the host application, and passed
  back to Docket to resume. Hosts may inspect the top-level fields (`id`,
  `graph_id`, `graph_hash`, `status`, `step`, `input`, `output`, and the
  timestamps) but should not interpret, pattern match, mutate, or rebuild
  Docket-owned execution internals such as channels, interrupts, or the
  changed-channel set.

  Code that needs an external storage format should use `to_map/1` and
  `from_map/1` rather than treating the run as a public map contract.

  ## Status

  - `:created` - built but never initialized; the fresh-run sentinel consumed
    by the runtime's init barrier. Never appears in a checkpoint.
  - `:running` - graph execution can proceed.
  - `:waiting` - open interrupts and nothing else can proceed.
  - `:done` / `:failed` / `:cancelled` - terminal.

  Status describes graph execution state, not Runtime process liveness.
  """

  alias Docket.Run.Serializer

  defstruct [
    :id,
    :graph_id,
    :graph_hash,
    :status,
    :input,
    :output,
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

  @type status :: :created | :running | :waiting | :done | :failed | :cancelled

  @type t :: %__MODULE__{
          id: String.t(),
          graph_id: String.t(),
          graph_hash: String.t() | nil,
          status: status(),
          input: map(),
          output: map() | nil,
          started_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          step: non_neg_integer(),
          channels: %{optional(String.t()) => Docket.Run.ChannelState.t()},
          changed_channels: MapSet.t(String.t()),
          pending_nodes: MapSet.t(String.t()),
          active_tasks: map(),
          pending_writes: list(),
          interrupts: %{optional(String.t()) => Docket.Run.InterruptState.t()},
          timers: map(),
          checkpoint_seq: non_neg_integer(),
          event_seq: non_neg_integer(),
          version: pos_integer(),
          metadata: map()
        }

  @statuses [:created, :running, :waiting, :done, :failed, :cancelled]
  @terminal_statuses [:done, :failed, :cancelled]

  @doc false
  @spec statuses() :: [status()]
  def statuses, do: @statuses

  @doc """
  Returns true when the run has reached a terminal status.
  """
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{status: status}), do: status in @terminal_statuses

  @doc """
  Dumps the run to the plain, JSON-safe v1 wire map.

  The wire map is the storage boundary for hosts that cannot persist Elixir
  structs directly. Raises `Docket.Error` if the run contains non-durable
  content, which indicates a Docket bug: the runtime coerces all open content
  to durable form at write barriers.
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
