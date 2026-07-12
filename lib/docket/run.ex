defmodule Docket.Run do
  @moduledoc """
  Durable execution state for one graph run.

  A run is created by Docket when a run starts, advanced through committed
  read-only checkpoint notifications, and stored by the configured backend.
  the top-level fields (`id`,
  `graph_id`, `graph_hash`, `status`, `step`, `input`, `output`, and the
  timestamps) but should not interpret, pattern match, mutate, or rebuild
  Docket-owned execution internals such as channels, interrupts, the
  changed-channel set, or the active-superstep state (`active_tasks`,
  `pending_writes`, and `timers`).

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
  , in an uncommitted transition, is not cancellable, and is rejected by durable storage.

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

  `failure` carries the durable `Docket.Run.Failure` cause of a
  terminal graph failure. It is present exactly when `status` is `:failed`
  (see `validate_failure/1`), so a failed run retains its cause even when
  event persistence is disabled.
  """

  alias Docket.{DurableCodec, Interrupt, Schema}

  alias Docket.Run.{
    ChannelState,
    Failure,
    InterruptState,
    PendingWrite,
    TaskState,
    TimerState
  }

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
          metadata: map()
        }

  @durable_statuses [:running, :waiting, :done, :failed, :cancelled]
  @terminal_statuses [:done, :failed, :cancelled]

  @doc """
  Returns the five durable/public graph statuses.

  The private `:created` sentinel is deliberately excluded: it must never be
  written to durable storage.
  """
  @spec durable_statuses() :: [durable_status()]
  def durable_statuses, do: @durable_statuses

  @doc "Returns the terminal subset of the durable graph statuses."
  @spec terminal_statuses() :: [durable_status()]
  def terminal_statuses, do: @terminal_statuses

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

  @doc false
  @spec validate_durable(t()) :: :ok | {:error, Docket.Error.t()}
  def validate_durable(%__MODULE__{} = run) do
    cond do
      not exact_struct?(run, __MODULE__) ->
        invalid_durable("run struct has missing or unexpected fields")

      not durable_status?(run.status) ->
        invalid_durable("status must be durable, got #{inspect(run.status)}")

      not valid_id?(run.id) or not valid_id?(run.graph_id) or not valid_id?(run.graph_hash) ->
        invalid_durable("id, graph_id, and graph_hash must be non-empty binaries")

      not counters_valid?(run) ->
        invalid_durable("step and sequence counters must be non-negative integers")

      not plain_map?(run.input) or not plain_map?(run.metadata) or
          not (is_nil(run.output) or plain_map?(run.output)) ->
        invalid_durable("input, output, and metadata have invalid shapes")

      not portable_map?(run.input) or not portable_map?(run.metadata) or
          not (is_nil(run.output) or portable_map?(run.output)) ->
        invalid_durable("input, output, and metadata must contain portable values")

      not is_nil(run.output) and run.status != :done ->
        invalid_durable("output is only present on a done run")

      not collections_valid?(run) ->
        invalid_durable("run collections have invalid shapes")

      not valid_datetime?(run.started_at) or not valid_datetime?(run.updated_at) or
          not (is_nil(run.finished_at) or valid_datetime?(run.finished_at)) ->
        invalid_durable("started_at and updated_at must be UTC DateTimes; finished_at may be nil")

      terminal?(run) != match?(%DateTime{}, run.finished_at) ->
        invalid_durable("finished_at must be present exactly for terminal runs")

      true ->
        with :ok <- validate_failure(run),
             true <- valid_failure?(run.failure),
             do: validate_active_superstep(run),
             else: (
               false -> invalid_durable("failure fields are invalid")
               {:error, %Docket.Error{}} = error -> error
             )
    end
  end

  def validate_durable(other), do: invalid_durable("expected Docket.Run, got #{inspect(other)}")

  defp counters_valid?(run) do
    Enum.all?([run.step, run.checkpoint_seq, run.event_seq], &(is_integer(&1) and &1 >= 0))
  end

  defp collections_valid?(run) do
    valid_channels?(run.channels) and
      id_set?(run.changed_channels) and
      id_set?(run.pending_nodes) and
      valid_tasks?(run, run.active_tasks) and
      valid_pending_writes?(run, run.pending_writes) and
      valid_interrupts?(run.interrupts) and
      valid_timers?(run.timers)
  end

  defp validate_active_superstep(run) do
    active_ids = run.active_tasks |> Map.keys() |> MapSet.new()
    timer_ids = run.timers |> Map.keys() |> MapSet.new()
    node_ids = Enum.map(Map.values(run.active_tasks) ++ run.pending_writes, & &1.node_id)

    has_superstep =
      map_size(run.active_tasks) > 0 or run.pending_writes != [] or map_size(run.timers) > 0

    cond do
      has_superstep and run.status != :running ->
        invalid_durable("active superstep state is only valid on a running run")

      active_ids != timer_ids ->
        invalid_durable("active task IDs must match retry timer IDs")

      run.pending_writes != [] and map_size(run.active_tasks) == 0 ->
        invalid_durable("pending writes require active tasks")

      length(node_ids) != MapSet.size(MapSet.new(node_ids)) ->
        invalid_durable("a node may appear only once in an active superstep")

      true ->
        :ok
    end
  end

  defp valid_id?(value), do: is_binary(value) and value != ""
  defp plain_map?(value), do: is_map(value) and not is_struct(value)

  defp portable_map?(value), do: plain_map?(value) and portable_value?(value)

  defp portable_value?(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: true

  defp portable_value?([]), do: true

  defp portable_value?([head | tail]),
    do: portable_value?(head) and portable_list?(tail)

  defp portable_value?(value) when is_map(value) and not is_struct(value) do
    Enum.all?(value, fn {key, child} -> is_binary(key) and portable_value?(child) end)
  end

  defp portable_value?(_value), do: false

  defp portable_list?([]), do: true

  defp portable_list?([head | tail]),
    do: portable_value?(head) and portable_list?(tail)

  defp portable_list?(_tail), do: false

  defp valid_channels?(channels) when is_map(channels) and not is_struct(channels) do
    Enum.all?(channels, fn
      {id, %ChannelState{} = channel} ->
        exact_struct?(channel, ChannelState) and valid_id?(id) and channel.channel_id == id and
          non_neg_integer?(channel.version) and
          string_list?(channel.barrier_seen) and portable_value?(channel.value)

      _other ->
        false
    end)
  end

  defp valid_channels?(_channels), do: false

  defp valid_tasks?(run, tasks) when is_map(tasks) and not is_struct(tasks) do
    Enum.all?(tasks, fn
      {task_id, %TaskState{} = task} -> valid_task?(run, task_id, task)
      _other -> false
    end)
  end

  defp valid_tasks?(_run, _tasks), do: false

  defp valid_task?(run, task_id, task) do
    failures = task.failures

    exact_struct?(task, TaskState) and valid_id?(task_id) and valid_id?(task.node_id) and
      task.task_id == task_id and
      task_id == TaskState.task_id(run.id, run.step, task.node_id) and task.step == run.step and
      task.status == :retry_scheduled and pos_integer?(task.attempt) and
      task.idempotency_key == TaskState.idempotency_key(task_id, task.attempt) and
      valid_id?(task.input_hash) and portable_map?(task.snapshot) and
      valid_source_versions?(task.source_versions) and valid_failures?(failures) and
      task.attempt == length(failures) + 1 and
      task.input_hash == TaskState.snapshot_hash(task.snapshot) and is_nil(task.started_at) and
      is_nil(task.deadline_at) and portable_map?(task.metadata)
  end

  defp valid_source_versions?(versions) when is_map(versions) and not is_struct(versions) do
    Enum.all?(versions, fn {id, version} -> valid_id?(id) and non_neg_integer?(version) end)
  end

  defp valid_source_versions?(_versions), do: false

  defp valid_failures?(failures) when is_list(failures) and failures != [] do
    if not proper_list?(failures) do
      false
    else
      Enum.with_index(failures, 1)
      |> Enum.all?(fn
        {%{attempt: attempt, reason: reason} = failure, attempt} ->
          Enum.sort(Map.keys(failure)) == [:attempt, :reason] and is_binary(reason)

        _other ->
          false
      end)
    end
  end

  defp valid_failures?(_failures), do: false

  defp valid_pending_writes?(run, pending) when is_list(pending) and pending != [] do
    proper_list?(pending) and Enum.all?(pending, &valid_pending_write?(run, &1))
  end

  defp valid_pending_writes?(_run, []), do: true

  defp valid_pending_writes?(_run, _pending), do: false

  defp valid_pending_write?(run, %PendingWrite{} = pending) do
    exact_struct?(pending, PendingWrite) and valid_id?(pending.node_id) and
      pending.task_id == TaskState.task_id(run.id, run.step, pending.node_id) and
      pos_integer?(pending.attempt) and valid_pending_value?(pending)
  end

  defp valid_pending_write?(_run, _pending), do: false

  defp valid_pending_value?(%PendingWrite{kind: :update, value: value}),
    do: portable_map?(value)

  defp valid_pending_value?(%PendingWrite{kind: :interrupt, node_id: node_id, value: value}),
    do: valid_interrupt_request?(value, node_id)

  defp valid_pending_value?(_pending), do: false

  defp valid_interrupt_request?(%Interrupt{} = interrupt, node_id) do
    exact_struct?(interrupt, Interrupt) and (is_nil(interrupt.id) or valid_id?(interrupt.id)) and
      interrupt.node_id in [nil, node_id] and optional_string?(interrupt.prompt) and
      valid_id?(interrupt.resume_channel) and valid_schema?(interrupt.schema) and
      portable_map?(interrupt.metadata)
  end

  defp valid_interrupt_request?(_interrupt, _node_id), do: false

  defp valid_interrupts?(interrupts) when is_map(interrupts) and not is_struct(interrupts) do
    Enum.all?(interrupts, fn
      {id, %InterruptState{} = interrupt} -> valid_interrupt_state?(id, interrupt)
      _other -> false
    end)
  end

  defp valid_interrupts?(_interrupts), do: false

  defp valid_interrupt_state?(id, interrupt) do
    exact_struct?(interrupt, InterruptState) and valid_id?(id) and interrupt.id == id and
      valid_id?(interrupt.node_id) and
      interrupt.status in [:open, :resolved] and valid_id?(interrupt.resume_channel) and
      optional_string?(interrupt.prompt) and valid_schema?(interrupt.schema) and
      valid_datetime?(interrupt.created_at) and interrupt_resolution_time?(interrupt) and
      portable_map?(interrupt.metadata)
  end

  defp interrupt_resolution_time?(%InterruptState{status: :open, resolved_at: nil}), do: true

  defp interrupt_resolution_time?(%InterruptState{status: :resolved, resolved_at: resolved_at}),
    do: valid_datetime?(resolved_at)

  defp interrupt_resolution_time?(_interrupt), do: false

  defp valid_timers?(timers) when is_map(timers) and not is_struct(timers) do
    Enum.all?(timers, fn
      {id, %TimerState{kind: :retry, fires_at: fires_at} = timer} ->
        exact_struct?(timer, TimerState) and valid_id?(id) and valid_datetime?(fires_at)

      _other ->
        false
    end)
  end

  defp valid_timers?(_timers), do: false

  defp valid_failure?(nil), do: true

  defp valid_failure?(%Failure{} = failure) do
    exact_struct?(failure, Failure) and valid_id?(failure.code) and valid_id?(failure.message) and
      optional_string?(failure.node_id) and
      portable_map?(failure.details)
  end

  defp valid_failure?(_failure), do: false

  defp valid_schema?(nil), do: true

  defp valid_schema?(%Schema{} = schema),
    do: exact_struct?(schema, Schema) and Schema.valid?(schema)

  defp valid_schema?(_schema), do: false

  defp optional_string?(nil), do: true
  defp optional_string?(value), do: is_binary(value)
  defp non_neg_integer?(value), do: is_integer(value) and value >= 0
  defp pos_integer?(value), do: is_integer(value) and value > 0

  defp string_list?(value),
    do: is_list(value) and proper_list?(value) and Enum.all?(value, &is_binary/1)

  defp id_set?(%MapSet{map: map} = set) when is_map(map) do
    exact_struct?(set, MapSet) and
      Enum.all?(map, fn {id, marker} -> is_binary(id) and marker == [] end)
  end

  defp id_set?(_set), do: false

  defp valid_datetime?(
         %DateTime{
           calendar: Calendar.ISO,
           time_zone: "Etc/UTC",
           zone_abbr: "UTC",
           utc_offset: 0,
           std_offset: 0
         } = datetime
       ),
       do: DurableCodec.valid_datetime?(datetime)

  defp valid_datetime?(_value), do: false

  defp proper_list?([]), do: true
  defp proper_list?([_head | tail]), do: proper_list?(tail)
  defp proper_list?(_tail), do: false

  defp exact_struct?(struct, module) do
    is_map(struct) and Map.get(struct, :__struct__) == module and
      MapSet.new(Map.keys(struct)) == MapSet.new(Map.keys(module.__struct__()))
  end

  defp invalid_durable(message), do: {:error, Docket.Error.new(:invalid_run, message)}
end
