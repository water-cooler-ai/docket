defmodule Docket.Runtime.Superstep do
  @moduledoc false

  # Settlement state for one dispatched superstep attempt, between the
  # dispatcher returning task results and the loop proposing the attempt's
  # commit boundary.
  #
  # `new/5` partitions the dispatch's results and runs every write and
  # interrupt validator exactly once; the loop pattern matches the struct's
  # fields to pick the boundary (fail, park, or barrier) instead of
  # re-deriving lists. `absorb_pending/1` folds the run's durable pending
  # sibling writes through the same validators at the barrier, so each result
  # batch is validated once per proposal no matter which boundary it reaches.

  alias Docket.{Run, Schema}
  alias Docket.Run.PendingWrite
  alias Docket.Runtime.{Activation, Algorithm, Config, TaskResult}

  defmodule Failure do
    @moduledoc false

    # Permanent failure of one node attempt: an `:error` result, an invalid
    # state update, or an invalid interrupt request. Always fails the run.

    @enforce_keys [:node_id, :task_id, :attempt, :reason]
    defstruct [:node_id, :task_id, :attempt, :reason]

    @type t :: %__MODULE__{
            node_id: String.t(),
            task_id: String.t(),
            attempt: pos_integer(),
            reason: term()
          }

    def new(%TaskResult{} = result, reason) do
      %__MODULE__{
        node_id: result.node_id,
        task_id: result.task_id,
        attempt: result.attempt,
        reason: reason
      }
    end
  end

  @enforce_keys [:rtg, :run, :config]
  defstruct [
    :rtg,
    :run,
    :config,
    activations: [],
    results: [],
    writes: [],
    interrupts: [],
    retries: [],
    failures: [],
    remaining_active?: false
  ]

  @type write :: {TaskResult.t(), map()}
  @type interrupt_spec :: {TaskResult.t(), Docket.Interrupt.t()}

  @type t :: %__MODULE__{
          rtg: Docket.Runtime.Graph.t(),
          run: Run.t(),
          config: Config.t(),
          activations: [Activation.t()],
          results: [TaskResult.t()],
          writes: [write()],
          interrupts: [interrupt_spec()],
          retries: [TaskResult.t()],
          failures: [Failure.t()],
          remaining_active?: boolean()
        }

  @doc false
  def new(rtg, run, config, activations, results) do
    results = Enum.sort_by(results, & &1.node_id)
    {oks, interrupt_results, retries, failures} = partition(results)
    {writes, write_failures} = validate_writes(rtg, oks)
    {interrupts, interrupt_failures} = validate_interrupts(rtg, run, interrupt_results)

    %__MODULE__{
      rtg: rtg,
      run: run,
      config: config,
      activations: activations,
      results: results,
      writes: writes,
      interrupts: interrupts,
      retries: retries,
      failures: failures ++ write_failures ++ interrupt_failures,
      remaining_active?: remaining_active?(run, results)
    }
  end

  @doc false
  # Folds the run's durable pending sibling writes (parked by earlier retry
  # commits) into this settlement's validated writes and interrupts, sorted
  # by node ID. Validation is deterministic, so runtime-produced pending
  # state validates identically to when it was parked; only corrupted durable
  # state fails here, and it fails through the same typed failure vocabulary
  # as fresh results instead of crashing the commit.
  def absorb_pending(%__MODULE__{} = superstep) do
    {oks, interrupt_results, [], []} = superstep.run |> rehydrate_pending() |> partition()
    {writes, write_failures} = validate_writes(superstep.rtg, oks)

    {interrupts, interrupt_failures} =
      validate_interrupts(superstep.rtg, superstep.run, interrupt_results)

    case write_failures ++ interrupt_failures do
      [] ->
        {:ok,
         %{
           superstep
           | writes: merge_by_node(writes, superstep.writes),
             interrupts: merge_by_node(interrupts, superstep.interrupts)
         }}

      failures ->
        {:error, %{superstep | failures: failures}}
    end
  end

  @doc false
  # Serializes this dispatch's finalized results into durable pending writes,
  # sorted by node ID. Interrupt values carry their node identity so the
  # parked write round-trips through `absorb_pending/1` unchanged.
  def to_pending_writes(%__MODULE__{} = superstep) do
    Enum.sort_by(
      Enum.map(superstep.writes, fn {result, update} ->
        pending_write(result, :update, update)
      end) ++
        Enum.map(superstep.interrupts, fn {result, interrupt} ->
          pending_write(result, :interrupt, %{interrupt | node_id: result.node_id})
        end),
      & &1.node_id
    )
  end

  defp pending_write(result, kind, value) do
    %PendingWrite{
      task_id: result.task_id,
      node_id: result.node_id,
      attempt: result.attempt,
      kind: kind,
      value: value
    }
  end

  # Rehydrates pending sibling results committed by earlier retry parks back
  # into task results. Their retried attempts' failure events were emitted by
  # the parks that recorded them, so they contribute no failure entries at
  # the barrier.
  defp rehydrate_pending(run) do
    Enum.map(run.pending_writes, fn %PendingWrite{} = pending ->
      %TaskResult{
        task_id: pending.task_id,
        node_id: pending.node_id,
        attempt: pending.attempt,
        status: if(pending.kind == :update, do: :ok, else: :interrupt),
        value: pending.value
      }
    end)
  end

  # Pending results merge ahead of fresh results; the sort is stable and
  # keyed by node ID, so merged order is deterministic.
  defp merge_by_node(pending, fresh) do
    Enum.sort_by(pending ++ fresh, fn {result, _value} -> result.node_id end)
  end

  # True when active tasks beyond this dispatch's results remain parked
  # (their retry deadlines were not due), so the barrier cannot commit yet.
  defp remaining_active?(run, results) do
    dispatched = MapSet.new(results, & &1.task_id)
    run.active_tasks |> Map.keys() |> Enum.any?(&(not MapSet.member?(dispatched, &1)))
  end

  defp partition(results) do
    Enum.reduce(results, {[], [], [], []}, fn result, {oks, interrupts, retries, failures} ->
      case result.status do
        :ok ->
          {[result | oks], interrupts, retries, failures}

        :interrupt ->
          {oks, [result | interrupts], retries, failures}

        :retry ->
          {oks, interrupts, [result | retries], failures}

        :error ->
          {oks, interrupts, retries, [Failure.new(result, result.value) | failures]}
      end
    end)
    |> then(fn {oks, interrupts, retries, failures} ->
      {Enum.reverse(oks), Enum.reverse(interrupts), Enum.reverse(retries), Enum.reverse(failures)}
    end)
  end

  defp validate_writes(rtg, oks) do
    Enum.reduce(oks, {[], []}, fn result, {writes, failures} ->
      case Algorithm.validate_state_update(rtg, result.node_id, result.value) do
        {:ok, update} ->
          {[{result, update} | writes], failures}

        {:error, reasons} ->
          {writes, [Failure.new(result, {:invalid_state_update, reasons}) | failures]}
      end
    end)
    |> then(fn {writes, failures} -> {Enum.reverse(writes), Enum.reverse(failures)} end)
  end

  defp validate_interrupts(rtg, run, interrupt_results) do
    Enum.reduce(interrupt_results, {[], []}, fn result, {specs, failures} ->
      interrupt = result.value

      case interrupt_errors(rtg, run, interrupt) do
        [] ->
          {[{result, interrupt} | specs], failures}

        reasons ->
          {specs, [Failure.new(result, {:invalid_interrupt, reasons}) | failures]}
      end
    end)
    |> then(fn {specs, failures} -> {Enum.reverse(specs), Enum.reverse(failures)} end)
  end

  defp interrupt_errors(rtg, run, interrupt) do
    check_resume_channel(rtg, interrupt) ++
      check_schema(interrupt) ++
      check_id_unused(run, interrupt)
  end

  defp check_resume_channel(rtg, interrupt) do
    if Map.has_key?(rtg.lowering.public_to_runtime.fields, interrupt.resume_channel || "") do
      []
    else
      [
        "interrupt resume_channel #{inspect(interrupt.resume_channel)} is not a declared state field"
      ]
    end
  end

  defp check_schema(interrupt) do
    case interrupt.schema do
      nil -> []
      %Schema{} -> []
      other -> ["interrupt schema must be a Docket.Schema or nil, got #{inspect(other)}"]
    end
  end

  defp check_id_unused(run, interrupt) do
    if interrupt.id != nil and Map.has_key?(run.interrupts, interrupt.id) do
      ["interrupt id #{inspect(interrupt.id)} already exists on this run"]
    else
      []
    end
  end
end
