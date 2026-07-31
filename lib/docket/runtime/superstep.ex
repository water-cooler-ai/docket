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
  #
  # Validated writes and interrupts are held as `PendingWrite` structs — the
  # same shape the retry park persists — so parking and absorbing durable
  # pending writes need no translation.

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

    # Accepts any attempt-identified source (`TaskResult` or `PendingWrite`).
    def new(%{node_id: node_id, task_id: task_id, attempt: attempt}, reason) do
      %__MODULE__{
        node_id: node_id,
        task_id: task_id,
        attempt: attempt,
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
    detached: [],
    failures: [],
    remaining_active?: false
  ]

  @type t :: %__MODULE__{
          rtg: Docket.Runtime.Graph.t(),
          run: Run.t(),
          config: Config.t(),
          activations: [Activation.t()],
          results: [TaskResult.t()],
          writes: [PendingWrite.t()],
          interrupts: [PendingWrite.t()],
          retries: [TaskResult.t()],
          detached: [TaskResult.t()],
          failures: [Failure.t()],
          remaining_active?: boolean()
        }

  @doc false
  def new(rtg, run, config, activations, results) do
    results = Enum.sort_by(results, & &1.node_id)
    {oks, interrupt_results, retries, detached, failures} = partition(results)
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
      detached: detached,
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
    {updates, parked_interrupts} =
      Enum.split_with(superstep.run.pending_writes, &(&1.kind == :update))

    {writes, write_failures} = validate_writes(superstep.rtg, updates)

    {interrupts, interrupt_failures} =
      validate_interrupts(superstep.rtg, superstep.run, parked_interrupts)

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
  # This dispatch's finalized writes and interrupts as one node-sorted list
  # for the retry park.
  def to_pending_writes(%__MODULE__{} = superstep) do
    Enum.sort_by(superstep.writes ++ superstep.interrupts, & &1.node_id)
  end

  defp pending_write(source, kind, value) do
    %PendingWrite{
      task_id: source.task_id,
      node_id: source.node_id,
      attempt: source.attempt,
      kind: kind,
      value: value
    }
  end

  # Pending writes merge ahead of fresh writes; the sort is stable and
  # keyed by node ID, so merged order is deterministic.
  defp merge_by_node(pending, fresh) do
    Enum.sort_by(pending ++ fresh, & &1.node_id)
  end

  # True when active tasks beyond this dispatch's results remain parked
  # (their retry deadlines were not due), so the barrier cannot commit yet.
  defp remaining_active?(run, results) do
    dispatched = MapSet.new(results, & &1.task_id)
    run.active_tasks |> Map.keys() |> Enum.any?(&(not MapSet.member?(dispatched, &1)))
  end

  defp partition(results) do
    Enum.reduce(results, {[], [], [], [], []}, fn result,
                                                  {oks, interrupts, retries, detached, failures} ->
      case result.status do
        :ok ->
          {[result | oks], interrupts, retries, detached, failures}

        :interrupt ->
          {oks, [result | interrupts], retries, detached, failures}

        :retry ->
          {oks, interrupts, [result | retries], detached, failures}

        :detached ->
          {oks, interrupts, retries, [result | detached], failures}

        :error ->
          {oks, interrupts, retries, detached, [Failure.new(result, result.value) | failures]}
      end
    end)
    |> then(fn {oks, interrupts, retries, detached, failures} ->
      {Enum.reverse(oks), Enum.reverse(interrupts), Enum.reverse(retries), Enum.reverse(detached),
       Enum.reverse(failures)}
    end)
  end

  # Both validators accept fresh `TaskResult`s and durable `PendingWrite`s:
  # each carries the attempt identity and value the checks and emitted
  # `PendingWrite`s need.
  defp validate_writes(rtg, oks) do
    Enum.reduce(oks, {[], []}, fn source, {writes, failures} ->
      case Algorithm.validate_state_update(rtg, source.node_id, source.value) do
        {:ok, update} ->
          {[pending_write(source, :update, update) | writes], failures}

        {:error, reasons} ->
          {writes, [Failure.new(source, {:invalid_state_update, reasons}) | failures]}
      end
    end)
    |> then(fn {writes, failures} -> {Enum.reverse(writes), Enum.reverse(failures)} end)
  end

  # Interrupt values are stamped with their node identity here, so parked
  # writes round-trip through `absorb_pending/1` unchanged.
  defp validate_interrupts(rtg, run, interrupt_sources) do
    Enum.reduce(interrupt_sources, {[], []}, fn source, {writes, failures} ->
      interrupt = %{source.value | node_id: source.node_id}

      case interrupt_errors(rtg, run, interrupt) do
        [] ->
          {[pending_write(source, :interrupt, interrupt) | writes], failures}

        reasons ->
          {writes, [Failure.new(source, {:invalid_interrupt, reasons}) | failures]}
      end
    end)
    |> then(fn {writes, failures} -> {Enum.reverse(writes), Enum.reverse(failures)} end)
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
