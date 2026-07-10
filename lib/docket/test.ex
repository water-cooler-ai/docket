defmodule Docket.Test do
  @moduledoc """
  Inline test runtime: executes graph transitions in the calling process
  using the same loop, algorithm, validation, reducer, and
  checkpoint-building code as the supervised Runtime.

  The inline runtime is not a second interpreter - only the shell differs.
  Use it for graph semantics, checkpoint ordering, reducers, guards,
  interrupts, and failure policy. Supervised tests are reserved for process
  lifecycle, crash recovery, and task-executor behavior.

  ## Options

  All helpers accept:

  - `:checkpoint` - `Docket.Checkpoint` module (default
    `Docket.Test.Checkpoint.Accept`)
  - `:checkpoint_overrides` - map forcing checkpoint types to `:sync`
  - `:executor` - `Docket.Executor` module (default `Docket.Executor.Local`)
  - `:executor_opts` - keyword list passed through to the executor
  - `:context` - application context passed to nodes and checkpoint handlers
  - `:clock`, `:id_generator`, `:sleeper` - determinism injection points;
    the sleeper serves each committed retry park's wait, and the helpers
    then treat the parked deadline as reached without re-reading the clock
  - `:max_supersteps` - runtime default when the graph declares no policy
  - `:max_steps` - stop driving after this many committed supersteps
  - `:run_id` - explicit run ID for the fresh run document
  - `:metadata` - application metadata map for the fresh run document

  Async checkpoints are drained synchronously in order before each helper
  returns, so ordinary semantic tests can assert a complete checkpoint
  sequence without `Process.sleep/1`. A failed async delivery is skipped
  from the returned list without blocking execution, matching production
  semantics where async failure never rolls back the active run.

  Return shape for all helpers:

      {:ok, Docket.Run.t(), [Docket.Checkpoint.t()]}
      | {:error, Docket.Error.t(), [Docket.Checkpoint.t()]}
  """

  alias Docket.{Error, Run}
  alias Docket.Runtime.{Config, Dispatcher, Loop}

  @doc """
  Compiles (when given a `Docket.Graph`), builds a fresh run from `input`,
  initializes through the same durable barrier as `Docket.run/4`, and
  executes supersteps until the run is terminal, waiting, or the step limit
  is reached.
  """
  def run_inline(graph_or_runtime_graph, input, opts \\ []) do
    opts = normalize_opts(opts)

    with {:ok, rtg} <- ensure_compiled(graph_or_runtime_graph, opts) do
      run = Loop.build_initial_run(rtg, input, opts)
      start(rtg, run, opts)
    else
      {:error, %Error{} = error} -> {:error, error, []}
    end
  end

  @doc """
  Resumes a saved `Docket.Run` through the same durable barrier used by
  `run_inline/3`, then continues execution.

  Requires `graph.id == run.graph_id` and a matching graph hash. A terminal
  run is returned unchanged without restarting execution.
  """
  def resume_inline(graph_or_runtime_graph, %Run{} = run, opts \\ []) do
    opts = normalize_opts(opts)

    with {:ok, rtg} <- ensure_compiled(graph_or_runtime_graph, opts) do
      start(rtg, run, opts)
    else
      {:error, %Error{} = error} -> {:error, error, []}
    end
  end

  @doc """
  Drives exactly one committed transition and returns the updated run with
  the accepted checkpoints from that transition.

  Requires the graph via `:graph` or `:runtime_graph` in `opts` (a run
  document does not carry its graph). A `:created` run performs
  initialization only; a `:running` run commits one superstep; `:waiting`
  and terminal runs return unchanged.
  """
  def step_inline(%Run{} = run, opts \\ []) do
    opts = normalize_opts(opts)

    with {:ok, rtg} <- graph_from_opts(opts) do
      cond do
        run.status == :created ->
          case Loop.init(rtg, run, opts) do
            {:ok, run, effects} -> {:ok, run, deliver(effects, opts)}
            {:error, error} -> {:error, error, []}
          end

        Run.terminal?(run) ->
          {:ok, run, []}

        true ->
          step_tick(rtg, run, opts, nil)
      end
    else
      {:error, %Error{} = error} -> {:error, error, []}
    end
  end

  # Drives ticks until one committed transition (a barrier, retry park, or
  # terminal commit) has happened. A retry wait commits nothing; keep going
  # so step_inline always returns a commit.
  defp step_tick(rtg, run, opts, resume_floor) do
    case tick(rtg, run, opts, resume_floor) do
      {:continue, run, checkpoints} -> {:ok, run, checkpoints}
      {:continue_at, run, _resume_at, checkpoints} -> {:ok, run, checkpoints}
      {:retry_wait, run, resume_at} -> step_tick(rtg, run, opts, resume_at)
      {:stop, run, checkpoints} -> {:ok, run, checkpoints}
      {:error, error, checkpoints} -> {:error, error, checkpoints}
    end
  end

  @doc """
  Resolves an open interrupt inline, then continues driving the run.

  Requires the graph via `:graph` or `:runtime_graph` in `opts`.
  """
  def resolve_interrupt_inline(%Run{} = run, interrupt_id, value, opts \\ []) do
    opts = normalize_opts(opts)

    with {:ok, rtg} <- graph_from_opts(opts) do
      case Loop.resolve_interrupt(rtg, run, interrupt_id, value, opts) do
        {:ok, run, effects} -> drive(rtg, run, opts, deliver(effects, opts), 0)
        {:error, error} -> {:error, error, []}
      end
    else
      {:error, %Error{} = error} -> {:error, error, []}
    end
  end

  # ---------------------------------------------------------------------------
  # Shell drive loop
  # ---------------------------------------------------------------------------

  defp start(rtg, run, opts) do
    case Loop.init(rtg, run, opts) do
      {:ok, run, effects} -> drive(rtg, run, opts, deliver(effects, opts), 0)
      {:error, error} -> {:error, error, []}
    end
  end

  # `resume_floor` carries a served park deadline into the next plan: the
  # sleeper call stands in for real waiting, so deadline checks must not
  # depend on the wall clock having advanced. A retry park does not count
  # toward `max_steps` - the graph step does not advance.
  defp drive(rtg, run, opts, checkpoints, steps, resume_floor \\ nil) do
    max_steps = Keyword.get(opts, :max_steps)

    cond do
      Run.terminal?(run) ->
        {:ok, run, checkpoints}

      is_integer(max_steps) and steps >= max_steps ->
        {:ok, run, checkpoints}

      true ->
        case tick(rtg, run, opts, resume_floor) do
          {:continue, run, delivered} ->
            drive(rtg, run, opts, checkpoints ++ delivered, steps + 1)

          {:continue_at, run, resume_at, delivered} ->
            drive(rtg, run, opts, checkpoints ++ delivered, steps, resume_at)

          {:retry_wait, run, resume_at} ->
            drive(rtg, run, opts, checkpoints, steps, resume_at)

          {:stop, run, delivered} ->
            {:ok, run, checkpoints ++ delivered}

          {:error, error, delivered} ->
            {:error, error, checkpoints ++ delivered}
        end
    end
  end

  defp tick(rtg, run, opts, resume_floor) do
    config = Config.resolve(opts)
    opts = put_resume_floor(opts, resume_floor)

    case Loop.plan(rtg, run, opts) do
      {:execute, run, activations} ->
        results = Dispatcher.dispatch(activations, rtg, run, config)

        case Loop.apply_results(rtg, run, activations, results, opts) do
          {:ok, run, effects} ->
            {:continue, run, deliver(effects, opts)}

          {:park, run, park, effects} ->
            config.sleeper.(park.wait_ms)
            {:continue_at, run, park.resume_at, deliver(effects, opts)}

          {:error, error} ->
            {:error, error, []}
        end

      # An uncommitted retry wait: nothing durable changed, the sleeper just
      # served the remaining time to the earliest parked deadline.
      {:park, run, park} ->
        config.sleeper.(park.wait_ms)
        {:retry_wait, run, park.resume_at}

      {:wait, run, _interrupt_ids} ->
        {:stop, run, []}

      {:terminal, run, effects} ->
        {:stop, run, deliver(effects, opts)}

      {:error, error} ->
        {:error, error, []}
    end
  end

  defp put_resume_floor(opts, nil), do: Keyword.delete(opts, :resume_floor)
  defp put_resume_floor(opts, %DateTime{} = floor), do: Keyword.put(opts, :resume_floor, floor)

  # Sync checkpoints in effects were already accepted inside the loop; async
  # ones are drained here, in order, in the calling process.
  defp deliver(effects, opts) do
    config = Config.resolve(opts)

    Enum.flat_map(effects, fn
      {:checkpoint, checkpoint, _context, :accepted} ->
        [checkpoint]

      {:checkpoint, checkpoint, context, :pending} ->
        case Loop.deliver_checkpoint(config.checkpoint, checkpoint, context) do
          :ok -> [checkpoint]
          {:error, _reason} -> []
        end
    end)
  end

  # ---------------------------------------------------------------------------
  # Inputs
  # ---------------------------------------------------------------------------

  defp normalize_opts(opts) do
    Keyword.put_new(opts, :checkpoint, Docket.Test.Checkpoint.Accept)
  end

  defp ensure_compiled(graph_or_runtime_graph, opts) do
    Docket.ensure_compiled(graph_or_runtime_graph, opts)
  end

  defp graph_from_opts(opts) do
    case {Keyword.get(opts, :graph), Keyword.get(opts, :runtime_graph)} do
      {nil, nil} ->
        {:error,
         Error.new(
           :invalid_graph,
           "step/resolve helpers require :graph or :runtime_graph in opts"
         )}

      {nil, rtg} ->
        ensure_compiled(rtg, opts)

      {graph, _} ->
        ensure_compiled(graph, opts)
    end
  end
end
