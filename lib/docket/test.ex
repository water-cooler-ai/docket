defmodule Docket.Test do
  @moduledoc """
  Inline test runtime: executes graph transitions in the calling process
  using the same loop, algorithm, validation, reducer, and
  checkpoint-building code as backend execution vehicles.

  The inline runtime is not a second interpreter - only the driver differs.
  Use it for graph semantics, checkpoint ordering, reducers, guards,
  interrupts, and failure policy. Backend tests cover durable lifecycle,
  crash recovery, claims, scheduling, and supervised task execution.

  ## Options

  All helpers accept:

  - `:executor` - `Docket.Executor` module (default `Docket.Executor.Local`)
  - `:executor_opts` - keyword list passed through to the executor
  - `:context` - application context passed to nodes and test sinks
  - `:clock`, `:id_generator`, `:sleeper` - determinism injection points;
    the sleeper serves each committed retry park's wait, and the helpers
    then treat the parked deadline as reached without re-reading the clock
  - `:max_supersteps` - runtime default when the graph declares no policy
  - `:max_steps` - stop driving after this many committed supersteps
  - `:run_id` - explicit run ID for the fresh run document
  - `:metadata` - application metadata map for the fresh run document

  Checkpoints are returned in order so ordinary semantic tests can
  assert a complete transition sequence without `Process.sleep/1`.

  Return shape for all helpers:

      {:ok, Docket.Run.t(), [Docket.Checkpoint.t()]}
      | {:error, Docket.Error.t(), [Docket.Checkpoint.t()]}
  """

  alias Docket.{Error, Run}
  alias Docket.Runtime.{Config, Loop, Moment, RunMutation}

  @doc """
  Compiles (when given a `Docket.Graph`), builds a fresh run from `input`,
  initializes through the same processless transition barrier used by backend vehicles, and
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
          case Loop.propose_init(rtg, run, opts) do
            {:ok, moment} ->
              {run, checkpoints} = accept_moment(moment)
              {:ok, run, checkpoints}

            {:error, error} ->
              {:error, error, []}
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
      config = Config.resolve(opts)

      case RunMutation.resolve_interrupt(rtg, run, interrupt_id, value, config.clock.()) do
        {:ok, moment} ->
          {run, checkpoints} = accept_moment(moment)
          drive(rtg, run, opts, checkpoints, 0)

        {:error, error} ->
          {:error, error, []}
      end
    else
      {:error, %Error{} = error} -> {:error, error, []}
    end
  end

  # ---------------------------------------------------------------------------
  # Shell drive loop
  # ---------------------------------------------------------------------------

  defp start(rtg, run, opts) do
    case Loop.propose_init(rtg, run, opts) do
      {:ok, moment} ->
        {run, checkpoints} = accept_moment(moment)
        drive(rtg, run, opts, checkpoints, 0)

      {:terminal, run} ->
        {:ok, run, []}

      {:error, error} ->
        {:error, error, []}
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

    case Loop.propose_advance(rtg, run, opts) do
      {:ok, %Moment{} = moment} ->
        {run, checkpoints} = accept_moment(moment)

        case moment.disposition do
          :continue ->
            {:continue, run, checkpoints}

          {:park, :immediate, _reason} ->
            {:continue, run, checkpoints}

          {:park, {:at, resume_at}, _reason} ->
            wait_ms = max(DateTime.diff(resume_at, moment.run.updated_at, :millisecond), 0)
            config.sleeper.(wait_ms)
            {:continue_at, run, resume_at, checkpoints}

          {:park, :external, _reason} ->
            {:stop, run, checkpoints}

          {:park, :terminal, _reason} ->
            {:stop, run, checkpoints}
        end

      # An uncommitted retry wait: nothing durable changed, the sleeper just
      # served the remaining time to the earliest parked deadline.
      {:park, run, park} ->
        config.sleeper.(park.wait_ms)
        {:retry_wait, run, park.resume_at}

      {:wait, run, _interrupt_ids} ->
        {:stop, run, []}

      {:terminal, run} ->
        {:stop, run, []}

      {:error, error} ->
        {:error, error, []}
    end
  end

  defp put_resume_floor(opts, nil), do: Keyword.delete(opts, :resume_floor)
  defp put_resume_floor(opts, %DateTime{} = floor), do: Keyword.put(opts, :resume_floor, floor)

  # Processless drivers accept the same pre-commit moment shape as durable
  # drivers, then expose its checkpoint as a read-only assertion value.
  defp accept_moment(%Moment{} = moment) do
    Docket.Telemetry.emit_events(moment.run, moment.events)
    {moment.run, [Moment.checkpoint(moment)]}
  end

  # ---------------------------------------------------------------------------
  # Inputs
  # ---------------------------------------------------------------------------

  defp normalize_opts(opts) do
    opts
  end

  defp ensure_compiled(graph_or_runtime_graph, opts) do
    with {:ok, rtg} <- Docket.ensure_compiled(graph_or_runtime_graph, opts),
         config = Config.resolve(opts),
         :ok <- Docket.Runtime.ExecutionPolicy.validate_graph(rtg, config.max_attempt_elapsed_ms) do
      {:ok, rtg}
    end
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
