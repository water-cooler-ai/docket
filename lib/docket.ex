defmodule Docket do
  @moduledoc """
  Public entry point for running graphs under a supervised runtime.

  A runtime instance is one `Docket.Runtime.Supervisor` tree, identified by
  the name it was started under. Hosts usually define one with `use Docket`:

      defmodule MyApp.Docket do
        use Docket, checkpoint: MyApp.DocketCheckpoint
      end

      # in the application supervision tree
      children = [MyApp.Docket]

      graph = MyApp.Graphs.fetch!("essay-review")
      {:ok, run} = MyApp.Docket.run(graph, %{"topic" => "Durable graphs"})
      {:ok, current} = MyApp.Docket.get_run(run.id)

  All run operations address the runtime instance plus `run_id`; Runtime
  PIDs never leave the library. `run/4` is a start barrier, not a completion
  barrier: it returns once the run is durably initialized through the
  required `:run_initialized` checkpoint, while execution continues in the
  owning `Docket.Runtime` process. Progress is observed through checkpoints
  (the durable truth) or `get_run/3` (the live in-memory snapshot).

  Options accepted by `run/4` and `resume/4` are the shared runtime options
  (see `Docket.Test` for the full list: `:checkpoint`, `:executor`,
  `:context`, determinism injection points, limits) plus `:run_id` /
  `:metadata` for fresh runs. Options given to the runtime instance at
  startup act as defaults; per-call options win.

  For processless in-test execution of the same loop, use `Docket.Test`.
  """

  alias Docket.{Error, Graph, Run}
  alias Docket.Runtime.Loop
  alias Docket.Runtime.Registry, as: RuntimeRegistry

  @doc """
  Child spec integration point: `{Docket, name: MyRuntime, checkpoint: ...}`
  starts a `Docket.Runtime.Supervisor` tree.
  """
  def child_spec(opts) do
    opts
    |> Docket.Runtime.Supervisor.child_spec()
    |> Map.put(:id, Keyword.get(opts, :name, Docket.Runtime.Supervisor))
  end

  @doc """
  Compiles `graph`, builds a fresh `Docket.Run` from `input`, and starts a
  supervised `Docket.Runtime` owning it.

  Returns `{:ok, run}` after the sync `:run_initialized` checkpoint is
  accepted and the first tick is scheduled; no node has executed yet when
  this returns. Input validation failures return
  `{:error, %Docket.Error{type: :invalid_input}}` before anything durable is
  written, and initial checkpoint failures return
  `{:error, %Docket.Error{type: :checkpoint_failed}}` with no runtime left
  registered.
  """
  def run(runtime, graph, input, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, rtg} <- ensure_compiled(graph, opts) do
      run = Loop.build_initial_run(rtg, input, opts)
      start_runtime(runtime, rtg, run, opts)
    end
  end

  @doc """
  Resumes a saved `Docket.Run` under a supervised runtime.

  Requires `graph.id == run.graph_id` and a graph whose computed hash
  matches `run.graph_hash`; a mismatch returns
  `{:error, %Docket.Error{type: :graph_mismatch}}`. A terminal run is
  returned unchanged without starting a runtime. Resume passes through the
  same `Docket.Runtime.Loop.init/3` durable barrier as `run/4`, so the
  checkpoint handler upserts the host run record by ID.
  """
  def resume(runtime, graph, %Run{} = run, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, rtg} <- ensure_compiled(graph, opts),
         :ok <- check_graph_match(rtg, run) do
      if Run.terminal?(run) do
        {:ok, run}
      else
        start_runtime(runtime, rtg, run, opts)
      end
    end
  end

  @doc """
  Reads the current in-memory `Docket.Run` snapshot from the active Runtime.

  Observational: it does not read host storage and does not emit a
  checkpoint; the latest accepted checkpoint remains the durable source of
  truth. When no active Runtime owns `run_id` (never started, finished, or
  evicted), returns `{:error, %Docket.Error{type: :not_found}}`.
  """
  def get_run(runtime, run_id, opts \\ []) do
    _ = opts

    call_active(runtime, run_id, fn pid -> Docket.Runtime.get_run(pid) end)
  end

  @doc """
  Resolves an open interrupt on an active run and schedules the next tick.

  Returns `{:ok, run}` after the sync `:interrupt_resolved` checkpoint is
  accepted. Unknown or already-resolved interrupts return
  `{:error, %Docket.Error{type: :not_found}}`; so do runs with no active
  Runtime. Authorization is host-owned and must happen before this call.
  """
  def resolve_interrupt(runtime, run_id, interrupt_id, value, opts \\ []) do
    _ = opts

    call_active(runtime, run_id, fn pid ->
      Docket.Runtime.resolve_interrupt(pid, interrupt_id, value)
    end)
  end

  @doc """
  Defines a host runtime module wrapping a named runtime instance:

      defmodule MyApp.Docket do
        use Docket, checkpoint: MyApp.DocketCheckpoint
      end

  The options become the instance's default run options. The host module
  gets `child_spec/1`, `start_link/1`, and `run/3`, `resume/3`, `get_run/2`,
  `resolve_interrupt/4` wrappers that call the `Docket` functions with the
  module as the runtime instance.
  """
  defmacro __using__(default_opts) do
    quote bind_quoted: [default_opts: default_opts] do
      @docket_default_opts default_opts

      def child_spec(overrides \\ []) do
        Docket.child_spec(__docket_instance_opts__(overrides))
      end

      def start_link(overrides \\ []) do
        Docket.Runtime.Supervisor.start_link(__docket_instance_opts__(overrides))
      end

      defp __docket_instance_opts__(overrides) do
        @docket_default_opts
        |> Keyword.merge(overrides)
        |> Keyword.put(:name, __MODULE__)
      end

      def run(graph, input, opts \\ []), do: Docket.run(__MODULE__, graph, input, opts)

      def resume(graph, run, opts \\ []), do: Docket.resume(__MODULE__, graph, run, opts)

      def get_run(run_id, opts \\ []), do: Docket.get_run(__MODULE__, run_id, opts)

      def resolve_interrupt(run_id, interrupt_id, value, opts \\ []) do
        Docket.resolve_interrupt(__MODULE__, run_id, interrupt_id, value, opts)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Shared graph compilation
  # ---------------------------------------------------------------------------

  @doc false
  # Accepts a canonical graph (compiled through the real compiler pipeline)
  # or a precompiled runtime graph. Shared with Docket.Test so both entry
  # points verify graphs identically.
  def ensure_compiled(%Graph{} = graph, opts) do
    compile_opts = [profile: :run] ++ Keyword.take(opts, [:max_supersteps])

    case Docket.Graph.Compiler.compile(graph, compile_opts) do
      {:ok, rtg} ->
        {:ok, rtg}

      {:error, %Graph{} = failed} ->
        {:error,
         Error.new(:invalid_graph, "graph #{inspect(graph.id)} failed verification",
           details: %{diagnostics: failed.diagnostics}
         )}
    end
  end

  def ensure_compiled(%Docket.Runtime.Graph{} = rtg, _opts), do: {:ok, rtg}

  def ensure_compiled(other, _opts) do
    {:error,
     Error.new(
       :invalid_graph,
       "expected a Docket.Graph or Docket.Runtime.Graph, got #{inspect(other)}"
     )}
  end

  # ---------------------------------------------------------------------------
  # Runtime instance plumbing
  # ---------------------------------------------------------------------------

  defp instance_opts(runtime, opts) do
    case RuntimeRegistry.defaults(runtime) do
      {:ok, defaults} ->
        task_supervisor = Docket.Runtime.Supervisor.task_supervisor(runtime)

        merged =
          defaults
          |> Keyword.merge(opts)
          |> Keyword.put(:task_supervisor, task_supervisor)
          |> Keyword.update(
            :executor_opts,
            [task_supervisor: task_supervisor],
            &Keyword.put_new(&1, :task_supervisor, task_supervisor)
          )

        {:ok, merged}

      :error ->
        {:error,
         Error.new(
           :runtime_unavailable,
           "runtime instance #{inspect(runtime)} is not started"
         )}
    end
  end

  defp check_graph_match(rtg, run) do
    if run.graph_id == rtg.graph_id and run.graph_hash == rtg.graph_hash do
      :ok
    else
      {:error,
       Error.new(:graph_mismatch, "run #{inspect(run.id)} does not match the supplied graph",
         details: %{
           run_graph_id: run.graph_id,
           run_graph_hash: run.graph_hash,
           graph_id: rtg.graph_id,
           graph_hash: rtg.graph_hash
         }
       )}
    end
  end

  defp start_runtime(runtime, rtg, run, opts) do
    reply_ref = make_ref()
    opts = Keyword.put(opts, :name, RuntimeRegistry.via(runtime, run.id))
    spec = {Docket.Runtime, {rtg, run, opts, {self(), reply_ref}}}

    case DynamicSupervisor.start_child(Docket.Runtime.Supervisor.run_supervisor(runtime), spec) do
      {:ok, _pid} ->
        # The initialized run was sent before Runtime.init returned, so it is
        # already in (or arriving to) this mailbox; the timeout only guards
        # against a Docket bug.
        receive do
          {^reply_ref, {:ok, run}} -> {:ok, run}
        after
          5_000 ->
            {:error,
             Error.new(
               :runtime_start_failed,
               "runtime for run #{inspect(run.id)} started but did not report initialization"
             )}
        end

      {:error, {:already_started, _pid}} ->
        {:error,
         Error.new(
           :already_active,
           "run #{inspect(run.id)} is already owned by an active runtime"
         )}

      {:error, {:shutdown, %Error{} = error}} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.new(
           :runtime_start_failed,
           "runtime for run #{inspect(run.id)} failed to start",
           reason: reason
         )}
    end
  end

  defp call_active(runtime, run_id, fun) do
    with {:ok, pid} <- lookup(runtime, run_id) do
      try do
        fun.(pid)
      catch
        # The runtime finished or was evicted between lookup and call.
        :exit, _reason -> not_found(run_id)
      end
    end
  end

  defp lookup(runtime, run_id) do
    case RuntimeRegistry.whereis(runtime, run_id) do
      {:ok, pid} -> {:ok, pid}
      :error -> not_found(run_id)
    end
  end

  defp not_found(run_id) do
    {:error, Error.new(:not_found, "no active runtime owns run #{inspect(run_id)}")}
  end
end
