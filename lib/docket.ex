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

  Durable hosts instead configure one backend bundle, save graph versions
  explicitly, and use the operational API (`start_run`, storage-backed reads,
  named signals, and `await_run`):

      defmodule MyApp.DurableDocket do
        use Docket,
          backend: MyApp.DocketBackend,
          tenant_mode: :required,
          checkpoint_observers: [MyApp.DocketObserver]
      end

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
  startup act as defaults; per-call options win except for the instance-owned
  `:backend` and `:tenant_mode` boundaries. Public durable calls resolve only
  `:tenantless` or an explicit `{:tenant, id}`; `:system` is reserved for
  internal dispatch/recovery.

  For processless in-test execution of the same loop, use `Docket.Test`.
  """

  alias Docket.{Error, Graph, GraphRef, Lifecycle, Run, RunInfo}
  alias Docket.Runtime.Loop
  alias Docket.Runtime.RunMutation
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
  Compiles `graph` (a `Docket.Graph`; a precompiled `Docket.Runtime.Graph`
  is accepted as-is), builds a fresh `Docket.Run` from `input`, and starts a
  supervised `Docket.Runtime` owning it.

  Returns `{:ok, run}` after the sync `:run_initialized` checkpoint is
  accepted. The returned run is the initialized pre-execution snapshot;
  execution proceeds concurrently in the Runtime process and may already be
  underway when this returns. Input validation failures return
  `{:error, %Docket.Error{type: :invalid_input}}` before anything durable is
  written, and initial checkpoint failures return
  `{:error, %Docket.Error{type: :checkpoint_failed}}` with no runtime left
  registered.
  """
  def run(runtime, graph, input, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         :ok <- legacy_driver(opts),
         {:ok, rtg} <- ensure_compiled(graph, opts) do
      run = Loop.build_initial_run(rtg, input, opts)
      start_runtime(runtime, rtg, run, opts)
    end
  end

  @doc """
  Saves one effective, canonical, content-addressed graph version.

  Publication snapshots each node implementation's configuration schema once
  and materializes its defaults into the canonical document before hashing.
  Storage keeps that effective document; execution loads and compiles it on
  the node that performs the work.
  """
  def save_graph(runtime, graph, opts \\ [])

  def save_graph(runtime, %Graph{} = graph, opts) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context}} <- configured_backend(opts),
         {:ok, effective, rtg} <- compile_for_publication(graph),
         {:ok, :saved} <-
           backend.storage().transaction(context, fn tx ->
             case backend.graphs().save_graph(
                    tx,
                    rtg.graph_id,
                    rtg.graph_hash,
                    Graph.to_map(effective)
                  ) do
               :ok -> {:ok, :saved}
               {:error, reason} -> {:error, reason}
             end
           end) do
      {:ok, %GraphRef{graph_id: rtg.graph_id, graph_hash: rtg.graph_hash}}
    end
  end

  def save_graph(_runtime, graph, _opts) do
    {:error, Error.new(:invalid_graph, "expected a Docket.Graph, got #{inspect(graph)}")}
  end

  defp compile_for_publication(graph) do
    case Docket.Graph.Compiler.compile_for_publication(graph, profile: :publish) do
      {:ok, effective, rtg} ->
        {:ok, effective, rtg}

      {:error, %Graph{} = failed} ->
        {:error,
         Error.new(:invalid_graph, "graph #{inspect(graph.id)} failed verification",
           details: %{diagnostics: failed.diagnostics}
         )}
    end
  end

  @doc """
  Starts a run from a previously saved graph reference.

  The effective graph is fetched, validated against local node contracts, and
  compiled without injecting defaults introduced after publication. The
  initialized run and assigned events then commit atomically. Durable
  checkpoint observers run only after that commit; starting a run never
  publishes or changes a graph document.
  """
  def start_run(runtime, graph_ref, input, opts \\ [])

  def start_run(runtime, %GraphRef{} = graph_ref, input, opts) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context} = backend_ref, scope} <- durable_access(opts),
         {:ok, document} <-
           backend.graphs().fetch_graph(
             context,
             graph_ref.graph_id,
             graph_ref.graph_hash
           ),
         {:ok, graph} <- Graph.from_map(document),
         {:ok, rtg} <- ensure_compiled_effective(graph, opts),
         :ok <- check_graph_ref(rtg, graph_ref),
         run = Loop.build_initial_run(rtg, input, opts),
         {:ok, moment} <- Loop.propose_init(rtg, run, opts),
         {:ok, moment} <- Lifecycle.start(backend_ref, scope, moment) do
      :ok = Lifecycle.after_commit(moment, opts)
      {:ok, moment.run}
    end
  end

  def start_run(_runtime, graph_ref, _input, _opts) do
    {:error,
     Error.new(
       :invalid_graph_reference,
       "durable start requires a Docket.GraphRef, got #{inspect(graph_ref)}"
     )}
  end

  @doc """
  Resumes a saved `Docket.Run` under a supervised runtime.

  Requires `graph.id == run.graph_id` and a graph whose computed hash
  matches `run.graph_hash`; a mismatch returns
  `{:error, %Docket.Error{type: :graph_mismatch}}`. A terminal run is
  returned unchanged without starting a runtime. Resume passes through the
  same durable initialization barrier as `run/4`, so the checkpoint handler
  upserts the host run record by ID.
  """
  def resume(runtime, graph, %Run{} = run, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         :ok <- legacy_driver(opts),
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
  Resolves an open interrupt and schedules the next tick or durable wake.

  Returns `{:ok, run}` after the sync `:interrupt_resolved` checkpoint is
  accepted. Unknown or already-resolved interrupts return
  `{:error, %Docket.Error{type: :not_found}}`; so do runs with no active
  Runtime. With `backend:` configured, the stored effective canonical graph is
  loaded and compiled on the executing node without injecting new defaults,
  the pure mutation and its events commit atomically, and tenant scope is enforced before storage access.
  Authorization remains host-owned.
  """
  def resolve_interrupt(runtime, run_id, interrupt_id, value, opts \\ []) do
    case RuntimeRegistry.defaults(runtime) do
      {:ok, defaults} when is_list(defaults) ->
        if Keyword.has_key?(defaults, :backend) do
          with {:ok, resolved} <- instance_opts(runtime, opts) do
            durable_resolve_interrupt(resolved, run_id, interrupt_id, value)
          end
        else
          call_active(runtime, run_id, fn pid ->
            Docket.Runtime.resolve_interrupt(pid, interrupt_id, value)
          end)
        end

      :error ->
        call_active(runtime, run_id, fn pid ->
          Docket.Runtime.resolve_interrupt(pid, interrupt_id, value)
        end)
    end
  end

  @doc "Cancels a durable active run and confirms the committed terminal state."
  def cancel_run(runtime, run_id, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, backend, scope} <- durable_access(opts),
         now = operation_now(opts),
         result <-
           Lifecycle.signal(backend, scope, run_id, fn run ->
             RunMutation.cancel_run(run, now)
           end) do
      finish_signal(result, opts)
    end
  end

  @doc "Clears a non-terminal run's poison state and schedules it immediately."
  def retry_poisoned_run(runtime, run_id, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.runs().retry_poisoned_run(context, scope, run_id, operation_now(opts))
    end
  end

  @doc "Reads the last committed durable `Docket.Run`."
  def fetch_run(runtime, run_id, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.runs().fetch_run(context, scope, run_id)
    end
  end

  @doc "Reads a durable run plus token-free operational state."
  def inspect_run(runtime, run_id, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.runs().inspect_run(context, scope, run_id)
    end
  end

  @doc """
  Polls durable operational state until waiting, terminal, poisoned, or timeout.

  `:timeout` is required and expressed in milliseconds. `:poll_interval`
  defaults to 50 milliseconds.
  """
  def await_run(runtime, run_id, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, backend, scope} <- durable_access(opts),
         {:ok, timeout, poll_interval} <- await_options(opts) do
      deadline = System.monotonic_time(:millisecond) + timeout
      do_await_run(backend, scope, run_id, deadline, poll_interval)
    end
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

      def start_run(graph_ref, input, opts \\ []) do
        Docket.start_run(__MODULE__, graph_ref, input, opts)
      end

      def save_graph(graph, opts \\ []), do: Docket.save_graph(__MODULE__, graph, opts)

      def resume(graph, run, opts \\ []), do: Docket.resume(__MODULE__, graph, run, opts)

      def get_run(run_id, opts \\ []), do: Docket.get_run(__MODULE__, run_id, opts)

      def resolve_interrupt(run_id, interrupt_id, value, opts \\ []) do
        Docket.resolve_interrupt(__MODULE__, run_id, interrupt_id, value, opts)
      end

      def cancel_run(run_id, opts \\ []), do: Docket.cancel_run(__MODULE__, run_id, opts)

      def retry_poisoned_run(run_id, opts \\ []) do
        Docket.retry_poisoned_run(__MODULE__, run_id, opts)
      end

      def fetch_run(run_id, opts \\ []), do: Docket.fetch_run(__MODULE__, run_id, opts)
      def inspect_run(run_id, opts \\ []), do: Docket.inspect_run(__MODULE__, run_id, opts)
      def await_run(run_id, opts \\ []), do: Docket.await_run(__MODULE__, run_id, opts)
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

  @doc false
  def ensure_compiled_effective(%Graph{} = graph, opts) do
    compile_opts = [profile: :run] ++ Keyword.take(opts, [:max_supersteps])

    case Docket.Graph.Compiler.compile_effective_document(graph, compile_opts) do
      {:ok, rtg} ->
        {:ok, rtg}

      {:error, %Graph{} = failed} ->
        {:error,
         Error.new(:invalid_graph, "graph #{inspect(graph.id)} failed verification",
           details: %{diagnostics: failed.diagnostics}
         )}
    end
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
          |> preserve_instance_option(defaults, :backend)
          |> preserve_instance_option(defaults, :backend_context)
          |> preserve_instance_option(defaults, :tenant_mode)
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

  defp preserve_instance_option(opts, defaults, key) do
    case Keyword.fetch(defaults, key) do
      {:ok, value} -> Keyword.put(opts, key, value)
      :error -> Keyword.delete(opts, key)
    end
  end

  defp durable_access(opts) do
    with {:ok, backend} <- configured_backend(opts),
         {:ok, scope} <- public_scope(opts) do
      {:ok, backend, scope}
    end
  end

  defp legacy_driver(opts) do
    if Keyword.has_key?(opts, :backend) do
      {:error,
       Error.new(
         :invalid_operation,
         "run/resume are storage-free driver operations; use save_graph/start_run " <>
           "with a durable backend"
       )}
    else
      :ok
    end
  end

  defp configured_backend(opts) do
    case {Keyword.get(opts, :backend), Keyword.fetch(opts, :backend_context)} do
      {backend, {:ok, context}} when is_atom(backend) ->
        {:ok, {backend, context}}

      {nil, _} ->
        {:error,
         Error.new(:storage_unavailable, "runtime instance has no durable storage backend")}

      _ ->
        {:error,
         Error.new(:invalid_backend, "runtime instance has an invalid durable backend context")}
    end
  end

  defp public_scope(opts) do
    case {Keyword.get(opts, :tenant_mode, :none), Keyword.fetch(opts, :tenant_id)} do
      {:none, :error} ->
        {:ok, :tenantless}

      {:none, {:ok, _tenant_id}} ->
        invalid_tenant("tenant_id is not accepted in :none mode")

      {:required, {:ok, tenant_id}} when is_binary(tenant_id) and byte_size(tenant_id) > 0 ->
        {:ok, {:tenant, tenant_id}}

      {:required, _} ->
        invalid_tenant("tenant_mode :required needs a non-empty tenant_id")

      {mode, _} ->
        invalid_tenant("unknown tenant_mode #{inspect(mode)}")
    end
  end

  defp invalid_tenant(message), do: {:error, Error.new(:invalid_tenant, message)}

  defp durable_resolve_interrupt(opts, run_id, interrupt_id, value) do
    with {:ok, {backend, context} = backend_ref, scope} <- durable_access(opts),
         {:ok, run} <- backend.runs().fetch_run(context, scope, run_id),
         {:ok, document} <- backend.graphs().fetch_graph(context, run.graph_id, run.graph_hash),
         {:ok, graph} <- Graph.from_map(document),
         {:ok, rtg} <- ensure_compiled_effective(graph, opts),
         now = operation_now(opts),
         result <-
           Lifecycle.signal(backend_ref, scope, run_id, fn current ->
             RunMutation.resolve_interrupt(rtg, current, interrupt_id, value, now)
           end) do
      finish_signal(result, opts)
    end
  end

  defp finish_signal({:ok, %Docket.Runtime.Moment{} = moment}, opts) do
    :ok = Lifecycle.after_commit(moment, opts)
    {:ok, moment.run}
  end

  defp finish_signal({:ok, %Run{} = run}, _opts), do: {:ok, run}
  defp finish_signal({:error, reason}, _opts), do: {:error, reason}

  defp operation_now(opts), do: Keyword.get(opts, :clock, &DateTime.utc_now/0).()

  defp await_options(opts) do
    timeout = Keyword.get(opts, :timeout)
    poll_interval = Keyword.get(opts, :poll_interval, 50)

    if is_integer(timeout) and timeout >= 0 and is_integer(poll_interval) and poll_interval > 0 do
      {:ok, timeout, poll_interval}
    else
      {:error,
       Error.new(
         :invalid_options,
         "await_run requires a non-negative :timeout and positive :poll_interval"
       )}
    end
  end

  defp do_await_run({backend, context} = backend_ref, scope, run_id, deadline, poll_interval) do
    case backend.runs().inspect_run(context, scope, run_id) do
      {:ok, %RunInfo{} = info} ->
        cond do
          RunInfo.poisoned?(info) ->
            {:error, {:poisoned, info}}

          info.run.status == :waiting or Run.terminal?(info.run) ->
            {:ok, info.run}

          System.monotonic_time(:millisecond) >= deadline ->
            {:error, :timeout}

          true ->
            remaining = max(deadline - System.monotonic_time(:millisecond), 0)
            Process.sleep(min(poll_interval, remaining))
            do_await_run(backend_ref, scope, run_id, deadline, poll_interval)
        end

      {:error, reason} ->
        {:error, reason}
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

  defp check_graph_ref(rtg, %GraphRef{} = reference) do
    if rtg.graph_id == reference.graph_id and rtg.graph_hash == reference.graph_hash do
      :ok
    else
      {:error,
       Error.new(:graph_mismatch, "saved graph document does not match its reference",
         details: %{
           reference_graph_id: reference.graph_id,
           reference_graph_hash: reference.graph_hash,
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
