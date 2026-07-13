defmodule Docket do
  @moduledoc """
  Public entry point for durable graph operations.

  A runtime instance is one `Docket.Runtime.Supervisor` tree, identified by
  the name it was started under. Hosts usually define one with `use Docket`:

      defmodule MyApp.Docket do
        use Docket,
          repo: MyApp.Repo,
          backend: Docket.Postgres
      end

      # in the application supervision tree
      children = [MyApp.Docket]

  Hosts save graph versions explicitly and use `start_run`, storage-backed
  reads, named signals, and `await_run`. Options given to the instance at
  startup act as defaults; per-call options win except for the instance-owned
  `:backend` and `:tenant_mode` boundaries. Public durable calls resolve only
  `:tenantless` or an explicit `{:tenant, id}`; `:system` is reserved for
  internal dispatch/recovery.

  For processless in-test execution of the same loop, use `Docket.Test`.
  """

  alias Docket.{Error, Graph, GraphRef, Lifecycle, Run, RunInfo}
  alias Docket.Runtime.{Instance, Loop}
  alias Docket.Runtime.RunMutation

  @doc """
  Child spec integration point: `{Docket, name: MyRuntime, backend: ...}`
  starts a `Docket.Runtime.Supervisor` tree.
  """
  def child_spec(opts) do
    opts
    |> Docket.Runtime.Supervisor.child_spec()
    |> Map.put(:id, Keyword.get(opts, :name, Docket.Runtime.Supervisor))
  end

  @doc """
  Saves one effective, content-addressed graph version.

  Publication snapshots each node implementation's configuration schema once
  and materializes its defaults into the durable graph before hashing.
  Storage keeps that effective graph; execution loads and compiles it on
  the node that performs the work.
  """
  def save_graph(runtime, graph, opts \\ [])

  def save_graph(runtime, %Graph{} = graph, opts) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context}, scope} <- durable_access(opts),
         {:ok, effective, rtg} <- compile_for_publication(graph),
         :ok <-
           backend.graphs().save_graph(
             context,
             scope,
             rtg.graph_id,
             rtg.graph_hash,
             effective
           ) do
      {:ok, %GraphRef{graph_id: rtg.graph_id, graph_hash: rtg.graph_hash}}
    end
  end

  def save_graph(_runtime, graph, _opts) do
    {:error, Error.new(:invalid_graph, "expected a Docket.Graph, got #{inspect(graph)}")}
  end

  @doc """
  Reads the exact effective graph selected by a `Docket.GraphRef`.

  A reference is relative to the resolved tenant owner scope. Equal references
  may be saved independently by different tenants; possession of a reference
  never bypasses tenant isolation. An unknown or differently-owned reference
  returns `{:error, :not_found}`.
  """
  @spec fetch_graph(term(), GraphRef.t(), keyword()) :: {:ok, Graph.t()} | {:error, term()}
  def fetch_graph(runtime, graph_ref, opts \\ [])

  def fetch_graph(runtime, %GraphRef{graph_id: graph_id, graph_hash: graph_hash}, opts)
      when is_binary(graph_id) and byte_size(graph_id) > 0 and is_binary(graph_hash) and
             byte_size(graph_hash) > 0 do
    with :ok <- validate_keyword_options(opts, :fetch_graph),
         {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.graphs().fetch_graph(context, scope, graph_id, graph_hash)
    end
  end

  def fetch_graph(_runtime, graph_ref, _opts) do
    {:error,
     Error.new(
       :invalid_graph_reference,
       "expected a Docket.GraphRef with non-empty graph_id and graph_hash, got #{inspect(graph_ref)}"
     )}
  end

  @doc """
  Reads the reference for the newest distinct version of a graph ID.

  Latest is scoped to the resolved tenant and uses the same durable ordering
  as `list_graph_versions/3`. Re-saving an existing version is idempotent and
  does not move it forward in that order.
  """
  @spec fetch_latest_graph_ref(term(), String.t(), keyword()) ::
          {:ok, GraphRef.t()} | {:error, term()}
  def fetch_latest_graph_ref(runtime, graph_id, opts \\ [])

  def fetch_latest_graph_ref(runtime, graph_id, opts)
      when is_binary(graph_id) and byte_size(graph_id) > 0 do
    with :ok <- validate_keyword_options(opts, :fetch_latest_graph_ref),
         :ok <- validate_latest_graph_options(opts),
         {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.graphs().fetch_latest_graph_ref(context, scope, graph_id)
    end
  end

  def fetch_latest_graph_ref(_runtime, graph_id, _opts) do
    {:error,
     Error.new(
       :invalid_graph_id,
       "expected a non-empty graph ID, got #{inspect(graph_id)}"
     )}
  end

  @doc """
  Lists one graph ID's saved versions newest first under the resolved tenant.

  `:before` is an exclusive cursor returned as `next_before` by the preceding
  page. `:limit` defaults to 100 and must be in `1..1000`. An unknown graph ID
  returns a successful empty `Docket.GraphVersionPage`.
  """
  @spec list_graph_versions(term(), String.t(), keyword()) ::
          {:ok, Docket.GraphVersionPage.t()} | {:error, term()}
  def list_graph_versions(runtime, graph_id, opts \\ [])

  def list_graph_versions(runtime, graph_id, opts)
      when is_binary(graph_id) and byte_size(graph_id) > 0 do
    with :ok <- validate_keyword_options(opts, :list_graph_versions),
         {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, query} <- list_graph_versions_options(opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.graphs().list_graph_versions(context, scope, graph_id, query)
    end
  end

  def list_graph_versions(_runtime, graph_id, _opts) do
    {:error,
     Error.new(
       :invalid_graph_id,
       "expected a non-empty graph ID, got #{inspect(graph_id)}"
     )}
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

  def start_run(
        runtime,
        %GraphRef{graph_id: graph_id, graph_hash: graph_hash} = graph_ref,
        input,
        opts
      )
      when is_binary(graph_id) and byte_size(graph_id) > 0 and is_binary(graph_hash) and
             byte_size(graph_hash) > 0 do
    with :ok <- validate_keyword_options(opts, :start_run),
         {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context} = backend_ref, scope} <- durable_access(opts),
         {:ok, graph} <-
           backend.graphs().fetch_graph(
             context,
             scope,
             graph_ref.graph_id,
             graph_ref.graph_hash
           ),
         {:ok, rtg} <- ensure_compiled_effective(graph, opts),
         :ok <- check_graph_ref(rtg, graph_ref),
         run = Loop.build_initial_run(rtg, input, opts),
         {:ok, moment} <- Loop.propose_init(rtg, run, opts),
         {:ok, moment} <- Lifecycle.start(backend_ref, scope, moment) do
      :ok = Lifecycle.after_commit(moment, opts)
      maybe_inline_drain(backend, opts, scope, moment.run)
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
  Resolves an open interrupt and schedules the next tick or durable wake.

  Unknown or already-resolved interrupts return
  `{:error, %Docket.Error{type: :not_found}}`. The stored effective graph is
  loaded and compiled on the executing node without injecting new defaults,
  the pure mutation and its events commit atomically, and tenant scope is enforced before storage access.
  Authorization remains host-owned.
  """
  def resolve_interrupt(runtime, run_id, interrupt_id, value, opts \\ []) do
    with {:ok, resolved} <- instance_opts(runtime, opts) do
      durable_resolve_interrupt(resolved, run_id, interrupt_id, value)
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
      case backend.runs().retry_poisoned_run(context, scope, run_id, operation_now(opts)) do
        {:ok, run} -> maybe_inline_drain(backend, opts, scope, run)
        other -> other
      end
    end
  end

  @doc "Synchronously claims and drains due durable runs in a backend testing mode."
  def drain_runs(runtime, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, _context}, _scope} <- durable_access(opts) do
      if function_exported?(backend, :drain_runs, 1) do
        backend.drain_runs(opts)
      else
        {:error,
         Error.new(:unsupported_operation, "configured backend does not support drain_runs")}
      end
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
  Lists durable runs visible to the resolved tenant scope.

  Runs are returned as lightweight `Docket.RunSummary` values in newest-first
  order by the immutable `{started_at, run_id}` key. `:before` accepts the
  previous page's `next_before` cursor. Optional filters are `:status`
  (one durable status or a non-empty list), `:graph_id`, and `:graph_hash`.
  `:limit` defaults to `100` and must be in `1..1000`.

  An empty result is a successful empty `Docket.RunPage`. Under
  `tenant_mode: :required`, `:tenant_id` is mandatory and is always an access
  scope, never an optional filter.
  """
  @spec list_runs(term(), keyword()) :: {:ok, Docket.RunPage.t()} | {:error, term()}
  def list_runs(runtime, opts \\ []) do
    with :ok <- validate_keyword_options(opts, :list_runs),
         {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, query} <- list_runs_options(opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.runs().list_runs(context, scope, query)
    end
  end

  @doc """
  Fetches the newest run summary matching the supplied tenant scope and
  optional graph/status filters.

  Returns `{:error, :not_found}` when the scoped query has no matches.
  """
  @spec fetch_latest_run(term(), keyword()) ::
          {:ok, Docket.RunSummary.t()} | {:error, term()}
  def fetch_latest_run(runtime, opts \\ []) do
    with :ok <- validate_keyword_options(opts, :fetch_latest_run),
         :ok <- validate_latest_run_options(opts),
         {:ok, opts} <- instance_opts(runtime, opts),
         query_opts = opts |> Keyword.delete(:before) |> Keyword.put(:limit, 1),
         {:ok, query} <- list_runs_options(query_opts),
         {:ok, {backend, context}, scope} <- durable_access(opts),
         {:ok, page} <- backend.runs().list_runs(context, scope, query) do
      case page.runs do
        [latest] -> {:ok, latest}
        [] -> {:error, :not_found}
      end
    end
  end

  @doc """
  Reads one retained durable event by its positive sequence number.

  A missing or pruned sequence, an unknown run, and a wrong tenant all return
  `{:error, :not_found}`.
  """
  @spec fetch_event(term(), String.t(), pos_integer(), keyword()) ::
          {:ok, Docket.Event.t()} | {:error, term()}
  def fetch_event(runtime, run_id, seq, opts \\ []) do
    with :ok <- validate_event_seq(seq),
         {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.events().fetch_event(context, scope, run_id, seq)
    end
  end

  @doc """
  Reads the latest retained durable event for a run.

  A visible run whose complete event history has been pruned returns
  `{:ok, nil}`. An unknown run and a wrong tenant return
  `{:error, :not_found}`.
  """
  @spec fetch_latest_event(term(), String.t(), keyword()) ::
          {:ok, Docket.Event.t() | nil} | {:error, term()}
  def fetch_latest_event(runtime, run_id, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.events().fetch_latest_event(context, scope, run_id)
    end
  end

  @doc """
  Reads a page of retained durable events for a run.

  Events come back in ascending sequence order, restricted to sequences
  greater than `:after_seq` (default `0`) and limited by `:limit` (default
  `250`, an integer in `1..1000`). Sequence gaps from persistence filtering
  and retention pruning are normal, so pages are not promised contiguous. The
  returned `Docket.EventPage` carries the retention bounds and the run's latest
  committed event sequence observed from the same snapshot.

  Invalid options return `{:error, %Docket.Error{type: :invalid_options}}`
  without reaching storage. A wrong tenant and an unknown run both return
  `{:error, :not_found}`.

      {:ok, page} = MyApp.Docket.list_events("run-123", after_seq: 10, limit: 50)
      page.events
  """
  def list_events(runtime, run_id, opts \\ []) do
    with {:ok, opts} <- instance_opts(runtime, opts),
         {:ok, page_opts} <- list_events_options(opts),
         {:ok, {backend, context}, scope} <- durable_access(opts) do
      backend.events().list_events(context, scope, run_id, page_opts)
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
        use Docket, backend: Docket.Postgres, repo: MyApp.Repo
      end

  The options become the instance's default run options. The host module
  gets supervision and operational wrappers that call `Docket` with the
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

      def start_run(graph_ref, input, opts \\ []) do
        Docket.start_run(__MODULE__, graph_ref, input, opts)
      end

      def save_graph(graph, opts \\ []), do: Docket.save_graph(__MODULE__, graph, opts)

      def fetch_graph(graph_ref, opts \\ []),
        do: Docket.fetch_graph(__MODULE__, graph_ref, opts)

      def fetch_latest_graph_ref(graph_id, opts \\ []),
        do: Docket.fetch_latest_graph_ref(__MODULE__, graph_id, opts)

      def list_graph_versions(graph_id, opts \\ []),
        do: Docket.list_graph_versions(__MODULE__, graph_id, opts)

      def resolve_interrupt(run_id, interrupt_id, value, opts \\ []) do
        Docket.resolve_interrupt(__MODULE__, run_id, interrupt_id, value, opts)
      end

      def cancel_run(run_id, opts \\ []), do: Docket.cancel_run(__MODULE__, run_id, opts)

      def retry_poisoned_run(run_id, opts \\ []) do
        Docket.retry_poisoned_run(__MODULE__, run_id, opts)
      end

      def fetch_run(run_id, opts \\ []), do: Docket.fetch_run(__MODULE__, run_id, opts)
      def inspect_run(run_id, opts \\ []), do: Docket.inspect_run(__MODULE__, run_id, opts)
      def list_runs(opts \\ []), do: Docket.list_runs(__MODULE__, opts)
      def fetch_latest_run(opts \\ []), do: Docket.fetch_latest_run(__MODULE__, opts)

      def fetch_event(run_id, seq, opts \\ []),
        do: Docket.fetch_event(__MODULE__, run_id, seq, opts)

      def fetch_latest_event(run_id, opts \\ []) do
        Docket.fetch_latest_event(__MODULE__, run_id, opts)
      end

      def list_events(run_id, opts \\ []), do: Docket.list_events(__MODULE__, run_id, opts)
      def await_run(run_id, opts \\ []), do: Docket.await_run(__MODULE__, run_id, opts)
      def drain_runs(opts \\ []), do: Docket.drain_runs(__MODULE__, opts)
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
    case Instance.defaults(runtime) do
      {:ok, defaults} ->
        task_supervisor = Docket.Runtime.Supervisor.task_supervisor(runtime)

        merged =
          defaults
          |> Keyword.merge(opts)
          |> preserve_instance_option(defaults, :backend)
          |> preserve_instance_option(defaults, :backend_context)
          |> preserve_instance_option(defaults, :tenant_mode)
          |> preserve_instance_option(defaults, :testing)
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

  defp validate_keyword_options(opts, operation) do
    if is_list(opts) and Keyword.keyword?(opts) do
      :ok
    else
      {:error,
       Error.new(
         :invalid_options,
         "#{operation} options must be a keyword list, got #{inspect(opts)}"
       )}
    end
  end

  defp list_graph_versions_options(opts) do
    limit = Keyword.get(opts, :limit, 100)
    before = Keyword.get(opts, :before)

    cond do
      not (is_integer(limit) and limit in 1..1000) ->
        {:error,
         Error.new(
           :invalid_options,
           "list_graph_versions :limit must be an integer in 1..1000"
         )}

      not valid_graph_version_cursor?(before) ->
        {:error,
         Error.new(
           :invalid_options,
           "list_graph_versions :before must be nil or a {DateTime, non-empty graph_hash} cursor"
         )}

      true ->
        {:ok, %{limit: limit, before: before}}
    end
  end

  defp valid_graph_version_cursor?(nil), do: true

  defp valid_graph_version_cursor?({%DateTime{}, graph_hash}),
    do: is_binary(graph_hash) and byte_size(graph_hash) > 0

  defp valid_graph_version_cursor?(_cursor), do: false

  defp validate_latest_graph_options(opts) do
    if Keyword.has_key?(opts, :limit) or Keyword.has_key?(opts, :before) do
      {:error,
       Error.new(
         :invalid_options,
         "fetch_latest_graph_ref does not accept :limit or :before"
       )}
    else
      :ok
    end
  end

  defp list_events_options(opts) do
    after_seq = Keyword.get(opts, :after_seq, 0)
    limit = Keyword.get(opts, :limit, 250)

    cond do
      not (is_integer(after_seq) and after_seq >= 0) ->
        {:error,
         Error.new(:invalid_options, "list_events :after_seq must be a non-negative integer")}

      not (is_integer(limit) and limit in 1..1000) ->
        {:error, Error.new(:invalid_options, "list_events :limit must be an integer in 1..1000")}

      true ->
        {:ok, %{after_seq: after_seq, limit: limit}}
    end
  end

  defp list_runs_options(opts) do
    limit = Keyword.get(opts, :limit, 100)
    before = Keyword.get(opts, :before)
    graph_id = Keyword.get(opts, :graph_id)
    graph_hash = Keyword.get(opts, :graph_hash)

    with :ok <- validate_run_limit(limit),
         :ok <- validate_run_cursor(before),
         :ok <- validate_optional_id(graph_id, :graph_id),
         :ok <- validate_optional_id(graph_hash, :graph_hash),
         {:ok, statuses} <- normalize_run_statuses(Keyword.get(opts, :status)) do
      {:ok,
       %{
         limit: limit,
         before: before,
         graph_id: graph_id,
         graph_hash: graph_hash,
         statuses: statuses
       }}
    end
  end

  defp validate_run_limit(limit) when is_integer(limit) and limit in 1..1000, do: :ok

  defp validate_run_limit(_limit) do
    {:error, Error.new(:invalid_options, "list_runs :limit must be an integer in 1..1000")}
  end

  defp validate_latest_run_options(opts) do
    if Keyword.has_key?(opts, :limit) or Keyword.has_key?(opts, :before) do
      {:error,
       Error.new(
         :invalid_options,
         "fetch_latest_run accepts graph/status filters but not :limit or :before"
       )}
    else
      :ok
    end
  end

  defp validate_run_cursor(nil), do: :ok

  defp validate_run_cursor({%DateTime{}, run_id})
       when is_binary(run_id) and byte_size(run_id) > 0,
       do: :ok

  defp validate_run_cursor(_cursor) do
    {:error,
     Error.new(
       :invalid_options,
       "list_runs :before must be nil or a {DateTime, non-empty run_id} cursor"
     )}
  end

  defp validate_optional_id(nil, _field), do: :ok
  defp validate_optional_id(value, _field) when is_binary(value) and byte_size(value) > 0, do: :ok

  defp validate_optional_id(_value, field) do
    {:error,
     Error.new(:invalid_options, "list_runs #{inspect(field)} must be a non-empty binary")}
  end

  defp normalize_run_statuses(nil), do: {:ok, nil}

  defp normalize_run_statuses(status) when is_atom(status) do
    if Run.durable_status?(status),
      do: {:ok, [status]},
      else: invalid_run_statuses()
  end

  defp normalize_run_statuses(statuses) when is_list(statuses) and statuses != [] do
    if Enum.all?(statuses, &Run.durable_status?/1),
      do: {:ok, Enum.uniq(statuses)},
      else: invalid_run_statuses()
  end

  defp normalize_run_statuses(_statuses), do: invalid_run_statuses()

  defp invalid_run_statuses do
    {:error,
     Error.new(
       :invalid_options,
       "list_runs :status must be a durable status or a non-empty list of durable statuses"
     )}
  end

  defp validate_event_seq(seq) when is_integer(seq) and seq > 0, do: :ok

  defp validate_event_seq(_seq) do
    {:error, Error.new(:invalid_options, "fetch_event sequence must be a positive integer")}
  end

  defp durable_resolve_interrupt(opts, run_id, interrupt_id, value) do
    with {:ok, {backend, context} = backend_ref, scope} <- durable_access(opts),
         {:ok, run} <- backend.runs().fetch_run(context, scope, run_id),
         {:ok, graph} <-
           backend.graphs().fetch_graph(context, scope, run.graph_id, run.graph_hash),
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
    {:ok, {backend, _context}, scope} = durable_access(opts)
    maybe_inline_drain(backend, opts, scope, moment.run)
  end

  defp finish_signal({:ok, %Run{} = run}, _opts), do: {:ok, run}
  defp finish_signal({:error, reason}, _opts), do: {:error, reason}

  defp maybe_inline_drain(backend, opts, scope, run) do
    if not Run.terminal?(run) and function_exported?(backend, :testing_mode, 1) and
         backend.testing_mode(opts) == :inline do
      case backend.drain_runs(opts) do
        {:ok, summary} -> inline_result(backend, opts, scope, run.id, summary)
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, run}
    end
  end

  defp inline_result(backend, opts, scope, run_id, summary) do
    context = backend.context(opts)

    with {:ok, current} <- backend.runs().fetch_run(context, scope, run_id) do
      if summary.limit_reached and current.status == :running do
        case backend.runs().inspect_run(context, scope, run_id) do
          {:ok, %RunInfo{wake_at: %DateTime{} = wake_at}} ->
            if DateTime.compare(wake_at, operation_now(opts)) in [:lt, :eq],
              do: {:error, {:inline_drain_limit_reached, summary}},
              else: {:ok, current}

          {:ok, %RunInfo{}} ->
            {:ok, current}

          error ->
            error
        end
      else
        {:ok, current}
      end
    end
  end

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
end
