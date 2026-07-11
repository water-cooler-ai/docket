if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Vehicle do
    @moduledoc """
    Ephemeral execution shell for one claimed run.

    A vehicle turns one dispatcher claim lease into runtime progress: it loads
    the committed run, loads and compiles the exact effective graph version
    the run references, then drains runtime moments one fenced commit at a
    time. Each iteration calculates exactly one `Docket.Runtime.Moment`,
    commits it through `Docket.Lifecycle.commit_moment/5`, triggers
    `Docket.Lifecycle.after_commit/2`, and consumes the committed disposition
    for loop control only: `:continue` drains the next moment under the
    retained claim; every park exits after its commit released the claim and
    recorded the run's next wake, when it has one.

    Graph compilation happens at most once per drain. The stored document is
    already effective: it is validated against the locally installed node
    contracts and compiled without materializing configuration defaults, so
    defaults introduced after publication never reach a running graph. The
    optional `:graph_cache` reuses compiled graphs across drains and remembers
    versions this node cannot run.

    Failure boundaries:

      * Deterministic pre-execution failure - the stored document does not
        decode, validate, or compile against local node contracts - hands the
        claim back through `c:Docket.Storage.Runs.abandon_claim/5` with a
        jittered future retry, and is never reported as node execution
        failure.
      * A lost commit fence or failed event append discards the calculated
        moment, releases the claim if it is still current, and stops; no
        checkpoint observer or telemetry ever fires for a discarded moment.
      * Everything else crashes the vehicle process and leaves the claim to
        expire into recovery.

    A claimed run that is not runnable under the lease's committed sequence
    is an invariant violation and raises.

    Operational notes:

      * Node external effects are at-least-once around claim expiry and
        steal; integrations requiring deduplication must key on the stable
        task and idempotency identity in the node context.
      * Fenced commits refresh the claim, but nothing refreshes it between
        commits, so a single node execution longer than the dispatcher's
        orphan TTL invites a claim steal.
      * Node execution holds no checked-out database connection; connections
        are used only inside each commit's transaction.
      * Assemblies should start the vehicle `Task.Supervisor` before the
        dispatcher and give its children a shutdown of at least the
        dispatcher's drain timeout, so graceful drain outlives supervisor
        shutdown ordering.
    """

    alias Docket.{Error, Lifecycle, Run}
    alias Docket.Postgres.GraphCache
    alias Docket.Runtime.Loop

    @default_abandon_backoff_ms 30_000
    @default_max_claim_abandons 5

    @moment_option_keys [
      :executor,
      :executor_opts,
      :max_supersteps,
      :context,
      :id_generator,
      :clock
    ]
    @after_commit_option_keys [:checkpoint_observers, :task_supervisor, :context]

    @type option ::
            {:backend, {module(), Docket.Storage.ctx()}}
            | {:task_supervisor, Supervisor.supervisor()}
            | {:clock, (-> DateTime.t())}
            | {:jitter, (pos_integer() -> non_neg_integer())}
            | {:abandon_backoff_ms, pos_integer()}
            | {:max_claim_abandons, pos_integer()}
            | {:graph_cache, module() | false}
            | {:graph_cache_opts, keyword()}
            | {:compiler,
               (Docket.Graph.t(), keyword() ->
                  {:ok, Docket.Runtime.Graph.t()} | {:error, Docket.Graph.t()})}
            | {:executor, module()}
            | {:executor_opts, keyword()}
            | {:max_supersteps, pos_integer()}
            | {:context, map()}
            | {:id_generator, (atom() -> String.t())}
            | {:checkpoint_observers, module() | [module()]}

    @typedoc """
    Result of one drain.

    `{:parked, kind}` drained to a committed park. `:fence_lost` and
    `{:discarded, reason}` stopped without commit authority. `{:abandoned,
    disposition, reason}` handed the claim back before execution.
    `{:deferred, disposition}` handed back a claim whose active superstep has
    no due attempt yet.
    """
    @type outcome ::
            {:ok,
             {:parked, Docket.Runtime.Moment.park_kind()}
             | :fence_lost
             | {:discarded, term()}
             | {:abandoned, :rescheduled | :poisoned | :stale, term()}
             | {:deferred, :rescheduled | :poisoned | :stale}}

    @doc """
    Launches a vehicle under the configured `:task_supervisor`.

    Shaped for the dispatcher's `:launch` callback:
    `launch: &Docket.Postgres.Vehicle.launch(&1, opts)`.
    """
    @spec launch(Docket.Storage.Runs.claim_lease(), [option()]) ::
            {:ok, pid()} | {:error, term()}
    def launch(lease, opts) do
      Task.Supervisor.start_child(Keyword.fetch!(opts, :task_supervisor), fn ->
        drain(lease, opts)
      end)
    end

    @doc """
    Synchronously drains one claim lease to its next park or stop.
    """
    @spec drain(Docket.Storage.Runs.claim_lease(), [option()]) :: outcome()
    def drain(lease, opts) do
      state = build(lease, opts)

      case claimed_run(state) do
        {:ok, run} ->
          case runtime_graph(state) do
            {:ok, rtg} -> advance(state, rtg, run, first_advance_opts(state))
            {:incompatible, reason} -> abandon(state, reason)
          end

        :fence_lost ->
          release(state, :fence_lost)
      end
    end

    defp build(lease, opts) do
      {backend, context} = backend_ref = Keyword.fetch!(opts, :backend)

      %{
        lease: lease,
        backend_ref: backend_ref,
        context: context,
        runs: backend.runs(),
        graphs: backend.graphs(),
        clock: Keyword.get(opts, :clock, &DateTime.utc_now/0),
        jitter: Keyword.get(opts, :jitter, &:rand.uniform/1),
        abandon_backoff_ms: Keyword.get(opts, :abandon_backoff_ms, @default_abandon_backoff_ms),
        max_claim_abandons: Keyword.get(opts, :max_claim_abandons, @default_max_claim_abandons),
        graph_cache: Keyword.get(opts, :graph_cache, GraphCache),
        graph_cache_opts: Keyword.get(opts, :graph_cache_opts, []),
        compiler:
          Keyword.get(opts, :compiler, &Docket.Graph.Compiler.compile_effective_document/2),
        moment_opts: Keyword.take(opts, @moment_option_keys),
        after_commit_opts: Keyword.take(opts, @after_commit_option_keys)
      }
    end

    # ---------------------------------------------------------------------
    # Claimed run
    # ---------------------------------------------------------------------

    defp claimed_run(%{lease: lease} = state) do
      case state.runs.fetch_run(state.context, :system, lease.run_id) do
        {:ok, %Run{checkpoint_seq: seq}} when seq != lease.checkpoint_seq ->
          :fence_lost

        {:ok, %Run{status: :running} = run} ->
          {:ok, run}

        {:ok, %Run{} = run} ->
          raise Error.new(
                  :claim_invariant,
                  "claimed run #{inspect(lease.run_id)} is not runnable",
                  details: %{status: run.status, checkpoint_seq: run.checkpoint_seq}
                )

        {:error, :not_found} ->
          raise Error.new(
                  :claim_invariant,
                  "claimed run #{inspect(lease.run_id)} was not found"
                )
      end
    end

    # ---------------------------------------------------------------------
    # Graph load and compile
    # ---------------------------------------------------------------------

    defp runtime_graph(state) do
      case cache_fetch(state) do
        {:ok, rtg} -> {:ok, rtg}
        {:incompatible, reason} -> {:incompatible, reason}
        :miss -> fetch_and_compile(state)
      end
    end

    defp fetch_and_compile(%{lease: lease} = state) do
      case state.graphs.fetch_graph(state.context, lease.graph_id, lease.graph_hash) do
        {:ok, graph} ->
          compile(state, graph)

        {:error, :corrupt_graph} ->
          incompatible(state, :undecodable, :corrupt_graph)

        {:error, :not_found} ->
          raise Error.new(
                  :claim_invariant,
                  "graph #{inspect(lease.graph_id)}@#{inspect(lease.graph_hash)} " <>
                    "is missing for claimed run #{inspect(lease.run_id)}"
                )
      end
    end

    defp compile(%{lease: lease} = state, graph) do
      case state.compiler.(graph, profile: :run) do
        {:ok, rtg} ->
          if rtg.graph_id == lease.graph_id and rtg.graph_hash == lease.graph_hash do
            cache_put_compiled(state, rtg)
            {:ok, rtg}
          else
            incompatible(state, graph, :effective_identity_mismatch)
          end

        {:error, %Docket.Graph{} = failed} ->
          incompatible(state, graph, {:graph_compilation_failed, failed.diagnostics})
      end
    end

    defp incompatible(state, source, reason) do
      cache_put_incompatible(state, source, reason)
      {:incompatible, reason}
    end

    # ---------------------------------------------------------------------
    # Moment loop
    # ---------------------------------------------------------------------

    defp advance(%{lease: lease} = state, rtg, run, advance_opts) do
      case Loop.propose_advance(rtg, run, advance_opts) do
        {:ok, moment} ->
          commit(state, rtg, run, moment)

        {:park, _run, park} ->
          defer(state, park)

        {:wait, _run, interrupt_ids} ->
          raise Error.new(
                  :claim_invariant,
                  "claimed run #{inspect(lease.run_id)} is blocked on open interrupts",
                  details: %{interrupt_ids: interrupt_ids}
                )

        {:terminal, run} ->
          raise Error.new(
                  :claim_invariant,
                  "claimed run #{inspect(lease.run_id)} is already terminal",
                  details: %{status: run.status}
                )

        {:error, %Error{} = error} ->
          raise error
      end
    end

    defp commit(state, rtg, run, moment) do
      case Lifecycle.commit_moment(
             state.backend_ref,
             :system,
             moment,
             run.checkpoint_seq,
             state.lease.claim_token
           ) do
        {:ok, moment} ->
          :ok = Lifecycle.after_commit(moment, state.after_commit_opts)
          continue(state, rtg, moment)

        {:error, reason} when reason in [:invalid_commit, :not_found] ->
          raise Error.new(
                  :claim_invariant,
                  "commit for claimed run #{inspect(state.lease.run_id)} was rejected",
                  details: %{reason: reason}
                )

        {:error, reason} ->
          release(state, {:discarded, reason})
      end
    end

    defp continue(state, rtg, %{disposition: :continue} = moment),
      do: advance(state, rtg, moment.run, state.moment_opts)

    defp continue(_state, _rtg, %{disposition: {:park, kind, _reason}}),
      do: {:ok, {:parked, kind}}

    defp first_advance_opts(state),
      do: Keyword.put(state.moment_opts, :resume_floor, state.lease.claimed_at)

    # ---------------------------------------------------------------------
    # Claim hand-back and release
    # ---------------------------------------------------------------------

    defp abandon(state, reason) do
      now = state.clock.()
      backoff_ms = state.abandon_backoff_ms + state.jitter.(state.abandon_backoff_ms)

      {:ok,
       {:abandoned, hand_back(state, now, DateTime.add(now, backoff_ms, :millisecond)), reason}}
    end

    defp defer(state, park) do
      now = state.clock.()

      retry_at =
        if DateTime.compare(park.resume_at, now) == :lt, do: now, else: park.resume_at

      {:ok, {:deferred, hand_back(state, now, retry_at)}}
    end

    defp hand_back(%{lease: lease} = state, now, retry_at) do
      {:ok, disposition} =
        state.runs.abandon_claim(state.context, :system, lease.run_id, lease.claim_token, %{
          expected_checkpoint_seq: lease.checkpoint_seq,
          now: now,
          retry_at: retry_at,
          max_claim_abandons: state.max_claim_abandons
        })

      disposition
    end

    defp release(%{lease: lease} = state, outcome) do
      :ok =
        state.runs.release_claim(
          state.context,
          :system,
          lease.run_id,
          lease.claim_token,
          state.clock.()
        )

      {:ok, outcome}
    end

    # ---------------------------------------------------------------------
    # Graph cache
    # ---------------------------------------------------------------------

    defp cache_fetch(%{graph_cache: false}), do: :miss

    defp cache_fetch(%{lease: lease} = state),
      do: state.graph_cache.fetch(lease.graph_id, lease.graph_hash, state.graph_cache_opts)

    defp cache_put_compiled(%{graph_cache: false}, _rtg), do: :ok

    defp cache_put_compiled(%{lease: lease} = state, rtg) do
      :ok =
        state.graph_cache.put_compiled(
          lease.graph_id,
          lease.graph_hash,
          rtg,
          state.graph_cache_opts
        )
    end

    defp cache_put_incompatible(%{graph_cache: false}, _source, _reason), do: :ok

    defp cache_put_incompatible(%{lease: lease} = state, source, reason) do
      :ok =
        state.graph_cache.put_incompatible(
          lease.graph_id,
          lease.graph_hash,
          source,
          reason,
          state.graph_cache_opts
        )
    end
  end
end
