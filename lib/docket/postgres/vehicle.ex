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

    ## Drain budget

    A continuously runnable graph is valid and would otherwise retain one
    dispatcher slot indefinitely. The optional `:drain_budget` bounds
    consecutive commits per drain at durable moment boundaries:

        drain_budget: [max_moments: 100, max_elapsed_ms: 1_000]

    `:infinity` (the default for both) disables a limit. Enabled limits must
    be positive integers. When the proposed moment's disposition is exactly
    `:continue` and either committing it would reach `:max_moments` for this
    drain, or the injectable `:monotonic_clock` observes `:max_elapsed_ms`
    expired at the pre-commit boundary, the vehicle narrows the moment with
    `Docket.Runtime.Moment.yield(moment, :drain_budget)` and commits it as an
    immediate park: one transaction persists the advanced run and events,
    clears the claim, records an immediate wake, and notifies. The run stays
    `:running` and re-enters the global eligible queue. Both limits firing
    together produce one yield.

    Budget scope and non-promises:

      * Elapsed decisions come only from `:monotonic_clock` (milliseconds,
        defaults to `System.monotonic_time/1`); the DateTime `:clock` never
        participates, so frozen or backward wall clocks cannot affect them.
      * The budget starts at the first moment calculation; claim fetch and
        graph load/compile never consume it.
      * The elapsed limit stops *starting* new supersteps once expiry is
        observed; the in-flight superstep is never preempted and runs to its
        commit boundary, so a single long node execution can overshoot the
        elapsed budget without bound.
      * Terminal, retry, timer, interrupt, external-wait, failure, and
        cancellation dispositions always win at an exact budget boundary;
        `max_supersteps` remains separate graph/runtime safety that
        terminally fails the run.
      * This bounds slot monopolization only. It promises no strict FIFO or
        round robin, no fixed maximum queue wait, no starvation freedom
        under sustained arrivals, and no tenant or workload fairness.
      * Every budget yield triggers both a transactional wake notification
        and the vehicle-exit poll; the dispatcher coalesces them, but tight
        budgets on cyclic runs make claim/launch/notify churn the steady
        state. Size limits deliberately.

    Each completed drain emits `[:docket, :postgres, :vehicle, :drain]` with
    measurements `%{committed_moments, elapsed_ms}` (the yield commit
    included) and metadata `%{outcome, budget}`, where `budget` is
    `:max_moments`, `:max_elapsed_ms`, or `:both` only when a budget yield
    durably committed, else `nil`. No run, task, token, tenant, or graph
    identity appears in the event.

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
      * Wire the dispatcher's `:launch` through `launcher/1` so malformed
        vehicle configuration raises where the supervision tree is built.
        Configuration this module validates raises inside the vehicle task
        otherwise - loud, but per claim rather than at boot.
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

    @type drain_budget :: [
            {:max_moments, pos_integer() | :infinity}
            | {:max_elapsed_ms, pos_integer() | :infinity}
          ]

    @type option ::
            {:backend, {module(), Docket.Storage.ctx()}}
            | {:task_supervisor, Supervisor.supervisor()}
            | {:clock, (-> DateTime.t())}
            | {:monotonic_clock, (-> integer())}
            | {:drain_budget, drain_budget()}
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
    Builds the dispatcher's `:launch` callback from eagerly validated options.

    Validation happens in this call, so a malformed `:drain_budget` or
    `:monotonic_clock` raises `ArgumentError` where the supervision tree is
    built - before any vehicle launches or any claim is consumed - instead
    of failing per claim. This is the recommended wiring:
    `launch: Docket.Postgres.Vehicle.launcher(opts)`.
    """
    @spec launcher([option()]) ::
            (Docket.Storage.Runs.claim_lease() -> {:ok, pid()} | {:error, term()})
    def launcher(opts) do
      _drain_budget = drain_budget!(opts)
      _monotonic_clock = monotonic_clock!(opts)
      _task_supervisor = Keyword.fetch!(opts, :task_supervisor)
      {_backend, _context} = Keyword.fetch!(opts, :backend)

      &launch(&1, opts)
    end

    @doc """
    Launches a vehicle under the configured `:task_supervisor`.

    Shaped for the dispatcher's `:launch` callback:
    `launch: &Docket.Postgres.Vehicle.launch(&1, opts)`. Prefer `launcher/1`,
    which validates the options once at assembly time.
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
            {:ok, rtg} ->
              budget = %{committed: 0, started: state.monotonic_clock.()}
              advance(state, rtg, run, first_advance_opts(state), budget)

            {:incompatible, reason} ->
              finish(state, nil, nil, abandon(state, reason))
          end

        :fence_lost ->
          finish(state, nil, nil, release(state, :fence_lost))
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
        monotonic_clock: monotonic_clock!(opts),
        drain_budget: drain_budget!(opts),
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

    defp advance(%{lease: lease} = state, rtg, run, advance_opts, budget) do
      case Loop.propose_advance(rtg, run, advance_opts) do
        {:ok, moment} ->
          commit(state, rtg, run, moment, budget)

        {:park, _run, park} ->
          finish(state, budget, nil, defer(state, park))

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

    defp commit(state, rtg, run, moment, budget) do
      {moment, fired} = maybe_yield(state, budget, moment)

      case Lifecycle.commit_moment(
             state.backend_ref,
             :system,
             moment,
             run.checkpoint_seq,
             state.lease.claim_token
           ) do
        {:ok, moment} ->
          :ok = Lifecycle.after_commit(moment, state.after_commit_opts)
          continue(state, rtg, moment, %{budget | committed: budget.committed + 1}, fired)

        {:error, reason} when reason in [:invalid_commit, :not_found] ->
          raise Error.new(
                  :claim_invariant,
                  "commit for claimed run #{inspect(state.lease.run_id)} was rejected",
                  details: %{reason: reason}
                )

        {:error, reason} ->
          finish(state, budget, nil, release(state, {:discarded, reason}))
      end
    end

    defp continue(state, rtg, %{disposition: :continue} = moment, budget, nil),
      do: advance(state, rtg, moment.run, state.moment_opts, budget)

    defp continue(state, _rtg, %{disposition: {:park, kind, _reason}}, budget, fired),
      do: finish(state, budget, fired, {:ok, {:parked, kind}})

    # A proposed :continue moment at or past the drain budget narrows to an
    # immediate drain-budget park before commit. Every other disposition wins
    # untouched at the boundary; both limits firing together yield once.
    defp maybe_yield(state, budget, %{disposition: :continue} = moment) do
      case fired_budget(state, budget) do
        nil ->
          {moment, nil}

        fired ->
          case Docket.Runtime.Moment.yield(moment, :drain_budget) do
            {:ok, yielded} ->
              {yielded, fired}

            {:error, reason} ->
              raise Error.new(
                      :claim_invariant,
                      "drain-budget yield for run #{inspect(state.lease.run_id)} was rejected",
                      details: %{reason: reason}
                    )
          end
      end
    end

    defp maybe_yield(_state, _budget, moment), do: {moment, nil}

    defp fired_budget(
           %{drain_budget: %{max_moments: moments, max_elapsed_ms: elapsed_ms}} = state,
           budget
         ) do
      count? = moments != :infinity and budget.committed + 1 >= moments

      elapsed? =
        elapsed_ms != :infinity and
          state.monotonic_clock.() - budget.started >= elapsed_ms

      cond do
        count? and elapsed? -> :both
        count? -> :max_moments
        elapsed? -> :max_elapsed_ms
        true -> nil
      end
    end

    # One observability fact per completed drain; `fired` is non-nil only
    # when a drain-budget yield durably committed. No identity labels.
    defp finish(state, budget, fired, {:ok, outcome} = result) do
      {committed, elapsed} =
        case budget do
          nil ->
            {0, 0}

          %{committed: committed, started: started} ->
            {committed, max(state.monotonic_clock.() - started, 0)}
        end

      :telemetry.execute(
        [:docket, :postgres, :vehicle, :drain],
        %{committed_moments: committed, elapsed_ms: elapsed},
        %{outcome: outcome_kind(outcome), budget: fired}
      )

      result
    end

    defp outcome_kind({:parked, _kind}), do: :parked
    defp outcome_kind(:fence_lost), do: :fence_lost
    defp outcome_kind({:discarded, _reason}), do: :discarded
    defp outcome_kind({:abandoned, _disposition, _reason}), do: :abandoned
    defp outcome_kind({:deferred, _disposition}), do: :deferred

    defp first_advance_opts(state),
      do: Keyword.put(state.moment_opts, :resume_floor, state.lease.claimed_at)

    # ---------------------------------------------------------------------
    # Option validation
    # ---------------------------------------------------------------------

    @doc false
    @spec drain_budget!([option()]) :: %{
            max_moments: pos_integer() | :infinity,
            max_elapsed_ms: pos_integer() | :infinity
          }
    def drain_budget!(opts) do
      budget = Keyword.get(opts, :drain_budget, [])

      unless Keyword.keyword?(budget) do
        raise ArgumentError, ":drain_budget must be a keyword list, got: #{inspect(budget)}"
      end

      case Keyword.keys(budget) -- [:max_moments, :max_elapsed_ms] do
        [] -> :ok
        unknown -> raise ArgumentError, ":drain_budget has unknown keys: #{inspect(unknown)}"
      end

      %{
        max_moments: budget_limit!(budget, :max_moments),
        max_elapsed_ms: budget_limit!(budget, :max_elapsed_ms)
      }
    end

    defp budget_limit!(budget, key) do
      case Keyword.get(budget, key, :infinity) do
        :infinity ->
          :infinity

        limit when is_integer(limit) and limit > 0 ->
          limit

        other ->
          raise ArgumentError,
                ":drain_budget #{key} must be a positive integer or :infinity, " <>
                  "got: #{inspect(other)}"
      end
    end

    defp monotonic_clock!(opts) do
      case Keyword.get(opts, :monotonic_clock, fn -> System.monotonic_time(:millisecond) end) do
        clock when is_function(clock, 0) ->
          clock

        other ->
          raise ArgumentError,
                ":monotonic_clock must be a zero-arity function, got: #{inspect(other)}"
      end
    end

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
