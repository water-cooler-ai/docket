if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Vehicle do
    @moduledoc """
    Ephemeral execution shell for one claimed run.

    Each vehicle loads and compiles the run's exact graph, validates every
    explicit node timeout against the host's finite attempt maximum, and
    commits one fenced runtime moment at a time. Host-policy incompatibility
    reschedules the claim without poisoning: the claim-attempt increment is
    reversed, the run stays valid, and the handback counts one claim abandon.
    The retry wake backs off exponentially with the consecutive abandon
    count — `min(abandon_backoff_ms * 2^abandons, abandon_backoff_cap_ms)` —
    so a fleet with no compatible host pushes the run back instead of
    spinning, and any committed progress resets the count. Stored-graph
    incompatibility shares the same abandon counter, so a persistently
    incompatible deployment still poisons through that path's budget.
    Deployments should still use homogeneous limits and audit stored graphs
    before rollout so compatible work is not needlessly delayed.

    Node attempt deadlines are enforced by the runtime dispatcher. The
    cooperative drain budget is checked only at moment boundaries and includes
    configured headroom below orphan-TTL crash recovery. Token/sequence fencing
    remains the sole authority for commits after a crash or steal.

    Vehicles never refresh claims. Attempt timeout and retry remain
    at-least-once boundaries: external effects and unlinked children cannot be
    retracted, so expected long work must park or detach durably.
    """

    alias Docket.{Error, Lifecycle, Run}
    alias Docket.Postgres.GraphCache
    alias Docket.Runtime.Loop

    @default_abandon_backoff_ms 30_000
    @default_abandon_backoff_cap_ms 3_600_000
    @default_max_claim_abandons 5

    @moment_option_keys [
      :executor,
      :executor_opts,
      :max_attempt_elapsed_ms,
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
            | {:max_attempt_elapsed_ms, pos_integer()}
            | {:jitter, (pos_integer() -> non_neg_integer())}
            | {:abandon_backoff_ms, pos_integer()}
            | {:abandon_backoff_cap_ms, pos_integer()}
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
    `{:discarded, reason}` stopped without commit authority.
    `{:host_incompatible, error}` released without poisoning because this
    host's attempt maximum cannot execute the graph. `{:abandoned, ...}` is
    a stored-graph incompatibility; `{:deferred, ...}` has no due attempt yet.
    """
    @type outcome ::
            {:ok,
             {:parked, Docket.Runtime.Moment.park_kind()}
             | :fence_lost
             | {:discarded, term()}
             | {:host_incompatible, Docket.Error.t()}
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
      _max_attempt_elapsed_ms = max_attempt_elapsed_ms!(opts)
      _abandon_backoff = abandon_backoff!(opts)
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
      try do
        Docket.Telemetry.span([:docket, :postgres, :vehicle], %{}, fn ->
          result = do_drain(lease, opts)
          {result, %{result: drain_result(result)}}
        end)
      rescue
        error ->
          emit_vehicle_crash(lease)
          reraise error, __STACKTRACE__
      catch
        kind, reason ->
          emit_vehicle_crash(lease)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end

    defp do_drain(lease, opts) do
      state = build(lease, opts)
      drain_claimed(state)
    end

    defp drain_result({:ok, {:parked, kind}}), do: kind
    defp drain_result({:ok, outcome}), do: outcome_kind(outcome)
    defp drain_result(_), do: :error

    defp emit_vehicle_crash(lease) do
      :telemetry.execute(
        [:docket, :postgres, :vehicle, :crash],
        %{
          count: 1,
          claim_held_ms:
            max(DateTime.diff(DateTime.utc_now(), lease.claimed_at, :millisecond), 0),
          claim_attempt: lease.claim_attempt
        },
        %{result: :crashed}
      )
    end

    defp drain_claimed(state) do
      case claimed_run(state) do
        {:ok, run} ->
          case runtime_graph(state) do
            {:ok, rtg} ->
              budget = %{committed: 0, started: state.monotonic_clock.()}
              advance(state, rtg, run, first_advance_opts(state), budget)

            {:incompatible, reason} ->
              finish(state, nil, nil, abandon(state, reason))

            {:host_incompatible, error} ->
              finish(state, nil, nil, host_incompatible(state, error))
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
        max_attempt_elapsed_ms: max_attempt_elapsed_ms!(opts),
        jitter: Keyword.get(opts, :jitter, &:rand.uniform/1),
        abandon_backoff: abandon_backoff!(opts),
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
      case state.runs.fetch_run(state.context, lease.owner_scope, lease.run_id) do
        {:ok, %Run{checkpoint_seq: seq}} when seq != lease.checkpoint_seq ->
          emit_fence_loss(:pre_fetch, :stale_fence)
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
        {:ok, rtg} -> validate_execution_policy(state, rtg)
        {:incompatible, reason} -> {:incompatible, reason}
        :miss -> fetch_and_compile(state)
      end
    end

    defp fetch_and_compile(%{lease: lease} = state) do
      result =
        Docket.Telemetry.span([:docket, :postgres, :graph, :fetch], %{}, fn ->
          result =
            state.graphs.fetch_graph(
              state.context,
              lease.owner_scope,
              lease.graph_id,
              lease.graph_hash
            )

          {result, %{result: Docket.Telemetry.result_kind(result)}}
        end)

      case result do
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
      result =
        Docket.Telemetry.span([:docket, :postgres, :graph, :compile], %{}, fn ->
          result = state.compiler.(graph, profile: :run)
          {result, %{result: Docket.Telemetry.result_kind(result)}}
        end)

      case result do
        {:ok, rtg} ->
          if rtg.graph_id == lease.graph_id and rtg.graph_hash == lease.graph_hash do
            case validate_execution_policy(state, rtg) do
              {:ok, rtg} = ok ->
                cache_put_compiled(state, rtg)
                ok

              host_incompatible ->
                host_incompatible
            end
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

    defp validate_execution_policy(state, rtg) do
      case Docket.Runtime.ExecutionPolicy.validate_graph(rtg, state.max_attempt_elapsed_ms) do
        :ok -> {:ok, rtg}
        {:error, error} -> {:host_incompatible, error}
      end
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
             state.lease.owner_scope,
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
          if reason == :stale_fence,
            do: emit_fence_loss(:commit, reason),
            else: emit_discard(:commit, reason)

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
        %{
          committed_moments: committed,
          elapsed_ms: elapsed,
          claim_held_ms: claim_held_ms(state),
          claim_attempt: state.lease.claim_attempt
        },
        %{outcome: outcome_kind(outcome), budget: fired}
      )

      result
    end

    defp outcome_kind({:parked, _kind}), do: :parked
    defp outcome_kind(:fence_lost), do: :fence_lost
    defp outcome_kind({:discarded, _reason}), do: :discarded
    defp outcome_kind({:abandoned, _disposition, _reason}), do: :abandoned
    defp outcome_kind({:deferred, _disposition}), do: :deferred
    defp outcome_kind({:host_incompatible, _error}), do: :host_incompatible

    defp claim_held_ms(%{lease: %{claimed_at: claimed_at}, clock: clock}) do
      max(DateTime.diff(clock.(), claimed_at, :millisecond), 0)
    end

    defp emit_fence_loss(stage, reason) do
      :telemetry.execute(
        [:docket, :postgres, :claim, :fence_lost],
        %{count: 1},
        %{stage: stage, result: reason}
      )
    end

    defp emit_discard(stage, reason) do
      :telemetry.execute(
        [:docket, :postgres, :vehicle, :discard],
        %{count: 1},
        %{stage: stage, result: reason}
      )
    end

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

    defp max_attempt_elapsed_ms!(opts) do
      case Keyword.get(opts, :max_attempt_elapsed_ms, 2_000) do
        value when is_integer(value) and value > 0 -> value
        _ -> raise ArgumentError, ":max_attempt_elapsed_ms must be a positive finite integer"
      end
    end

    @doc false
    @spec abandon_backoff!([option()]) :: %{base_ms: pos_integer(), cap_ms: pos_integer()}
    def abandon_backoff!(opts) do
      base = Keyword.get(opts, :abandon_backoff_ms, @default_abandon_backoff_ms)
      cap = Keyword.get(opts, :abandon_backoff_cap_ms, @default_abandon_backoff_cap_ms)

      unless is_integer(base) and base > 0 do
        raise ArgumentError, ":abandon_backoff_ms must be a positive integer"
      end

      unless is_integer(cap) and cap >= base do
        raise ArgumentError,
              ":abandon_backoff_cap_ms must be an integer at least :abandon_backoff_ms"
      end

      %{base_ms: base, cap_ms: cap}
    end

    # ---------------------------------------------------------------------
    # Claim hand-back and release
    # ---------------------------------------------------------------------

    defp abandon(state, reason) do
      now = state.clock.()
      base_ms = state.abandon_backoff.base_ms
      backoff_ms = base_ms + state.jitter.(base_ms)

      {:ok,
       {:abandoned, hand_back(state, now, DateTime.add(now, backoff_ms, :millisecond)), reason}}
    end

    defp host_incompatible(state, error) do
      now = state.clock.()
      retry_at = DateTime.add(now, state.abandon_backoff.base_ms, :millisecond)

      case state.runs.abandon_claim(
             state.context,
             :system,
             state.lease.run_id,
             state.lease.claim_token,
             %{
               expected_checkpoint_seq: state.lease.checkpoint_seq,
               now: now,
               retry_at: retry_at,
               max_claim_abandons: state.max_claim_abandons,
               non_poisoning: true,
               backoff: state.abandon_backoff
             }
           ) do
        {:ok, :rescheduled} -> {:ok, {:host_incompatible, error}}
        {:ok, :stale} -> {:ok, :fence_lost}
      end
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
      do: state.graph_cache.fetch(lease.graph_id, lease.graph_hash, scoped_cache_opts(state))

    defp cache_put_compiled(%{graph_cache: false}, _rtg), do: :ok

    defp cache_put_compiled(%{lease: lease} = state, rtg) do
      :ok =
        state.graph_cache.put_compiled(
          lease.graph_id,
          lease.graph_hash,
          rtg,
          scoped_cache_opts(state)
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
          scoped_cache_opts(state)
        )
    end

    defp scoped_cache_opts(%{lease: lease} = state) do
      {repo, prefix} = Docket.Postgres.Storage.context!(state.context)

      Keyword.put(
        state.graph_cache_opts,
        :cache_scope,
        {repo, prefix, lease.owner_scope}
      )
    end
  end
end
