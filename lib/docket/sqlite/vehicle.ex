if Code.ensure_loaded?(Exqlite.Sqlite3) do
  defmodule Docket.SQLite.Vehicle do
    @moduledoc """
    Drains one claimed run: loads and compiles its graph, then proposes and
    commits one fenced runtime moment at a time until the run parks.

    This is the `Docket.Postgres.Vehicle` loop without the pieces a single
    database connection makes unnecessary: there is no shared graph-cache
    generation protocol (one ETS table per runner) and no cross-node claim
    accounting. Fencing is unchanged: every commit carries the lease's claim
    token and expected checkpoint sequence.
    """

    alias Docket.{Error, Lifecycle, Run}
    alias Docket.Runtime.{Clock, Loop}

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

    @default_abandon_backoff_ms 30_000
    @default_abandon_backoff_cap_ms 3_600_000
    @default_max_claim_abandons 5

    @type outcome ::
            {:ok,
             {:parked, Docket.Runtime.Moment.park_kind()}
             | :fence_lost
             | {:discarded, term()}
             | {:host_incompatible, Docket.Error.t()}
             | {:abandoned, :rescheduled | :poisoned | :stale, term()}
             | {:deferred, :rescheduled | :poisoned | :stale}}

    @doc "Synchronously drains one claim lease to its next park or stop."
    @spec drain(Docket.Backend.RunStore.claim_lease(), keyword()) :: outcome()
    def drain(lease, opts) do
      state = build(lease, opts)
      started = state.monotonic_clock.()
      {result, committed} = drain_claimed(state)

      :telemetry.execute(
        [:docket, :sqlite, :vehicle, :drain],
        %{committed_moments: committed, elapsed_ms: state.monotonic_clock.() - started},
        %{outcome: outcome_kind(result)}
      )

      result
    end

    defp build(lease, opts) do
      {backend, context} = backend_ref = Keyword.fetch!(opts, :backend)
      budget = Keyword.get(opts, :drain_budget, [])

      %{
        lease: lease,
        backend_ref: backend_ref,
        context: context,
        runs: backend.runs(),
        graphs: backend.graphs(),
        clock: Clock.wall_clock(opts),
        monotonic_clock:
          Keyword.get(opts, :monotonic_clock, fn -> System.monotonic_time(:millisecond) end),
        max_moments: Keyword.get(budget, :max_moments, :infinity),
        max_elapsed_ms: Keyword.get(budget, :max_elapsed_ms, :infinity),
        max_attempt_elapsed_ms: Keyword.get(opts, :max_attempt_elapsed_ms, 2_000),
        jitter: Keyword.get(opts, :jitter, &:rand.uniform/1),
        abandon_backoff: %{
          base_ms: Keyword.get(opts, :abandon_backoff_ms, @default_abandon_backoff_ms),
          cap_ms: Keyword.get(opts, :abandon_backoff_cap_ms, @default_abandon_backoff_cap_ms)
        },
        max_claim_abandons: Keyword.get(opts, :max_claim_abandons, @default_max_claim_abandons),
        graph_cache: Keyword.get(opts, :graph_cache, false),
        moment_opts: Keyword.take(opts, @moment_option_keys),
        after_commit_opts: Keyword.take(opts, @after_commit_option_keys)
      }
    end

    defp drain_claimed(state) do
      case claimed_run(state) do
        {:ok, run} ->
          case runtime_graph(state) do
            {:ok, rtg} ->
              budget = %{committed: 0, started: state.monotonic_clock.()}
              opts = Keyword.put(state.moment_opts, :resume_floor, state.lease.claimed_at)
              advance(state, rtg, run, opts, budget)

            {:incompatible, reason} ->
              {abandon(state, reason), 0}

            {:host_incompatible, error} ->
              {host_incompatible(state, error), 0}
          end

        :fence_lost ->
          {release(state, :fence_lost), 0}
      end
    end

    defp claimed_run(%{lease: lease} = state) do
      case state.runs.fetch_run(state.context, lease.owner_scope, lease.run_id) do
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
          raise Error.new(:claim_invariant, "claimed run #{inspect(lease.run_id)} was not found")
      end
    end

    # -----------------------------------------------------------------
    # Graph load and compile
    # -----------------------------------------------------------------

    defp runtime_graph(%{lease: lease} = state) do
      case cache_fetch(state) do
        {:ok, rtg} ->
          validate_execution_policy(state, rtg)

        :miss ->
          case state.graphs.fetch_graph(
                 state.context,
                 lease.owner_scope,
                 lease.graph_id,
                 lease.graph_hash
               ) do
            {:ok, graph} ->
              compile(state, graph)

            {:error, :corrupt_graph} ->
              {:incompatible, :corrupt_graph}

            {:error, :not_found} ->
              raise Error.new(
                      :claim_invariant,
                      "graph #{inspect(lease.graph_id)}@#{inspect(lease.graph_hash)} " <>
                        "is missing for claimed run #{inspect(lease.run_id)}"
                    )
          end
      end
    end

    defp compile(%{lease: lease} = state, graph) do
      case Docket.Graph.Compiler.compile_effective_document(graph, profile: :run) do
        {:ok, rtg} when rtg.graph_id == lease.graph_id and rtg.graph_hash == lease.graph_hash ->
          cache_put(state, rtg)
          validate_execution_policy(state, rtg)

        {:ok, _rtg} ->
          {:incompatible, :effective_identity_mismatch}

        {:error, %Docket.Graph{} = failed} ->
          {:incompatible, {:graph_compilation_failed, failed.diagnostics}}
      end
    end

    defp validate_execution_policy(state, rtg) do
      case Docket.Runtime.ExecutionPolicy.validate_graph(rtg, state.max_attempt_elapsed_ms) do
        :ok -> {:ok, rtg}
        {:error, error} -> {:host_incompatible, error}
      end
    end

    defp cache_fetch(%{graph_cache: false}), do: :miss

    defp cache_fetch(%{graph_cache: table, lease: lease}) do
      case :ets.lookup(table, {lease.graph_id, lease.graph_hash}) do
        [{_key, rtg}] -> {:ok, rtg}
        [] -> :miss
      end
    end

    defp cache_put(%{graph_cache: false}, _rtg), do: :ok

    defp cache_put(%{graph_cache: table}, rtg) do
      true = :ets.insert(table, {{rtg.graph_id, rtg.graph_hash}, rtg})
      :ok
    end

    # -----------------------------------------------------------------
    # Moment loop
    # -----------------------------------------------------------------

    defp advance(%{lease: lease} = state, rtg, run, advance_opts, budget) do
      case Loop.propose_advance(rtg, run, advance_opts) do
        {:ok, moment} ->
          commit(state, rtg, run, moment, budget)

        {:park, _run, park} ->
          {defer(state, park), budget.committed}

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
      moment = maybe_yield(state, budget, moment)

      case Lifecycle.commit_moment(
             state.backend_ref,
             state.lease.owner_scope,
             moment,
             run.checkpoint_seq,
             state.lease.claim_token
           ) do
        {:ok, moment} ->
          :ok = Lifecycle.after_commit(moment, state.after_commit_opts)
          budget = %{budget | committed: budget.committed + 1}

          case moment.disposition do
            :continue -> advance(state, rtg, moment.run, state.moment_opts, budget)
            {:park, kind, _reason} -> {{:ok, {:parked, kind}}, budget.committed}
          end

        {:error, reason} when reason in [:invalid_commit, :invalid_transition, :not_found] ->
          raise Error.new(
                  :claim_invariant,
                  "commit for claimed run #{inspect(state.lease.run_id)} was rejected",
                  details: %{reason: reason}
                )

        {:error, reason} ->
          {release(state, {:discarded, reason}), budget.committed}
      end
    end

    # A :continue moment at or past the drain budget becomes an immediate
    # drain-budget park, so one long run cannot hold a runner slot forever.
    defp maybe_yield(state, budget, %{disposition: :continue} = moment) do
      count? = state.max_moments != :infinity and budget.committed + 1 >= state.max_moments

      elapsed? =
        state.max_elapsed_ms != :infinity and
          state.monotonic_clock.() - budget.started >= state.max_elapsed_ms

      if count? or elapsed? do
        case Docket.Runtime.Moment.yield(moment, :drain_budget) do
          {:ok, yielded} ->
            yielded

          {:error, reason} ->
            raise Error.new(:claim_invariant, "drain-budget yield was rejected",
                    details: %{reason: reason}
                  )
        end
      else
        moment
      end
    end

    defp maybe_yield(_state, _budget, moment), do: moment

    # -----------------------------------------------------------------
    # Claim hand-back and release
    # -----------------------------------------------------------------

    defp abandon(state, reason) do
      now = state.clock.()
      base_ms = state.abandon_backoff.base_ms
      retry_at = DateTime.add(now, base_ms + state.jitter.(base_ms), :millisecond)
      {:ok, {:abandoned, hand_back(state, now, retry_at), reason}}
    end

    defp host_incompatible(state, error) do
      now = state.clock.()

      case state.runs.abandon_claim(
             state.context,
             :system,
             state.lease.run_id,
             state.lease.claim_token,
             %{
               expected_checkpoint_seq: state.lease.checkpoint_seq,
               now: now,
               retry_at: DateTime.add(now, state.abandon_backoff.base_ms, :millisecond),
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
      retry_at = if DateTime.compare(park.resume_at, now) == :lt, do: now, else: park.resume_at
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

    defp outcome_kind({:ok, {:parked, _kind}}), do: :parked
    defp outcome_kind({:ok, :fence_lost}), do: :fence_lost
    defp outcome_kind({:ok, {:discarded, _reason}}), do: :discarded
    defp outcome_kind({:ok, {:abandoned, _disposition, _reason}}), do: :abandoned
    defp outcome_kind({:ok, {:deferred, _disposition}}), do: :deferred
    defp outcome_kind({:ok, {:host_incompatible, _error}}), do: :host_incompatible
  end
end
