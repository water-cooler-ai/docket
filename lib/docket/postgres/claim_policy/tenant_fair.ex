if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair do
    @moduledoc """
    Exact-cap PostgreSQL admission engine authorized by the prefix gate.

    The client plan performs bounded tenant-blind partition discovery and
    invokes the prefix-owned v1 claim function exactly once. The database
    function owns every authority lock, fresh live count, and run mutation;
    instance configuration is validated data only and is never a claim-time
    policy fallback.
    """

    @behaviour Docket.Postgres.ClaimPolicy

    alias Docket.Postgres.ClaimPolicy.Plan
    alias Docket.Postgres.ClaimPolicy.TenantFair.{Config, Function, Observation, SQL}
    alias Docket.Postgres.Storage

    @function_contract Function.version()
    @empty_stats %{
      ready_candidates: 0,
      expired_candidates: 0,
      ready_selected: 0,
      expired_selected: 0,
      steals: 0,
      ready_oldest_age_ms: 0,
      expired_oldest_age_ms: 0
    }

    @impl true
    def init(options, _context), do: Config.new(options)

    @impl true
    def activation_contract(%Config{}) do
      %{engine: :tenant_fair, function_contract: @function_contract}
    end

    @impl true
    def build_plan(
          %{prefix: prefix, identifiers: %{runs: runs}},
          %{
            now: %DateTime{} = now,
            limit: limit,
            orphan_ttl_ms: ttl,
            max_claim_attempts: max,
            preference: preference
          },
          %Config{}
        ) do
      now = normalize_database_datetime(now)
      cutoff = DateTime.add(now, -ttl, :millisecond)
      function = Storage.qualified_table(prefix, Function.name())

      %Plan{
        statement: SQL.statement(runs, function),
        params: [now, cutoff, limit, max, preference && Atom.to_string(preference)],
        decoder: %{now: now, orphan_ttl_ms: ttl},
        observation: %{
          admission_observation: Observation.plan(),
          demand: limit,
          preference: preference
        }
      }
    end

    @impl true
    def decode(rows, %{now: now, orphan_ttl_ms: ttl}, %Config{}) when is_list(rows) do
      {errors, remaining} = Enum.split_with(rows, &error_row?/1)
      {summaries, outcomes} = Enum.split_with(remaining, &summary_row?/1)

      case {errors, summaries} do
        {[["error", reason | _tail]], []} when outcomes == [] ->
          {:error, {:claim_policy_unavailable, load_error_reason(reason)}, %{}}

        {[], [summary]} ->
          {observation, mode_epoch, function_contract} = decode_observation(summary)
          batch = decode_outcomes(outcomes, ttl)
          stats = decode_stats(summary, observation, now, ttl)

          {:ok, batch,
           Map.merge(stats, %{
             admission_observation: observation,
             mode_epoch: mode_epoch,
             function_contract: function_contract
           })}

        _other ->
          raise ArgumentError, "invalid TenantFair claim-function row set"
      end
    end

    @impl true
    def observe(
          %{demand: demand, preference: preference},
          %{admission_observation: %Observation{} = observation} = stats,
          {:ok, batch},
          duration,
          %Config{}
        ) do
      legacy_stats = Map.take(stats, Map.keys(@empty_stats))

      fallback? =
        demand == 1 and preference != nil and
          ((preference == :ready and observation.expired_leases > 0) or
             (preference == :expired and observation.ready_leases > 0))

      :telemetry.execute(
        [:docket, :postgres, :run_store, :claim],
        Map.merge(legacy_stats, %{
          duration: duration,
          demand: demand,
          leases: length(batch.leases),
          poisoned: length(batch.poisoned),
          claim_attempts: Enum.sum(Enum.map(batch.leases, & &1.claim_attempt))
        }),
        %{preference: preference, fallback: fallback?, result: :ok}
      )

      Enum.each(batch.leases, fn lease ->
        :telemetry.execute(
          [:docket, :postgres, :claim, :attempt],
          %{count: 1, claim_attempts: lease.claim_attempt},
          %{result: if(lease.claim_attempt == 1, do: :acquired, else: :reacquired)}
        )
      end)

      Enum.each(batch.poisoned, fn poison ->
        :telemetry.execute(
          [:docket, :postgres, :claim, :poisoned],
          %{count: 1},
          %{reason: poison_reason(poison.poison_reason)}
        )
      end)

      :ok
    end

    def observe(
          %{demand: demand, preference: preference},
          _decoded,
          {:error, _reason},
          duration,
          %Config{}
        ) do
      :telemetry.execute(
        [:docket, :postgres, :run_store, :claim],
        %{
          duration: duration,
          demand: demand,
          leases: 0,
          poisoned: 0,
          steals: 0,
          claim_attempts: 0
        },
        %{preference: preference, fallback: false, result: :error}
      )

      :ok
    end

    @doc false
    def claim_statement(runs, function) when is_binary(runs) and is_binary(function) do
      SQL.statement(runs, function)
    end

    defp decode_outcomes(rows, ttl) do
      {leases, poisoned} =
        Enum.reduce(rows, {[], []}, fn
          [
            "outcome",
            nil,
            run_id,
            tenant_id,
            graph_id,
            graph_hash,
            checkpoint_seq,
            claim_token,
            claimed_at,
            claim_attempt,
            nil,
            nil,
            _work_class,
            _eligible_at
            | observation_tail
          ],
          {leases, poisoned}
          when length(observation_tail) == 28 ->
            lease = %{
              run_id: run_id,
              owner_scope: owner_scope(tenant_id),
              graph_id: graph_id,
              graph_hash: graph_hash,
              checkpoint_seq: checkpoint_seq,
              claim_token: load_uuid!(claim_token),
              claimed_at: claimed_at,
              claim_attempt: claim_attempt,
              orphan_ttl_ms: ttl
            }

            {[lease | leases], poisoned}

          [
            "outcome",
            nil,
            run_id,
            _tenant_id,
            _graph_id,
            _graph_hash,
            _checkpoint_seq,
            nil,
            nil,
            _claim_attempt,
            %DateTime{} = poisoned_at,
            poison_reason,
            _work_class,
            _eligible_at
            | observation_tail
          ],
          {leases, poisoned}
          when length(observation_tail) == 28 ->
            poison = %{
              run_id: run_id,
              poisoned_at: poisoned_at,
              poison_reason: poison_reason
            }

            {leases, [poison | poisoned]}

          _row, _acc ->
            raise ArgumentError, "invalid TenantFair outcome row"
        end)

      %{leases: Enum.reverse(leases), poisoned: Enum.reverse(poisoned)}
    end

    defp decode_observation([
           "summary",
           nil,
           nil,
           nil,
           nil,
           nil,
           nil,
           nil,
           nil,
           nil,
           nil,
           nil,
           nil,
           nil,
           eligible_partitions,
           locked_partitions,
           skipped_partitions,
           cap_denied_partitions,
           below_preferred_partitions,
           default_policy_partitions,
           override_policy_partitions,
           running_partitions,
           hold_new_partitions,
           drain_partitions,
           preferred_admissions,
           borrowed_admissions,
           ready_leases,
           ready_poisoned,
           expired_leases,
           expired_poisoned,
           candidate_rows_examined,
           under_claimed,
           ready_wait_count,
           ready_wait_sum,
           ready_wait_max,
           expired_wait_count,
           expired_wait_sum,
           expired_wait_max,
           mode_epoch,
           function_contract,
           _ready_candidates,
           _expired_candidates
         ]) do
      unless is_integer(mode_epoch) and mode_epoch > 0 and
               function_contract == @function_contract do
        raise ArgumentError, "TenantFair summary has stale gate evidence"
      end

      observation =
        Observation.new!(%{
          eligible_partitions: eligible_partitions,
          locked_partitions: locked_partitions,
          skipped_partitions: skipped_partitions,
          cap_denied_partitions: cap_denied_partitions,
          below_preferred_partitions: below_preferred_partitions,
          default_policy_partitions: default_policy_partitions,
          override_policy_partitions: override_policy_partitions,
          running_partitions: running_partitions,
          hold_new_partitions: hold_new_partitions,
          drain_partitions: drain_partitions,
          preferred_admissions: preferred_admissions,
          borrowed_admissions: borrowed_admissions,
          ready_leases: ready_leases,
          ready_poisoned: ready_poisoned,
          expired_leases: expired_leases,
          expired_poisoned: expired_poisoned,
          candidate_rows_examined: candidate_rows_examined,
          under_claimed: under_claimed,
          ready_claim_wait_ms_count: ready_wait_count,
          ready_claim_wait_ms_sum: ready_wait_sum,
          ready_claim_wait_ms_max: ready_wait_max,
          expired_recovery_wait_ms_count: expired_wait_count,
          expired_recovery_wait_ms_sum: expired_wait_sum,
          expired_recovery_wait_ms_max: expired_wait_max
        })

      {observation, mode_epoch, function_contract}
    end

    defp decode_observation(_row), do: raise(ArgumentError, "invalid TenantFair summary row")

    defp decode_stats(summary, observation, _now, ttl) do
      ready_candidates = Enum.at(summary, 40)
      expired_candidates = Enum.at(summary, 41)

      %{
        @empty_stats
        | ready_candidates: ready_candidates,
          expired_candidates: expired_candidates,
          ready_selected: observation.ready_leases + observation.ready_poisoned,
          expired_selected: observation.expired_leases + observation.expired_poisoned,
          steals: observation.expired_leases,
          ready_oldest_age_ms: observation.ready_claim_wait_ms_max,
          expired_oldest_age_ms: observation.expired_recovery_wait_ms_max + ttl
      }
    end

    defp error_row?(["error", _reason | _tail]), do: true
    defp error_row?(_row), do: false
    defp summary_row?(["summary" | _tail]), do: true
    defp summary_row?(_row), do: false

    defp load_error_reason("read_only_transaction"), do: :read_only_transaction
    defp load_error_reason("unsupported_isolation"), do: :unsupported_isolation
    defp load_error_reason("lock_contention"), do: :lock_contention
    defp load_error_reason("inactive_engine"), do: :inactive_engine
    defp load_error_reason("not_ready"), do: :not_ready
    defp load_error_reason("not_initialized"), do: :not_initialized
    defp load_error_reason("function_contract_mismatch"), do: :function_contract_mismatch
    defp load_error_reason(_other), do: raise(ArgumentError, "invalid TenantFair error row")

    defp owner_scope(nil), do: :tenantless
    defp owner_scope(tenant_id) when is_binary(tenant_id), do: {:tenant, tenant_id}

    defp load_uuid!(token) do
      case Ecto.UUID.load(token) do
        {:ok, uuid} -> uuid
        :error -> raise ArgumentError, "Postgres returned an invalid claim UUID"
      end
    end

    defp poison_reason("max_claim_attempts_exceeded"), do: :max_claim_attempts
    defp poison_reason("max_claim_abandons_exceeded"), do: :max_claim_abandons
    defp poison_reason(_other), do: :other

    defp normalize_database_datetime(%DateTime{} = datetime) do
      datetime
      |> DateTime.to_unix(:microsecond)
      |> DateTime.from_unix!(:microsecond)
    end
  end
end
