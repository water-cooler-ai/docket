if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.TenantFair do
    @moduledoc """
    Database-authoritative exact-cap admission with bounded cross-tenant rotation.

    The configured default cap only bootstraps an uninitialized database. Once
    persisted, the default and tenant overrides are managed through `Admin`.

    ## Responsibility boundary

    `RingFunction` owns the database-side scheduling state machine: cursor and
    partition authority, ring traversal, bounded candidate discovery, exact row
    locking, authoritative rechecks, run mutation, continuation persistence,
    and service-epoch accounting.

    This module remains the application-side `ClaimPolicy` implementation. It:

    * validates the configured bootstrap cap through `Config`;
    * normalizes runtime timestamps and derives the expired-claim cutoff;
    * builds one data-only `ClaimPolicy.Plan` with the six semantic bind values;
    * resolves the prefix-qualified claim function installed by the migration;
    * decodes the unchanged fourteen public columns into leases and poisoned
      results; and
    * emits bounded claim, poison, steal, age, duration, and contention
      observations after execution.

    `SQL` is the narrow wrapper between these two sides. It invokes the claim function with raw
    tracing disabled, removes internal inspection rows and columns, and orders
    public outcomes by the function's visit and outcome ordinals. `RunStore`
    remains the sole executor of the resulting plan, while `Admin` is the
    separate API for persisted default and per-scope cap state.

    In other words, `RingFunction` decides and transactionally applies *which
    runs are admitted*; this module handles configuration, invocation, public
    decoding, and observability around that decision.
    """

    @behaviour Docket.Postgres.ClaimPolicy

    alias Docket.Postgres.ClaimPolicy.Plan
    alias Docket.Postgres.ClaimPolicy.TenantFair.{Config, RingFunction, SQL}
    alias Docket.Postgres.Storage

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
    def build_plan(
          %{prefix: prefix},
          %{
            now: %DateTime{} = now,
            limit: limit,
            orphan_ttl_ms: ttl,
            max_claim_attempts: max,
            preference: preference
          },
          %Config{default_max_active: default_max_active}
        ) do
      now = normalize_database_datetime(now)
      cutoff = DateTime.add(now, -ttl, :millisecond)
      function = Storage.qualified_table(prefix, RingFunction.name())

      %Plan{
        statement: SQL.statement(function),
        params: [
          now,
          cutoff,
          limit,
          max,
          preference && Atom.to_string(preference),
          default_max_active
        ],
        decoder: %{now: now, orphan_ttl_ms: ttl},
        observation: %{demand: limit, preference: preference}
      }
    end

    @impl true
    def decode([["error", "lock_contention" | _tail]], _decoder, %Config{}) do
      {:error, {:claim_policy_unavailable, :lock_contention}, %{contention_phase: :policy_cursor}}
    end

    def decode([["error", reason | _tail]], _decoder, %Config{}) do
      {:error, {:claim_policy_unavailable, load_error_reason(reason)}, %{}}
    end

    def decode(rows, %{now: now, orphan_ttl_ms: ttl}, %Config{}) when is_list(rows) do
      {leases, poisoned, stats} =
        Enum.reduce(rows, {[], [], @empty_stats}, fn
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
            work_class,
            eligible_at
          ],
          {leases, poisoned, stats} ->
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

            {[lease | leases], poisoned,
             observe_outcome(stats, work_class, eligible_at, now, true)}

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
            work_class,
            eligible_at
          ],
          {leases, poisoned, stats} ->
            result = %{run_id: run_id, poisoned_at: poisoned_at, poison_reason: poison_reason}

            {leases, [result | poisoned],
             observe_outcome(stats, work_class, eligible_at, now, false)}

          row, _acc ->
            raise ArgumentError, "invalid TenantFair claim row: #{inspect(row)}"
        end)

      {:ok, %{leases: Enum.reverse(leases), poisoned: Enum.reverse(poisoned)}, stats}
    end

    @impl true
    def observe(
          %{demand: demand, preference: preference},
          stats,
          {:ok, batch},
          duration,
          %Config{}
        ) do
      fallback? =
        demand == 1 and preference != nil and
          ((preference == :ready and stats.expired_selected > 0) or
             (preference == :expired and stats.ready_selected > 0))

      :telemetry.execute(
        [:docket, :postgres, :run_store, :claim],
        Map.merge(stats, %{
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

    defp observe_outcome(stats, class, eligible_at, now, lease?) do
      age = max(DateTime.diff(now, eligible_at, :millisecond), 0)

      case class do
        "ready" ->
          %{
            stats
            | ready_candidates: stats.ready_candidates + 1,
              ready_selected: stats.ready_selected + 1,
              ready_oldest_age_ms: max(stats.ready_oldest_age_ms, age)
          }

        "expired" ->
          %{
            stats
            | expired_candidates: stats.expired_candidates + 1,
              expired_selected: stats.expired_selected + 1,
              steals: stats.steals + if(lease?, do: 1, else: 0),
              expired_oldest_age_ms: max(stats.expired_oldest_age_ms, age)
          }
      end
    end

    defp load_error_reason("read_only_transaction"), do: :read_only_transaction
    defp load_error_reason("unsupported_isolation"), do: :unsupported_isolation
    defp load_error_reason("lock_contention"), do: :lock_contention
    defp load_error_reason(_reason), do: :unavailable

    defp owner_scope(nil), do: :tenantless
    defp owner_scope(tenant_id) when is_binary(tenant_id), do: {:tenant, tenant_id}

    defp load_uuid!(token) do
      case Ecto.UUID.load(token) do
        {:ok, uuid} -> uuid
        :error -> raise "Postgres returned an invalid claim UUID"
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
