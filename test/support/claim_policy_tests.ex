if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.AlternateClaimPolicy do
    @moduledoc false

    @behaviour Docket.Postgres.ClaimPolicy

    alias Docket.Postgres.ClaimPolicy.Plan

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
    def init([marker: marker], context) do
      relay({:alternate_claim_policy, :init, marker, context})
      {:ok, %{marker: marker}}
    end

    def init(options, _context), do: {:error, {:expected_marker, options}}

    @impl true
    def build_plan(
          %{identifiers: %{runs: table}},
          %{now: now, limit: limit, orphan_ttl_ms: ttl, preference: preference},
          %{marker: marker}
        ) do
      relay({:alternate_claim_policy, :build_plan, marker, self()})

      %Plan{
        statement: """
        /* independent alternate claim plan: #{marker} */
        WITH candidates AS MATERIALIZED (
          SELECT id
          FROM #{table}
          WHERE status = 'running'
            AND poisoned_at IS NULL
            AND claim_token IS NULL
            AND wake_at <= $1
          ORDER BY wake_at, id
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        ),
        updated AS (
          UPDATE #{table} AS runs
          SET claim_token = gen_random_uuid(),
              claimed_at = $1,
              wake_at = NULL,
              claim_attempts = runs.claim_attempts + 1
          FROM candidates
          WHERE runs.id = candidates.id
          RETURNING
            runs.run_id,
            runs.tenant_id,
            runs.graph_id,
            runs.graph_hash,
            runs.checkpoint_seq,
            runs.claim_token,
            runs.claimed_at,
            runs.claim_attempts
        )
        SELECT * FROM updated ORDER BY run_id
        """,
        params: [now, limit],
        decoder: %{orphan_ttl_ms: ttl},
        observation: %{demand: limit, preference: preference, marker: marker}
      }
    end

    @impl true
    def decode(rows, %{orphan_ttl_ms: ttl}, %{marker: marker}) do
      relay({:alternate_claim_policy, :decode, marker, self()})

      leases =
        Enum.map(rows, fn [
                            run_id,
                            tenant_id,
                            graph_id,
                            graph_hash,
                            checkpoint_seq,
                            claim_token,
                            claimed_at,
                            claim_attempt
                          ] ->
          %{
            run_id: run_id,
            owner_scope: if(tenant_id, do: {:tenant, tenant_id}, else: :tenantless),
            graph_id: graph_id,
            graph_hash: graph_hash,
            checkpoint_seq: checkpoint_seq,
            claim_token: Ecto.UUID.load!(claim_token),
            claimed_at: claimed_at,
            claim_attempt: claim_attempt,
            orphan_ttl_ms: ttl
          }
        end)

      count = length(leases)
      stats = %{@empty_stats | ready_candidates: count, ready_selected: count}
      {:ok, %{leases: leases, poisoned: []}, stats}
    end

    @impl true
    def observe(
          %{demand: demand, preference: preference},
          stats,
          {:ok, batch},
          duration,
          %{marker: marker}
        ) do
      relay({:alternate_claim_policy, :observe, marker, :ok})

      :telemetry.execute(
        [:docket, :postgres, :run_store, :claim],
        Map.merge(stats, %{
          duration: duration,
          demand: demand,
          leases: length(batch.leases),
          poisoned: 0,
          claim_attempts: Enum.sum(Enum.map(batch.leases, & &1.claim_attempt))
        }),
        %{preference: preference, fallback: false, result: :ok}
      )

      Enum.each(batch.leases, fn lease ->
        :telemetry.execute(
          [:docket, :postgres, :claim, :attempt],
          %{count: 1, claim_attempts: lease.claim_attempt},
          %{result: if(lease.claim_attempt == 1, do: :acquired, else: :reacquired)}
        )
      end)

      :ok
    end

    def observe(
          %{demand: demand, preference: preference},
          nil,
          {:error, _reason},
          duration,
          %{marker: marker}
        ) do
      relay({:alternate_claim_policy, :observe, marker, :error})

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

    defp relay(message) do
      if pid = Process.whereis(:docket_claim_policy_relay), do: send(pid, message)
      :ok
    end
  end

  defmodule Docket.Test.ClaimPolicyTests do
    @moduledoc false

    defmacro __using__(opts) do
      implementation = Keyword.fetch!(opts, :implementation)
      implementation_opts = Keyword.get(opts, :options, [])

      quote bind_quoted: [
              implementation: implementation,
              implementation_opts: implementation_opts
            ] do
        @claim_policy_implementation implementation
        @claim_policy_options implementation_opts
        @now ~U[2026-07-15 12:00:00.000000Z]

        test "builds one data-only plan from normalized inputs and quoted identifiers" do
          context = %{repo: __MODULE__.ContractRepo, prefix: "policy_contract"}

          claim_policy =
            Docket.Postgres.ClaimPolicy.new(
              [implementation: @claim_policy_implementation] ++ @claim_policy_options,
              context
            )

          effective =
            Docket.Postgres.ClaimPolicy.effective_policy!(%{
              now: @now,
              limit: 7,
              orphan_ttl_ms: 5_000,
              max_claim_attempts: 4,
              preference: :expired
            })

          plan = Docket.Postgres.ClaimPolicy.build_plan(claim_policy, context, effective)

          assert %Docket.Postgres.ClaimPolicy.Plan{} = plan
          assert plan.statement =~ ~s("policy_contract"."docket_runs")
          assert is_list(plan.params)
          assert plan.observation.demand == 7
          refute contains_function?(plan)
        end

        defp contains_function?(value) when is_function(value), do: true

        defp contains_function?(value) when is_list(value),
          do: Enum.any?(value, &contains_function?/1)

        defp contains_function?(value) when is_tuple(value),
          do: value |> Tuple.to_list() |> contains_function?()

        defp contains_function?(%_{} = value),
          do: value |> Map.from_struct() |> contains_function?()

        defp contains_function?(value) when is_map(value) do
          Enum.any?(value, fn {key, nested} ->
            contains_function?(key) or contains_function?(nested)
          end)
        end

        defp contains_function?(_value), do: false
      end
    end
  end
end
