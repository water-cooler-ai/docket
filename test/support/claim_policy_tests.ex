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
    def decode([["__bounded_policy_error__"]], _decoder, %{marker: marker}) do
      relay({:alternate_claim_policy, :decode, marker, self()})
      {:error, {:claim_policy_unavailable, :lock_contention}, %{gate: :unavailable}}
    end

    def decode([["__invalid_policy_error__"]], _decoder, %{marker: marker}) do
      relay({:alternate_claim_policy, :decode, marker, self()})
      {:error, {:invalid_reason, self()}, %{}}
    end

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

  defmodule Docket.Test.TenantFairConfigClaimPolicy do
    @moduledoc false

    @behaviour Docket.Postgres.ClaimPolicy

    alias Docket.Postgres.ClaimPolicy.TenantFair.Config

    @marker :tenant_fair_config_contract

    @impl true
    def init(options, context) do
      with {:ok, config} <- Config.new(options) do
        relay({:tenant_fair_config_claim_policy, :init, config, context})
        {:ok, config}
      end
    end

    @impl true
    def build_plan(context, policy, %Config{} = config) do
      relay({:tenant_fair_config_claim_policy, :build_plan, config, self()})
      Docket.Test.AlternateClaimPolicy.build_plan(context, policy, %{marker: @marker})
    end

    @impl true
    def decode(rows, decoder, %Config{} = config) do
      relay({:tenant_fair_config_claim_policy, :decode, config, self()})
      Docket.Test.AlternateClaimPolicy.decode(rows, decoder, %{marker: @marker})
    end

    @impl true
    def observe(plan, decoded, result, duration, %Config{} = config) do
      relay({:tenant_fair_config_claim_policy, :observe, config, result})

      Docket.Test.AlternateClaimPolicy.observe(
        plan,
        decoded,
        result,
        duration,
        %{marker: @marker}
      )
    end

    defp relay(message) do
      if pid = Process.whereis(:docket_claim_policy_relay), do: send(pid, message)
      :ok
    end
  end

  defmodule Docket.Test.ObservedClaimPolicy do
    @moduledoc false

    @behaviour Docket.Postgres.ClaimPolicy

    alias Docket.Postgres.ClaimPolicy.Plan
    alias Docket.Postgres.ClaimPolicy.TenantFair.Observation

    @impl true
    def init(options, _context) do
      {:ok,
       %{
         batch: Keyword.fetch!(options, :batch),
         observation: Keyword.get(options, :observation),
         declare?: Keyword.get(options, :declare?, true),
         decode?: Keyword.get(options, :decode?, true),
         observe: Keyword.get(options, :observe, :ok)
       }}
    end

    @impl true
    def build_plan(_context, _policy, state) do
      observation =
        if state.declare?,
          do: %{admission_observation: Observation.plan(), private_plan_marker: :retained},
          else: %{private_plan_marker: :retained}

      %Plan{
        statement: "SELECT 1",
        params: [],
        decoder: %{},
        observation: observation
      }
    end

    @impl true
    def decode(_rows, _decoder, state) do
      observation =
        if state.decode?,
          do: %{admission_observation: state.observation, private_decode_marker: :retained},
          else: %{private_decode_marker: :retained}

      {:ok, state.batch, observation}
    end

    @impl true
    def observe(_plan, _decoded, _result, _duration, %{observe: :raise}) do
      raise "TenantFair observer failed"
    end

    def observe(_plan, _decoded, _result, _duration, _state), do: :ok
  end

  defmodule Docket.Test.ClaimPolicyTests do
    @moduledoc false

    @callback rows(Docket.Postgres.ClaimPolicy.claim_batch()) :: [list()]
    @callback invalid_rows() :: [list()]
    @callback policy_error_rows() :: [list()]
    @callback invalid_policy_error_rows() :: [list()]
    @callback detailed_observation?() :: boolean()

    @optional_callbacks policy_error_rows: 0,
                        invalid_policy_error_rows: 0,
                        detailed_observation?: 0

    defmacro __using__(opts) do
      implementation = Keyword.fetch!(opts, :implementation)
      implementation_opts = Keyword.get(opts, :options, [])
      fixture = Keyword.fetch!(opts, :fixture)

      quote bind_quoted: [
              implementation: implementation,
              implementation_opts: implementation_opts,
              fixture: fixture
            ] do
        @claim_policy_implementation implementation
        @claim_policy_options implementation_opts
        @claim_policy_fixture fixture
        @now ~U[2026-07-15 12:00:00.000000Z]
        @orphan_ttl_ms 5_000

        test "constructs and resolves the selected implementation" do
          {claim_policy, context} = contract_policy()

          assert Docket.Postgres.ClaimPolicy.implementation(claim_policy) ==
                   @claim_policy_implementation

          assert Docket.Postgres.ClaimPolicy.resolve(context) === claim_policy
        end

        test "builds one data-only single-statement plan from normalized inputs" do
          {claim_policy, context} = contract_policy()

          plan =
            Docket.Postgres.ClaimPolicy.build_plan(claim_policy, context, effective_policy())

          assert %Docket.Postgres.ClaimPolicy.Plan{} = plan
          assert String.trim(plan.statement) != ""
          refute String.contains?(plan.statement, ";")
          assert is_list(plan.params)
          assert plan.demand == 7
          refute contains_function?(plan)
        end

        test "rejects invalid portable runtime input before plan construction" do
          for invalid <- [
                %{now: @now, limit: 0, orphan_ttl_ms: 1_000, max_claim_attempts: 3},
                %{now: @now, limit: 1, orphan_ttl_ms: -1, max_claim_attempts: 3},
                %{now: @now, limit: 1, orphan_ttl_ms: 1_000, max_claim_attempts: 0},
                %{
                  now: @now,
                  limit: 1,
                  orphan_ttl_ms: 1_000,
                  max_claim_attempts: 3,
                  preference: :sideways
                }
              ] do
            assert_raise ArgumentError, fn ->
              Docket.Postgres.ClaimPolicy.effective_policy!(invalid)
            end
          end
        end

        test "decodes the suite-owned portable batch exactly" do
          {claim_policy, context} = contract_policy()

          plan =
            Docket.Postgres.ClaimPolicy.build_plan(claim_policy, context, effective_policy())

          expected_batch = contract_batch()
          rows = @claim_policy_fixture.rows(expected_batch)

          assert {:ok, ^expected_batch, observation} =
                   Docket.Postgres.ClaimPolicy.decode(claim_policy, plan, rows)

          assert is_map(observation)
          assert map_size(observation) <= 32
          refute contains_function?(observation)
        end

        test "keeps fixture-identified decoder failures in the portable error contract" do
          {claim_policy, context} = contract_policy()

          plan =
            Docket.Postgres.ClaimPolicy.build_plan(claim_policy, context, effective_policy())

          assert {:error, {:claim_policy_decode_failed, _reason}} =
                   Docket.Postgres.ClaimPolicy.decode(
                     claim_policy,
                     plan,
                     @claim_policy_fixture.invalid_rows()
                   )
        end

        test "accepts the alternate implementation's bounded data-only policy error variant" do
          if function_exported?(@claim_policy_fixture, :policy_error_rows, 0) do
            {claim_policy, context} = contract_policy()

            plan =
              Docket.Postgres.ClaimPolicy.build_plan(claim_policy, context, effective_policy())

            assert {:error, {:claim_policy_unavailable, :lock_contention}, %{gate: :unavailable}} =
                     Docket.Postgres.ClaimPolicy.decode(
                       claim_policy,
                       plan,
                       apply(@claim_policy_fixture, :policy_error_rows, [])
                     )
          end
        end

        test "rejects the alternate implementation's non-data policy error reason" do
          if function_exported?(@claim_policy_fixture, :invalid_policy_error_rows, 0) do
            {claim_policy, context} = contract_policy()

            plan =
              Docket.Postgres.ClaimPolicy.build_plan(claim_policy, context, effective_policy())

            assert {:error, {:claim_policy_decode_failed, {:invalid_return, _invalid}}} =
                     Docket.Postgres.ClaimPolicy.decode(
                       claim_policy,
                       plan,
                       apply(@claim_policy_fixture, :invalid_policy_error_rows, [])
                     )
          end
        end

        test "identifies the selected implementation in exact success and error telemetry" do
          {claim_policy, context} = contract_policy()

          plan =
            Docket.Postgres.ClaimPolicy.build_plan(claim_policy, context, effective_policy())

          expected_batch = contract_batch()
          rows = @claim_policy_fixture.rows(expected_batch)

          {:ok, ^expected_batch, observation} =
            Docket.Postgres.ClaimPolicy.decode(claim_policy, plan, rows)

          handler = "claim-policy-contract-#{System.unique_integer([:positive])}"

          :telemetry.attach_many(
            handler,
            [
              [:docket, :postgres, :claim_policy, :admission],
              [:docket, :postgres, :claim_policy, :admission, :observation]
            ],
            &Docket.Test.TelemetryRelay.raw/4,
            self()
          )

          on_exit(fn -> :telemetry.detach(handler) end)

          assert :ok =
                   Docket.Postgres.ClaimPolicy.observe(
                     claim_policy,
                     plan,
                     observation,
                     {:ok, expected_batch},
                     System.monotonic_time()
                   )

          assert_receive {[:docket, :postgres, :claim_policy, :admission], success_measurements,
                          %{
                            implementation: @claim_policy_implementation,
                            result: :ok
                          } = success_metadata}

          assert success_metadata == %{
                   implementation: @claim_policy_implementation,
                   result: :ok
                 }

          assert %{
                   demand: 7,
                   duration: success_duration,
                   leases: success_leases,
                   poisoned: success_poisoned
                 } = success_measurements

          assert map_size(success_measurements) == 4
          assert is_integer(success_duration) and success_duration >= 0
          assert success_leases == length(expected_batch.leases)
          assert success_poisoned == length(expected_batch.poisoned)

          if function_exported?(@claim_policy_fixture, :detailed_observation?, 0) and
               @claim_policy_fixture.detailed_observation?() do
            assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                            detail_measurements,
                            %{
                              implementation: @claim_policy_implementation,
                              result: :ok,
                              admission_class: :none,
                              observation_status: :available
                            }}

            assert %{
                     preferred_admissions: 0,
                     borrowed_admissions: 0,
                     below_preferred_partitions: 0
                   } = detail_measurements
          end

          assert :ok =
                   Docket.Postgres.ClaimPolicy.observe(
                     claim_policy,
                     plan,
                     nil,
                     {:error, :admission_failed},
                     System.monotonic_time()
                   )

          assert_receive {[:docket, :postgres, :claim_policy, :admission], error_measurements,
                          %{
                            implementation: @claim_policy_implementation,
                            result: :error
                          } = error_metadata}

          assert error_metadata == %{
                   implementation: @claim_policy_implementation,
                   result: :error
                 }

          assert %{demand: 7, duration: error_duration, leases: 0, poisoned: 0} =
                   error_measurements

          assert map_size(error_measurements) == 4
          assert is_integer(error_duration) and error_duration >= 0

          if @claim_policy_fixture.detailed_observation?() do
            assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                            %{demand: 7, duration: detail_error_duration},
                            %{
                              implementation: @claim_policy_implementation,
                              result: :error,
                              admission_class: :none,
                              observation_status: :unavailable
                            }}

            assert is_integer(detail_error_duration) and detail_error_duration >= 0
          else
            refute_receive {[:docket, :postgres, :claim_policy, :admission, :observation], _, _}
          end
        end

        defp contract_policy do
          root = %{repo: __MODULE__.ContractRepo, prefix: "policy_contract"}

          claim_policy =
            Docket.Postgres.ClaimPolicy.new(
              [implementation: @claim_policy_implementation] ++ @claim_policy_options,
              root
            )

          {claim_policy, Map.put(root, :claim_policy, claim_policy)}
        end

        defp effective_policy do
          Docket.Postgres.ClaimPolicy.effective_policy!(%{
            now: @now,
            limit: 7,
            orphan_ttl_ms: @orphan_ttl_ms,
            max_claim_attempts: 4,
            preference: :expired
          })
        end

        defp contract_batch do
          %{
            leases: [
              %{
                run_id: "contract-lease",
                owner_scope: :tenantless,
                graph_id: "contract-graph",
                graph_hash: "contract-hash",
                checkpoint_seq: 6,
                claim_token: "00000000-0000-0000-0000-000000000055",
                claimed_at: @now,
                claim_attempt: 2,
                orphan_ttl_ms: @orphan_ttl_ms
              }
            ],
            poisoned: []
          }
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

  defmodule Docket.Test.LegacyClaimPolicyContract do
    @moduledoc false
    @behaviour Docket.Test.ClaimPolicyTests

    @impl true
    def rows(%{leases: [lease], poisoned: []}) do
      {:ok, claim_token} = Ecto.UUID.dump(lease.claim_token)

      [
        [
          lease.run_id,
          nil,
          lease.graph_id,
          lease.graph_hash,
          lease.checkpoint_seq,
          claim_token,
          lease.claimed_at,
          lease.claim_attempt,
          nil,
          nil,
          "expired",
          DateTime.add(lease.claimed_at, -3, :second),
          0,
          1
        ]
      ]
    end

    @impl true
    def invalid_rows, do: [[:invalid_legacy_row]]

    @impl true
    def detailed_observation?, do: false
  end

  defmodule Docket.Test.AlternateClaimPolicyContract do
    @moduledoc false
    @behaviour Docket.Test.ClaimPolicyTests

    @impl true
    def rows(%{leases: [lease], poisoned: []}) do
      {:ok, claim_token} = Ecto.UUID.dump(lease.claim_token)

      [
        [
          lease.run_id,
          nil,
          lease.graph_id,
          lease.graph_hash,
          lease.checkpoint_seq,
          claim_token,
          lease.claimed_at,
          lease.claim_attempt
        ]
      ]
    end

    @impl true
    def invalid_rows, do: [[:invalid_alternate_row]]

    @impl true
    def policy_error_rows, do: [["__bounded_policy_error__"]]

    @impl true
    def invalid_policy_error_rows, do: [["__invalid_policy_error__"]]

    @impl true
    def detailed_observation?, do: false
  end

  defmodule Docket.Test.TenantFairClaimPolicyContract do
    @moduledoc false
    @behaviour Docket.Test.ClaimPolicyTests

    @impl true
    def rows(%{leases: [lease], poisoned: []}) do
      {:ok, claim_token} = Ecto.UUID.dump(lease.claim_token)

      outcome =
        [
          "outcome",
          nil,
          lease.run_id,
          nil,
          lease.graph_id,
          lease.graph_hash,
          lease.checkpoint_seq,
          claim_token,
          lease.claimed_at,
          lease.claim_attempt,
          nil,
          nil,
          "expired",
          DateTime.add(lease.claimed_at, -8, :second)
        ] ++ List.duplicate(nil, 28)

      summary =
        [
          "summary",
          nil
        ] ++
          List.duplicate(nil, 12) ++
          [
            1,
            1,
            0,
            0,
            0,
            1,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            1,
            0,
            0,
            0,
            0,
            1,
            3_000,
            3_000,
            1,
            1,
            0,
            1
          ]

      [outcome, summary]
    end

    @impl true
    def invalid_rows, do: [["invalid_tenant_fair_row"]]

    @impl true
    def detailed_observation?, do: true
  end
end
