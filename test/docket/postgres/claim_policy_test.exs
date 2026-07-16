if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  for %{implementation: implementation, options: options, fixture: fixture} <-
        Docket.Test.ClaimPolicyMatrix.implementations() do
    defmodule Module.concat(implementation, ContractTest) do
      use ExUnit.Case, async: true

      use Docket.Test.ClaimPolicyTests,
        implementation: implementation,
        options: options,
        fixture: fixture
    end
  end

  defmodule Docket.Postgres.ClaimPolicyTest do
    use ExUnit.Case, async: false

    alias Docket.Postgres.ClaimPolicy
    alias Docket.Postgres.ClaimPolicy.Plan
    alias Docket.Postgres.ClaimPolicy.TenantFair.Config
    alias Docket.Postgres.ClaimPolicy.TenantFair.Observation

    @now ~U[2026-07-15 12:00:00.000000Z]
    @maximum_integer 2_147_483_647
    @tenant_fair_options [
      partition_by: :tenant_id,
      default_preferred_active: 2,
      default_max_active: 4,
      default_weight: 1
    ]

    defmodule MissingCallbacks do
    end

    defmodule RejectingImplementation do
      def init(_options, _context), do: {:error, :invalid_rollout}
      def build_plan(_context, _policy, _state), do: :unreachable
      def decode(_rows, _decoder, _state), do: :unreachable
      def observe(_plan, _decoded, _result, _duration, _state), do: :ok
    end

    defmodule InvalidPlanImplementation do
      def init([], _context), do: {:ok, nil}

      def build_plan(_context, _policy, nil) do
        %Plan{statement: "", params: [], decoder: fn -> :escape end, observation: %{}}
      end

      def decode(_rows, _decoder, nil), do: {:ok, %{leases: [], poisoned: []}, %{}}
      def observe(_plan, _decoded, _result, _duration, nil), do: :ok
    end

    defmodule MultiStatementImplementation do
      def init([], _context), do: {:ok, nil}

      def build_plan(_context, _policy, nil) do
        %Plan{
          statement: "SELECT 1; SELECT 2",
          params: [],
          decoder: %{},
          observation: %{demand: 1}
        }
      end

      def decode(_rows, _decoder, nil), do: {:ok, %{leases: [], poisoned: []}, %{}}
      def observe(_plan, _decoded, _result, _duration, nil), do: :ok
    end

    defmodule FunctionDecoderImplementation do
      def init([], _context), do: {:ok, nil}

      def build_plan(_context, _policy, nil) do
        %Plan{
          statement: "SELECT 1",
          params: [],
          decoder: fn -> :escape end,
          observation: %{demand: 1}
        }
      end

      def decode(_rows, _decoder, nil), do: {:ok, %{leases: [], poisoned: []}, %{}}
      def observe(_plan, _decoded, _result, _duration, nil), do: :ok
    end

    defmodule FailingCallbackImplementation do
      def init([mode: mode], _context), do: {:ok, mode}

      def build_plan(_context, _policy, _mode) do
        %Plan{statement: "SELECT 1", params: [], decoder: %{}, observation: %{}}
      end

      def decode(_rows, _decoder, :decode), do: raise("decoder failed")
      def decode(_rows, _decoder, _mode), do: {:ok, %{leases: [], poisoned: []}, %{}}

      def observe(_plan, _decoded, _result, _duration, :observe), do: raise("observer failed")
      def observe(_plan, _decoded, _result, _duration, _mode), do: :ok
    end

    setup do
      Process.register(self(), :docket_claim_policy_relay)

      on_exit(fn ->
        if Process.whereis(:docket_claim_policy_relay),
          do: Process.unregister(:docket_claim_policy_relay)
      end)

      :ok
    end

    test "rejects incomplete implementations and invalid implementation configuration" do
      context = %{repo: __MODULE__.Repo, prefix: nil}

      assert_raise ArgumentError, ~r/missing init\/2, build_plan\/3, decode\/3, observe\/5/, fn ->
        ClaimPolicy.new([implementation: MissingCallbacks], context)
      end

      assert_raise ArgumentError, ~r/rejected its configuration: :invalid_rollout/, fn ->
        ClaimPolicy.new([implementation: RejectingImplementation], context)
      end

      assert_raise ArgumentError, ~r/:claim_policy must be a keyword list/, fn ->
        ClaimPolicy.new(:legacy, context)
      end

      assert_raise ArgumentError, ~r/duplicate keys: \[:implementation\]/, fn ->
        ClaimPolicy.new(
          [implementation: RejectingImplementation, implementation: MissingCallbacks],
          context
        )
      end
    end

    test "normalizes the future TenantFair configuration into one bounded data-only value" do
      assert {:ok,
              %Config{
                partition_by: :tenant_id,
                default_preferred_active: 2,
                default_max_active: 4,
                default_weight: 1,
                borrowing: false
              } = expected} = Config.new(@tenant_fair_options)

      context = %{repo: __MODULE__.Repo, prefix: "tenant_fair_config"}

      claim_policy =
        ClaimPolicy.new(
          [implementation: Docket.Test.TenantFairConfigClaimPolicy] ++ @tenant_fair_options,
          context
        )

      assert_receive {:tenant_fair_config_claim_policy, :init, ^expected,
                      %{
                        prefix: "tenant_fair_config",
                        identifiers: %{runs: ~s("tenant_fair_config"."docket_runs")}
                      }}

      resolved = Map.put(context, :claim_policy, claim_policy)
      assert ClaimPolicy.resolve(resolved) === claim_policy
      refute_receive {:tenant_fair_config_claim_policy, :init, _, _}

      assert {:ok,
              %Config{
                default_preferred_active: @maximum_integer,
                default_max_active: @maximum_integer,
                default_weight: @maximum_integer,
                borrowing: true
              }} =
               Config.new(
                 partition_by: :tenant_id,
                 default_preferred_active: @maximum_integer,
                 default_max_active: @maximum_integer,
                 default_weight: @maximum_integer,
                 borrowing: true
               )
    end

    test "rejects malformed TenantFair configuration with ClaimPolicy context" do
      invalid = [
        {Keyword.put(@tenant_fair_options, :partition_by, :scope_key), ":partition_by"},
        {Keyword.put(@tenant_fair_options, :default_preferred_active, -1),
         ":default_preferred_active"},
        {Keyword.put(@tenant_fair_options, :default_preferred_active, 1.0),
         ":default_preferred_active"},
        {Keyword.put(@tenant_fair_options, :default_preferred_active, true),
         ":default_preferred_active"},
        {Keyword.put(@tenant_fair_options, :default_max_active, @maximum_integer + 1),
         ":default_max_active"},
        {Keyword.put(@tenant_fair_options, :default_max_active, nil), ":default_max_active"},
        {Keyword.put(@tenant_fair_options, :default_weight, 0), ":default_weight"},
        {Keyword.put(@tenant_fair_options, :default_weight, @maximum_integer + 1),
         ":default_weight"},
        {Keyword.put(@tenant_fair_options, :default_weight, 1.5), ":default_weight"},
        {Keyword.put(@tenant_fair_options, :borrowing, :yes), ":borrowing"},
        {Keyword.put(@tenant_fair_options, :default_preferred_active, 5),
         ":invalid_relationship"},
        {@tenant_fair_options ++ [unexpected: :value], ":unknown_options"}
      ]

      for {options, fragment} <- invalid do
        error =
          assert_raise ArgumentError, fn ->
            ClaimPolicy.new(
              [implementation: Docket.Test.TenantFairConfigClaimPolicy] ++ options,
              %{repo: __MODULE__.Repo, prefix: nil}
            )
          end

        assert Exception.message(error) =~ fragment
        assert Exception.message(error) =~ "rejected its configuration"
      end

      for missing <- [
            :partition_by,
            :default_preferred_active,
            :default_max_active,
            :default_weight
          ] do
        error =
          assert_raise ArgumentError, fn ->
            ClaimPolicy.new(
              [implementation: Docket.Test.TenantFairConfigClaimPolicy] ++
                Keyword.delete(@tenant_fair_options, missing),
              %{repo: __MODULE__.Repo, prefix: nil}
            )
          end

        assert Exception.message(error) =~ ":missing_options"
        assert Exception.message(error) =~ inspect(missing)
      end

      duplicate_error =
        assert_raise ArgumentError, fn ->
          ClaimPolicy.new(
            [implementation: Docket.Test.TenantFairConfigClaimPolicy] ++
              @tenant_fair_options ++ [default_weight: 2],
            %{repo: __MODULE__.Repo, prefix: nil}
          )
        end

      assert Exception.message(duplicate_error) =~ "rejected its configuration"
      assert Exception.message(duplicate_error) =~ ":duplicate_options"
      assert Exception.message(duplicate_error) =~ ":default_weight"

      assert {:error, {:duplicate_options, [:default_weight]}} =
               Config.new(@tenant_fair_options ++ [default_weight: 2])

      assert {:error, {:expected_keyword_list, %{partition_by: :tenant_id}}} =
               Config.new(%{partition_by: :tenant_id})
    end

    test "keeps dormant maximum capacity valid when borrowing is disabled" do
      assert {:ok,
              %Config{
                default_preferred_active: 0,
                default_max_active: 0,
                default_weight: 1,
                borrowing: false
              }} =
               Config.new(
                 partition_by: :tenant_id,
                 default_preferred_active: 0,
                 default_max_active: 0,
                 default_weight: 1
               )

      assert {:ok,
              %Config{
                default_preferred_active: 0,
                default_max_active: 4,
                default_weight: 1,
                borrowing: false
              }} =
               Config.new(
                 partition_by: :tenant_id,
                 default_preferred_active: 0,
                 default_max_active: 4,
                 default_weight: 1,
                 borrowing: false
               )
    end

    test "validates neutral runtime input before any plan is built" do
      for invalid <- [
            %{now: @now, limit: 0, orphan_ttl_ms: 1_000, max_claim_attempts: 3},
            %{now: @now, limit: 1, orphan_ttl_ms: -1, max_claim_attempts: 3},
            %{
              now: @now,
              limit: 1,
              orphan_ttl_ms: 1_000,
              max_claim_attempts: 3,
              preference: :sideways
            }
          ] do
        assert_raise ArgumentError, fn -> ClaimPolicy.effective_policy!(invalid) end
      end
    end

    test "normalizes now before an independent implementation builds its plan" do
      context = %{repo: __MODULE__.Repo, prefix: nil}

      claim_policy =
        ClaimPolicy.new(
          [implementation: Docket.Test.AlternateClaimPolicy, marker: :normalized_clock],
          context
        )

      non_utc_high_precision = %{
        ~U[2026-07-15 12:00:00.123456Z]
        | hour: 14,
          time_zone: "Etc/GMT-2",
          zone_abbr: "+02",
          utc_offset: 7_200,
          microsecond: {123_456, 9}
      }

      effective =
        ClaimPolicy.effective_policy!(%{
          now: non_utc_high_precision,
          limit: 2,
          orphan_ttl_ms: 1_000,
          max_claim_attempts: 3,
          preference: :expired
        })

      assert effective.now == ~U[2026-07-15 12:00:00.123456Z]

      plan = ClaimPolicy.build_plan(claim_policy, context, effective)
      assert hd(plan.params) == effective.now
    end

    test "rejects capability-bearing or multi-statement plans before execution" do
      context = %{repo: __MODULE__.Repo, prefix: nil}
      policy = ClaimPolicy.new([implementation: InvalidPlanImplementation], context)

      assert_raise ArgumentError, ~r/plan requires a non-empty SQL statement/, fn ->
        ClaimPolicy.build_plan(policy, context, effective_policy())
      end

      assert_raise ArgumentError, ~r/one SQL statement/, fn ->
        build_plan_from(MultiStatementImplementation)
      end

      assert_raise ArgumentError, ~r/data-only decoder contract/, fn ->
        build_plan_from(FunctionDecoderImplementation)
      end
    end

    test "initialization receives normalized PostgreSQL context once for one resolved value" do
      context = %{repo: __MODULE__.Repo, prefix: "claims"}

      claim_policy =
        ClaimPolicy.new(
          [implementation: Docket.Test.AlternateClaimPolicy, marker: :init_contract],
          context
        )

      assert_receive {:alternate_claim_policy, :init, :init_contract,
                      %{
                        prefix: "claims",
                        identifiers: %{runs: ~s("claims"."docket_runs")}
                      }}

      assert ClaimPolicy.resolve(Map.put(context, :claim_policy, claim_policy)) === claim_policy
      refute_receive {:alternate_claim_policy, :init, :init_contract, _}
    end

    test "admission rejects unresolved and malformed contexts" do
      assert_raise ArgumentError, ~r/requires a resolved ClaimPolicy/, fn ->
        ClaimPolicy.resolve(__MODULE__.Repo)
      end

      assert_raise ArgumentError, ~r/requires a resolved ClaimPolicy/, fn ->
        ClaimPolicy.resolve(%{repo: __MODULE__.Repo})
      end

      assert_raise ArgumentError, ~r/invalid resolved ClaimPolicy/, fn ->
        ClaimPolicy.resolve(%{repo: __MODULE__.Repo, claim_policy: :legacy})
      end
    end

    test "the selected implementation remains bound to plan decoding and generic telemetry" do
      context = %{repo: __MODULE__.Repo, prefix: nil}

      claim_policy =
        ClaimPolicy.new(
          [implementation: Docket.Test.AlternateClaimPolicy, marker: :decode_contract],
          context
        )

      plan = ClaimPolicy.build_plan(claim_policy, context, effective_policy())
      {:ok, token} = Ecto.UUID.dump(Ecto.UUID.generate())

      {:ok, batch, stats} =
        ClaimPolicy.decode(claim_policy, plan, [
          ["alternate", nil, "graph", "hash", 3, token, @now, 1]
        ])

      assert %{leases: [%{run_id: "alternate"}], poisoned: []} = batch
      assert stats.ready_selected == 1
      assert_receive {:alternate_claim_policy, :decode, :decode_contract, _pid}

      handler = "claim-policy-selected-#{System.unique_integer([:positive])}"

      :telemetry.attach_many(
        handler,
        [
          [:docket, :postgres, :run_store, :claim],
          [:docket, :postgres, :claim_policy, :admission]
        ],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      :ok = ClaimPolicy.observe(claim_policy, plan, stats, {:ok, batch}, System.monotonic_time())

      assert_receive {[:docket, :postgres, :run_store, :claim], %{leases: 1}, %{result: :ok}}

      assert_receive {[:docket, :postgres, :claim_policy, :admission], %{leases: 1},
                      %{implementation: Docket.Test.AlternateClaimPolicy, result: :ok}}
    end

    test "decoder failures stay in the error contract and observation failures cannot hide results" do
      context = %{repo: __MODULE__.Repo, prefix: nil}

      decode_policy =
        ClaimPolicy.new(
          [implementation: FailingCallbackImplementation, mode: :decode],
          context
        )

      decode_plan = ClaimPolicy.build_plan(decode_policy, context, effective_policy())

      assert {:error, {:claim_policy_decode_failed, {:raised, %RuntimeError{}, _stacktrace}}} =
               ClaimPolicy.decode(decode_policy, decode_plan, [[1]])

      observe_policy =
        ClaimPolicy.new(
          [implementation: FailingCallbackImplementation, mode: :observe],
          context
        )

      observe_plan = ClaimPolicy.build_plan(observe_policy, context, effective_policy())
      handler = "claim-policy-observer-failure-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :claim_policy, :admission],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert :ok =
               ClaimPolicy.observe(
                 observe_policy,
                 observe_plan,
                 %{},
                 {:ok, %{leases: [], poisoned: []}},
                 System.monotonic_time()
               )

      assert_receive {[:docket, :postgres, :claim_policy, :admission], %{leases: 0},
                      %{implementation: FailingCallbackImplementation, result: :ok}}
    end

    test "emits a closed TenantFair observation for mixed success despite observer failure" do
      batch = %{leases: [%{kind: :ready}, %{kind: :expired}], poisoned: [%{kind: :ready}]}

      observation =
        Observation.new!(
          eligible_partitions: 3,
          locked_partitions: 2,
          skipped_partitions: 1,
          below_preferred_partitions: 2,
          default_policy_partitions: 1,
          override_policy_partitions: 1,
          running_partitions: 2,
          preferred_admissions: 1,
          ready_leases: 1,
          ready_poisoned: 1,
          expired_leases: 1,
          candidate_rows_examined: 3,
          ready_claim_wait_ms_count: 1,
          ready_claim_wait_ms_sum: 20,
          ready_claim_wait_ms_max: 20,
          expired_recovery_wait_ms_count: 1,
          expired_recovery_wait_ms_sum: 30,
          expired_recovery_wait_ms_max: 30
        )

      {claim_policy, plan} =
        observed_policy(batch, observation, 3, observe: :raise)

      assert {:ok, ^batch, decoded} = ClaimPolicy.decode(claim_policy, plan, [])
      attach_admission_events()

      assert :ok =
               ClaimPolicy.observe(
                 claim_policy,
                 plan,
                 decoded,
                 {:ok, batch},
                 System.monotonic_time()
               )

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation], measurements,
                      %{
                        implementation: Docket.Test.ObservedClaimPolicy,
                        schema: :tenant_fair_v1,
                        result: :ok,
                        observation_status: :available,
                        admission_class: :preferred,
                        work_class: :mixed,
                        batch_shape: :full,
                        policy_source: :mixed,
                        admin_state: :running
                      } = metadata}

      assert map_size(metadata) == 9
      assert measurements.demand == 3
      assert measurements.leases == 2
      assert measurements.poisoned == 1
      assert measurements.outcomes == 3
      assert measurements.unfilled_demand == 0
      assert measurements.steals == 1
      assert measurements.cap_denied_partitions == 0
      assert measurements.ready_claim_wait_ms_max == 20
      assert measurements.expired_recovery_wait_ms_max == 30
      refute Map.has_key?(measurements, :partition_lock_skip_delay_ms_count)

      assert_receive {[:docket, :postgres, :claim_policy, :admission],
                      %{demand: 3, leases: 2, poisoned: 1} = generic_measurements,
                      %{implementation: Docket.Test.ObservedClaimPolicy, result: :ok} =
                        generic_metadata}

      assert map_size(generic_measurements) == 4
      assert map_size(generic_metadata) == 2
    end

    test "distinguishes an observed no-op from a proven avoidable under-claim" do
      attach_admission_events()

      empty_batch = %{leases: [], poisoned: []}

      capped_no_op =
        Observation.new!(
          eligible_partitions: 2,
          locked_partitions: 1,
          skipped_partitions: 1,
          cap_denied_partitions: 1,
          default_policy_partitions: 1,
          running_partitions: 1,
          partition_lock_skip_delay_ms_count: 1,
          partition_lock_skip_delay_ms_sum: 40,
          partition_lock_skip_delay_ms_max: 40
        )

      {empty_policy, empty_plan} = observed_policy(empty_batch, capped_no_op, 2)
      assert {:ok, ^empty_batch, empty_decoded} = ClaimPolicy.decode(empty_policy, empty_plan, [])

      assert :ok =
               ClaimPolicy.observe(
                 empty_policy,
                 empty_plan,
                 empty_decoded,
                 {:ok, empty_batch},
                 System.monotonic_time()
               )

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                      %{
                        outcomes: 0,
                        unfilled_demand: 2,
                        cap_denied_partitions: 1,
                        partition_lock_skip_delay_ms_max: 40
                      },
                      %{
                        batch_shape: :no_op,
                        observation_status: :available,
                        policy_source: :default,
                        admin_state: :running
                      }}

      partial_batch = %{leases: [%{kind: :ready}], poisoned: []}

      borrowed_partial =
        Observation.new!(
          eligible_partitions: 1,
          locked_partitions: 1,
          default_policy_partitions: 1,
          running_partitions: 1,
          borrowed_admissions: 1,
          ready_leases: 1,
          candidate_rows_examined: 1,
          ready_claim_wait_ms_count: 1,
          ready_claim_wait_ms_sum: 3,
          ready_claim_wait_ms_max: 3
        )

      {partial_policy, partial_plan} = observed_policy(partial_batch, borrowed_partial, 2)

      assert {:ok, ^partial_batch, partial_decoded} =
               ClaimPolicy.decode(partial_policy, partial_plan, [])

      assert :ok =
               ClaimPolicy.observe(
                 partial_policy,
                 partial_plan,
                 partial_decoded,
                 {:ok, partial_batch},
                 System.monotonic_time()
               )

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                      %{under_claimed: 0, outcomes: 1, unfilled_demand: 1},
                      %{batch_shape: :partial, admission_class: :borrowed}}

      under_claimed_batch = %{leases: [%{kind: :ready}], poisoned: []}

      under_claimed =
        Observation.new!(
          eligible_partitions: 2,
          locked_partitions: 2,
          below_preferred_partitions: 2,
          default_policy_partitions: 2,
          running_partitions: 2,
          preferred_admissions: 1,
          ready_leases: 1,
          candidate_rows_examined: 1,
          under_claimed: 1,
          ready_claim_wait_ms_count: 1,
          ready_claim_wait_ms_sum: 5,
          ready_claim_wait_ms_max: 5
        )

      {under_policy, under_plan} = observed_policy(under_claimed_batch, under_claimed, 2)

      assert {:ok, ^under_claimed_batch, under_decoded} =
               ClaimPolicy.decode(under_policy, under_plan, [])

      assert :ok =
               ClaimPolicy.observe(
                 under_policy,
                 under_plan,
                 under_decoded,
                 {:ok, under_claimed_batch},
                 System.monotonic_time()
               )

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                      %{under_claimed: 1, outcomes: 1, unfilled_demand: 1},
                      %{batch_shape: :under_claim, work_class: :ready}}

      expired_poison_batch = %{leases: [], poisoned: [%{kind: :expired}]}

      expired_poison =
        Observation.new!(
          eligible_partitions: 1,
          locked_partitions: 1,
          default_policy_partitions: 1,
          running_partitions: 1,
          expired_poisoned: 1,
          candidate_rows_examined: 1,
          expired_recovery_wait_ms_count: 1,
          expired_recovery_wait_ms_sum: 9,
          expired_recovery_wait_ms_max: 9
        )

      {poison_policy, poison_plan} = observed_policy(expired_poison_batch, expired_poison, 1)

      assert {:ok, ^expired_poison_batch, poison_decoded} =
               ClaimPolicy.decode(poison_policy, poison_plan, [])

      assert :ok =
               ClaimPolicy.observe(
                 poison_policy,
                 poison_plan,
                 poison_decoded,
                 {:ok, expired_poison_batch},
                 System.monotonic_time()
               )

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                      %{expired_poisoned: 1, steals: 0, poisoned: 1},
                      %{batch_shape: :full, work_class: :expired, admission_class: :none}}
    end

    test "invalid or missing TenantFair decoded summaries fail closed and emit unavailable detail" do
      invalid = %Observation{ready_leases: 1}
      batch = %{leases: [], poisoned: []}
      {claim_policy, plan} = observed_policy(batch, invalid, 1)

      assert {:error, {:claim_policy_decode_failed, {:raised, %ArgumentError{}, _stacktrace}}} =
               ClaimPolicy.decode(claim_policy, plan, [])

      attach_admission_events()

      assert :ok =
               ClaimPolicy.observe(
                 claim_policy,
                 plan,
                 nil,
                 {:error, :invalid_observation},
                 System.monotonic_time()
               )

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                      %{duration: duration, demand: 1} = unavailable_measurements,
                      %{
                        result: :error,
                        observation_status: :unavailable,
                        admission_class: :none,
                        work_class: :none,
                        batch_shape: :error,
                        policy_source: :none,
                        admin_state: :none
                      }}

      assert is_integer(duration) and duration >= 0
      assert map_size(unavailable_measurements) == 2

      for options <- [[decode?: false], [declare?: false]] do
        {policy, mismatch_plan} = observed_policy(batch, Observation.new!(), 1, options)

        assert {:error, {:claim_policy_decode_failed, {:raised, %ArgumentError{}, _stacktrace}}} =
                 ClaimPolicy.decode(policy, mismatch_plan, [])
      end
    end

    test "TenantFair observations reject unknown identity fields and invalid aggregates" do
      assert_raise ArgumentError, ~r/unknown fields: \[:tenant_id\]/, fn ->
        Observation.new!(tenant_id: "tenant-secret")
      end

      assert_raise ArgumentError, ~r/eligible_partitions must be a non-negative integer/, fn ->
        Observation.new!(eligible_partitions: -1)
      end

      assert_raise ArgumentError, ~r/count\/sum\/max must be all set or all nil/, fn ->
        Observation.new!(partition_lock_skip_delay_ms_count: 1)
      end

      assert_raise ArgumentError, ~r/sum cannot exceed.*count.*max/, fn ->
        Observation.new!(
          ready_claim_wait_ms_count: 1,
          ready_claim_wait_ms_sum: 100,
          ready_claim_wait_ms_max: 1
        )
      end

      assert_raise ArgumentError, ~r/policy-source partition counts must equal/, fn ->
        Observation.new!(locked_partitions: 1, eligible_partitions: 1)
      end
    end

    test "the built Hex artifact excludes ClaimPolicy test support" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-claim-policy-package-#{System.unique_integer([:positive, :monotonic])}"
        )

      on_exit(fn -> File.rm_rf!(output) end)

      assert {_build_output, 0} =
               System.cmd("mix", ["hex.build", "--unpack", "--output", output],
                 stderr_to_stdout: true
               )

      packaged_files = Path.wildcard(Path.join(output, "**/*"), match_dot: true)

      refute Enum.any?(packaged_files, fn path ->
               relative = Path.relative_to(path, output)

               relative == "test" or String.starts_with?(relative, "test/") or
                 String.contains?(relative, "claim_policy_tests") or
                 String.contains?(relative, "claim_policy_run_store_tests")
             end)
    end

    test "source ownership forbids reverse dispatch and duplicate Legacy admission code" do
      root = File.cwd!()
      dispatcher = File.read!(Path.join(root, "lib/docket/postgres/dispatcher.ex"))
      postgres = File.read!(Path.join(root, "lib/docket/postgres.ex"))
      legacy_path = Path.join(root, "lib/docket/postgres/claim_policy/legacy.ex")
      legacy = File.read!(legacy_path)

      other_production_sources =
        root
        |> Path.join("lib/**/*.ex")
        |> Path.wildcard()
        |> Enum.reject(&(&1 == legacy_path))
        |> Enum.map(&File.read!/1)

      for signature <- [
            "ready_candidates",
            "expired_candidates",
            "max_claim_attempts_exceeded",
            "ready_oldest_age_ms",
            "claim_statement"
          ] do
        assert legacy =~ signature

        for source <- other_production_sources do
          refute source =~ signature
        end
      end

      for dormant <- ["decode_claim_batch", "emit_claim_telemetry", "emit_claim_error"] do
        for source <- other_production_sources do
          refute source =~ dormant
        end
      end

      refute dispatcher =~ "ClaimPolicy.claim_due("
      refute postgres =~ "ClaimPolicy.claim_due("

      implementation_sources =
        Path.wildcard(Path.join(root, "lib/docket/postgres/claim_policy/**/*.ex")) ++
          [Path.join(root, "test/support/claim_policy_tests.ex")]

      for source <- implementation_sources do
        refute File.read!(source) =~ "RunStore.claim_due("
      end

      refute Code.ensure_loaded?(Docket.Postgres.ClaimPolicy.TenantFair)
    end

    defp effective_policy(limit \\ 1) do
      ClaimPolicy.effective_policy!(%{
        now: @now,
        limit: limit,
        orphan_ttl_ms: 1_000,
        max_claim_attempts: 3,
        preference: :ready
      })
    end

    defp observed_policy(batch, observation, demand, options \\ []) do
      context = %{repo: __MODULE__.Repo, prefix: nil}

      config =
        [
          implementation: Docket.Test.ObservedClaimPolicy,
          batch: batch,
          observation: observation
        ] ++ options

      claim_policy = ClaimPolicy.new(config, context)
      plan = ClaimPolicy.build_plan(claim_policy, context, effective_policy(demand))
      {claim_policy, plan}
    end

    defp attach_admission_events do
      handler = "tenant-fair-observation-#{System.unique_integer([:positive])}"

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
    end

    defp build_plan_from(implementation) do
      context = %{repo: __MODULE__.Repo, prefix: nil}
      policy = ClaimPolicy.new([implementation: implementation], context)
      ClaimPolicy.build_plan(policy, context, effective_policy())
    end
  end
end
