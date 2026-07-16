if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyActivationTest do
    use ExUnit.Case, async: false

    import Ecto.Query, only: [from: 2]

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy
    alias Docket.Postgres.ClaimPolicy.{Activation, Admin, Backfill, OnlineDDL, Readiness}
    alias Docket.Postgres.ClaimPolicy.Admin.Codec
    alias Docket.Postgres.ClaimPolicy.TenantFair.Function
    alias Docket.Postgres.ClaimPolicyAdminTestRepo, as: TestRepo
    alias Docket.Postgres.{OnlineMigration, RunStore}
    alias Docket.Postgres.Schemas.GraphVersion

    @migration_version 20_260_716_000_071
    @private_migration_version 20_260_716_000_171
    @policy %{preferred_active: 2, max_active: 4, weight: 1, borrowing: false}
    @now ~U[2026-07-16 12:00:00.000000Z]

    defmodule TenantFairActivationFixture do
      @behaviour Docket.Postgres.ClaimPolicy

      alias Docket.Postgres.ClaimPolicy.TenantFair

      @options [
        partition_by: :tenant_id,
        default_preferred_active: 2,
        default_max_active: 4,
        default_weight: 1,
        borrowing: false
      ]

      @impl true
      def init([], context), do: TenantFair.init(@options, context)

      @impl true
      defdelegate activation_contract(state), to: TenantFair

      @impl true
      defdelegate build_plan(context, policy, state), to: TenantFair

      @impl true
      defdelegate decode(rows, decoder, state), to: TenantFair

      @impl true
      defdelegate observe(plan, decoded, result, duration, state), to: TenantFair
    end

    defmodule InstallDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule InstallPrivateDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(prefix: "docket_private")
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_private")
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      :ok =
        Ecto.Migrator.up(TestRepo, @private_migration_version, InstallPrivateDocket, log: false)

      %{
        context: Docket.Postgres.context(repo: TestRepo),
        activation_context:
          Docket.Postgres.context(
            repo: TestRepo,
            claim_policy: [implementation: TenantFairActivationFixture]
          ),
        private_context: Docket.Postgres.context(repo: TestRepo, prefix: "docket_private"),
        private_activation_context:
          Docket.Postgres.context(
            repo: TestRepo,
            prefix: "docket_private",
            claim_policy: [implementation: TenantFairActivationFixture]
          )
      }
    end

    test "preflight is advisory, bounded, and prefix isolated", %{
      context: context,
      activation_context: activation_context,
      private_activation_context: private
    } do
      assert {:ok,
              %{
                activatable: false,
                mode: :legacy,
                mode_epoch: 0,
                readiness: :not_ready,
                readiness_epoch: 0,
                required_function_contract: 1,
                selected_implementation_contract: :none,
                active_implementation_contract: :none,
                recorded_missing_partition_count: nil,
                missing_partition_count: 0,
                live_capability_count: 0,
                old_binary_assertion_expires_at: nil,
                reasons: [
                  :capability_mismatch,
                  :function_contract_mismatch,
                  :not_ready,
                  :old_binary_assertion_expired
                ]
              }} = Activation.preflight(context)

      prepare_ready!(activation_context, "public")
      install_function!("public")
      assertion = attest!(activation_context, "public-proof")
      register!(activation_context)

      assert {:ok,
              %{
                activatable: true,
                mode: :legacy,
                mode_epoch: 0,
                readiness: :ready,
                selected_implementation_contract: %{
                  engine: :tenant_fair,
                  function_contract: 1
                },
                active_implementation_contract: :none,
                default_fingerprint: default_fingerprint,
                verified_default_fingerprint: verified_default_fingerprint,
                schema_generation: 2,
                backfill_phase: :complete,
                online_phase: :complete,
                recorded_missing_partition_count: 0,
                missing_partition_count: 0,
                ready_index_valid: true,
                live_index_valid: true,
                foreign_key_validated: true,
                expected_tables_present: true,
                expected_table_count: 13,
                expected_table_total: 13,
                live_capability_count: 1,
                old_binary_assertion_expires_at: %DateTime{},
                reasons: []
              }} = Activation.preflight(activation_context)

      assert default_fingerprint == Codec.default_fingerprint(@policy)
      assert verified_default_fingerprint == default_fingerprint

      assert assertion.assertion_id

      assert {:ok, %{mode: :legacy, mode_epoch: 0, readiness: :not_ready}} =
               Activation.preflight(private)
    end

    test "activation and deactivation are epoch-CASed, replayable, audited, and gate Legacy", %{
      context: context,
      activation_context: activation_context
    } do
      prepare_ready!(activation_context, "public")
      install_function!("public")
      register!(activation_context)
      assertion = attest!(activation_context, "activate-proof")
      insert_ready_run!(context)

      activate_opts = mode_opts("activate", 0, assertion.assertion_id)

      assert {:ok,
              %{
                outcome: :applied,
                target: :activation,
                previous_version: 0,
                version: 1,
                audit_id: activation_audit
              }} = Activation.activate(activation_context, activate_opts)

      assert {:ok,
              %{
                active_implementation_contract: %{
                  engine: :tenant_fair,
                  function_contract: 1
                }
              }} = Activation.preflight(activation_context)

      assert {:ok,
              %{
                outcome: :replayed,
                original: %{audit_id: ^activation_audit, version: 1}
              }} = Activation.activate(activation_context, activate_opts)

      assert {:error, {:event_conflict, %{source: "activation-test", event_id: "activate"}}} =
               Activation.activate(
                 activation_context,
                 mode_opts("activate", 1, assertion.assertion_id)
               )

      assert {:ok,
              %{
                leases: [%{run_id: "gate-blocked-run", claim_token: tenant_fair_token}],
                poisoned: []
              }} =
               RunStore.claim_due(activation_context, :system, policy())

      assert :ok =
               RunStore.release_claim(
                 activation_context,
                 :system,
                 "gate-blocked-run",
                 tenant_fair_token,
                 @now
               )

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(context, :system, policy())

      assert [[nil]] =
               TestRepo.query!(
                 "SELECT claim_token FROM docket_runs WHERE run_id = 'gate-blocked-run'"
               ).rows

      assert ClaimPolicy.implementation(context.claim_policy) == ClaimPolicy.Legacy

      assert {:ok,
              %{
                outcome: :applied,
                target: :activation,
                previous_version: 1,
                version: 2
              }} = Activation.deactivate(activation_context, mode_opts("deactivate", 1))

      assert {:ok, %{leases: [%{run_id: "gate-blocked-run"}], poisoned: []}} =
               RunStore.claim_due(context, :system, policy())

      assert [["legacy", 2]] =
               TestRepo.query!(
                 "SELECT admission_mode, mode_epoch FROM docket_claim_admission_gate"
               ).rows

      assert [[false]] =
               TestRepo.query!("""
               SELECT (after_value->>'exact_cap_guarantee')::boolean
               FROM docket_claim_policy_events
               WHERE operation = 'deactivated'
               """).rows
    end

    test "activation and deactivation are fully prefix isolated", %{
      private_activation_context: private
    } do
      prepare_ready!(private, "docket_private")
      install_function!("docket_private")
      register!(private)
      assertion = attest!(private, "private-prefix-proof")

      assert {:ok, %{activatable: true, mode: :legacy, mode_epoch: 0}} =
               Activation.preflight(private)

      assert {:ok, %{outcome: :applied, previous_version: 0, version: 1}} =
               Activation.activate(
                 private,
                 mode_opts("private-prefix-activate", 0, assertion.assertion_id)
               )

      assert {:ok,
              %{
                mode: :tenant_fair,
                mode_epoch: 1,
                active_implementation_contract: %{
                  engine: :tenant_fair,
                  function_contract: 1
                }
              }} = Activation.preflight(private)

      assert {:ok, %{outcome: :applied, previous_version: 1, version: 2}} =
               Activation.deactivate(private, mode_opts("private-prefix-deactivate", 1))

      assert [["legacy", 2]] =
               TestRepo.query!("""
               SELECT admission_mode, mode_epoch
               FROM "docket_private".docket_claim_admission_gate
               """).rows

      assert [
               ["private-prefix-activate", "activated"],
               ["private-prefix-deactivate", "deactivated"]
             ] =
               TestRepo.query!("""
               SELECT event_id, operation
               FROM "docket_private".docket_claim_policy_events
               WHERE source = 'activation-test'
                 AND event_id IN ('private-prefix-activate', 'private-prefix-deactivate')
               ORDER BY audit_id
               """).rows

      assert [["private-prefix-activate", "applied"], ["private-prefix-deactivate", "applied"]] =
               TestRepo.query!("""
               SELECT event_id, outcome
               FROM "docket_private".docket_claim_policy_receipts
               WHERE source = 'activation-test'
                 AND event_id IN ('private-prefix-activate', 'private-prefix-deactivate')
               ORDER BY event_id
               """).rows

      assert [["legacy", 0]] =
               TestRepo.query!("""
               SELECT admission_mode, mode_epoch
               FROM docket_claim_admission_gate
               """).rows

      assert [[0]] =
               TestRepo.query!("SELECT count(*)::bigint FROM docket_claim_policy_events").rows

      assert [[0]] =
               TestRepo.query!("SELECT count(*)::bigint FROM docket_claim_policy_receipts").rows
    end

    test "test TenantFair function is fail closed, active-only, and excluded from Legacy", %{
      context: legacy_context,
      activation_context: tenant_fair_context
    } do
      install_function!("public")
      insert_ready_run!(legacy_context, "tenant-fair-first")

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(tenant_fair_context, :system, policy())

      prepare_ready!(tenant_fair_context, "public")

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(tenant_fair_context, :system, policy())

      register!(tenant_fair_context)
      assertion = attest!(tenant_fair_context, "tenant-fair-behavior-proof")

      assert {:ok, %{outcome: :applied, version: 1}} =
               Activation.activate(
                 tenant_fair_context,
                 mode_opts("tenant-fair-behavior-activate", 0, assertion.assertion_id)
               )

      assert {:ok,
              %{
                selected_implementation_contract: :none,
                active_implementation_contract: %{
                  engine: :tenant_fair,
                  function_contract: 1
                }
              }} = Activation.preflight(legacy_context)

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(legacy_context, :system, policy())

      assert {:ok, %{leases: [%{run_id: "tenant-fair-first"}], poisoned: []}} =
               RunStore.claim_due(tenant_fair_context, :system, policy())

      insert_ready_run!(legacy_context, "tenant-fair-held-gate")
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT id FROM docket_claim_admission_gate FOR UPDATE")
            send(parent, {:tenant_fair_gate_held, self()})

            receive do
              :release_tenant_fair_gate -> :ok
            end
          end)
        end)

      assert_receive {:tenant_fair_gate_held, holder_pid}
      started = System.monotonic_time(:millisecond)

      assert {:error, {:claim_policy_unavailable, :lock_contention}} =
               RunStore.claim_due(tenant_fair_context, :system, policy())

      assert System.monotonic_time(:millisecond) - started < 500
      send(holder_pid, :release_tenant_fair_gate)
      assert {:ok, _} = Task.await(holder)

      assert {:ok, %{outcome: :applied, version: 2}} =
               Activation.deactivate(
                 tenant_fair_context,
                 mode_opts("tenant-fair-behavior-deactivate", 1)
               )

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(tenant_fair_context, :system, policy())

      assert [[nil]] =
               TestRepo.query!(
                 "SELECT claim_token FROM docket_runs WHERE run_id = 'tenant-fair-held-gate'"
               ).rows
    end

    test "concurrent identical activation resolves as one apply and one exact replay", %{
      activation_context: context
    } do
      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)
      assertion = attest!(context, "concurrent-replay-proof")
      parent = self()

      holder = hold_shared_gate(parent, :concurrent_replay)
      assert_receive {:shared_gate_held, :concurrent_replay, holder_pid}

      opts = mode_opts("concurrent-replay", 0, assertion.assertion_id)
      first = Task.async(fn -> Activation.activate(context, opts) end)
      second = Task.async(fn -> Activation.activate(context, opts) end)

      wait_until(fn -> gate_update_waiter_count() == 2 end)
      send(holder_pid, {:release_shared_gate, :concurrent_replay})
      assert {:ok, _} = Task.await(holder)

      results = [Task.await(first), Task.await(second)]

      assert Enum.count(results, &match?({:ok, %{outcome: :applied, version: 1}}, &1)) == 1

      assert Enum.count(
               results,
               &match?(
                 {:ok,
                  %{
                    outcome: :replayed,
                    original: %{outcome: :applied, previous_version: 0, version: 1}
                  }},
                 &1
               )
             ) == 1

      assert [[1]] =
               TestRepo.query!(
                 "SELECT count(*)::bigint FROM docket_claim_policy_events WHERE operation = 'activated'"
               ).rows
    end

    test "concurrent changed fingerprint resolves conflict before stale epoch", %{
      activation_context: context
    } do
      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)
      first_assertion = attest!(context, "concurrent-conflict-proof-one")
      second_assertion = attest!(context, "concurrent-conflict-proof-two")
      parent = self()

      holder = hold_shared_gate(parent, :concurrent_conflict)
      assert_receive {:shared_gate_held, :concurrent_conflict, holder_pid}

      first =
        Task.async(fn ->
          Activation.activate(
            context,
            mode_opts("concurrent-conflict", 0, first_assertion.assertion_id)
          )
        end)

      second =
        Task.async(fn ->
          Activation.activate(
            context,
            mode_opts("concurrent-conflict", 0, second_assertion.assertion_id)
          )
        end)

      wait_until(fn -> gate_update_waiter_count() == 2 end)
      send(holder_pid, {:release_shared_gate, :concurrent_conflict})
      assert {:ok, _} = Task.await(holder)

      results = [Task.await(first), Task.await(second)]

      assert Enum.count(results, &match?({:ok, %{outcome: :applied, version: 1}}, &1)) == 1

      assert Enum.count(
               results,
               &match?(
                 {:error,
                  {:event_conflict, %{source: "activation-test", event_id: "concurrent-conflict"}}},
                 &1
               )
             ) == 1
    end

    test "gate-wait expiry uses post-lock database wall time for proof and capabilities", %{
      activation_context: context
    } do
      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)
      parent = self()

      assertion_expiry = database_expiry(500)

      assert {:ok, short_assertion} =
               Activation.attest_old_binaries_absent(
                 context,
                 Keyword.put(assertion_opts("short-proof"), :expires_at, assertion_expiry)
               )

      assertion_holder = hold_shared_gate(parent, :assertion_expiry)
      assert_receive {:shared_gate_held, :assertion_expiry, assertion_holder_pid}
      refute database_time_reached?(assertion_expiry)

      assertion_activation =
        Task.async(fn ->
          Activation.activate(
            context,
            mode_opts("assertion-expiry", 0, short_assertion.assertion_id)
          )
        end)

      wait_until(fn -> gate_update_waiter_count() == 1 end)
      wait_until(fn -> database_time_reached?(assertion_expiry) end, 300)
      send(assertion_holder_pid, {:release_shared_gate, :assertion_expiry})
      assert {:ok, _} = Task.await(assertion_holder)

      assert {:error, {:activation_precondition_failed, :old_binary_assertion_expired}} =
               Task.await(assertion_activation)

      assert {:ok, capability} =
               Activation.register_capability(
                 context,
                 "00000000-0000-4000-8000-000000000071",
                 capability_opts(ttl_ms: 500)
               )

      long_assertion = attest!(context, "capability-expiry-proof")
      capability_holder = hold_shared_gate(parent, :capability_expiry)
      assert_receive {:shared_gate_held, :capability_expiry, capability_holder_pid}
      refute database_time_reached?(capability.expires_at)

      capability_activation =
        Task.async(fn ->
          Activation.activate(
            context,
            mode_opts("capability-expiry", 0, long_assertion.assertion_id)
          )
        end)

      wait_until(fn -> gate_update_waiter_count() == 1 end)
      wait_until(fn -> database_time_reached?(capability.expires_at) end, 300)
      send(capability_holder_pid, {:release_shared_gate, :capability_expiry})
      assert {:ok, _} = Task.await(capability_holder)

      assert {:error, {:activation_precondition_failed, :capability_mismatch}} =
               Task.await(capability_activation)

      assert [["legacy", 0]] =
               TestRepo.query!(
                 "SELECT admission_mode, mode_epoch FROM docket_claim_admission_gate"
               ).rows

      assert [[0]] =
               TestRepo.query!(
                 "SELECT count(*)::bigint FROM docket_claim_policy_events WHERE operation IN ('activated', 'activation_unchanged')"
               ).rows
    end

    test "old-binary assertion lifetime is bounded by database wall time", %{
      activation_context: context
    } do
      within_limit = database_expiry(:timer.hours(24) - 1_000)

      assert {:ok, %{assertion_id: assertion_id, expires_at: ^within_limit}} =
               Activation.attest_old_binaries_absent(
                 context,
                 Keyword.put(assertion_opts("bounded-proof"), :expires_at, within_limit)
               )

      too_long = database_expiry(:timer.hours(24) + 60_000)

      assert {:error, :invalid_activation_options} =
               Activation.attest_old_binaries_absent(
                 context,
                 Keyword.put(assertion_opts("too-long-proof"), :expires_at, too_long)
               )

      expired = database_expiry(-1_000)

      assert {:error, :invalid_activation_options} =
               Activation.attest_old_binaries_absent(
                 context,
                 Keyword.put(assertion_opts("already-expired-proof"), :expires_at, expired)
               )

      assert [[0]] =
               TestRepo.query!(
                 "SELECT count(*)::bigint FROM docket_claim_policy_events WHERE event_id IN ('too-long-proof', 'already-expired-proof')"
               ).rows

      TestRepo.query!("""
      UPDATE docket_claim_assertions
      SET expires_at = asserted_at + interval '25 hours'
      WHERE assertion_id = '#{assertion_id}'::uuid
      """)

      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)

      assert {:ok, %{old_binary_assertion_expires_at: nil}} = Activation.preflight(context)

      assert {:error, {:activation_precondition_failed, :old_binary_assertion_expired}} =
               Activation.activate(context, mode_opts("tampered-long-proof", 0, assertion_id))
    end

    test "activation options own missing and malformed assertion UUID errors", %{
      activation_context: context
    } do
      assert {:error, :invalid_activation_options} =
               Activation.activate(context, mode_opts("missing-assertion-id", 0))

      assert {:error, :invalid_activation_options} =
               Activation.activate(context, mode_opts("malformed-assertion-id", 0, "not-a-uuid"))
    end

    test "pre-flip incompatible capability commits before activation validates", %{
      activation_context: context
    } do
      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)
      assertion = attest!(context, "capability-register-first-proof")
      parent = self()

      table_holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("LOCK TABLE docket_claim_capabilities IN ACCESS EXCLUSIVE MODE")

            send(parent, {:capability_table_held, self()})

            receive do
              :release_capability_table -> :ok
            end
          end)
        end)

      assert_receive {:capability_table_held, table_holder_pid}

      incompatible =
        Task.async(fn ->
          Activation.register_capability(
            context,
            "00000000-0000-4000-8000-000000000072",
            capability_opts(gate_contract: 0)
          )
        end)

      wait_until(fn -> capability_insert_waiting?() end)

      activation =
        Task.async(fn ->
          Activation.activate(
            context,
            mode_opts("capability-register-first", 0, assertion.assertion_id)
          )
        end)

      wait_until(fn -> gate_update_waiting?() end)
      send(table_holder_pid, :release_capability_table)
      assert {:ok, _} = Task.await(table_holder)

      assert {:ok, %{instance_id: "00000000-0000-4000-8000-000000000072"}} =
               Task.await(incompatible)

      assert {:error, {:activation_precondition_failed, :capability_mismatch}} =
               Task.await(activation)

      assert [["legacy", 0]] =
               TestRepo.query!(
                 "SELECT admission_mode, mode_epoch FROM docket_claim_admission_gate"
               ).rows
    end

    test "activation flip excludes and then rejects incompatible capability registration", %{
      activation_context: context
    } do
      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)
      assertion = attest!(context, "capability-activation-first-proof")
      parent = self()

      rollout_holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT id FROM docket_claim_rollout FOR UPDATE")
            send(parent, {:rollout_held, self()})

            receive do
              :release_rollout -> :ok
            end
          end)
        end)

      assert_receive {:rollout_held, rollout_holder_pid}

      activation =
        Task.async(fn ->
          Activation.activate(
            context,
            mode_opts("capability-activation-first", 0, assertion.assertion_id)
          )
        end)

      wait_until(fn -> rollout_share_waiting?() end)

      incompatible =
        Task.async(fn ->
          Activation.register_capability(
            context,
            "00000000-0000-4000-8000-000000000072",
            capability_opts(gate_contract: 0)
          )
        end)

      wait_until(fn -> capability_gate_waiting?() end)
      refute Task.yield(incompatible, 20)
      send(rollout_holder_pid, :release_rollout)
      assert {:ok, _} = Task.await(rollout_holder)
      assert {:ok, %{outcome: :applied, version: 1}} = Task.await(activation)
      assert {:error, :incompatible_capability} = Task.await(incompatible)

      assert {:error, :incompatible_capability} =
               Activation.register_capability(
                 context,
                 "00000000-0000-4000-8000-000000000073",
                 capability_opts(function_contract: 0)
               )

      assert {:ok, %{instance_id: "00000000-0000-4000-8000-000000000074"}} =
               Activation.register_capability(
                 context,
                 "00000000-0000-4000-8000-000000000074",
                 capability_opts()
               )
    end

    test "activation rejects incomplete readiness, stale epochs, expired proof, and bad capabilities",
         %{activation_context: context} do
      register!(context)
      assertion = attest!(context, "not-ready-proof")

      assert {:error, {:activation_precondition_failed, :not_ready}} =
               Activation.activate(context, mode_opts("not-ready", 0, assertion.assertion_id))

      prepare_ready!(context, "public")
      install_function!("public")

      assert {:error, {:version_conflict, %{target: :activation, expected: 9, actual: 0}}} =
               Activation.activate(context, mode_opts("stale", 9, assertion.assertion_id))

      assert {:error, {:activation_precondition_failed, :old_binary_assertion_expired}} =
               Activation.activate(
                 context,
                 mode_opts("wrong-assertion", 0, Ecto.UUID.generate())
               )

      register!(context, gate_contract: 0)

      assert {:error, {:activation_precondition_failed, :capability_mismatch}} =
               Activation.activate(
                 context,
                 mode_opts("bad-capability", 0, assertion.assertion_id)
               )

      assert [["legacy", 0]] =
               TestRepo.query!(
                 "SELECT admission_mode, mode_epoch FROM docket_claim_admission_gate"
               ).rows
    end

    test "activation rechecks every coherent DCKT-72 readiness field under the gate", %{
      activation_context: context
    } do
      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)
      assertion = attest!(context, "readiness-negative-proof")

      [[dual_write_id, backfill_completed_at, online_completed_at]] =
        TestRepo.query!("""
        SELECT dual_write_assertion_id::text, backfill_completed_at, online_completed_at
        FROM docket_claim_rollout
        WHERE id = 1
        """).rows

      corruptions = [
        {"dual-write", "SET dual_write_assertion_id = NULL",
         "SET dual_write_assertion_id = '#{dual_write_id}'::uuid"},
        {"dual-write-kind", "SET dual_write_assertion_id = '#{assertion.assertion_id}'::uuid",
         "SET dual_write_assertion_id = '#{dual_write_id}'::uuid"},
        {"backfill", "SET backfill_phase = 'running', backfill_completed_at = NULL",
         "SET backfill_phase = 'complete', backfill_completed_at = '#{DateTime.to_iso8601(backfill_completed_at)}'::timestamptz"},
        {"backfill-error", "SET backfill_last_error = 'stale_failure'",
         "SET backfill_last_error = NULL"},
        {"missing", "SET missing_partition_count = 1", "SET missing_partition_count = 0"},
        {"online", "SET online_phase = 'live_index', online_completed_at = NULL",
         "SET online_phase = 'complete', online_completed_at = '#{DateTime.to_iso8601(online_completed_at)}'::timestamptz"},
        {"ready-index",
         "SET online_phase = 'not_started', online_completed_at = NULL, ready_index_valid = false, live_index_valid = false",
         "SET online_phase = 'complete', online_completed_at = '#{DateTime.to_iso8601(online_completed_at)}'::timestamptz, ready_index_valid = true, live_index_valid = true"},
        {"live-index",
         "SET online_phase = 'ready_index', online_completed_at = NULL, live_index_valid = false",
         "SET online_phase = 'complete', online_completed_at = '#{DateTime.to_iso8601(online_completed_at)}'::timestamptz, live_index_valid = true"},
        {"fk",
         "SET online_phase = 'fk_not_valid', online_completed_at = NULL, fk_disposition = 'not_valid'",
         "SET online_phase = 'complete', online_completed_at = '#{DateTime.to_iso8601(online_completed_at)}'::timestamptz, fk_disposition = 'validated'"},
        {"ready-hash", "SET ready_index_ddl_sha256 = decode(repeat('00', 32), 'hex')",
         "SET ready_index_ddl_sha256 = decode('#{Base.encode16(OnlineDDL.index_fingerprint("public", :ready), case: :lower)}', 'hex')"},
        {"live-hash", "SET live_index_ddl_sha256 = decode(repeat('00', 32), 'hex')",
         "SET live_index_ddl_sha256 = decode('#{Base.encode16(OnlineDDL.index_fingerprint("public", :live), case: :lower)}', 'hex')"},
        {"default-hash", "SET verified_default_fingerprint = decode(repeat('00', 32), 'hex')",
         "SET verified_default_fingerprint = decode('#{Base.encode16(Codec.default_fingerprint(@policy), case: :lower)}', 'hex')"}
      ]

      Enum.each(corruptions, fn {name, corrupt, restore} ->
        TestRepo.query!("UPDATE docket_claim_rollout #{corrupt} WHERE id = 1")
        assert_activation_not_ready(context, assertion.assertion_id, "readiness-#{name}")
        TestRepo.query!("UPDATE docket_claim_rollout #{restore} WHERE id = 1")
      end)

      assert {:ok, %{version: 2}} =
               Admin.put_default(context, %{@policy | max_active: 5},
                 expected_version: 1,
                 source: "activation-test",
                 event_id: "readiness-default-change",
                 actor: "operator"
               )

      assert_activation_not_ready(context, assertion.assertion_id, "readiness-current-default")

      assert {:ok, %{version: 3}} =
               Admin.put_default(context, @policy,
                 expected_version: 2,
                 source: "activation-test",
                 event_id: "readiness-default-restore",
                 actor: "operator"
               )

      TestRepo.query!("DROP INDEX #{OnlineDDL.index_name(:ready)}")

      assert {:ok,
              %{
                activatable: false,
                ready_index_valid: false,
                reasons: reasons
              }} = Activation.preflight(context)

      assert :not_ready in reasons
      assert_activation_not_ready(context, assertion.assertion_id, "readiness-live-catalog")

      assert [["legacy", 0]] =
               TestRepo.query!(
                 "SELECT admission_mode, mode_epoch FROM docket_claim_admission_gate"
               ).rows
    end

    test "activation preserves unknown catalog counts when a source table is absent", %{
      activation_context: context
    } do
      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)
      assertion = attest!(context, "missing-table-proof")

      TestRepo.query!("DROP TABLE docket_claim_partitions CASCADE")

      assert {:ok,
              %{
                activatable: false,
                expected_tables_present: false,
                expected_table_count: 12,
                expected_table_total: 13,
                recorded_missing_partition_count: 0,
                missing_partition_count: nil,
                reasons: reasons
              }} = Activation.preflight(context)

      assert :not_ready in reasons

      fingerprints = OnlineDDL.index_fingerprints("public")

      verify_opts = [
        expected_readiness_epoch: 1,
        ready_index_ddl_sha256: fingerprints.ready,
        live_index_ddl_sha256: fingerprints.live,
        source: "activation-readiness",
        event_id: "demote-missing-source-table",
        actor: "operator"
      ]

      assert {:ok,
              %{
                outcome: :demoted,
                previous_version: 1,
                version: 2,
                reasons: demotion_reasons,
                audit_id: demotion_audit_id
              }} = Readiness.verify(context, verify_opts)

      assert :schema_generation in demotion_reasons

      assert {:ok,
              %{
                outcome: :replayed,
                original: %{
                  outcome: :demoted,
                  previous_version: 1,
                  version: 2,
                  audit_id: ^demotion_audit_id
                }
              }} = Readiness.verify(context, verify_opts)

      assert [["not_ready", 2, 0]] =
               TestRepo.query!("""
               SELECT gate.readiness, gate.readiness_epoch, rollout.missing_partition_count
               FROM docket_claim_admission_gate AS gate
               CROSS JOIN docket_claim_rollout AS rollout
               WHERE gate.id = 1 AND rollout.id = 1
               """).rows

      assert {:ok,
              %{
                activatable: false,
                readiness: :not_ready,
                recorded_missing_partition_count: 0,
                missing_partition_count: nil,
                reasons: post_demotion_reasons
              }} = Activation.preflight(context)

      assert :not_ready in post_demotion_reasons

      assert {:error, {:activation_precondition_failed, :not_ready}} =
               Activation.activate(
                 context,
                 mode_opts("missing-table", 0, assertion.assertion_id)
               )

      assert [[1]] =
               TestRepo.query!("""
               SELECT count(*)::bigint
               FROM docket_claim_policy_events
               WHERE operation = 'readiness_demoted'
                 AND audit_id = #{demotion_audit_id}
               """).rows

      assert [["legacy", 0]] =
               TestRepo.query!(
                 "SELECT admission_mode, mode_epoch FROM docket_claim_admission_gate"
               ).rows

      assert [[0]] =
               TestRepo.query!(
                 "SELECT count(*)::bigint FROM docket_claim_policy_events WHERE operation IN ('activated', 'activation_unchanged')"
               ).rows
    end

    test "activation forces read committed before receipt reads and refreshes after the gate", %{
      activation_context: context
    } do
      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)
      assertion = attest!(context, "repeatable-read-proof")
      parent = self()

      holder = hold_shared_gate(parent, :repeatable_read)
      assert_receive {:shared_gate_held, :repeatable_read, holder_pid}

      activation =
        Task.async(fn ->
          TestRepo.checkout(fn ->
            TestRepo.query!(
              "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ"
            )

            try do
              assert [["repeatable read"]] =
                       TestRepo.query!("SHOW default_transaction_isolation").rows

              Activation.activate(
                context,
                mode_opts("repeatable-read", 0, assertion.assertion_id)
              )
            after
              TestRepo.query!(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"
              )
            end
          end)
        end)

      wait_until(fn -> gate_update_waiter_count() == 1 end)
      TestRepo.query!("UPDATE docket_claim_rollout SET missing_partition_count = 1 WHERE id = 1")
      send(holder_pid, {:release_shared_gate, :repeatable_read})
      assert {:ok, _} = Task.await(holder)

      assert {:error, {:activation_precondition_failed, :not_ready}} = Task.await(activation)

      assert [["legacy", 0]] =
               TestRepo.query!(
                 "SELECT admission_mode, mode_epoch FROM docket_claim_admission_gate"
               ).rows

      assert [[0]] =
               TestRepo.query!(
                 "SELECT count(*)::bigint FROM docket_claim_policy_events WHERE operation IN ('activated', 'activation_unchanged')"
               ).rows
    end

    test "activation requires both the selected TenantFair contract and exact function catalog",
         %{
           context: legacy_context,
           activation_context: context
         } do
      prepare_ready!(context, "public")
      register!(context)
      assertion = attest!(context, "function-negative-proof")
      drop_function!("public")

      assert {:ok, %{reasons: reasons}} = Activation.preflight(context)
      assert :function_contract_mismatch in reasons

      assert {:error, {:claim_policy_unavailable, :function_contract_mismatch}} =
               Activation.activate(
                 context,
                 mode_opts("function-absent", 0, assertion.assertion_id)
               )

      install_function!("public")

      assert {:ok, %{reasons: []}} = Activation.preflight(context)

      assert {:error, {:claim_policy_unavailable, :function_contract_mismatch}} =
               Activation.activate(
                 legacy_context,
                 mode_opts("implementation-legacy", 0, assertion.assertion_id)
               )

      TestRepo.query!("""
      ALTER FUNCTION docket_tenant_fair_claim_v1(
        timestamp with time zone, timestamp with time zone, integer, integer, text, text[]
      ) STABLE
      """)

      assert {:ok, %{reasons: reasons}} = Activation.preflight(context)
      assert :function_contract_mismatch in reasons

      assert {:error, {:claim_policy_unavailable, :function_contract_mismatch}} =
               Activation.activate(
                 context,
                 mode_opts("function-stable", 0, assertion.assertion_id)
               )

      install_function!("public")

      tampered_source = "\nBEGIN\n  RETURN;\nEND;\n"

      tampered_sql =
        Function.create_sql("public")
        |> String.replace_prefix("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION")
        |> String.replace(Function.prosrc("public"), tampered_source)

      TestRepo.query!(tampered_sql)

      assert {:ok, %{reasons: reasons}} = Activation.preflight(context)
      assert :function_contract_mismatch in reasons

      assert {:error, {:claim_policy_unavailable, :function_contract_mismatch}} =
               Activation.activate(
                 context,
                 mode_opts("function-body-tampered", 0, assertion.assertion_id)
               )

      install_function!("public")

      TestRepo.query!("""
      CREATE FUNCTION docket_tenant_fair_claim_v1(integer)
      RETURNS integer
      LANGUAGE sql
      IMMUTABLE
      AS 'SELECT $1'
      """)

      assert {:ok, %{reasons: reasons}} = Activation.preflight(context)
      assert :function_contract_mismatch in reasons

      assert {:error, {:claim_policy_unavailable, :function_contract_mismatch}} =
               Activation.activate(
                 context,
                 mode_opts("function-overload", 0, assertion.assertion_id)
               )

      assert [["legacy", 0]] =
               TestRepo.query!(
                 "SELECT admission_mode, mode_epoch FROM docket_claim_admission_gate"
               ).rows
    end

    test "a held gate makes Legacy admission return promptly without claiming", %{
      context: context
    } do
      insert_ready_run!(context)
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT id FROM docket_claim_admission_gate FOR UPDATE")
            send(parent, {:gate_locked, self()})

            receive do
              :release_gate -> :ok
            end
          end)
        end)

      assert_receive {:gate_locked, holder_pid}
      started = System.monotonic_time(:millisecond)

      assert {:error, {:claim_policy_unavailable, :lock_contention}} =
               RunStore.claim_due(context, :system, policy())

      assert System.monotonic_time(:millisecond) - started < 500

      assert [[nil]] =
               TestRepo.query!(
                 "SELECT claim_token FROM docket_runs WHERE run_id = 'gate-blocked-run'"
               ).rows

      send(holder_pid, :release_gate)
      assert {:ok, _} = Task.await(holder)
    end

    test "activation waits out a participating admission and closes Legacy after the flip",
         %{context: context, activation_context: activation_context} do
      prepare_ready!(activation_context, "public")
      install_function!("public")
      register!(activation_context)
      assertion = attest!(activation_context, "barrier-proof")
      parent = self()

      admission =
        Task.async(fn ->
          Docket.Postgres.transaction(context, fn tx ->
            assert {:ok, %{leases: [], poisoned: []}} =
                     RunStore.claim_due(tx, :system, policy())

            send(parent, {:admission_holds_gate, self()})

            receive do
              :release_admission -> {:ok, :released}
            end
          end)
        end)

      assert_receive {:admission_holds_gate, admission_pid}

      activation =
        Task.async(fn ->
          Activation.activate(
            activation_context,
            mode_opts("barrier-activate", 0, assertion.assertion_id)
          )
        end)

      wait_until(fn -> gate_update_waiting?() end)

      # PostgreSQL permits another compatible SHARE participant to join the
      # existing row MultiXact even while the activation UPDATE is queued.
      # Safety therefore comes from activation waiting for *every* participant,
      # not from an anti-barging property that row locks do not provide.
      barging_admission =
        Task.async(fn ->
          Docket.Postgres.transaction(context, fn tx ->
            assert {:ok, %{leases: [], poisoned: []}} =
                     RunStore.claim_due(tx, :system, policy())

            send(parent, {:barging_admission_holds_gate, self()})

            receive do
              :release_barging_admission -> {:ok, :released}
            end
          end)
        end)

      assert_receive {:barging_admission_holds_gate, barging_pid}

      refute Task.yield(activation, 20)
      send(admission_pid, :release_admission)
      assert {:ok, :released} = Task.await(admission)
      refute Task.yield(activation, 20)
      send(barging_pid, :release_barging_admission)
      assert {:ok, :released} = Task.await(barging_admission)
      assert {:ok, %{outcome: :applied, version: 1}} = Task.await(activation)

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(context, :system, policy())
    end

    test "sustained shared authority bounds activation with the exact gate timeout", %{
      activation_context: context
    } do
      prepare_ready!(context, "public")
      install_function!("public")
      register!(context)
      assertion = attest!(context, "timeout-proof")
      parent = self()

      holder =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT id FROM docket_claim_admission_gate FOR SHARE")
            send(parent, {:shared_gate_locked, self()})

            receive do
              :release_shared_gate -> :ok
            end
          end)
        end)

      assert_receive {:shared_gate_locked, holder_pid}
      started = System.monotonic_time(:millisecond)

      assert {:error, {:lock_timeout, :gate}} =
               Activation.activate(
                 context,
                 mode_opts("timeout-activate", 0, assertion.assertion_id)
               )

      elapsed = System.monotonic_time(:millisecond) - started
      assert elapsed >= 800
      assert elapsed < 2_500
      send(holder_pid, :release_shared_gate)
      assert {:ok, _} = Task.await(holder)

      assert [["legacy", 0]] =
               TestRepo.query!(
                 "SELECT admission_mode, mode_epoch FROM docket_claim_admission_gate"
               ).rows
    end

    test "control-plane mutators reject transaction-owned contexts before SQL", %{
      context: context
    } do
      assert {:ok, :checked} =
               Docket.Postgres.transaction(context, fn tx ->
                 assert {:error, :transaction_context_forbidden} =
                          Activation.register_capability(
                            tx,
                            Ecto.UUID.generate(),
                            capability_opts()
                          )

                 assert {:error, :transaction_context_forbidden} =
                          Activation.attest_old_binaries_absent(tx, assertion_opts("tx"))

                 assert {:error, :transaction_context_forbidden} =
                          Activation.activate(
                            tx,
                            mode_opts("tx-activate", 0, Ecto.UUID.generate())
                          )

                 assert {:error, :transaction_context_forbidden} =
                          Activation.deactivate(tx, mode_opts("tx-deactivate", 0))

                 {:ok, :checked}
               end)
    end

    defp prepare_ready!(context, prefix) do
      suffix = String.replace(prefix, "_", "-")

      assert {:ok, _assertion} =
               Readiness.attest_dual_write(context,
                 evidence_fingerprint: :crypto.hash(:sha256, "dual-write-#{suffix}"),
                 source: "activation-readiness",
                 event_id: "dual-write-#{suffix}",
                 actor: "operator"
               )

      advance_until_complete!(context)

      assert {:ok, %{version: 1}} =
               Admin.bootstrap_default(context, @policy,
                 source: "activation-test",
                 event_id: "bootstrap-#{prefix}",
                 actor: "operator",
                 expected_version: 0
               )

      assert :ok = OnlineMigration.up(repo: TestRepo, prefix: prefix)
      fingerprints = OnlineDDL.index_fingerprints(prefix)

      assert {:ok, %{outcome: :applied, target: :readiness, version: 1}} =
               Readiness.verify(context,
                 expected_readiness_epoch: 0,
                 ready_index_ddl_sha256: fingerprints.ready,
                 live_index_ddl_sha256: fingerprints.live,
                 source: "activation-readiness",
                 event_id: "verify-#{suffix}",
                 actor: "operator"
               )
    end

    defp advance_until_complete!(context) do
      case Backfill.advance(context, batch_size: 10_000) do
        {:ok, %{phase: :complete}} -> :ok
        {:ok, _state} -> advance_until_complete!(context)
      end
    end

    defp install_function!(prefix) do
      drop_function!(prefix)
      TestRepo.query!(Function.create_sql(prefix))
    end

    defp drop_function!(prefix), do: TestRepo.query!(Function.drop_sql(prefix))

    defp register!(context, overrides \\ []) do
      assert {:ok, %{instance_id: instance_id, expires_at: %DateTime{}}} =
               Activation.register_capability(
                 context,
                 Keyword.get(overrides, :instance_id, "00000000-0000-4000-8000-000000000071"),
                 capability_opts(overrides)
               )

      instance_id
    end

    defp capability_opts(overrides \\ []) do
      [
        binary_fingerprint: :crypto.hash(:sha256, "dckt-71-test-binary"),
        writer_contract: Keyword.get(overrides, :writer_contract, 1),
        gate_contract: Keyword.get(overrides, :gate_contract, 1),
        function_contract: Keyword.get(overrides, :function_contract, 1),
        ttl_ms: Keyword.get(overrides, :ttl_ms, :timer.minutes(5))
      ]
    end

    defp attest!(context, event_id) do
      assert {:ok, assertion} =
               Activation.attest_old_binaries_absent(context, assertion_opts(event_id))

      assertion
    end

    defp assertion_opts(event_id) do
      [
        source: "activation-test",
        event_id: event_id,
        actor: "release-operator",
        evidence_fingerprint: :crypto.hash(:sha256, event_id),
        expires_at: DateTime.add(DateTime.utc_now(), 300, :second)
      ]
    end

    defp mode_opts(event_id, epoch, assertion_id \\ nil) do
      base = [
        source: "activation-test",
        event_id: event_id,
        actor: "release-operator",
        expected_epoch: epoch
      ]

      if assertion_id, do: base ++ [old_binary_assertion_id: assertion_id], else: base
    end

    defp assert_activation_not_ready(context, assertion_id, event_id) do
      assert {:error, {:activation_precondition_failed, :not_ready}} =
               Activation.activate(context, mode_opts(event_id, 0, assertion_id))
    end

    defp policy do
      %{
        now: @now,
        limit: 1,
        orphan_ttl_ms: 60_000,
        max_claim_attempts: 5,
        preference: nil
      }
    end

    defp insert_ready_run!(context, run_id \\ "gate-blocked-run") do
      unless TestRepo.exists?(
               from(version in GraphVersion,
                 where:
                   is_nil(version.tenant_id) and version.graph_id == "activation-graph" and
                     version.graph_hash == "activation-hash"
               )
             ) do
        TestRepo.insert!(
          GraphVersion.changeset(%{
            tenant_id: nil,
            graph_id: "activation-graph",
            graph_hash: "activation-hash",
            graph: <<131, 106>>
          })
        )
      end

      run = %Docket.Run{
        id: run_id,
        graph_id: "activation-graph",
        graph_hash: "activation-hash",
        status: :running,
        input: %{},
        metadata: %{},
        started_at: @now,
        updated_at: @now,
        checkpoint_seq: 1
      }

      assert {:ok, ^run} =
               RunStore.insert_run(context, :tenantless, run, :run_initialized, @now)
    end

    defp gate_update_waiting? do
      gate_update_waiter_count() > 0
    end

    defp gate_update_waiter_count do
      TestRepo.query!("""
      SELECT count(*)::bigint
      FROM pg_stat_activity AS activity
      WHERE activity.datname = current_database()
        AND activity.pid <> pg_backend_pid()
        AND activity.wait_event_type = 'Lock'
        AND activity.query LIKE '%docket_claim_admission_gate%'
        AND activity.query LIKE '%FOR UPDATE%'
      """).rows
      |> then(fn [[count]] -> count end)
    end

    defp capability_insert_waiting? do
      waiting_query?("%INSERT INTO%", "%docket_claim_capabilities%")
    end

    defp rollout_share_waiting? do
      waiting_query?("%FOR SHARE OF rollout%", "%docket_claim_rollout%")
    end

    defp capability_gate_waiting? do
      waiting_query?("%FOR SHARE%", "%docket_claim_admission_gate%")
    end

    defp waiting_query?(first_pattern, second_pattern) do
      TestRepo.query!(
        """
        SELECT EXISTS (
          SELECT 1
          FROM pg_stat_activity AS activity
          WHERE activity.datname = current_database()
            AND activity.pid <> pg_backend_pid()
            AND activity.wait_event_type = 'Lock'
            AND activity.query LIKE $1
            AND activity.query LIKE $2
        )
        """,
        [first_pattern, second_pattern]
      ).rows == [[true]]
    end

    defp hold_shared_gate(parent, token) do
      Task.async(fn ->
        TestRepo.transaction(fn ->
          TestRepo.query!("SELECT id FROM docket_claim_admission_gate FOR SHARE")
          send(parent, {:shared_gate_held, token, self()})

          receive do
            {:release_shared_gate, ^token} -> :ok
          end
        end)
      end)
    end

    defp database_expiry(milliseconds) do
      [[expires_at]] =
        TestRepo.query!(
          "SELECT clock_timestamp() + ($1::bigint * interval '1 millisecond')",
          [milliseconds]
        ).rows

      expires_at
    end

    defp database_time_reached?(expires_at) do
      TestRepo.query!("SELECT clock_timestamp() >= $1", [expires_at]).rows == [[true]]
    end

    defp wait_until(predicate, attempts \\ 100)

    defp wait_until(predicate, attempts) when attempts > 0 do
      if predicate.() do
        :ok
      else
        receive do
        after
          10 -> wait_until(predicate, attempts - 1)
        end
      end
    end

    defp wait_until(_predicate, 0), do: flunk("deterministic gate barrier was not reached")
  end
end
