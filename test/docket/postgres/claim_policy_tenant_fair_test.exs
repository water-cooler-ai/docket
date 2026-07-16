if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyTenantFairTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.{Activation, Admin, Backfill, OnlineDDL, Readiness}
    alias Docket.Postgres.ClaimPolicy.TenantFair
    alias Docket.Postgres.ClaimPolicy.TenantFair.Function
    alias Docket.Postgres.ClaimPolicyAdminTestRepo, as: TestRepo
    alias Docket.Postgres.{OnlineMigration, RunStore}
    alias Docket.Postgres.Schemas.GraphVersion
    alias Docket.Test.ConcurrentAdmissionHarness
    alias Docket.Test.Fixtures.Graphs

    @migration_version 20_260_716_000_068
    @now ~U[2026-07-16 12:00:00.000000Z]
    @default_policy %{preferred_active: 2, max_active: 4, weight: 1, borrowing: false}

    defmodule InstallDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule TenantFairManualHost do
      use Docket,
        backend: Docket.Postgres,
        repo: Docket.Postgres.ClaimPolicyAdminTestRepo,
        testing: :manual,
        notifier: :none,
        claim_policy: [
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          partition_by: :tenant_id,
          default_preferred_active: 2,
          default_max_active: 4,
          default_weight: 1,
          borrowing: false
        ]
    end

    defmodule TenantFairInlineHost do
      use Docket,
        backend: Docket.Postgres,
        repo: Docket.Postgres.ClaimPolicyAdminTestRepo,
        testing: :inline,
        notifier: :none,
        claim_policy: [
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          partition_by: :tenant_id,
          default_preferred_active: 2,
          default_max_active: 4,
          default_weight: 1,
          borrowing: false
        ]
    end

    defmodule TenantFairSupervisedHost do
      use Docket,
        backend: Docket.Postgres,
        repo: Docket.Postgres.ClaimPolicyAdminTestRepo,
        notifier: :none,
        dispatcher: [concurrency: 1, poll_interval_ms: 10],
        pruner: [
          interval_ms: :timer.hours(1),
          event_retention_ms: :timer.hours(24 * 30),
          run_retention_ms: :timer.hours(24 * 90),
          batch_size: 100
        ],
        claim_policy: [
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          partition_by: :tenant_id,
          default_preferred_active: 2,
          default_max_active: 4,
          default_weight: 1,
          borrowing: false
        ]
    end

    setup do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      context =
        Docket.Postgres.context(
          repo: TestRepo,
          claim_policy: [
            implementation: TenantFair,
            partition_by: :tenant_id,
            default_preferred_active: 2,
            default_max_active: 4,
            default_weight: 1,
            borrowing: false
          ]
        )

      for tenant_id <- [nil, "tenant-a", "tenant-b", "tenant-c"] do
        TestRepo.insert!(
          GraphVersion.changeset(%{
            tenant_id: tenant_id,
            graph_id: "tenant-fair-graph",
            graph_hash: "tenant-fair-hash",
            graph: <<131, 106>>
          })
        )
      end

      activate!(context)
      %{context: context}
    end

    test "state, class, poison, cap-zero, and epoch witness matrix is exact", %{context: context} do
      cases = [
        {:running, :ready, :lease, 1},
        {:running, :expired, :lease, 0},
        {:running, :ready_poison, :poison, 0},
        {:running, :expired_poison, :poison, 0},
        {:hold_new, :ready, :empty, 0},
        {:hold_new, :expired, :lease, 0},
        {:hold_new, :ready_poison, :poison, 0},
        {:hold_new, :expired_poison, :poison, 0},
        {:drain, :ready, :empty, 0},
        {:drain, :expired, :empty, 0},
        {:drain, :ready_poison, :poison, 0},
        {:drain, :expired_poison, :poison, 0}
      ]

      for {state, kind, expected, epoch} <- cases do
        reset_work!()
        insert_candidate!(context, "tenant-a", "matrix", kind)
        set_partition!("tenant-a", state: state)

        assert_claim_shape(context, expected, "matrix")
        assert [[^epoch]] = partition_query("tenant-a", "admission_epoch")
      end

      for kind <- [:ready, :expired] do
        reset_work!()
        insert_candidate!(context, "tenant-a", "zero-cap", kind)
        set_partition!("tenant-a", max_active: 0)

        expected = if kind == :ready, do: :empty, else: :lease
        assert_claim_shape(context, expected, "zero-cap")
      end

      for kind <- [:ready_poison, :expired_poison] do
        reset_work!()
        insert_candidate!(context, "tenant-a", "zero-cap-poison", kind)
        set_partition!("tenant-a", max_active: 0)
        assert_claim_shape(context, :poison, "zero-cap-poison")
      end
    end

    test "concurrent tenant and tenantless contenders cannot overfill the last slot", %{
      context: context
    } do
      for tenant_id <- ["tenant-a", nil] do
        reset_work!()
        scope_key = tenant_id || ""

        insert_candidate!(context, tenant_id, "live", :live)
        insert_candidate!(context, tenant_id, "ready-a", :ready)
        insert_candidate!(context, tenant_id, "ready-b", :ready)
        set_partition!(scope_key, max_active: 2)

        results =
          ConcurrentAdmissionHarness.run_callers!(TestRepo, [
            {:one, fn -> claim(context) end},
            {:two, fn -> claim(context) end}
          ])

        leases =
          Enum.flat_map(results, fn %{result: result} ->
            case result do
              {:ok, %{leases: leases}} -> leases
              {:error, {:claim_policy_unavailable, :lock_contention}} -> []
            end
          end)

        assert length(leases) == 1
        assert hd(leases).run_id in ["ready-a", "ready-b"]

        assert [[2]] =
                 TestRepo.query!(
                   """
                   SELECT count(*)::bigint
                   FROM docket_runs
                   WHERE scope_key = $1 AND status = 'running'
                     AND poisoned_at IS NULL AND claim_token IS NOT NULL
                   """,
                   [scope_key]
                 ).rows
      end
    end

    test "tenantless partitions inherit the persisted default without materializing an override",
         %{
           context: context
         } do
      reset_work!()
      TestRepo.query!("UPDATE docket_claim_policy SET preferred_active = 0, max_active = 1")
      insert_candidate!(context, nil, "tenantless-a", :ready)
      insert_candidate!(context, nil, "tenantless-b", :ready)

      handler = "tenant-fair-default-inheritance-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :claim_policy, :admission, :observation],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok, %{leases: [lease], poisoned: []}} = claim(context, limit: 2)
      assert lease.run_id in ["tenantless-a", "tenantless-b"]

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                      %{default_policy_partitions: 1, override_policy_partitions: 0, outcomes: 1},
                      %{policy_source: :default}}

      assert [[nil, nil, nil, nil]] =
               TestRepo.query!("""
               SELECT preferred_active, max_active, weight, borrowing
               FROM docket_claim_partitions
               WHERE scope_key = ''
               """).rows
    end

    test "outer transaction claim authority is provisional on rollback and durable on commit", %{
      context: context
    } do
      for disposition <- [:rollback, :commit] do
        reset_work!()
        insert_candidate!(context, "tenant-a", "held-#{disposition}", :ready)
        set_partition!("tenant-a", max_active: 1)

        holder = hold_claim_transaction!(context, disposition)
        assert {:error, {:claim_policy_unavailable, :lock_contention}} = claim(context)
        release_claim_transaction!(holder, disposition)

        expected = if disposition == :rollback, do: :lease, else: :empty
        assert_claim_shape(context, expected, "held-#{disposition}")
      end

      reset_work!()
      insert_candidate!(context, "tenant-a", "rollback-run", :ready)
      set_partition!("tenant-a", max_active: 1)

      assert {:error, :rollback_after_claim} =
               Docket.Postgres.transaction(context, fn tx ->
                 assert_claim_shape(tx, :lease, "rollback-run")
                 {:error, :rollback_after_claim}
               end)

      assert_claim_shape(context, :lease, "rollback-run")
      release_all!(context)

      assert {:ok, :committed} =
               Docket.Postgres.transaction(context, fn tx ->
                 assert_claim_shape(tx, :lease, "rollback-run")
                 {:ok, :committed}
               end)

      assert_claim_shape(context, :empty, "rollback-run")

      reset_work!()
      insert_candidate!(context, "tenant-a", "raised-run", :ready)

      assert_raise RuntimeError, "rollback claim", fn ->
        Docket.Postgres.transaction(context, fn tx ->
          assert_claim_shape(tx, :lease, "raised-run")
          raise "rollback claim"
        end)
      end

      assert_claim_shape(context, :lease, "raised-run")
    end

    test "default and override downgrades create debt without blocking zero-net steals", %{
      context: context
    } do
      reset_work!()
      insert_candidate!(context, "tenant-a", "default-live", :live)
      insert_candidate!(context, "tenant-a", "default-ready", :ready)
      insert_candidate!(context, "tenant-b", "override-live", :live)
      insert_candidate!(context, "tenant-b", "override-ready", :ready)

      TestRepo.query!("UPDATE docket_claim_policy SET preferred_active = 0, max_active = 1")
      set_partition!("tenant-b", max_active: 0)

      assert {:ok, %{leases: [], poisoned: []}} = claim(context, limit: 2)

      TestRepo.query!(
        """
        UPDATE docket_runs
        SET claimed_at = $1
        WHERE run_id IN ('default-live', 'override-live')
        """,
        [DateTime.add(@now, -120, :second)]
      )

      assert {:ok, %{leases: leases, poisoned: []}} =
               claim(context, limit: 2, preference: :expired)

      assert leases |> Enum.map(& &1.run_id) |> Enum.sort() ==
               ["default-live", "override-live"]

      assert [[1], [1]] =
               TestRepo.query!("""
               SELECT count(*)::bigint
               FROM docket_runs
               WHERE scope_key IN ('tenant-a', 'tenant-b')
                 AND poisoned_at IS NULL AND claim_token IS NOT NULL
               GROUP BY scope_key
               ORDER BY scope_key
               """).rows
    end

    test "held gate, default, partition, and run rows produce exact prompt or partial outcomes",
         %{
           context: context
         } do
      for {table, key_sql, key_params} <- [
            {"docket_claim_admission_gate", "id = 1", []},
            {"docket_claim_policy", "id = 1", []},
            {"docket_claim_partitions", "scope_key = $1", ["tenant-a"]},
            {"docket_runs", "run_id = $1", ["sole-run"]}
          ] do
        reset_work!()
        insert_candidate!(context, "tenant-a", "sole-run", :ready)
        holder = hold_row!(table, key_sql, key_params)
        started = System.monotonic_time(:millisecond)

        assert {:error, {:claim_policy_unavailable, :lock_contention}} = claim(context)
        assert System.monotonic_time(:millisecond) - started < 1_000

        release_holder!(holder)
      end

      reset_work!()
      insert_candidate!(context, "tenant-a", "locked-partition-run", :ready)
      insert_candidate!(context, "tenant-b", "free-partition-run", :ready)
      holder = hold_row!("docket_claim_partitions", "scope_key = $1", ["tenant-a"])

      assert {:ok, %{leases: [%{run_id: "free-partition-run"}], poisoned: []}} =
               claim(context, limit: 2)

      release_holder!(holder)

      reset_work!()
      insert_candidate!(context, "tenant-a", "locked-run", :ready)
      insert_candidate!(context, "tenant-a", "free-run", :ready)
      holder = hold_row!("docket_runs", "run_id = $1", ["locked-run"])

      assert {:ok, %{leases: [%{run_id: "free-run"}], poisoned: []}} =
               claim(context, limit: 2)

      release_holder!(holder)
    end

    test "global stale hints distinguish all-skipped from invalidated and mixed candidates", %{
      context: context
    } do
      reset_work!()
      insert_candidate!(context, "tenant-a", "hint-a", :ready)
      insert_candidate!(context, "tenant-b", "hint-b", :ready)

      holder_a = hold_row!("docket_claim_partitions", "scope_key = $1", ["tenant-a"])
      holder_b = hold_row!("docket_claim_partitions", "scope_key = $1", ["tenant-b"])

      assert {:error, {:claim_policy_unavailable, :lock_contention}} = claim(context, limit: 2)
      release_holder!(holder_a)
      release_holder!(holder_b)

      reset_work!()
      insert_candidate!(context, "tenant-a", "stale", :ready)
      TestRepo.query!("DELETE FROM docket_runs WHERE run_id = 'stale'")

      assert {:ok, %{leases: [], poisoned: []}} =
               invoke_function(context, ["tenant-a"], demand: 1)

      insert_candidate!(context, "tenant-b", "mixed", :ready)
      TestRepo.query!("DELETE FROM docket_runs WHERE run_id = 'mixed'")
      holder = hold_row!("docket_claim_partitions", "scope_key = $1", ["tenant-a"])

      assert {:ok, %{leases: [], poisoned: []}} =
               invoke_function(context, ["tenant-a", "tenant-b"], demand: 2)

      release_holder!(holder)
    end

    test "read-only and repeatable-read sentinels leave transactions usable and restore lock_timeout",
         %{
           context: context
         } do
      reset_work!()
      insert_candidate!(context, "tenant-a", "tx-sentinel", :ready)

      assert {:ok, :read_only_usable} =
               TestRepo.transaction(fn ->
                 TestRepo.query!("SET TRANSACTION READ ONLY")

                 assert {:error, {:claim_policy_unavailable, :read_only_transaction}} =
                          claim(context)

                 assert [[1]] = TestRepo.query!("SELECT 1").rows
                 :read_only_usable
               end)

      assert {:ok, :repeatable_read_usable} =
               TestRepo.transaction(fn ->
                 TestRepo.query!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                 TestRepo.query!("SET LOCAL lock_timeout = '777ms'")

                 assert {:error, {:claim_policy_unavailable, :unsupported_isolation}} =
                          claim(context)

                 assert [["777ms"]] = TestRepo.query!("SHOW lock_timeout").rows
                 assert [[1]] = TestRepo.query!("SELECT 1").rows
                 :repeatable_read_usable
               end)

      assert {:ok, :restored} =
               TestRepo.transaction(fn ->
                 TestRepo.query!("SET LOCAL lock_timeout = '777ms'")
                 assert_claim_shape(context, :lease, "tx-sentinel")
                 assert [["777ms"]] = TestRepo.query!("SHOW lock_timeout").rows
                 :restored
               end)

      raw_statement = """
      SELECT claimed.*
      FROM "public"."#{Function.name()}"($1, $2, $3, $4, $5, $6)
      AS claimed(#{Function.result_definition()})
      """

      assert {:error, %Postgrex.Error{postgres: %{code: :invalid_parameter_value}}} =
               TestRepo.query(raw_statement, [
                 @now,
                 DateTime.add(@now, -60, :second),
                 0,
                 5,
                 nil,
                 []
               ])

      assert [[1]] = TestRepo.query!("SELECT 1").rows
    end

    test "caught lock_not_available rolls back earlier epoch mutation and emits no provisional row",
         %{
           context: context
         } do
      reset_work!()
      insert_candidate!(context, "tenant-a", "triggered", :ready)

      TestRepo.query!("""
      CREATE FUNCTION docket_test_raise_lock_not_available()
      RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'test lock failure' USING ERRCODE = '55P03';
      END
      $$
      """)

      TestRepo.query!("""
      CREATE TRIGGER docket_test_lock_failure
      BEFORE UPDATE OF claim_token ON docket_runs
      FOR EACH ROW EXECUTE FUNCTION docket_test_raise_lock_not_available()
      """)

      assert {:error, {:claim_policy_unavailable, :lock_contention}} = claim(context)
      assert [[0]] = partition_query("tenant-a", "admission_epoch")

      assert [[nil, nil, 0]] =
               TestRepo.query!(
                 "SELECT claim_token, claimed_at, claim_attempts FROM docket_runs WHERE run_id = 'triggered'"
               ).rows
    end

    test "preference progresses both classes while reserved policy fields are behaviorally inert",
         %{
           context: context
         } do
      reset_work!()
      insert_candidate!(context, "tenant-a", "ready", :ready)
      insert_candidate!(context, "tenant-a", "expired", :expired)

      assert {:ok, %{leases: [%{run_id: "ready"}]}} = claim(context, preference: :ready)
      assert {:ok, %{leases: [%{run_id: "expired"}]}} = claim(context, preference: :expired)

      snapshots =
        for {preferred, weight, borrowing} <- [{0, 1, false}, {4, 99, true}] do
          reset_work!()

          TestRepo.query!(
            """
            UPDATE docket_claim_policy
            SET preferred_active = $1, max_active = 4, weight = $2, borrowing = $3
            WHERE id = 1
            """,
            [preferred, weight, borrowing]
          )

          insert_candidate!(context, "tenant-a", "reserved-ready", :ready)
          insert_candidate!(context, "tenant-a", "reserved-expired", :expired)

          assert {:ok, %{leases: leases, poisoned: []}} = claim(context, limit: 2)
          Enum.map(leases, &{&1.run_id, &1.claim_attempt})
        end

      assert [first, first] = snapshots
    end

    test "page-wide class reservation progresses a later partition in either key order", %{
      context: context
    } do
      for {first_candidates, second_candidates, expected} <- [
            {
              [{"first-ready-a", :ready}, {"first-ready-b", :ready}],
              [{"second-expired", :expired}],
              ["first-ready-a", "second-expired"]
            },
            {
              [{"first-expired", :expired}],
              [{"second-ready-a", :ready}, {"second-ready-b", :ready}],
              ["first-expired", "second-ready-a"]
            }
          ] do
        reset_work!()

        for {run_id, kind} <- first_candidates,
            do: insert_candidate!(context, "tenant-a", run_id, kind)

        for {run_id, kind} <- second_candidates,
            do: insert_candidate!(context, "tenant-b", run_id, kind)

        assert {:ok, %{leases: leases, poisoned: []}} = claim(context, limit: 2)
        assert Enum.map(leases, & &1.run_id) |> Enum.sort() == Enum.sort(expected)
      end
    end

    test "demand one honors class preference and falls through within its single key", %{
      context: context
    } do
      for {preference, available, expected} <- [
            {:ready, [:ready, :expired], "ready"},
            {:expired, [:ready, :expired], "expired"},
            {:ready, [:expired], "expired"},
            {:expired, [:ready], "ready"}
          ] do
        reset_work!()

        for kind <- available do
          insert_candidate!(context, "tenant-a", Atom.to_string(kind), kind)
        end

        assert {:ok, %{leases: [%{run_id: ^expected}], poisoned: []}} =
                 claim(context, preference: preference)
      end
    end

    test "demand one applies preference after class-balanced decision sourcing", %{
      context: context
    } do
      for {class_order, preference} <- [
            {[:expired, :ready], :ready},
            {[:expired, :ready], :expired},
            {[:ready, :expired], :ready},
            {[:ready, :expired], :expired}
          ] do
        reset_work!()

        for class <- class_order do
          insert_candidate!(context, "tenant-a", "#{class}-poison", poison_kind(class))
          insert_candidate!(context, "tenant-a", "#{class}-ordinary", class)
        end

        expected = "#{preference}-poison"

        assert {:ok, %{leases: [], poisoned: [%{run_id: ^expected}]}} =
                 claim(context, preference: preference)
      end

      for {preference, available} <- [{:ready, :expired}, {:expired, :ready}] do
        reset_work!()
        insert_candidate!(context, "tenant-a", "fallback-poison", poison_kind(available))
        insert_candidate!(context, "tenant-a", "fallback-ordinary", available)

        assert {:ok, %{leases: [], poisoned: [%{run_id: "fallback-poison"}]}} =
                 claim(context, preference: preference)
      end
    end

    test "same-key class progress survives four lower-ID rows from the other class", %{
      context: context
    } do
      reset_work!()

      for run_id <- ["ready-poison-a", "ready-poison-b"] do
        insert_candidate!(context, "tenant-a", run_id, :ready_poison)
      end

      for run_id <- ["ready-ordinary-a", "ready-ordinary-b"] do
        insert_candidate!(context, "tenant-a", run_id, :ready)
      end

      insert_candidate!(context, "tenant-a", "higher-id-expired", :expired)
      set_partition!("tenant-a", max_active: 8)

      handler = "tenant-fair-same-key-classes-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :run_store, :claim],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok,
              %{
                leases: [%{run_id: "higher-id-expired"}],
                poisoned: [%{run_id: ready_poison}]
              }} = claim(context, limit: 2)

      assert ready_poison in ["ready-poison-a", "ready-poison-b"]

      assert_receive {[:docket, :postgres, :run_store, :claim],
                      %{ready_candidates: 4, expired_candidates: 1}, %{result: :ok}}
    end

    test "cross-key reservation cannot lose its later class to the global lock budget", %{
      context: context
    } do
      cases = [
        {
          [{"early-ready-poison", :ready_poison}, {"early-ready", :ready}],
          [{"later-expired", :expired}],
          "early-ready-poison",
          "later-expired"
        },
        {
          [{"early-expired-poison", :expired_poison}, {"early-expired", :expired}],
          [{"later-ready", :ready}],
          "early-expired-poison",
          "later-ready"
        }
      ]

      for {early, later, expected_poison, expected_lease} <- cases do
        reset_work!()

        for {run_id, kind} <- early,
            do: insert_candidate!(context, "tenant-a", run_id, kind)

        for {run_id, kind} <- later,
            do: insert_candidate!(context, "tenant-b", run_id, kind)

        assert {:ok,
                %{
                  leases: [%{run_id: ^expected_lease}],
                  poisoned: [%{run_id: ^expected_poison}]
                }} = claim(context, limit: 2)
      end
    end

    test "decision-source row locks stay within twice demand while raw telemetry stays truthful",
         %{
           context: context
         } do
      reset_work!()

      for class <- [:ready, :expired] do
        for suffix <- ["a", "b"] do
          insert_candidate!(context, "tenant-a", "#{class}-poison-#{suffix}", poison_kind(class))
          insert_candidate!(context, "tenant-a", "#{class}-ordinary-#{suffix}", class)
        end
      end

      set_partition!("tenant-a", max_active: 8)
      handler = "tenant-fair-lock-footprint-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :run_store, :claim],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      {holder, holder_pid, result} = hold_claim!(context, limit: 2)
      assert {:ok, %{poisoned: poisoned}} = result
      assert length(poisoned) == 2

      unlocked =
        TestRepo.query!("""
        SELECT id
        FROM docket_runs
        WHERE scope_key = 'tenant-a' AND status = 'running'
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        """).num_rows

      assert 8 - unlocked == 4
      assert 8 - unlocked <= 2 * 2

      assert_receive {[:docket, :postgres, :run_store, :claim],
                      %{ready_candidates: 4, expired_candidates: 4}, %{result: :ok}}

      release_claim!(holder, holder_pid, result)
    end

    test "a stale class hint does not block progress or become false contention", %{
      context: context
    } do
      reset_work!()
      insert_candidate!(context, "tenant-a", "invalidated-ready", :ready)
      insert_candidate!(context, "tenant-b", "surviving-expired", :expired)
      TestRepo.query!("DELETE FROM docket_runs WHERE run_id = 'invalidated-ready'")

      assert {:ok, %{leases: [%{run_id: "surviving-expired"}], poisoned: []}} =
               invoke_function(context, ["tenant-a", "tenant-b"], demand: 2)
    end

    test "a held run in one class allows a partial outcome from the other class", %{
      context: context
    } do
      reset_work!()
      insert_candidate!(context, "tenant-a", "held-ready", :ready)
      insert_candidate!(context, "tenant-a", "free-expired", :expired)
      holder = hold_row!("docket_runs", "run_id = $1", ["held-ready"])

      assert {:ok, %{leases: [%{run_id: "free-expired"}], poisoned: []}} =
               claim(context, limit: 2)

      release_holder!(holder)
    end

    test "candidate accounting reports bounded raw lanes, including cap-denied and poison rows",
         %{
           context: context
         } do
      handler = "tenant-fair-accounting-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :claim_policy, :admission, :observation],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      :telemetry.attach(
        "#{handler}-run-store",
        [:docket, :postgres, :run_store, :claim],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn ->
        :telemetry.detach(handler)
        :telemetry.detach("#{handler}-run-store")
      end)

      reset_work!()

      for index <- 1..64 do
        insert_candidate!(context, "tenant-a", "backlog-#{index}", :ready)
      end

      set_partition!("tenant-a", max_active: 0)
      assert {:ok, %{leases: [], poisoned: []}} = claim(context, limit: 3)

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                      %{candidate_rows_examined: 3, outcomes: 0, cap_denied_partitions: 1},
                      %{result: :ok}}

      reset_work!()
      insert_candidate!(context, "tenant-a", "denied-ordinary", :ready)
      insert_candidate!(context, "tenant-a", "visible-poison", :ready_poison)
      set_partition!("tenant-a", max_active: 0)

      assert {:ok, %{leases: [], poisoned: [%{run_id: "visible-poison"}]}} = claim(context)

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                      %{
                        candidate_rows_examined: 2,
                        ready_poisoned: 1,
                        outcomes: 1
                      }, %{result: :ok}}

      assert_receive {[:docket, :postgres, :run_store, :claim],
                      %{ready_candidates: 2, expired_candidates: 0}, %{result: :ok}}
    end

    test "actual manual, inline, and supervised callers execute TenantFair end to end" do
      start_supervised!(TenantFairManualHost)
      assert {:ok, manual_ref} = TenantFairManualHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %Docket.Run{status: :running} = manual_run} =
               TenantFairManualHost.start_run(manual_ref, %{"value" => "manual"})

      assert {:ok, %{drained: 1, poisoned: [], limit_reached: true}} =
               TenantFairManualHost.drain_runs(max_runs: 1)

      assert {:ok, %Docket.Run{status: :done}} =
               TenantFairManualHost.fetch_run(manual_run.id)

      start_supervised!(TenantFairInlineHost)
      assert {:ok, inline_ref} = TenantFairInlineHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %Docket.Run{status: :done}} =
               TenantFairInlineHost.start_run(inline_ref, %{"value" => "inline"})

      start_supervised!(TenantFairSupervisedHost)
      assert {:ok, supervised_ref} = TenantFairSupervisedHost.save_graph(Graphs.minimal_linear())

      assert {:ok, supervised_run} =
               TenantFairSupervisedHost.start_run(supervised_ref, %{"value" => "supervised"})

      assert {:ok, %Docket.Run{status: :done}} =
               TenantFairSupervisedHost.await_run(supervised_run.id, timeout: 5_000)
    end

    test "actual manual caller preserves TenantFair gate errors and expired recovery" do
      start_supervised!(TenantFairManualHost)
      assert {:ok, reference} = TenantFairManualHost.save_graph(Graphs.minimal_linear())

      assert {:ok, %Docket.Run{status: :running}} =
               TenantFairManualHost.start_run(reference, %{"value" => "contention"})

      holder = hold_row!("docket_claim_admission_gate", "id = 1", [])

      assert {:error, {:claim_policy_unavailable, :lock_contention}} =
               TenantFairManualHost.drain_runs(max_runs: 1)

      release_holder!(holder)

      TestRepo.query!(
        "UPDATE docket_claim_admission_gate SET readiness = 'not_ready' WHERE id = 1"
      )

      assert {:error, {:claim_policy_unavailable, :not_ready}} =
               TenantFairManualHost.drain_runs(max_runs: 1)

      TestRepo.query!("UPDATE docket_claim_admission_gate SET readiness = 'ready' WHERE id = 1")

      assert {:ok, %{drained: 1}} = TenantFairManualHost.drain_runs(max_runs: 1)

      assert {:ok, %Docket.Run{status: :running} = recovery} =
               TenantFairManualHost.start_run(reference, %{"value" => "recovery"})

      TestRepo.query!(
        """
        UPDATE docket_runs
        SET wake_at = NULL,
            claim_token = $2,
            claimed_at = $3,
            claim_attempts = 1
        WHERE run_id = $1
        """,
        [
          recovery.id,
          Ecto.UUID.dump!(Ecto.UUID.generate()),
          DateTime.add(DateTime.utc_now(), -120, :second)
        ]
      )

      assert {:ok, %{drained: 1}} = TenantFairManualHost.drain_runs(max_runs: 1)
      assert {:ok, %Docket.Run{status: :done}} = TenantFairManualHost.fetch_run(recovery.id)
    end

    test "empty discovery still produces one valid neutral observation summary", %{
      context: context
    } do
      reset_work!()
      handler = "tenant-fair-empty-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :claim_policy, :admission, :observation],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok, %{leases: [], poisoned: []}} = claim(context)

      assert_receive {[:docket, :postgres, :claim_policy, :admission, :observation],
                      %{
                        eligible_partitions: 0,
                        locked_partitions: 0,
                        preferred_admissions: 0,
                        borrowed_admissions: 0,
                        below_preferred_partitions: 0,
                        outcomes: 0,
                        unfilled_demand: 1
                      },
                      %{
                        result: :ok,
                        observation_status: :available,
                        admission_class: :none,
                        batch_shape: :no_op
                      }}
    end

    defp activate!(context) do
      assert {:ok, _} =
               Readiness.attest_dual_write(context,
                 evidence_fingerprint: :crypto.hash(:sha256, "tenant-fair-dual-write"),
                 source: "tenant-fair-test",
                 event_id: "dual-write",
                 actor: "test"
               )

      advance_until_complete!(context)

      assert {:ok, %{version: 1}} =
               Admin.bootstrap_default(context, @default_policy,
                 source: "tenant-fair-test",
                 event_id: "bootstrap",
                 actor: "test",
                 expected_version: 0
               )

      assert :ok = OnlineMigration.up(repo: TestRepo)
      fingerprints = OnlineDDL.index_fingerprints("public")

      assert {:ok, %{version: 1}} =
               Readiness.verify(context,
                 expected_readiness_epoch: 0,
                 ready_index_ddl_sha256: fingerprints.ready,
                 live_index_ddl_sha256: fingerprints.live,
                 source: "tenant-fair-test",
                 event_id: "verify",
                 actor: "test"
               )

      assert {:ok, _} =
               Activation.register_capability(context, "00000000-0000-4000-8000-000000000068",
                 binary_fingerprint: :crypto.hash(:sha256, "tenant-fair-binary"),
                 writer_contract: 1,
                 gate_contract: 1,
                 function_contract: Function.version(),
                 ttl_ms: :timer.minutes(5)
               )

      assert {:ok, assertion} =
               Activation.attest_old_binaries_absent(context,
                 source: "tenant-fair-test",
                 event_id: "old-binaries",
                 actor: "test",
                 evidence_fingerprint: :crypto.hash(:sha256, "old-binaries"),
                 expires_at: DateTime.add(DateTime.utc_now(), 300, :second)
               )

      assert {:ok, %{outcome: :applied, version: 1}} =
               Activation.activate(context,
                 source: "tenant-fair-test",
                 event_id: "activate",
                 actor: "test",
                 expected_epoch: 0,
                 old_binary_assertion_id: assertion.assertion_id
               )
    end

    defp advance_until_complete!(context) do
      case Backfill.advance(context, batch_size: 10_000) do
        {:ok, %{phase: :complete}} -> :ok
        {:ok, _} -> advance_until_complete!(context)
      end
    end

    defp reset_work! do
      TestRepo.query!("""
      TRUNCATE TABLE docket_events, docket_runs, docket_claim_partitions
      RESTART IDENTITY CASCADE
      """)
    end

    defp insert_candidate!(context, tenant_id, run_id, kind) do
      run = %Docket.Run{
        id: run_id,
        graph_id: "tenant-fair-graph",
        graph_hash: "tenant-fair-hash",
        status: :running,
        input: %{},
        metadata: %{},
        started_at: @now,
        updated_at: @now,
        checkpoint_seq: 1
      }

      scope = if tenant_id, do: {:tenant, tenant_id}, else: :tenantless
      assert {:ok, ^run} = RunStore.insert_run(context, scope, run, :run_initialized, @now)

      case kind do
        :ready ->
          :ok

        :ready_poison ->
          set_candidate!(run_id, nil, nil, 5)

        :live ->
          set_candidate!(run_id, Ecto.UUID.dump!(Ecto.UUID.generate()), @now, 1)

        :expired ->
          set_candidate!(
            run_id,
            Ecto.UUID.dump!(Ecto.UUID.generate()),
            DateTime.add(@now, -120, :second),
            0
          )

        :expired_poison ->
          set_candidate!(
            run_id,
            Ecto.UUID.dump!(Ecto.UUID.generate()),
            DateTime.add(@now, -120, :second),
            5
          )
      end
    end

    defp set_candidate!(run_id, token, claimed_at, attempts) do
      TestRepo.query!(
        """
        UPDATE docket_runs
        SET wake_at = CASE WHEN $2::uuid IS NULL THEN $4::timestamp with time zone ELSE NULL END,
            claim_token = $2,
            claimed_at = $3::timestamp with time zone,
            claim_attempts = $5
        WHERE run_id = $1
        """,
        [run_id, token, claimed_at, @now, attempts]
      )
    end

    defp poison_kind(:ready), do: :ready_poison
    defp poison_kind(:expired), do: :expired_poison

    defp set_partition!(scope_key, opts) do
      state = Keyword.get(opts, :state, :running)
      max_active = Keyword.get(opts, :max_active)

      TestRepo.query!(
        """
        UPDATE docket_claim_partitions
        SET admin_state = $2,
            preferred_active = CASE WHEN $3::integer IS NULL THEN preferred_active ELSE 0 END,
            max_active = COALESCE($3, max_active),
            weight = CASE WHEN $3::integer IS NULL THEN weight ELSE 1 END,
            borrowing = CASE WHEN $3::integer IS NULL THEN borrowing ELSE false END
        WHERE scope_key = $1
        """,
        [scope_key, Atom.to_string(state), max_active]
      )
    end

    defp claim(context, overrides \\ []) do
      RunStore.claim_due(context, :system, policy(overrides))
    end

    defp policy(overrides) do
      %{
        now: @now,
        limit: Keyword.get(overrides, :limit, 1),
        orphan_ttl_ms: 60_000,
        max_claim_attempts: 5,
        preference: Keyword.get(overrides, :preference)
      }
    end

    defp assert_claim_shape(context, expected, run_id) do
      case {expected, claim(context)} do
        {:lease, {:ok, %{leases: [%{run_id: ^run_id}], poisoned: []}}} -> :ok
        {:poison, {:ok, %{leases: [], poisoned: [%{run_id: ^run_id}]}}} -> :ok
        {:empty, {:ok, %{leases: [], poisoned: []}}} -> :ok
        {expected, actual} -> flunk("expected #{expected} claim shape, got: #{inspect(actual)}")
      end
    end

    defp partition_query(scope_key, column) do
      TestRepo.query!(
        "SELECT #{column} FROM docket_claim_partitions WHERE scope_key = $1",
        [scope_key]
      ).rows
    end

    defp release_all!(context) do
      for [run_id, token] <-
            TestRepo.query!(
              "SELECT run_id, claim_token FROM docket_runs WHERE claim_token IS NOT NULL"
            ).rows do
        :ok = RunStore.release_claim(context, :system, run_id, Ecto.UUID.load!(token), @now)
      end
    end

    defp hold_row!(table, predicate, params) do
      parent = self()

      task =
        Task.async(fn ->
          TestRepo.transaction(fn ->
            TestRepo.query!("SELECT 1 FROM #{table} WHERE #{predicate} FOR UPDATE", params)
            send(parent, {:tenant_fair_row_held, self()})

            receive do
              :release_tenant_fair_row -> :ok
            end
          end)
        end)

      assert_receive {:tenant_fair_row_held, holder_pid}
      {task, holder_pid}
    end

    defp release_holder!({task, holder_pid}) do
      send(holder_pid, :release_tenant_fair_row)
      assert {:ok, :ok} = Task.await(task)
    end

    defp hold_claim_transaction!(context, disposition) do
      parent = self()

      task =
        Task.async(fn ->
          Docket.Postgres.transaction(context, fn tx ->
            assert {:ok, %{leases: [_lease], poisoned: []}} = claim(tx)
            send(parent, {:tenant_fair_claim_held, self()})

            receive do
              :release_tenant_fair_claim -> :ok
            end

            case disposition do
              :rollback -> {:error, :forced_rollback}
              :commit -> {:ok, :forced_commit}
            end
          end)
        end)

      assert_receive {:tenant_fair_claim_held, holder_pid}
      {task, holder_pid}
    end

    defp release_claim_transaction!({task, holder_pid}, disposition) do
      send(holder_pid, :release_tenant_fair_claim)

      case disposition do
        :rollback -> assert {:error, :forced_rollback} = Task.await(task)
        :commit -> assert {:ok, :forced_commit} = Task.await(task)
      end
    end

    defp hold_claim!(context, overrides) do
      parent = self()

      task =
        Task.async(fn ->
          Docket.Postgres.transaction(context, fn tx ->
            result = claim(tx, overrides)
            send(parent, {:tenant_fair_claim_result_held, self(), result})

            receive do
              :release_tenant_fair_claim_result -> :ok
            end

            {:ok, result}
          end)
        end)

      assert_receive {:tenant_fair_claim_result_held, holder_pid, result}
      {task, holder_pid, result}
    end

    defp release_claim!(task, holder_pid, result) do
      send(holder_pid, :release_tenant_fair_claim_result)
      assert {:ok, ^result} = Task.await(task)
    end

    defp invoke_function(context, keys, opts) do
      policy = policy(limit: Keyword.fetch!(opts, :demand))
      claim_policy = Docket.Postgres.ClaimPolicy.resolve(context)
      plan = Docket.Postgres.ClaimPolicy.build_plan(claim_policy, context, policy)

      function = ~s("public"."#{Function.name()}")

      statement = """
      SELECT claimed.*
      FROM #{function}($1, $2, $3, $4, $5, $6)
      AS claimed(#{Function.result_definition()})
      """

      rows =
        TestRepo.query!(statement, [
          policy.now,
          DateTime.add(policy.now, -div(policy.orphan_ttl_ms, 1_000), :second),
          policy.limit,
          policy.max_claim_attempts,
          policy.preference,
          keys
        ]).rows

      case Docket.Postgres.ClaimPolicy.decode(claim_policy, %{plan | decoder: plan.decoder}, rows) do
        {:ok, batch, _observation} -> {:ok, batch}
        {:error, reason, _observation} -> {:error, reason}
      end
    end
  end
end
