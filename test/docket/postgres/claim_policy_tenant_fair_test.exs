if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyTenantFairTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.ClaimPolicy.Admin
    alias Docket.Postgres.RunStore
    alias Docket.Postgres.TestRepo

    @migration_version 20_260_716_000_168
    @prefixed_migration_version 20_260_716_000_169
    @now ~U[2026-07-16 12:00:00.000000Z]

    defmodule SecondRepo do
      use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
    end

    defmodule InstallDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule InstallDocketPrefixed do
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

      Application.put_env(:docket, SecondRepo, Keyword.put(config, :pool_size, 4))
      start_supervised!(SecondRepo)

      context = tenant_fair_context(TestRepo, 4)
      second_context = tenant_fair_context(SecondRepo, 4)
      %{context: context, second_context: second_context}
    end

    test "two independent pools cannot overfill the final slot", %{
      context: context,
      second_context: second_context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "cap-race-a", @now)
      insert_ready("tenant", "cap-race-b", @now)

      parent = self()
      gate = make_ref()

      tasks =
        for caller_context <- [context, second_context] do
          Task.async(fn ->
            send(parent, {gate, :ready})
            receive do: ({^gate, :go} -> :ok)
            RunStore.claim_due(caller_context, :system, policy(1))
          end)
        end

      for _ <- tasks, do: assert_receive({^gate, :ready}, 2_000)
      Enum.each(tasks, &send(&1.pid, {gate, :go}))
      results = Enum.map(tasks, &Task.await(&1, 5_000))

      leases =
        Enum.flat_map(results, fn
          {:ok, %{leases: leases}} -> leases
          {:error, {:claim_policy_unavailable, :lock_contention}} -> []
        end)

      assert length(leases) == 1
      assert live_count("tenant") == 1
    end

    test "cap reduction creates debt and release restores capacity only below the cap", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 3, expected_version: 0)

      for id <- ~w(active-a active-b active-c), do: insert_ready("tenant", id, @now)
      # Demand four reserves the unmatched expired class and intentionally
      # underfills by one while still admitting all three ready rows.
      assert {:ok, %{leases: leases}} = RunStore.claim_due(context, :system, policy(4))
      assert length(leases) == 3

      assert {:ok, %{version: 2}} = Admin.put_default(context, 1, expected_version: 1)
      insert_ready("tenant", "waiting", @now)
      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(1))

      [first, second, third] = leases
      complete_claim(first)
      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(1))
      complete_claim(second)
      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(1))
      complete_claim(third)

      assert {:ok, %{leases: [%{run_id: "waiting"}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "cap increase promotes the oldest queued run without replacing the admitted cohort", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "first", DateTime.add(@now, -3, :second))
      insert_ready("tenant", "second", DateTime.add(@now, -2, :second))
      insert_ready("tenant", "third", DateTime.add(@now, -1, :second))

      assert {:ok, %{leases: [%{run_id: "first"}]}} =
               RunStore.claim_due(context, :system, policy(1))

      assert {:ok, %{version: 2}} = Admin.put_default(context, 2, expected_version: 1)

      assert {:ok, %{leases: [%{run_id: "second"}]}} =
               RunStore.claim_due(context, :system, policy(1))

      assert [["first"], ["second"]] = admitted_run_ids("tenant")
    end

    test "demand and candidate pages larger than cap preserve the cap-ten oldest identities", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 10, expected_version: 0)

      for index <- 1..100 do
        insert_ready(
          "tenant",
          "run-#{String.pad_leading(to_string(index), 3, "0")}",
          DateTime.add(@now, index - 101, :microsecond)
        )
      end

      assert {:ok, %{leases: leases}} = RunStore.claim_due(context, :system, policy(50))
      assert length(leases) == 10

      expected = for index <- 1..10, do: ["run-#{String.pad_leading(to_string(index), 3, "0")}"]
      assert admitted_run_ids("tenant") == expected

      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(50))
      assert admitted_run_ids("tenant") == expected
    end

    test "demand-one discovery rotates from a deep tenant to another tenant", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 4, expected_version: 0)
      insert_ready("a", "a-1", DateTime.add(@now, -3, :second))
      insert_ready("a", "a-2", DateTime.add(@now, -2, :second))
      insert_ready("a", "a-3", DateTime.add(@now, -1, :second))
      insert_ready("b", "b-1", @now)

      assert {:ok, %{leases: [%{owner_scope: {:tenant, "a"}}]}} =
               RunStore.claim_due(context, :system, policy(1))

      assert {:ok, %{leases: [%{owner_scope: {:tenant, "b"}}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "an admitted run remains sticky across an immediate cooperative release", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "first", DateTime.add(@now, -2, :second))
      insert_ready("tenant", "second", DateTime.add(@now, -1, :second))

      assert {:ok, %{leases: [%{run_id: "first"} = first]}} =
               RunStore.claim_due(context, :system, policy(1))

      TestRepo.query!(
        """
        UPDATE docket_runs
        SET claim_token = NULL, claimed_at = NULL, wake_at = $3
        WHERE run_id = $1 AND claim_token = $2
        """,
        [first.run_id, Ecto.UUID.dump!(first.claim_token), @now]
      )

      assert {:ok, %{leases: [%{run_id: "first"} = first_again]}} =
               RunStore.claim_due(context, :system, policy(1))

      TestRepo.query!(
        """
        UPDATE docket_runs
        SET claim_token = NULL, claimed_at = NULL, tenant_admitted_at = NULL,
            wake_at = $3
        WHERE run_id = $1 AND claim_token = $2
        """,
        [
          first_again.run_id,
          Ecto.UUID.dump!(first_again.claim_token),
          DateTime.add(@now, 1, :hour)
        ]
      )

      assert {:ok, %{leases: [%{run_id: "second"}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "two pollers keep a cap-two cohort sticky ahead of a deep backlog", %{
      context: context,
      second_context: second_context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 2, expected_version: 0)

      for index <- 1..100 do
        insert_ready("tenant", "queued-#{String.pad_leading(to_string(index), 3, "0")}", @now)
      end

      assert {:ok, %{leases: [first]}} = RunStore.claim_due(context, :system, policy(1))
      assert {:ok, %{leases: [second]}} = RunStore.claim_due(second_context, :system, policy(1))
      assert Enum.sort([first.run_id, second.run_id]) == ["queued-001", "queued-002"]

      leases =
        Enum.reduce(1..3, [first, second], fn _cycle, current ->
          Enum.each(current, fn lease ->
            TestRepo.query!(
              """
              UPDATE docket_runs
              SET claim_token = NULL, claimed_at = NULL, wake_at = $3
              WHERE run_id = $1 AND claim_token = $2
              """,
              [lease.run_id, Ecto.UUID.dump!(lease.claim_token), @now]
            )
          end)

          assert {:ok, %{leases: [left]}} =
                   RunStore.claim_due(second_context, :system, policy(1))

          assert {:ok, %{leases: [right]}} =
                   RunStore.claim_due(context, :system, policy(1))

          assert Enum.sort([left.run_id, right.run_id]) == ["queued-001", "queued-002"]
          [left, right]
        end)

      first = Enum.find(leases, &(&1.run_id == "queued-001"))

      TestRepo.query!(
        """
        UPDATE docket_runs
        SET claim_token = NULL, claimed_at = NULL, tenant_admitted_at = NULL,
            wake_at = $3
        WHERE run_id = $1 AND claim_token = $2
        """,
        [first.run_id, Ecto.UUID.dump!(first.claim_token), DateTime.add(@now, 1, :hour)]
      )

      assert {:ok, %{leases: [%{run_id: "queued-003"}]}} =
               RunStore.claim_due(second_context, :system, policy(1))

      assert [["queued-002"], ["queued-003"]] =
               TestRepo.query!("""
               SELECT run_id FROM docket_runs
               WHERE scope_key = 'tenant' AND tenant_admitted_at IS NOT NULL
               ORDER BY run_id
               """).rows
    end

    test "capped heads rotate so a later eligible tenant makes progress", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)

      insert_claimed("a", "a-live", @now)
      insert_ready("a", "a-waiting", DateTime.add(@now, -3, :second))
      insert_claimed("b", "b-live", @now)
      insert_ready("b", "b-waiting", DateTime.add(@now, -2, :second))
      insert_ready("c", "c-ready", DateTime.add(@now, -1, :second))

      assert {:ok, %{leases: [%{run_id: "c-ready"}]}} =
               RunStore.claim_due(context, :system, policy(1))

      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(1))
    end

    test "an expired steal is count-neutral and does not admit queued work", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_claimed("tenant", "expired", DateTime.add(@now, -10, :second))
      insert_ready("tenant", "queued", DateTime.add(@now, -5, :second))

      assert {:ok, %{leases: [%{run_id: "expired", claim_attempt: 2}]}} =
               RunStore.claim_due(context, :system, policy(2))

      assert live_count("tenant") == 1

      assert [[nil]] =
               TestRepo.query!("SELECT claim_token FROM docket_runs WHERE run_id = 'queued'").rows
    end

    test "admitted expired work outranks a ready-preferred queued promotion", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 2, expected_version: 0)
      insert_claimed("tenant", "expired", DateTime.add(@now, -10, :second))
      insert_ready("tenant", "queued", DateTime.add(@now, -5, :second))

      preferred = %{policy(1) | preference: :ready}

      assert {:ok, %{leases: [%{run_id: "expired", claim_attempt: 2}]}} =
               RunStore.claim_due(context, :system, preferred)

      assert [[nil]] =
               TestRepo.query!("SELECT claim_token FROM docket_runs WHERE run_id = 'queued'").rows
    end

    test "class reservation keeps a queued-ready head inside K behind deep admitted expired work",
         %{
           context: context
         } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 100, expected_version: 0)

      for index <- 1..40 do
        insert_claimed(
          "tenant",
          "expired-#{String.pad_leading(to_string(index), 2, "0")}",
          DateTime.add(@now, -10, :second)
        )
      end

      insert_ready("tenant", "queued", DateTime.add(@now, -5, :second))

      assert {:ok, %{leases: [expired, %{run_id: "queued"}], poisoned: []}} =
               RunStore.claim_due(context, :system, policy(2))

      assert String.starts_with?(expired.run_id, "expired-")
      assert length(admitted_run_ids("tenant")) == 41
    end

    test "poison makes progress without consuming the tenant cap", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_run("tenant", "poison", nil, nil, DateTime.add(@now, -2, :second), 5)
      insert_ready("tenant", "ordinary", DateTime.add(@now, -1, :second))

      assert {:ok,
              %{
                poisoned: [
                  %{
                    run_id: "poison",
                    poisoned_at: @now,
                    poison_reason: "max_claim_attempts_exceeded"
                  }
                ],
                leases: []
              }} = RunStore.claim_due(context, :system, policy(2))

      assert {:ok, %{leases: [%{run_id: "ordinary"}], poisoned: []}} =
               RunStore.claim_due(context, :system, policy(1))

      assert live_count("tenant") == 1
    end

    test "admitted poison releases a slot consumed by the queued head in the same visit", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "admitted-poison", DateTime.add(@now, -2, :second))
      insert_ready("tenant", "queued", DateTime.add(@now, -1, :second))

      TestRepo.query!(
        "UPDATE docket_runs SET tenant_admitted_at = $2, claim_attempts = 5 " <>
          "WHERE run_id = $1",
        ["admitted-poison", @now]
      )

      assert {:ok,
              %{
                poisoned: [%{run_id: "admitted-poison"}],
                leases: [%{run_id: "queued"}]
              }} = RunStore.claim_due(context, :system, policy(3))

      assert [["queued"]] = admitted_run_ids("tenant")

      assert [[1]] =
               TestRepo.query!(
                 "SELECT admission_epoch FROM docket_claim_partitions " <>
                   "WHERE scope_key = 'tenant'"
               ).rows

      assert [[nil, true]] =
               TestRepo.query!(
                 "SELECT tenant_admitted_at, poisoned_at IS NOT NULL " <>
                   "FROM docket_runs WHERE run_id = 'admitted-poison'"
               ).rows
    end

    test "exact run locks skip a locked candidate and continue around the ring", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("a", "locked", DateTime.add(@now, -1, :second))
      insert_ready("b", "available", @now)

      parent = self()
      gate = make_ref()

      blocker =
        Task.async(fn ->
          SecondRepo.transaction(fn ->
            SecondRepo.query!("SELECT id FROM docket_runs WHERE run_id = 'locked' FOR UPDATE")
            send(parent, {gate, :locked})
            receive do: ({^gate, :release} -> :ok)
          end)
        end)

      assert_receive {^gate, :locked}, 2_000

      assert {:ok, %{leases: [%{run_id: "available"}], poisoned: []}} =
               RunStore.claim_due(context, :system, policy(1))

      send(blocker.pid, {gate, :release})
      assert {:ok, :ok} = Task.await(blocker, 2_000)

      assert [[nil, nil, 0]] =
               TestRepo.query!(
                 "SELECT claim_token, claimed_at, claim_attempts " <>
                   "FROM docket_runs WHERE run_id = 'locked'"
               ).rows
    end

    test "TenantFair fails closed in read-only and non-read-committed transactions", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "transaction-mode", @now)

      assert {:error, {:claim_policy_unavailable, :read_only_transaction}} =
               Docket.Postgres.transaction(context, fn tx ->
                 TestRepo.query!("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY")
                 RunStore.claim_due(tx, :system, policy(1))
               end)

      assert {:error, {:claim_policy_unavailable, :unsupported_isolation}} =
               Docket.Postgres.transaction(context, fn tx ->
                 TestRepo.query!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE")
                 RunStore.claim_due(tx, :system, policy(1))
               end)

      assert [[nil, nil, 0]] =
               TestRepo.query!(
                 "SELECT claim_token, claimed_at, claim_attempts FROM docket_runs " <>
                   "WHERE run_id = 'transaction-mode'"
               ).rows
    end

    test "TenantFair fails closed when the policy row is contended", %{
      context: context
    } do
      handler_id = {__MODULE__, self(), make_ref()}
      parent = self()

      :telemetry.attach(
        handler_id,
        [:docket, :postgres, :claim_policy, :admission],
        &Docket.Test.TelemetryRelay.raw/4,
        parent
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "policy-lock", @now)
      policy_before = policy_row()

      gate = make_ref()

      blocker =
        Task.async(fn ->
          SecondRepo.transaction(fn ->
            SecondRepo.query!("SELECT id FROM docket_claim_policy WHERE id = 1 FOR UPDATE")
            send(parent, {gate, :locked})
            receive do: ({^gate, :release} -> :ok)
          end)
        end)

      assert_receive {^gate, :locked}, 2_000

      result = RunStore.claim_due(context, :system, policy(1))
      send(blocker.pid, {gate, :release})
      assert {:ok, :ok} = Task.await(blocker, 2_000)

      assert {:error, {:claim_policy_unavailable, :lock_contention}} = result
      assert policy_row() == policy_before
      assert live_count("tenant") == 0

      assert [[0]] =
               TestRepo.query!(
                 "SELECT admission_epoch FROM docket_claim_schedule " <>
                   "JOIN docket_claim_partitions USING (scope_key) " <>
                   "WHERE scope_key = 'tenant'"
               ).rows

      assert [[nil, nil, 0]] =
               TestRepo.query!(
                 "SELECT claim_token, claimed_at, claim_attempts FROM docket_runs " <>
                   "WHERE run_id = 'policy-lock'"
               ).rows

      assert_receive {[:docket, :postgres, :claim_policy, :admission], %{contentions: 1},
                      %{contention_phase: :policy_cursor, result: :error}}
    end

    test "schedule membership reads do not create a second claim lock path", %{
      context: context
    } do
      handler_id = {__MODULE__, self(), make_ref()}
      parent = self()

      :telemetry.attach(
        handler_id,
        [:docket, :postgres, :claim_policy, :admission],
        &Docket.Test.TelemetryRelay.raw/4,
        parent
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "schedule-lock", @now)
      policy_before = policy_row()

      gate = make_ref()

      blocker =
        Task.async(fn ->
          SecondRepo.transaction(fn ->
            SecondRepo.query!(
              "SELECT scope_key FROM docket_claim_schedule " <>
                "WHERE scope_key = 'tenant' FOR UPDATE"
            )

            send(parent, {gate, :locked})
            receive do: ({^gate, :release} -> :ok)
          end)
        end)

      assert_receive {^gate, :locked}, 2_000

      result = RunStore.claim_due(context, :system, policy(1))
      send(blocker.pid, {gate, :release})
      assert {:ok, :ok} = Task.await(blocker, 2_000)

      assert {:ok, %{leases: [%{run_id: "schedule-lock"}], poisoned: []}} = result
      assert policy_row() != policy_before

      assert [[1]] =
               TestRepo.query!(
                 "SELECT admission_epoch FROM docket_claim_schedule " <>
                   "JOIN docket_claim_partitions USING (scope_key) " <>
                   "WHERE scope_key = 'tenant'"
               ).rows

      assert [[token, @now, 1]] =
               TestRepo.query!(
                 "SELECT claim_token, claimed_at, claim_attempts FROM docket_runs " <>
                   "WHERE run_id = 'schedule-lock'"
               ).rows

      refute is_nil(token)

      assert_receive {[:docket, :postgres, :claim_policy, :admission], %{contentions: 0},
                      %{contention_phase: :none, result: :ok}}
    end

    test "TenantFair admits tenantless work", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("", "tenantless", @now)

      assert {:ok, %{leases: [%{run_id: "tenantless", owner_scope: :tenantless}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "TenantFair admits work from a custom prefix" do
      TestRepo.query!("CREATE SCHEMA docket_private")

      :ok =
        Ecto.Migrator.up(
          TestRepo,
          @prefixed_migration_version,
          InstallDocketPrefixed,
          log: false
        )

      context = tenant_fair_context(TestRepo, 1, prefix: "docket_private")
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "prefixed", @now, prefix: "docket_private")

      assert {:ok, %{leases: [%{run_id: "prefixed", owner_scope: {:tenant, "tenant"}}]}} =
               RunStore.claim_due(context, :system, policy(1))
    end

    test "transaction-scoped TenantFair admission commits and rolls back with its caller", %{
      context: context
    } do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "transactional", @now)

      assert {:error, :test_rollback} =
               Docket.Postgres.transaction(context, fn tx ->
                 assert tx.claim_policy === context.claim_policy

                 assert {:ok, %{leases: [%{run_id: "transactional"}]}} =
                          RunStore.claim_due(tx, :system, policy(1))

                 {:error, :test_rollback}
               end)

      assert live_count("tenant") == 0

      assert {:ok, %{leases: [%{run_id: "transactional"}]}} =
               Docket.Postgres.transaction(context, fn tx ->
                 RunStore.claim_due(tx, :system, policy(1))
               end)

      assert live_count("tenant") == 1
    end

    test "switching to TenantFair makes a Legacy dispatcher fail closed", %{
      context: context,
      second_context: second_context
    } do
      legacy_context = Docket.Postgres.context(repo: SecondRepo)
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "tenant-fair", @now)
      insert_ready("other", "legacy-blocked", @now)

      assert {:ok, %{leases: [_lease]}} = RunStore.claim_due(context, :system, policy(1))

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(legacy_context, :system, policy(1))

      assert second_context.claim_policy.implementation ==
               Docket.Postgres.ClaimPolicy.TenantFair
    end

    defp tenant_fair_context(repo, default_max_active_runs, opts \\ []) do
      opts =
        opts
        |> Keyword.put(:repo, repo)
        |> Keyword.put(:claim_policy,
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          default_max_active_runs: default_max_active_runs
        )

      Docket.Postgres.context(opts)
    end

    defp policy(limit) do
      %{
        now: @now,
        limit: limit,
        orphan_ttl_ms: 1_000,
        max_claim_attempts: 5,
        preference: nil
      }
    end

    defp complete_claim(lease) do
      TestRepo.query!(
        """
        UPDATE docket_runs
        SET status = 'done', claim_token = NULL, claimed_at = NULL,
            tenant_admitted_at = NULL,
            finished_at = $3, updated_at = $3
        WHERE run_id = $1 AND claim_token = $2
        """,
        [lease.run_id, Ecto.UUID.dump!(lease.claim_token), @now]
      )
    end

    defp insert_ready(scope_key, run_id, wake_at, opts \\ []) do
      insert_run(scope_key, run_id, nil, nil, wake_at, 0, opts)
    end

    defp insert_claimed(scope_key, run_id, claimed_at) do
      insert_run(
        scope_key,
        run_id,
        Ecto.UUID.generate(),
        claimed_at,
        nil,
        1,
        []
      )
    end

    defp insert_run(scope_key, run_id, claim_token, claimed_at, wake_at, attempts, opts \\ []) do
      repo = Keyword.get(opts, :repo, TestRepo)
      prefix = Keyword.get(opts, :prefix)
      graphs = qualified_table(prefix, "docket_graph_versions")
      partitions = qualified_table(prefix, "docket_claim_partitions")
      runs = qualified_table(prefix, "docket_runs")
      tenant_id = if scope_key == "", do: nil, else: scope_key
      graph_hash = "hash-#{scope_key}"

      repo.query!(
        """
        INSERT INTO #{graphs}
          (tenant_id, graph_id, graph_hash, graph, inserted_at)
        VALUES ($1, 'graph', $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT (scope_key, graph_id, graph_hash) DO NOTHING
        """,
        [tenant_id, graph_hash, <<1>>]
      )

      repo.query!(
        "INSERT INTO #{partitions} (scope_key) VALUES ($1) ON CONFLICT DO NOTHING",
        [scope_key]
      )

      repo.query!(
        """
        INSERT INTO #{runs}
          (run_id, tenant_id, graph_id, graph_hash, status, state,
           checkpoint_seq, claim_token, claimed_at, tenant_admitted_at,
           wake_at, claim_attempts,
           inserted_at, started_at, updated_at)
        VALUES ($1, $2, 'graph', $3, 'running', $4,
                7, $5, $6, $6, $7, $8,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [
          run_id,
          tenant_id,
          graph_hash,
          <<1>>,
          claim_token && Ecto.UUID.dump!(claim_token),
          claimed_at,
          wake_at,
          attempts
        ]
      )
    end

    defp qualified_table(nil, table), do: ~s("#{table}")
    defp qualified_table(prefix, table), do: ~s("#{prefix}"."#{table}")

    defp live_count(scope_key) do
      TestRepo.query!(
        """
        SELECT count(*)::integer
        FROM docket_runs
        WHERE scope_key = $1 AND status = 'running' AND poisoned_at IS NULL
          AND claim_token IS NOT NULL
        """,
        [scope_key]
      ).rows
      |> hd()
      |> hd()
    end

    defp admitted_run_ids(scope_key) do
      TestRepo.query!(
        "SELECT run_id FROM docket_runs WHERE scope_key = $1 " <>
          "AND tenant_admitted_at IS NOT NULL ORDER BY wake_at, id",
        [scope_key]
      ).rows
    end

    defp policy_row do
      TestRepo.query!(
        "SELECT admission_mode, max_active, policy_version, initialized_at, updated_at, " <>
          "scan_ring_position FROM docket_claim_policy WHERE id = 1"
      ).rows
    end
  end
end
