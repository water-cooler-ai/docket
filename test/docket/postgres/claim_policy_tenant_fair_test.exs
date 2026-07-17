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
      assert {:ok, %{leases: leases}} = RunStore.claim_due(context, :system, policy(3))
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

    test "capped heads rotate so a later eligible tenant makes progress", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)

      insert_claimed("a", "a-live", @now)
      insert_ready("a", "a-waiting", DateTime.add(@now, -3, :second))
      insert_claimed("b", "b-live", @now)
      insert_ready("b", "b-waiting", DateTime.add(@now, -2, :second))
      insert_ready("c", "c-ready", DateTime.add(@now, -1, :second))

      assert {:ok, %{leases: []}} = RunStore.claim_due(context, :system, policy(1))

      assert {:ok, %{leases: [%{run_id: "c-ready"}]}} =
               RunStore.claim_due(context, :system, policy(1))
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
                leases: [%{run_id: "ordinary"}]
              }} = RunStore.claim_due(context, :system, policy(2))

      assert live_count("tenant") == 1
    end

    test "stale candidate mutations are rechecked before admission", %{context: context} do
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)

      cases = [
        {"wake", 0, "claim_token = NULL, claimed_at = NULL, wake_at = $2, claim_attempts = 0",
         [DateTime.add(@now, 1, :second)],
         fn [token, _claimed_at, wake_at, attempts, poisoned_at] ->
           assert token == nil
           assert wake_at == DateTime.add(@now, 1, :second)
           assert attempts == 0
           assert poisoned_at == nil
         end},
        {"claim-token", 0,
         "claim_token = $2, claimed_at = $3, wake_at = NULL, claim_attempts = 1",
         [Ecto.UUID.dump!(Ecto.UUID.generate()), @now],
         fn [token, claimed_at, wake_at, attempts, poisoned_at] ->
           assert is_binary(token)
           assert claimed_at == @now
           assert wake_at == nil
           assert attempts == 1
           assert poisoned_at == nil
         end},
        {"attempt-class", 5,
         "claim_token = NULL, claimed_at = NULL, wake_at = $2, claim_attempts = 4",
         [DateTime.add(@now, -1, :second)],
         fn [token, _claimed_at, wake_at, attempts, poisoned_at] ->
           assert token == nil
           assert wake_at == DateTime.add(@now, -1, :second)
           assert attempts == 4
           assert poisoned_at == nil
         end},
        {"cutoff", 1,
         "claim_token = claim_token, claimed_at = $2, wake_at = NULL, claim_attempts = 1", [@now],
         fn [token, claimed_at, wake_at, attempts, poisoned_at] ->
           assert is_binary(token)
           assert claimed_at == @now
           assert wake_at == nil
           assert attempts == 1
           assert poisoned_at == nil
         end}
      ]

      Enum.each(cases, fn {name, attempts, mutation, params, assertion} ->
        scope = "stale-#{name}"
        run_id = "stale-#{name}"

        if name == "cutoff" do
          insert_claimed(scope, run_id, DateTime.add(@now, -10, :second))
        else
          insert_run(
            scope,
            run_id,
            nil,
            nil,
            DateTime.add(@now, -10, :second),
            attempts
          )
        end

        assert_stale_candidate_rechecked(context, run_id, mutation, params)

        assert [[token, claimed_at, wake_at, persisted_attempts, poisoned_at]] =
                 TestRepo.query!(
                   "SELECT claim_token, claimed_at, wake_at, claim_attempts, poisoned_at " <>
                     "FROM docket_runs WHERE run_id = $1",
                   [run_id]
                 ).rows

        assertion.([token, claimed_at, wake_at, persisted_attempts, poisoned_at])
        finish_run(run_id)
      end)
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
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "policy-lock", @now)

      parent = self()
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
      assert live_count("tenant") == 0
    end

    test "concurrent Legacy and TenantFair admission serializes across the engine switch", %{
      context: context,
      second_context: second_context
    } do
      legacy_context = Docket.Postgres.context(repo: SecondRepo)
      assert {:ok, %{version: 1}} = Admin.put_default(context, 1, expected_version: 0)
      insert_ready("tenant", "engine-a", DateTime.add(@now, -1, :second))
      insert_ready("tenant", "engine-b", @now)

      parent = self()
      gate = make_ref()

      tasks =
        for caller_context <- [legacy_context, second_context] do
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
          {:ok, %{leases: leases}} ->
            leases

          {:error, {:claim_policy_unavailable, reason}}
          when reason in [:inactive_engine, :lock_contention] ->
            []
        end)

      assert length(leases) == 1
      assert live_count("tenant") == 1

      if Enum.at(results, 1) == {:error, {:claim_policy_unavailable, :lock_contention}} do
        assert {:ok, %{leases: []}} =
                 RunStore.claim_due(second_context, :system, policy(1))
      end

      assert {:error, {:claim_policy_unavailable, :inactive_engine}} =
               RunStore.claim_due(legacy_context, :system, policy(1))
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

    defp tenant_fair_context(repo, default_max_active, opts \\ []) do
      opts =
        opts
        |> Keyword.put(:repo, repo)
        |> Keyword.put(:claim_policy,
          implementation: Docket.Postgres.ClaimPolicy.TenantFair,
          default_max_active: default_max_active
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
           checkpoint_seq, claim_token, claimed_at, wake_at, claim_attempts,
           inserted_at, started_at, updated_at)
        VALUES ($1, $2, 'graph', $3, 'running', $4,
                7, $5, $6, $7, $8,
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

    defp assert_stale_candidate_rechecked(context, run_id, mutation, mutation_params) do
      parent = self()
      gate = make_ref()

      blocker =
        Task.async(fn ->
          SecondRepo.transaction(fn ->
            SecondRepo.query!("SELECT id FROM docket_runs WHERE run_id = $1 FOR UPDATE", [run_id])
            send(parent, {gate, :locked})

            receive do
              {^gate, :mutate} ->
                SecondRepo.query!(
                  "UPDATE docket_runs SET #{mutation} WHERE run_id = $1",
                  [run_id | mutation_params]
                )
            end
          end)
        end)

      assert_receive {^gate, :locked}, 2_000

      claimant =
        Task.async(fn ->
          Docket.Postgres.transaction(context, fn tx ->
            [[backend_pid]] = TestRepo.query!("SELECT pg_backend_pid()").rows
            send(parent, {gate, :claimant, backend_pid})
            RunStore.claim_due(tx, :system, policy(1))
          end)
        end)

      assert_receive {^gate, :claimant, backend_pid}, 2_000
      assert :ok = await_backend_lock(backend_pid)
      send(blocker.pid, {gate, :mutate})

      assert {:ok, _mutation} = Task.await(blocker, 2_000)
      assert {:ok, %{leases: [], poisoned: []}} = Task.await(claimant, 2_000)
    end

    defp await_backend_lock(backend_pid, attempts \\ 100)

    defp await_backend_lock(_backend_pid, 0), do: {:error, :claimant_did_not_wait_on_lock}

    defp await_backend_lock(backend_pid, attempts) do
      case TestRepo.query!(
             "SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1",
             [backend_pid]
           ).rows do
        [["Lock"]] ->
          :ok

        _rows ->
          Process.sleep(1)
          await_backend_lock(backend_pid, attempts - 1)
      end
    end

    defp finish_run(run_id) do
      TestRepo.query!(
        """
        UPDATE docket_runs
        SET status = 'done', claim_token = NULL, claimed_at = NULL, wake_at = NULL,
            poisoned_at = NULL, poison_reason = NULL, finished_at = $2, updated_at = $2
        WHERE run_id = $1
        """,
        [run_id, @now]
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
  end
end
