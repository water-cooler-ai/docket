if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.TenantFairRunStoreSetup do
    @moduledoc false

    alias Docket.Postgres.ClaimPolicy.{Activation, Admin, Backfill, OnlineDDL, Readiness}
    alias Docket.Postgres.ClaimPolicy.TenantFair.Function
    alias Docket.Postgres.OnlineMigration

    @default_policy %{preferred_active: 1, max_active: 2, weight: 1, borrowing: false}

    def prepare!(repo, implementation_opts) do
      context =
        Docket.Postgres.context(
          repo: repo,
          claim_policy:
            [implementation: Docket.Postgres.ClaimPolicy.TenantFair] ++ implementation_opts
        )

      {:ok, _} =
        Readiness.attest_dual_write(context,
          evidence_fingerprint: :crypto.hash(:sha256, "run-store-matrix-dual-write"),
          source: "run-store-matrix",
          event_id: "dual-write",
          actor: "test"
        )

      advance_until_complete!(context)

      {:ok, %{version: 1}} =
        Admin.bootstrap_default(context, @default_policy,
          source: "run-store-matrix",
          event_id: "bootstrap",
          actor: "test",
          expected_version: 0
        )

      :ok = OnlineMigration.up(repo: repo)
      fingerprints = OnlineDDL.index_fingerprints("public")

      {:ok, %{version: 1}} =
        Readiness.verify(context,
          expected_readiness_epoch: 0,
          ready_index_ddl_sha256: fingerprints.ready,
          live_index_ddl_sha256: fingerprints.live,
          source: "run-store-matrix",
          event_id: "verify",
          actor: "test"
        )

      {:ok, _} =
        Activation.register_capability(context, "00000000-0000-4000-8000-000000000069",
          binary_fingerprint: :crypto.hash(:sha256, "run-store-matrix-binary"),
          writer_contract: 1,
          gate_contract: 1,
          function_contract: Function.version(),
          ttl_ms: :timer.minutes(5)
        )

      {:ok, assertion} =
        Activation.attest_old_binaries_absent(context,
          source: "run-store-matrix",
          event_id: "old-binaries",
          actor: "test",
          evidence_fingerprint: :crypto.hash(:sha256, "run-store-matrix-old-binaries"),
          expires_at: DateTime.add(DateTime.utc_now(), 300, :second)
        )

      {:ok, %{outcome: :applied, version: 1}} =
        Activation.activate(context,
          source: "run-store-matrix",
          event_id: "activate",
          actor: "test",
          expected_epoch: 0,
          old_binary_assertion_id: assertion.assertion_id
        )

      repo.query!(
        "INSERT INTO docket_claim_partitions (scope_key) VALUES ('') ON CONFLICT DO NOTHING"
      )

      :ok
    end

    defp advance_until_complete!(context) do
      case Backfill.advance(context, batch_size: 10_000) do
        {:ok, %{phase: :complete}} -> :ok
        {:ok, _} -> advance_until_complete!(context)
      end
    end
  end

  defmodule Docket.Test.ClaimPolicyRunStoreTests do
    @moduledoc false

    defmacro run_store_matrix(opts) do
      repo = opts |> Keyword.fetch!(:repo) |> Macro.expand(__CALLER__)
      query_event = Keyword.fetch!(opts, :query_event)

      tests =
        Docket.Test.ClaimPolicyMatrix.implementations()
        |> Enum.filter(&Map.get(&1, :run_store?, true))
        |> Enum.flat_map(&contract_tests(&1, repo, query_event))

      quote do
        (unquote_splicing(tests))
      end
    end

    defp contract_tests(spec, repo, query_event) do
      name = spec.name
      implementation = spec.implementation
      implementation_opts = spec.options
      query_marker = spec.query_marker
      run_store_setup = Map.get(spec, :run_store_setup)

      setup_call =
        if run_store_setup do
          quote do
            :ok = unquote(run_store_setup).prepare!(repo, implementation_opts)
          end
        else
          quote do
            :ok
          end
        end

      [
        quote do
          test unquote("#{name} RunStore executes one selected plan query and returns its batch") do
            implementation = unquote(implementation)
            implementation_opts = unquote(Macro.escape(implementation_opts))
            repo = unquote(repo)
            query_event = unquote(query_event)
            query_marker = unquote(query_marker)

            run_id =
              "claim-policy-contract-#{System.unique_integer([:positive, :monotonic])}"

            root = %{repo: repo, prefix: nil}

            claim_policy =
              Docket.Postgres.ClaimPolicy.new(
                [implementation: implementation] ++ implementation_opts,
                root
              )

            context = Map.put(root, :claim_policy, claim_policy)
            unquote(setup_call)

            inserted = insert_run!(run_id)
            handler = "claim-policy-run-store-#{System.unique_integer([:positive])}"

            :telemetry.attach(
              handler,
              query_event,
              &Docket.Test.TelemetryRelay.raw/4,
              self()
            )

            on_exit(fn -> :telemetry.detach(handler) end)

            assert {:ok, %{leases: [lease], poisoned: []}} =
                     Docket.Postgres.RunStore.claim_due(context, :system, policy(@now))

            assert_receive {^query_event, _measurements, %{query: query}}
            assert query =~ query_marker
            refute_receive {^query_event, _, _}
            :ok = :telemetry.detach(handler)

            claimed = row!(run_id)

            assert %{
                     run_id: ^run_id,
                     owner_scope: :tenantless,
                     graph_id: "graph",
                     graph_hash: "hash",
                     checkpoint_seq: 7,
                     claim_token: claim_token,
                     claimed_at: @now,
                     claim_attempt: 1,
                     orphan_ttl_ms: 1_000
                   } = lease

            assert map_size(lease) == 9
            assert is_binary(claim_token)
            assert inserted.run_id == run_id
            assert inserted.claim_token == nil
            assert inserted.claim_attempts == 0
            assert inserted.wake_at == @now
            assert claimed.claim_token == claim_token
            assert claimed.claimed_at == @now
            assert claimed.claim_attempts == 1
            assert claimed.wake_at == nil
          end
        end,
        quote do
          test unquote("#{name} RunStore preserves a one-query PostgreSQL error") do
            implementation = unquote(implementation)
            implementation_opts = unquote(Macro.escape(implementation_opts))
            repo = unquote(repo)
            query_event = unquote(query_event)
            query_marker = unquote(query_marker)

            prefix =
              "missing_claim_policy_contract_#{System.unique_integer([:positive, :monotonic])}"

            root = %{repo: repo, prefix: prefix}

            claim_policy =
              Docket.Postgres.ClaimPolicy.new(
                [implementation: implementation] ++ implementation_opts,
                root
              )

            context = Map.put(root, :claim_policy, claim_policy)

            effective_policy =
              Docket.Postgres.ClaimPolicy.effective_policy!(policy(@now))

            plan =
              Docket.Postgres.ClaimPolicy.build_plan(
                claim_policy,
                context,
                effective_policy
              )

            assert {:error, %Postgrex.Error{} = expected_error} =
                     Ecto.Adapters.SQL.query(repo, plan.statement, plan.params)

            handler = "claim-policy-run-store-error-#{System.unique_integer([:positive])}"

            :telemetry.attach(
              handler,
              query_event,
              &Docket.Test.TelemetryRelay.raw/4,
              self()
            )

            on_exit(fn -> :telemetry.detach(handler) end)

            assert {:error, %Postgrex.Error{} = returned_error} =
                     Docket.Postgres.RunStore.claim_due(context, :system, policy(@now))

            assert returned_error.postgres == expected_error.postgres
            assert Exception.message(returned_error) == Exception.message(expected_error)

            assert_receive {^query_event, _measurements, %{query: query}}
            assert query =~ query_marker
            refute_receive {^query_event, _, _}
          end
        end
      ]
    end
  end
end
