if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.WindowedRunStoreSetup do
    @moduledoc false

    def prepare!(repo, implementation_opts) do
      context =
        Docket.Postgres.context(
          repo: repo,
          claim_policy:
            [implementation: Docket.Postgres.ClaimPolicy.WindowedInterleave] ++
              implementation_opts
        )

      claim_policy = Docket.Postgres.ClaimPolicy.resolve(context)

      :ok =
        Docket.Postgres.ClaimPolicy.configure(claim_policy, context, fn statement, params ->
          repo.query(statement, params, log: false)
        end)

      repo.query!(
        "INSERT INTO docket_claim_partitions (scope_key) VALUES ('') ON CONFLICT DO NOTHING"
      )

      :ok
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
            assert inserted.tenant_admitted_at == nil
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
