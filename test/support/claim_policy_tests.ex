if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.AlternateClaimPolicy do
    @moduledoc false

    @behaviour Docket.Postgres.ClaimPolicy

    @impl true
    def init(marker: marker), do: {:ok, marker}
    def init(options), do: {:error, {:expected_marker, options}}

    @impl true
    def claim_due(run_store, context, :system, policy, marker) do
      run_store.claim_due(context, :system, Map.put(policy, :alternate_marker, marker))
    end
  end

  defmodule Docket.Test.ClaimPolicyTests do
    @moduledoc false

    defmodule RunStore do
      @moduledoc false

      def claim_due(agent, :system, policy) do
        Agent.get_and_update(agent, fn state ->
          {state.result, %{state | calls: state.calls ++ [policy]}}
        end)
      end
    end

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

        setup do
          batch =
            {:ok,
             %{
               leases: [%{run_id: "contract-run"}],
               poisoned: [%{run_id: "contract-poison"}]
             }}

          {:ok, agent} =
            start_supervised({Agent, fn -> %{result: batch, calls: []} end})

          %{agent: agent, batch: batch}
        end

        test "constructs the portable policy and delegates one admission operation", %{
          agent: agent,
          batch: batch
        } do
          claim_policy =
            Docket.Postgres.ClaimPolicy.new(
              [implementation: @claim_policy_implementation] ++ @claim_policy_options
            )

          runtime_input = %{
            now: @now,
            limit: 7,
            orphan_ttl_ms: 5_000,
            max_claim_attempts: 4,
            preference: :expired
          }

          assert ^batch =
                   Docket.Postgres.ClaimPolicy.claim_due(
                     claim_policy,
                     Docket.Test.ClaimPolicyTests.RunStore,
                     agent,
                     runtime_input
                   )

          assert [effective] = Agent.get(agent, & &1.calls)
          assert Map.take(effective, Map.keys(runtime_input)) == runtime_input
        end

        test "passes implementation errors through unchanged and identifies it in telemetry", %{
          agent: agent
        } do
          Agent.update(agent, &%{&1 | result: {:error, :admission_failed}})
          handler = "claim-policy-contract-#{System.unique_integer([:positive])}"

          :telemetry.attach(
            handler,
            [:docket, :postgres, :claim_policy, :admission],
            &Docket.Test.TelemetryRelay.raw/4,
            self()
          )

          on_exit(fn -> :telemetry.detach(handler) end)

          claim_policy =
            Docket.Postgres.ClaimPolicy.new(
              [implementation: @claim_policy_implementation] ++ @claim_policy_options
            )

          assert {:error, :admission_failed} =
                   Docket.Postgres.ClaimPolicy.claim_due(
                     claim_policy,
                     Docket.Test.ClaimPolicyTests.RunStore,
                     agent,
                     %{
                       now: @now,
                       limit: 1,
                       orphan_ttl_ms: 1_000,
                       max_claim_attempts: 3,
                       preference: :ready
                     }
                   )

          assert_receive {[:docket, :postgres, :claim_policy, :admission],
                          %{demand: 1, leases: 0, poisoned: 0},
                          %{
                            implementation: @claim_policy_implementation,
                            result: :error
                          }}

          assert [_one_call] = Agent.get(agent, & &1.calls)
        end
      end
    end
  end
end
