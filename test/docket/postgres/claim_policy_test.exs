if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicyTest do
    use ExUnit.Case, async: true

    alias Docket.Postgres.ClaimPolicy
    alias Docket.Postgres.ClaimPolicy.TenantFair
    alias Docket.Postgres.ClaimPolicy.TenantFair.Config

    @context %{repo: Docket.Postgres.TestRepo, prefix: "public"}

    test "defaults to the legacy admission engine" do
      policy = ClaimPolicy.new([], @context)
      assert ClaimPolicy.implementation(policy) == Docket.Postgres.ClaimPolicy.Legacy
    end

    test "TenantFair accepts only one positive default cap" do
      assert {:ok, %Config{default_max_active_runs: 4}} = Config.new(default_max_active_runs: 4)
      assert {:error, {:missing_option, :default_max_active_runs}} = Config.new([])

      assert {:error, {:invalid_option, :default_max_active_runs}} =
               Config.new(default_max_active_runs: 0)

      assert {:error, {:unknown_options, [:weight]}} =
               Config.new(default_max_active_runs: 4, weight: 2)

      assert {:error, {:unknown_options, [:default_max_active]}} =
               Config.new(default_max_active: 4)
    end

    test "TenantFair builds one bounded database-authoritative statement" do
      policy =
        ClaimPolicy.new(
          [implementation: TenantFair, default_max_active_runs: 3],
          @context
        )

      runtime =
        ClaimPolicy.effective_policy!(%{
          now: ~U[2026-07-16 00:00:00.000000Z],
          limit: 8,
          orphan_ttl_ms: 1_000,
          max_claim_attempts: 5,
          preference: :ready
        })

      plan = ClaimPolicy.build_plan(policy, @context, runtime)

      assert plan.demand == 8

      assert plan.params == [
               ~U[2026-07-16 00:00:00.000000Z],
               ~U[2026-07-15 23:59:59.000000Z],
               8,
               5,
               "ready",
               3
             ]

      assert plan.statement =~ "docket_tenant_fair_claim"
      assert plan.statement =~ "false"
      assert plan.statement =~ "WHERE claimed.row_kind IN ('outcome', 'error')"
      assert plan.statement =~ "ORDER BY claimed.visit_ordinal"
      refute plan.statement =~ "eligible_partitions"
    end

    test "TenantFair outcome observations distinguish promotion, reacquisition, and steal" do
      now = ~U[2026-07-16 00:00:00.000000Z]
      {:ok, config} = Config.new(default_max_active_runs: 3)

      rows =
        [
          {"queued", "queued_ready", 1, 3_000},
          {"admitted", "admitted_ready", 1, 2_000},
          {"expired", "expired", 2, 4_000}
        ]
        |> Enum.with_index(1)
        |> Enum.map(fn {{run_id, work_class, attempt, age_ms}, index} ->
          [
            "outcome",
            nil,
            run_id,
            "tenant",
            "graph",
            "hash",
            7,
            Ecto.UUID.dump!(
              "00000000-0000-0000-0000-#{String.pad_leading(to_string(index), 12, "0")}"
            ),
            now,
            attempt,
            nil,
            nil,
            work_class,
            DateTime.add(now, -age_ms, :millisecond)
          ]
        end)

      assert {:ok, %{leases: leases, poisoned: []}, stats} =
               TenantFair.decode(rows, %{now: now, orphan_ttl_ms: 1_000}, config)

      assert Enum.map(leases, & &1.run_id) == ["queued", "admitted", "expired"]

      assert %{
               queued_promotions: 1,
               queued_ready_selected: 1,
               admitted_ready_selected: 1,
               expired_selected: 1,
               steals: 1
             } = stats
    end

    test "TenantFair reports an admitted poison as an admission release" do
      now = ~U[2026-07-16 00:00:00.000000Z]
      {:ok, config} = Config.new(default_max_active_runs: 1)
      handler = "tenant-fair-poison-release-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:docket, :postgres, :admission, :release],
        &Docket.Test.TelemetryRelay.raw/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      rows = [
        [
          "outcome",
          nil,
          "poisoned",
          "tenant",
          "graph",
          "hash",
          7,
          nil,
          nil,
          5,
          now,
          "max_claim_attempts_exceeded",
          "admitted_ready",
          now
        ]
      ]

      assert {:ok, %{leases: [], poisoned: [_]}, %{admission_releases: 1} = stats} =
               TenantFair.decode(rows, %{now: now, orphan_ttl_ms: 1_000}, config)

      assert :ok =
               TenantFair.observe(
                 %{demand: 1, preference: nil},
                 stats,
                 {:ok,
                  %{leases: [], poisoned: [%{poison_reason: "max_claim_attempts_exceeded"}]}},
                 1,
                 config
               )

      assert_receive {[:docket, :postgres, :admission, :release], %{count: 1}, %{reason: :poison}}
    end

    test "rejects unknown implementations and invalid runtime input" do
      assert_raise ArgumentError, ~r/does not implement/, fn ->
        ClaimPolicy.new([implementation: String], @context)
      end

      assert_raise ArgumentError, ~r/requires DateTime/, fn ->
        ClaimPolicy.effective_policy!(%{now: :now, limit: 0})
      end
    end
  end
end
