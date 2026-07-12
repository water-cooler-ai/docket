if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Benchmark.Postgres.Scenario do
    @moduledoc false

    @type config :: map()
    @type context :: %{database: map(), repo: pid()}
    @type result :: {:ok, map()}

    @callback name() :: String.t()
    @callback run(config(), context()) :: result()

    @scenarios %{
      "smoke" => Docket.Benchmark.Postgres.Scenarios.EmptyOneStep,
      "empty_one_step" => Docket.Benchmark.Postgres.Scenarios.EmptyOneStep,
      "claim_only" => Docket.Benchmark.Postgres.Scenarios.ClaimOnly,
      "blocked_vehicles" => Docket.Benchmark.Postgres.Scenarios.BlockedVehicles,
      "mixed_service_times" => Docket.Benchmark.Postgres.Scenarios.MixedServiceTimes,
      "parked_wait_vs_blocking_wait" =>
        Docket.Benchmark.Postgres.Scenarios.ParkedWaitVsBlockingWait,
      "cyclic_vs_one_step" => Docket.Benchmark.Postgres.Scenarios.CyclicVsOneStep
    }

    @doc "Returns the benchmark module for a validated scenario name."
    def fetch!(name), do: Map.fetch!(@scenarios, name)

    @doc "Returns the canonical scenario name written to benchmark artifacts."
    def canonical_name(name), do: name |> fetch!() |> apply(:name, [])
  end
end
