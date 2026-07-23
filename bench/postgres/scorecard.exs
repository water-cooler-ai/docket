unless Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  Mix.raise("scorecard benchmarks require ecto_sql and postgrex")
end

for file <- [
      "config.ex",
      "stats.ex",
      "db.ex",
      "nodes.ex",
      "seed.ex",
      "runtime.ex",
      "probe.ex",
      "invariants.ex",
      "scenario.ex",
      "scenarios/throughput.ex",
      "scenarios/concurrency.ex",
      "scenarios/claim_ceiling.ex",
      "scenarios/tenant_fairness.ex",
      "scenarios/fast_slow.ex",
      "scenarios/sticky_cohort.ex",
      "scenarios/surge.ex",
      "report.ex"
    ] do
  Code.require_file(Path.expand("../support/scorecard/#{file}", __DIR__))
end

Code.require_file(Path.expand("../support/scorecard.ex", __DIR__))

Docket.Bench.Scorecard.main(System.argv())
