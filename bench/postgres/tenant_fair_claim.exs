unless Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  Mix.raise("tenant-fair claim benchmarks require ecto_sql and postgrex")
end

Code.require_file(Path.expand("../support/tenant_fair_claim.ex", __DIR__))

Docket.Bench.TenantFairClaim.main(System.argv())
