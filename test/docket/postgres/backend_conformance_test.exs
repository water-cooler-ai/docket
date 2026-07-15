if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.BackendConformanceTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    use Docket.Backend.Conformance,
      harness: Docket.Test.BackendConformance.PostgresHarness
  end
end
