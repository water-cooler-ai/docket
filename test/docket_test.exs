defmodule DocketTest do
  use ExUnit.Case, async: true

  doctest Docket

  test "loads the root module" do
    assert Code.ensure_loaded?(Docket)
  end

  if System.get_env("DOCKET_CORE_ONLY") in ["1", "true"] do
    test "the core build excludes every PostgreSQL integration module" do
      assert Code.ensure_loaded?(Docket.Test)

      for module <- [
            Ecto.Adapters.SQL,
            Postgrex,
            Docket.Postgres,
            Docket.Postgres.MomentStore,
            Docket.Postgres.Telemetry,
            Mix.Tasks.Docket.Gen.Migration
          ] do
        refute Code.ensure_loaded?(module)
      end
    end
  end
end
