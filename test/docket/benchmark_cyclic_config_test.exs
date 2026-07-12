defmodule Docket.BenchmarkCyclicConfigTest do
  use ExUnit.Case, async: true

  test "cyclic fairness defaults exceed the bounded vehicle drain" do
    assert {:ok, config} =
             Docket.Benchmark.parse(~w(--scenario cyclic_vs_one_step --runs 4))

    assert config.cycle_moments == 12
    assert config.drain_max_moments == 4
    assert config.drain_max_elapsed_ms == nil
    assert config.cycle_moments > config.drain_max_moments

    if Code.ensure_loaded?(Docket.Benchmark.Postgres) do
      opts = apply(Docket.Benchmark.Postgres, :runtime_opts, [config])
      assert get_in(opts, [:vehicle, :drain_budget]) == [max_moments: 4]
    end
  end

  test "accepts an optional elapsed drain limit and rejects non-yielding shapes" do
    assert {:ok, config} =
             Docket.Benchmark.parse(
               ~w(--scenario cyclic_vs_one_step --runs 4 --cycle-moments 20 --drain-max-moments 5 --drain-max-elapsed-ms 250)
             )

    assert config.cycle_moments == 20
    assert config.drain_max_moments == 5
    assert config.drain_max_elapsed_ms == 250

    if Code.ensure_loaded?(Docket.Benchmark.Postgres) do
      opts = apply(Docket.Benchmark.Postgres, :runtime_opts, [config])

      assert get_in(opts, [:vehicle, :drain_budget]) ==
               [max_elapsed_ms: 250, max_moments: 5]
    end

    assert {:error, message} =
             Docket.Benchmark.parse(
               ~w(--scenario cyclic_vs_one_step --runs 4 --cycle-moments 4 --drain-max-moments 4)
             )

    assert message =~ "cycle-moments must be greater than drain-max-moments"
  end

  test "rejects cycle and drain controls outside cyclic_vs_one_step" do
    for args <- [
          ~w(--scenario smoke --cycle-moments 12),
          ~w(--scenario mixed_service_times --runs 4 --drain-max-moments 4),
          ~w(--scenario blocked_vehicles --runs 4 --concurrency 2 --drain-max-elapsed-ms 100)
        ] do
      assert {:error, message} = Docket.Benchmark.parse(args)
      assert message =~ "cycle/drain controls are only valid for cyclic_vs_one_step"
    end
  end
end
