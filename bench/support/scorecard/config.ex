defmodule Docket.Bench.Scorecard.Config do
  @moduledoc "Profiles, CLI parsing, and validation for the Postgres scorecard suite."

  @default_seed 62_038

  @claim_policies [%{name: "legacy", config: []}]

  @scenario_names [
    "throughput",
    "concurrency",
    "claim_ceiling",
    "tenant_fairness",
    "fast_slow",
    "surge"
  ]

  @profiles %{
    "smoke" => %{
      poll_interval_ms: 50,
      drain_timeout_ms: 30_000,
      scenarios: %{
        "throughput" => %{n: 300, concurrency: 8, target_runs_per_sec: 400},
        "concurrency" => %{levels: [2, 8], per_slot: 50},
        "claim_ceiling" => %{n: 1_000, workers: 4, batch: 50, target_claims_per_sec: 5_000},
        "tenant_fairness" => %{tenants: 6, hot_fraction: 0.6, n: 300, concurrency: 8},
        "fast_slow" => %{
          concurrency: 8,
          n_fast: 200,
          hold_ms: 400,
          slowdown_good: 1.5,
          slowdown_bad: 8.0
        },
        "surge" => %{window_ms: 20_000, concurrency: 8}
      }
    },
    "local" => %{
      poll_interval_ms: 50,
      drain_timeout_ms: 120_000,
      scenarios: %{
        "throughput" => %{n: 2_000, concurrency: 32, target_runs_per_sec: 1_500},
        "concurrency" => %{levels: [4, 16, 64], per_slot: 60},
        "claim_ceiling" => %{n: 20_000, workers: 8, batch: 50, target_claims_per_sec: 25_000},
        "tenant_fairness" => %{tenants: 10, hot_fraction: 0.6, n: 1_500, concurrency: 16},
        "fast_slow" => %{
          concurrency: 16,
          n_fast: 800,
          hold_ms: 1_000,
          slowdown_good: 1.5,
          slowdown_bad: 8.0
        },
        "surge" => %{window_ms: 60_000, concurrency: 16}
      }
    },
    "scale" => %{
      poll_interval_ms: 50,
      drain_timeout_ms: 600_000,
      scenarios: %{
        "throughput" => %{n: 10_000, concurrency: 64, target_runs_per_sec: 2_500},
        "concurrency" => %{levels: [8, 32, 128], per_slot: 100},
        "claim_ceiling" => %{n: 100_000, workers: 16, batch: 50, target_claims_per_sec: 40_000},
        "tenant_fairness" => %{tenants: 20, hot_fraction: 0.6, n: 6_000, concurrency: 32},
        "fast_slow" => %{
          concurrency: 32,
          n_fast: 2_000,
          hold_ms: 1_000,
          slowdown_good: 1.5,
          slowdown_bad: 8.0
        },
        "surge" => %{window_ms: 120_000, concurrency: 32}
      }
    }
  }

  @switches [
    profile: :string,
    only: :string,
    output: :string,
    check: :boolean,
    keep_schema: :boolean,
    seed: :integer,
    help: :boolean
  ]

  def scenario_names, do: @scenario_names

  def claim_policy_config(ctx) do
    case Map.get(ctx, :claim_policy) do
      %{config: config} -> config
      nil -> []
    end
  end

  def parse!(argv) do
    {opts, positional} = OptionParser.parse!(argv, strict: @switches)

    if positional != [] do
      raise ArgumentError, "unexpected positional arguments: #{inspect(positional)}"
    end

    if opts[:help] do
      IO.puts(usage())
      System.halt(0)
    end

    profile = Keyword.get(opts, :profile, "local")
    base = Map.fetch!(@profiles, profile)

    config =
      Map.merge(base, %{
        profile: profile,
        seed: Keyword.get(opts, :seed, @default_seed),
        only: parse_only(Keyword.get(opts, :only)),
        output: Keyword.get(opts, :output),
        check: Keyword.get(opts, :check, false),
        keep_schema: Keyword.get(opts, :keep_schema, false),
        claim_policies: @claim_policies
      })

    validate!(config)
  rescue
    KeyError ->
      raise ArgumentError,
            "unknown profile; expected one of #{inspect(Map.keys(@profiles))}\n\n#{usage()}"
  end

  def pool_size(config) do
    scenarios = config.scenarios

    concurrencies = [
      scenarios["throughput"].concurrency,
      Enum.max(scenarios["concurrency"].levels),
      scenarios["claim_ceiling"].workers,
      scenarios["tenant_fairness"].concurrency,
      scenarios["fast_slow"].concurrency,
      scenarios["surge"].concurrency
    ]

    Enum.max(concurrencies) + 6
  end

  def usage do
    """
    Usage:
      DOCKET_BENCH_DATABASE_URL=postgres://... \\
        mix run bench/postgres/scorecard.exs -- [options]

    Options:
      --profile smoke|local|scale   scenario knobs and score targets (default: local)
      --only scenario1,scenario2    run only the named scenarios
      --output PATH                 artifact directory (default under tmp/bench/postgres/scorecard)
      --check                       raise on any invariant violation (no timing gates)
      --keep-schema                 retain the generated scratch schema
      --seed N                      recorded in manifests for provenance; execution is
                                    deterministic by construction (default: #{@default_seed})

    Scenarios: #{Enum.join(@scenario_names, ", ")}
    """
  end

  defp parse_only(nil), do: nil

  defp parse_only(value) do
    names =
      value
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.uniq()

    unknown = Enum.reject(names, &(&1 in @scenario_names))

    if unknown != [] do
      raise ArgumentError,
            "unknown scenarios in --only: #{inspect(unknown)}; expected #{inspect(@scenario_names)}"
    end

    if names == [] do
      raise ArgumentError, "--only requires at least one scenario name"
    end

    names
  end

  defp validate!(config) do
    unless is_integer(config.seed) and config.seed >= 0 do
      raise ArgumentError, "seed must be a non-negative integer, got: #{inspect(config.seed)}"
    end

    Enum.each(@scenario_names, fn name ->
      unless Map.has_key?(config.scenarios, name) do
        raise ArgumentError, "profile #{config.profile} is missing scenario knobs for #{name}"
      end
    end)

    validate_scenario!(config.scenarios["throughput"], [:n, :concurrency, :target_runs_per_sec])

    validate_scenario!(config.scenarios["claim_ceiling"], [
      :n,
      :workers,
      :batch,
      :target_claims_per_sec
    ])

    validate_scenario!(config.scenarios["tenant_fairness"], [:tenants, :n, :concurrency])
    validate_scenario!(config.scenarios["fast_slow"], [:concurrency, :n_fast, :hold_ms])
    validate_scenario!(config.scenarios["surge"], [:window_ms, :concurrency])
    validate_scenario!(config.scenarios["concurrency"], [:per_slot])

    levels = config.scenarios["concurrency"].levels

    unless is_list(levels) and levels != [] and Enum.all?(levels, &(is_integer(&1) and &1 > 0)) do
      raise ArgumentError, "concurrency levels must be a non-empty list of positive integers"
    end

    hot_fraction = config.scenarios["tenant_fairness"].hot_fraction

    unless is_float(hot_fraction) and hot_fraction > 0.0 and hot_fraction < 1.0 do
      raise ArgumentError,
            "hot_fraction must be a float strictly between 0.0 and 1.0, got: #{inspect(hot_fraction)}"
    end

    %{slowdown_good: good, slowdown_bad: bad} = config.scenarios["fast_slow"]

    unless is_number(good) and is_number(bad) and good > 0 and bad > good do
      raise ArgumentError,
            "fast_slow requires slowdown_bad > slowdown_good > 0, got good=#{inspect(good)} bad=#{inspect(bad)}"
    end

    validate_claim_policies!(config.claim_policies)

    config
  end

  defp validate_claim_policies!(policies) do
    if policies == [] do
      raise ArgumentError, "claim_policies must name at least one policy"
    end

    names = Enum.map(policies, & &1.name)

    if Enum.uniq(names) != names do
      raise ArgumentError, "claim_policies names must be unique, got: #{inspect(names)}"
    end

    Enum.each(policies, fn %{name: name, config: config} ->
      case Keyword.get(config, :implementation) do
        nil ->
          :ok

        module ->
          unless Code.ensure_loaded?(module) do
            raise ArgumentError,
                  "claim policy #{name} names implementation #{inspect(module)}, which is not available"
          end
      end
    end)
  end

  defp validate_scenario!(knobs, keys) do
    Enum.each(keys, fn key ->
      value = Map.fetch!(knobs, key)

      unless is_integer(value) and value > 0 do
        raise ArgumentError, "#{key} must be a positive integer, got: #{inspect(value)}"
      end
    end)
  end
end
