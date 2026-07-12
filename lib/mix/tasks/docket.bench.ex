defmodule Mix.Tasks.Docket.Bench do
  use Mix.Task

  @shortdoc "Runs reproducible Postgres dispatcher benchmarks"

  @moduledoc """
  Runs a bounded benchmark through the real supervised Postgres runtime.

      mix docket.bench --scenario smoke --runs 10 --concurrency 2 \\
        --pool-size 5 --output results/smoke.json

  Current scenarios are `smoke` and `empty_one_step`. Results are exploratory;
  unsupported scenarios and production-incompatible options are rejected.
  """

  @impl Mix.Task
  def run(argv) do
    Mix.Task.run("app.start")

    with {:ok, config} <- Docket.Benchmark.parse(argv),
         {:ok, result} <- Docket.Benchmark.run(config) do
      Mix.shell().info("wrote #{result.output}")
    else
      {:error, reason} -> Mix.raise(reason)
    end
  end
end
