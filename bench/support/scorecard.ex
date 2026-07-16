defmodule Docket.Bench.Scorecard do
  @moduledoc """
  Orchestrates the Postgres scorecard suite.

  Boots a dedicated Repo, provisions a scratch schema, runs the selected
  scenarios against the real supervised runtime, renders the console scorecard,
  and writes manifest.json and scorecard.json. Every scenario runs inside
  try/rescue so one failure cannot end the suite; a scenario whose invariants
  fail is scored nil and gates the overall line. `--check` raises on invariant
  violations and on any scenario that failed or produced no score; it never
  gates on the timing scores themselves.
  """

  alias Docket.Bench.Scorecard.{Config, Db, Report, Scenario}

  def main(argv) do
    config = Config.parse!(Enum.drop_while(argv, &(&1 == "--")))
    url = Db.database_url!()
    git = Db.git_metadata()
    prefix = Db.scratch_prefix()
    started_at = DateTime.utc_now()

    Db.start_repo!(url, Config.pool_size(config))

    try do
      postgres = Db.postgres_metadata!()
      Db.require_postgres_13!(postgres.server_version_num)
      Db.create_schema!(prefix)

      artifacts_dir = prepare_artifacts!(config, git)
      ctx = %{repo: Db.repo(), prefix: prefix, config: config, artifacts_dir: artifacts_dir}
      selected = config.only || Scenario.names()

      Report.write_manifest!(artifacts_dir, %{
        schema_version: 1,
        started_at: started_at,
        git: git,
        runtime: Db.runtime_metadata(),
        postgres: postgres,
        profile: config.profile,
        config: config,
        seed: config.seed,
        scratch_schema: prefix,
        selected: selected,
        artifacts_dir: artifacts_dir
      })

      results = Enum.flat_map(selected, fn name -> Scenario.run_variants(name, ctx) end)

      Report.render(results, %{
        git: git,
        pg: postgres.server_version,
        profile: config.profile,
        seed: config.seed
      })

      Report.write_scorecard!(artifacts_dir, %{
        schema_version: 1,
        run_id: Path.basename(artifacts_dir),
        started_at: started_at,
        finished_at: DateTime.utc_now(),
        git: git,
        profile: config.profile,
        seed: config.seed,
        results: results
      })

      IO.puts("\nwrote #{Path.join(artifacts_dir, "scorecard.json")}")

      if config.check, do: check!(results)
    after
      try do
        if not config.keep_schema, do: Db.drop_schema_if_exists!(prefix)
      after
        Db.stop_repo()
      end
    end
  end

  defp check!(results) do
    invariant_failures =
      for result <- results,
          invariant <- result.invariants,
          not invariant.pass do
        {result.scenario, invariant.name, invariant.expected, invariant.actual}
      end

    scenario_failures =
      for result <- results, not result.passed or result.score == nil do
        {result.scenario, result.evidence}
      end

    failures = invariant_failures ++ scenario_failures

    if failures != [] do
      raise "scorecard --check failed: #{inspect(failures)}"
    end

    :ok
  end

  defp prepare_artifacts!(config, git) do
    nonce = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)

    run_id =
      DateTime.utc_now()
      |> DateTime.truncate(:second)
      |> DateTime.to_iso8601(:basic)
      |> String.replace([":", "-"], "")
      |> Kernel.<>("-#{String.slice(git.sha, 0, 8)}-#{nonce}")

    root = config.output || Path.join(["tmp", "bench", "postgres", "scorecard", run_id])
    root = Path.expand(root)

    if File.exists?(root) do
      raise ArgumentError,
            "refusing to reuse scorecard artifact directory #{root}; choose a fresh --output path"
    end

    File.mkdir_p!(root)
    root
  end
end
