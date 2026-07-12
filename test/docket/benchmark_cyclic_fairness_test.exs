if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.BenchmarkCyclicFairnessTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    test "cyclic comparison yields, reacquires, and records one-step tail impact" do
      output =
        Path.join(
          System.tmp_dir!(),
          "docket-cyclic-fairness-#{System.unique_integer([:positive])}.json"
        )

      on_exit(fn -> File.rm(output) end)

      assert {:ok, config} =
               Docket.Benchmark.parse(
                 ~w(--scenario cyclic_vs_one_step --runs 4 --concurrency 1 --pool-size 2 --cycle-moments 6 --drain-max-moments 2 --timeout-ms 30000 --output #{output})
               )

      assert {:ok, %{artifacts: [artifact]}} = Docket.Benchmark.run(config)
      assert artifact.success

      assert artifact.runtime_configuration.drain_budget == %{
               max_moments: 2,
               max_elapsed_ms: 3_000
             }

      assert artifact.runtime_configuration.max_attempt_elapsed_ms == 2_000

      assert artifact.workload.graph_shape.requested_cycle_iterations == 6
      assert artifact.workload.graph_shape.compatibility_option == "--cycle-moments"
      assert artifact.workload.graph_shape.iteration_semantics =~ "separate decision superstep"
      refute Map.has_key?(artifact.workload.graph_shape, :requested_cycle_moments)
      assert artifact.workload.graph_shape.terminal_max_supersteps > 6

      fairness = artifact.measurements.drain_fairness
      cyclic_runs = artifact.workload.cohorts.cyclic
      one_step_runs = artifact.workload.cohorts.one_step

      assert fairness.budget_yields.max_moments >= cyclic_runs
      assert fairness.budget_yields.max_elapsed_ms == 0
      assert fairness.budget_yields.both == 0
      assert fairness.budget_yields.total == fairness.budget_yields.max_moments

      assert fairness.claims.reacquisitions_after_yield == fairness.budget_yields.total
      assert fairness.claims.total_claims == config.runs + fairness.budget_yields.total
      assert fairness.claims.yield_reacquisition_difference == 0

      observed = fairness.observed_commits

      assert observed.exact_lifecycle_commits_across_all_cohorts ==
               artifact.measurements.counts.committed_moments

      assert observed.exact_lifecycle_commits_across_all_cohorts ==
               observed.exact_checkpoint_commits_across_all_cohorts

      retained = observed.checkpoint_commits_per_run_retained_sample
      assert retained.sampled_runs == config.runs
      assert retained.population_runs == config.runs
      assert retained.scope =~ "bounded correlation sample only"

      assert Enum.reduce(
               retained.frequencies,
               0,
               fn frequency, total ->
                 total + frequency.checkpoint_commits_per_run * frequency.run_count
               end
             ) == retained.retained_sample_checkpoint_commits

      assert Enum.any?(retained.frequencies, fn frequency ->
               frequency.checkpoint_commits_per_run > config.cycle_moments and
                 frequency.run_count == cyclic_runs
             end)

      assert observed.scope =~ "exact global aggregate"
      assert fairness.proof_scope =~ "aggregate-only"

      assert fairness.fast_one_step_tail_impact.runs == one_step_runs
      assert fairness.fast_one_step_tail_impact.completed_runs == one_step_runs
      assert is_number(fairness.fast_one_step_tail_impact.activation_to_first_claim_p95_us)
      assert is_number(fairness.fast_one_step_tail_impact.activation_to_terminal_p95_us)
      assert is_number(fairness.fast_one_step_tail_impact.first_claim_to_terminal_p95_us)

      assert artifact.measurements.cohorts.one_step.activation_to_terminal_commit_offset_us.p95 <
               artifact.measurements.cohorts.cyclic.activation_to_terminal_commit_offset_us.p95

      invariant_names = Enum.map(artifact.invariants, & &1.name)
      assert "aggregate budget-yield count is at least the cyclic cohort size" in invariant_names
      assert "aggregate claim reacquisitions equal aggregate budget yields" in invariant_names

      assert "exact aggregate lifecycle commits equal exact aggregate checkpoint commits" in invariant_names

      refute Enum.any?(invariant_names, &String.starts_with?(&1, "every cyclic run"))

      assert Enum.any?(artifact.warnings, fn warning ->
               warning =~ "controls cycle iterations" and warning =~ "observed_commits"
             end)

      assert Enum.all?(artifact.invariants, & &1.pass)
      assert artifact.cleanup.isolated_database_removed
    end
  end
end
