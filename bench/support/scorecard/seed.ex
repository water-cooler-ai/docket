defmodule Docket.Bench.Scorecard.Seed do
  @moduledoc "Graph publish, deterministic run seeding, and wake_at staging via a manual runtime."

  alias Docket.Bench.Scorecard.Db

  @runtime_name Docket.Bench.Scorecard.Instance

  def runtime_name, do: @runtime_name

  def run_id(scenario, idx), do: "sc-#{scenario}-#{idx}"

  def seed(ctx, plan) do
    {:ok, runtime} =
      Docket.Runtime.Supervisor.start_link(manual_runtime_opts(ctx, plan.tenant_mode))

    try do
      refs = save_graphs(plan)

      Enum.each(plan.runs, fn run ->
        start_one(refs, run, run_id(plan.scenario, run.idx), plan.tenant_mode)
      end)
    after
      if Process.alive?(runtime), do: Supervisor.stop(runtime, :normal, 5_000)
    end

    stage(ctx, plan)
    seed_map(plan)
  end

  defp manual_runtime_opts(ctx, tenant_mode) do
    [
      name: @runtime_name,
      backend: Docket.Postgres,
      repo: ctx.repo,
      prefix: ctx.prefix,
      tenant_mode: tenant_mode,
      testing: :manual
    ]
  end

  defp save_graphs(%{tenant_mode: :none, graph: graph}) do
    {:ok, ref} = Docket.save_graph(@runtime_name, graph)
    %{nil => ref}
  end

  defp save_graphs(%{tenant_mode: :required, graph: graph, runs: runs}) do
    runs
    |> Enum.map(& &1.tenant)
    |> Enum.uniq()
    |> Map.new(fn tenant ->
      {:ok, ref} = Docket.save_graph(@runtime_name, graph, tenant_id: tenant)
      {tenant, ref}
    end)
  end

  defp start_one(refs, _run, run_id, :none) do
    {:ok, _run} = Docket.start_run(@runtime_name, refs[nil], %{}, run_id: run_id)
  end

  defp start_one(refs, run, run_id, :required) do
    {:ok, _run} =
      Docket.start_run(@runtime_name, refs[run.tenant], %{},
        run_id: run_id,
        tenant_id: run.tenant
      )
  end

  defp stage(ctx, plan) do
    runs = Db.table(ctx.prefix, "docket_runs")

    plan.runs
    |> Enum.group_by(& &1.due_at)
    |> Enum.each(fn {due_at, group} ->
      ids = Enum.map(group, &run_id(plan.scenario, &1.idx))

      Db.repo().query!(
        "UPDATE #{runs} SET wake_at = $1 WHERE run_id = ANY($2) AND status = 'running' AND claim_token IS NULL AND poisoned_at IS NULL",
        [due_at, ids]
      )
    end)
  end

  defp seed_map(plan) do
    Map.new(plan.runs, fn run ->
      {run_id(plan.scenario, run.idx),
       %{due_at: run.due_at, cohort: run.cohort, tenant: run.tenant}}
    end)
  end
end
