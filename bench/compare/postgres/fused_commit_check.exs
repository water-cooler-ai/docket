for file <- ["db.ex"] do
  Code.require_file(Path.expand("../../support/scorecard/#{file}", __DIR__))
end

defmodule Docket.Bench.Compare.Postgres.FusedCommitCheck do
  alias Docket.Bench.Scorecard.Db
  alias Docket.Postgres.{ClaimPolicy, EventStore, GraphStore, RunStore, Storage}
  alias Docket.Runtime.Moment

  def main do
    database_url = System.fetch_env!("DOCKET_BENCH_DATABASE_URL")
    prefix = Db.scratch_prefix()

    Db.start_repo!(database_url, 4, ssl_options(database_url))

    try do
      Db.create_schema!(prefix)
      ctx = %{repo: Db.repo(), prefix: prefix}
      graph = publish_graph!(ctx)

      check_success!(ctx, graph)
      check_conflict_rollback!(ctx, graph)

      IO.puts("FUSED_CHECK,pass,prefix=#{prefix}")
    after
      try do
        Db.drop_schema_if_exists!(prefix)
      after
        Db.stop_repo()
      end
    end
  end

  defp check_success!(ctx, {graph_id, graph_hash}) do
    initial = initialization_moment("fused-check-success", graph_id, graph_hash)
    backend = {Docket.Postgres, ctx}

    expect!({:ok, initial}, Docket.Lifecycle.start(backend, :tenantless, initial))
    lease = claim_one!(ctx)
    next = next_moment(initial, "success")

    expect!(
      {:ok, next},
      Docket.Lifecycle.commit_moment(
        backend,
        :tenantless,
        next,
        initial.run.checkpoint_seq,
        lease.claim_token
      )
    )

    expect!({:ok, next.run}, RunStore.fetch_run(ctx, :tenantless, next.run.id))

    events = Db.table(ctx.prefix, "docket_events")

    [[event_count]] =
      Db.repo().query!("SELECT count(*) FROM #{events} WHERE run_id = $1", [
        next.run.id
      ]).rows

    expect!(length(initial.events ++ next.events), event_count)
  end

  defp check_conflict_rollback!(ctx, {graph_id, graph_hash}) do
    initial = initialization_moment("fused-check-conflict", graph_id, graph_hash)
    backend = {Docket.Postgres, ctx}

    expect!({:ok, initial}, Docket.Lifecycle.start(backend, :tenantless, initial))
    lease = claim_one!(ctx)
    next = next_moment(initial, "conflict")
    [event | _rest] = next.events
    conflicting = %{event | payload: %{"different" => true}}

    expect!(:ok, EventStore.append_events(ctx, :tenantless, next.run.id, [conflicting]))
    {:ok, before} = RunStore.inspect_run(ctx, :tenantless, initial.run.id)

    expect!(
      {:error, :event_conflict},
      Docket.Lifecycle.commit_moment(
        backend,
        :tenantless,
        next,
        initial.run.checkpoint_seq,
        lease.claim_token
      )
    )

    expect!({:ok, before}, RunStore.inspect_run(ctx, :tenantless, initial.run.id))
    expect!({:ok, initial.run}, RunStore.fetch_run(ctx, :tenantless, initial.run.id))
  end

  defp publish_graph!(ctx) do
    graph_id = "fused-commit-check"
    authored = Docket.Graph.new!(id: graph_id)
    {:ok, document, runtime_graph} = Docket.Graph.Compiler.compile_for_publication(authored)

    expect!(
      {:ok, :published},
      Storage.transaction(ctx, fn transaction_ctx ->
        case GraphStore.save_graph(
               transaction_ctx,
               :tenantless,
               graph_id,
               runtime_graph.graph_hash,
               document
             ) do
          :ok -> {:ok, :published}
          {:error, reason} -> {:error, reason}
        end
      end)
    )

    {graph_id, runtime_graph.graph_hash}
  end

  defp claim_one!(ctx) do
    root = %{repo: ctx.repo, prefix: ctx.prefix}
    admission_ctx = Map.put(root, :claim_policy, ClaimPolicy.new([], root))

    {:ok, %{leases: [lease], poisoned: []}} =
      RunStore.claim_due(admission_ctx, :system, %{
        now: DateTime.utc_now(),
        limit: 1,
        orphan_ttl_ms: 60_000,
        max_claim_attempts: 3
      })

    lease
  end

  defp initialization_moment(run_id, graph_id, graph_hash) do
    now = DateTime.utc_now()

    run = %Docket.Run{
      id: run_id,
      graph_id: graph_id,
      graph_hash: graph_hash,
      status: :running,
      input: %{},
      started_at: now,
      updated_at: now
    }

    Moment.propose(
      run,
      :run_initialized,
      [Moment.event_entry(:run_initialized, 0)],
      :continue,
      now
    )
  end

  defp next_moment(initial, marker) do
    Moment.propose(
      initial.run,
      :step_committed,
      [Moment.event_entry(:node_completed, 1, node_id: marker)],
      :continue,
      DateTime.utc_now()
    )
  end

  defp expect!(expected, expected), do: :ok

  defp expect!(expected, actual) do
    raise "expected #{inspect(expected)}, got: #{inspect(actual)}"
  end

  defp ssl_options(database_url) do
    query = database_url |> URI.parse() |> Map.get(:query)
    sslmode = query && URI.decode_query(query)["sslmode"]

    if sslmode == "require", do: [ssl: [verify: :verify_none]], else: []
  end
end

Docket.Bench.Compare.Postgres.FusedCommitCheck.main()
