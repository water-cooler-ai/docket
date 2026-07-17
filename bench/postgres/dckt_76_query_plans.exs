unless Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  Mix.raise("DCKT-76 query-plan evidence requires ecto_sql and postgrex")
end

Postgrex.Types.define(Docket.Bench.DCKT76QueryPlans.Types, [], json: JSON)

defmodule Docket.Bench.DCKT76QueryPlans.Repo do
  @moduledoc false
  use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
end

defmodule Docket.Bench.DCKT76QueryPlans do
  @moduledoc false

  alias Docket.Bench.DCKT76QueryPlans.Repo
  alias Docket.Bench.DCKT76QueryPlans.Types
  alias Docket.Postgres.ClaimPolicy.TenantFair.{Budgets, QueryShapes}

  @partitions 20_000
  @deep_ready 50_000
  @one_row_tenants 10_000
  @future_timers 20_000
  @expired_rows 10_000

  def run(args) do
    {opts, _rest} =
      OptionParser.parse!(args,
        strict: [output: :string, database_url: :string]
      )

    output =
      opts[:output] ||
        Path.expand("evidence/dckt-76-query-plans.json", __DIR__)

    url =
      opts[:database_url] ||
        System.get_env("DOCKET_DCKT76_DATABASE_URL") ||
        "postgres://localhost:5432/postgres"

    {:ok, _pid} = Repo.start_link(url: url, pool_size: 2, types: Types, log: false)
    prefix = "docket_dckt76_#{System.unique_integer([:positive, :monotonic])}"

    try do
      Repo.query!("CREATE SCHEMA #{quote_identifier(prefix)}")
      tables = create_fixture!(prefix)
      seed_fixture!(tables)
      Repo.query!("ANALYZE #{tables.schedule}")
      Repo.query!("ANALYZE #{tables.schedule_sparse}")
      Repo.query!("ANALYZE #{tables.schedule_small}")
      Repo.query!("ANALYZE #{tables.runs}")
      Repo.query!("ANALYZE #{tables.partitions}")

      report = collect_report!(tables)
      File.mkdir_p!(Path.dirname(output))
      File.write!(output, JSON.encode!(report))
      IO.puts(output)
    after
      Repo.query!("DROP SCHEMA IF EXISTS #{quote_identifier(prefix)} CASCADE")
      Supervisor.stop(Repo)
    end
  end

  defp create_fixture!(prefix) do
    schedule = table(prefix, "docket_claim_schedule")
    schedule_sparse = table(prefix, "docket_claim_schedule_sparse")
    schedule_small = table(prefix, "docket_claim_schedule_small")
    runs = table(prefix, "docket_runs")
    partitions = table(prefix, "docket_claim_partitions")
    policy = table(prefix, "docket_claim_policy")

    Repo.query!("""
    CREATE TABLE #{schedule} (
      scope_key text PRIMARY KEY,
      ring_position bigint GENERATED ALWAYS AS IDENTITY NOT NULL UNIQUE,
      unfinished_count bigint NOT NULL,
      ready_candidate_cursor_at timestamptz NULL,
      ready_candidate_cursor_id bigint NULL,
      CHECK (unfinished_count >= 0),
      CHECK ((ready_candidate_cursor_at IS NULL) = (ready_candidate_cursor_id IS NULL))
    )
    """)

    Repo.query!("""
    CREATE INDEX docket_claim_schedule_unfinished_ring_index
    ON #{schedule} (ring_position)
    INCLUDE (
      scope_key, unfinished_count,
      ready_candidate_cursor_at, ready_candidate_cursor_id
    )
    WHERE unfinished_count > 0
    """)

    Repo.query!("""
    CREATE TABLE #{schedule_sparse} (
      scope_key text PRIMARY KEY,
      ring_position bigint GENERATED ALWAYS AS IDENTITY NOT NULL UNIQUE,
      unfinished_count bigint NOT NULL,
      ready_candidate_cursor_at timestamptz NULL,
      ready_candidate_cursor_id bigint NULL,
      CHECK (unfinished_count >= 0),
      CHECK ((ready_candidate_cursor_at IS NULL) = (ready_candidate_cursor_id IS NULL))
    )
    """)

    Repo.query!("""
    CREATE INDEX docket_claim_schedule_sparse_unfinished_ring_index
    ON #{schedule_sparse} (ring_position)
    INCLUDE (
      scope_key, unfinished_count,
      ready_candidate_cursor_at, ready_candidate_cursor_id
    )
    WHERE unfinished_count > 0
    """)

    Repo.query!("""
    CREATE TABLE #{schedule_small} (
      scope_key text PRIMARY KEY,
      ring_position bigint GENERATED ALWAYS AS IDENTITY NOT NULL UNIQUE,
      unfinished_count bigint NOT NULL,
      ready_candidate_cursor_at timestamptz NULL,
      ready_candidate_cursor_id bigint NULL,
      CHECK (unfinished_count >= 0),
      CHECK ((ready_candidate_cursor_at IS NULL) = (ready_candidate_cursor_id IS NULL))
    )
    """)

    Repo.query!("""
    CREATE INDEX docket_claim_schedule_small_unfinished_ring_index
    ON #{schedule_small} (ring_position)
    INCLUDE (
      scope_key, unfinished_count,
      ready_candidate_cursor_at, ready_candidate_cursor_id
    )
    WHERE unfinished_count > 0
    """)

    Repo.query!("""
    CREATE TABLE #{partitions} (
      scope_key text PRIMARY KEY,
      max_active integer NULL,
      partition_version bigint NOT NULL DEFAULT 0,
      admission_epoch bigint NOT NULL DEFAULT 0
    )
    """)

    Repo.query!("""
    CREATE TABLE #{policy} (
      id smallint PRIMARY KEY,
      scan_ring_position bigint NOT NULL
    )
    """)

    Repo.query!("""
    CREATE TABLE #{runs} (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      scope_key text NOT NULL,
      status text NOT NULL,
      poisoned_at timestamptz NULL,
      claim_token uuid NULL,
      wake_at timestamptz NULL,
      claimed_at timestamptz NULL,
      claim_attempts integer NOT NULL DEFAULT 0
    )
    """)

    Repo.query!("""
    CREATE INDEX docket_runs_scope_ready_index
    ON #{runs} (scope_key, wake_at, id)
    WHERE status = 'running' AND poisoned_at IS NULL AND
          claim_token IS NULL AND wake_at IS NOT NULL
    """)

    Repo.query!("""
    CREATE INDEX docket_runs_scope_expired_index
    ON #{runs} (scope_key, claimed_at, id)
    WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL
    """)

    %{
      schedule: schedule,
      schedule_sparse: schedule_sparse,
      schedule_small: schedule_small,
      runs: runs,
      partitions: partitions,
      policy: policy
    }
  end

  defp seed_fixture!(tables) do
    Repo.query!(
      """
      INSERT INTO #{tables.schedule}
        (scope_key, unfinished_count)
      SELECT 'tenant-' || lpad(series::text, 5, '0'), 1
      FROM generate_series(1, $1) AS series
      """,
      [@partitions]
    )

    Repo.query!("""
    INSERT INTO #{tables.schedule}
      (scope_key, unfinished_count)
    VALUES ('hot', #{@deep_ready})
    """)

    Repo.query!("""
    INSERT INTO #{tables.schedule_sparse} (scope_key, unfinished_count)
    SELECT 'tenant-' || lpad(series::text, 5, '0'),
           CASE WHEN series % 250 = 0 OR series % 400 = 0 THEN 1 ELSE 0 END
    FROM generate_series(1, #{@partitions}) AS series
    """)

    Repo.query!(
      "INSERT INTO #{tables.schedule_small} (scope_key, unfinished_count) VALUES ('only', 1)"
    )

    Repo.query!("""
    INSERT INTO #{tables.partitions} (scope_key)
    SELECT 'tenant-' || lpad(series::text, 5, '0')
    FROM generate_series(1, #{@partitions}) AS series
    """)

    Repo.query!("INSERT INTO #{tables.policy} VALUES (1, 0)")

    Repo.query!(
      """
      INSERT INTO #{tables.runs}
        (scope_key, status, wake_at, claim_attempts)
      SELECT 'hot', 'running',
             CURRENT_TIMESTAMP - interval '1 day' + series * interval '1 microsecond',
             series % 8
      FROM generate_series(1, $1) AS series
      """,
      [@deep_ready]
    )

    Repo.query!(
      """
      INSERT INTO #{tables.runs}
        (scope_key, status, wake_at, claim_attempts)
      SELECT 'tenant-' || lpad(series::text, 5, '0'), 'running',
             CURRENT_TIMESTAMP - interval '1 minute', series % 8
      FROM generate_series(1, $1) AS series
      """,
      [@one_row_tenants]
    )

    Repo.query!(
      """
      INSERT INTO #{tables.runs}
        (scope_key, status, wake_at, claim_attempts)
      SELECT 'tenant-' || lpad(((series % $2) + 1)::text, 5, '0'), 'running',
             CURRENT_TIMESTAMP + interval '1 day' + series * interval '1 microsecond',
             series % 8
      FROM generate_series(1, $1) AS series
      """,
      [@future_timers, @partitions]
    )

    Repo.query!(
      """
      INSERT INTO #{tables.runs}
        (scope_key, status, claim_token, claimed_at, claim_attempts)
      SELECT 'tenant-' || lpad(series::text, 5, '0'), 'running',
             ('00000000-0000-0000-0000-' || lpad(series::text, 12, '0'))::uuid,
             CURRENT_TIMESTAMP - interval '2 hours', series % 8
      FROM generate_series(1, $1) AS series
      """,
      [@expired_rows]
    )
  end

  defp collect_report!(tables) do
    now = DateTime.utc_now()
    cutoff = DateTime.add(now, -3_600, :second)

    [[ready_cursor_id, ready_cursor_at]] =
      Repo.query!("""
      SELECT id, wake_at
      FROM #{tables.runs}
      WHERE scope_key = 'hot' AND claim_token IS NULL
      ORDER BY wake_at, id
      OFFSET #{@deep_ready - 6} LIMIT 1
      """).rows

    rejected_global = """
    WITH eligible AS MATERIALIZED (
      SELECT scope_key, wake_at AS eligible_at
      FROM #{tables.runs}
      WHERE status = 'running' AND poisoned_at IS NULL AND
            claim_token IS NULL AND wake_at IS NOT NULL AND wake_at <= $1
      UNION ALL
      SELECT scope_key, claimed_at AS eligible_at
      FROM #{tables.runs}
      WHERE status = 'running' AND poisoned_at IS NULL AND
            claim_token IS NOT NULL AND claimed_at < $2
    )
    SELECT scope_key, min(eligible_at) AS eligible_at
    FROM eligible
    GROUP BY scope_key
    ORDER BY eligible_at, scope_key
    LIMIT 32
    """

    rejected_rank_before_lock = """
    WITH ranked AS MATERIALIZED (
      SELECT id, scope_key, wake_at,
             row_number() OVER (ORDER BY wake_at, id) AS global_rank
      FROM #{tables.runs}
      WHERE status = 'running' AND poisoned_at IS NULL AND
            claim_token IS NULL AND wake_at IS NOT NULL AND wake_at <= $1
    )
    SELECT id, scope_key, wake_at
    FROM ranked
    WHERE global_rank <= 16
    ORDER BY global_rank
    """

    plans = %{
      rejected_global_grouping: explain(rejected_global, [now, cutoff]),
      rejected_rank_before_lock: explain(rejected_rank_before_lock, [now]),
      selected_ring_scan: explain(QueryShapes.scan_positions(tables.schedule), [0]),
      selected_sparse_ring_scan: explain(QueryShapes.scan_positions(tables.schedule_sparse), [0]),
      selected_ring_scan_h_lt_s:
        explain_with_index_bias(QueryShapes.scan_positions(tables.schedule_small), [0]),
      selected_ready_candidates:
        explain(QueryShapes.run_candidates(tables.runs, :ready), ["hot", now]),
      selected_ready_candidate_continuation:
        explain(
          QueryShapes.rotating_run_candidates(tables.runs, :ready),
          ["hot", now, ready_cursor_at, ready_cursor_id]
        ),
      selected_expired_candidates:
        explain(
          QueryShapes.run_candidates(tables.runs, :expired),
          ["tenant-00001", cutoff]
        ),
      selected_scan_cursor_lock: explain(QueryShapes.scan_cursor_lock(tables.policy), []),
      selected_mutation_ids: explain(QueryShapes.mutation_ids(), [Enum.to_list(1..100)])
    }

    locked_plan = locked_prefix_plan!(tables.runs)
    plans = Map.put(plans, :selected_exact_lock_attempts_locked_prefix, locked_plan)
    partition_locked_plan = locked_partition_plan!(tables.partitions)
    plans = Map.put(plans, :selected_exact_partition_lock_locked, partition_locked_plan)

    summaries = Map.new(plans, fn {name, plan} -> {name, summarize(plan)} end)
    assert_evidence!(summaries)

    [[server_version_num]] = Repo.query!("SHOW server_version_num").rows
    [[server_version]] = Repo.query!("SHOW server_version").rows

    %{
      ticket: "DCKT-76",
      generated_at: DateTime.utc_now(),
      postgres: %{
        server_version_num: String.to_integer(server_version_num),
        server_version: server_version
      },
      fixtures: %{
        partitions: @partitions,
        unfinished_ring_positions: @partitions + 1,
        sparse_unfinished_ring_positions: 120,
        deep_ready_rows: @deep_ready,
        one_row_tenants: @one_row_tenants,
        future_timer_rows: @future_timers,
        expired_rows: @expired_rows,
        target_scan_positions_per_call: Budgets.scan_inspections()
      },
      budgets: Budgets.as_map(),
      decisions: %{
        rejected_global_grouping: "work grows with eligible run and tenant populations",
        rejected_rank_before_lock: "global window ranks before partition authority",
        selected_ring_scan: "fixed recursive keyset seeks, including repeated wrap when H < S",
        selected_candidates:
          "fixed exact-partition ready and expired structural prefixes before bounded ranking"
      },
      assertions: %{
        query_plan_evidence_is_not_fairness_proof: true,
        schema_v3_requires_dckt_78_79_for_shipped_proof: true,
        selected_shapes_pass_fixed_logical_work_budgets: true
      },
      summaries: summaries,
      explain_analyze_buffers_format_json: plans
    }
  end

  defp locked_prefix_plan!(runs) do
    parent = self()

    blocker =
      Task.async(fn ->
        Repo.transaction(fn ->
          Repo.query!("SELECT id FROM #{runs} WHERE id = 1 FOR UPDATE")
          send(parent, {:dckt76_locked, self()})

          receive do
            :release -> :released
          after
            10_000 -> raise "timed out waiting to release evidence lock"
          end
        end)
      end)

    assert_receive!({:dckt76_locked, blocker.pid})

    try do
      explain(QueryShapes.exact_run_lock_attempts(runs), [Enum.to_list(1..100)])
    after
      send(blocker.pid, :release)
      {:ok, :released} = Task.await(blocker, 5_000)
    end
  end

  defp locked_partition_plan!(partitions) do
    parent = self()
    scope_key = "tenant-00001"

    blocker =
      Task.async(fn ->
        Repo.transaction(fn ->
          Repo.query!("SELECT scope_key FROM #{partitions} WHERE scope_key = $1 FOR UPDATE", [
            scope_key
          ])

          send(parent, {:dckt76_partition_locked, self()})

          receive do
            :release -> :released
          after
            10_000 -> raise "timed out waiting to release evidence partition lock"
          end
        end)
      end)

    assert_receive!({:dckt76_partition_locked, blocker.pid})

    try do
      explain(QueryShapes.partition_lock_attempt(partitions), [scope_key])
    after
      send(blocker.pid, :release)
      {:ok, :released} = Task.await(blocker, 5_000)
    end
  end

  defp explain(statement, params) do
    [[document]] =
      Repo.query!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " <> statement,
        params,
        timeout: 120_000
      ).rows

    document
  end

  defp explain_with_index_bias(statement, params) do
    Repo.checkout(fn ->
      Repo.query!("SET enable_seqscan = off")

      try do
        explain(statement, params)
      after
        Repo.query!("SET enable_seqscan = on")
      end
    end)
  end

  defp summarize([%{"Plan" => root}] = document) do
    nodes = flatten_nodes(root)

    %{
      actual_rows: root["Actual Rows"],
      actual_loops: root["Actual Loops"],
      planning_time_ms: hd(document)["Planning Time"],
      execution_time_ms: hd(document)["Execution Time"],
      node_count: length(nodes),
      index_tuple_visits:
        nodes
        |> Enum.filter(&(Map.get(&1, "Node Type", "") in ["Index Scan", "Index Only Scan"]))
        |> Enum.map(&(Map.get(&1, "Actual Rows", 0) * Map.get(&1, "Actual Loops", 0)))
        |> Enum.sum(),
      rows_removed_by_filter:
        nodes
        |> Enum.map(&Map.get(&1, "Rows Removed by Filter", 0))
        |> Enum.sum(),
      nodes:
        Enum.map(nodes, fn node ->
          %{
            node_type: node["Node Type"],
            relation: node["Relation Name"],
            index: node["Index Name"],
            actual_rows: node["Actual Rows"],
            actual_loops: node["Actual Loops"],
            rows_removed_by_filter: Map.get(node, "Rows Removed by Filter", 0),
            shared_hit_blocks: Map.get(node, "Shared Hit Blocks", 0),
            shared_read_blocks: Map.get(node, "Shared Read Blocks", 0)
          }
        end)
    }
  end

  defp flatten_nodes(node) do
    [node | Enum.flat_map(Map.get(node, "Plans", []), &flatten_nodes/1)]
  end

  defp assert_evidence!(summaries) do
    expected_rows = %{
      selected_ring_scan: Budgets.scan_inspections(),
      selected_sparse_ring_scan: Budgets.scan_inspections(),
      selected_ring_scan_h_lt_s: Budgets.scan_inspections(),
      selected_ready_candidates: Budgets.run_lock_attempts(),
      selected_ready_candidate_continuation: Budgets.run_lock_attempts(),
      selected_expired_candidates: 1,
      selected_scan_cursor_lock: 1,
      selected_exact_partition_lock_locked: 0,
      selected_exact_lock_attempts_locked_prefix: Budgets.run_lock_attempts() - 1,
      selected_mutation_ids: Budgets.grant_outcomes()
    }

    Enum.each(expected_rows, fn {name, expected} ->
      actual = get_in(summaries, [name, :actual_rows])

      if actual != expected do
        raise "#{name} returned #{actual} rows; fixed budget expected #{expected}"
      end
    end)

    selected =
      Map.drop(summaries, [:rejected_global_grouping, :rejected_rank_before_lock])

    Enum.each(selected, fn {name, summary} ->
      if Enum.any?(summary.nodes, &(&1.node_type in ["Seq Scan", "Bitmap Heap Scan"])) do
        raise "#{name} used an unbounded population scan"
      end

      scan_filter_removals =
        summary.nodes
        |> Enum.filter(&(&1.node_type in ["Index Scan", "Index Only Scan"]))
        |> Enum.map(& &1.rows_removed_by_filter)
        |> Enum.sum()

      if scan_filter_removals != 0 do
        raise "#{name} removed #{scan_filter_removals} rows inside selected index scans"
      end
    end)

    required_indexes = %{
      selected_ring_scan: "docket_claim_schedule_unfinished_ring_index",
      selected_sparse_ring_scan: "docket_claim_schedule_sparse_unfinished_ring_index",
      selected_ring_scan_h_lt_s: "docket_claim_schedule_small_unfinished_ring_index",
      selected_ready_candidates: "docket_runs_scope_ready_index",
      selected_ready_candidate_continuation: "docket_runs_scope_ready_index",
      selected_expired_candidates: "docket_runs_scope_expired_index",
      selected_scan_cursor_lock: "docket_claim_policy_pkey",
      selected_exact_partition_lock_locked: "docket_claim_partitions_pkey",
      selected_exact_lock_attempts_locked_prefix: "docket_runs_pkey"
    }

    Enum.each(required_indexes, fn {name, index} ->
      unless Enum.any?(summaries[name].nodes, &(&1.index == index)) do
        raise "#{name} did not use required index #{index}"
      end
    end)

    continuation_scan_count =
      summaries.selected_ready_candidate_continuation.nodes
      |> Enum.count(fn node ->
        node.index == "docket_runs_scope_ready_index" and node.actual_rows > 0 and
          node.actual_loops > 0
      end)

    if continuation_scan_count < 2 do
      raise "selected ready continuation did not execute both keyset halves"
    end

    index_visit_ceilings = %{
      selected_ring_scan: 2 * Budgets.scan_inspections(),
      selected_sparse_ring_scan: 2 * Budgets.scan_inspections(),
      selected_ring_scan_h_lt_s: 2 * Budgets.scan_inspections(),
      selected_ready_candidates: Budgets.run_lock_attempts(),
      selected_ready_candidate_continuation: 2 * Budgets.run_lock_attempts(),
      selected_expired_candidates: Budgets.run_lock_attempts(),
      selected_scan_cursor_lock: 1,
      selected_exact_partition_lock_locked: 1,
      selected_exact_lock_attempts_locked_prefix: Budgets.run_lock_attempts()
    }

    Enum.each(index_visit_ceilings, fn {name, ceiling} ->
      actual = summaries[name].index_tuple_visits

      if actual > ceiling do
        raise "#{name} visited #{actual} index tuples; logical ceiling is #{ceiling}"
      end
    end)

    recursive_row_ceilings = %{
      selected_ring_scan: Budgets.scan_inspections(),
      selected_sparse_ring_scan: Budgets.scan_inspections(),
      selected_ring_scan_h_lt_s: Budgets.scan_inspections()
    }

    Enum.each(recursive_row_ceilings, fn {name, ceiling} ->
      for node <- summaries[name].nodes, node.node_type == "Recursive Union" do
        if node.actual_rows > ceiling or node.actual_loops > 1 do
          raise "#{name} recursive work exceeded #{ceiling} rows in one loop"
        end
      end
    end)
  end

  defp assert_receive!(message) do
    receive do
      ^message -> :ok
    after
      5_000 -> raise "timed out waiting for #{inspect(message)}"
    end
  end

  defp table(prefix, name), do: quote_identifier(prefix) <> "." <> quote_identifier(name)

  defp quote_identifier(value), do: ~s("#{String.replace(value, "\"", "\"\"")}")
end

Docket.Bench.DCKT76QueryPlans.run(System.argv())
