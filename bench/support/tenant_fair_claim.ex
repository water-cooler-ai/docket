Postgrex.Types.define(Docket.Bench.TenantFairClaim.Types, [], json: JSON)

defmodule Docket.Bench.TenantFairClaim.Repo do
  @moduledoc false
  use Ecto.Repo, otp_app: :docket, adapter: Ecto.Adapters.Postgres
end

defmodule Docket.Bench.TenantFairClaim.InstallDocket do
  @moduledoc false
  use Ecto.Migration

  def up do
    prefix = Application.fetch_env!(:docket, :tenant_fair_claim_bench_prefix)
    Docket.Postgres.Migration.up(prefix: prefix, create_schema: false)
  end

  def down do
    prefix = Application.fetch_env!(:docket, :tenant_fair_claim_bench_prefix)
    Docket.Postgres.Migration.down(prefix: prefix)
  end
end

defmodule Docket.Bench.TenantFairClaim.Config do
  @moduledoc false

  @fixed_now ~U[2026-07-15 12:00:00.000000Z]
  @profiles %{
    "smoke" => %{
      queued_rows: 160,
      tenants: 24,
      dormant_tenants: 120,
      hot_rows: 80,
      one_row_tenants: 16,
      capped_tenants: 4,
      active_per_capped_tenant: 2,
      expired_percent: 25,
      poison_percent: 5,
      demand: 4,
      workers: 2,
      iterations: 3,
      warmup: 1,
      page_size: 4,
      oversampling: 2,
      reconciliation_budget: 64,
      seed: 62,
      statement_timeout_ms: 15_000
    },
    "local" => %{
      queued_rows: 100_000,
      tenants: 10_000,
      dormant_tenants: 50_000,
      hot_rows: 30_000,
      one_row_tenants: 8_000,
      capped_tenants: 1_000,
      active_per_capped_tenant: 2,
      expired_percent: 25,
      poison_percent: 2,
      demand: 50,
      workers: 8,
      iterations: 30,
      warmup: 3,
      page_size: 128,
      oversampling: 4,
      reconciliation_budget: 512,
      seed: 62,
      statement_timeout_ms: 60_000
    },
    "scale" => %{
      queued_rows: 1_000_000,
      tenants: 100_000,
      dormant_tenants: 1_000_000,
      hot_rows: 250_000,
      one_row_tenants: 80_000,
      capped_tenants: 10_000,
      active_per_capped_tenant: 4,
      expired_percent: 25,
      poison_percent: 2,
      demand: 100,
      workers: 16,
      iterations: 100,
      warmup: 5,
      page_size: 256,
      oversampling: 4,
      reconciliation_budget: 2_048,
      seed: 62,
      statement_timeout_ms: 120_000
    }
  }

  @switches [
    profile: :string,
    queued_rows: :integer,
    tenants: :integer,
    dormant_tenants: :integer,
    hot_rows: :integer,
    one_row_tenants: :integer,
    capped_tenants: :integer,
    active_per_capped_tenant: :integer,
    expired_percent: :integer,
    poison_percent: :integer,
    demand: :integer,
    workers: :integer,
    iterations: :integer,
    warmup: :integer,
    page_size: :integer,
    oversampling: :integer,
    reconciliation_budget: :integer,
    seed: :integer,
    statement_timeout_ms: :integer,
    output: :string,
    check: :boolean,
    keep_schema: :boolean,
    help: :boolean
  ]

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
      Enum.reduce(opts, base, fn
        {key, _value}, acc when key in [:profile, :output, :check, :keep_schema, :help] -> acc
        {key, value}, acc -> Map.put(acc, key, value)
      end)
      |> Map.merge(%{
        profile: profile,
        output: Keyword.get(opts, :output),
        check: Keyword.get(opts, :check, false),
        keep_schema: Keyword.get(opts, :keep_schema, false),
        fixed_now: @fixed_now
      })

    validate!(config)
  rescue
    KeyError ->
      raise ArgumentError,
            "unknown profile; expected one of #{inspect(Map.keys(@profiles))}\n\n#{usage()}"
  end

  def usage do
    """
    Usage:
      DOCKET_BENCH_DATABASE_URL=postgres://... \\
        mix run bench/postgres/tenant_fair_claim.exs -- [options]

    Options:
      --profile smoke|local|scale
      --queued-rows N              independently configured eligible rows
      --tenants N                  independently configured active partitions
      --dormant-tenants N          partition rows without queued work
      --hot-rows N                 queued rows assigned to the hot tenant
      --one-row-tenants N          tenants forced to have one queued row first
      --capped-tenants N           tenants seeded at max_active
      --active-per-capped-tenant N live claims on each capped tenant
      --expired-percent N          deterministic expired-class bucket rate
      --poison-percent N           deterministic max-attempt bucket rate
      --demand N --workers N --iterations N --warmup N
      --page-size N --oversampling N --reconciliation-budget N
      --seed N --statement-timeout-ms N --output PATH
      --check                      fail on deterministic smoke invariants
      --keep-schema                retain the generated scratch schema
    """
  end

  defp validate!(config) do
    positive = [
      :queued_rows,
      :tenants,
      :hot_rows,
      :demand,
      :workers,
      :iterations,
      :page_size,
      :oversampling,
      :reconciliation_budget,
      :active_per_capped_tenant,
      :statement_timeout_ms
    ]

    Enum.each(positive, fn key ->
      unless is_integer(config[key]) and config[key] > 0 do
        raise ArgumentError, "#{key} must be a positive integer, got: #{inspect(config[key])}"
      end
    end)

    Enum.each(
      [:dormant_tenants, :one_row_tenants, :capped_tenants, :warmup, :seed],
      fn key ->
        unless is_integer(config[key]) and config[key] >= 0 do
          raise ArgumentError,
                "#{key} must be a non-negative integer, got: #{inspect(config[key])}"
        end
      end
    )

    Enum.each([:expired_percent, :poison_percent], fn key ->
      unless is_integer(config[key]) and config[key] in 0..100 do
        raise ArgumentError, "#{key} must be an integer in 0..100"
      end
    end)

    cond do
      config.hot_rows > config.queued_rows ->
        raise ArgumentError, "hot_rows cannot exceed queued_rows"

      config.hot_rows + config.one_row_tenants > config.queued_rows ->
        raise ArgumentError, "hot_rows + one_row_tenants cannot exceed queued_rows"

      config.queued_rows - config.hot_rows < config.tenants - 1 ->
        raise ArgumentError,
              "queued_rows must realize the hot tenant plus at least one row for every other tenant"

      config.one_row_tenants > max(config.tenants - 1, 0) ->
        raise ArgumentError, "one_row_tenants cannot exceed tenants - 1"

      config.queued_rows > config.hot_rows + config.one_row_tenants and
          config.one_row_tenants >= config.tenants - 1 ->
        raise ArgumentError,
              "at least one non-hot, non-one-row tenant is required for remaining queued rows"

      config.capped_tenants > config.tenants ->
        raise ArgumentError, "capped_tenants cannot exceed tenants"

      config.capped_tenants > 0 and config.tenants == 1 and config.hot_rows < 2 ->
        raise ArgumentError, "a capped single-tenant fixture requires at least two hot rows"

      config.capped_tenants > 0 and config.tenants > 1 and
          (config.one_row_tenants >= config.tenants - 1 or
             config.queued_rows - config.hot_rows - config.one_row_tenants <
               2 * (config.tenants - config.one_row_tenants - 1)) ->
        raise ArgumentError,
              "capped fixtures require at least two queued rows on each non-one-row tenant"

      config.expired_percent + config.poison_percent > 100 ->
        raise ArgumentError, "expired_percent + poison_percent cannot exceed 100"

      config.page_size * config.oversampling < config.demand ->
        raise ArgumentError, "page_size * oversampling must be at least demand"

      config.reconciliation_budget < config.demand ->
        raise ArgumentError, "reconciliation_budget must be at least demand"

      rem(config.reconciliation_budget, config.page_size * config.oversampling) != 0 ->
        raise ArgumentError,
              "reconciliation_budget must be divisible by page_size * oversampling"

      config.queued_rows < config.demand * config.workers * config.iterations ->
        raise ArgumentError,
              "queued_rows must cover demand * workers * iterations for committed samples"

      true ->
        config
    end
  end
end

defmodule Docket.Bench.TenantFairClaim.SQL do
  @moduledoc false

  @candidates [:ranking_window, :distinct_on, :hint_cursor, :recursive_loose_scan]
  def candidates, do: @candidates
  def role(candidate) when candidate in [:ranking_window, :distinct_on], do: "failure_baseline"
  def role(:hint_cursor), do: "admission_prototype"
  def role(:recursive_loose_scan), do: "reconciliation_prototype"

  def statement(candidate, prefix) do
    runs = table(prefix, "docket_runs")
    partitions = table(prefix, "docket_bench_claim_partitions")
    discovery = discovery(candidate, runs, partitions)

    """
    WITH #{discovery},
    class_ranked AS MATERIALIZED (
      SELECT candidate_heads.*,
             ROW_NUMBER() OVER (PARTITION BY class ORDER BY eligible_at, id) AS class_rank
      FROM candidate_heads
    ),
    ordered_candidates AS MATERIALIZED (
      SELECT id, scope_key, class, eligible_at,
             ROW_NUMBER() OVER (
               ORDER BY
                 CASE WHEN $3 >= 2 AND class_rank = 1 THEN 0 ELSE 1 END,
                 eligible_at,
                 id
             ) AS admission_ordinal
      FROM class_ranked
      ORDER BY
        CASE WHEN $3 >= 2 AND class_rank = 1 THEN 0 ELSE 1 END,
        eligible_at,
        id
      #{candidate_limit(candidate)}
    ),
    locked AS MATERIALIZED (
      SELECT runs.id, ordered_candidates.scope_key,
             ordered_candidates.class, ordered_candidates.eligible_at,
             ordered_candidates.admission_ordinal
      FROM #{runs} AS runs
      JOIN ordered_candidates ON ordered_candidates.id = runs.id
      WHERE runs.status = 'running'
        AND runs.poisoned_at IS NULL
        AND (
          (ordered_candidates.class = 'ready' AND runs.claim_token IS NULL AND runs.wake_at <= $1)
          OR
          (ordered_candidates.class = 'expired' AND runs.claim_token IS NOT NULL AND runs.claimed_at < $2)
        )
      ORDER BY ordered_candidates.admission_ordinal, runs.id
      LIMIT $3
      FOR UPDATE OF runs SKIP LOCKED
    ),
    updated AS (
      UPDATE #{runs} AS runs
      SET claim_token = CASE WHEN runs.claim_attempts < $4 THEN gen_random_uuid() ELSE NULL END,
          claimed_at = CASE WHEN runs.claim_attempts < $4 THEN $1 ELSE NULL END,
          wake_at = NULL,
          claim_attempts = CASE
            WHEN runs.claim_attempts < $4 THEN runs.claim_attempts + 1
            ELSE runs.claim_attempts
          END,
          poisoned_at = CASE WHEN runs.claim_attempts < $4 THEN NULL ELSE $1 END,
          poison_reason = CASE
            WHEN runs.claim_attempts < $4 THEN NULL
            ELSE 'max_claim_attempts_exceeded'
          END
      FROM locked
      WHERE runs.id = locked.id
      RETURNING runs.id, runs.run_id, runs.scope_key, locked.class, locked.eligible_at,
                runs.claim_token, runs.poisoned_at
    ),
    #{rotation_advance(candidate, partitions)}
    SELECT id, run_id, scope_key, class, eligible_at,
           (SELECT count(*) FROM advanced_partitions) AS advanced_partitions,
           #{next_cursor(candidate)} AS next_cursor,
           CASE WHEN claim_token IS NULL THEN 'poisoned' ELSE 'leased' END AS outcome
    FROM updated
    UNION ALL
    SELECT NULL::bigint, NULL::text, NULL::text, NULL::text, NULL::timestamptz,
           (SELECT count(*) FROM advanced_partitions),
           #{next_cursor(candidate)},
           'page_metadata'::text
    WHERE NOT EXISTS (SELECT 1 FROM updated)
    ORDER BY id NULLS LAST
    """
  end

  def params(candidate, config, cursor \\ "", demand \\ nil, budget \\ nil) do
    cutoff = DateTime.add(config.fixed_now, -60_000, :millisecond)
    demand = demand || config.demand
    budget = budget || config.reconciliation_budget

    params = [
      config.fixed_now,
      cutoff,
      demand,
      3,
      config.page_size,
      config.oversampling,
      budget,
      cursor
    ]

    case candidate do
      candidate when candidate in [:ranking_window, :distinct_on] -> Enum.take(params, 4)
      :recursive_loose_scan -> Enum.take(params, 7)
      :hint_cursor -> Enum.take(params, 6) ++ [cursor]
    end
  end

  def audit_params(config), do: Enum.take(params(:hint_cursor, config), 3)

  def query_hash(candidate, prefix) do
    :crypto.hash(:sha256, statement(candidate, prefix)) |> Base.encode16(case: :lower)
  end

  def provisional_ddl(prefix) do
    runs = table(prefix, "docket_runs")
    partitions = table(prefix, "docket_bench_claim_partitions")

    [
      """
      CREATE TABLE #{partitions} (
        scope_key text PRIMARY KEY,
        preferred_active integer NOT NULL DEFAULT 2 CHECK (preferred_active >= 0),
        max_active integer NOT NULL DEFAULT 2 CHECK (max_active >= preferred_active),
        weight integer NOT NULL DEFAULT 1 CHECK (weight > 0),
        borrowing boolean NOT NULL DEFAULT false,
        last_claimed_at timestamptz,
        next_ready_at_hint timestamptz,
        next_expired_at_hint timestamptz
      )
      """,
      """
      CREATE INDEX docket_bench_partitions_cursor_index
      ON #{partitions} (scope_key)
      WHERE next_ready_at_hint IS NOT NULL OR next_expired_at_hint IS NOT NULL
      """,
      """
      CREATE INDEX docket_bench_runs_ready_tenant_index
      ON #{runs} (scope_key, wake_at, id)
      WHERE status = 'running' AND poisoned_at IS NULL
        AND claim_token IS NULL AND wake_at IS NOT NULL
      """,
      """
      CREATE INDEX docket_bench_runs_claimed_tenant_index
      ON #{runs} (scope_key, claimed_at, id)
      WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL
      """
    ]
  end

  def ddl_hash(prefix) do
    provisional_ddl(prefix)
    |> Enum.join("\n")
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  def table(prefix, name), do: quote_identifier(prefix) <> "." <> quote_identifier(name)
  def quote_identifier(value), do: ~s("#{String.replace(value, "\"", "\"\"")}")

  defp discovery(:ranking_window, runs, partitions) do
    """
    eligible AS MATERIALIZED (
      #{eligible_union(runs, partitions)}
    ),
    ranked AS MATERIALIZED (
      SELECT eligible.*,
             ROW_NUMBER() OVER (
               PARTITION BY scope_key
               ORDER BY eligible_at, id
             ) AS partition_rank
      FROM eligible
    ),
    candidate_heads AS MATERIALIZED (
      SELECT id, scope_key, class, eligible_at
      FROM ranked
      WHERE partition_rank = 1
      ORDER BY eligible_at, id
      LIMIT $3
    )
    """
  end

  defp discovery(:distinct_on, runs, partitions) do
    """
    eligible AS MATERIALIZED (
      #{eligible_union(runs, partitions)}
    ),
    distinct_heads AS MATERIALIZED (
      SELECT DISTINCT ON (scope_key) id, scope_key, class, eligible_at
      FROM eligible
      ORDER BY scope_key, eligible_at, id
    ),
    candidate_heads AS MATERIALIZED (
      SELECT id, scope_key, class, eligible_at
      FROM distinct_heads
      ORDER BY eligible_at, id
      LIMIT $3
    )
    """
  end

  defp discovery(:hint_cursor, runs, partitions) do
    """
    partition_page AS MATERIALIZED (
      SELECT p.scope_key, p.last_claimed_at, p.max_active
      FROM #{partitions} AS p
      WHERE p.scope_key > $7
        AND (p.next_ready_at_hint <= $1 OR p.next_expired_at_hint < $2)
      ORDER BY p.scope_key
      LIMIT ($5::bigint * $6::bigint)
    ),
    locked_partitions AS MATERIALIZED (
      SELECT p.scope_key, p.last_claimed_at, p.max_active
      FROM #{partitions} AS p
      JOIN partition_page page ON page.scope_key = p.scope_key
      WHERE #{eligible_partition(runs, "p")}
      ORDER BY p.scope_key
      LIMIT $3
      FOR NO KEY UPDATE OF p SKIP LOCKED
    ),
    candidate_heads AS MATERIALIZED (
      SELECT head.id, p.scope_key, head.class, head.eligible_at
      FROM locked_partitions AS p
      CROSS JOIN LATERAL (
        SELECT eligible.id, eligible.class, eligible.eligible_at
        FROM (
          (SELECT ready.id, 'ready'::text AS class, ready.wake_at AS eligible_at
          FROM #{runs} AS ready
          WHERE ready.scope_key = p.scope_key
            AND ready.status = 'running' AND ready.poisoned_at IS NULL
            AND ready.claim_token IS NULL AND ready.wake_at <= $1
            AND (
              SELECT count(*)
              FROM #{runs} AS active
              WHERE active.scope_key = p.scope_key
                AND active.status = 'running' AND active.poisoned_at IS NULL
                AND active.claim_token IS NOT NULL
            ) < p.max_active
          ORDER BY ready.wake_at, ready.id
          LIMIT LEAST(
            $6::bigint,
            GREATEST(
              p.max_active - (
                SELECT count(*)
                FROM #{runs} AS active
                WHERE active.scope_key = p.scope_key
                  AND active.status = 'running' AND active.poisoned_at IS NULL
                  AND active.claim_token IS NOT NULL
              ),
              0
            )
          ))
          UNION ALL
          (SELECT expired.id, 'expired'::text AS class, expired.claimed_at AS eligible_at
          FROM #{runs} AS expired
          WHERE expired.scope_key = p.scope_key
            AND expired.status = 'running' AND expired.poisoned_at IS NULL
            AND expired.claim_token IS NOT NULL AND expired.claimed_at < $2
          ORDER BY expired.claimed_at, expired.id
          LIMIT $6)
        ) AS eligible
        ORDER BY eligible_at, id
        LIMIT $6
      ) AS head
      ORDER BY p.last_claimed_at ASC NULLS FIRST, head.eligible_at, p.scope_key
    )
    """
  end

  defp discovery(:recursive_loose_scan, runs, partitions) do
    """
    RECURSIVE eligible_keys(scope_key, ordinal) AS (
      (
        SELECT scope_key, 1
        FROM #{runs}
        WHERE status = 'running' AND poisoned_at IS NULL
          AND $5::bigint >= 0 AND $6::bigint >= 0
          AND ((claim_token IS NULL AND wake_at <= $1)
            OR (claim_token IS NOT NULL AND claimed_at < $2))
        ORDER BY scope_key
        LIMIT 1
      )
      UNION ALL
      SELECT next_key.scope_key, previous.ordinal + 1
      FROM eligible_keys AS previous
      CROSS JOIN LATERAL (
        SELECT scope_key
        FROM #{runs}
        WHERE scope_key > previous.scope_key
          AND status = 'running' AND poisoned_at IS NULL
          AND ((claim_token IS NULL AND wake_at <= $1)
            OR (claim_token IS NOT NULL AND claimed_at < $2))
        ORDER BY scope_key
        LIMIT 1
      ) AS next_key
      WHERE previous.ordinal < $7
    ),
    bounded_keys AS MATERIALIZED (
      SELECT scope_key FROM eligible_keys ORDER BY ordinal LIMIT $7
    ),
    locked_partitions AS MATERIALIZED (
      SELECT p.scope_key, p.last_claimed_at, p.max_active
      FROM #{partitions} AS p
      JOIN bounded_keys keys ON keys.scope_key = p.scope_key
      WHERE #{eligible_partition(runs, "p")}
      ORDER BY p.scope_key
      LIMIT $3
      FOR NO KEY UPDATE OF p SKIP LOCKED
    ),
    candidate_heads AS MATERIALIZED (
      SELECT head.id, p.scope_key, head.class, head.eligible_at
      FROM locked_partitions AS p
      CROSS JOIN LATERAL (
        SELECT eligible.id, eligible.class, eligible.eligible_at
        FROM (
          (SELECT ready.id, 'ready'::text AS class, ready.wake_at AS eligible_at
          FROM #{runs} AS ready
          WHERE ready.scope_key = p.scope_key
            AND ready.status = 'running' AND ready.poisoned_at IS NULL
            AND ready.claim_token IS NULL AND ready.wake_at <= $1
            AND (
              SELECT count(*)
              FROM #{runs} AS active
              WHERE active.scope_key = p.scope_key
                AND active.status = 'running' AND active.poisoned_at IS NULL
                AND active.claim_token IS NOT NULL
            ) < p.max_active
          ORDER BY ready.wake_at, ready.id
          LIMIT LEAST(
            $6::bigint,
            GREATEST(
              p.max_active - (
                SELECT count(*)
                FROM #{runs} AS active
                WHERE active.scope_key = p.scope_key
                  AND active.status = 'running' AND active.poisoned_at IS NULL
                  AND active.claim_token IS NOT NULL
              ),
              0
            )
          ))
          UNION ALL
          (SELECT expired.id, 'expired'::text AS class, expired.claimed_at AS eligible_at
          FROM #{runs} AS expired
          WHERE expired.scope_key = p.scope_key
            AND expired.status = 'running' AND expired.poisoned_at IS NULL
            AND expired.claim_token IS NOT NULL AND expired.claimed_at < $2
          ORDER BY expired.claimed_at, expired.id
          LIMIT $6)
        ) AS eligible
        ORDER BY eligible_at, id
        LIMIT $6
      ) AS head
      ORDER BY p.last_claimed_at ASC NULLS FIRST, head.eligible_at, p.scope_key
    )
    """
  end

  defp eligible_union(runs, partitions) do
    """
    SELECT ready.id, ready.scope_key, 'ready'::text AS class, ready.wake_at AS eligible_at
    FROM #{runs} AS ready
    JOIN #{partitions} AS policy ON policy.scope_key = ready.scope_key
    LEFT JOIN (
      SELECT scope_key, count(*) AS active_count
      FROM #{runs}
      WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL
      GROUP BY scope_key
    ) AS active ON active.scope_key = ready.scope_key
    WHERE ready.status = 'running' AND ready.poisoned_at IS NULL
      AND ready.claim_token IS NULL AND ready.wake_at <= $1
      AND COALESCE(active.active_count, 0) < policy.max_active
    UNION ALL
    SELECT expired.id, expired.scope_key, 'expired'::text AS class,
           expired.claimed_at AS eligible_at
    FROM #{runs} AS expired
    WHERE expired.status = 'running' AND expired.poisoned_at IS NULL
      AND expired.claim_token IS NOT NULL AND expired.claimed_at < $2
    """
  end

  defp eligible_partition(runs, alias_name) do
    """
    (
      EXISTS (
        SELECT 1 FROM #{runs} AS expired
        WHERE expired.scope_key = #{alias_name}.scope_key
          AND expired.status = 'running' AND expired.poisoned_at IS NULL
          AND expired.claim_token IS NOT NULL AND expired.claimed_at < $2
      )
      OR (
        EXISTS (
          SELECT 1 FROM #{runs} AS ready
          WHERE ready.scope_key = #{alias_name}.scope_key
            AND ready.status = 'running' AND ready.poisoned_at IS NULL
            AND ready.claim_token IS NULL AND ready.wake_at <= $1
        )
        AND (
          SELECT count(*) FROM #{runs} AS active
          WHERE active.scope_key = #{alias_name}.scope_key
            AND active.status = 'running' AND active.poisoned_at IS NULL
            AND active.claim_token IS NOT NULL
        ) < #{alias_name}.max_active
      )
    )
    """
  end

  defp candidate_limit(candidate) when candidate in [:ranking_window, :distinct_on],
    do: "LIMIT $3"

  defp candidate_limit(_candidate), do: ""

  defp rotation_advance(candidate, partitions)
       when candidate in [:hint_cursor, :recursive_loose_scan] do
    """
    advanced_partitions AS (
      UPDATE #{partitions} AS partition
      SET last_claimed_at = $1
      FROM (SELECT DISTINCT scope_key FROM updated) AS claimed
      WHERE partition.scope_key = claimed.scope_key
      RETURNING partition.scope_key
    )
    """
  end

  defp rotation_advance(_candidate, _partitions) do
    "advanced_partitions AS (SELECT NULL::text AS scope_key WHERE false)"
  end

  defp next_cursor(:hint_cursor), do: "(SELECT max(scope_key) FROM partition_page)"
  defp next_cursor(_candidate), do: "NULL::text"
end

defmodule Docket.Bench.TenantFairClaim.Schema do
  @moduledoc false

  alias Docket.Bench.TenantFairClaim.{InstallDocket, Repo, SQL}

  @migration_version 20_260_715_000_062

  def create!(prefix) do
    validate_owned_prefix!(prefix)
    Repo.query!("CREATE SCHEMA #{SQL.quote_identifier(prefix)}")
    Application.put_env(:docket, :tenant_fair_claim_bench_prefix, prefix)

    :ok =
      Ecto.Migrator.up(Repo, @migration_version, InstallDocket,
        log: false,
        prefix: prefix
      )

    Enum.each(SQL.provisional_ddl(prefix), &Repo.query!/1)
    :ok
  end

  def drop!(prefix) do
    validate_owned_prefix!(prefix)
    Repo.query!("DROP SCHEMA #{SQL.quote_identifier(prefix)} CASCADE")
    :ok
  end

  def drop_if_exists!(prefix) do
    validate_owned_prefix!(prefix)
    Repo.query!("DROP SCHEMA IF EXISTS #{SQL.quote_identifier(prefix)} CASCADE")
    :ok
  end

  defp validate_owned_prefix!("docket_bench_" <> suffix = prefix) when byte_size(suffix) > 0 do
    if String.match?(prefix, ~r/\Adocket_bench_[a-z0-9_]+\z/) and byte_size(prefix) <= 63 do
      prefix
    else
      raise ArgumentError, "unsafe benchmark schema name: #{inspect(prefix)}"
    end
  end

  defp validate_owned_prefix!(prefix) do
    raise ArgumentError,
          "refusing to manage non-benchmark schema #{inspect(prefix)}; expected docket_bench_*"
  end
end

defmodule Docket.Bench.TenantFairClaim.Seed do
  @moduledoc false

  alias Docket.Bench.TenantFairClaim.{Repo, SQL}

  def reset!(prefix, config) do
    events = SQL.table(prefix, "docket_events")
    runs = SQL.table(prefix, "docket_runs")
    graphs = SQL.table(prefix, "docket_graph_versions")
    partitions = SQL.table(prefix, "docket_bench_claim_partitions")

    Repo.query!("TRUNCATE #{events}, #{runs}, #{graphs}, #{partitions} RESTART IDENTITY CASCADE")

    Repo.query!(
      """
      INSERT INTO #{partitions}
        (scope_key, preferred_active, max_active, weight, borrowing)
      SELECT 'tenant-' || lpad(n::text, 8, '0'),
             LEAST(2, $2),
             CASE WHEN n > $1 - $3 THEN $2 ELSE GREATEST($4, 2) END,
             1 + mod(n + $5, 4),
             false
      FROM generate_series(1, $1) AS n
      """,
      [
        config.tenants,
        config.active_per_capped_tenant,
        config.capped_tenants,
        config.demand,
        config.seed
      ]
    )

    if config.dormant_tenants > 0 do
      Repo.query!(
        """
        INSERT INTO #{partitions}
          (scope_key, preferred_active, max_active, weight, borrowing)
        SELECT 'dormant-' || lpad(n::text, 8, '0'), 2, GREATEST($2, 2), 1, false
        FROM generate_series(1, $1) AS n
        """,
        [config.dormant_tenants, config.demand]
      )
    end

    Repo.query!(
      """
      INSERT INTO #{graphs}
        (tenant_id, graph_id, graph_hash, graph, inserted_at)
      SELECT 'tenant-' || lpad(n::text, 8, '0'), 'bench-graph', 'bench-hash',
             decode('836a', 'hex'), $2
      FROM generate_series(1, $1) AS n
      """,
      [config.tenants, config.fixed_now]
    )

    Repo.query!(queued_insert(runs), [
      config.queued_rows,
      config.hot_rows,
      config.one_row_tenants,
      config.tenants,
      config.seed,
      config.expired_percent,
      config.poison_percent,
      config.fixed_now,
      config.capped_tenants
    ])

    if config.capped_tenants > 0 do
      Repo.query!(capped_insert(runs), [
        config.tenants,
        config.capped_tenants,
        config.active_per_capped_tenant,
        config.fixed_now
      ])
    end

    Repo.query!("""
    UPDATE #{partitions} AS p
    SET next_ready_at_hint = heads.next_ready_at,
        next_expired_at_hint = heads.next_expired_at
    FROM (
      SELECT scope_key,
             min(wake_at) FILTER (
               WHERE claim_token IS NULL AND wake_at IS NOT NULL
             ) AS next_ready_at,
             min(claimed_at) FILTER (
               WHERE claim_token IS NOT NULL
             ) AS next_expired_at
      FROM #{runs}
      WHERE status = 'running' AND poisoned_at IS NULL
      GROUP BY scope_key
    ) AS heads
    WHERE p.scope_key = heads.scope_key
    """)

    Repo.query!("ANALYZE #{runs}")
    Repo.query!("ANALYZE #{partitions}")
    manifest(prefix, config)
  end

  def manifest(prefix, config) do
    runs = SQL.table(prefix, "docket_runs")
    partitions = SQL.table(prefix, "docket_bench_claim_partitions")

    [[queued, active_tenants, ready, expired, poison_boundary, live]] =
      Repo.query!(
        """
        SELECT
          count(*) FILTER (
            WHERE poisoned_at IS NULL AND (
              (claim_token IS NULL AND wake_at IS NOT NULL)
              OR (claim_token IS NOT NULL AND claimed_at < $1)
            )
          ),
          count(DISTINCT scope_key),
          count(*) FILTER (WHERE claim_token IS NULL AND wake_at IS NOT NULL),
          count(*) FILTER (WHERE claim_token IS NOT NULL AND claimed_at < $1),
          count(*) FILTER (WHERE claim_attempts >= 3),
          count(*) FILTER (WHERE claim_token IS NOT NULL)
        FROM #{runs}
        """,
        [~U[2026-07-15 11:59:00.000000Z]]
      ).rows

    [[partition_count, dormant_count]] =
      Repo.query!("""
      SELECT count(*), count(*) FILTER (
        WHERE next_ready_at_hint IS NULL AND next_expired_at_hint IS NULL
      )
      FROM #{partitions}
      """).rows

    [[runs_checksum]] =
      Repo.query!("""
      SELECT md5(count(*)::text || ':' || COALESCE(sum(hashtextextended(
        concat_ws('|', run_id, scope_key, status, claim_token IS NOT NULL,
          claimed_at::text, wake_at::text, claim_attempts, poisoned_at::text),
        62
      )), 0)::text)
      FROM #{runs}
      """).rows

    [[partitions_checksum]] =
      Repo.query!("""
      SELECT md5(count(*)::text || ':' || COALESCE(sum(hashtextextended(
        concat_ws('|', scope_key, preferred_active, max_active, weight, borrowing,
          last_claimed_at::text, next_ready_at_hint::text, next_expired_at_hint::text),
        62
      )), 0)::text)
      FROM #{partitions}
      """).rows

    [[hot_tenant_ready_rows]] =
      Repo.query!("""
      SELECT count(*)
      FROM #{runs}
      WHERE scope_key = 'tenant-00000001'
        AND run_id LIKE 'bench-%'
        AND claim_token IS NULL AND wake_at IS NOT NULL
      """).rows

    [[one_row_tenants]] =
      Repo.query!("""
      SELECT count(*)
      FROM (
        SELECT scope_key
        FROM #{runs}
        WHERE run_id LIKE 'bench-%'
        GROUP BY scope_key
        HAVING count(*) = 1
      ) AS one_row
      """).rows

    [[capped_at_max, capped_over_max]] =
      Repo.query!("""
      WITH active AS (
        SELECT scope_key, count(*) AS active_count,
               bool_or(run_id LIKE 'capped-%') AS seeded_capped
        FROM #{runs}
        WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL
        GROUP BY scope_key
      )
      SELECT count(*) FILTER (WHERE active_count = p.max_active),
             count(*) FILTER (WHERE active_count > p.max_active)
      FROM active
      JOIN #{partitions} AS p USING (scope_key)
      WHERE seeded_capped
      """).rows

    %{
      queued_rows: queued,
      active_tenants_with_rows: active_tenants,
      ready_rows: ready,
      expired_rows: expired,
      poison_boundary_rows: poison_boundary,
      live_claims: live,
      partition_rows: partition_count,
      dormant_partition_rows: dormant_count,
      hot_tenant_ready_rows: hot_tenant_ready_rows,
      one_row_tenants: one_row_tenants,
      capped_tenants_at_max: capped_at_max,
      capped_tenants_over_max: capped_over_max,
      requested_bucket_rates: %{
        expired_percent: config.expired_percent,
        poison_percent: config.poison_percent,
        basis:
          "deterministic hash buckets; expired excludes hot and capped tenant buckets by design"
      },
      checksum: %{runs: runs_checksum, partitions: partitions_checksum}
    }
  end

  defp queued_insert(runs) do
    """
    WITH generated AS (
      SELECT n,
             CASE
               WHEN n <= $2 THEN 1
               WHEN n <= $2 + $3 THEN 1 + (n - $2)
               WHEN $4 - $3 - 1 <= 0 THEN 1
               ELSE $3 + 2 + mod((n - $2 - $3 - 1 + $5), ($4 - $3 - 1))
             END::integer AS tenant_number,
             mod((n::bigint * 37 + $5), 100)::integer AS class_bucket
      FROM generate_series(1, $1) AS n
    )
    INSERT INTO #{runs}
      (run_id, tenant_id, graph_id, graph_hash, status, step, state,
       checkpoint_seq, latest_checkpoint_type, claim_token, claimed_at, wake_at,
       claim_attempts, claim_abandons, poisoned_at, poison_reason,
       inserted_at, started_at, updated_at, finished_at)
    SELECT 'bench-' || lpad(n::text, 10, '0'),
           'tenant-' || lpad(tenant_number::text, 8, '0'),
           'bench-graph', 'bench-hash', 'running', 0, decode('836a', 'hex'),
           1, 'run_initialized',
           CASE WHEN tenant_number > 1 AND tenant_number <= $4 - $9
                  AND class_bucket >= $7 AND class_bucket < $7 + $6
             THEN md5('expired-' || n)::uuid ELSE NULL END,
           CASE WHEN tenant_number > 1 AND tenant_number <= $4 - $9
                  AND class_bucket >= $7 AND class_bucket < $7 + $6
             THEN $8::timestamptz - interval '120 seconds' - make_interval(secs => mod(n + $5, 3600)::integer)
             ELSE NULL END,
           CASE WHEN tenant_number > 1 AND tenant_number <= $4 - $9
                  AND class_bucket >= $7 AND class_bucket < $7 + $6
             THEN NULL
             ELSE $8::timestamptz - make_interval(secs => mod(n + $5, 3600)::integer)
           END,
           CASE WHEN class_bucket < $7 THEN 3 ELSE 0 END,
           0, NULL, NULL, $8::timestamptz, $8::timestamptz, $8::timestamptz, NULL
    FROM generated
    """
  end

  defp capped_insert(runs) do
    """
    INSERT INTO #{runs}
      (run_id, tenant_id, graph_id, graph_hash, status, step, state,
       checkpoint_seq, latest_checkpoint_type, claim_token, claimed_at, wake_at,
       claim_attempts, claim_abandons, poisoned_at, poison_reason,
       inserted_at, started_at, updated_at, finished_at)
    SELECT 'capped-' || lpad(tenant_number::text, 8, '0') || '-' ||
             lpad(slot::text, 4, '0'),
           'tenant-' || lpad(tenant_number::text, 8, '0'),
           'bench-graph', 'bench-hash', 'running', 0, decode('836a', 'hex'),
           1, 'run_initialized',
           md5('capped-' || tenant_number || '-' || slot)::uuid,
           $4::timestamptz - interval '10 seconds', NULL, 1, 0, NULL, NULL,
           $4::timestamptz, $4::timestamptz, $4::timestamptz, NULL
    FROM generate_series($1::integer - $2::integer + 1, $1::integer) AS tenant_number
    CROSS JOIN generate_series(1, $3::integer) AS slot
    """
  end
end

defmodule Docket.Bench.TenantFairClaim.Artifacts do
  @moduledoc false

  def prepare!(config, git) do
    nonce = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)

    run_id =
      DateTime.utc_now()
      |> DateTime.truncate(:second)
      |> DateTime.to_iso8601(:basic)
      |> String.replace([":", "-"], "")
      |> Kernel.<>("-#{String.slice(git.sha, 0, 8)}-#{nonce}")

    root =
      config.output ||
        Path.join(["tmp", "bench", "postgres", "tenant_fair_claim", run_id])

    root = Path.expand(root)

    if File.exists?(root) do
      raise ArgumentError,
            "refusing to reuse benchmark artifact directory #{root}; choose a fresh --output path"
    end

    File.mkdir_p!(Path.join(root, "plans"))
    %{root: root, run_id: run_id, samples: Path.join(root, "samples.ndjson")}
  end

  def write_plan!(artifacts, candidate, plan) do
    path = Path.join([artifacts.root, "plans", "#{candidate}.json"])
    write_json!(path, plan)
    Path.relative_to(path, artifacts.root)
  end

  def write_samples!(artifacts, samples) do
    body = Enum.map_join(samples, "", &(JSON.encode!(json_safe(&1)) <> "\n"))
    File.write!(artifacts.samples, body)
    Path.relative_to(artifacts.samples, artifacts.root)
  end

  def write_summary!(artifacts, summary) do
    path = Path.join(artifacts.root, "summary.json")
    write_json!(path, summary)
    path
  end

  def write_manifest!(artifacts, manifest) do
    path = Path.join(artifacts.root, "manifest.json")
    write_json!(path, manifest)
    path
  end

  def summarize(samples, demand, elapsed_us) do
    query = Enum.map(samples, & &1.query_us)
    checkout = samples |> Enum.uniq_by(& &1.worker) |> Enum.map(& &1.checkout_us)
    commit = Enum.map(samples, & &1.commit_us)
    outcomes = Enum.sum(Enum.map(samples, & &1.outcomes))
    transactions = length(samples)
    elapsed_seconds = max(elapsed_us / 1_000_000, 0.000_001)

    %{
      sample_count: transactions,
      query_us: percentiles(query),
      checkout_sample_count: length(checkout),
      checkout_us: percentiles(checkout),
      commit_us: percentiles(commit),
      outcomes: outcomes,
      poisoned: Enum.sum(Enum.map(samples, & &1.poisoned)),
      ready_outcomes: Enum.sum(Enum.map(samples, & &1.ready)),
      expired_outcomes: Enum.sum(Enum.map(samples, & &1.expired)),
      pages: Enum.sum(Enum.map(samples, & &1.pages)),
      sql_statements: Enum.sum(Enum.map(samples, & &1.sql_statements)),
      work_budget_exhausted_transactions: Enum.count(samples, & &1.work_budget_exhausted),
      partial_transactions: Enum.count(samples, &(&1.outcomes < demand)),
      avoidable_underclaim_transactions: Enum.count(samples, & &1.avoidable_underclaim),
      error_count: Enum.count(samples, &Map.has_key?(&1, :error)),
      transaction_throughput_per_second: transactions / elapsed_seconds,
      outcome_throughput_per_second: outcomes / elapsed_seconds,
      elapsed_us: elapsed_us
    }
  end

  def plan_metrics(plan, evidence) do
    root = plan |> List.first() |> Map.fetch!("Plan")
    scan_rows = base_relation_scan_rows(root)
    leases = evidence.leased

    %{
      planning_time_ms: List.first(plan)["Planning Time"],
      execution_time_ms: List.first(plan)["Execution Time"],
      leased_outcomes: leases,
      poisoned_outcomes: evidence.poisoned,
      base_relation_scan_rows: scan_rows,
      base_relation_scan_rows_per_lease: if(leases > 0, do: scan_rows / leases, else: nil),
      shared_hit_blocks: Map.get(root, "Shared Hit Blocks", 0),
      shared_read_blocks: Map.get(root, "Shared Read Blocks", 0),
      shared_dirtied_blocks: Map.get(root, "Shared Dirtied Blocks", 0),
      shared_written_blocks: Map.get(root, "Shared Written Blocks", 0),
      wal_records: Map.get(root, "WAL Records", 0),
      wal_bytes: Map.get(root, "WAL Bytes", 0)
    }
  end

  def json_safe(%_{} = struct) do
    case struct do
      %DateTime{} -> DateTime.to_iso8601(struct)
      %NaiveDateTime{} -> NaiveDateTime.to_iso8601(struct)
      _ -> struct |> Map.from_struct() |> json_safe()
    end
  end

  def json_safe(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), json_safe(value)} end)
  end

  def json_safe(list) when is_list(list), do: Enum.map(list, &json_safe/1)
  def json_safe(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> json_safe()

  def json_safe(atom) when is_atom(atom) and atom not in [true, false, nil],
    do: Atom.to_string(atom)

  def json_safe(value), do: value

  defp write_json!(path, value) do
    File.write!(path, JSON.encode!(json_safe(value)) <> "\n")
  end

  defp percentiles([]), do: %{p50: nil, p95: nil, p99: nil, min: nil, max: nil}

  defp percentiles(values) do
    sorted = Enum.sort(values)

    %{
      p50: nearest_rank(sorted, 0.50),
      p95: nearest_rank(sorted, 0.95),
      p99: nearest_rank(sorted, 0.99),
      min: hd(sorted),
      max: List.last(sorted)
    }
  end

  defp nearest_rank(sorted, percentile) do
    index = max(ceil(length(sorted) * percentile) - 1, 0)
    Enum.at(sorted, index)
  end

  defp base_relation_scan_rows(node) do
    own =
      if base_relation_scan?(node) do
        loops = Map.get(node, "Actual Loops", 0)

        (Map.get(node, "Actual Rows", 0) +
           Map.get(node, "Rows Removed by Filter", 0) +
           Map.get(node, "Rows Removed by Index Recheck", 0)) * loops
      else
        0
      end

    own + Enum.sum(Enum.map(Map.get(node, "Plans", []), &base_relation_scan_rows/1))
  end

  defp base_relation_scan?(%{"Relation Name" => _, "Node Type" => type}),
    do: String.contains?(type, "Scan")

  defp base_relation_scan?(%{"Node Type" => "Bitmap Index Scan"}), do: true
  defp base_relation_scan?(_node), do: false
end

defmodule Docket.Bench.TenantFairClaim.Runner do
  @moduledoc false

  alias Docket.Bench.TenantFairClaim.{Artifacts, Repo, SQL, Seed}

  def run_candidate!(candidate, prefix, config, artifacts) do
    seed_manifest = Seed.reset!(prefix, config)
    envelope = contention_envelope!(candidate, prefix, config)
    hot_contention = hot_contention_audit!(candidate, prefix, config)
    cap_safety = cap_safety_audit!(candidate, prefix, config)
    _ = Seed.reset!(prefix, config)

    Enum.each(1..config.warmup//1, fn _ -> rollback_query!(candidate, prefix, config) end)

    {plan, plan_evidence} = capture_plan!(candidate, prefix, config)
    plan_path = Artifacts.write_plan!(artifacts, candidate, plan)
    _ = Seed.reset!(prefix, config)
    {samples, elapsed_us} = measured_samples!(candidate, prefix, config)
    policy_audit = policy_audit!(prefix)

    %{
      candidate: candidate,
      role: SQL.role(candidate),
      query_sha256: SQL.query_hash(candidate, "docket_bench_schema"),
      seed: seed_manifest,
      contention: envelope,
      hot_contention: hot_contention,
      cap_safety: cap_safety,
      post_measurement_policy: policy_audit,
      plan_path: plan_path,
      plan: Artifacts.plan_metrics(plan, plan_evidence),
      measurements: Artifacts.summarize(samples, config.demand, elapsed_us),
      samples: samples
    }
  end

  defp hot_contention_audit!(candidate, _prefix, _config)
       when candidate in [:ranking_window, :distinct_on] do
    %{audited: false, reason: "admission and reconciliation prototypes only"}
  end

  defp hot_contention_audit!(candidate, prefix, config) do
    _ = Seed.reset!(prefix, config)
    partitions = SQL.table(prefix, "docket_bench_claim_partitions")
    hot_scope_key = "tenant-00000001"
    parent = self()
    barrier = make_ref()

    blocker =
      Task.async(fn ->
        Repo.checkout(fn ->
          Repo.query!("BEGIN")
          [[backend_pid]] = Repo.query!("SELECT pg_backend_pid()").rows

          [[^hot_scope_key]] =
            Repo.query!(
              "SELECT scope_key FROM #{partitions} WHERE scope_key = $1 FOR NO KEY UPDATE",
              [hot_scope_key]
            ).rows

          send(parent, {barrier, :hot_locked, backend_pid})

          receive do
            {^barrier, :release_hot} -> Repo.query!("ROLLBACK")
          after
            config.statement_timeout_ms ->
              Repo.query!("ROLLBACK")
              raise "hot-partition contention blocker timed out"
          end
        end)
      end)

    blocker_pid =
      receive do
        {^barrier, :hot_locked, backend_pid} -> backend_pid
      after
        config.statement_timeout_ms -> raise "hot partition was not locked for contention audit"
      end

    try do
      {:error, subject} =
        Repo.transaction(fn ->
          [[subject_pid]] = Repo.query!("SELECT pg_backend_pid()").rows
          result = execute_claim!(candidate, prefix, config, "")
          outcomes = Enum.filter(result.rows, &(List.last(&1) in ["leased", "poisoned"]))

          Repo.rollback(%{
            backend_pid: subject_pid,
            outcomes: length(outcomes),
            hot_tenant_outcomes: Enum.count(outcomes, &(Enum.at(&1, 2) == hot_scope_key)),
            other_tenant_outcomes: Enum.count(outcomes, &(Enum.at(&1, 2) != hot_scope_key)),
            pages: result.pages,
            sql_statements: result.sql_statements,
            work_budget_exhausted: result.work_budget_exhausted
          })
        end)

      %{
        audited: true,
        hot_scope_key: hot_scope_key,
        blocker_backend_pid: blocker_pid,
        subject_backend_pid: subject.backend_pid,
        requested: config.demand,
        outcomes: subject.outcomes,
        hot_tenant_outcomes: subject.hot_tenant_outcomes,
        other_tenant_outcomes: subject.other_tenant_outcomes,
        pages: subject.pages,
        sql_statements: subject.sql_statements,
        work_budget_exhausted: subject.work_budget_exhausted,
        progressed_around_locked_hot_partition:
          subject.hot_tenant_outcomes == 0 and subject.other_tenant_outcomes == config.demand
      }
    after
      send(blocker.pid, {barrier, :release_hot})
      _ = Task.await(blocker, config.statement_timeout_ms)
    end
  end

  defp policy_audit!(prefix) do
    runs = SQL.table(prefix, "docket_runs")
    partitions = SQL.table(prefix, "docket_bench_claim_partitions")

    [[over_max, max_excess]] =
      Repo.query!("""
      WITH active AS (
        SELECT scope_key, count(*) AS active_count
        FROM #{runs}
        WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL
        GROUP BY scope_key
      )
      SELECT count(*) FILTER (WHERE active.active_count > partitions.max_active),
             COALESCE(max(active.active_count - partitions.max_active), 0)
      FROM active
      JOIN #{partitions} AS partitions USING (scope_key)
      """).rows

    %{tenants_over_max: over_max, max_excess: max_excess, respected: over_max == 0}
  end

  defp cap_safety_audit!(candidate, _prefix, _config)
       when candidate in [:ranking_window, :distinct_on] do
    %{audited: false, reason: "admission prototype with capped tenants required"}
  end

  defp cap_safety_audit!(_candidate, _prefix, %{capped_tenants: 0}) do
    %{audited: false, reason: "profile has no capped tenants"}
  end

  defp cap_safety_audit!(candidate, prefix, config) do
    _ = Seed.reset!(prefix, config)
    runs = SQL.table(prefix, "docket_runs")
    partitions = SQL.table(prefix, "docket_bench_claim_partitions")
    tenant_number = config.tenants
    scope_key = "tenant-" <> String.pad_leading(Integer.to_string(tenant_number), 8, "0")

    Repo.query!(
      """
      DELETE FROM #{runs}
      WHERE id = (
        SELECT id FROM #{runs}
        WHERE scope_key = $1 AND run_id LIKE 'capped-%'
        ORDER BY id LIMIT 1
      )
      """,
      [scope_key]
    )

    Repo.query!(
      """
      UPDATE #{runs}
      SET claim_token = NULL, claimed_at = NULL, wake_at = $1::timestamptz + interval '1 day',
          claim_attempts = 0
      WHERE run_id LIKE 'bench-%'
      """,
      [config.fixed_now]
    )

    %{num_rows: 2} =
      Repo.query!(
        """
        WITH chosen AS (
          SELECT id FROM #{runs}
          WHERE scope_key = $1 AND run_id LIKE 'bench-%'
          ORDER BY id LIMIT 2
        )
        UPDATE #{runs} AS runs
        SET wake_at = $2::timestamptz - interval '1 second'
        FROM chosen
        WHERE runs.id = chosen.id
        """,
        [scope_key, config.fixed_now]
      )

    Repo.query!(
      """
      UPDATE #{partitions}
      SET next_ready_at_hint = CASE
            WHEN scope_key = $1 THEN $2::timestamptz
            ELSE NULL
          END,
          next_expired_at_hint = NULL,
          last_claimed_at = NULL
      """,
      [scope_key, config.fixed_now]
    )

    [[before_active, max_active]] =
      Repo.query!(
        """
        SELECT count(runs.id), partitions.max_active
        FROM #{partitions} AS partitions
        LEFT JOIN #{runs} AS runs
          ON runs.scope_key = partitions.scope_key
         AND runs.status = 'running' AND runs.poisoned_at IS NULL
         AND runs.claim_token IS NOT NULL
        WHERE partitions.scope_key = $1
        GROUP BY partitions.max_active
        """,
        [scope_key]
      ).rows

    unless before_active == max_active - 1 do
      raise "cap race fixture did not resolve to max_active - 1"
    end

    parent = self()
    barrier = make_ref()
    audit_config = %{config | demand: 1}

    tasks =
      for worker <- 1..2 do
        Task.async(fn ->
          Repo.checkout(fn ->
            Repo.query!("BEGIN")
            send(parent, {barrier, :ready, worker})

            receive do
              {^barrier, :go} -> :ok
            after
              config.statement_timeout_ms -> raise "cap race worker missed barrier"
            end

            result = execute_claim!(candidate, prefix, audit_config, "")
            Repo.query!("COMMIT")
            outcome_count(result.rows)
          end)
        end)
      end

    Enum.each(tasks, fn _ ->
      receive do
        {^barrier, :ready, _worker} -> :ok
      after
        config.statement_timeout_ms -> raise "cap race workers did not become ready"
      end
    end)

    Enum.each(tasks, &send(&1.pid, {barrier, :go}))
    outcomes = Enum.map(tasks, &Task.await(&1, config.statement_timeout_ms * 2))

    [[after_active]] =
      Repo.query!(
        """
        SELECT count(*) FROM #{runs}
        WHERE scope_key = $1 AND status = 'running' AND poisoned_at IS NULL
          AND claim_token IS NOT NULL
        """,
        [scope_key]
      ).rows

    %{
      audited: true,
      scope_key: scope_key,
      before_active: before_active,
      max_active: max_active,
      admitted: Enum.sum(outcomes),
      after_active: after_active,
      respected: after_active <= max_active and Enum.sum(outcomes) <= 1
    }
  end

  defp rollback_query!(candidate, prefix, config) do
    {:error, :rollback} =
      Repo.transaction(fn ->
        _ = execute_claim!(candidate, prefix, config, "")

        Repo.rollback(:rollback)
      end)

    :ok
  end

  defp capture_plan!(candidate, prefix, config) do
    sql =
      "EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON) " <>
        SQL.statement(candidate, prefix)

    {:error, {plan, evidence}} =
      Repo.transaction(fn ->
        Repo.query!("SAVEPOINT plan_denominator")

        evidence_rows =
          Repo.query!(SQL.statement(candidate, prefix), SQL.params(candidate, config),
            timeout: config.statement_timeout_ms
          ).rows

        evidence = outcome_evidence(evidence_rows)
        Repo.query!("ROLLBACK TO SAVEPOINT plan_denominator")

        [[plan]] =
          Repo.query!(sql, SQL.params(candidate, config), timeout: config.statement_timeout_ms).rows

        Repo.rollback({plan, evidence})
      end)

    {plan, evidence}
  end

  defp measured_samples!(candidate, prefix, config) do
    parent = self()
    barrier = make_ref()
    started = monotonic_us()

    tasks =
      for worker <- 1..config.workers do
        Task.async(fn -> measured_worker(parent, barrier, worker, candidate, prefix, config) end)
      end

    checked_out =
      for _ <- tasks do
        receive do
          {^barrier, :checked_out, worker, backend_pid, checkout_us} ->
            %{worker: worker, backend_pid: backend_pid, checkout_us: checkout_us}
        after
          config.statement_timeout_ms ->
            raise "benchmark workers timed out before the start barrier"
        end
      end

    backend_pids = Enum.map(checked_out, & &1.backend_pid)

    if length(Enum.uniq(backend_pids)) != length(backend_pids) do
      raise "benchmark workers did not receive distinct PostgreSQL backends"
    end

    Enum.each(tasks, &send(&1.pid, {barrier, :go}))
    timeout = config.statement_timeout_ms * max(config.iterations, 2)
    samples = tasks |> Enum.flat_map(&Task.await(&1, timeout))
    {samples, monotonic_us() - started}
  end

  defp measured_worker(parent, barrier, worker, candidate, prefix, config) do
    checkout_started = monotonic_us()

    Repo.checkout(
      fn ->
        checkout_us = monotonic_us() - checkout_started
        [[backend_pid]] = Repo.query!("SELECT pg_backend_pid()").rows
        send(parent, {barrier, :checked_out, worker, backend_pid, checkout_us})

        receive do
          {^barrier, :go} -> :ok
        after
          config.statement_timeout_ms -> raise "worker #{worker} missed start barrier"
        end

        {samples, _cursor} =
          Enum.map_reduce(1..config.iterations, initial_cursor(worker, config), fn iteration,
                                                                                   cursor ->
            sample_transaction!(
              worker,
              iteration,
              checkout_us,
              candidate,
              prefix,
              config,
              cursor
            )
          end)

        samples
      end,
      timeout: config.statement_timeout_ms
    )
  end

  defp sample_transaction!(
         worker,
         iteration,
         checkout_us,
         candidate,
         prefix,
         config,
         cursor
       ) do
    Repo.query!("BEGIN")
    Repo.query!("SET LOCAL statement_timeout = '#{config.statement_timeout_ms}ms'")
    query_started = monotonic_us()

    execution = execute_claim!(candidate, prefix, config, cursor)

    query_us = monotonic_us() - query_started
    evidence = outcome_evidence(execution.rows)
    control_lockable_after = audit_remaining!(prefix, config, evidence.outcomes)
    commit_started = monotonic_us()
    Repo.query!("COMMIT")
    commit_us = monotonic_us() - commit_started

    sample = %{
      candidate: candidate,
      worker: worker,
      iteration: iteration,
      checkout_us: checkout_us,
      query_us: query_us,
      commit_us: commit_us,
      outcomes: evidence.outcomes,
      ready: evidence.ready,
      expired: evidence.expired,
      poisoned: evidence.poisoned,
      pages: execution.pages,
      sql_statements: execution.sql_statements,
      work_budget_exhausted: execution.work_budget_exhausted,
      control_lockable_after: control_lockable_after,
      avoidable_underclaim: evidence.outcomes < config.demand and control_lockable_after > 0,
      backend_pid: backend_pid!(),
      cursor_before: if(candidate == :hint_cursor, do: cursor, else: nil),
      cursor_after: if(candidate == :hint_cursor, do: execution.cursor, else: nil)
    }

    {sample, execution.cursor}
  rescue
    error ->
      _ = Repo.query("ROLLBACK")
      reraise error, __STACKTRACE__
  end

  defp audit_remaining!(prefix, config, outcomes) do
    if outcomes >= config.demand do
      0
    else
      remaining = config.demand - outcomes
      Repo.query!("SAVEPOINT measured_underclaim_audit")

      rows =
        Repo.query!(
          control_statement(prefix),
          SQL.params(:hint_cursor, config, "", remaining) |> Enum.take(3),
          timeout: config.statement_timeout_ms
        ).rows

      Repo.query!("ROLLBACK TO SAVEPOINT measured_underclaim_audit")
      length(rows)
    end
  end

  defp execute_claim!(:hint_cursor, prefix, config, cursor) do
    page_capacity = config.page_size * config.oversampling
    max_pages = max(ceil(config.reconciliation_budget / page_capacity), 1)

    execute_hint_pages!(prefix, config, cursor, max_pages, 0, 0, [])
  end

  defp execute_claim!(:recursive_loose_scan, prefix, config, _cursor) do
    page_capacity = config.page_size * config.oversampling
    max_pages = max(ceil(config.reconciliation_budget / page_capacity), 1)

    execute_recursive_pages!(prefix, config, page_capacity, max_pages, 0, [])
  end

  defp execute_claim!(candidate, prefix, config, _cursor) do
    result =
      Repo.query!(SQL.statement(candidate, prefix), SQL.params(candidate, config),
        timeout: config.statement_timeout_ms
      )

    %{
      rows: result.rows,
      cursor: "",
      pages: 1,
      sql_statements: 1,
      work_budget_exhausted: false
    }
  end

  defp execute_recursive_pages!(prefix, config, page_capacity, max_pages, pages, rows) do
    remaining = config.demand - outcome_count(rows)

    cond do
      remaining <= 0 ->
        %{
          rows: rows,
          cursor: "",
          pages: pages,
          sql_statements: pages,
          work_budget_exhausted: false
        }

      pages >= max_pages ->
        %{
          rows: rows,
          cursor: "",
          pages: pages,
          sql_statements: pages,
          work_budget_exhausted: true
        }

      true ->
        result =
          Repo.query!(
            SQL.statement(:recursive_loose_scan, prefix),
            SQL.params(:recursive_loose_scan, config, "", remaining, page_capacity),
            timeout: config.statement_timeout_ms
          )

        execute_recursive_pages!(
          prefix,
          config,
          page_capacity,
          max_pages,
          pages + 1,
          rows ++ result.rows
        )
    end
  end

  defp execute_hint_pages!(prefix, config, cursor, max_pages, pages, statements, rows) do
    remaining = config.demand - outcome_count(rows)

    cond do
      remaining <= 0 ->
        %{
          rows: rows,
          cursor: cursor,
          pages: pages,
          sql_statements: statements,
          work_budget_exhausted: false
        }

      pages >= max_pages ->
        %{
          rows: rows,
          cursor: cursor,
          pages: pages,
          sql_statements: statements,
          work_budget_exhausted: true
        }

      true ->
        result =
          Repo.query!(
            SQL.statement(:hint_cursor, prefix),
            SQL.params(:hint_cursor, config, cursor, remaining),
            timeout: config.statement_timeout_ms
          )

        next_cursor = result.rows |> List.first() |> then(&if(&1, do: Enum.at(&1, -2), else: nil))
        next_cursor = next_cursor || ""
        page_outcomes = outcome_count(result.rows)

        cond do
          next_cursor == "" and page_outcomes == 0 and cursor != "" ->
            execute_hint_pages!(prefix, config, "", max_pages, pages, statements + 1, rows)

          next_cursor == "" and page_outcomes == 0 ->
            %{
              rows: rows,
              cursor: "",
              pages: pages,
              sql_statements: statements + 1,
              work_budget_exhausted: false
            }

          true ->
            execute_hint_pages!(
              prefix,
              config,
              next_cursor,
              max_pages,
              pages + 1,
              statements + 1,
              rows ++ result.rows
            )
        end
    end
  end

  defp contention_envelope!(candidate, prefix, config) do
    _ = Seed.reset!(prefix, config)
    parent = self()
    barrier = make_ref()

    blocker =
      Task.async(fn ->
        Repo.checkout(fn ->
          Repo.query!("BEGIN")
          [[backend_pid]] = Repo.query!("SELECT pg_backend_pid()").rows

          locked =
            Repo.query!(blocker_statement(prefix), SQL.audit_params(config),
              timeout: config.statement_timeout_ms
            ).rows

          send(parent, {barrier, :locked, backend_pid, locked})

          receive do
            {^barrier, :release} -> Repo.query!("ROLLBACK")
          after
            config.statement_timeout_ms ->
              Repo.query!("ROLLBACK")
              raise "contention blocker timed out"
          end
        end)
      end)

    {blocker_pid, locked} =
      receive do
        {^barrier, :locked, backend_pid, rows} -> {backend_pid, rows}
      after
        config.statement_timeout_ms -> raise "contention blocker failed to acquire the first page"
      end

    try do
      {:error, subject} =
        Repo.transaction(fn ->
          [[subject_pid]] = Repo.query!("SELECT pg_backend_pid()").rows

          result = execute_claim!(candidate, prefix, config, "")

          Repo.rollback(%{
            backend_pid: subject_pid,
            outcomes: outcome_count(result.rows),
            pages: result.pages,
            sql_statements: result.sql_statements,
            work_budget_exhausted: result.work_budget_exhausted
          })
        end)

      {:error, control} =
        Repo.transaction(fn ->
          [[control_pid]] = Repo.query!("SELECT pg_backend_pid()").rows

          rows =
            Repo.query!(control_statement(prefix), SQL.audit_params(config),
              timeout: config.statement_timeout_ms
            ).rows

          Repo.rollback(%{backend_pid: control_pid, lockable_rows: length(rows)})
        end)

      %{
        blocker_backend_pid: blocker_pid,
        subject_backend_pid: subject.backend_pid,
        control_backend_pid: control.backend_pid,
        locked_first_page: length(locked),
        outcomes: subject.outcomes,
        pages: subject.pages,
        sql_statements: subject.sql_statements,
        work_budget_exhausted: subject.work_budget_exhausted,
        requested: config.demand,
        control_lockable_rows: control.lockable_rows,
        underclaimed: subject.outcomes < config.demand and control.lockable_rows > 0
      }
    after
      send(blocker.pid, {barrier, :release})
      _ = Task.await(blocker, config.statement_timeout_ms)
    end
  end

  defp blocker_statement(prefix) do
    runs = SQL.table(prefix, "docket_runs")
    partitions = SQL.table(prefix, "docket_bench_claim_partitions")

    """
    WITH eligible AS MATERIALIZED (
      #{baseline_eligible(runs, partitions)}
    ),
    ranked AS MATERIALIZED (
      SELECT eligible.*,
             ROW_NUMBER() OVER (PARTITION BY scope_key ORDER BY eligible_at, id) AS rank
      FROM eligible
    ),
    first_page AS MATERIALIZED (
      SELECT id, eligible_at FROM ranked
      WHERE rank = 1 ORDER BY eligible_at, id LIMIT $3
    )
    SELECT runs.id
    FROM #{runs} AS runs
    JOIN first_page ON first_page.id = runs.id
    ORDER BY first_page.eligible_at, runs.id
    FOR UPDATE OF runs
    """
  end

  defp control_statement(prefix) do
    runs = SQL.table(prefix, "docket_runs")
    partitions = SQL.table(prefix, "docket_bench_claim_partitions")

    """
    WITH eligible AS MATERIALIZED (
      #{baseline_eligible(runs, partitions)}
    )
    SELECT runs.id
    FROM #{runs} AS runs
    JOIN eligible ON eligible.id = runs.id
    ORDER BY eligible.eligible_at, runs.id
    LIMIT $3
    FOR UPDATE OF runs SKIP LOCKED
    """
  end

  defp baseline_eligible(runs, partitions) do
    """
    SELECT ready.id, ready.scope_key, ready.wake_at AS eligible_at
    FROM #{runs} AS ready
    JOIN #{partitions} AS policy ON policy.scope_key = ready.scope_key
    LEFT JOIN (
      SELECT scope_key, count(*) AS active_count
      FROM #{runs}
      WHERE status = 'running' AND poisoned_at IS NULL AND claim_token IS NOT NULL
      GROUP BY scope_key
    ) AS active ON active.scope_key = ready.scope_key
    WHERE ready.status = 'running' AND ready.poisoned_at IS NULL
      AND ready.claim_token IS NULL AND ready.wake_at <= $1
      AND COALESCE(active.active_count, 0) < policy.max_active
    UNION ALL
    SELECT expired.id, expired.scope_key, expired.claimed_at AS eligible_at
    FROM #{runs} AS expired
    WHERE expired.status = 'running' AND expired.poisoned_at IS NULL
      AND expired.claim_token IS NOT NULL AND expired.claimed_at < $2
    """
  end

  defp outcome_count(rows),
    do: Enum.count(rows, &(List.last(&1) in ["leased", "poisoned"]))

  defp outcome_evidence(rows) do
    outcomes = Enum.filter(rows, &(List.last(&1) in ["leased", "poisoned"]))

    %{
      outcomes: length(outcomes),
      leased: Enum.count(outcomes, &(List.last(&1) == "leased")),
      poisoned: Enum.count(outcomes, &(List.last(&1) == "poisoned")),
      ready: Enum.count(outcomes, &(Enum.at(&1, 3) == "ready")),
      expired: Enum.count(outcomes, &(Enum.at(&1, 3) == "expired"))
    }
  end

  defp initial_cursor(1, _config), do: ""

  defp initial_cursor(worker, config) do
    tenant_number = div((worker - 1) * config.tenants, config.workers)
    "tenant-" <> String.pad_leading(Integer.to_string(tenant_number), 8, "0")
  end

  defp backend_pid!, do: Repo.query!("SELECT pg_backend_pid()").rows |> hd() |> hd()
  defp monotonic_us, do: System.monotonic_time(:microsecond)
end

defmodule Docket.Bench.TenantFairClaim do
  @moduledoc false

  alias Docket.Bench.TenantFairClaim.{Artifacts, Config, Repo, Runner, SQL, Schema, Types}

  def main(argv) do
    config = Config.parse!(Enum.drop_while(argv, &(&1 == "--")))

    database_url =
      System.get_env("DOCKET_BENCH_DATABASE_URL") ||
        raise "DOCKET_BENCH_DATABASE_URL must name a dedicated benchmark-capable database"

    git = git_metadata()
    source = benchmark_source_metadata()
    artifacts = Artifacts.prepare!(config, git)
    prefix = scratch_prefix()

    {:ok, _pid} =
      Repo.start_link(
        url: database_url,
        pool_size: config.workers + 3,
        queue_target: 5_000,
        queue_interval: 5_000,
        timeout: config.statement_timeout_ms,
        types: Types,
        log: false
      )

    try do
      postgres = postgres_metadata!()
      require_postgres_13!(postgres.server_version_num)
      Schema.create!(prefix)

      candidate_order = candidate_order(config)

      manifest = %{
        schema: "docket.postgres.tenant_fair_claim/v1",
        status: "exploratory_pre_runtime_prototype",
        runtime_parity: false,
        runtime_parity_requirement:
          "replace prototype SQL/DDL hashes with exact TenantFair runtime plan and migration hashes before regression claims",
        run_id: artifacts.run_id,
        started_at: DateTime.utc_now(),
        git: git,
        source: source,
        runtime: runtime_metadata(),
        postgres: postgres,
        config: config,
        scratch_schema: prefix,
        thresholds: %{
          page_size: config.page_size,
          oversampling: config.oversampling,
          reconciliation_work_budget: config.reconciliation_budget,
          rationale:
            "profile-owned exploratory values; validate on held-out cardinalities before runtime adoption"
        },
        metric_definitions: %{
          percentiles: "nearest-rank over committed client-observed samples",
          durations: "integer microseconds",
          transaction_throughput:
            "committed claim transactions divided by measured wall-clock seconds",
          outcome_throughput:
            "leased plus poisoned outcomes divided by measured wall-clock seconds",
          base_relation_scan_rows:
            "sum over base-relation and bitmap-index scan nodes of (Actual Rows + Rows Removed by Filter + Rows Removed by Index Recheck) * Actual Loops",
          contention:
            "explicit locked-first-page outcomes plus cap-aware SKIP LOCKED controls for the envelope and every measured partial; query duration is not lock-wait time",
          partial_batch:
            "outcomes below demand; only avoidable when the cap-aware savepoint control finds lockable work",
          plan_denominator:
            "leased outcomes from a rolled-back execution on the identical fixture; poison and metadata rows are excluded"
        },
        provisional_ddl_sha256: SQL.ddl_hash("docket_bench_schema"),
        candidate_order: candidate_order,
        artifact_root: artifacts.root
      }

      Artifacts.write_manifest!(artifacts, manifest)

      results =
        Enum.map(candidate_order, fn candidate ->
          IO.puts("benchmarking #{candidate} (#{SQL.role(candidate)})")
          Runner.run_candidate!(candidate, prefix, config, artifacts)
        end)

      samples = Enum.flat_map(results, & &1.samples)
      samples_path = Artifacts.write_samples!(artifacts, samples)

      summary = %{
        schema: "docket.postgres.tenant_fair_claim/v1",
        status: "exploratory_pre_runtime_prototype",
        run_id: artifacts.run_id,
        source: source,
        manifest_path: "manifest.json",
        samples_path: samples_path,
        config: config,
        thresholds: manifest.thresholds,
        candidates:
          Enum.map(results, fn result ->
            Map.drop(result, [:samples])
          end)
      }

      summary_path = Artifacts.write_summary!(artifacts, summary)
      if config.check, do: check!(summary, artifacts)
      IO.puts("wrote #{summary_path}")
    after
      try do
        if not config.keep_schema, do: Schema.drop_if_exists!(prefix)
      after
        Supervisor.stop(Repo)
      end
    end
  end

  defp check!(summary, artifacts) do
    by_name = Map.new(summary.candidates, &{&1.candidate, &1})
    seed = hd(summary.candidates).seed

    unless seed.queued_rows == summary.config.queued_rows and
             seed.active_tenants_with_rows == summary.config.tenants and
             seed.dormant_partition_rows == summary.config.dormant_tenants and
             seed.hot_tenant_ready_rows == summary.config.hot_rows and
             seed.one_row_tenants >= summary.config.one_row_tenants and
             seed.capped_tenants_at_max == summary.config.capped_tenants and
             seed.capped_tenants_over_max == 0 do
      raise "resolved seed does not match the requested profile: #{inspect(seed)}"
    end

    unless summary.candidates |> Enum.map(& &1.seed.checksum) |> Enum.uniq() |> length() == 1 do
      raise "candidate fixtures do not have identical deterministic checksums"
    end

    Enum.each([:ranking_window, :distinct_on], fn candidate ->
      envelope = by_name[candidate].contention

      unless envelope.underclaimed and envelope.control_lockable_rows > 0 do
        raise "#{candidate} did not reproduce audited first-page under-claim"
      end
    end)

    Enum.each([:hint_cursor, :recursive_loose_scan], fn candidate ->
      envelope = by_name[candidate].contention
      measurements = by_name[candidate].measurements
      cap_safety = by_name[candidate].cap_safety
      hot_contention = by_name[candidate].hot_contention
      post_measurement_policy = by_name[candidate].post_measurement_policy

      unless envelope.outcomes == envelope.requested do
        raise "#{candidate} failed to fill demand past the locked first page: #{inspect(envelope)}"
      end

      unless measurements.avoidable_underclaim_transactions == 0 do
        raise "#{candidate} left audited lockable work after a measured partial transaction"
      end

      unless hot_contention.audited and
               hot_contention.progressed_around_locked_hot_partition do
        raise "#{candidate} failed to progress around the locked hot tenant"
      end

      unless (seed.ready_rows == 0 or measurements.ready_outcomes > 0) and
               (seed.expired_rows == 0 or measurements.expired_outcomes > 0) do
        raise "#{candidate} did not exercise every populated admission class"
      end

      unless summary.config.capped_tenants == 0 or
               (cap_safety.audited and cap_safety.respected) do
        raise "#{candidate} failed the concurrent max-minus-one cap audit: #{inspect(cap_safety)}"
      end

      unless post_measurement_policy.respected do
        raise "#{candidate} exceeded max_active during measured concurrency"
      end
    end)

    Enum.each(summary.candidates, fn candidate ->
      plan_path = Path.join(artifacts.root, candidate.plan_path)

      unless File.regular?(plan_path) and candidate.measurements.sample_count > 0 do
        raise "#{candidate.candidate} did not produce a plan and measured samples"
      end

      plan = plan_path |> File.read!() |> JSON.decode!()

      unless is_list(plan) and match?(%{"Plan" => _}, List.first(plan)) do
        raise "#{candidate.candidate} plan artifact has an invalid JSON shape"
      end

      unless candidate.measurements.error_count == 0 do
        raise "#{candidate.candidate} recorded benchmark errors"
      end

      unless is_integer(candidate.plan.base_relation_scan_rows) and
               candidate.plan.base_relation_scan_rows > 0 and
               candidate.plan.leased_outcomes > 0 and
               is_number(candidate.plan.base_relation_scan_rows_per_lease) and
               is_binary(candidate.query_sha256) and byte_size(candidate.query_sha256) == 64 do
        raise "#{candidate.candidate} plan metrics or query hash are incomplete"
      end

      relation_names = plan |> List.first() |> Map.fetch!("Plan") |> relation_names()

      unless MapSet.member?(relation_names, "docket_runs") and
               MapSet.member?(relation_names, "docket_bench_claim_partitions") do
        raise "#{candidate.candidate} plan did not exercise both run and partition relations"
      end
    end)

    unless File.regular?(Path.join(artifacts.root, "manifest.json")) and
             File.regular?(Path.join(artifacts.root, "samples.ndjson")) do
      raise "benchmark artifact set is incomplete"
    end

    manifest = artifacts.root |> Path.join("manifest.json") |> File.read!() |> JSON.decode!()

    persisted_summary =
      artifacts.root |> Path.join("summary.json") |> File.read!() |> JSON.decode!()

    unless manifest["run_id"] == persisted_summary["run_id"] and
             manifest["source"]["sha256"] == persisted_summary["source"]["sha256"] do
      raise "benchmark artifacts do not share one run id and source hash"
    end

    artifacts.root
    |> Path.join("samples.ndjson")
    |> File.stream!()
    |> Enum.each(&(String.trim(&1) |> JSON.decode!()))

    :ok
  end

  defp postgres_metadata! do
    [[server_version_num]] = Repo.query!("SHOW server_version_num").rows
    [[version]] = Repo.query!("SELECT version()").rows

    [[database, user, server_address, server_port]] =
      Repo.query!(
        "SELECT current_database(), current_user, inet_server_addr()::text, inet_server_port()"
      ).rows

    settings =
      for name <- [
            "jit",
            "max_parallel_workers_per_gather",
            "random_page_cost",
            "effective_cache_size",
            "work_mem",
            "plan_cache_mode"
          ],
          into: %{} do
        [[value]] = Repo.query!("SHOW #{name}").rows
        {name, value}
      end

    %{
      server_version_num: String.to_integer(server_version_num),
      version: version,
      database: database,
      user: user,
      server_address: server_address,
      server_port: server_port,
      settings: settings
    }
  end

  defp relation_names(node) do
    children = Enum.map(Map.get(node, "Plans", []), &relation_names/1)
    own = if name = Map.get(node, "Relation Name"), do: [name], else: []
    Enum.reduce(children, MapSet.new(own), &MapSet.union/2)
  end

  defp require_postgres_13!(version) when version >= 130_000, do: :ok

  defp require_postgres_13!(version) do
    raise "tenant-fair claim benchmark requires PostgreSQL 13+, got server_version_num=#{version}"
  end

  defp runtime_metadata do
    %{
      elixir: System.version(),
      otp_release: System.otp_release(),
      ecto_sql: app_version(:ecto_sql),
      postgrex: app_version(:postgrex),
      os: :os.type() |> inspect(),
      schedulers_online: System.schedulers_online()
    }
  end

  defp app_version(app) do
    case Application.spec(app, :vsn) do
      nil -> nil
      version -> to_string(version)
    end
  end

  defp git_metadata do
    {sha, 0} = System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true)
    {status, 0} = System.cmd("git", ["status", "--porcelain"], stderr_to_stdout: true)
    %{sha: String.trim(sha), dirty: String.trim(status) != ""}
  end

  defp benchmark_source_metadata do
    paths = [
      "bench/postgres/tenant_fair_claim.exs",
      "bench/support/tenant_fair_claim.ex"
    ]

    body = Enum.map_join(paths, "\n", &File.read!/1)

    %{
      files: paths,
      sha256: :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)
    }
  end

  defp candidate_order(config) do
    Enum.sort_by(SQL.candidates(), fn candidate ->
      :crypto.hash(:sha256, "#{config.seed}:#{candidate}")
    end)
  end

  defp scratch_prefix do
    suffix = System.unique_integer([:positive, :monotonic])
    "docket_bench_#{System.pid()}_#{suffix}" |> String.replace(~r/[^a-z0-9_]/, "_")
  end
end
