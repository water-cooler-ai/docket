if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.ConcurrentAdmissionHarness do
    @moduledoc false

    alias Docket.Postgres.{RunStore, Storage}

    @default_timeout 5_000

    defmodule KnownBadRankThenLock do
      @moduledoc false

      @behaviour Docket.Postgres.ClaimPolicy

      alias Docket.Postgres.ClaimPolicy.{Legacy, Plan}

      @impl true
      def init([], _context), do: {:ok, nil}
      def init(options, _context), do: {:error, {:unknown_options, Keyword.keys(options)}}

      @impl true
      def build_plan(
            %{identifiers: %{runs: table}},
            %{now: now, limit: limit, orphan_ttl_ms: ttl, max_claim_attempts: max},
            nil
          ) do
        %Plan{
          statement: statement(table),
          params: [now, limit, max],
          decoder: %{now: now, orphan_ttl_ms: ttl},
          observation: %{demand: limit, preference: nil}
        }
      end

      @impl true
      def decode(rows, decoder, nil), do: Legacy.decode(rows, decoder, nil)

      @impl true
      def observe(plan, decoded, result, duration, nil),
        do: Legacy.observe(plan, decoded, result, duration, nil)

      defp statement(table) do
        """
        WITH ranked AS MATERIALIZED (
          SELECT
            id,
            scope_key,
            wake_at AS eligible_at,
            ROW_NUMBER() OVER (
              PARTITION BY scope_key
              ORDER BY wake_at, id
            ) AS partition_rank
          FROM #{table}
          WHERE status = 'running'
            AND poisoned_at IS NULL
            AND claim_token IS NULL
            AND wake_at <= $1
        ),
        first_page AS MATERIALIZED (
          SELECT id, scope_key, eligible_at
          FROM ranked
          WHERE partition_rank = 1
          ORDER BY eligible_at, id
          LIMIT $2
        ),
        locked AS MATERIALIZED (
          SELECT runs.id, first_page.eligible_at
          FROM #{table} AS runs
          JOIN first_page ON first_page.id = runs.id
          WHERE runs.status = 'running'
            AND runs.poisoned_at IS NULL
            AND runs.claim_token IS NULL
            AND runs.wake_at <= $1
          ORDER BY first_page.eligible_at, runs.id
          FOR UPDATE OF runs SKIP LOCKED
        ),
        updated AS (
          UPDATE #{table} AS runs
          SET claim_token =
                CASE WHEN runs.claim_attempts < $3 THEN gen_random_uuid() ELSE NULL END,
              claimed_at =
                CASE WHEN runs.claim_attempts < $3 THEN $1 ELSE NULL END,
              wake_at = NULL,
              claim_attempts =
                CASE
                  WHEN runs.claim_attempts < $3 THEN runs.claim_attempts + 1
                  ELSE runs.claim_attempts
                END,
              poisoned_at =
                CASE WHEN runs.claim_attempts < $3 THEN NULL ELSE $1 END,
              poison_reason =
                CASE
                  WHEN runs.claim_attempts < $3 THEN NULL
                  ELSE 'max_claim_attempts_exceeded'
                END
          FROM locked
          WHERE runs.id = locked.id
          RETURNING
            runs.run_id,
            runs.tenant_id,
            runs.graph_id,
            runs.graph_hash,
            runs.checkpoint_seq,
            runs.claim_token,
            runs.claimed_at,
            runs.claim_attempts,
            runs.poisoned_at,
            runs.poison_reason,
            locked.eligible_at
        )
        SELECT
          run_id,
          tenant_id,
          graph_id,
          graph_hash,
          checkpoint_seq,
          claim_token,
          claimed_at,
          claim_attempts,
          poisoned_at,
          poison_reason,
          'ready' AS class,
          eligible_at,
          (SELECT count(*) FROM first_page),
          0::bigint
        FROM updated
        ORDER BY run_id
        """
      end
    end

    defmodule PinnedRunStore do
      @moduledoc false

      def claim_due(
            %{concurrent_admission_probe: probe} = context,
            :system,
            policy
          ) do
        {repo, _prefix} = Docket.Postgres.Storage.context!(context)
        timeout = Map.get(probe, :timeout, 5_000)

        repo.checkout(
          fn ->
            %{rows: [[backend_pid]]} = repo.query!("SELECT pg_backend_pid()")

            send(
              probe.owner,
              {__MODULE__, probe.ref, :checked_out, probe.name, self(), backend_pid}
            )

            receive do
              {__MODULE__, ref, :go} when ref == probe.ref ->
                Docket.Postgres.RunStore.claim_due(context, :system, policy)
            after
              timeout -> {:error, {:concurrent_admission_timeout, probe.name, backend_pid}}
            end
          end,
          timeout: timeout
        )
      end

      defdelegate release_claim(context, scope, run_id, token, now),
        to: Docket.Postgres.RunStore
    end

    @doc false
    def run_callers!(repo, callers, opts \\ []) when is_list(callers) do
      timeout = Keyword.get(opts, :timeout, @default_timeout)
      require_observer_capacity!(repo, length(callers))
      parent = self()
      ref = make_ref()
      {:ok, task_supervisor} = Task.Supervisor.start_link()

      tasks =
        Enum.map(callers, fn {name, caller} when is_function(caller, 0) ->
          task =
            Task.Supervisor.async_nolink(task_supervisor, fn ->
              repo.checkout(
                fn ->
                  backend_pid = backend_pid!(repo)
                  send(parent, {__MODULE__, ref, :checked_out, name, self(), backend_pid})
                  await_go!(ref, name, timeout)
                  result = caller.()
                  send(parent, {__MODULE__, ref, :finished, name, backend_pid})
                  %{name: name, owner: self(), backend_pid: backend_pid, result: result}
                end,
                timeout: timeout
              )
            end)

          {name, task}
        end)

      try do
        checked_out = await_checked_out!(repo, ref, tasks, timeout)
        assert_distinct_backends!(checked_out)
        Enum.each(tasks, fn {_name, task} -> send(task.pid, {__MODULE__, ref, :go}) end)

        Enum.map(tasks, fn {name, task} ->
          case Task.yield(task, timeout) do
            {:ok, result} ->
              result

            {:exit, reason} ->
              raise "concurrent admission caller #{inspect(name)} exited: #{inspect(reason)}"

            nil ->
              fail_timeout!(repo, {:caller_finished, name}, ref, checked_out, tasks)
          end
        end)
      after
        Enum.each(tasks, fn {_name, task} ->
          if Process.alive?(task.pid), do: Task.shutdown(task, :brutal_kill)
        end)

        if Process.alive?(task_supervisor), do: Supervisor.stop(task_supervisor)
      end
    end

    @doc false
    def reproduce_known_bad_underclaim!(context, opts) do
      now = Keyword.fetch!(opts, :now)
      demand = Keyword.get(opts, :demand, 2)
      poller_count = Keyword.get(opts, :pollers, 2)
      timeout = Keyword.get(opts, :timeout, @default_timeout)
      {repo, prefix} = Storage.context!(context)
      require_observer_capacity!(repo, poller_count + 1)
      table = quoted_table(prefix, "docket_runs")
      parent = self()
      ref = make_ref()
      {:ok, blocker_supervisor} = Task.Supervisor.start_link()

      blocker =
        Task.Supervisor.async_nolink(blocker_supervisor, fn ->
          repo.transaction(
            fn ->
              set_timeouts!(repo, timeout * 2)
              backend_pid = backend_pid!(repo)
              send(parent, {__MODULE__, ref, :blocker_checked_out, backend_pid})

              rows =
                repo.query!(lock_ranked_page_statement(table), [now, demand], timeout: timeout).rows

              send(parent, {__MODULE__, ref, :blocker_locked, backend_pid, rows})
              await_release!(ref, timeout * 2)
              %{backend_pid: backend_pid, rows: rows}
            end,
            timeout: timeout * 2
          )
        end)

      try do
        {blocker_backend_pid, locked_rows} =
          await_blocker!(repo, ref, blocker, timeout)

        locked_partitions = Enum.map(locked_rows, fn [_run_id, scope_key] -> scope_key end)

        bad_context =
          Docket.Postgres.TestAdmissionContext.resolve(
            context,
            %{},
            implementation: KnownBadRankThenLock
          )

        policy = %{
          now: now,
          limit: demand,
          orphan_ttl_ms: 60_000,
          max_claim_attempts: 3
        }

        callers =
          for index <- 1..poller_count do
            {{:known_bad, index}, fn -> RunStore.claim_due(bad_context, :system, policy) end}
          end

        pollers = run_callers!(repo, callers, timeout: timeout)
        control = lockable_control!(repo, table, now, locked_partitions, timeout)

        %{
          demand: demand,
          blocker_backend_pid: blocker_backend_pid,
          locked:
            Enum.map(locked_rows, fn [run_id, scope_key] ->
              %{run_id: run_id, scope_key: scope_key}
            end),
          pollers: pollers,
          control: control
        }
      after
        send(blocker.pid, {__MODULE__, ref, :release})

        case Task.yield(blocker, timeout) || Task.shutdown(blocker, :brutal_kill) do
          {:ok, {:ok, _result}} -> :ok
          _ -> :ok
        end

        if Process.alive?(blocker_supervisor), do: Supervisor.stop(blocker_supervisor)
      end
    end

    @doc false
    def active_claims_by_partition(context) do
      {repo, prefix} = Storage.context!(context)
      table = quoted_table(prefix, "docket_runs")

      repo.query!("""
      SELECT scope_key, count(*)
      FROM #{table}
      WHERE status = 'running'
        AND poisoned_at IS NULL
        AND claim_token IS NOT NULL
      GROUP BY scope_key
      ORDER BY scope_key
      """).rows
      |> Map.new(fn [scope_key, count] -> {scope_key, count} end)
    end

    @doc false
    def assert_active_claims!(context, expected) when is_map(expected) do
      actual = active_claims_by_partition(context)

      if actual != expected do
        raise "active claims by partition mismatch: expected #{inspect(expected)}, got #{inspect(actual)}"
      end

      :ok
    end

    @doc false
    def outcome_count(%{leases: leases, poisoned: poisoned}),
      do: length(leases) + length(poisoned)

    @doc false
    def assert_full_demand!(batch, demand) when is_integer(demand) and demand > 0 do
      actual = outcome_count(batch)

      if actual != demand do
        raise "admission under-claimed: expected #{demand} outcomes, got #{actual}"
      end

      :ok
    end

    defp await_checked_out!(repo, ref, tasks, timeout) do
      expected = MapSet.new(tasks, &elem(&1, 0))
      deadline = System.monotonic_time(:millisecond) + timeout
      collect_checked_out!(repo, ref, tasks, expected, MapSet.size(expected), %{}, deadline)
    end

    defp collect_checked_out!(_repo, _ref, _tasks, _expected, expected_count, ready, _deadline)
         when map_size(ready) == expected_count,
         do: ready

    defp collect_checked_out!(repo, ref, tasks, expected, expected_count, ready, deadline) do
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {__MODULE__, ^ref, :checked_out, name, owner, backend_pid} ->
          if MapSet.member?(expected, name) do
            collect_checked_out!(
              repo,
              ref,
              tasks,
              expected,
              expected_count,
              Map.put(ready, name, %{owner: owner, backend_pid: backend_pid}),
              deadline
            )
          else
            collect_checked_out!(repo, ref, tasks, expected, expected_count, ready, deadline)
          end
      after
        remaining -> fail_timeout!(repo, :checked_out, ref, ready, tasks)
      end
    end

    defp await_blocker!(repo, ref, blocker, timeout, checked_out \\ %{}) do
      receive do
        {__MODULE__, ^ref, :blocker_checked_out, backend_pid} ->
          await_blocker!(
            repo,
            ref,
            blocker,
            timeout,
            %{blocker: %{owner: blocker.pid, backend_pid: backend_pid}}
          )

        {__MODULE__, ^ref, :blocker_locked, backend_pid, rows} ->
          {backend_pid, rows}
      after
        timeout ->
          fail_timeout!(repo, :blocker_locked, ref, checked_out, blocker: blocker)
      end
    end

    defp await_go!(ref, name, timeout) do
      receive do
        {__MODULE__, ^ref, :go} -> :ok
      after
        timeout ->
          raise "concurrent admission caller #{inspect(name)} timed out in phase checked_out"
      end
    end

    defp await_release!(ref, timeout) do
      receive do
        {__MODULE__, ^ref, :release} -> :ok
      after
        timeout -> raise "concurrent admission blocker timed out in phase blocker_locked"
      end
    end

    defp assert_distinct_backends!(checked_out) do
      backend_pids = Enum.map(checked_out, fn {_name, participant} -> participant.backend_pid end)

      if Enum.uniq(backend_pids) != backend_pids do
        raise "concurrent admission callers reused a PostgreSQL backend: #{inspect(checked_out)}"
      end
    end

    defp lockable_control!(repo, table, now, locked_partitions, timeout) do
      repo.transaction(
        fn ->
          set_timeouts!(repo, timeout)
          backend_pid = backend_pid!(repo)

          rows =
            repo.query!(
              """
              SELECT run_id, scope_key
              FROM #{table}
              WHERE status = 'running'
                AND poisoned_at IS NULL
                AND claim_token IS NULL
                AND wake_at <= $1
                AND NOT (scope_key = ANY($2::text[]))
              ORDER BY wake_at, id
              LIMIT 1
              FOR UPDATE SKIP LOCKED
              """,
              [now, locked_partitions],
              timeout: timeout
            ).rows

          %{backend_pid: backend_pid, rows: rows}
        end,
        timeout: timeout
      )
      |> case do
        {:ok, result} -> result
        {:error, reason} -> raise "underclaim control query rolled back: #{inspect(reason)}"
      end
    end

    defp lock_ranked_page_statement(table) do
      """
      WITH ranked AS MATERIALIZED (
        SELECT
          id,
          scope_key,
          wake_at AS eligible_at,
          ROW_NUMBER() OVER (
            PARTITION BY scope_key
            ORDER BY wake_at, id
          ) AS partition_rank
        FROM #{table}
        WHERE status = 'running'
          AND poisoned_at IS NULL
          AND claim_token IS NULL
          AND wake_at <= $1
      ),
      first_page AS MATERIALIZED (
        SELECT id, scope_key, eligible_at
        FROM ranked
        WHERE partition_rank = 1
        ORDER BY eligible_at, id
        LIMIT $2
      )
      SELECT runs.run_id, runs.scope_key
      FROM #{table} AS runs
      JOIN first_page ON first_page.id = runs.id
      ORDER BY first_page.eligible_at, runs.id
      FOR UPDATE OF runs
      """
    end

    defp require_observer_capacity!(repo, caller_count) do
      pool_size = Keyword.get(repo.config(), :pool_size, 10)

      if pool_size < caller_count + 1 do
        raise "concurrent admission harness requires pool_size >= #{caller_count + 1}, got #{pool_size}"
      end
    end

    defp set_timeouts!(repo, timeout) do
      repo.query!("SET LOCAL lock_timeout = '#{timeout}ms'")
      repo.query!("SET LOCAL statement_timeout = '#{timeout}ms'")
      repo.query!("SET LOCAL idle_in_transaction_session_timeout = '#{timeout * 2}ms'")
      :ok
    end

    defp backend_pid!(repo), do: repo.query!("SELECT pg_backend_pid()").rows |> hd() |> hd()

    defp fail_timeout!(repo, phase, ref, ready, tasks) do
      task_states =
        Map.new(tasks, fn {name, task} ->
          {name, %{pid: task.pid, alive?: Process.alive?(task.pid)}}
        end)

      diagnostics = activity_diagnostics(repo, ready)

      raise """
      concurrent admission timeout
      phase: #{inspect(phase)}
      barrier: #{inspect(ref)}
      participants: #{inspect(task_states)}
      checked_out: #{inspect(ready)}
      postgres: #{inspect(diagnostics)}
      """
    end

    defp activity_diagnostics(repo, ready) do
      backend_pids = Enum.map(ready, fn {_name, participant} -> participant.backend_pid end)

      if backend_pids == [] do
        []
      else
        repo.query!(
          """
          SELECT pid, state, wait_event_type, wait_event, pg_blocking_pids(pid)
          FROM pg_stat_activity
          WHERE pid = ANY($1::int[])
          ORDER BY pid
          """,
          [backend_pids],
          timeout: 1_000
        ).rows
      end
    rescue
      error -> {:diagnostics_unavailable, Exception.message(error)}
    end

    defp quoted_table(nil, table), do: quote_identifier(table)

    defp quoted_table(prefix, table),
      do: quote_identifier(prefix) <> "." <> quote_identifier(table)

    defp quote_identifier(identifier),
      do: ~s("#{String.replace(identifier, "\"", "\"\"")}")
  end
end
