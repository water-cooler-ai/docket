if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.ConcurrentAdmissionHarness do
    @moduledoc false

    alias Docket.Postgres.{RunStore, Storage}

    @default_timeout 5_000

    defmodule FairRotationOracle do
      @moduledoc false

      @unsuccessful_dispositions [:lock_skip, :cap_denied, :stale, :empty]

      @doc false
      def bounds!(opts) when is_list(opts) do
        target = Keyword.fetch!(opts, :target)
        cohort = opts |> Keyword.fetch!(:cohort) |> MapSet.new()
        hints = hints!(opts)
        hint_count = length(hints)
        scan_budget = positive!(opts, :scan_budget)
        quantum = positive!(opts, :quantum)
        lock_failures = non_negative!(opts, :lock_failures)
        population = MapSet.size(cohort)

        unless MapSet.member?(cohort, target) do
          raise ArgumentError, "fair-rotation cohort must contain the target"
        end

        hint_set = MapSet.new(hints)

        unless MapSet.subset?(cohort, hint_set) do
          raise ArgumentError, "fair-rotation hints must contain every cohort partition"
        end

        if population == 0 or population > hint_count do
          raise ArgumentError,
                "fair-rotation population must satisfy 1 <= A <= H, got A=#{population}, H=#{hint_count}"
        end

        grant_bound = (lock_failures + 1) * (population - 1)

        %{
          population: population,
          hints: hints,
          hint_count: hint_count,
          scan_budget: scan_budget,
          quantum: quantum,
          lock_failures: lock_failures,
          other_grants: grant_bound,
          other_outcomes: quantum * grant_bound,
          scan_calls:
            (lock_failures + 1) *
              (population - 1 + ceil_div(hint_count - population + 1, scan_budget))
        }
      end

      @doc false
      def assert_trace!(trace, opts) when is_list(trace) and is_list(opts) do
        target = Keyword.fetch!(opts, :target)
        cohort = opts |> Keyword.fetch!(:cohort) |> MapSet.new()
        bounds = bounds!(opts)
        committed = Enum.map(trace, &normalize_event!/1)
        assert_ordered_calls!(committed)
        target_index = target_grant_index!(committed, target)
        target_call = committed |> Enum.at(target_index) |> Map.fetch!(:call)
        through_target_call = Enum.take_while(committed, &(&1.call <= target_call))
        window = Enum.take(committed, target_index + 1)

        assert_cursor!(through_target_call, bounds.hints)
        assert_call_budgets!(through_target_call, bounds.scan_budget)
        assert_events!(window, cohort, bounds.quantum)
        assert_rounds!(window, target)

        target_failures =
          window
          |> Enum.drop(-1)
          |> Enum.count(&(&1.partition == target))

        if target_failures > bounds.lock_failures do
          fail!("target failed #{target_failures} inspections; L allows #{bounds.lock_failures}")
        end

        before_target = Enum.drop(window, -1)
        other_grants = Enum.count(before_target, &(&1.disposition == :grant))

        other_outcomes =
          before_target
          |> Enum.filter(&(&1.disposition == :grant))
          |> Enum.sum_by(& &1.outcomes)

        scan_calls = window |> Enum.map(& &1.call) |> Enum.uniq() |> length()

        assert_at_most!(other_grants, bounds.other_grants, "other-partition grants")
        assert_at_most!(other_outcomes, bounds.other_outcomes, "other-partition outcomes")
        assert_at_most!(scan_calls, bounds.scan_calls, "qualifying scan calls")

        Map.merge(bounds, %{
          observed_other_grants: other_grants,
          observed_other_outcomes: other_outcomes,
          observed_scan_calls: scan_calls
        })
      end

      defp normalize_event!(
             %{
               call: call,
               ordinal: ordinal,
               cursor_before: cursor_before,
               cursor_after: cursor_after,
               demand: demand,
               partition: partition,
               disposition: disposition,
               outcomes: outcomes,
               epoch_delta: epoch_delta
             } = event
           )
           when is_integer(call) and call > 0 and is_integer(ordinal) and ordinal > 0 and
                  is_integer(cursor_before) and cursor_before >= 0 and
                  is_integer(cursor_after) and cursor_after >= 0 and
                  is_integer(demand) and demand > 0 and
                  is_integer(outcomes) and outcomes >= 0 and
                  is_integer(epoch_delta) do
        if Map.get(event, :committed, true) != true do
          fail!("fair-rotation oracle accepts committed trace events only")
        end

        %{
          call: call,
          ordinal: ordinal,
          cursor_before: cursor_before,
          cursor_after: cursor_after,
          demand: demand,
          partition: partition,
          disposition: disposition,
          outcomes: outcomes,
          epoch_delta: epoch_delta
        }
      end

      defp normalize_event!(event),
        do: fail!("invalid fair-rotation trace event: #{inspect(event)}")

      defp target_grant_index!(events, target) do
        case Enum.find_index(events, &(&1.partition == target and &1.disposition == :grant)) do
          nil -> fail!("fair-rotation trace contains no committed target grant")
          index -> index
        end
      end

      defp assert_ordered_calls!(events) do
        positions = Enum.map(events, &{&1.call, &1.ordinal})

        if positions != Enum.sort(positions) or Enum.uniq(positions) != positions do
          fail!("fair-rotation trace is not ordered by database scan sequence")
        end

        Enum.each(Enum.group_by(events, & &1.call), fn {call, inspected} ->
          ordinals = Enum.map(inspected, & &1.ordinal)

          if ordinals != Enum.to_list(1..length(inspected)) do
            fail!("scan call #{call} has non-contiguous visit ordinals: #{inspect(ordinals)}")
          end
        end)
      end

      defp assert_call_budgets!(events, scan_budget) do
        Enum.each(Enum.group_by(events, & &1.call), fn {call, inspected} ->
          count = length(inspected)

          if count > scan_budget do
            fail!("scan call #{call} inspected #{count} positions; S=#{scan_budget}")
          end

          demands = inspected |> Enum.map(& &1.demand) |> Enum.uniq()

          if length(demands) != 1 do
            fail!("scan call #{call} has inconsistent demand: #{inspect(demands)}")
          end

          remaining =
            Enum.reduce(inspected, hd(demands), fn event, remaining ->
              if remaining == 0 do
                fail!("scan call #{call} continued after filling demand")
              end

              if event.outcomes > remaining do
                fail!("scan call #{call} returned more outcomes than remaining demand")
              end

              remaining - event.outcomes
            end)

          if remaining > 0 and count != scan_budget do
            fail!(
              "unfilled scan call #{call} advanced #{count} positions; expected full S=#{scan_budget}"
            )
          end
        end)
      end

      defp assert_cursor!(events, hints) do
        hint_count = length(hints)

        Enum.reduce(events, nil, fn event, prior_after ->
          if prior_after != nil and event.cursor_before != prior_after do
            fail!("scan cursor is not contiguous between visits")
          end

          unless event.cursor_before < hint_count and
                   event.cursor_after == rem(event.cursor_before + 1, hint_count) do
            fail!("scan cursor did not advance exactly one cyclic hint position")
          end

          unless Enum.at(hints, event.cursor_before) == event.partition do
            fail!("inspected partition does not match the frozen cursor position")
          end

          event.cursor_after
        end)

        :ok
      end

      defp assert_events!(events, cohort, quantum) do
        Enum.each(events, fn event ->
          case event.disposition do
            :grant ->
              unless MapSet.member?(cohort, event.partition) do
                fail!("partition outside the frozen cohort received a grant")
              end

              unless event.outcomes in 1..quantum do
                fail!("grant outcomes must be in 1..Q, got #{event.outcomes} with Q=#{quantum}")
              end

              if event.epoch_delta != 1 do
                fail!("a committed grant must advance admission_epoch exactly once")
              end

            disposition when disposition in @unsuccessful_dispositions ->
              if event.outcomes != 0 or event.epoch_delta != 0 do
                fail!(
                  "unsuccessful inspections cannot return outcomes or advance admission_epoch"
                )
              end

            disposition ->
              fail!("unknown fair-rotation disposition: #{inspect(disposition)}")
          end
        end)
      end

      defp assert_rounds!(events, target) do
        Enum.reduce(events, MapSet.new(), fn event, granted ->
          cond do
            event.partition == target ->
              MapSet.new()

            event.disposition == :grant and MapSet.member?(granted, event.partition) ->
              fail!(
                "partition #{inspect(event.partition)} received two grants in one target interval"
              )

            event.disposition == :grant ->
              MapSet.put(granted, event.partition)

            true ->
              granted
          end
        end)

        :ok
      end

      defp assert_at_most!(observed, bound, _label) when observed <= bound, do: :ok

      defp assert_at_most!(observed, bound, label),
        do: fail!("#{label} exceeded bound: observed #{observed}, bound #{bound}")

      defp positive!(opts, key) do
        case Keyword.fetch!(opts, key) do
          value when is_integer(value) and value > 0 ->
            value

          value ->
            raise ArgumentError, "#{key} must be a positive integer, got: #{inspect(value)}"
        end
      end

      defp non_negative!(opts, key) do
        case Keyword.fetch!(opts, key) do
          value when is_integer(value) and value >= 0 ->
            value

          value ->
            raise ArgumentError, "#{key} must be a non-negative integer, got: #{inspect(value)}"
        end
      end

      defp hints!(opts) do
        case Keyword.fetch!(opts, :hints) do
          hints when is_list(hints) and hints != [] ->
            if Enum.uniq(hints) == hints do
              hints
            else
              raise ArgumentError, "fair-rotation hints must be duplicate-free"
            end

          hints ->
            raise ArgumentError, "hints must be a non-empty list, got: #{inspect(hints)}"
        end
      end

      defp ceil_div(dividend, divisor), do: div(dividend + divisor - 1, divisor)

      defp fail!(message), do: raise(ArgumentError, message)
    end

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
