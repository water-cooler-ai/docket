if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.Backfill do
    @moduledoc """
    One-step, prefix-local claim-partition backfill.

    Each `advance/2` call owns one bounded root transaction and either captures
    a finite run-ID target, processes one ascending keyset page, performs the
    decisive reconciliation, or rechecks a completed rollout. Hosts cancel,
    delay, or throttle only between calls.
    """

    alias Docket.Postgres.ClaimPolicy.ControlContext

    @default_batch_size 1_000
    @max_batch_size 10_000
    @default_lock_timeout_ms 1_000
    @max_lock_timeout_ms 1_000
    @default_statement_timeout_ms 5_000
    @max_statement_timeout_ms 60_000

    @doc "Advances the durable partition backfill by one bounded unit."
    @spec advance(Docket.Backend.ctx(), keyword()) :: {:ok, map()} | {:error, term()}
    def advance(context, opts \\ []) do
      with {:ok, control} <- ControlContext.resolve(context, :mutate),
           {:ok, limits} <- validate_opts(opts) do
        control
        |> transact(limits)
        |> normalize_result()
      end
    end

    defp transact(control, limits) do
      case control.repo.transaction(fn ->
             configure_transaction(control.repo, limits)

             with {:ok, readiness} <- lock_gate(control),
                  :ok <- acquire_runner(control),
                  {:ok, state} <- lock_rollout(control) do
               if state.assertion_kind == "dual_write" do
                 execute_unit(control, limits, readiness, state)
               else
                 committed_error(control, state, :dual_write_unattested, false)
               end
             else
               {:error, reason} -> control.repo.rollback(reason)
             end
           end) do
        {:ok, value} -> {:ok, value}
        {:error, reason} -> {:error, reason}
      end
    rescue
      error in Postgrex.Error -> {:error, error}
      _error -> {:error, :backfill_failed}
    catch
      _kind, _reason -> {:error, :backfill_failed}
    end

    defp configure_transaction(repo, limits) do
      repo.query!("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE", [], log: false)

      repo.query!(
        "SELECT set_config('lock_timeout', $1, true)",
        [
          "#{limits.lock_timeout_ms}ms"
        ],
        log: false
      )

      repo.query!(
        "SELECT set_config('statement_timeout', $1, true)",
        [
          "#{limits.statement_timeout_ms}ms"
        ],
        log: false
      )

      :ok
    end

    defp lock_gate(control) do
      case control.repo.query(
             "SELECT readiness FROM #{control.identifiers.gate} WHERE id = 1 FOR SHARE NOWAIT",
             [],
             log: false
           ) do
        {:ok, %{rows: [[readiness]]}} -> {:ok, readiness}
        {:ok, _other} -> {:error, :invalid_admin_context}
        {:error, error} -> {:error, error}
      end
    end

    defp acquire_runner(control) do
      key = "docket-claim-partition-backfill-v1:" <> control.prefix

      case control.repo.query!("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))", [key],
             log: false
           ).rows do
        [[true]] -> :ok
        [[false]] -> {:error, :backfill_running}
        _ -> {:error, :invalid_admin_context}
      end
    end

    defp lock_rollout(control) do
      result =
        control.repo.query(
          """
          SELECT rollout.backfill_phase, rollout.backfill_target_id,
                 rollout.backfill_cursor, rollout.backfill_batches,
                 rollout.backfill_rows, rollout.backfill_retries,
                 rollout.missing_partition_count, rollout.backfill_completed_at,
                 rollout.backfill_last_error, rollout.updated_at,
                 assertion.assertion_kind
          FROM #{control.identifiers.rollout} AS rollout
          LEFT JOIN #{control.identifiers.assertions} AS assertion
            ON assertion.assertion_id = rollout.dual_write_assertion_id
          WHERE rollout.id = 1
          FOR UPDATE OF rollout
          """,
          [],
          log: false
        )

      case result do
        {:ok, %{rows: rows}} ->
          decode_rollout_rows(rows)

        {:error, error} ->
          case postgres_code(error) do
            code when code in [:lock_not_available, :lock_timeout] ->
              {:error, {:lock_timeout, :rollout}}

            :query_canceled ->
              {:error, :backfill_timeout}

            _ ->
              {:error, :backfill_failed}
          end
      end
    end

    defp decode_rollout_rows(rows) do
      case rows do
        [
          [
            phase,
            target,
            cursor,
            batches,
            scanned,
            retries,
            missing,
            completed,
            error,
            updated,
            kind
          ]
        ] ->
          {:ok,
           %{
             phase: phase,
             target_id: target,
             cursor: cursor,
             batches: batches,
             rows: scanned,
             retries: retries,
             missing_partition_count: missing,
             completed_at: completed,
             last_error: error,
             updated_at: updated,
             assertion_kind: kind
           }}

        _ ->
          {:error, :invalid_admin_context}
      end
    end

    defp execute_unit(control, limits, readiness, state) do
      control.repo.query!("SAVEPOINT docket_claim_backfill_unit", [], log: false)

      result =
        case state.phase do
          "not_started" ->
            initialize(control)

          "running" ->
            process_page(control, limits.batch_size, :running)

          "reconciling" ->
            if state.cursor < state.target_id,
              do: process_page(control, limits.batch_size, :reconciling),
              else: reconcile(control)

          "complete" ->
            recheck_complete(control, readiness)

          _ ->
            {:error, :invalid_admin_context}
        end

      case result do
        {:ok, value} ->
          control.repo.query!("RELEASE SAVEPOINT docket_claim_backfill_unit", [], log: false)
          value

        {:sql_error, error} ->
          rollback_unit(control)
          committed_error(control, state, classify_unit_error(error), true)

        {:error, reason} ->
          rollback_unit(control)
          committed_error(control, state, reason, false)
      end
    end

    defp rollback_unit(control) do
      control.repo.query!("ROLLBACK TO SAVEPOINT docket_claim_backfill_unit", [], log: false)
      control.repo.query!("RELEASE SAVEPOINT docket_claim_backfill_unit", [], log: false)
      :ok
    end

    defp initialize(control) do
      query(
        control,
        """
        WITH target AS (
          SELECT COALESCE(max(id), 0)::bigint AS target_id
          FROM #{control.identifiers.runs}
        )
        UPDATE #{control.identifiers.rollout} AS rollout
        SET backfill_phase = 'running',
            backfill_target_id = target.target_id,
            backfill_cursor = 0,
            backfill_last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        FROM target
        WHERE rollout.id = 1
        RETURNING rollout.backfill_phase, rollout.backfill_target_id,
                  rollout.backfill_cursor, rollout.backfill_batches,
                  rollout.backfill_rows, rollout.backfill_retries,
                  rollout.missing_partition_count, rollout.backfill_completed_at,
                  rollout.backfill_last_error, rollout.updated_at,
                  0::bigint, 0::bigint, NULL::bigint
        """,
        [],
        :advanced
      )
    end

    defp process_page(control, batch_size, phase) do
      running_phase = if phase == :running, do: "running", else: "reconciling"

      query(
        control,
        """
        WITH page AS MATERIALIZED (
          SELECT runs.id, runs.scope_key
          FROM #{control.identifiers.runs} AS runs
          CROSS JOIN #{control.identifiers.rollout} AS ledger
          WHERE ledger.id = 1
            AND runs.id > ledger.backfill_cursor
            AND runs.id <= ledger.backfill_target_id
          ORDER BY runs.id ASC
          LIMIT $1
        ),
        inserted AS (
          INSERT INTO #{control.identifiers.partitions} (scope_key)
          SELECT keys.scope_key
          FROM (
            SELECT DISTINCT page.scope_key COLLATE "C" AS scope_key
            FROM page
          ) AS keys
          ORDER BY keys.scope_key COLLATE "C" ASC
          ON CONFLICT (scope_key) DO NOTHING
          RETURNING scope_key
        ),
        stats AS (
          SELECT count(*)::bigint AS batch_rows,
                 COALESCE(max(id), 0)::bigint AS last_id
          FROM page
        ),
        insert_stats AS (
          SELECT count(*)::bigint AS inserted_partitions FROM inserted
        )
        UPDATE #{control.identifiers.rollout} AS rollout
        SET backfill_phase = CASE WHEN stats.batch_rows = 0 THEN 'reconciling' ELSE $2 END,
            backfill_cursor = CASE
              WHEN stats.batch_rows = 0 THEN rollout.backfill_target_id
              ELSE stats.last_id
            END,
            backfill_batches = rollout.backfill_batches +
              CASE WHEN stats.batch_rows = 0 THEN 0 ELSE 1 END,
            backfill_rows = rollout.backfill_rows + stats.batch_rows,
            backfill_last_error = CASE
              WHEN $2 = 'reconciling' THEN rollout.backfill_last_error
              ELSE NULL
            END,
            updated_at = CURRENT_TIMESTAMP
        FROM stats, insert_stats
        WHERE rollout.id = 1
        RETURNING rollout.backfill_phase, rollout.backfill_target_id,
                  rollout.backfill_cursor, rollout.backfill_batches,
                  rollout.backfill_rows, rollout.backfill_retries,
                  rollout.missing_partition_count, rollout.backfill_completed_at,
                  rollout.backfill_last_error, rollout.updated_at,
                  stats.batch_rows, insert_stats.inserted_partitions, NULL::bigint
        """,
        [batch_size, running_phase],
        :advanced
      )
    end

    defp reconcile(control) do
      query(
        control,
        """
        WITH observation AS (
          SELECT count(DISTINCT runs.scope_key)::bigint AS missing_count,
                 COALESCE((SELECT max(id) FROM #{control.identifiers.runs}), 0)::bigint AS next_target
          FROM #{control.identifiers.runs} AS runs
          CROSS JOIN #{control.identifiers.rollout} AS ledger
          WHERE ledger.id = 1
            AND runs.id <= ledger.backfill_target_id
            AND NOT EXISTS (
              SELECT 1
              FROM #{control.identifiers.partitions} AS partitions
              WHERE partitions.scope_key = runs.scope_key
            )
        )
        UPDATE #{control.identifiers.rollout} AS rollout
        SET backfill_phase = CASE WHEN observation.missing_count = 0 THEN 'complete' ELSE 'reconciling' END,
            backfill_target_id = CASE
              WHEN observation.missing_count = 0 THEN rollout.backfill_target_id
              ELSE observation.next_target
            END,
            backfill_cursor = CASE
              WHEN observation.missing_count = 0 THEN rollout.backfill_target_id
              ELSE 0
            END,
            backfill_completed_at = CASE
              WHEN observation.missing_count = 0 THEN CURRENT_TIMESTAMP
              ELSE NULL
            END,
          missing_partition_count = observation.missing_count,
          backfill_last_error = CASE
              WHEN observation.missing_count = 0 THEN NULL
              ELSE 'missing_partitions'
            END,
            updated_at = CURRENT_TIMESTAMP
        FROM observation
        WHERE rollout.id = 1
        RETURNING rollout.backfill_phase, rollout.backfill_target_id,
                  rollout.backfill_cursor, rollout.backfill_batches,
                  rollout.backfill_rows, rollout.backfill_retries,
                  rollout.missing_partition_count, rollout.backfill_completed_at,
                  rollout.backfill_last_error, rollout.updated_at,
                  0::bigint, 0::bigint, observation.missing_count
        """,
        [],
        :reconciled
      )
    end

    defp recheck_complete(control, readiness) do
      sql = """
      WITH observation AS (
        SELECT count(DISTINCT runs.scope_key)::bigint AS missing_count,
               COALESCE((SELECT max(id) FROM #{control.identifiers.runs}), 0)::bigint AS next_target
        FROM #{control.identifiers.runs} AS runs
        WHERE NOT EXISTS (
          SELECT 1
          FROM #{control.identifiers.partitions} AS partitions
          WHERE partitions.scope_key = runs.scope_key
        )
      )
      SELECT missing_count, next_target FROM observation
      """

      case control.repo.query(sql, [], log: false) do
        {:ok, %{rows: [[0, _target]]}} ->
          query(
            control,
            """
            UPDATE #{control.identifiers.rollout} AS rollout
            SET backfill_last_error = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            RETURNING rollout.backfill_phase, rollout.backfill_target_id,
                      rollout.backfill_cursor, rollout.backfill_batches,
                      rollout.backfill_rows, rollout.backfill_retries,
                      rollout.missing_partition_count, rollout.backfill_completed_at,
                      rollout.backfill_last_error, rollout.updated_at,
                      0::bigint, 0::bigint, 0::bigint
            """,
            [],
            :unchanged
          )

        {:ok, %{rows: [[missing, target]]}} when missing > 0 and readiness == "not_ready" ->
          query(
            control,
            """
            UPDATE #{control.identifiers.rollout} AS rollout
            SET backfill_phase = 'reconciling', backfill_target_id = $1, backfill_cursor = 0,
              backfill_completed_at = NULL, missing_partition_count = $2,
                backfill_last_error = 'missing_partitions', updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            RETURNING rollout.backfill_phase, rollout.backfill_target_id,
                      rollout.backfill_cursor, rollout.backfill_batches,
                      rollout.backfill_rows, rollout.backfill_retries,
                      rollout.missing_partition_count, rollout.backfill_completed_at,
                      rollout.backfill_last_error, rollout.updated_at,
                      0::bigint, 0::bigint, $2::bigint
            """,
            [target, missing],
            :repairing
          )

        {:ok, %{rows: [[missing, _target]]}} when missing > 0 ->
          {:error, :prefix_ready}

        {:ok, _other} ->
          {:error, :invalid_admin_context}

        {:error, error} ->
          {:sql_error, error}
      end
    end

    defp query(control, sql, params, outcome) do
      case control.repo.query(sql, params, log: false) do
        {:ok, %{rows: [row]}} -> {:ok, decode_result(row, outcome)}
        {:ok, _other} -> {:error, :invalid_admin_context}
        {:error, error} -> {:sql_error, error}
      end
    end

    defp decode_result(
           [
             phase,
             target,
             cursor,
             batches,
             rows,
             retries,
             missing,
             completed,
             last_error,
             updated,
             batch_rows,
             inserted,
             observed_missing
           ],
           outcome
         ) do
      %{
        outcome: outcome,
        phase: decode_phase(phase),
        target_id: target,
        cursor: cursor,
        batches: batches,
        rows: rows,
        retries: retries,
        missing_partition_count: missing,
        completed_at: completed,
        last_error: decode_error(last_error),
        updated_at: updated,
        batch_rows: batch_rows,
        inserted_partitions: inserted,
        observed_missing_partitions: observed_missing
      }
    end

    defp committed_error(control, state, reason, retry?) do
      error = persisted_error(reason)

      case control.repo.query!(
             """
             UPDATE #{control.identifiers.rollout}
             SET backfill_retries = backfill_retries + $2,
                 backfill_last_error = $1,
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = 1
             RETURNING backfill_retries
             """,
             [error, if(retry?, do: 1, else: 0)],
             log: false
           ).rows do
        [[_retries]] -> {:backfill_error, reason}
        _ -> {:backfill_error, Map.get(state, :phase) && :invalid_admin_context}
      end
    end

    defp validate_opts(opts) when is_list(opts) do
      if Keyword.keyword?(opts) and Enum.uniq(Keyword.keys(opts)) == Keyword.keys(opts) and
           Enum.all?(
             Keyword.keys(opts),
             &(&1 in [:batch_size, :lock_timeout_ms, :statement_timeout_ms])
           ) do
        batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
        lock_timeout = Keyword.get(opts, :lock_timeout_ms, @default_lock_timeout_ms)

        statement_timeout =
          Keyword.get(opts, :statement_timeout_ms, @default_statement_timeout_ms)

        if is_integer(batch_size) and batch_size in 1..@max_batch_size and
             is_integer(lock_timeout) and lock_timeout in 1..@max_lock_timeout_ms and
             is_integer(statement_timeout) and statement_timeout in 1..@max_statement_timeout_ms do
          {:ok,
           %{
             batch_size: batch_size,
             lock_timeout_ms: lock_timeout,
             statement_timeout_ms: statement_timeout
           }}
        else
          {:error, :invalid_backfill_options}
        end
      else
        {:error, :invalid_backfill_options}
      end
    end

    defp validate_opts(_opts), do: {:error, :invalid_backfill_options}

    defp normalize_result({:ok, {:backfill_error, reason}}),
      do: normalize_result({:error, reason})

    defp normalize_result({:ok, value}), do: {:ok, value}

    defp normalize_result({:error, %Postgrex.Error{} = error}) do
      case postgres_code(error) do
        :lock_not_available -> {:error, {:lock_timeout, :gate}}
        :lock_timeout -> {:error, {:lock_timeout, :rollout}}
        :query_canceled -> {:error, :backfill_timeout}
        _ -> {:error, :backfill_failed}
      end
    end

    defp normalize_result({:error, reason})
         when reason in [
                :backfill_running,
                :dual_write_unattested,
                :backfill_lock_timeout,
                :backfill_timeout,
                :backfill_failed,
                :prefix_ready,
                :invalid_admin_context,
                :transaction_context_forbidden,
                :invalid_backfill_options
              ],
         do: {:error, reason}

    defp normalize_result({:error, {:lock_timeout, authority}})
         when authority in [:gate, :rollout],
         do: {:error, {:lock_timeout, authority}}

    defp normalize_result({:error, _reason}), do: {:error, :backfill_failed}

    defp classify_unit_error(error) do
      case postgres_code(error) do
        code when code in [:lock_not_available, :lock_timeout] -> :backfill_lock_timeout
        :query_canceled -> :backfill_timeout
        _ -> :backfill_failed
      end
    end

    defp persisted_error(:dual_write_unattested), do: "dual_write_unattested"
    defp persisted_error(:backfill_lock_timeout), do: "lock_timeout"
    defp persisted_error(:backfill_timeout), do: "statement_timeout"
    defp persisted_error(:prefix_ready), do: "prefix_ready"
    defp persisted_error(_reason), do: "backfill_failed"

    defp decode_error(nil), do: nil
    defp decode_error("dual_write_unattested"), do: :dual_write_unattested
    defp decode_error("lock_timeout"), do: :backfill_lock_timeout
    defp decode_error("statement_timeout"), do: :backfill_timeout
    defp decode_error("missing_partitions"), do: :missing_partitions
    defp decode_error("prefix_ready"), do: :prefix_ready
    defp decode_error("backfill_failed"), do: :backfill_failed

    defp decode_phase("not_started"), do: :not_started
    defp decode_phase("running"), do: :running
    defp decode_phase("reconciling"), do: :reconciling
    defp decode_phase("complete"), do: :complete

    defp postgres_code(%Postgrex.Error{postgres: postgres}) when is_map(postgres),
      do: Map.get(postgres, :code)

    defp postgres_code(_error), do: nil
  end
end
