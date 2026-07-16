if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.OnlineMigration do
    @moduledoc """
    Resumable, nontransactional v2 online DDL for one PostgreSQL prefix.

    Host migrations must set `@disable_ddl_transaction true`. `up/1` also
    takes a prefix-derived session advisory lock, so a second migration runner
    fails promptly instead of performing concurrent repair work. Each DDL
    boundary is autocommitted and followed by a durable rollout checkpoint.
    Interrupted invalid same-name indexes are dropped concurrently and rebuilt.
    """

    alias Docket.Postgres.ClaimPolicy.OnlineDDL
    alias Docket.Postgres.Storage

    @default_prefix "public"
    @default_lock_timeout_ms 1_000
    @max_lock_timeout_ms 1_000
    @min_statement_timeout_ms 1_000
    @default_statement_timeout_ms 300_000
    @max_statement_timeout_ms 3_600_000

    @type evidence :: %{
            ready_index_valid: boolean(),
            live_index_valid: boolean(),
            fk_disposition: :absent | :not_valid | :validated,
            fk_definition_valid: boolean()
          }

    @doc "Runs or resumes every online DDL phase for a prefix."
    @spec up(keyword()) :: :ok
    def up(opts \\ []) do
      %{repo: repo} = config = validate_opts!(opts)
      assert_repo_migration_lock!(repo)

      if function_exported?(repo, :in_transaction?, 0) and repo.in_transaction?() do
        raise ArgumentError,
              "Docket online migration cannot run inside a transaction; " <>
                "set @disable_ddl_transaction true"
      end

      repo.checkout(fn ->
        previous = capture_session(repo)
        Process.put({__MODULE__, :query_timeout}, config.statement_timeout_ms + 5_000)
        Process.put({__MODULE__, :runner_acquired}, false)

        try do
          configure_session(repo, config)
          acquire_runner!(repo, config.prefix)
          Process.put({__MODULE__, :runner_acquired}, true)
          run_phases!(repo, config.prefix)
        after
          try do
            cleanup_session!(repo, config.prefix, previous)
          after
            Process.delete({__MODULE__, :runner_acquired})
            Process.delete({__MODULE__, :query_timeout})
          end
        end
      end)
    end

    @doc "Reverses unactivated online objects; activation history requires destructive teardown."
    @spec down(keyword()) :: :ok
    def down(opts \\ []) do
      %{repo: repo} = config = validate_opts!(opts)
      assert_repo_migration_lock!(repo)

      if function_exported?(repo, :in_transaction?, 0) and repo.in_transaction?() do
        raise ArgumentError,
              "Docket online migration cannot run inside a transaction; " <>
                "set @disable_ddl_transaction true"
      end

      repo.checkout(fn ->
        previous = capture_session(repo)
        Process.put({__MODULE__, :query_timeout}, config.statement_timeout_ms + 5_000)
        Process.put({__MODULE__, :runner_acquired}, false)

        try do
          configure_session(repo, config)
          acquire_runner!(repo, config.prefix)
          Process.put({__MODULE__, :runner_acquired}, true)
          assert_reversible!(repo, config.prefix)
          assert_down_objects_safe!(repo, config.prefix)
          pause_after_down_guard!()
          drop_foreign_key(repo, config.prefix)
          drop_index_if_present(repo, config.prefix, :live)
          drop_index_if_present(repo, config.prefix, :ready)
          reset_rollout!(repo, config.prefix)
        after
          try do
            cleanup_session!(repo, config.prefix, previous)
          after
            Process.delete({__MODULE__, :runner_acquired})
            Process.delete({__MODULE__, :query_timeout})
          end
        end
      end)
    end

    @doc false
    @spec inspect_state(module(), String.t()) :: {:ok, evidence()} | {:error, atom()}
    def inspect_state(repo, prefix) do
      with true <- Storage.valid_prefix?(prefix),
           {:ok, ready} <- inspect_index(repo, prefix, :ready),
           {:ok, live} <- inspect_index(repo, prefix, :live) do
        {fk, fk_definition_valid} =
          case inspect_foreign_key(repo, prefix) do
            {:ok, disposition} -> {disposition, true}
            {:error, :definition_mismatch} -> {:absent, false}
          end

        {:ok,
         %{
           ready_index_valid: ready == :valid,
           live_index_valid: live == :valid,
           fk_disposition: fk,
           fk_definition_valid: fk_definition_valid
         }}
      else
        _ -> {:error, :online_inspection_failed}
      end
    rescue
      error -> {:error, {:online_inspection_failed, error}}
    end

    defp run_phases!(repo, prefix) do
      phase!(repo, prefix, :ready_index, fn -> ensure_index!(repo, prefix, :ready) end)
      phase!(repo, prefix, :live_index, fn -> ensure_index!(repo, prefix, :live) end)
      phase!(repo, prefix, :foreign_key, fn -> ensure_foreign_key!(repo, prefix) end)
      phase!(repo, prefix, :foreign_key_validation, fn -> validate_foreign_key!(repo, prefix) end)
      :ok
    end

    defp phase!(repo, prefix, phase, operation) do
      try do
        mark_attempt!(repo, prefix)
        operation.()
        inject_after_ddl_failure!(phase)
        sync_evidence!(repo, prefix)
      rescue
        error ->
          persist_error(repo, prefix, classify_error(error, phase))
          reraise error, __STACKTRACE__
      end
    end

    defp inject_after_ddl_failure!(phase) do
      if Process.get({__MODULE__, :fail_after_ddl}) == phase do
        raise "injected Docket online interruption after committed #{phase} DDL"
      end
    end

    defp pause_after_down_guard! do
      case Process.get({__MODULE__, :pause_after_down_guard}) do
        {owner, token} when is_pid(owner) ->
          send(owner, {:docket_online_down_guarded, token})

          receive do
            {:continue_docket_online_down, ^token} -> :ok
          end

        _ ->
          :ok
      end
    end

    defp ensure_index!(repo, prefix, kind) do
      case inspect_index(repo, prefix, kind) do
        {:ok, :valid} ->
          :ok

        {:ok, :absent} ->
          assert_online_mutation_safe!(repo, prefix)
          repo.query!(OnlineDDL.create_index_sql(prefix, kind), [], online_query_opts())
          :ok

        {:ok, :invalid} ->
          assert_online_mutation_safe!(repo, prefix)
          repo.query!(OnlineDDL.drop_index_sql(prefix, kind), [], online_query_opts())
          repo.query!(OnlineDDL.create_index_sql(prefix, kind), [], online_query_opts())
          :ok

        {:ok, :definition_mismatch} ->
          raise "Docket #{kind} same-name index does not match the approved definition"
      end
    end

    defp ensure_foreign_key!(repo, prefix) do
      assert_foreign_key_prerequisites!(repo, prefix)

      case inspect_foreign_key(repo, prefix) do
        {:ok, :absent} ->
          assert_online_mutation_safe!(repo, prefix)
          repo.query!(OnlineDDL.add_foreign_key_sql(prefix), [], online_query_opts())
          :ok

        {:ok, disposition} when disposition in [:not_valid, :validated] ->
          :ok

        {:error, reason} ->
          raise "Docket claim-partition foreign key conflicts with the approved definition: " <>
                  inspect(reason)
      end
    end

    defp validate_foreign_key!(repo, prefix) do
      assert_foreign_key_prerequisites!(repo, prefix)

      case inspect_foreign_key(repo, prefix) do
        {:ok, :validated} ->
          :ok

        {:ok, :not_valid} ->
          assert_online_mutation_safe!(repo, prefix)
          repo.query!(OnlineDDL.validate_foreign_key_sql(prefix), [], online_query_opts())

        {:ok, :absent} ->
          raise "Docket claim-partition foreign key is absent"

        {:error, reason} ->
          raise "Docket claim-partition foreign key is invalid: #{inspect(reason)}"
      end
    end

    defp assert_foreign_key_prerequisites!(repo, prefix) do
      rollout = table(prefix, "docket_claim_rollout")
      assertions = table(prefix, "docket_claim_assertions")
      runs = table(prefix, "docket_runs")
      partitions = table(prefix, "docket_claim_partitions")

      sql = """
      SELECT
        assertion.assertion_kind = 'dual_write',
        rollout.backfill_phase = 'complete',
        rollout.missing_partition_count = 0,
        NOT EXISTS (
          SELECT 1 FROM #{runs} AS runs
          WHERE NOT EXISTS (
            SELECT 1 FROM #{partitions} AS partitions
            WHERE partitions.scope_key = runs.scope_key
          )
        )
      FROM #{rollout} AS rollout
      LEFT JOIN #{assertions} AS assertion
        ON assertion.assertion_id = rollout.dual_write_assertion_id
      WHERE rollout.id = 1
      """

      case repo.query!(sql, [], online_query_opts()).rows do
        [[true, true, true, true]] ->
          :ok

        _ ->
          raise "Docket online foreign key requires dual-write attestation and final zero reconciliation"
      end
    end

    defp inspect_index(repo, prefix, kind) do
      sql = """
      SELECT class.relkind::text, index.indisvalid, index.indisready, index.indislive,
             access.amname, index.indisunique, index.indisexclusion,
             index.indisprimary, index.indimmediate,
             NOT EXISTS (
               SELECT 1 FROM pg_constraint AS owner
               WHERE owner.conindid = class.oid
             ),
             NOT EXISTS (
               SELECT 1
               FROM pg_depend AS dependency
               JOIN pg_constraint AS owner ON owner.oid = dependency.refobjid
               WHERE dependency.classid = 'pg_class'::regclass
                 AND dependency.objid = class.oid
                 AND dependency.refclassid = 'pg_constraint'::regclass
             ),
             source_namespace.nspname, source.relname,
             source.oid = to_regclass(format('%I.%I', $1::text, 'docket_runs')),
             index.indnatts, index.indnkeyatts,
             ARRAY(
               SELECT attribute.attname::text
               FROM unnest(index.indkey) WITH ORDINALITY AS key(attnum, position)
               JOIN pg_attribute AS attribute
                 ON attribute.attrelid = index.indrelid AND attribute.attnum = key.attnum
               WHERE key.position <= index.indnkeyatts
               ORDER BY key.position
             ),
             ARRAY(
               SELECT opclass.opcdefault AND opclass.opcmethod = access.oid AND
                      opclass.opcintype = attribute.atttypid
               FROM unnest(index.indclass) WITH ORDINALITY AS item(opclass_oid, position)
               JOIN pg_opclass AS opclass ON opclass.oid = item.opclass_oid
               JOIN unnest(index.indkey) WITH ORDINALITY AS key(attnum, position)
                 USING (position)
               JOIN pg_attribute AS attribute
                 ON attribute.attrelid = index.indrelid AND attribute.attnum = key.attnum
               ORDER BY item.position
             ),
             ARRAY(
               SELECT item.collation_oid = attribute.attcollation
               FROM unnest(index.indcollation) WITH ORDINALITY AS item(collation_oid, position)
               JOIN unnest(index.indkey) WITH ORDINALITY AS key(attnum, position)
                 USING (position)
               JOIN pg_attribute AS attribute
                 ON attribute.attrelid = index.indrelid AND attribute.attnum = key.attnum
               ORDER BY item.position
             ),
             ARRAY(SELECT option = 0 FROM unnest(index.indoption) AS option),
             pg_get_expr(index.indpred, index.indrelid)
      FROM pg_class AS class
      JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
      LEFT JOIN pg_index AS index ON index.indexrelid = class.oid
      LEFT JOIN pg_am AS access ON access.oid = class.relam
      LEFT JOIN pg_class AS source ON source.oid = index.indrelid
      LEFT JOIN pg_namespace AS source_namespace ON source_namespace.oid = source.relnamespace
      WHERE namespace.nspname = $1 AND class.relname = $2
      """

      case repo.query!(sql, [prefix, OnlineDDL.index_name(kind)], online_query_opts()).rows do
        [] ->
          {:ok, :absent}

        [
          [
            "i",
            valid,
            ready,
            live,
            "btree",
            false,
            false,
            false,
            true,
            true,
            true,
            ^prefix,
            "docket_runs",
            true,
            3,
            3,
            columns,
            opclasses,
            collations,
            options,
            predicate
          ]
        ] ->
          cond do
            columns != OnlineDDL.columns(kind) ->
              {:ok, :definition_mismatch}

            opclasses != [true, true, true] ->
              {:ok, :definition_mismatch}

            collations != [true, true, true] ->
              {:ok, :definition_mismatch}

            options != [true, true, true] ->
              {:ok, :definition_mismatch}

            predicate != OnlineDDL.catalog_predicate(kind) ->
              {:ok, :definition_mismatch}

            not valid or not ready or not live ->
              {:ok, :invalid}

            true ->
              {:ok, :valid}
          end

        [_other] ->
          {:ok, :definition_mismatch}
      end
    end

    defp inspect_foreign_key(repo, prefix) do
      sql = """
      SELECT con.contype::text, con.convalidated,
             con.confmatchtype::text, con.condeferrable,
             con.condeferred, con.confupdtype::text,
             con.confdeltype::text,
             source_namespace.nspname, source.relname,
             target_namespace.nspname, target.relname,
             ARRAY(
               SELECT attribute.attname::text
               FROM unnest(con.conkey) WITH ORDINALITY AS key(attnum, position)
               JOIN pg_attribute AS attribute
                 ON attribute.attrelid = con.conrelid AND attribute.attnum = key.attnum
               ORDER BY key.position
             ),
             ARRAY(
               SELECT attribute.attname::text
               FROM unnest(con.confkey) WITH ORDINALITY AS key(attnum, position)
               JOIN pg_attribute AS attribute
                 ON attribute.attrelid = con.confrelid AND attribute.attnum = key.attnum
               ORDER BY key.position
             )
      FROM pg_constraint AS con
      JOIN pg_class AS source ON source.oid = con.conrelid
      JOIN pg_namespace AS source_namespace ON source_namespace.oid = source.relnamespace
      LEFT JOIN pg_class AS target ON target.oid = con.confrelid
      LEFT JOIN pg_namespace AS target_namespace ON target_namespace.oid = target.relnamespace
      WHERE source_namespace.nspname = $1 AND source.relname = 'docket_runs'
        AND con.conname = $2
      """

      case repo.query!(sql, [prefix, OnlineDDL.foreign_key_name()], online_query_opts()).rows do
        [] ->
          {:ok, :absent}

        [
          [
            "f",
            validated,
            "s",
            false,
            false,
            "r",
            "r",
            ^prefix,
            "docket_runs",
            ^prefix,
            "docket_claim_partitions",
            ["scope_key"],
            ["scope_key"]
          ]
        ] ->
          {:ok, if(validated, do: :validated, else: :not_valid)}

        [_other] ->
          {:error, :definition_mismatch}
      end
    end

    defp sync_evidence!(repo, prefix) do
      {:ok, evidence} = inspect_state(repo, prefix)
      rollout = table(prefix, "docket_claim_rollout")
      ready_hash = OnlineDDL.index_fingerprint(prefix, :ready)
      live_hash = OnlineDDL.index_fingerprint(prefix, :live)

      {phase, completed?} =
        cond do
          evidence.fk_disposition == :validated and evidence.ready_index_valid and
              evidence.live_index_valid ->
            {"complete", true}

          evidence.fk_disposition == :not_valid and evidence.ready_index_valid and
              evidence.live_index_valid ->
            {"fk_not_valid", false}

          evidence.ready_index_valid and evidence.live_index_valid ->
            {"live_index", false}

          evidence.ready_index_valid ->
            {"ready_index", false}

          true ->
            {"not_started", false}
        end

      repo.query!(
        """
        UPDATE #{rollout}
        SET online_phase = $1,
            ready_index_valid = $2,
            live_index_valid = $3,
            ready_index_ddl_sha256 = CASE WHEN $2 THEN $4::bytea ELSE NULL END,
            live_index_ddl_sha256 = CASE WHEN $3 THEN $5::bytea ELSE NULL END,
            fk_disposition = $6,
            online_last_error = NULL,
            online_completed_at = CASE WHEN $7 THEN COALESCE(online_completed_at, CURRENT_TIMESTAMP) ELSE NULL END,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """,
        [
          phase,
          evidence.ready_index_valid,
          evidence.live_index_valid,
          ready_hash,
          live_hash,
          Atom.to_string(evidence.fk_disposition),
          completed?
        ],
        online_query_opts()
      )

      :ok
    end

    defp mark_attempt!(repo, prefix) do
      repo.query!(
        """
        UPDATE #{table(prefix, "docket_claim_rollout")}
        SET online_attempts = online_attempts + 1,
            online_started_at = COALESCE(online_started_at, CURRENT_TIMESTAMP),
            online_last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """,
        [],
        online_query_opts()
      )
    end

    defp persist_error(repo, prefix, error) do
      _ =
        repo.query(
          """
          UPDATE #{table(prefix, "docket_claim_rollout")}
          SET online_last_error = $1, updated_at = CURRENT_TIMESTAMP
          WHERE id = 1
          """,
          [Atom.to_string(error)],
          online_query_opts()
        )

      :ok
    end

    defp classify_error(%Postgrex.Error{postgres: postgres}, _phase) do
      case Map.get(postgres || %{}, :code) do
        :query_canceled -> :statement_timeout
        code when code in [:lock_not_available, :lock_timeout] -> :lock_timeout
        _ -> :database_error
      end
    end

    defp classify_error(_error, :ready_index), do: :ready_index_failed
    defp classify_error(_error, :live_index), do: :live_index_failed
    defp classify_error(_error, :foreign_key), do: :foreign_key_failed
    defp classify_error(_error, :foreign_key_validation), do: :foreign_key_validation_failed

    defp assert_reversible!(repo, prefix) do
      rows =
        repo.query!(
          """
          SELECT readiness, readiness_epoch, admission_mode, mode_epoch
          FROM #{table(prefix, "docket_claim_admission_gate")}
          WHERE id = 1
          """,
          [],
          online_query_opts()
        ).rows

      unless rows == [["not_ready", 0, "legacy", 0]] do
        raise "Docket online down refused: readiness or activation history requires the explicit destructive teardown contract"
      end
    end

    defp assert_down_objects_safe!(repo, prefix) do
      case inspect_foreign_key(repo, prefix) do
        {:ok, disposition} when disposition in [:absent, :not_valid, :validated] ->
          :ok

        {:error, reason} ->
          raise "Docket online down found a conflicting foreign key: #{inspect(reason)}"
      end

      Enum.each([:live, :ready], fn kind ->
        case inspect_index(repo, prefix, kind) do
          {:ok, state} when state in [:absent, :invalid, :valid] ->
            :ok

          {:ok, :definition_mismatch} ->
            raise "Docket online down refuses to drop a same-name index with a foreign definition"
        end
      end)
    end

    defp drop_foreign_key(repo, prefix) do
      case inspect_foreign_key(repo, prefix) do
        {:ok, :absent} ->
          :ok

        {:ok, disposition} when disposition in [:not_valid, :validated] ->
          repo.query!(
            "ALTER TABLE #{table(prefix, "docket_runs")} DROP CONSTRAINT #{OnlineDDL.foreign_key_name()}",
            [],
            online_query_opts()
          )

        {:error, reason} ->
          raise "Docket online down found a conflicting foreign key: #{inspect(reason)}"
      end
    end

    defp drop_index_if_present(repo, prefix, kind) do
      case inspect_index(repo, prefix, kind) do
        {:ok, :absent} ->
          :ok

        {:ok, :definition_mismatch} ->
          raise "Docket online down refuses to drop a same-name index with a foreign definition"

        {:ok, _present} ->
          repo.query!(OnlineDDL.drop_index_sql(prefix, kind), [], online_query_opts())
      end
    end

    defp reset_rollout!(repo, prefix) do
      repo.query!(
        """
        UPDATE #{table(prefix, "docket_claim_rollout")}
        SET online_phase = 'not_started', online_attempts = 0, online_last_error = NULL,
            online_started_at = NULL, online_completed_at = NULL,
            ready_index_valid = false, live_index_valid = false,
            ready_index_ddl_sha256 = NULL, live_index_ddl_sha256 = NULL,
            fk_disposition = 'absent', verified_default_fingerprint = NULL,
            verified_at = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """,
        [],
        online_query_opts()
      )

      :ok
    end

    defp configure_session(repo, config) do
      repo.query!(
        "SELECT set_config('lock_timeout', $1, false)",
        ["#{config.lock_timeout_ms}ms"],
        online_query_opts()
      )

      repo.query!(
        "SELECT set_config('statement_timeout', $1, false)",
        ["#{config.statement_timeout_ms}ms"],
        online_query_opts()
      )

      :ok
    end

    defp capture_session(repo) do
      [[lock_timeout]] = repo.query!("SHOW lock_timeout", [], online_query_opts()).rows
      [[statement_timeout]] = repo.query!("SHOW statement_timeout", [], online_query_opts()).rows
      %{lock_timeout: lock_timeout, statement_timeout: statement_timeout}
    end

    defp cleanup_session!(repo, prefix, previous) do
      cleanup_opts = [timeout: :infinity, log: false]

      # A server-side timeout can fire at the DDL boundary. Disable it before
      # cleanup so the pooled session cannot escape with Docket's settings or
      # session advisory lock still attached.
      repo.query!("SELECT set_config('statement_timeout', '0', false)", [], cleanup_opts)

      try do
        if Process.get({__MODULE__, :runner_acquired}, false) do
          case repo.query!(
                 "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                 [runner_key(prefix)],
                 cleanup_opts
               ).rows do
            [[true]] -> :ok
            rows -> raise "Docket online migration failed to release its runner: #{inspect(rows)}"
          end
        end
      after
        repo.query!(
          "SELECT set_config('lock_timeout', $1, false), set_config('statement_timeout', $2, false)",
          [previous.lock_timeout, previous.statement_timeout],
          cleanup_opts
        )
      end

      :ok
    end

    defp assert_repo_migration_lock!(repo) do
      unless Keyword.get(repo.config(), :migration_lock) == :pg_advisory_lock do
        raise ArgumentError,
              "Docket online migration requires the Repo option migration_lock: :pg_advisory_lock"
      end
    end

    defp assert_online_mutation_safe!(repo, prefix) do
      case repo.query!(
             "SELECT readiness FROM #{table(prefix, "docket_claim_admission_gate")} WHERE id = 1",
             [],
             online_query_opts()
           ).rows do
        [["not_ready"]] -> :ok
        _ -> raise "Docket online DDL repair requires a not-ready prefix"
      end
    end

    defp online_query_opts do
      [
        timeout: Process.get({__MODULE__, :query_timeout}, @default_statement_timeout_ms + 5_000),
        log: false
      ]
    end

    defp acquire_runner!(repo, prefix) do
      key = runner_key(prefix)

      case repo.query!(
             "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
             [key],
             online_query_opts()
           ).rows do
        [[true]] ->
          :ok

        [[false]] ->
          raise "Docket online migration already has a runner for prefix #{inspect(prefix)}"

        _ ->
          raise "Docket online migration could not acquire its runner"
      end
    end

    defp runner_key(prefix), do: "docket-v2-online-migration-v1:" <> prefix

    defp validate_opts!(opts) do
      allowed = [:lock_timeout_ms, :prefix, :repo, :statement_timeout_ms]

      unless Keyword.keyword?(opts) and Enum.all?(Keyword.keys(opts), &(&1 in allowed)) do
        raise ArgumentError, "invalid Docket online migration options"
      end

      repo = Keyword.get_lazy(opts, :repo, &Ecto.Migration.repo/0)
      prefix = Keyword.get(opts, :prefix, @default_prefix)
      lock_timeout_ms = Keyword.get(opts, :lock_timeout_ms, @default_lock_timeout_ms)

      statement_timeout_ms =
        Keyword.get(opts, :statement_timeout_ms, @default_statement_timeout_ms)

      unless is_atom(repo), do: raise(ArgumentError, ":repo must be an Ecto repository module")

      unless Storage.valid_prefix?(prefix) do
        raise ArgumentError, "invalid Docket online migration prefix: #{inspect(prefix)}"
      end

      unless is_integer(lock_timeout_ms) and lock_timeout_ms in 1..@max_lock_timeout_ms do
        raise ArgumentError, ":lock_timeout_ms must be in 1..#{@max_lock_timeout_ms}"
      end

      unless is_integer(statement_timeout_ms) and
               statement_timeout_ms in @min_statement_timeout_ms..@max_statement_timeout_ms do
        raise ArgumentError,
              ":statement_timeout_ms must be in #{@min_statement_timeout_ms}..#{@max_statement_timeout_ms}"
      end

      %{
        repo: repo,
        prefix: prefix,
        lock_timeout_ms: lock_timeout_ms,
        statement_timeout_ms: statement_timeout_ms
      }
    end

    defp table(prefix, name), do: Storage.qualified_table(prefix, name)
  end
end
