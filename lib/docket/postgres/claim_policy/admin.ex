if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.Admin do
    @moduledoc """
    Database-authoritative administration for PostgreSQL claim policy.

    The host authenticates and authorizes callers before invoking this module.
    Docket accepts only a resolved PostgreSQL backend context and validates the
    target, complete policy tuple, compare-and-swap version, and bounded audit
    identity. Mutators always own a short root transaction; a transaction-scoped
    context is rejected before SQL is issued.
    """

    alias Docket.Postgres.{ClaimPolicy, Storage}
    alias Docket.Postgres.ClaimPolicy.Admin.Codec
    alias Docket.Runtime.Clock

    @policy_keys [:borrowing, :max_active, :preferred_active, :weight]
    @admin_states [:running, :hold_new, :drain]
    @max_bulk_targets 100
    @max_audit_batch 500
    @lock_timeout_ms 1_000
    @statement_timeout_ms 5_000
    @max_bigint 9_223_372_036_854_775_807

    @type policy :: %{
            required(:preferred_active) => non_neg_integer(),
            required(:max_active) => non_neg_integer(),
            required(:weight) => pos_integer(),
            required(:borrowing) => boolean()
          }

    @doc "Initializes the prefix default exactly once from explicit reviewed values."
    def bootstrap_default(context, policy, opts) do
      with {:ok, admin} <- mutator_context(context),
           {:ok, policy} <- validate_policy(policy),
           {:ok, meta} <- validate_cas_opts(opts),
           :ok <- require_expected(meta, 0) do
        request = {:bootstrap_default, policy, meta.expected_version, meta.source, meta.event_id}

        mutate(admin, meta, request, fn tx, fingerprint ->
          with {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- lock_gate(tx),
               {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- lock_rollout(tx),
               {:new, nil} <- replay(tx, meta, fingerprint),
               {:ok, before} <- lock_default(tx, :update),
               {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- ensure_bootstrappable(before),
               {:ok, after_row} <- update_default(tx, policy, true),
               {:ok, audit_id} <-
                 insert_event(
                   tx,
                   :default,
                   ["default"],
                   "bootstrap_default",
                   meta,
                   fingerprint,
                   default_value(before),
                   default_value(after_row),
                   [before.version],
                   [after_row.version]
                 ),
               :ok <-
                 insert_receipt(
                   tx,
                   :default,
                   [target_fingerprint("default")],
                   [before.version],
                   [after_row.version],
                   audit_id,
                   meta,
                   fingerprint
                 ) do
            {:ok, applied_result(:default, before.version, after_row.version, audit_id)}
          else
            {:replay, receipt} -> {:ok, replay_result(receipt, :default)}
            {:error, reason} -> {:error, reason}
          end
        end)
      end
    end

    @doc "Changes the initialized prefix default with versioned CAS."
    def put_default(context, policy, opts) do
      with {:ok, admin} <- mutator_context(context),
           {:ok, policy} <- validate_policy(policy),
           {:ok, meta} <- validate_cas_opts(opts) do
        request = {:put_default, policy, meta.expected_version, meta.source, meta.event_id}

        mutate(admin, meta, request, fn tx, fingerprint ->
          with {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- lock_gate(tx),
               {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- lock_rollout(tx),
               {:new, nil} <- replay(tx, meta, fingerprint),
               {:ok, before} <- lock_default(tx, :update),
               {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- ensure_initialized(before),
               :ok <- compare_default(before, meta.expected_version),
               {:ok, after_row} <- update_default(tx, policy, false),
               {:ok, audit_id} <-
                 insert_event(
                   tx,
                   :default,
                   ["default"],
                   "put_default",
                   meta,
                   fingerprint,
                   default_value(before),
                   default_value(after_row),
                   [before.version],
                   [after_row.version]
                 ),
               :ok <-
                 insert_receipt(
                   tx,
                   :default,
                   [target_fingerprint("default")],
                   [before.version],
                   [after_row.version],
                   audit_id,
                   meta,
                   fingerprint
                 ) do
            {:ok, applied_result(:default, before.version, after_row.version, audit_id)}
          else
            {:replay, receipt} -> {:ok, replay_result(receipt, :default)}
            {:error, reason} -> {:error, reason}
          end
        end)
      end
    end

    @doc "Installs a complete per-partition override."
    def put_override(context, owner_scope, policy, opts) do
      partition_mutation(context, owner_scope, {:put_override, policy}, opts)
    end

    @doc "Returns a partition to complete database-default inheritance."
    def reset_override(context, owner_scope, opts) do
      partition_mutation(context, owner_scope, :reset_override, opts)
    end

    @doc "Changes the partition-local running, hold-new, or drain state."
    def put_state(context, owner_scope, admin_state, opts) do
      partition_mutation(context, owner_scope, {:put_state, admin_state}, opts)
    end

    @doc "Applies at most 100 distinct partition changes atomically in key order."
    def apply_partition_changes(context, changes, opts) do
      with {:ok, admin} <- mutator_context(context),
           {:ok, changes} <- validate_changes(changes),
           {:ok, meta} <- validate_event_opts(opts) do
        request = {:apply_partition_changes, request_changes(changes), meta.source, meta.event_id}

        mutate(admin, meta, request, fn tx, fingerprint ->
          apply_changes_transaction(tx, changes, meta, fingerprint, true)
        end)
      end
    end

    @doc "Reads the initialized database default without changing authority state."
    def get_default(context) do
      with {:ok, admin} <- read_context(context) do
        read_transaction(admin, fn tx ->
          case fetch_default(tx) do
            {:ok, %{initialized: false}} -> {:error, :not_initialized}
            {:ok, row} -> {:ok, default_public(row)}
            {:error, reason} -> {:error, reason}
          end
        end)
      end
    end

    @doc "Reads one effective policy, state, debt, and prefix gate snapshot."
    def get_effective(context, owner_scope) do
      with {:ok, admin} <- read_context(context),
           {:ok, target} <- normalize_target(owner_scope) do
        read_transaction(admin, fn tx -> effective_read(tx, target) end)
      end
    end

    @doc "Reads bounded rollout, gate, capability, policy, and audit watermarks."
    def get_prefix_state(context) do
      with {:ok, admin} <- read_context(context) do
        read_transaction(admin, &prefix_state/1)
      end
    end

    @doc "Lists immutable audit history using ascending audit-id keyset pagination."
    def list_events(context, opts) do
      with {:ok, admin} <- read_context(context),
           {:ok, page} <- validate_list_opts(opts) do
        read_transaction(admin, fn tx ->
          fetched = fetch_events(tx, page.after_audit_id, page.limit + 1)
          events = Enum.take(fetched, page.limit)

          {:ok,
           %{
             events: events,
             has_more: length(fetched) > page.limit,
             next_after_audit_id:
               if(events == [], do: page.after_audit_id, else: List.last(events).audit_id)
           }}
        end)
      end
    end

    @doc "Records the host's externally completed contiguous audit watermark."
    def export_events(context, opts) do
      with {:ok, admin} <- mutator_context(context),
           {:ok, export} <- validate_export_opts(opts) do
        meta = Map.take(export, [:actor, :source, :event_id])

        request =
          {:export_events, export.through_audit_id, export.location_fingerprint, meta.source,
           meta.event_id}

        mutate(admin, meta, request, fn tx, fingerprint ->
          export_transaction(tx, export, meta, fingerprint)
        end)
      end
    end

    @doc "Creates a bounded legal-hold range as a separate control-plane fact."
    def put_legal_hold(context, opts) do
      with {:ok, admin} <- mutator_context(context),
           {:ok, hold} <- validate_hold_opts(opts) do
        meta = Map.take(hold, [:actor, :source, :event_id])

        request =
          {:put_legal_hold, hold.first_audit_id, hold.last_audit_id, hold.reason, meta.source,
           meta.event_id}

        mutate(admin, meta, request, fn tx, fingerprint ->
          hold_id = deterministic_uuid(fingerprint)

          with {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- lock_rollout(tx),
               {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- validate_hold_watermark(tx, hold),
               {:ok, audit_id} <-
                 insert_event(
                   tx,
                   :audit,
                   [hold_id],
                   "put_legal_hold",
                   meta,
                   fingerprint,
                   %{},
                   Map.take(hold, [:first_audit_id, :last_audit_id, :reason]),
                   [0],
                   [1]
                 ),
               :ok <- insert_hold(tx, hold_id, hold, meta),
               :ok <-
                 insert_receipt(
                   tx,
                   :audit,
                   [target_fingerprint(hold_id)],
                   [0],
                   [1],
                   audit_id,
                   meta,
                   fingerprint
                 ) do
            {:ok, %{outcome: :applied, hold_id: hold_id, audit_id: audit_id}}
          else
            {:replay, receipt} ->
              {:ok,
               %{
                 outcome: :replayed,
                 original: %{
                   outcome: receipt.outcome,
                   hold_id: hold_id,
                   audit_id: receipt.audit_id
                 }
               }}

            {:error, reason} ->
              {:error, reason}
          end
        end)
      end
    end

    @doc "Deletes a legal-hold fact without changing or rewriting audit events."
    def delete_legal_hold(context, hold_id, opts) do
      with {:ok, admin} <- mutator_context(context),
           {:ok, hold_id} <- validate_uuid(hold_id),
           {:ok, meta} <- validate_event_opts(opts) do
        request = {:delete_legal_hold, hold_id, meta.source, meta.event_id}

        mutate(admin, meta, request, fn tx, fingerprint ->
          with {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- lock_rollout(tx),
               {:new, nil} <- replay(tx, meta, fingerprint),
               {:ok, hold} <- fetch_hold(tx, hold_id),
               {:new, nil} <- replay(tx, meta, fingerprint),
               :ok <- delete_hold(tx, hold_id),
               {:ok, audit_id} <-
                 insert_event(
                   tx,
                   :audit,
                   [hold_id],
                   "delete_legal_hold",
                   meta,
                   fingerprint,
                   hold,
                   %{},
                   [1],
                   [2]
                 ),
               :ok <-
                 insert_receipt(
                   tx,
                   :audit,
                   [target_fingerprint(hold_id)],
                   [1],
                   [2],
                   audit_id,
                   meta,
                   fingerprint
                 ) do
            {:ok, %{outcome: :applied, hold_id: hold_id, audit_id: audit_id}}
          else
            {:replay, receipt} ->
              {:ok,
               %{
                 outcome: :replayed,
                 original: %{
                   outcome: receipt.outcome,
                   hold_id: hold_id,
                   audit_id: receipt.audit_id
                 }
               }}

            {:error, reason} ->
              {:error, reason}
          end
        end)
      end
    end

    @doc "Prunes one exported, unheld, cutoff-bounded audit-id page."
    def prune_events(context, opts) do
      with {:ok, admin} <- mutator_context(context),
           {:ok, prune} <- validate_prune_opts(opts) do
        meta = Map.take(prune, [:actor, :source, :event_id])
        request = {:prune_events, prune.cutoff, prune.limit, meta.source, meta.event_id}

        mutate(admin, meta, request, fn tx, fingerprint ->
          prune_transaction(tx, prune, meta, fingerprint)
        end)
      end
    end

    defp partition_mutation(context, owner_scope, operation, opts) do
      with {:ok, admin} <- mutator_context(context),
           {:ok, target} <- normalize_target(owner_scope),
           {:ok, operation} <- validate_operation(operation),
           {:ok, meta} <- validate_cas_opts(opts) do
        change = %{target: target, expected_version: meta.expected_version, operation: operation}
        request = {:partition_change, request_change(change), meta.source, meta.event_id}

        mutate(admin, meta, request, fn tx, fingerprint ->
          apply_changes_transaction(tx, [change], meta, fingerprint, false)
        end)
      end
    end

    defp apply_changes_transaction(tx, changes, meta, fingerprint, bulk?) do
      targets = Enum.map(changes, & &1.target.owner_scope)

      with {:new, nil} <- replay(tx, meta, fingerprint),
           :ok <- lock_gate(tx),
           {:new, nil} <- replay(tx, meta, fingerprint),
           :ok <- lock_rollout(tx),
           {:new, nil} <- replay(tx, meta, fingerprint),
           {:ok, default} <- lock_default(tx, :share),
           {:new, nil} <- replay(tx, meta, fingerprint),
           :ok <- ensure_initialized(default),
           {:ok, changes} <- materialize_partitions(tx, changes),
           {:ok, before_rows} <- lock_partitions(tx, changes),
           {:new, nil} <- replay(tx, meta, fingerprint),
           :ok <- compare_partitions(changes, before_rows, bulk?),
           {:ok, after_rows} <- update_partitions(tx, changes, before_rows),
           {:ok, audit_id} <-
             insert_partition_event(
               tx,
               changes,
               before_rows,
               after_rows,
               meta,
               fingerprint,
               bulk?
             ),
           :ok <-
             insert_receipt(
               tx,
               if(bulk?, do: :bulk, else: :partition),
               Enum.map(changes, &target_fingerprint(&1.target.scope_key)),
               Enum.map(before_rows, & &1.version),
               Enum.map(after_rows, & &1.version),
               audit_id,
               meta,
               fingerprint
             ) do
        result = partition_result(targets, before_rows, after_rows, audit_id, bulk?)
        {:ok, result}
      else
        {:replay, receipt} ->
          target = if(bulk?, do: targets, else: hd(targets))
          {:ok, replay_result(receipt, target)}

        {:error, reason} ->
          {:error, reason}
      end
    end

    defp mutate(admin, _meta, request, fun) do
      fingerprint = request_fingerprint({:v1, request})

      result = run_mutation(admin, fun, fingerprint)

      result =
        if source_event_race?(result) do
          run_mutation(admin, fun, fingerprint)
        else
          result
        end

      normalize_database_error(result, mutation_authority(request))
    end

    defp run_mutation(admin, fun, fingerprint) do
      transaction(admin, fn tx ->
        with :ok <- set_timeouts(tx) do
          fun.(tx, fingerprint)
        end
      end)
    end

    defp transaction(%{repo: repo} = admin, fun) do
      case repo.transaction(fn ->
             case fun.(admin) do
               {:ok, value} -> value
               {:error, reason} -> repo.rollback(reason)
             end
           end) do
        {:ok, value} -> {:ok, value}
        {:error, reason} -> {:error, reason}
      end
    rescue
      exception in Postgrex.Error -> {:error, exception}
    end

    defp read_transaction(%{repo: repo} = admin, fun) do
      if repo.in_transaction?() do
        read_existing_transaction(admin, fun)
      else
        read_owned_transaction(admin, fun)
      end
    end

    defp read_existing_transaction(%{repo: repo} = admin, fun) do
      repo.query!("SAVEPOINT docket_claim_admin_read", [], log: false)
      previous_timeout = configure_read_timeout(repo, true)

      try do
        result = fun.(admin)
        restore_read_timeout(repo, previous_timeout)
        repo.query!("RELEASE SAVEPOINT docket_claim_admin_read", [], log: false)
        result
      rescue
        error in Postgrex.Error ->
          _ = repo.query("ROLLBACK TO SAVEPOINT docket_claim_admin_read", [], log: false)
          _ = restore_read_timeout(repo, previous_timeout)
          _ = repo.query("RELEASE SAVEPOINT docket_claim_admin_read", [], log: false)
          classify_read_error(error)
      end
    end

    defp read_owned_transaction(%{repo: repo} = admin, fun) do
      case repo.transaction(fn ->
             repo.query!("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY", [],
               log: false
             )

             _previous_timeout = configure_read_timeout(repo, false)

             case fun.(admin) do
               {:ok, value} -> value
               {:error, reason} -> repo.rollback(reason)
             end
           end) do
        {:ok, value} -> {:ok, value}
        {:error, reason} -> {:error, reason}
      end
    rescue
      error in Postgrex.Error -> classify_read_error(error)
    end

    defp classify_read_error(error) do
      if postgres_code(error) == :query_canceled,
        do: {:error, :admin_timeout},
        else: {:error, :invalid_admin_context}
    end

    defp configure_read_timeout(repo, true) do
      case repo.query!(
             """
             SELECT current_setting('statement_timeout'),
                    set_config(
                      'statement_timeout',
                      CASE
                        WHEN current_setting('statement_timeout') = '0'
                          OR current_setting('statement_timeout')::interval > interval '5 seconds'
                        THEN $1
                        ELSE current_setting('statement_timeout')
                      END,
                      true
                    )
             """,
             ["#{@statement_timeout_ms}ms"],
             log: false
           ).rows do
        [[previous, _effective]] -> previous
      end
    end

    defp configure_read_timeout(repo, false) do
      repo.query!("SELECT set_config('statement_timeout', $1, true)", [
        "#{@statement_timeout_ms}ms"
      ])

      nil
    end

    defp restore_read_timeout(_repo, nil), do: :ok

    defp restore_read_timeout(repo, previous) do
      _ = repo.query!("SELECT set_config('statement_timeout', $1, true)", [previous], log: false)
      :ok
    end

    defp set_timeouts(%{repo: repo}) do
      _ =
        repo.query!("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE", [], log: false)

      _ =
        repo.query!("SELECT set_config('lock_timeout', $1, true)", [
          "#{@lock_timeout_ms}ms"
        ])

      _ =
        repo.query!("SELECT set_config('statement_timeout', $1, true)", [
          "#{@statement_timeout_ms}ms"
        ])

      :ok
    end

    defp lock_gate(%{repo: repo, identifiers: ids}) do
      lock_query(repo, "SELECT id FROM #{ids.gate} WHERE id = 1 FOR SHARE NOWAIT", [], :gate, fn
        [[1]] -> :ok
        _ -> {:error, :invalid_admin_context}
      end)
    end

    defp lock_rollout(%{repo: repo, identifiers: ids}) do
      lock_query(
        repo,
        "SELECT id FROM #{ids.rollout} WHERE id = 1 FOR UPDATE",
        [],
        :rollout,
        fn
          [[1]] -> :ok
          _ -> {:error, :invalid_admin_context}
        end
      )
    end

    defp lock_default(%{repo: repo, identifiers: ids}, mode) do
      suffix = if mode == :update, do: "FOR UPDATE", else: "FOR SHARE NOWAIT"

      lock_query(
        repo,
        """
        SELECT preferred_active, max_active, weight, borrowing, policy_version,
               initialized_at, updated_at
        FROM #{ids.policy}
        WHERE id = 1
        #{suffix}
        """,
        [],
        :default,
        fn
          [row] -> {:ok, decode_default(row)}
          _ -> {:error, :invalid_admin_context}
        end
      )
    end

    defp lock_query(repo, sql, params, authority, decode) do
      decode.(repo.query!(sql, params, log: false).rows)
    rescue
      error in Postgrex.Error ->
        if lock_error?(error),
          do: {:error, {:lock_timeout, authority}},
          else: reraise(error, __STACKTRACE__)
    end

    defp lock_partitions(tx, changes) do
      Enum.reduce_while(changes, {:ok, []}, fn change, {:ok, rows} ->
        result =
          lock_query(
            tx.repo,
            """
            SELECT scope_key, preferred_active, max_active, weight, borrowing, admin_state,
                   partition_version, admission_epoch, inserted_at, updated_at
            FROM #{tx.identifiers.partitions}
            WHERE scope_key = $1
            FOR NO KEY UPDATE
            """,
            [change.target.scope_key],
            {:partition, change.target.owner_scope},
            fn
              [row] ->
                {:ok, decode_partition(row, change.target, change.target.virtual_before)}

              _ ->
                {:error, :invalid_admin_context}
            end
          )

        case result do
          {:ok, row} -> {:cont, {:ok, rows ++ [row]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end

    defp materialize_partitions(tx, changes) do
      Enum.reduce_while(changes, {:ok, []}, fn change, {:ok, materialized} ->
        result =
          lock_query(
            tx.repo,
            """
            INSERT INTO #{tx.identifiers.partitions} (scope_key)
            VALUES ($1)
            ON CONFLICT (scope_key) DO NOTHING
            RETURNING scope_key
            """,
            [change.target.scope_key],
            {:partition, change.target.owner_scope},
            fn rows ->
              {:ok, put_in(change.target.virtual_before, rows == [[change.target.scope_key]])}
            end
          )

        case result do
          {:ok, changed} -> {:cont, {:ok, materialized ++ [changed]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end

    defp compare_partitions(changes, rows, bulk?) do
      conflicts =
        Enum.zip(changes, rows)
        |> Enum.flat_map(fn {change, row} ->
          if change.expected_version == row.version do
            []
          else
            [
              %{
                target: change.target.owner_scope,
                expected: change.expected_version,
                actual: row.version
              }
            ]
          end
        end)

      case conflicts do
        [] ->
          :ok

        conflicts when bulk? ->
          {:error, {:version_conflict, %{conflicts: conflicts}}}

        [conflict] ->
          {:error, {:version_conflict, conflict}}
      end
    end

    defp update_partitions(tx, changes, rows) do
      after_rows =
        Enum.zip(changes, rows)
        |> Enum.map(fn {change, before} -> update_partition(tx, change, before) end)

      {:ok, after_rows}
    end

    defp update_partition(tx, change, before) do
      {preferred, maximum, weight, borrowing, state} =
        case change.operation do
          {:put_override, policy} ->
            {policy.preferred_active, policy.max_active, policy.weight, policy.borrowing,
             before.state}

          :reset_override ->
            {nil, nil, nil, nil, before.state}

          {:put_state, state} ->
            {before.policy.preferred_active, before.policy.max_active, before.policy.weight,
             before.policy.borrowing, state}
        end

      rows =
        tx.repo.query!(
          """
          UPDATE #{tx.identifiers.partitions}
          SET preferred_active = $2, max_active = $3, weight = $4, borrowing = $5,
              admin_state = $6, partition_version = partition_version + 1,
              updated_at = CURRENT_TIMESTAMP
          WHERE scope_key = $1
          RETURNING scope_key, preferred_active, max_active, weight, borrowing, admin_state,
                    partition_version, admission_epoch, inserted_at, updated_at
          """,
          [change.target.scope_key, preferred, maximum, weight, borrowing, Atom.to_string(state)],
          log: false
        ).rows

      [row] = rows
      decode_partition(row, change.target, false)
    end

    defp insert_partition_event(tx, changes, before_rows, after_rows, meta, fingerprint, bulk?) do
      target_kind = if bulk?, do: :bulk, else: :partition

      operation =
        if bulk?, do: "apply_partition_changes", else: operation_name(hd(changes).operation)

      before_value = partition_event_value(before_rows, bulk?)
      after_value = partition_event_value(after_rows, bulk?)

      insert_event(
        tx,
        target_kind,
        Enum.map(changes, & &1.target.scope_key),
        operation,
        meta,
        fingerprint,
        before_value,
        after_value,
        Enum.map(before_rows, & &1.version),
        Enum.map(after_rows, & &1.version)
      )
    end

    defp partition_event_value([row], false), do: partition_value(row)
    defp partition_event_value(rows, true), do: Enum.map(rows, &partition_value/1)

    defp update_default(tx, policy, bootstrap?) do
      initialized = if bootstrap?, do: "initialized_at = CURRENT_TIMESTAMP,", else: ""

      rows =
        tx.repo.query!(
          """
          UPDATE #{tx.identifiers.policy}
          SET preferred_active = $1, max_active = $2, weight = $3, borrowing = $4,
              #{initialized}
              policy_version = policy_version + 1, updated_at = CURRENT_TIMESTAMP
          WHERE id = 1
          RETURNING preferred_active, max_active, weight, borrowing, policy_version,
                    initialized_at, updated_at
          """,
          [policy.preferred_active, policy.max_active, policy.weight, policy.borrowing],
          log: false
        ).rows

      case rows do
        [row] -> {:ok, decode_default(row)}
        _ -> {:error, :invalid_admin_context}
      end
    end

    defp insert_event(
           tx,
           target_kind,
           target_keys,
           operation,
           meta,
           fingerprint,
           before_value,
           after_value,
           before_versions,
           after_versions
         ) do
      rows =
        tx.repo.query!(
          """
          INSERT INTO #{tx.identifiers.events}
            (target_kind, target_keys, operation, actor, source, event_id,
             request_fingerprint, before_value, after_value, before_versions, after_versions)
          VALUES
            ($1, $2::text[], $3, $4, $5, $6, $7,
             convert_from($8::bytea, 'UTF8')::jsonb,
             convert_from($9::bytea, 'UTF8')::jsonb,
             $10::bigint[], $11::bigint[])
          RETURNING audit_id
          """,
          [
            Atom.to_string(target_kind),
            target_keys,
            operation,
            meta.actor,
            meta.source,
            meta.event_id,
            fingerprint,
            json_encode(before_value),
            json_encode(after_value),
            before_versions,
            after_versions
          ],
          log: false
        ).rows

      case rows do
        [[audit_id]] -> {:ok, audit_id}
        _ -> {:error, :invalid_admin_context}
      end
    end

    defp insert_receipt(
           tx,
           target_kind,
           target_fingerprints,
           previous_versions,
           versions,
           audit_id,
           meta,
           fingerprint
         ) do
      _ =
        tx.repo.query!(
          """
          INSERT INTO #{tx.identifiers.receipts}
            (source, event_id, request_fingerprint, target_kind, target_fingerprints,
             outcome, previous_versions, versions, audit_id)
          VALUES ($1, $2, $3, $4, $5::bytea[], 'applied', $6::bigint[], $7::bigint[], $8)
          """,
          [
            meta.source,
            meta.event_id,
            fingerprint,
            Atom.to_string(target_kind),
            target_fingerprints,
            previous_versions,
            versions,
            audit_id
          ],
          log: false
        )

      :ok
    end

    defp replay(tx, meta, fingerprint) do
      rows =
        tx.repo.query!(
          """
          SELECT request_fingerprint, target_kind, outcome, previous_versions, versions, audit_id
          FROM #{tx.identifiers.receipts}
          WHERE source = $1 AND event_id = $2
          """,
          [meta.source, meta.event_id],
          log: false
        ).rows

      case rows do
        [] ->
          {:new, nil}

        [[^fingerprint, target_kind, outcome, previous, versions, audit_id]] ->
          {:replay,
           %{
             target_kind: decode_target_kind(target_kind),
             outcome: decode_outcome(outcome),
             previous_versions: previous,
             versions: versions,
             audit_id: audit_id
           }}

        [[_other | _]] ->
          {:error, {:event_conflict, %{source: meta.source, event_id: meta.event_id}}}
      end
    end

    defp export_transaction(tx, export, meta, fingerprint) do
      export_id = deterministic_uuid(fingerprint)

      with {:new, nil} <- replay(tx, meta, fingerprint),
           :ok <- lock_rollout(tx),
           {:new, nil} <- replay(tx, meta, fingerprint),
           :ok <- validate_export_watermark(tx, export.through_audit_id) do
        tx.repo.query!(
          """
          INSERT INTO #{tx.identifiers.exports}
            (export_id, through_audit_id, location_fingerprint, actor, source, event_id)
          VALUES ($1::text::uuid, $2, $3, $4, $5, $6)
          """,
          [
            export_id,
            export.through_audit_id,
            export.location_fingerprint,
            meta.actor,
            meta.source,
            meta.event_id
          ],
          log: false
        )

        {:ok, audit_id} =
          insert_event(
            tx,
            :audit,
            [export_id],
            "export_events",
            meta,
            fingerprint,
            %{},
            %{through_audit_id: export.through_audit_id},
            [0],
            [export.through_audit_id]
          )

        :ok =
          insert_receipt(
            tx,
            :audit,
            [target_fingerprint(export_id)],
            [0],
            [export.through_audit_id],
            audit_id,
            meta,
            fingerprint
          )

        {:ok,
         %{
           outcome: :applied,
           export_id: export_id,
           through_audit_id: export.through_audit_id,
           audit_id: audit_id
         }}
      else
        {:replay, receipt} ->
          {:ok,
           %{
             outcome: :replayed,
             original: %{
               outcome: receipt.outcome,
               export_id: export_id,
               through_audit_id: hd(receipt.versions),
               audit_id: receipt.audit_id
             }
           }}

        {:error, reason} ->
          {:error, reason}
      end
    end

    defp prune_transaction(tx, prune, meta, fingerprint) do
      with {:new, nil} <- replay(tx, meta, fingerprint),
           :ok <- lock_rollout(tx),
           {:new, nil} <- replay(tx, meta, fingerprint),
           {:ok, export_watermark} <- export_watermark(tx) do
        rows =
          tx.repo.query!(
            """
            SELECT events.audit_id
            FROM #{tx.identifiers.events} AS events
            WHERE events.audit_id <= $1
              AND events.occurred_at < $2
              AND NOT EXISTS (
                SELECT 1
                FROM #{tx.identifiers.holds} AS holds
                WHERE events.audit_id BETWEEN holds.first_audit_id AND holds.last_audit_id
              )
            ORDER BY events.audit_id
            LIMIT $3
            FOR UPDATE OF events SKIP LOCKED
            """,
            [export_watermark, prune.cutoff, prune.limit],
            log: false
          ).rows

        audit_ids = Enum.map(rows, &hd/1)

        if audit_ids != [] do
          tx.repo.query!(
            "DELETE FROM #{tx.identifiers.events} WHERE audit_id = ANY($1::bigint[])",
            [audit_ids],
            log: false
          )
        end

        {:ok, audit_id} =
          insert_event(
            tx,
            :audit,
            ["prune"],
            "prune_events",
            meta,
            fingerprint,
            %{export_watermark: export_watermark},
            %{deleted_count: length(audit_ids), last_deleted_audit_id: List.last(audit_ids)},
            [length(audit_ids)],
            [List.last(audit_ids) || 0]
          )

        :ok =
          insert_receipt(
            tx,
            :audit,
            [target_fingerprint("prune")],
            [length(audit_ids)],
            [List.last(audit_ids) || 0],
            audit_id,
            meta,
            fingerprint
          )

        {:ok,
         %{
           outcome: :applied,
           deleted_count: length(audit_ids),
           last_deleted_audit_id: List.last(audit_ids),
           audit_id: audit_id
         }}
      else
        {:replay, receipt} ->
          {:ok,
           %{
             outcome: :replayed,
             original: %{
               outcome: receipt.outcome,
               deleted_count: hd(receipt.previous_versions),
               last_deleted_audit_id:
                 case hd(receipt.versions) do
                   0 -> nil
                   audit_id -> audit_id
                 end,
               audit_id: receipt.audit_id
             }
           }}

        {:error, reason} ->
          {:error, reason}
      end
    end

    defp fetch_events(tx, after_audit_id, limit) do
      tx.repo.query!(
        """
        SELECT audit_id, target_kind, target_keys, operation, actor, source, event_id,
               encode(request_fingerprint, 'hex'), before_value::text, after_value::text,
               before_versions, after_versions, mode_epoch, occurred_at
        FROM #{tx.identifiers.events}
        WHERE audit_id > $1
        ORDER BY audit_id
        LIMIT $2
        """,
        [after_audit_id, limit],
        log: false
      ).rows
      |> Enum.map(fn [
                       audit_id,
                       target_kind,
                       target_keys,
                       operation,
                       actor,
                       source,
                       event_id,
                       fingerprint,
                       before_value,
                       after_value,
                       before_versions,
                       after_versions,
                       mode_epoch,
                       occurred_at
                     ] ->
        %{
          audit_id: audit_id,
          target_kind: decode_target_kind(target_kind),
          target_keys: target_keys,
          operation: operation,
          actor: actor,
          source: source,
          event_id: event_id,
          request_fingerprint: fingerprint,
          before_value: json_decode!(before_value),
          after_value: json_decode!(after_value),
          before_versions: before_versions,
          after_versions: after_versions,
          mode_epoch: mode_epoch,
          occurred_at: occurred_at
        }
      end)
    end

    defp effective_read(tx, target) do
      rows =
        tx.repo.query!(
          """
          SELECT policy.preferred_active, policy.max_active, policy.weight, policy.borrowing,
                 policy.policy_version, policy.initialized_at,
                 partitions.scope_key, partitions.preferred_active, partitions.max_active,
                 partitions.weight, partitions.borrowing, partitions.admin_state,
                 partitions.partition_version, partitions.admission_epoch,
                 gate.readiness, gate.readiness_epoch, gate.admission_mode, gate.mode_epoch,
                 (SELECT count(*)
                  FROM #{tx.identifiers.runs} AS runs
                  WHERE runs.scope_key = $1 AND runs.status = 'running'
                    AND runs.poisoned_at IS NULL AND runs.claim_token IS NOT NULL)
          FROM #{tx.identifiers.policy} AS policy
          CROSS JOIN #{tx.identifiers.gate} AS gate
          LEFT JOIN #{tx.identifiers.partitions} AS partitions ON partitions.scope_key = $1
          WHERE policy.id = 1 AND gate.id = 1
          """,
          [target.scope_key],
          log: false
        ).rows

      case rows do
        [[_dp, _dm, _dw, _db, _dv, nil | _rest]] ->
          {:error, :not_initialized}

        [
          [
            default_preferred,
            default_maximum,
            default_weight,
            default_borrowing,
            default_version,
            _initialized_at,
            scope_key,
            preferred,
            maximum,
            weight,
            borrowing,
            state,
            version,
            epoch,
            readiness,
            readiness_epoch,
            mode,
            mode_epoch,
            live_count
          ]
        ] ->
          present? = not is_nil(scope_key)
          override? = present? and not is_nil(preferred)

          policy =
            if override? do
              policy_map(preferred, maximum, weight, borrowing)
            else
              policy_map(default_preferred, default_maximum, default_weight, default_borrowing)
            end

          {:ok,
           Map.merge(policy, %{
             policy_source: if(override?, do: :override, else: :default),
             default_version: default_version,
             partition_version: version || 0,
             partition_present: present?,
             state: if(state, do: decode_admin_state(state), else: :running),
             admission_epoch: epoch || 0,
             live_count: live_count,
             debt: max(live_count - policy.max_active, 0),
             readiness: decode_readiness(readiness),
             readiness_epoch: readiness_epoch,
             mode: decode_mode(mode),
             mode_epoch: mode_epoch
           })}

        _ ->
          {:error, :invalid_admin_context}
      end
    end

    defp prefix_state(tx) do
      rows =
        tx.repo.query!(
          """
          SELECT rollout.schema_generation, rollout.dual_write_assertion_id,
                 rollout.backfill_phase, rollout.backfill_target_id, rollout.backfill_cursor,
                 rollout.backfill_batches, rollout.backfill_rows, rollout.backfill_retries,
                 rollout.backfill_completed_at,
                 rollout.backfill_last_error, rollout.updated_at,
                 rollout.online_phase, rollout.online_attempts, rollout.online_last_error,
                 rollout.online_started_at, rollout.online_completed_at,
                 rollout.ready_index_valid, rollout.live_index_valid, rollout.fk_disposition,
                 encode(rollout.ready_index_ddl_sha256, 'hex'),
                 encode(rollout.live_index_ddl_sha256, 'hex'),
                 rollout.missing_partition_count,
                 encode(rollout.verified_default_fingerprint, 'hex'), rollout.verified_at,
                 gate.readiness, gate.readiness_epoch, gate.admission_mode, gate.mode_epoch,
                 gate.required_function_contract, gate.updated_at,
                 policy.preferred_active, policy.max_active, policy.weight, policy.borrowing,
                 policy.policy_version, policy.initialized_at,
                 (SELECT count(*) FROM #{tx.identifiers.capabilities}
                  WHERE expires_at > CURRENT_TIMESTAMP),
                 (SELECT count(*) FROM #{tx.identifiers.capabilities}),
                 (SELECT min(audit_id) FROM #{tx.identifiers.events}),
                 (SELECT max(audit_id) FROM #{tx.identifiers.events}),
                 (SELECT count(*) FROM #{tx.identifiers.events}),
                 (SELECT max(through_audit_id) FROM #{tx.identifiers.exports}),
                 (SELECT count(*) FROM #{tx.identifiers.exports}),
                 (SELECT count(*) FROM #{tx.identifiers.partitions} AS partitions
                  WHERE NOT EXISTS (SELECT 1 FROM #{tx.identifiers.runs} AS runs
                                    WHERE runs.scope_key = partitions.scope_key))
          FROM #{tx.identifiers.rollout} AS rollout
          CROSS JOIN #{tx.identifiers.gate} AS gate
          CROSS JOIN #{tx.identifiers.policy} AS policy
          WHERE rollout.id = 1 AND gate.id = 1 AND policy.id = 1
          """,
          [],
          log: false
        ).rows

      case rows do
        [
          [
            generation,
            assertion,
            phase,
            target_id,
            cursor,
            batches,
            backfill_rows,
            backfill_retries,
            completed_at,
            backfill_last_error,
            rollout_updated_at,
            online_phase,
            online_attempts,
            online_last_error,
            online_started_at,
            online_completed_at,
            ready_index,
            live_index,
            fk,
            ready_index_ddl_sha256,
            live_index_ddl_sha256,
            missing_count,
            verified_fingerprint,
            verified_at,
            readiness,
            readiness_epoch,
            mode,
            mode_epoch,
            function_contract,
            gate_updated_at,
            preferred,
            maximum,
            weight,
            borrowing,
            default_version,
            initialized_at,
            live_capabilities,
            total_capabilities,
            first_audit,
            last_audit,
            audit_count,
            export_watermark,
            export_count,
            dormant_count
          ]
        ] ->
          default =
            if initialized_at do
              policy_map(preferred, maximum, weight, borrowing)
            else
              nil
            end

          {:ok,
           %{
             schema_generation: generation,
             dual_write_assertion_id: assertion,
             backfill_phase: decode_backfill_phase(phase),
             backfill_target_id: target_id,
             backfill_cursor: cursor,
             backfill_batches: batches,
             backfill_rows: backfill_rows,
             backfill_retries: backfill_retries,
             backfill_completed_at: completed_at,
             backfill_last_error: backfill_last_error,
             rollout_updated_at: rollout_updated_at,
             online_phase: decode_online_phase(online_phase),
             online_attempts: online_attempts,
             online_last_error: online_last_error,
             online_started_at: online_started_at,
             online_completed_at: online_completed_at,
             ready_index_valid: ready_index,
             live_index_valid: live_index,
             ready_index_ddl_sha256: ready_index_ddl_sha256,
             live_index_ddl_sha256: live_index_ddl_sha256,
             fk_disposition: decode_fk_disposition(fk),
             missing_partition_count: missing_count,
             verified_default_fingerprint: verified_fingerprint,
             verified_at: verified_at,
             default: default,
             default_version: default_version,
             default_fingerprint:
               if(default,
                 do: Base.encode16(Codec.default_fingerprint(default), case: :lower)
               ),
             readiness: decode_readiness(readiness),
             readiness_epoch: readiness_epoch,
             mode: decode_mode(mode),
             mode_epoch: mode_epoch,
             required_function_contract: function_contract,
             gate_updated_at: gate_updated_at,
             capability_summary: %{live: live_capabilities, total: total_capabilities},
             audit_watermark: %{first: first_audit, last: last_audit, count: audit_count},
             export_watermark: %{through: export_watermark, count: export_count},
             dormant_partition_count: dormant_count
           }}

        _ ->
          {:error, :invalid_admin_context}
      end
    end

    defp fetch_default(%{repo: repo, identifiers: ids}) do
      case repo.query!(
             """
             SELECT preferred_active, max_active, weight, borrowing, policy_version,
                    initialized_at, updated_at
             FROM #{ids.policy}
             WHERE id = 1
             """,
             [],
             log: false
           ).rows do
        [row] -> {:ok, decode_default(row)}
        _ -> {:error, :invalid_admin_context}
      end
    end

    defp decode_default([
           preferred,
           maximum,
           weight,
           borrowing,
           version,
           initialized_at,
           updated_at
         ]) do
      %{
        policy: policy_map(preferred, maximum, weight, borrowing),
        version: version,
        initialized: not is_nil(initialized_at),
        initialized_at: initialized_at,
        updated_at: updated_at
      }
    end

    defp decode_partition(
           [
             scope_key,
             preferred,
             maximum,
             weight,
             borrowing,
             state,
             version,
             epoch,
             inserted,
             updated
           ],
           target,
           virtual_before
         ) do
      %{
        scope_key: scope_key,
        owner_scope: target.owner_scope,
        policy: policy_map(preferred, maximum, weight, borrowing),
        state: decode_admin_state(state),
        version: if(virtual_before, do: 0, else: version),
        admission_epoch: if(virtual_before, do: 0, else: epoch),
        partition_present: not virtual_before,
        inserted_at: if(virtual_before, do: nil, else: inserted),
        updated_at: if(virtual_before, do: nil, else: updated)
      }
    end

    defp policy_map(preferred, maximum, weight, borrowing) do
      %{
        preferred_active: preferred,
        max_active: maximum,
        weight: weight,
        borrowing: borrowing
      }
    end

    defp default_value(row) do
      Map.merge(row.policy, %{
        policy_version: row.version,
        initialized_at: row.initialized_at,
        updated_at: row.updated_at
      })
    end

    defp partition_value(row) do
      Map.merge(row.policy, %{
        scope_key: row.scope_key,
        admin_state: row.state,
        partition_version: row.version,
        admission_epoch: row.admission_epoch,
        partition_present: row.partition_present,
        inserted_at: row.inserted_at,
        updated_at: row.updated_at
      })
    end

    defp default_public(row) do
      Map.merge(row.policy, %{
        version: row.version,
        initialized_at: row.initialized_at,
        updated_at: row.updated_at
      })
    end

    defp ensure_bootstrappable(%{initialized: false, version: 0}), do: :ok
    defp ensure_bootstrappable(%{version: version}), do: {:error, {:already_initialized, version}}

    defp ensure_initialized(%{initialized: true}), do: :ok
    defp ensure_initialized(_row), do: {:error, :not_initialized}

    defp compare_default(%{version: version}, version), do: :ok

    defp compare_default(%{version: actual}, expected) do
      {:error, {:version_conflict, %{target: :default, expected: expected, actual: actual}}}
    end

    defp applied_result(target, previous, version, audit_id) do
      %{
        outcome: :applied,
        target: target,
        previous_version: previous,
        version: version,
        audit_id: audit_id
      }
    end

    defp partition_result(targets, before, after_rows, audit_id, true) do
      applied_result(
        targets,
        Enum.zip(targets, before)
        |> Enum.map(fn {target, row} -> %{target: target, version: row.version} end),
        Enum.zip(targets, after_rows)
        |> Enum.map(fn {target, row} -> %{target: target, version: row.version} end),
        audit_id
      )
    end

    defp partition_result([target], [before], [after_row], audit_id, false) do
      applied_result(target, before.version, after_row.version, audit_id)
    end

    defp replay_result(receipt, target) when is_list(target) do
      previous =
        Enum.zip(target, receipt.previous_versions)
        |> Enum.map(fn {item, version} -> %{target: item, version: version} end)

      versions =
        Enum.zip(target, receipt.versions)
        |> Enum.map(fn {item, version} -> %{target: item, version: version} end)

      %{
        outcome: :replayed,
        original: applied_result(target, previous, versions, receipt.audit_id)
      }
    end

    defp replay_result(receipt, target) do
      %{
        outcome: :replayed,
        original:
          applied_result(
            target,
            hd(receipt.previous_versions),
            hd(receipt.versions),
            receipt.audit_id
          )
      }
    end

    defp validate_policy(policy) when is_map(policy) do
      if Enum.sort(Map.keys(policy)) == @policy_keys do
        preferred = policy.preferred_active
        maximum = policy.max_active
        weight = policy.weight
        borrowing = policy.borrowing

        if is_integer(preferred) and preferred >= 0 and is_integer(maximum) and
             maximum >= preferred and maximum <= 2_147_483_647 and is_integer(weight) and
             weight > 0 and weight <= 2_147_483_647 and is_boolean(borrowing) do
          {:ok, Map.take(policy, @policy_keys)}
        else
          {:error, :invalid_policy}
        end
      else
        {:error, :invalid_policy}
      end
    end

    defp validate_policy(_policy), do: {:error, :invalid_policy}

    defp validate_operation({:put_override, policy}) do
      case validate_policy(policy) do
        {:ok, policy} -> {:ok, {:put_override, policy}}
        error -> error
      end
    end

    defp validate_operation(:reset_override), do: {:ok, :reset_override}

    defp validate_operation({:put_state, state}) when state in @admin_states,
      do: {:ok, {:put_state, state}}

    defp validate_operation(_operation), do: {:error, :invalid_partition_change}

    defp validate_changes(changes)
         when is_list(changes) and changes != [] and length(changes) <= @max_bulk_targets do
      result =
        Enum.reduce_while(changes, {:ok, []}, fn change, {:ok, normalized} ->
          with true <- is_map(change),
               true <-
                 Enum.sort(Map.keys(change)) == [:expected_version, :operation, :owner_scope],
               {:ok, target} <- normalize_target(change.owner_scope),
               true <- valid_version?(change.expected_version),
               {:ok, operation} <- validate_operation(change.operation) do
            item = %{
              target: target,
              expected_version: change.expected_version,
              operation: operation
            }

            {:cont, {:ok, [item | normalized]}}
          else
            _ -> {:halt, {:error, :invalid_partition_change}}
          end
        end)

      with {:ok, normalized} <- result do
        sorted = Enum.sort_by(normalized, & &1.target.scope_key)
        keys = Enum.map(sorted, & &1.target.scope_key)

        if Enum.uniq(keys) == keys do
          {:ok, mark_virtual_targets(sorted)}
        else
          {:error, :duplicate_partition_target}
        end
      end
    end

    defp validate_changes(_changes), do: {:error, :invalid_partition_changes}

    defp normalize_target(:tenantless) do
      {:ok, %{owner_scope: :tenantless, scope_key: "", virtual_before: false}}
    end

    defp normalize_target({:tenant, tenant_id})
         when is_binary(tenant_id) and byte_size(tenant_id) > 0 do
      if String.valid?(tenant_id) and not String.contains?(tenant_id, <<0>>) do
        {:ok, %{owner_scope: {:tenant, tenant_id}, scope_key: tenant_id, virtual_before: false}}
      else
        {:error, :invalid_target}
      end
    end

    defp normalize_target(_target), do: {:error, :invalid_target}

    defp mark_virtual_targets(changes), do: Enum.map(changes, & &1)

    defp validate_cas_opts(opts) do
      allowed = [:actor, :event_id, :expected_version, :source]

      with {:ok, meta} <- validate_identity(opts, allowed),
           {:ok, expected} <- fetch_option(opts, :expected_version),
           true <- valid_version?(expected) do
        {:ok, Map.put(meta, :expected_version, expected)}
      else
        _ -> {:error, :invalid_admin_options}
      end
    end

    defp validate_event_opts(opts), do: validate_identity(opts, [:actor, :event_id, :source])

    defp validate_identity(opts, allowed_keys) when is_list(opts) do
      with true <- Keyword.keyword?(opts),
           true <- Enum.sort(Keyword.keys(opts)) == Enum.sort(allowed_keys),
           {:ok, source} <- fetch_option(opts, :source),
           {:ok, event_id} <- fetch_option(opts, :event_id),
           {:ok, actor} <- fetch_option(opts, :actor),
           true <- bounded_binary?(source, 64),
           true <- bounded_binary?(event_id, 255),
           true <- bounded_binary?(actor, 255) do
        {:ok, %{source: source, event_id: event_id, actor: actor}}
      else
        _ -> {:error, :invalid_admin_options}
      end
    end

    defp validate_identity(_opts, _allowed_keys), do: {:error, :invalid_admin_options}

    defp validate_list_opts(opts) when is_list(opts) do
      if Keyword.keyword?(opts) and
           Enum.uniq(Keyword.keys(opts)) == Keyword.keys(opts) and
           Enum.all?(Keyword.keys(opts), &(&1 in [:after_audit_id, :limit])) do
        after_audit_id = Keyword.get(opts, :after_audit_id, 0)
        limit = Keyword.get(opts, :limit, 100)

        if valid_audit_id?(after_audit_id) and is_integer(limit) and
             limit in 1..@max_audit_batch do
          {:ok, %{after_audit_id: after_audit_id, limit: limit}}
        else
          {:error, :invalid_audit_options}
        end
      else
        {:error, :invalid_audit_options}
      end
    end

    defp validate_list_opts(_opts), do: {:error, :invalid_audit_options}

    defp validate_export_opts(opts) do
      allowed = [:actor, :event_id, :location_fingerprint, :source, :through_audit_id]

      with {:ok, meta} <- validate_identity(opts, allowed),
           {:ok, location} <- fetch_option(opts, :location_fingerprint),
           true <- is_binary(location) and byte_size(location) == 32,
           {:ok, through_audit_id} <- fetch_option(opts, :through_audit_id),
           true <-
             is_integer(through_audit_id) and through_audit_id > 0 and
               through_audit_id <= @max_bigint do
        {:ok,
         Map.merge(meta, %{
           through_audit_id: through_audit_id,
           location_fingerprint: location
         })}
      else
        _ -> {:error, :invalid_audit_options}
      end
    end

    defp validate_hold_opts(opts) do
      allowed = [:actor, :event_id, :first_audit_id, :last_audit_id, :reason, :source]

      with {:ok, meta} <- validate_identity(opts, allowed),
           {:ok, first} <- fetch_option(opts, :first_audit_id),
           {:ok, last} <- fetch_option(opts, :last_audit_id),
           {:ok, reason} <- fetch_option(opts, :reason),
           true <- is_integer(first) and first > 0 and first <= @max_bigint,
           true <- is_integer(last) and last >= first and last <= @max_bigint,
           true <- bounded_binary?(reason, 512) do
        {:ok, Map.merge(meta, %{first_audit_id: first, last_audit_id: last, reason: reason})}
      else
        _ -> {:error, :invalid_audit_options}
      end
    end

    defp validate_prune_opts(opts) when is_list(opts) do
      if Keyword.keyword?(opts) do
        allowed = [:actor, :cutoff, :event_id, :limit, :source]
        actual_keys = Keyword.keys(opts)
        allowed = if :limit in actual_keys, do: allowed, else: List.delete(allowed, :limit)

        with {:ok, meta} <- validate_identity(opts, allowed),
             {:ok, cutoff} <- fetch_option(opts, :cutoff),
             {:ok, cutoff} <- normalize_cutoff(cutoff),
             limit when is_integer(limit) and limit in 1..@max_audit_batch <-
               Keyword.get(opts, :limit, 100) do
          {:ok, Map.merge(meta, %{cutoff: cutoff, limit: limit})}
        else
          _ -> {:error, :invalid_audit_options}
        end
      else
        {:error, :invalid_audit_options}
      end
    end

    defp validate_prune_opts(_opts), do: {:error, :invalid_audit_options}

    defp normalize_cutoff(%DateTime{} = cutoff) do
      {:ok, Clock.normalize!(cutoff)}
    rescue
      _ -> {:error, :invalid_audit_options}
    end

    defp normalize_cutoff(_cutoff), do: {:error, :invalid_audit_options}

    defp require_expected(%{expected_version: expected}, expected), do: :ok
    defp require_expected(_meta, _expected), do: {:error, :invalid_admin_options}

    defp valid_version?(version),
      do: is_integer(version) and version >= 0 and version < @max_bigint

    defp valid_audit_id?(value), do: is_integer(value) and value >= 0 and value <= @max_bigint

    defp bounded_binary?(value, maximum),
      do:
        is_binary(value) and byte_size(value) in 1..maximum and String.valid?(value) and
          not String.contains?(value, <<0>>)

    defp fetch_option(opts, key) do
      if Keyword.keyword?(opts) and Keyword.has_key?(opts, key) and
           Enum.count(opts, fn {item, _} -> item == key end) == 1 do
        {:ok, Keyword.fetch!(opts, key)}
      else
        :error
      end
    end

    defp validate_uuid(value) when is_binary(value) do
      case Ecto.UUID.cast(value) do
        {:ok, uuid} -> {:ok, uuid}
        :error -> {:error, :invalid_hold_id}
      end
    end

    defp validate_uuid(_value), do: {:error, :invalid_hold_id}

    defp mutator_context(%{transaction_scope: true}), do: {:error, :transaction_context_forbidden}

    defp mutator_context(context) do
      case read_context(context) do
        {:ok, %{repo: repo} = admin} ->
          if function_exported?(repo, :in_transaction?, 0) and repo.in_transaction?() do
            {:error, :transaction_context_forbidden}
          else
            {:ok, admin}
          end

        _ ->
          {:error, :invalid_admin_context}
      end
    end

    defp read_context(
           %{
             repo: repo,
             prefix: prefix,
             postgres_backend: Docket.Postgres,
             postgres_admin_identity: admin_identity,
             claim_policy: %ClaimPolicy{} = claim_policy
           } = context
         )
         when is_atom(repo) and is_binary(prefix) do
      if Storage.valid_prefix?(prefix) do
        plan = ClaimPolicy.plan_context!(%{repo: repo, prefix: prefix})

        if ClaimPolicy.admin_context?(
             claim_policy,
             admin_identity,
             repo,
             prefix,
             plan.identifiers
           ) do
          {:ok,
           %{
             repo: repo,
             prefix: prefix,
             transaction_scope: Map.get(context, :transaction_scope, false),
             identifiers: %{
               policy: plan.identifiers.claim_policy,
               partitions: plan.identifiers.claim_partitions,
               receipts: plan.identifiers.claim_policy_receipts,
               events: plan.identifiers.claim_policy_events,
               holds: plan.identifiers.claim_policy_holds,
               exports: plan.identifiers.claim_audit_exports,
               rollout: plan.identifiers.claim_rollout,
               gate: plan.identifiers.claim_admission_gate,
               capabilities: plan.identifiers.claim_capabilities,
               runs: plan.identifiers.runs
             }
           }}
        else
          {:error, :invalid_admin_context}
        end
      else
        {:error, :invalid_admin_context}
      end
    rescue
      _ -> {:error, :invalid_admin_context}
    end

    defp read_context(_context), do: {:error, :invalid_admin_context}

    defp normalize_database_error({:error, %Postgrex.Error{} = error}, authority) do
      cond do
        lock_error?(error) -> {:error, {:lock_timeout, authority}}
        postgres_code(error) == :query_canceled -> {:error, :admin_timeout}
        true -> {:error, :invalid_admin_context}
      end
    end

    defp normalize_database_error({:error, reason}, _authority), do: {:error, reason}
    defp normalize_database_error({:ok, value}, _authority), do: {:ok, value}

    defp mutation_authority({operation, _policy, _version, _source, _event})
         when operation in [:bootstrap_default, :put_default],
         do: :default

    defp mutation_authority(
           {:partition_change, {scope_key, _version, _operation}, _source, _event}
         ),
         do: {:partition, owner_scope(scope_key)}

    defp mutation_authority({:apply_partition_changes, [first | _], _source, _event}),
      do: {:partition, owner_scope(elem(first, 0))}

    defp mutation_authority(_request), do: :rollout

    defp owner_scope(""), do: :tenantless
    defp owner_scope(scope_key), do: {:tenant, scope_key}

    defp lock_error?(%Postgrex.Error{postgres: postgres}) when is_map(postgres) do
      Map.get(postgres, :code) in [:lock_not_available, :lock_timeout]
    end

    defp lock_error?(_error), do: false

    defp postgres_code(%Postgrex.Error{postgres: postgres}) when is_map(postgres),
      do: Map.get(postgres, :code)

    defp postgres_code(_error), do: nil

    defp source_event_race?({:error, %Postgrex.Error{postgres: postgres}})
         when is_map(postgres) do
      Map.get(postgres, :code) == :unique_violation and
        Map.get(postgres, :constraint) in [
          "docket_claim_policy_events_source_event_index",
          "docket_claim_policy_receipts_pkey"
        ]
    end

    defp source_event_race?(_result), do: false

    defp validate_hold_watermark(tx, hold) do
      case tx.repo.query!(
             "SELECT max(audit_id) FROM #{tx.identifiers.events}",
             [],
             log: false
           ).rows do
        [[maximum]] when is_integer(maximum) and hold.last_audit_id <= maximum -> :ok
        _ -> {:error, :invalid_audit_range}
      end
    end

    defp insert_hold(tx, hold_id, hold, meta) do
      tx.repo.query!(
        """
        INSERT INTO #{tx.identifiers.holds}
          (hold_id, first_audit_id, last_audit_id, reason, actor, source, event_id)
        VALUES ($1::text::uuid, $2, $3, $4, $5, $6, $7)
        """,
        [
          hold_id,
          hold.first_audit_id,
          hold.last_audit_id,
          hold.reason,
          meta.actor,
          meta.source,
          meta.event_id
        ],
        log: false
      )

      :ok
    end

    defp fetch_hold(tx, hold_id) do
      case tx.repo.query!(
             """
             SELECT first_audit_id, last_audit_id, reason, actor, source, event_id, created_at
             FROM #{tx.identifiers.holds}
             WHERE hold_id = $1::text::uuid
             FOR UPDATE
             """,
             [hold_id],
             log: false
           ).rows do
        [[first, last, reason, actor, source, event_id, created_at]] ->
          {:ok,
           %{
             first_audit_id: first,
             last_audit_id: last,
             reason: reason,
             actor: actor,
             source: source,
             event_id: event_id,
             created_at: created_at
           }}

        [] ->
          {:error, :legal_hold_not_found}
      end
    end

    defp delete_hold(tx, hold_id) do
      _ =
        tx.repo.query!(
          "DELETE FROM #{tx.identifiers.holds} WHERE hold_id = $1::text::uuid",
          [hold_id],
          log: false
        )

      :ok
    end

    defp export_watermark(tx) do
      case tx.repo.query!(
             "SELECT max(through_audit_id) FROM #{tx.identifiers.exports}",
             [],
             log: false
           ).rows do
        [[watermark]] when is_integer(watermark) -> {:ok, watermark}
        _ -> {:error, :audit_export_required}
      end
    end

    defp validate_export_watermark(tx, through_audit_id) do
      case tx.repo.query!(
             """
             SELECT (SELECT max(audit_id) FROM #{tx.identifiers.events}),
                    (SELECT max(through_audit_id) FROM #{tx.identifiers.exports})
             """,
             [],
             log: false
           ).rows do
        [[audit_high, previous]]
        when is_integer(audit_high) and through_audit_id <= audit_high and
               (is_nil(previous) or through_audit_id >= previous) ->
          :ok

        _ ->
          {:error, :invalid_export_watermark}
      end
    end

    defp request_changes(changes), do: Enum.map(changes, &request_change/1)

    defp request_change(change) do
      {change.target.scope_key, change.expected_version, request_operation(change.operation)}
    end

    defp request_operation({:put_override, policy}), do: {:put_override, policy}
    defp request_operation(other), do: other

    defp operation_name({:put_override, _policy}), do: "put_override"
    defp operation_name(:reset_override), do: "reset_override"
    defp operation_name({:put_state, _state}), do: "put_state"

    defp decode_target_kind("default"), do: :default
    defp decode_target_kind("partition"), do: :partition
    defp decode_target_kind("bulk"), do: :bulk
    defp decode_target_kind("activation"), do: :activation
    defp decode_target_kind("readiness"), do: :readiness
    defp decode_target_kind("audit"), do: :audit

    defp decode_outcome("applied"), do: :applied
    defp decode_outcome("unchanged"), do: :unchanged
    defp decode_outcome("demoted"), do: :demoted

    defp decode_admin_state("running"), do: :running
    defp decode_admin_state("hold_new"), do: :hold_new
    defp decode_admin_state("drain"), do: :drain

    defp decode_readiness("not_ready"), do: :not_ready
    defp decode_readiness("ready"), do: :ready

    defp decode_mode("legacy"), do: :legacy
    defp decode_mode("tenant_fair"), do: :tenant_fair

    defp decode_backfill_phase("not_started"), do: :not_started
    defp decode_backfill_phase("running"), do: :running
    defp decode_backfill_phase("reconciling"), do: :reconciling
    defp decode_backfill_phase("complete"), do: :complete

    defp decode_online_phase("not_started"), do: :not_started
    defp decode_online_phase("ready_index"), do: :ready_index
    defp decode_online_phase("live_index"), do: :live_index
    defp decode_online_phase("fk_not_valid"), do: :fk_not_valid
    defp decode_online_phase("complete"), do: :complete

    defp decode_fk_disposition("absent"), do: :absent
    defp decode_fk_disposition("not_valid"), do: :not_valid
    defp decode_fk_disposition("validated"), do: :validated

    defp request_fingerprint(request), do: Codec.request_fingerprint(request)
    defp target_fingerprint(target), do: Codec.target_fingerprint(target)
    defp deterministic_uuid(fingerprint), do: Codec.deterministic_uuid(fingerprint)
    defp json_encode(value), do: Codec.json_encode(value)
    defp json_decode!(value), do: Codec.json_decode!(value)
  end
end
