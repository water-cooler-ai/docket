if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.Activation do
    @moduledoc """
    Prefix-wide admission activation and capability control plane.

    Preflight is advisory. `activate/2` always reacquires the exclusive gate and
    rechecks readiness, the initialized default, rollout evidence, the selected
    old-binary assertion, and every live capability before changing mode.

    Deactivation is only the database step of rollback. Operators must stop new
    TenantFair polling first; the exact-cap guarantee is abandoned when the
    deactivation transaction commits, and gate-aware Legacy may be restarted
    only afterward.
    """

    alias Docket.Postgres.ClaimPolicy.Admin.Codec
    alias Docket.Postgres.ClaimPolicy.ControlContext
    alias Docket.Postgres.ClaimPolicy.Readiness
    alias Docket.Runtime.Clock

    @writer_contract 1
    @gate_contract 1
    @function_contract 1
    @function_name "docket_tenant_fair_claim_v1"
    @function_identity_arguments "timestamp with time zone, timestamp with time zone, integer, integer, text, text[]"
    @function_result "SETOF record"
    @function_search_path ["search_path=pg_catalog, pg_temp"]
    @max_bigint 9_223_372_036_854_775_807
    @max_capability_ttl_ms :timer.hours(24)
    @max_assertion_ttl_ms :timer.hours(24)
    @lock_timeout_ms 1_000
    @statement_timeout_ms 5_000

    @type implementation_contract ::
            %{engine: :tenant_fair, function_contract: pos_integer()} | :none
    @type preflight_report :: %{
            activatable: boolean(),
            mode: :legacy | :tenant_fair,
            mode_epoch: non_neg_integer(),
            readiness: :not_ready | :ready,
            readiness_epoch: non_neg_integer(),
            required_function_contract: non_neg_integer(),
            selected_implementation_contract: implementation_contract(),
            active_implementation_contract: implementation_contract(),
            default_fingerprint: binary() | nil,
            verified_default_fingerprint: binary() | nil,
            schema_generation: non_neg_integer(),
            backfill_phase: :not_started | :running | :reconciling | :complete,
            online_phase: :not_started | :ready_index | :live_index | :fk_not_valid | :complete,
            recorded_missing_partition_count: non_neg_integer() | nil,
            missing_partition_count: non_neg_integer() | nil,
            ready_index_valid: boolean(),
            live_index_valid: boolean(),
            foreign_key_validated: boolean(),
            expected_tables_present: boolean(),
            expected_table_count: non_neg_integer(),
            expected_table_total: pos_integer(),
            live_capability_count: non_neg_integer(),
            old_binary_assertion_expires_at: DateTime.t() | nil,
            reasons: [atom()]
          }

    @doc false
    def function_contract do
      %{
        name: @function_name,
        identity_arguments: @function_identity_arguments,
        result: @function_result,
        volatility: :volatile,
        parallel: :unsafe,
        security: :invoker,
        search_path: @function_search_path,
        version: @function_contract
      }
    end

    @doc """
    Registers one expiring, prefix-local upgraded-binary capability.

    `opts` must contain `:binary_fingerprint`, `:writer_contract`,
    `:gate_contract`, `:function_contract`, and `:ttl_ms`. The database clock
    owns both heartbeat time and expiry. Registration takes the admission gate
    `FOR SHARE`; while TenantFair is active an incompatible heartbeat returns
    `{:error, :incompatible_capability}` without changing the row.
    """
    @spec register_capability(Docket.Backend.ctx(), Ecto.UUID.t(), keyword()) ::
            {:ok, map()} | {:error, term()}
    def register_capability(context, instance_id, opts) do
      with {:ok, control} <- ControlContext.resolve(context, :mutate),
           {:ok, instance_id} <- cast_uuid(instance_id),
           {:ok, capability} <- validate_capability_opts(opts) do
        register(control, instance_id, capability)
      end
    end

    @doc """
    Records expiring external proof that activation-unaware binaries are absent.

    Expiry must be after the database wall clock and no more than 24 hours in
    the future when the assertion transaction serializes its audit record.
    """
    @spec attest_old_binaries_absent(Docket.Backend.ctx(), keyword()) ::
            {:ok, map()} | {:error, term()}
    def attest_old_binaries_absent(context, opts) do
      with {:ok, control} <- ControlContext.resolve(context, :mutate),
           {:ok, meta} <- validate_assertion_opts(opts) do
        fingerprint =
          Codec.request_fingerprint(
            {:v1,
             {:old_binaries_absent, meta.evidence_fingerprint, meta.expires_at, meta.source,
              meta.event_id}}
          )

        assertion_id = Codec.deterministic_uuid(fingerprint)

        control
        |> transact_assertion(meta, fingerprint, assertion_id)
        |> retry_assertion_race(control, meta, fingerprint, assertion_id)
        |> normalize_control_error()
      end
    end

    @doc "Returns the bounded advisory activation report for a trusted prefix context."
    @spec preflight(Docket.Backend.ctx()) :: {:ok, preflight_report()} | {:error, term()}
    def preflight(context) do
      with {:ok, control} <- ControlContext.resolve(context, :read) do
        read_preflight(control)
      end
    end

    @doc "CAS-activates TenantFair under the prefix-wide exclusive gate."
    @spec activate(Docket.Backend.ctx(), keyword()) :: {:ok, map()} | {:error, term()}
    def activate(context, opts) do
      with {:ok, control} <- ControlContext.resolve(context, :mutate),
           {:ok, meta} <- validate_mode_opts(:activate, opts) do
        change_mode(control, :tenant_fair, meta)
      end
    end

    @doc "CAS-deactivates TenantFair; callers own the documented stop/declare/restart ordering."
    @spec deactivate(Docket.Backend.ctx(), keyword()) :: {:ok, map()} | {:error, term()}
    def deactivate(context, opts) do
      with {:ok, control} <- ControlContext.resolve(context, :mutate),
           {:ok, meta} <- validate_mode_opts(:deactivate, opts) do
        change_mode(control, :legacy, meta)
      end
    end

    defp register(control, instance_id, capability) do
      case control.repo.transaction(fn ->
             configure_transaction(control.repo)

             with {:ok, gate} <- lock_capability_gate(control),
                  :ok <- validate_registered_capability(gate, capability),
                  {:ok, result} <- upsert_capability(control, instance_id, capability) do
               result
             else
               {:error, reason} -> control.repo.rollback(reason)
             end
           end) do
        {:ok, result} -> {:ok, result}
        {:error, reason} -> {:error, reason}
      end
    rescue
      error in Postgrex.Error -> normalize_database_error(error, :capability_registration_failed)
      _error -> {:error, :capability_registration_failed}
    catch
      _kind, _reason -> {:error, :capability_registration_failed}
    end

    defp lock_capability_gate(control) do
      lock_one(
        control,
        """
        SELECT admission_mode, required_function_contract
        FROM #{control.identifiers.gate}
        WHERE id = 1
        FOR SHARE
        """,
        :gate,
        fn [mode, function_contract] ->
          %{mode: mode, function_contract: function_contract}
        end
      )
    end

    defp validate_registered_capability(%{mode: "legacy"}, _capability), do: :ok

    defp validate_registered_capability(
           %{mode: "tenant_fair", function_contract: @function_contract},
           %{
             writer_contract: @writer_contract,
             gate_contract: @gate_contract,
             function_contract: @function_contract
           }
         ),
         do: :ok

    defp validate_registered_capability(_gate, _capability),
      do: {:error, :incompatible_capability}

    defp upsert_capability(control, instance_id, capability) do
      case control.repo.query(
             """
             INSERT INTO #{control.identifiers.capabilities}
               (instance_id, binary_fingerprint, writer_contract, gate_contract,
                function_contract, last_seen_at, expires_at)
             SELECT $1::text::uuid, $2, $3, $4, $5, wall.now,
                    wall.now + ($6::bigint * interval '1 millisecond')
             FROM (SELECT clock_timestamp() AS now) AS wall
             ON CONFLICT (instance_id) DO UPDATE
             SET binary_fingerprint = EXCLUDED.binary_fingerprint,
                 writer_contract = EXCLUDED.writer_contract,
                 gate_contract = EXCLUDED.gate_contract,
                 function_contract = EXCLUDED.function_contract,
                 last_seen_at = EXCLUDED.last_seen_at,
                 expires_at = EXCLUDED.expires_at
             RETURNING last_seen_at, expires_at
             """,
             [
               instance_id,
               capability.binary_fingerprint,
               capability.writer_contract,
               capability.gate_contract,
               capability.function_contract,
               capability.ttl_ms
             ],
             log: false
           ) do
        {:ok, %{rows: [[last_seen_at, expires_at]]}} ->
          {:ok, %{instance_id: instance_id, last_seen_at: last_seen_at, expires_at: expires_at}}

        {:error, error} ->
          normalize_database_error(error, :capability_registration_failed)
      end
    end

    defp read_preflight(control) do
      case control.repo.query(
             """
             SELECT gate.admission_mode, gate.mode_epoch, gate.readiness,
                    gate.readiness_epoch, gate.required_function_contract,
                    capabilities.live_count, capabilities.mismatch_count,
                    assertion.expires_at
             FROM #{control.identifiers.gate} AS gate
             CROSS JOIN LATERAL (
               SELECT count(*) FILTER (WHERE expires_at > CURRENT_TIMESTAMP)::bigint AS live_count,
                      count(*) FILTER (
                        WHERE expires_at > CURRENT_TIMESTAMP AND
                          (writer_contract <> $1 OR gate_contract <> $2 OR
                           function_contract <> $3)
                      )::bigint AS mismatch_count
               FROM #{control.identifiers.capabilities}
             ) AS capabilities
             LEFT JOIN LATERAL (
               SELECT max(expires_at) AS expires_at
               FROM #{control.identifiers.assertions}
               WHERE assertion_kind = 'old_binaries_absent'
                 AND expires_at > CURRENT_TIMESTAMP
                 AND expires_at <= asserted_at + ($4::bigint * interval '1 millisecond')
             ) AS assertion ON true
             WHERE gate.id = 1
             """,
             [
               @writer_contract,
               @gate_contract,
               @function_contract,
               @max_assertion_ttl_ms
             ],
             log: false
           ) do
        {:ok,
         %{
           rows: [
             [
               mode,
               mode_epoch,
               readiness,
               readiness_epoch,
               required,
               live,
               mismatched,
               expires_at
             ]
           ]
         }} ->
          gate = %{
            mode: mode,
            epoch: mode_epoch,
            readiness: readiness,
            readiness_epoch: readiness_epoch,
            function_contract: required
          }

          with {:ok, rollout} <- lock_rollout(control, :none),
               {:ok, default} <- read_default(control),
               {:ok, catalog} <- Readiness.catalog_evidence(control) do
            function_ready = function_contract_ready?(control, required)
            readiness_ready = readiness_complete?(control, gate, rollout, default, catalog)

            reasons =
              preflight_reasons(
                readiness_ready,
                required,
                live,
                mismatched,
                expires_at,
                function_ready
              )

            selected_contract = control.implementation_contract

            {:ok,
             %{
               activatable: reasons == [],
               mode: decode_mode(mode),
               mode_epoch: mode_epoch,
               readiness: decode_readiness(readiness),
               readiness_epoch: readiness_epoch,
               required_function_contract: required,
               selected_implementation_contract: selected_contract,
               active_implementation_contract:
                 if(mode == "tenant_fair",
                   do: %{engine: :tenant_fair, function_contract: required},
                   else: :none
                 ),
               default_fingerprint: default.fingerprint,
               verified_default_fingerprint: rollout.verified_default,
               schema_generation: rollout.generation,
               backfill_phase: decode_backfill_phase(rollout.backfill),
               online_phase: decode_online_phase(rollout.online),
               recorded_missing_partition_count: rollout.missing,
               missing_partition_count: catalog.missing_count,
               ready_index_valid: catalog.ready_index_valid,
               live_index_valid: catalog.live_index_valid,
               foreign_key_validated:
                 catalog.fk_definition_valid and catalog.fk_disposition == :validated,
               expected_tables_present: catalog.schema_complete,
               expected_table_count: catalog.expected_table_count,
               expected_table_total: catalog.expected_table_total,
               live_capability_count: live,
               old_binary_assertion_expires_at: expires_at,
               reasons: reasons
             }}
          end

        {:ok, _} ->
          {:error, :invalid_admin_context}

        {:error, error} ->
          normalize_database_error(error, :preflight_failed)
      end
    rescue
      _error -> {:error, :preflight_failed}
    catch
      _kind, _reason -> {:error, :preflight_failed}
    end

    defp preflight_reasons(
           readiness_ready,
           required,
           live,
           mismatched,
           expires_at,
           function_ready
         ) do
      []
      |> maybe_reason(
        required != @function_contract or not function_ready,
        :function_contract_mismatch
      )
      |> maybe_reason(live == 0 or mismatched > 0, :capability_mismatch)
      |> maybe_reason(not readiness_ready, :not_ready)
      |> maybe_reason(is_nil(expires_at), :old_binary_assertion_expired)
      |> Enum.sort()
    end

    defp change_mode(control, requested_mode, meta) do
      fingerprint =
        Codec.request_fingerprint(
          {:v1,
           {:admission_mode, requested_mode, meta.expected_epoch,
            Map.get(meta, :old_binary_assertion_id), meta.source, meta.event_id}}
        )

      control
      |> transact_mode(requested_mode, meta, fingerprint)
      |> retry_mode_race(control, requested_mode, meta, fingerprint)
      |> normalize_control_error()
    end

    defp transact_mode(control, requested_mode, meta, fingerprint) do
      case control.repo.transaction(fn ->
             configure_transaction(control.repo)

             with {:new, nil} <- mode_replay(control, meta, fingerprint),
                  {:ok, gate} <- lock_gate(control),
                  {:new, nil} <- mode_replay(control, meta, fingerprint),
                  :ok <- compare_epoch(gate, meta),
                  {:ok, rollout} <- lock_rollout(control, :share),
                  {:ok, default} <- lock_default(control),
                  :ok <-
                    validate_mode_preconditions(
                      control,
                      requested_mode,
                      meta,
                      gate,
                      rollout,
                      default
                    ) do
               commit_mode(control, requested_mode, meta, fingerprint, gate, rollout, default)
             else
               {:replay, result} -> result
               {:error, reason} -> control.repo.rollback(reason)
             end
           end) do
        {:ok, result} -> {:ok, result}
        {:error, reason} -> {:error, reason}
      end
    rescue
      error in Postgrex.Error -> {:error, error}
      _error -> {:error, :activation_failed}
    catch
      _kind, _reason -> {:error, :activation_failed}
    end

    defp validate_mode_preconditions(_control, :legacy, _meta, _gate, _rollout, _default),
      do: :ok

    defp validate_mode_preconditions(control, :tenant_fair, meta, gate, rollout, default) do
      cond do
        not readiness_complete?(control, gate, rollout, default) ->
          {:error, {:activation_precondition_failed, :not_ready}}

        not function_contract_ready?(control, gate.function_contract) ->
          {:error, {:claim_policy_unavailable, :function_contract_mismatch}}

        true ->
          validation_time = sample_wall_time!(control)

          with :ok <- validate_assertion(control, meta.old_binary_assertion_id, validation_time),
               :ok <- validate_capabilities(control, validation_time) do
            :ok
          end
      end
    end

    defp sample_wall_time!(control) do
      [[validation_time]] =
        control.repo.query!("SELECT clock_timestamp()", [], log: false).rows

      validation_time
    end

    defp validate_assertion(control, assertion_id, validation_time) do
      case control.repo.query!(
             """
             SELECT 1
             FROM #{control.identifiers.assertions}
             WHERE assertion_id = $1::text::uuid
               AND assertion_kind = 'old_binaries_absent'
               AND expires_at > $2
               AND expires_at <= asserted_at + ($3::bigint * interval '1 millisecond')
             """,
             [assertion_id, validation_time, @max_assertion_ttl_ms],
             log: false
           ).rows do
        [[1]] -> :ok
        _ -> {:error, {:activation_precondition_failed, :old_binary_assertion_expired}}
      end
    end

    defp validate_capabilities(control, validation_time) do
      case control.repo.query!(
             """
             SELECT count(*)::bigint,
                    count(*) FILTER (
                      WHERE writer_contract <> $1 OR gate_contract <> $2 OR
                            function_contract <> $3
                    )::bigint
             FROM #{control.identifiers.capabilities}
             WHERE expires_at > $4
             """,
             [@writer_contract, @gate_contract, @function_contract, validation_time],
             log: false
           ).rows do
        [[live, 0]] when live > 0 -> :ok
        _ -> {:error, {:activation_precondition_failed, :capability_mismatch}}
      end
    end

    defp readiness_complete?(control, gate, rollout, default, catalog \\ nil) do
      catalog_result = if catalog, do: {:ok, catalog}, else: Readiness.catalog_evidence(control)

      with {:ok, catalog} <- catalog_result do
        catalog.schema_complete and catalog.missing_count == 0 and gate.readiness == "ready" and
          rollout.generation == 2 and
          not is_nil(rollout.dual_write_assertion_id) and
          rollout.dual_write_kind == "dual_write" and rollout.backfill == "complete" and
          is_integer(rollout.backfill_target) and rollout.backfill_target >= 0 and
          rollout.backfill_cursor == rollout.backfill_target and
          not is_nil(rollout.backfill_completed_at) and
          is_nil(rollout.backfill_last_error) and rollout.online == "complete" and
          not is_nil(rollout.online_completed_at) and rollout.missing == 0 and rollout.ready and
          rollout.live and rollout.ready_hash == catalog.ready_hash and
          rollout.live_hash == catalog.live_hash and rollout.fk == "validated" and
          catalog.ready_index_valid and catalog.live_index_valid and
          catalog.fk_disposition == :validated and catalog.fk_definition_valid and
          default.initialized and rollout.verified_default == default.fingerprint
      else
        _ -> false
      end
    rescue
      _error -> false
    catch
      _kind, _reason -> false
    end

    defp function_contract_ready?(control, required) do
      implementation_matches? =
        control.implementation_contract == %{
          engine: :tenant_fair,
          function_contract: @function_contract
        }

      catalog_matches? =
        case control.repo.query(
               """
               SELECT count(*)::bigint,
                      count(*) FILTER (
                        WHERE procedure.prokind = 'f'
                          AND procedure.pronargs = 6
                          AND pg_get_function_identity_arguments(procedure.oid) = $3
                          AND procedure.proretset
                          AND procedure.prorettype = 'record'::regtype
                          AND pg_get_function_result(procedure.oid) = $4
                          AND procedure.provolatile = 'v'
                          AND procedure.proparallel = 'u'
                          AND NOT procedure.prosecdef
                          AND procedure.proconfig = $5::text[]
                          AND language.lanname = 'plpgsql'
                      )::bigint
               FROM pg_proc AS procedure
               JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
               JOIN pg_language AS language ON language.oid = procedure.prolang
               WHERE namespace.nspname = $1 AND procedure.proname = $2
               """,
               [
                 control.prefix,
                 @function_name,
                 @function_identity_arguments,
                 @function_result,
                 @function_search_path
               ],
               log: false
             ) do
          {:ok, %{rows: [[1, 1]]}} -> true
          _ -> false
        end

      required == @function_contract and implementation_matches? and catalog_matches?
    rescue
      _error -> false
    catch
      _kind, _reason -> false
    end

    defp lock_gate(control) do
      lock_one(
        control,
        """
        SELECT readiness, readiness_epoch, admission_mode, mode_epoch,
               required_function_contract
        FROM #{control.identifiers.gate}
        WHERE id = 1
        FOR UPDATE
        """,
        :gate,
        fn [readiness, readiness_epoch, mode, epoch, function_contract] ->
          %{
            readiness: readiness,
            readiness_epoch: readiness_epoch,
            mode: mode,
            epoch: epoch,
            function_contract: function_contract
          }
        end
      )
    end

    defp lock_rollout(control, lock_mode) do
      clause =
        case lock_mode do
          :update -> "FOR UPDATE OF rollout"
          :share -> "FOR SHARE OF rollout"
          :none -> ""
        end

      lock_one(
        control,
        """
        SELECT rollout.schema_generation, rollout.dual_write_assertion_id,
               dual_write.assertion_kind, rollout.backfill_phase,
               rollout.backfill_target_id, rollout.backfill_cursor,
               rollout.backfill_completed_at, rollout.backfill_last_error,
               rollout.online_phase, rollout.online_completed_at,
               rollout.missing_partition_count, rollout.ready_index_valid,
               rollout.live_index_valid, rollout.ready_index_ddl_sha256,
               rollout.live_index_ddl_sha256, rollout.fk_disposition,
               rollout.verified_default_fingerprint
        FROM #{control.identifiers.rollout} AS rollout
        LEFT JOIN #{control.identifiers.assertions} AS dual_write
          ON dual_write.assertion_id = rollout.dual_write_assertion_id
        WHERE rollout.id = 1
        #{clause}
        """,
        :rollout,
        fn [
             generation,
             dual_write_assertion_id,
             dual_write_kind,
             backfill,
             backfill_target,
             backfill_cursor,
             backfill_completed_at,
             backfill_last_error,
             online,
             online_completed_at,
             missing,
             ready,
             live,
             ready_hash,
             live_hash,
             fk,
             verified_default
           ] ->
          %{
            generation: generation,
            dual_write_assertion_id: dual_write_assertion_id,
            dual_write_kind: dual_write_kind,
            backfill: backfill,
            backfill_target: backfill_target,
            backfill_cursor: backfill_cursor,
            backfill_completed_at: backfill_completed_at,
            backfill_last_error: backfill_last_error,
            online: online,
            online_completed_at: online_completed_at,
            missing: missing,
            ready: ready,
            live: live,
            ready_hash: ready_hash,
            live_hash: live_hash,
            fk: fk,
            verified_default: verified_default
          }
        end
      )
    end

    defp lock_default(control), do: read_default(control, "FOR SHARE")
    defp read_default(control), do: read_default(control, "")

    defp read_default(control, clause) do
      lock_one(
        control,
        """
        SELECT preferred_active, max_active, weight, borrowing,
               policy_version, initialized_at
        FROM #{control.identifiers.policy}
        WHERE id = 1
        #{clause}
        """,
        :default,
        fn [preferred, maximum, weight, borrowing, version, initialized_at] ->
          policy = %{
            preferred_active: preferred,
            max_active: maximum,
            weight: weight,
            borrowing: borrowing
          }

          %{
            initialized: not is_nil(initialized_at) and version > 0,
            fingerprint: if(initialized_at, do: Codec.default_fingerprint(policy)),
            version: version
          }
        end
      )
    end

    defp lock_one(control, statement, authority, decode) do
      case control.repo.query(statement, [], log: false) do
        {:ok, %{rows: [row]}} -> {:ok, decode.(row)}
        {:ok, _} -> {:error, :invalid_admin_context}
        {:error, error} -> lock_error(error, authority)
      end
    end

    defp commit_mode(control, requested_mode, meta, fingerprint, gate, rollout, default) do
      unchanged? = gate.mode == encode_mode(requested_mode)
      outcome = if unchanged?, do: :unchanged, else: :applied
      next_epoch = if unchanged?, do: gate.epoch, else: gate.epoch + 1

      unless unchanged? do
        control.repo.query!(
          """
          UPDATE #{control.identifiers.gate}
          SET admission_mode = $1, mode_epoch = $2, updated_at = CURRENT_TIMESTAMP
          WHERE id = 1
          """,
          [encode_mode(requested_mode), next_epoch],
          log: false
        )
      end

      operation =
        cond do
          unchanged? -> "activation_unchanged"
          requested_mode == :tenant_fair -> "activated"
          true -> "deactivated"
        end

      before_value =
        Codec.json_encode(%{
          admission_mode: decode_mode(gate.mode),
          mode_epoch: gate.epoch,
          readiness: decode_readiness(gate.readiness),
          readiness_epoch: gate.readiness_epoch,
          default_version: default.version,
          schema_generation: rollout.generation
        })

      after_value =
        Codec.json_encode(%{
          admission_mode: requested_mode,
          mode_epoch: next_epoch,
          exact_cap_guarantee: requested_mode == :tenant_fair
        })

      [[audit_id]] =
        control.repo.query!(
          """
          INSERT INTO #{control.identifiers.events}
            (target_kind, target_keys, operation, actor, source, event_id,
             request_fingerprint, before_value, after_value, before_versions,
             after_versions, mode_epoch)
          VALUES
            ('activation', ARRAY['activation']::text[], $1, $2, $3, $4, $5,
             convert_from($6::bytea, 'UTF8')::jsonb,
             convert_from($7::bytea, 'UTF8')::jsonb,
             ARRAY[$8]::bigint[], ARRAY[$9]::bigint[], $9)
          RETURNING audit_id
          """,
          [
            operation,
            meta.actor,
            meta.source,
            meta.event_id,
            fingerprint,
            before_value,
            after_value,
            gate.epoch,
            next_epoch
          ],
          log: false
        ).rows

      control.repo.query!(
        """
        INSERT INTO #{control.identifiers.receipts}
          (source, event_id, request_fingerprint, target_kind, target_fingerprints,
           outcome, previous_versions, versions, audit_id, result_value)
        VALUES
          ($1, $2, $3, 'activation', ARRAY[$4]::bytea[], $5,
           ARRAY[$6]::bigint[], ARRAY[$7]::bigint[], $8, '{}'::jsonb)
        """,
        [
          meta.source,
          meta.event_id,
          fingerprint,
          Codec.target_fingerprint("activation"),
          Atom.to_string(outcome),
          gate.epoch,
          next_epoch,
          audit_id
        ],
        log: false
      )

      %{
        outcome: outcome,
        target: :activation,
        previous_version: gate.epoch,
        version: next_epoch,
        audit_id: audit_id
      }
    end

    defp mode_replay(control, meta, fingerprint) do
      rows =
        control.repo.query!(
          """
          SELECT request_fingerprint, target_kind, outcome,
                 previous_versions[1], versions[1], audit_id
          FROM #{control.identifiers.receipts}
          WHERE source = $1 AND event_id = $2
          """,
          [meta.source, meta.event_id],
          log: false
        ).rows

      case rows do
        [] ->
          {:new, nil}

        [[^fingerprint, "activation", outcome, previous, version, audit_id]] ->
          original = %{
            outcome: String.to_existing_atom(outcome),
            target: :activation,
            previous_version: previous,
            version: version,
            audit_id: audit_id
          }

          {:replay, %{outcome: :replayed, original: original}}

        [[_fingerprint, _kind, _outcome, _previous, _version, _audit_id]] ->
          {:error, {:event_conflict, %{source: meta.source, event_id: meta.event_id}}}
      end
    end

    defp compare_epoch(%{epoch: epoch}, %{expected_epoch: epoch}), do: :ok

    defp compare_epoch(%{epoch: actual}, %{expected_epoch: expected}) do
      {:error, {:version_conflict, %{target: :activation, expected: expected, actual: actual}}}
    end

    defp transact_assertion(control, meta, fingerprint, assertion_id) do
      case control.repo.transaction(fn ->
             configure_transaction(control.repo)

             with {:new, nil} <- assertion_replay(control, meta, fingerprint),
                  {:ok, _rollout} <- lock_rollout(control, :update),
                  {:new, nil} <- assertion_replay(control, meta, fingerprint),
                  {:ok, asserted_at} <- validate_assertion_expiry(control, meta.expires_at),
                  {:ok, audit_id} <- insert_assertion_event(control, meta, fingerprint),
                  :ok <- insert_assertion(control, meta, assertion_id, asserted_at, audit_id),
                  :ok <-
                    insert_assertion_receipt(control, meta, fingerprint, assertion_id, audit_id) do
               assertion_result(assertion_id, meta.expires_at, audit_id)
             else
               {:replay, result} -> result
               {:error, reason} -> control.repo.rollback(reason)
             end
           end) do
        {:ok, result} -> {:ok, result}
        {:error, reason} -> {:error, reason}
      end
    rescue
      error in Postgrex.Error -> {:error, error}
      _error -> {:error, :activation_failed}
    catch
      _kind, _reason -> {:error, :activation_failed}
    end

    defp validate_assertion_expiry(control, expires_at) do
      case control.repo.query!(
             """
             WITH wall AS MATERIALIZED (SELECT clock_timestamp() AS now)
             SELECT wall.now
             FROM wall
             WHERE $1 > wall.now
               AND $1 <= wall.now + ($2::bigint * interval '1 millisecond')
             """,
             [expires_at, @max_assertion_ttl_ms],
             log: false
           ).rows do
        [[asserted_at]] -> {:ok, asserted_at}
        _ -> {:error, :invalid_activation_options}
      end
    end

    defp insert_assertion_event(control, meta, fingerprint) do
      case control.repo.query(
             """
             INSERT INTO #{control.identifiers.events}
               (target_kind, target_keys, operation, actor, source, event_id,
                request_fingerprint, before_value, after_value, before_versions,
                after_versions, mode_epoch)
             SELECT 'activation', ARRAY['old_binaries_absent']::text[],
                    'old_binaries_absent', $1, $2, $3, $4, '{}'::jsonb,
                    convert_from($5::bytea, 'UTF8')::jsonb,
                    ARRAY[0]::bigint[], ARRAY[0]::bigint[], gate.mode_epoch
             FROM #{control.identifiers.gate} AS gate
             WHERE gate.id = 1
             RETURNING audit_id
             """,
             [
               meta.actor,
               meta.source,
               meta.event_id,
               fingerprint,
               Codec.json_encode(%{expires_at: meta.expires_at})
             ],
             log: false
           ) do
        {:ok, %{rows: [[audit_id]]}} -> {:ok, audit_id}
        {:ok, _} -> {:error, :invalid_admin_context}
        {:error, error} -> {:error, error}
      end
    end

    defp insert_assertion(control, meta, assertion_id, asserted_at, audit_id) do
      control.repo.query!(
        """
        INSERT INTO #{control.identifiers.assertions}
          (assertion_id, assertion_kind, evidence_fingerprint, actor, source,
           event_id, asserted_at, expires_at, audit_id)
        VALUES
          ($1::text::uuid, 'old_binaries_absent', $2, $3, $4, $5,
           $6, $7, $8)
        """,
        [
          assertion_id,
          meta.evidence_fingerprint,
          meta.actor,
          meta.source,
          meta.event_id,
          asserted_at,
          meta.expires_at,
          audit_id
        ],
        log: false
      )

      :ok
    end

    defp insert_assertion_receipt(control, meta, fingerprint, assertion_id, audit_id) do
      control.repo.query!(
        """
        INSERT INTO #{control.identifiers.receipts}
          (source, event_id, request_fingerprint, target_kind, target_fingerprints,
           outcome, previous_versions, versions, audit_id, result_value)
        VALUES
          ($1, $2, $3, 'activation', ARRAY[$4]::bytea[], 'applied',
           ARRAY[0]::bigint[], ARRAY[0]::bigint[], $5,
           convert_from($6::bytea, 'UTF8')::jsonb)
        """,
        [
          meta.source,
          meta.event_id,
          fingerprint,
          Codec.target_fingerprint("old_binaries_absent"),
          audit_id,
          Codec.json_encode(%{assertion_id: assertion_id, expires_at: meta.expires_at})
        ],
        log: false
      )

      :ok
    end

    defp assertion_replay(control, meta, fingerprint) do
      case control.repo.query!(
             """
             SELECT request_fingerprint, target_kind, outcome, audit_id, result_value::text
             FROM #{control.identifiers.receipts}
             WHERE source = $1 AND event_id = $2
             """,
             [meta.source, meta.event_id],
             log: false
           ).rows do
        [] ->
          {:new, nil}

        [[^fingerprint, "activation", "applied", audit_id, result_json]] ->
          result = Codec.json_decode!(result_json)

          original =
            assertion_result(
              result["assertion_id"],
              DateTime.from_iso8601(result["expires_at"]) |> elem(1),
              audit_id
            )

          {:replay, %{outcome: :replayed, original: original}}

        [[_fingerprint, _kind, _outcome, _audit_id, _result_json]] ->
          {:error, {:event_conflict, %{source: meta.source, event_id: meta.event_id}}}
      end
    end

    defp assertion_result(assertion_id, expires_at, audit_id) do
      %{
        outcome: :applied,
        target: :old_binaries_absent,
        assertion_id: assertion_id,
        expires_at: expires_at,
        audit_id: audit_id
      }
    end

    defp retry_mode_race({:error, %Postgrex.Error{} = error}, control, mode, meta, fingerprint) do
      if source_event_race?(error),
        do: transact_mode(control, mode, meta, fingerprint),
        else: {:error, error}
    end

    defp retry_mode_race(result, _control, _mode, _meta, _fingerprint), do: result

    defp retry_assertion_race(
           {:error, %Postgrex.Error{} = error},
           control,
           meta,
           fingerprint,
           assertion_id
         ) do
      if source_event_race?(error),
        do: transact_assertion(control, meta, fingerprint, assertion_id),
        else: {:error, error}
    end

    defp retry_assertion_race(result, _control, _meta, _fingerprint, _assertion_id), do: result

    defp validate_capability_opts(opts) when is_list(opts) do
      allowed = [
        :binary_fingerprint,
        :function_contract,
        :gate_contract,
        :ttl_ms,
        :writer_contract
      ]

      with true <- keyword_once?(opts, allowed),
           fingerprint when is_binary(fingerprint) and byte_size(fingerprint) == 32 <-
             Keyword.get(opts, :binary_fingerprint),
           writer when is_integer(writer) and writer >= 0 <- Keyword.get(opts, :writer_contract),
           gate when is_integer(gate) and gate >= 0 <- Keyword.get(opts, :gate_contract),
           function when is_integer(function) and function >= 0 <-
             Keyword.get(opts, :function_contract),
           ttl when is_integer(ttl) and ttl in 1..@max_capability_ttl_ms <-
             Keyword.get(opts, :ttl_ms) do
        {:ok,
         %{
           binary_fingerprint: fingerprint,
           writer_contract: writer,
           gate_contract: gate,
           function_contract: function,
           ttl_ms: ttl
         }}
      else
        _ -> {:error, :invalid_capability}
      end
    end

    defp validate_capability_opts(_opts), do: {:error, :invalid_capability}

    defp validate_assertion_opts(opts) when is_list(opts) do
      allowed = [:actor, :event_id, :evidence_fingerprint, :expires_at, :source]

      with true <- keyword_once?(opts, allowed),
           {:ok, identity} <- validate_identity(opts),
           evidence when is_binary(evidence) and byte_size(evidence) == 32 <-
             Keyword.get(opts, :evidence_fingerprint),
           %DateTime{} = expires_at <- Keyword.get(opts, :expires_at),
           expires_at <- Clock.normalize!(expires_at) do
        {:ok, Map.merge(identity, %{evidence_fingerprint: evidence, expires_at: expires_at})}
      else
        _ -> {:error, :invalid_activation_options}
      end
    rescue
      _error -> {:error, :invalid_activation_options}
    end

    defp validate_assertion_opts(_opts), do: {:error, :invalid_activation_options}

    defp validate_mode_opts(:activate, opts) when is_list(opts) do
      allowed = [:actor, :event_id, :expected_epoch, :old_binary_assertion_id, :source]

      with true <- keyword_once?(opts, allowed),
           {:ok, meta} <- validate_identity(opts),
           epoch when is_integer(epoch) and epoch >= 0 and epoch < @max_bigint <-
             Keyword.get(opts, :expected_epoch),
           {:ok, assertion_id} <-
             cast_activation_assertion_uuid(Keyword.get(opts, :old_binary_assertion_id)) do
        {:ok, Map.merge(meta, %{expected_epoch: epoch, old_binary_assertion_id: assertion_id})}
      else
        _ -> {:error, :invalid_activation_options}
      end
    end

    defp validate_mode_opts(:deactivate, opts) when is_list(opts) do
      allowed = [:actor, :event_id, :expected_epoch, :source]

      with true <- keyword_once?(opts, allowed),
           {:ok, meta} <- validate_identity(opts),
           epoch when is_integer(epoch) and epoch >= 0 and epoch < @max_bigint <-
             Keyword.get(opts, :expected_epoch) do
        {:ok, Map.put(meta, :expected_epoch, epoch)}
      else
        _ -> {:error, :invalid_activation_options}
      end
    end

    defp validate_mode_opts(_operation, _opts), do: {:error, :invalid_activation_options}

    defp validate_identity(opts) do
      source = Keyword.get(opts, :source)
      event_id = Keyword.get(opts, :event_id)
      actor = Keyword.get(opts, :actor)

      if bounded_binary?(source, 64) and bounded_binary?(event_id, 255) and
           bounded_binary?(actor, 255) do
        {:ok, %{source: source, event_id: event_id, actor: actor}}
      else
        {:error, :invalid_activation_options}
      end
    end

    defp keyword_once?(opts, expected) do
      Keyword.keyword?(opts) and Enum.sort(Keyword.keys(opts)) == Enum.sort(expected) and
        Enum.uniq(Keyword.keys(opts)) == Keyword.keys(opts)
    end

    defp bounded_binary?(value, maximum) do
      is_binary(value) and byte_size(value) in 1..maximum and String.valid?(value) and
        not String.contains?(value, <<0>>)
    end

    defp cast_uuid(value) when is_binary(value) do
      case Ecto.UUID.cast(value) do
        {:ok, uuid} -> {:ok, uuid}
        :error -> {:error, :invalid_capability}
      end
    end

    defp cast_uuid(_value), do: {:error, :invalid_capability}

    defp cast_activation_assertion_uuid(value) do
      case cast_uuid(value) do
        {:ok, uuid} -> {:ok, uuid}
        {:error, :invalid_capability} -> {:error, :invalid_activation_options}
      end
    end

    defp configure_transaction(repo) do
      repo.query!("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE", [], log: false)
      repo.query!("SET LOCAL lock_timeout = '#{@lock_timeout_ms}ms'", [], log: false)
      repo.query!("SET LOCAL statement_timeout = '#{@statement_timeout_ms}ms'", [], log: false)
      :ok
    end

    defp lock_error(error, authority) do
      case postgres_code(error) do
        code when code in [:lock_not_available, :lock_timeout] ->
          {:error, {:lock_timeout, authority}}

        :query_canceled ->
          {:error, :admin_timeout}

        _ ->
          {:error, error}
      end
    end

    defp normalize_database_error(error, fallback) do
      if match?(%Postgrex.Error{}, error), do: {:error, error}, else: {:error, fallback}
    end

    defp normalize_control_error({:error, %Postgrex.Error{} = error}) do
      case postgres_code(error) do
        code when code in [:lock_not_available, :lock_timeout] ->
          {:error, {:lock_timeout, :rollout}}

        :query_canceled ->
          {:error, :admin_timeout}

        _ ->
          {:error, :activation_failed}
      end
    end

    defp normalize_control_error(result), do: result

    defp postgres_code(%Postgrex.Error{postgres: postgres}) when is_map(postgres),
      do: Map.get(postgres, :code)

    defp postgres_code(_error), do: nil

    defp source_event_race?(%Postgrex.Error{postgres: postgres}) when is_map(postgres) do
      Map.get(postgres, :code) == :unique_violation and
        Map.get(postgres, :constraint) in [
          "docket_claim_policy_events_source_event_index",
          "docket_claim_policy_receipts_pkey",
          "docket_claim_assertions_source_event_index"
        ]
    end

    defp source_event_race?(_error), do: false

    defp maybe_reason(reasons, true, reason), do: [reason | reasons]
    defp maybe_reason(reasons, false, _reason), do: reasons

    defp encode_mode(:legacy), do: "legacy"
    defp encode_mode(:tenant_fair), do: "tenant_fair"
    defp decode_mode("legacy"), do: :legacy
    defp decode_mode("tenant_fair"), do: :tenant_fair
    defp decode_readiness("not_ready"), do: :not_ready
    defp decode_readiness("ready"), do: :ready
    defp decode_backfill_phase("not_started"), do: :not_started
    defp decode_backfill_phase("running"), do: :running
    defp decode_backfill_phase("reconciling"), do: :reconciling
    defp decode_backfill_phase("complete"), do: :complete
    defp decode_online_phase("not_started"), do: :not_started
    defp decode_online_phase("ready_index"), do: :ready_index
    defp decode_online_phase("live_index"), do: :live_index
    defp decode_online_phase("fk_not_valid"), do: :fk_not_valid
    defp decode_online_phase("complete"), do: :complete
  end
end
