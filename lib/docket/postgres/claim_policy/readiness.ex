if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.ClaimPolicy.Readiness do
    @moduledoc """
    Deployment attestations used by the exact-cap rollout.

    `attest_dual_write/2` records the host operator's evidence that every run
    writer uses atomic claim-partition dual-write and that older writers,
    including their open transactions, have drained. The assertion is durable
    and non-expiring; hosts must attest again after a fleet change before
    continuing reconciliation.
    """

    alias Docket.Postgres.ClaimPolicy.Admin.Codec
    alias Docket.Postgres.ClaimPolicy.OnlineDDL
    alias Docket.Postgres.ClaimPolicy.ControlContext
    alias Docket.Postgres.OnlineMigration

    @lock_timeout_ms 1_000
    @statement_timeout_ms 5_000

    @doc "Records fleet-wide partition dual-write evidence atomically with audit and replay state."
    @spec attest_dual_write(Docket.Backend.ctx(), keyword()) ::
            {:ok, map()} | {:error, term()}
    def attest_dual_write(context, opts) do
      with {:ok, control} <- ControlContext.resolve(context, :mutate),
           {:ok, meta} <- validate_opts(opts) do
        fingerprint =
          Codec.request_fingerprint(
            {:v1, {:attest_dual_write, meta.evidence_fingerprint, meta.source, meta.event_id}}
          )

        assertion_id = Codec.deterministic_uuid(fingerprint)

        control
        |> transact_attestation(meta, fingerprint, assertion_id)
        |> retry_source_event_race(control, meta, fingerprint, assertion_id)
        |> normalize_error()
      end
    end

    @doc "Verifies live online state and atomically promotes or demotes prefix readiness."
    @spec verify(Docket.Backend.ctx(), keyword()) :: {:ok, map()} | {:error, term()}
    def verify(context, opts) do
      with {:ok, control} <- ControlContext.resolve(context, :mutate),
           {:ok, meta} <- validate_verify_opts(opts) do
        fingerprint =
          Codec.request_fingerprint(
            {:v1,
             {:verify_readiness, meta.expected_readiness_epoch, meta.ready_index_ddl_sha256,
              meta.live_index_ddl_sha256, meta.source, meta.event_id}}
          )

        control
        |> transact_verification(meta, fingerprint)
        |> retry_verification_source_event_race(control, meta, fingerprint)
        |> normalize_error()
      end
    end

    defp transact_verification(control, meta, fingerprint) do
      case control.repo.transaction(fn ->
             configure_transaction(control.repo)

             with {:new, nil} <- verification_replay(control, meta, fingerprint),
                  :ok <- acquire_online_authority(control),
                  {:ok, gate} <- lock_verification_gate(control),
                  :ok <- verify_expected_epoch(gate, meta),
                  {:ok, rollout} <- lock_verification_rollout(control),
                  {:ok, default} <- lock_verification_default(control),
                  {:ok, evidence} <- collect_evidence(control, rollout, gate, default, meta) do
               case complete_verification(
                      control,
                      meta,
                      fingerprint,
                      gate,
                      rollout,
                      default,
                      evidence
                    ) do
                 {:error, reason} -> control.repo.rollback(reason)
                 value -> value
               end
             else
               {:replay, result} -> result
               {:error, reason} -> control.repo.rollback(reason)
             end
           end) do
        {:ok, value} -> {:ok, value}
        {:error, reason} -> {:error, reason}
      end
    rescue
      error in Postgrex.Error -> {:error, error}
      _error -> {:error, :readiness_failed}
    catch
      _kind, _reason -> {:error, :readiness_failed}
    end

    defp acquire_online_authority(control) do
      key = "docket-v2-online-migration-v1:" <> control.prefix

      case control.repo.query!(
             "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
             [key],
             log: false
           ).rows do
        [[true]] -> :ok
        [[false]] -> {:error, {:lock_timeout, :rollout}}
        _ -> {:error, :readiness_failed}
      end
    end

    defp retry_verification_source_event_race(
           {:error, %Postgrex.Error{} = error},
           control,
           meta,
           fingerprint
         ) do
      if source_event_race?(error),
        do: transact_verification(control, meta, fingerprint),
        else: {:error, error}
    end

    defp retry_verification_source_event_race(result, _control, _meta, _fingerprint), do: result

    defp lock_verification_gate(control) do
      case control.repo.query(
             """
             SELECT readiness, readiness_epoch, admission_mode, mode_epoch,
                    required_function_contract
             FROM #{control.identifiers.gate}
             WHERE id = 1
             FOR UPDATE
             """,
             [],
             log: false
           ) do
        {:ok, %{rows: [[readiness, epoch, mode, mode_epoch, function_contract]]}} ->
          {:ok,
           %{
             readiness: readiness,
             epoch: epoch,
             mode: mode,
             mode_epoch: mode_epoch,
             function_contract: function_contract
           }}

        {:ok, _} ->
          {:error, :invalid_admin_context}

        {:error, error} ->
          verification_lock_error(error, :gate)
      end
    end

    defp lock_verification_rollout(control) do
      case control.repo.query(
             """
             SELECT rollout.schema_generation, rollout.backfill_phase,
                    rollout.missing_partition_count, rollout.online_phase,
                    rollout.ready_index_valid, rollout.live_index_valid,
                    rollout.ready_index_ddl_sha256, rollout.live_index_ddl_sha256,
                    rollout.fk_disposition, rollout.verified_default_fingerprint,
                    assertion.assertion_kind
             FROM #{control.identifiers.rollout} AS rollout
             LEFT JOIN #{control.identifiers.assertions} AS assertion
               ON assertion.assertion_id = rollout.dual_write_assertion_id
             WHERE rollout.id = 1
             FOR UPDATE OF rollout
             """,
             [],
             log: false
           ) do
        {:ok,
         %{
           rows: [
             [
               generation,
               backfill,
               missing,
               online,
               ready,
               live,
               ready_hash,
               live_hash,
               fk,
               verified_default,
               assertion_kind
             ]
           ]
         }} ->
          {:ok,
           %{
             generation: generation,
             backfill: backfill,
             missing: missing,
             online: online,
             ready: ready,
             live: live,
             ready_hash: ready_hash,
             live_hash: live_hash,
             fk: fk,
             verified_default: verified_default,
             assertion_kind: assertion_kind
           }}

        {:ok, _} ->
          {:error, :invalid_admin_context}

        {:error, error} ->
          verification_lock_error(error, :rollout)
      end
    end

    defp lock_verification_default(control) do
      case control.repo.query(
             """
             SELECT preferred_active, max_active, weight, borrowing,
                    policy_version, initialized_at
             FROM #{control.identifiers.policy}
             WHERE id = 1
             FOR SHARE
             """,
             [],
             log: false
           ) do
        {:ok, %{rows: [[preferred, maximum, weight, borrowing, version, initialized_at]]}} ->
          policy = %{
            preferred_active: preferred,
            max_active: maximum,
            weight: weight,
            borrowing: borrowing
          }

          {:ok,
           %{
             initialized: not is_nil(initialized_at) and version > 0,
             fingerprint: if(initialized_at, do: Codec.default_fingerprint(policy)),
             version: version
           }}

        {:ok, _} ->
          {:error, :invalid_admin_context}

        {:error, error} ->
          verification_lock_error(error, :default)
      end
    end

    defp verification_lock_error(error, authority) do
      case postgres_code(error) do
        code when code in [:lock_not_available, :lock_timeout] ->
          {:error, {:lock_timeout, authority}}

        :query_canceled ->
          {:error, :admin_timeout}

        _ ->
          {:error, :readiness_failed}
      end
    end

    defp collect_evidence(control, rollout, gate, default, meta) do
      missing =
        control.repo.query!(
          """
          SELECT count(DISTINCT runs.scope_key)::bigint
          FROM #{control.identifiers.runs} AS runs
          WHERE NOT EXISTS (
            SELECT 1 FROM #{control.identifiers.partitions} AS partitions
            WHERE partitions.scope_key = runs.scope_key
          )
          """,
          [],
          log: false
        ).rows

      expected_tables = [
        "docket_graph_versions",
        "docket_runs",
        "docket_events",
        "docket_claim_policy",
        "docket_claim_partitions",
        "docket_claim_policy_events",
        "docket_claim_policy_receipts",
        "docket_claim_policy_holds",
        "docket_claim_audit_exports",
        "docket_claim_assertions",
        "docket_claim_rollout",
        "docket_claim_admission_gate",
        "docket_claim_capabilities"
      ]

      table_count =
        control.repo.query!(
          """
          SELECT count(*)::bigint
          FROM pg_class AS class
          JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
          WHERE namespace.nspname = $1 AND class.relkind = 'r'
            AND class.relname = ANY($2::text[])
          """,
          [control.prefix, expected_tables],
          log: false
        ).rows

      with [[missing_count]] <- missing,
           [[13]] <- table_count,
           {:ok, online} <- OnlineMigration.inspect_state(control.repo, control.prefix) do
        expected = OnlineDDL.index_fingerprints(control.prefix)

        reasons =
          []
          |> reason(rollout.generation != 2, :schema_generation)
          |> reason(rollout.assertion_kind != "dual_write", :dual_write_unattested)
          |> reason(rollout.backfill != "complete", :backfill_incomplete)
          |> reason(rollout.missing != 0 or missing_count != 0, :missing_partitions)
          |> reason(not default.initialized, :default_uninitialized)
          |> reason(gate.function_contract != 1, :gate_contract_invalid)
          |> reason(
            not online.ready_index_valid or not rollout.ready or
              rollout.ready_hash != expected.ready or
              meta.ready_index_ddl_sha256 != expected.ready,
            :ready_index_invalid
          )
          |> reason(
            not online.live_index_valid or not rollout.live or
              rollout.live_hash != expected.live or meta.live_index_ddl_sha256 != expected.live,
            :live_index_invalid
          )
          |> reason(
            not online.fk_definition_valid or online.fk_disposition != :validated or
              rollout.fk != "validated" or
              rollout.online != "complete",
            :foreign_key_unvalidated
          )
          |> reason(
            gate.readiness == "ready" and default.initialized and
              rollout.verified_default != default.fingerprint,
            :default_fingerprint_changed
          )
          |> Enum.sort()

        {:ok,
         %{
           reasons: reasons,
           missing_count: missing_count,
           ready_index_valid: online.ready_index_valid,
           live_index_valid: online.live_index_valid,
           fk_disposition: online.fk_disposition,
           fk_definition_valid: online.fk_definition_valid,
           ready_hash: expected.ready,
           live_hash: expected.live
         }}
      else
        [[_missing_count]] when table_count != [[13]] ->
          {:ok,
           %{
             reasons: [:schema_generation],
             missing_count: rollout.missing || 0,
             ready_index_valid: false,
             live_index_valid: false,
             fk_disposition: :absent,
             fk_definition_valid: false,
             ready_hash: OnlineDDL.index_fingerprint(control.prefix, :ready),
             live_hash: OnlineDDL.index_fingerprint(control.prefix, :live)
           }}

        _ ->
          {:error, :readiness_failed}
      end
    end

    defp complete_verification(control, meta, fingerprint, gate, rollout, default, evidence) do
      cond do
        evidence.reasons == [] and gate.readiness == "not_ready" ->
          commit_readiness_change(
            control,
            meta,
            fingerprint,
            gate,
            rollout,
            default,
            evidence,
            :applied
          )

        evidence.reasons == [] ->
          commit_readiness_change(
            control,
            meta,
            fingerprint,
            gate,
            rollout,
            default,
            evidence,
            :unchanged
          )

        gate.readiness == "ready" ->
          commit_readiness_change(
            control,
            meta,
            fingerprint,
            gate,
            rollout,
            default,
            evidence,
            :demoted
          )

        true ->
          {:error, {:not_ready, evidence.reasons}}
      end
    end

    defp commit_readiness_change(
           control,
           meta,
           fingerprint,
           gate,
           rollout,
           default,
           evidence,
           outcome
         ) do
      next_epoch = if outcome == :unchanged, do: gate.epoch, else: gate.epoch + 1

      if outcome != :unchanged do
        readiness = if outcome == :demoted, do: "not_ready", else: "ready"
        {online_phase, online_complete?} = evidence_online_phase(evidence)

        control.repo.query!(
          """
          UPDATE #{control.identifiers.gate}
          SET readiness = $1, readiness_epoch = $2, updated_at = CURRENT_TIMESTAMP
          WHERE id = 1
          """,
          [readiness, next_epoch],
          log: false
        )

        control.repo.query!(
          """
          UPDATE #{control.identifiers.rollout}
          SET online_phase = $1,
              online_completed_at = CASE WHEN $2 THEN COALESCE(online_completed_at, CURRENT_TIMESTAMP) ELSE NULL END,
              ready_index_valid = $3, live_index_valid = $4,
              ready_index_ddl_sha256 = CASE WHEN $3 THEN $5::bytea ELSE NULL END,
              live_index_ddl_sha256 = CASE WHEN $4 THEN $6::bytea ELSE NULL END,
              fk_disposition = $7, missing_partition_count = $8,
              verified_default_fingerprint = $9, verified_at = CURRENT_TIMESTAMP,
              updated_at = CURRENT_TIMESTAMP
          WHERE id = 1
          """,
          [
            online_phase,
            online_complete?,
            evidence.ready_index_valid,
            evidence.live_index_valid,
            evidence.ready_hash,
            evidence.live_hash,
            Atom.to_string(evidence.fk_disposition),
            evidence.missing_count,
            if(outcome == :demoted, do: rollout.verified_default, else: default.fingerprint)
          ],
          log: false
        )
      else
        control.repo.query!(
          """
          UPDATE #{control.identifiers.rollout}
          SET verified_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
          WHERE id = 1
          """,
          [],
          log: false
        )
      end

      operation =
        case outcome do
          :applied -> "readiness_promoted"
          :unchanged -> "readiness_unchanged"
          :demoted -> "readiness_demoted"
        end

      after_value =
        Codec.json_encode(%{
          readiness: if(outcome == :demoted, do: :not_ready, else: :ready),
          reasons: evidence.reasons
        })

      before_value =
        Codec.json_encode(%{
          readiness: gate.readiness,
          readiness_epoch: gate.epoch,
          admission_mode: gate.mode,
          mode_epoch: gate.mode_epoch,
          schema_generation: rollout.generation,
          online_phase: rollout.online,
          ready_index_valid: rollout.ready,
          live_index_valid: rollout.live,
          fk_disposition: rollout.fk,
          missing_partition_count: rollout.missing
        })

      [[audit_id]] =
        control.repo.query!(
          """
          INSERT INTO #{control.identifiers.events}
            (target_kind, target_keys, operation, actor, source, event_id,
             request_fingerprint, before_value, after_value, before_versions, after_versions,
             mode_epoch)
          VALUES
            ('readiness', ARRAY['readiness']::text[], $1, $2, $3, $4, $5,
             convert_from($6::bytea, 'UTF8')::jsonb,
             convert_from($7::bytea, 'UTF8')::jsonb,
             ARRAY[$8]::bigint[], ARRAY[$9]::bigint[], $10)
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
            next_epoch,
            gate.mode_epoch
          ],
          log: false
        ).rows

      control.repo.query!(
        """
        INSERT INTO #{control.identifiers.receipts}
          (source, event_id, request_fingerprint, target_kind, target_fingerprints,
           outcome, previous_versions, versions, audit_id, result_value)
        VALUES
          ($1, $2, $3, 'readiness', ARRAY[$4]::bytea[], $5,
           ARRAY[$6]::bigint[], ARRAY[$7]::bigint[], $8,
           convert_from($9::bytea, 'UTF8')::jsonb)
        """,
        [
          meta.source,
          meta.event_id,
          fingerprint,
          Codec.target_fingerprint("readiness"),
          Atom.to_string(outcome),
          gate.epoch,
          next_epoch,
          audit_id,
          Codec.json_encode(%{reasons: evidence.reasons})
        ],
        log: false
      )

      result = %{
        outcome: outcome,
        target: :readiness,
        previous_version: gate.epoch,
        version: next_epoch,
        audit_id: audit_id
      }

      if outcome == :demoted, do: Map.put(result, :reasons, evidence.reasons), else: result
    end

    defp evidence_online_phase(evidence) do
      cond do
        evidence.ready_index_valid and evidence.live_index_valid and
            evidence.fk_disposition == :validated ->
          {"complete", true}

        evidence.ready_index_valid and evidence.live_index_valid and
            evidence.fk_disposition == :not_valid ->
          {"fk_not_valid", false}

        evidence.ready_index_valid and evidence.live_index_valid ->
          {"live_index", false}

        evidence.ready_index_valid ->
          {"ready_index", false}

        true ->
          {"not_started", false}
      end
    end

    defp verification_replay(control, meta, fingerprint) do
      rows =
        control.repo.query!(
          """
          SELECT receipt.request_fingerprint, receipt.target_kind, receipt.outcome,
                 receipt.previous_versions[1], receipt.versions[1], receipt.audit_id,
                 receipt.result_value::text
          FROM #{control.identifiers.receipts} AS receipt
          WHERE receipt.source = $1 AND receipt.event_id = $2
          """,
          [meta.source, meta.event_id],
          log: false
        ).rows

      case rows do
        [] ->
          {:new, nil}

        [[^fingerprint, "readiness", outcome, previous, version, audit_id, result_json]] ->
          result_value = Codec.json_decode!(result_json)

          original = %{
            outcome: String.to_existing_atom(outcome),
            target: :readiness,
            previous_version: previous,
            version: version,
            audit_id: audit_id
          }

          original =
            if outcome == "demoted" do
              reasons =
                (result_value["reasons"] || result_value[:reasons] || [])
                |> Enum.map(&String.to_existing_atom/1)

              Map.put(original, :reasons, Enum.sort(reasons))
            else
              original
            end

          {:replay, %{outcome: :replayed, original: original}}

        [[_fingerprint, _kind, _outcome, _previous, _version, _audit, _after]] ->
          {:error, {:event_conflict, %{source: meta.source, event_id: meta.event_id}}}
      end
    end

    defp verify_expected_epoch(%{epoch: epoch}, %{expected_readiness_epoch: epoch}), do: :ok

    defp verify_expected_epoch(%{epoch: actual}, %{expected_readiness_epoch: expected}) do
      {:error, {:version_conflict, %{target: :readiness, expected: expected, actual: actual}}}
    end

    defp validate_verify_opts(opts) when is_list(opts) do
      allowed = [
        :actor,
        :event_id,
        :expected_readiness_epoch,
        :live_index_ddl_sha256,
        :ready_index_ddl_sha256,
        :source
      ]

      with true <- Keyword.keyword?(opts),
           true <- Enum.sort(Keyword.keys(opts)) == Enum.sort(allowed),
           epoch when is_integer(epoch) and epoch >= 0 and epoch < 9_223_372_036_854_775_807 <-
             Keyword.get(opts, :expected_readiness_epoch),
           ready when is_binary(ready) and byte_size(ready) == 32 <-
             Keyword.get(opts, :ready_index_ddl_sha256),
           live when is_binary(live) and byte_size(live) == 32 <-
             Keyword.get(opts, :live_index_ddl_sha256),
           source <- Keyword.get(opts, :source),
           event_id <- Keyword.get(opts, :event_id),
           actor <- Keyword.get(opts, :actor),
           true <- bounded_binary?(source, 64),
           true <- bounded_binary?(event_id, 255),
           true <- bounded_binary?(actor, 255) do
        {:ok,
         %{
           expected_readiness_epoch: epoch,
           ready_index_ddl_sha256: ready,
           live_index_ddl_sha256: live,
           source: source,
           event_id: event_id,
           actor: actor
         }}
      else
        _ -> {:error, :invalid_readiness_options}
      end
    end

    defp validate_verify_opts(_opts), do: {:error, :invalid_readiness_options}

    defp reason(reasons, true, reason), do: [reason | reasons]
    defp reason(reasons, false, _reason), do: reasons

    defp transact_attestation(control, meta, fingerprint, assertion_id) do
      case control.repo.transaction(fn ->
             configure_transaction(control.repo)

             with {:new, nil} <- replay(control, meta, fingerprint, assertion_id),
                  :ok <- lock_rollout(control),
                  {:new, nil} <- replay(control, meta, fingerprint, assertion_id),
                  {:ok, audit_id} <- insert_event(control, meta, fingerprint, assertion_id),
                  :ok <- insert_assertion(control, meta, assertion_id, audit_id),
                  :ok <- link_rollout(control, assertion_id),
                  :ok <- insert_receipt(control, meta, fingerprint, audit_id) do
               applied(assertion_id, audit_id)
             else
               {:replay, result} -> result
               {:error, reason} -> control.repo.rollback(reason)
             end
           end) do
        {:ok, value} -> {:ok, value}
        {:error, reason} -> {:error, reason}
      end
    rescue
      error in Postgrex.Error -> {:error, error}
      _error -> {:error, :readiness_failed}
    catch
      _kind, _reason -> {:error, :readiness_failed}
    end

    defp retry_source_event_race(
           {:error, %Postgrex.Error{} = error},
           control,
           meta,
           fingerprint,
           id
         ) do
      if source_event_race?(error),
        do: transact_attestation(control, meta, fingerprint, id),
        else: {:error, error}
    end

    defp retry_source_event_race(result, _control, _meta, _fingerprint, _id), do: result

    defp configure_transaction(repo) do
      repo.query!("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE", [], log: false)

      repo.query!("SELECT set_config('lock_timeout', $1, true)", ["#{@lock_timeout_ms}ms"],
        log: false
      )

      repo.query!(
        "SELECT set_config('statement_timeout', $1, true)",
        [
          "#{@statement_timeout_ms}ms"
        ],
        log: false
      )

      :ok
    end

    defp lock_rollout(control) do
      case control.repo.query!(
             "SELECT id FROM #{control.identifiers.rollout} WHERE id = 1 FOR UPDATE",
             [],
             log: false
           ).rows do
        [[1]] -> :ok
        _ -> {:error, :invalid_admin_context}
      end
    end

    defp replay(control, meta, fingerprint, assertion_id) do
      rows =
        control.repo.query!(
          """
          SELECT request_fingerprint, target_kind, outcome, audit_id
          FROM #{control.identifiers.receipts}
          WHERE source = $1 AND event_id = $2
          """,
          [meta.source, meta.event_id],
          log: false
        ).rows

      case rows do
        [] ->
          {:new, nil}

        [[^fingerprint, "readiness", "applied", audit_id]] ->
          {:replay, %{outcome: :replayed, original: applied(assertion_id, audit_id)}}

        [[_fingerprint, _kind, _outcome, _audit_id]] ->
          {:error, {:event_conflict, %{source: meta.source, event_id: meta.event_id}}}
      end
    end

    defp insert_event(control, meta, fingerprint, assertion_id) do
      after_json = Codec.json_encode(%{assertion_id: assertion_id})

      case control.repo.query!(
             """
             INSERT INTO #{control.identifiers.events}
               (target_kind, target_keys, operation, actor, source, event_id,
                request_fingerprint, before_value, after_value, before_versions, after_versions)
             VALUES
               ('readiness', ARRAY['dual_write']::text[], 'attest_dual_write', $1, $2, $3,
                $4, '{}'::jsonb, convert_from($5::bytea, 'UTF8')::jsonb,
                ARRAY[0]::bigint[], ARRAY[1]::bigint[])
             RETURNING audit_id
             """,
             [meta.actor, meta.source, meta.event_id, fingerprint, after_json],
             log: false
           ).rows do
        [[audit_id]] -> {:ok, audit_id}
        _ -> {:error, :invalid_admin_context}
      end
    end

    defp insert_assertion(control, meta, assertion_id, audit_id) do
      control.repo.query!(
        """
        INSERT INTO #{control.identifiers.assertions}
          (assertion_id, assertion_kind, evidence_fingerprint, actor, source, event_id, audit_id)
        VALUES ($1::text::uuid, 'dual_write', $2, $3, $4, $5, $6)
        """,
        [
          assertion_id,
          meta.evidence_fingerprint,
          meta.actor,
          meta.source,
          meta.event_id,
          audit_id
        ],
        log: false
      )

      :ok
    end

    defp link_rollout(control, assertion_id) do
      control.repo.query!(
        """
        UPDATE #{control.identifiers.rollout}
        SET dual_write_assertion_id = $1::text::uuid, updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """,
        [assertion_id],
        log: false
      )

      :ok
    end

    defp insert_receipt(control, meta, fingerprint, audit_id) do
      control.repo.query!(
        """
        INSERT INTO #{control.identifiers.receipts}
          (source, event_id, request_fingerprint, target_kind, target_fingerprints,
           outcome, previous_versions, versions, audit_id)
        VALUES
          ($1, $2, $3, 'readiness', ARRAY[$4]::bytea[], 'applied',
           ARRAY[0]::bigint[], ARRAY[1]::bigint[], $5)
        """,
        [
          meta.source,
          meta.event_id,
          fingerprint,
          Codec.target_fingerprint("dual_write"),
          audit_id
        ],
        log: false
      )

      :ok
    end

    defp applied(assertion_id, audit_id) do
      %{
        outcome: :applied,
        target: :dual_write,
        assertion_id: assertion_id,
        audit_id: audit_id
      }
    end

    defp validate_opts(opts) when is_list(opts) do
      allowed = [:actor, :evidence_fingerprint, :event_id, :source]

      with true <- Keyword.keyword?(opts),
           true <- Enum.sort(Keyword.keys(opts)) == Enum.sort(allowed),
           evidence when is_binary(evidence) <- Keyword.get(opts, :evidence_fingerprint),
           true <- byte_size(evidence) == 32,
           source <- Keyword.get(opts, :source),
           event_id <- Keyword.get(opts, :event_id),
           actor <- Keyword.get(opts, :actor),
           true <- bounded_binary?(source, 64),
           true <- bounded_binary?(event_id, 255),
           true <- bounded_binary?(actor, 255) do
        {:ok, %{evidence_fingerprint: evidence, source: source, event_id: event_id, actor: actor}}
      else
        _ -> {:error, :invalid_readiness_options}
      end
    end

    defp validate_opts(_opts), do: {:error, :invalid_readiness_options}

    defp bounded_binary?(value, max) do
      is_binary(value) and byte_size(value) in 1..max and String.valid?(value) and
        not String.contains?(value, <<0>>)
    end

    defp normalize_error({:error, %Postgrex.Error{} = error}) do
      case postgres_code(error) do
        code when code in [:lock_not_available, :lock_timeout] ->
          {:error, {:lock_timeout, :rollout}}

        :query_canceled ->
          {:error, :admin_timeout}

        _ ->
          {:error, :invalid_admin_context}
      end
    end

    defp normalize_error({:error, reason})
         when reason in [
                :invalid_readiness_options,
                :invalid_admin_context,
                :transaction_context_forbidden,
                :admin_timeout,
                :readiness_failed
              ],
         do: {:error, reason}

    defp normalize_error({:error, {:event_conflict, %{source: source, event_id: event_id}}})
         when is_binary(source) and is_binary(event_id),
         do: {:error, {:event_conflict, %{source: source, event_id: event_id}}}

    defp normalize_error({:error, {:lock_timeout, :rollout}}),
      do: {:error, {:lock_timeout, :rollout}}

    defp normalize_error({:error, {:lock_timeout, :gate}}),
      do: {:error, {:lock_timeout, :gate}}

    defp normalize_error({:error, {:lock_timeout, :default}}),
      do: {:error, {:lock_timeout, :default}}

    defp normalize_error({:error, {:not_ready, reasons}}) when is_list(reasons),
      do: {:error, {:not_ready, reasons}}

    defp normalize_error(
           {:error,
            {:version_conflict,
             %{target: :readiness, expected: expected, actual: actual} = conflict}}
         )
         when is_integer(expected) and is_integer(actual),
         do: {:error, {:version_conflict, conflict}}

    defp normalize_error({:error, _reason}), do: {:error, :readiness_failed}
    defp normalize_error(result), do: result

    defp source_event_race?(error) do
      postgres_code(error) == :unique_violation and
        Map.get(error.postgres, :constraint) in [
          "docket_claim_policy_events_source_event_index",
          "docket_claim_policy_receipts_pkey",
          "docket_claim_assertions_source_event_index"
        ]
    end

    defp postgres_code(%Postgrex.Error{postgres: postgres}) when is_map(postgres),
      do: Map.get(postgres, :code)

    defp postgres_code(_error), do: nil
  end
end
