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
    alias Docket.Postgres.ClaimPolicy.ControlContext

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
