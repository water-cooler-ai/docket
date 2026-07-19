if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Test.FairRotationProofJournal do
    @moduledoc false

    alias Docket.Postgres.ClaimPolicy.TenantFair.{Budgets, RingFunction}
    alias Docket.Test.FairRotationAdversarialVerifier

    @result_definition RingFunction.result_definition()

    @doc false
    def install!(repo) do
      Enum.each(schema_statements(), &repo.query!/1)
      :ok
    end

    @doc false
    def open_window!(repo, target, now, cutoff, max_attempts)
        when is_binary(target) and is_integer(max_attempts) do
      id = Ecto.UUID.generate()

      [[^id]] =
        repo.query!(
          """
          INSERT INTO docket_fair_proof_windows (
            window_id, target_scope, opened_xid, opened_call_seq, opened_audit_seq,
            opened_cursor, ring_snapshot, count_snapshot, policy_snapshot,
            epoch_snapshot, target_witness
          )
          SELECT
            $1::uuid,
            $2::text,
            pg_catalog.txid_current(),
            clock.scan_call_seq,
            COALESCE((SELECT max(audit_seq) FROM docket_fair_proof_audit), 0),
            policy.scan_ring_position,
            COALESCE((
              SELECT jsonb_agg(jsonb_build_object(
                'position', schedule.ring_position,
                'scope', schedule.scope_key,
                'unfinished_count', schedule.unfinished_count
              ) ORDER BY schedule.ring_position)
              FROM docket_claim_schedule AS schedule
              WHERE schedule.unfinished_count > 0
            ), '[]'::jsonb),
            COALESCE((
              SELECT jsonb_agg(jsonb_build_object(
                'scope', partition.scope_key,
                'recorded', schedule.unfinished_count,
                'independent', (
                  SELECT count(*)
                  FROM docket_runs AS run
                  WHERE run.scope_key = partition.scope_key
                    AND run.status IN ('running', 'waiting')
                )
              ) ORDER BY partition.scope_key)
              FROM docket_claim_partitions AS partition
              JOIN docket_claim_schedule AS schedule USING (scope_key)
            ), '[]'::jsonb),
            docket_fair_proof_policy_snapshot(),
            docket_fair_proof_epoch_snapshot(),
            docket_fair_proof_target_witness($2, $3, $4, $5)
          FROM docket_fair_proof_clock AS clock
          CROSS JOIN docket_claim_policy AS policy
          WHERE clock.id = true AND policy.id = 1
          RETURNING window_id::text
          """,
          [Ecto.UUID.dump!(id), target, now, cutoff, max_attempts]
        ).rows

      id
    end

    @doc false
    def claim!(repo, window_id, now, cutoff, opts \\ []) do
      demand = Keyword.get(opts, :demand, 1)
      max_attempts = Keyword.get(opts, :max_attempts, 5)
      preference = Keyword.get(opts, :preference)
      default_max = Keyword.get(opts, :default_max, 2_000)

      repo.query!(
        """
        WITH witness AS MATERIALIZED (
          SELECT
            proof_window.target_scope,
            policy.scan_ring_position AS cursor_before_call,
            docket_fair_proof_ring_snapshot() AS ring_before_call,
            docket_fair_proof_policy_snapshot() AS policy_before_call,
            docket_fair_proof_target_witness(proof_window.target_scope, $2, $3, $5)
              AS target_witness
          FROM docket_fair_proof_windows AS proof_window
          CROSS JOIN docket_claim_policy AS policy
          WHERE proof_window.window_id = $1::uuid AND policy.id = 1
        ),
        raw AS MATERIALIZED (
          SELECT claimed.*
          FROM witness
          CROSS JOIN LATERAL docket_tenant_fair_claim($2, $3, $4, $5, $6, $7, true)
            AS claimed(#{@result_definition})
          WHERE (witness.target_witness->>'eligible')::boolean
        ),
        clock AS (
          UPDATE docket_fair_proof_clock
          SET scan_call_seq = scan_call_seq + 1
          WHERE id = true AND EXISTS (SELECT 1 FROM raw)
          RETURNING scan_call_seq
        ),
        recorded_call AS (
          INSERT INTO docket_fair_proof_calls (
            window_id, scan_call_seq, call_token, transaction_id, demand,
            cursor_before, cursor_after, row_count, row_digest,
            ring_before_call, policy_before_call, target_witness, epoch_after
          )
          SELECT
            $1::uuid,
            clock.scan_call_seq,
            (array_agg(raw.call_token))[1],
            min(raw.transaction_id),
            min(raw.demand),
            COALESCE((
              SELECT inspected.cursor_before
              FROM raw AS inspected
              WHERE inspected.row_kind = 'inspection'
              ORDER BY inspected.visit_ordinal
              LIMIT 1
            ), witness.cursor_before_call),
            COALESCE((
              SELECT inspected.cursor_after
              FROM raw AS inspected
              WHERE inspected.row_kind = 'inspection'
              ORDER BY inspected.visit_ordinal DESC
              LIMIT 1
            ), witness.cursor_before_call),
            count(*)::integer,
            md5(string_agg(to_jsonb(raw)::text, '|' ORDER BY
              raw.visit_ordinal NULLS FIRST,
              raw.outcome_ordinal NULLS FIRST,
              raw.row_kind)),
            witness.ring_before_call,
            witness.policy_before_call,
            witness.target_witness,
            docket_fair_proof_epoch_snapshot()
          FROM raw
          CROSS JOIN clock
          CROSS JOIN witness
          GROUP BY clock.scan_call_seq, witness.cursor_before_call,
                   witness.ring_before_call, witness.policy_before_call,
                   witness.target_witness
          RETURNING scan_call_seq
        ),
        recorded_rows AS (
          INSERT INTO docket_fair_proof_rows (
            window_id, scan_call_seq, row_order, trace_row, durable_row
          )
          SELECT
            $1::uuid,
            recorded_call.scan_call_seq,
            row_number() OVER (ORDER BY
              raw.visit_ordinal NULLS FIRST,
              raw.outcome_ordinal NULLS FIRST,
              raw.row_kind),
            to_jsonb(raw),
            CASE WHEN raw.row_kind = 'outcome'
              THEN docket_fair_proof_durable_run(raw.run_id)
              ELSE NULL
            END
          FROM raw
          CROSS JOIN recorded_call
          RETURNING scan_call_seq, row_order, trace_row
        )
        SELECT scan_call_seq, row_order, trace_row::text
        FROM recorded_rows
        ORDER BY row_order
        """,
        [Ecto.UUID.dump!(window_id), now, cutoff, demand, max_attempts, preference, default_max]
      ).rows
      |> Enum.map(fn [sequence, _row_order, row] ->
        row
        |> decode_json()
        |> atomize_trace_row()
        |> Map.put(:database_call_sequence, sequence)
      end)
    end

    @doc false
    def verify!(repo, window_id, opts) do
      window = load_window!(repo, window_id)
      calls = load_calls(repo, window_id)
      rows = load_rows(repo, window_id)

      if calls == [] or rows == [] do
        fail!("committed proof journal contains no qualifying calls")
      end

      assert_open_authority!(window)
      assert_call_sequences!(window, calls)
      assert_call_completeness!(calls, rows)
      assert_boundary_evidence!(window, calls)
      assert_work_counters!(calls, rows)

      events = inspection_events(rows)

      {through_target, target_call, _target_run_ids} =
        through_target!(events, window.target_scope)

      assert_audit_continuity!(repo, window, calls, target_call)
      assert_epoch_accounting!(window, calls, rows)
      _all_durable_outcome_ids = assert_durable_outcomes!(rows)
      durable_outcome_ids = Enum.flat_map(through_target, & &1.outcome_ids)

      cohort =
        through_target
        |> Enum.take_while(fn event ->
          not (event.partition == window.target_scope and event.disposition == :grant)
        end)
        |> Enum.filter(&(&1.disposition == :grant))
        |> Enum.map(& &1.partition)
        |> MapSet.new()
        |> MapSet.put(window.target_scope)
        |> MapSet.to_list()

      ring = Enum.map(window.ring_snapshot, &{&1["position"], &1["scope"]})
      first_sequence = hd(through_target).database_call_sequence

      result =
        FairRotationAdversarialVerifier.assert_trace!(through_target,
          target: window.target_scope,
          cohort: cohort,
          ring: ring,
          scan_budget: Keyword.get(opts, :scan_budget, Budgets.scan_inspections()),
          quantum: Keyword.get(opts, :quantum, Budgets.grant_outcomes()),
          lock_failures: Keyword.fetch!(opts, :lock_failures),
          first_database_call_sequence: first_sequence,
          committed_call_sequences:
            calls
            |> Enum.filter(&(&1.scan_call_seq <= target_call))
            |> Enum.map(& &1.scan_call_seq),
          durable_outcome_ids: durable_outcome_ids
        )

      Map.merge(result, %{
        journal_window_id: window_id,
        derived_cohort: Enum.sort(cohort),
        committed_call_sequences: Enum.map(calls, & &1.scan_call_seq),
        full_trace_rows: length(rows)
      })
    end

    defp load_window!(repo, id) do
      columns = [
        :window_id,
        :target_scope,
        :opened_xid,
        :opened_call_seq,
        :opened_audit_seq,
        :opened_cursor,
        :ring_snapshot,
        :count_snapshot,
        :policy_snapshot,
        :epoch_snapshot,
        :target_witness
      ]

      case repo.query!(
             """
             SELECT window_id::text, target_scope, opened_xid, opened_call_seq,
                    opened_audit_seq, opened_cursor, ring_snapshot::text,
                    count_snapshot::text, policy_snapshot::text,
                    epoch_snapshot::text, target_witness::text
             FROM docket_fair_proof_windows
             WHERE window_id = $1::uuid
             """,
             [Ecto.UUID.dump!(id)]
           ).rows do
        [row] ->
          window = Map.new(Enum.zip(columns, row))

          Enum.reduce(
            [:ring_snapshot, :count_snapshot, :policy_snapshot, :epoch_snapshot, :target_witness],
            window,
            fn key, decoded -> Map.update!(decoded, key, &decode_json/1) end
          )

        [] ->
          fail!("unknown proof window #{id}")
      end
    end

    defp load_calls(repo, id) do
      columns = [
        :scan_call_seq,
        :call_token,
        :transaction_id,
        :demand,
        :cursor_before,
        :cursor_after,
        :row_count,
        :row_digest,
        :ring_before_call,
        :policy_before_call,
        :target_witness,
        :epoch_after
      ]

      repo.query!(
        """
        SELECT scan_call_seq, call_token::text, transaction_id, demand,
               cursor_before, cursor_after, row_count, row_digest,
               ring_before_call::text, policy_before_call::text,
               target_witness::text, epoch_after::text
        FROM docket_fair_proof_calls
        WHERE window_id = $1::uuid
        ORDER BY scan_call_seq
        """,
        [Ecto.UUID.dump!(id)]
      ).rows
      |> Enum.map(fn row ->
        call = Map.new(Enum.zip(columns, row))

        Enum.reduce(
          [:ring_before_call, :policy_before_call, :target_witness, :epoch_after],
          call,
          fn key, decoded -> Map.update!(decoded, key, &decode_json/1) end
        )
      end)
    end

    defp load_rows(repo, id) do
      repo.query!(
        """
        SELECT scan_call_seq, row_order, trace_row::text, durable_row::text
        FROM docket_fair_proof_rows
        WHERE window_id = $1::uuid
        ORDER BY scan_call_seq, row_order
        """,
        [Ecto.UUID.dump!(id)]
      ).rows
      |> Enum.map(fn [sequence, row_order, trace, durable] ->
        %{
          scan_call_seq: sequence,
          row_order: row_order,
          trace: trace |> decode_json() |> atomize_trace_row(),
          durable: if(durable, do: decode_json(durable), else: nil)
        }
      end)
    end

    defp assert_open_authority!(window) do
      unless window.target_witness["eligible"] do
        fail!("target was not admissible when the proof window opened")
      end

      Enum.each(window.count_snapshot, fn count ->
        unless count["recorded"] == count["independent"] do
          fail!("unfinished-count authority disagrees with independent nonterminal count")
        end
      end)

      ring_from_counts =
        window.count_snapshot
        |> Enum.filter(&(&1["independent"] > 0))
        |> Enum.map(& &1["scope"])
        |> MapSet.new()

      ring_scopes = window.ring_snapshot |> Enum.map(& &1["scope"]) |> MapSet.new()

      unless ring_scopes == ring_from_counts do
        fail!("positive unfinished ring is not the complete nonterminal population")
      end

      positions = Enum.map(window.ring_snapshot, & &1["position"])
      scopes = Enum.map(window.ring_snapshot, & &1["scope"])

      unless positions == Enum.sort(positions) and Enum.uniq(positions) == positions and
               Enum.uniq(scopes) == scopes and window.target_scope in scopes do
        fail!("proof window ring is not ordered, duplicate-free, and target-complete")
      end
    end

    defp assert_call_sequences!(window, calls) do
      observed = Enum.map(calls, & &1.scan_call_seq)
      expected = Enum.to_list((window.opened_call_seq + 1)..List.last(observed))

      unless observed == expected do
        fail!("committed scan-call journal is reordered or has a gap")
      end

      unless hd(calls).cursor_before == window.opened_cursor do
        fail!("first journaled call does not begin at the window-open cursor")
      end

      calls
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [left, right] ->
        unless left.cursor_after == right.cursor_before do
          fail!("journaled calls do not form one contiguous database cursor sequence")
        end
      end)

      tokens = Enum.map(calls, & &1.call_token)

      unless Enum.all?(tokens, &is_binary/1) and Enum.uniq(tokens) == tokens do
        fail!("committed journal has missing or duplicate call identities")
      end
    end

    defp assert_call_completeness!(calls, rows) do
      Enum.each(calls, fn call ->
        call_rows = Enum.filter(rows, &(&1.scan_call_seq == call.scan_call_seq))

        unless length(call_rows) == call.row_count do
          fail!("journal row count does not match database-recorded call cardinality")
        end

        # The digest is database-authored; cardinality and row PKs separately
        # prove completeness to the Elixir verifier.
        unless is_binary(call.row_digest) and byte_size(call.row_digest) == 32 do
          fail!("journal call has no database-authored completeness digest")
        end

        orders = Enum.map(call_rows, & &1.row_order)

        unless orders == Enum.to_list(1..call.row_count) do
          fail!("journal rows are not complete and contiguous")
        end
      end)
    end

    defp assert_boundary_evidence!(window, calls) do
      open_ring = Enum.map(window.ring_snapshot, &Map.take(&1, ["position", "scope"]))

      Enum.each(calls, fn call ->
        call_ring = Enum.map(call.ring_before_call, &Map.take(&1, ["position", "scope"]))

        unless call_ring == open_ring do
          fail!("qualified window changed its frozen positive ring")
        end

        unless call.policy_before_call == window.policy_snapshot do
          fail!("qualified window changed policy, engine, cap, schema, or function identity")
        end

        unless call.target_witness["eligible"] do
          fail!("target was not continuously admissible before every qualifying call")
        end
      end)
    end

    defp assert_work_counters!(calls, rows) do
      k = Budgets.run_lock_attempts()
      q = Budgets.grant_outcomes()
      s = Budgets.scan_inspections()

      Enum.each(calls, fn call ->
        inspections =
          rows
          |> Enum.filter(&(&1.scan_call_seq == call.scan_call_seq))
          |> Enum.map(& &1.trace)
          |> Enum.filter(&(&1.row_kind == "inspection"))

        if length(inspections) > s do
          fail!("call exceeded S inspection visits")
        end

        Enum.each(inspections, fn row ->
          counters = [
            row.ready_structural_count,
            row.expired_structural_count,
            row.attempt_set_count,
            row.exact_lock_attempt_count,
            row.locked_count,
            row.mutation_input_count,
            row.outcome_count
          ]

          unless Enum.all?(counters, &(is_integer(&1) and &1 >= 0)) do
            fail!("inspection is missing database-authored logical-work counters")
          end

          unless row.ready_structural_count <= k and row.expired_structural_count <= k and
                   row.attempt_set_count <= k and row.exact_lock_attempt_count <= k and
                   row.exact_lock_attempt_count == row.attempt_set_count and
                   row.locked_count <= row.exact_lock_attempt_count and
                   row.mutation_input_count <= q and row.outcome_count <= q do
            fail!("inspection exceeded K/Q structural, lock, or mutation ceilings")
          end
        end)

        if Enum.sum(Enum.map(inspections, & &1.exact_lock_attempt_count)) > s * k do
          fail!("call exceeded S*K exact-lock attempts")
        end

        if Enum.sum(Enum.map(inspections, & &1.mutation_input_count)) > s * q do
          fail!("call exceeded S*M mutation inputs")
        end
      end)
    end

    defp inspection_events(rows) do
      Enum.flat_map(rows, fn row ->
        if row.trace.row_kind == "inspection" do
          outcomes =
            rows
            |> Enum.filter(fn candidate ->
              candidate.scan_call_seq == row.scan_call_seq and
                candidate.trace.row_kind == "outcome" and
                candidate.trace.visit_ordinal == row.trace.visit_ordinal
            end)
            |> Enum.map(& &1.trace.run_id)

          [
            %{
              database_call_sequence: row.scan_call_seq,
              ordinal: row.trace.visit_ordinal,
              cursor_before: row.trace.cursor_before,
              cursor_after: row.trace.cursor_after,
              demand: row.trace.demand,
              partition: row.trace.scope_key,
              disposition: normalize_disposition(row.trace.disposition),
              outcomes: row.trace.outcome_count,
              outcome_ids: outcomes,
              epoch_delta: row.trace.epoch_delta,
              committed: true
            }
          ]
        else
          []
        end
      end)
    end

    defp through_target!(events, target_scope) do
      target =
        Enum.find(events, fn event ->
          event.partition == target_scope and event.disposition == :grant
        end)

      if target == nil do
        fail!("committed proof trace has no target grant")
      end

      through_target_call =
        Enum.take_while(events, &(&1.database_call_sequence <= target.database_call_sequence))

      {through_target_call, target.database_call_sequence, target.outcome_ids}
    end

    defp assert_audit_continuity!(repo, window, calls, target_call) do
      target_tx =
        calls |> Enum.find(&(&1.scan_call_seq == target_call)) |> Map.fetch!(:transaction_id)

      audits =
        repo.query!(
          """
          SELECT kind, scope_key, run_id, xid
          FROM docket_fair_proof_audit
          WHERE audit_seq > $1
          ORDER BY audit_seq
          """,
          [window.opened_audit_seq]
        ).rows

      Enum.each(audits, fn [kind, scope, _run_id, xid] ->
        expected_close_mutation = xid == target_tx and scope == window.target_scope

        cond do
          kind in ["policy", "cap", "function"] ->
            fail!("qualification fact changed during the committed window: #{kind}")

          kind == "ring" and not (xid == target_tx and scope == window.target_scope) ->
            fail!("positive ring membership changed during the committed window")

          kind == "run" and scope == window.target_scope and not expected_close_mutation ->
            fail!("target admissibility changed during the committed window")

          true ->
            :ok
        end
      end)
    end

    defp assert_epoch_accounting!(window, calls, rows) do
      Enum.reduce(calls, window.epoch_snapshot, fn call, before ->
        expected =
          rows
          |> Enum.filter(fn row ->
            row.scan_call_seq == call.scan_call_seq and row.trace.row_kind == "inspection" and
              row.trace.disposition == "grant"
          end)
          |> Enum.frequencies_by(& &1.trace.scope_key)

        actual =
          Map.new(before, fn {scope, epoch_before} ->
            {scope, Map.fetch!(call.epoch_after, scope) - epoch_before}
          end)

        normalized_expected =
          Map.new(actual, fn {scope, _} -> {scope, Map.get(expected, scope, 0)} end)

        unless actual == normalized_expected do
          fail!("durable epochs reveal an omitted or misaccounted grant")
        end

        call.epoch_after
      end)
    end

    defp assert_durable_outcomes!(rows) do
      outcomes = Enum.filter(rows, &(&1.trace.row_kind == "outcome"))
      ids = Enum.map(outcomes, & &1.trace.run_id)

      unless Enum.all?(ids, &is_binary/1) and Enum.uniq(ids) == ids do
        fail!("outcome identities are missing or repeated across the proof window")
      end

      Enum.each(outcomes, fn row ->
        durable = row.durable || fail!("outcome has no independent durable run evidence")

        unless durable["run_id"] == row.trace.run_id and
                 durable["scope_key"] == row.trace.scope_key and
                 durable["poison_reason"] == row.trace.poison_reason and
                 durable["claim_attempts"] == row.trace.claim_attempt do
          fail!(
            "trace outcome disagrees with durable run mutation: " <>
              "trace=#{inspect(row.trace)} durable=#{inspect(durable)}"
          )
        end

        if row.trace.poison_reason do
          unless durable["claim_token"] == nil and durable["poisoned_at"] != nil do
            fail!("poison outcome installed a token or lacks durable poison state")
          end
        else
          unless durable["claim_token"] == row.trace.claim_token and
                   durable["claimed_at"] != nil do
            fail!("lease outcome disagrees with durable claim state")
          end
        end
      end)

      ids
    end

    defp normalize_disposition("grant"), do: :grant
    defp normalize_disposition("partition_lock_skip"), do: :lock_skip
    defp normalize_disposition("lock_miss"), do: :lock_skip
    defp normalize_disposition("denied"), do: :cap_denied
    defp normalize_disposition("cap_debt_denial"), do: :cap_denied
    defp normalize_disposition("stale"), do: :stale
    defp normalize_disposition("empty_page"), do: :empty
    defp normalize_disposition(value), do: fail!("unknown trace disposition #{inspect(value)}")

    defp atomize_trace_row(row) do
      Map.new(row, fn {key, value} -> {String.to_existing_atom(key), value} end)
    end

    defp decode_json(value) when is_binary(value), do: value |> :json.decode() |> normalize_json()

    defp normalize_json(:null), do: nil
    defp normalize_json(value) when is_list(value), do: Enum.map(value, &normalize_json/1)

    defp normalize_json(value) when is_map(value) do
      Map.new(value, fn {key, nested} -> {key, normalize_json(nested)} end)
    end

    defp normalize_json(value), do: value

    defp schema_statements do
      [
        """
        CREATE TABLE docket_fair_proof_clock (
          id boolean PRIMARY KEY DEFAULT true CHECK (id),
          scan_call_seq bigint NOT NULL DEFAULT 0
        )
        """,
        "INSERT INTO docket_fair_proof_clock (id) VALUES (true)",
        """
        CREATE TABLE docket_fair_proof_audit (
          audit_seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          kind text NOT NULL,
          scope_key text,
          run_id text,
          xid bigint NOT NULL DEFAULT pg_catalog.txid_current()
        )
        """,
        """
        CREATE TABLE docket_fair_proof_windows (
          window_id uuid PRIMARY KEY,
          target_scope text NOT NULL,
          opened_xid bigint NOT NULL,
          opened_call_seq bigint NOT NULL,
          opened_audit_seq bigint NOT NULL,
          opened_cursor bigint NOT NULL,
          ring_snapshot jsonb NOT NULL,
          count_snapshot jsonb NOT NULL,
          policy_snapshot jsonb NOT NULL,
          epoch_snapshot jsonb NOT NULL,
          target_witness jsonb NOT NULL
        )
        """,
        """
        CREATE TABLE docket_fair_proof_calls (
          window_id uuid NOT NULL REFERENCES docket_fair_proof_windows(window_id),
          scan_call_seq bigint NOT NULL UNIQUE,
          call_token uuid NOT NULL UNIQUE,
          transaction_id bigint NOT NULL,
          demand integer NOT NULL,
          cursor_before bigint NOT NULL,
          cursor_after bigint NOT NULL,
          row_count integer NOT NULL CHECK (row_count > 0),
          row_digest text NOT NULL,
          ring_before_call jsonb NOT NULL,
          policy_before_call jsonb NOT NULL,
          target_witness jsonb NOT NULL,
          epoch_after jsonb NOT NULL,
          PRIMARY KEY (window_id, scan_call_seq)
        )
        """,
        """
        CREATE TABLE docket_fair_proof_rows (
          window_id uuid NOT NULL,
          scan_call_seq bigint NOT NULL,
          row_order integer NOT NULL,
          trace_row jsonb NOT NULL,
          durable_row jsonb,
          PRIMARY KEY (window_id, scan_call_seq, row_order),
          FOREIGN KEY (window_id, scan_call_seq)
            REFERENCES docket_fair_proof_calls(window_id, scan_call_seq)
        )
        """,
        target_witness_function(),
        ring_snapshot_function(),
        epoch_snapshot_function(),
        policy_snapshot_function(),
        durable_run_function(),
        audit_function(),
        """
        CREATE TRIGGER docket_fair_proof_schedule_audit
        AFTER INSERT OR UPDATE OR DELETE ON docket_claim_schedule
        FOR EACH ROW EXECUTE FUNCTION docket_fair_proof_audit_change()
        """,
        """
        CREATE TRIGGER docket_fair_proof_policy_audit
        AFTER UPDATE ON docket_claim_policy
        FOR EACH ROW EXECUTE FUNCTION docket_fair_proof_audit_change()
        """,
        """
        CREATE TRIGGER docket_fair_proof_partition_audit
        AFTER INSERT OR UPDATE OR DELETE ON docket_claim_partitions
        FOR EACH ROW EXECUTE FUNCTION docket_fair_proof_audit_change()
        """,
        """
        CREATE TRIGGER docket_fair_proof_run_audit
        AFTER INSERT OR UPDATE OR DELETE ON docket_runs
        FOR EACH ROW EXECUTE FUNCTION docket_fair_proof_audit_change()
        """,
        ddl_audit_function(),
        """
        CREATE EVENT TRIGGER docket_fair_proof_ddl_audit
        ON ddl_command_end
        WHEN TAG IN ('CREATE FUNCTION', 'ALTER FUNCTION', 'DROP FUNCTION')
        EXECUTE FUNCTION docket_fair_proof_audit_ddl()
        """
      ]
    end

    defp target_witness_function do
      """
      CREATE FUNCTION docket_fair_proof_target_witness(
        p_scope text,
        p_now timestamp with time zone,
        p_cutoff timestamp with time zone,
        p_max_attempts integer
      ) RETURNS jsonb
      LANGUAGE sql VOLATILE
      AS $proof$
        WITH authority AS (
          SELECT COALESCE(partition.max_active, policy.max_active)::bigint AS cap,
                 (SELECT count(*)
                  FROM docket_runs AS live
                  WHERE live.scope_key = p_scope
                    AND live.status = 'running'
                    AND live.poisoned_at IS NULL
                    AND live.claim_token IS NOT NULL) AS live_count
          FROM docket_claim_partitions AS partition
          CROSS JOIN docket_claim_policy AS policy
          WHERE partition.scope_key = p_scope AND policy.id = 1
        ), classes AS (
          SELECT
            EXISTS (SELECT 1 FROM docket_runs, authority
              WHERE scope_key = p_scope AND status = 'running' AND poisoned_at IS NULL
                AND claim_token IS NULL AND wake_at <= p_now
                AND claim_attempts < p_max_attempts AND live_count < cap) AS ready,
            EXISTS (SELECT 1 FROM docket_runs
              WHERE scope_key = p_scope AND status = 'running' AND poisoned_at IS NULL
                AND claim_token IS NOT NULL AND claimed_at < p_cutoff
                AND claim_attempts < p_max_attempts) AS expired,
            EXISTS (SELECT 1 FROM docket_runs
              WHERE scope_key = p_scope AND status = 'running' AND poisoned_at IS NULL
                AND claim_token IS NULL AND wake_at <= p_now
                AND claim_attempts >= p_max_attempts) AS ready_poison,
            EXISTS (SELECT 1 FROM docket_runs
              WHERE scope_key = p_scope AND status = 'running' AND poisoned_at IS NULL
                AND claim_token IS NOT NULL AND claimed_at < p_cutoff
                AND claim_attempts >= p_max_attempts) AS expired_poison,
            (SELECT cap FROM authority) AS cap,
            (SELECT live_count FROM authority) AS live_count
        )
        SELECT jsonb_build_object(
          'eligible', ready OR expired OR ready_poison OR expired_poison,
          'ready', ready,
          'expired', expired,
          'ready_poison', ready_poison,
          'expired_poison', expired_poison,
          'cap', cap,
          'live_count', live_count
        )
        FROM classes
      $proof$
      """
    end

    defp ring_snapshot_function do
      """
      CREATE FUNCTION docket_fair_proof_ring_snapshot() RETURNS jsonb
      LANGUAGE sql VOLATILE AS $proof$
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
          'position', ring_position,
          'scope', scope_key,
          'unfinished_count', unfinished_count
        ) ORDER BY ring_position), '[]'::jsonb)
        FROM docket_claim_schedule
        WHERE unfinished_count > 0
      $proof$
      """
    end

    defp epoch_snapshot_function do
      """
      CREATE FUNCTION docket_fair_proof_epoch_snapshot() RETURNS jsonb
      LANGUAGE sql VOLATILE AS $proof$
        SELECT COALESCE(jsonb_object_agg(scope_key, admission_epoch), '{}'::jsonb)
        FROM docket_claim_partitions
      $proof$
      """
    end

    defp policy_snapshot_function do
      """
      CREATE FUNCTION docket_fair_proof_policy_snapshot() RETURNS jsonb
      LANGUAGE sql VOLATILE AS $proof$
        SELECT jsonb_build_object(
          'database_oid', (SELECT oid FROM pg_database WHERE datname = current_database()),
          'schema_oid', 'public'::regnamespace::oid,
          'engine', policy.admission_mode,
          'default_cap', policy.max_active,
          'policy_version', policy.policy_version,
          'function_oid', 'docket_tenant_fair_claim(timestamp with time zone,
            timestamp with time zone, integer, integer, text, integer, boolean)'::regprocedure::oid,
          'function_hash', (SELECT md5(prosrc) FROM pg_proc WHERE oid =
            'docket_tenant_fair_claim(timestamp with time zone,
              timestamp with time zone, integer, integer, text, integer, boolean)'::regprocedure),
          'partition_caps', COALESCE((SELECT jsonb_object_agg(scope_key,
            jsonb_build_object('cap', max_active, 'version', partition_version))
            FROM docket_claim_partitions), '{}'::jsonb)
        )
        FROM docket_claim_policy AS policy
        WHERE policy.id = 1
      $proof$
      """
    end

    defp durable_run_function do
      """
      CREATE FUNCTION docket_fair_proof_durable_run(p_run_id text) RETURNS jsonb
      LANGUAGE sql VOLATILE AS $proof$
        SELECT jsonb_build_object(
          'run_id', run.run_id,
          'scope_key', run.scope_key,
          'status', run.status,
          'claim_token', run.claim_token,
          'claimed_at', run.claimed_at,
          'claim_attempts', run.claim_attempts,
          'poisoned_at', run.poisoned_at,
          'poison_reason', run.poison_reason
        )
        FROM docket_runs AS run
        WHERE run.run_id = p_run_id
      $proof$
      """
    end

    defp audit_function do
      """
      CREATE FUNCTION docket_fair_proof_audit_change() RETURNS trigger
      LANGUAGE plpgsql AS $proof$
      DECLARE
        v_scope text;
        v_run_id text;
      BEGIN
        IF TG_TABLE_NAME = 'docket_claim_policy' THEN
          IF OLD.max_active IS DISTINCT FROM NEW.max_active OR
             OLD.admission_mode IS DISTINCT FROM NEW.admission_mode OR
             OLD.policy_version IS DISTINCT FROM NEW.policy_version THEN
            INSERT INTO public.docket_fair_proof_audit(kind) VALUES ('policy');
          END IF;
        ELSIF TG_TABLE_NAME = 'docket_claim_partitions' THEN
          v_scope := CASE WHEN TG_OP = 'DELETE' THEN OLD.scope_key ELSE NEW.scope_key END;
          IF TG_OP <> 'UPDATE' OR OLD.max_active IS DISTINCT FROM NEW.max_active OR
             OLD.partition_version IS DISTINCT FROM NEW.partition_version THEN
            INSERT INTO public.docket_fair_proof_audit(kind, scope_key) VALUES ('cap', v_scope);
          END IF;
        ELSIF TG_TABLE_NAME = 'docket_claim_schedule' THEN
          v_scope := CASE WHEN TG_OP = 'DELETE' THEN OLD.scope_key ELSE NEW.scope_key END;
          IF TG_OP = 'INSERT' OR TG_OP = 'DELETE' OR
             (OLD.unfinished_count > 0) IS DISTINCT FROM (NEW.unfinished_count > 0) THEN
            INSERT INTO public.docket_fair_proof_audit(kind, scope_key) VALUES ('ring', v_scope);
          END IF;
        ELSIF TG_TABLE_NAME = 'docket_runs' THEN
          v_scope := CASE WHEN TG_OP = 'DELETE' THEN OLD.scope_key ELSE NEW.scope_key END;
          v_run_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.run_id ELSE NEW.run_id END;
          INSERT INTO public.docket_fair_proof_audit(kind, scope_key, run_id)
          VALUES ('run', v_scope, v_run_id);
        END IF;
        IF TG_OP = 'DELETE' THEN
          RETURN OLD;
        END IF;
        RETURN NEW;
      END
      $proof$
      """
    end

    defp ddl_audit_function do
      """
      CREATE FUNCTION docket_fair_proof_audit_ddl() RETURNS event_trigger
      LANGUAGE plpgsql AS $proof$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM pg_event_trigger_ddl_commands()
          WHERE object_identity LIKE '%docket_tenant_fair_claim%'
        ) THEN
          INSERT INTO public.docket_fair_proof_audit(kind) VALUES ('function');
        END IF;
      END
      $proof$
      """
    end

    defp fail!(message), do: raise(ArgumentError, message)
  end
end
