if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.Migrations.V03 do
    @moduledoc false

    use Ecto.Migration

    alias Docket.Postgres.Storage

    def up(%{prefix: prefix}) do
      runs = Storage.qualified_table(prefix, "docket_runs")
      events = Storage.qualified_table(prefix, "docket_events")
      receipts = Storage.qualified_table(prefix, "docket_transition_receipts")

      execute("LOCK TABLE #{runs} IN SHARE ROW EXCLUSIVE MODE")
      execute("ALTER TABLE #{runs} ADD COLUMN event_seq bigint")
      flush()

      backfill_event_sequences(runs, events)

      execute("ALTER TABLE #{runs} ALTER COLUMN event_seq SET DEFAULT 0")
      execute("ALTER TABLE #{runs} ALTER COLUMN event_seq SET NOT NULL")

      execute("""
      ALTER TABLE #{runs}
      ADD CONSTRAINT docket_runs_event_seq_check CHECK (event_seq >= 0)
      """)

      execute("""
      CREATE TABLE #{receipts} (
        transition_id text PRIMARY KEY,
        scope_key text NOT NULL,
        operation text NOT NULL,
        digest bytea NOT NULL,
        result bytea NOT NULL,
        attempt_id uuid NOT NULL,
        inserted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT docket_transition_receipts_id_check CHECK (transition_id <> ''),
        CONSTRAINT docket_transition_receipts_operation_check
          CHECK (operation IN ('initialize', 'claimed', 'unclaimed'))
      )
      """)
    end

    def down(%{prefix: prefix}) do
      runs = Storage.qualified_table(prefix, "docket_runs")
      receipts = Storage.qualified_table(prefix, "docket_transition_receipts")

      execute("DROP TABLE IF EXISTS #{receipts}")
      execute("ALTER TABLE #{runs} DROP CONSTRAINT IF EXISTS docket_runs_event_seq_check")
      execute("ALTER TABLE #{runs} DROP COLUMN IF EXISTS event_seq")
    end

    defp backfill_event_sequences(runs, events) do
      for [id, state] <- repo().query!("SELECT id, state FROM #{runs}", [], log: false).rows do
        event_seq =
          case Docket.DurableCodec.decode(state, :run) do
            {:ok, %{event_seq: value}} when is_integer(value) and value >= 0 ->
              value

            _legacy_or_corrupt_state ->
              [[value]] =
                repo().query!(
                  "SELECT COALESCE(max(seq), 0)::bigint FROM #{events} " <>
                    "WHERE run_id = (SELECT run_id FROM #{runs} WHERE id = $1)",
                  [id],
                  log: false
                ).rows

              value
          end

        repo().query!("UPDATE #{runs} SET event_seq = $1 WHERE id = $2", [event_seq, id],
          log: false
        )
      end
    end
  end
end
