defmodule Docket.Bench.Scorecard.Invariants do
  @moduledoc """
  Locked-contract SQL invariant checks applied after every runtime trial.

  `no_active_claims`, `no_poisoned`, `no_stranded`, `dup_claim_tokens`, and
  `event_seq_unique` are schema-regression guards after a successful drain: once
  every run has reached a terminal status the database CHECK constraints make
  those violating states impossible, so a failure here signals a schema
  regression rather than a live fault. Live violations while runs are still in
  flight surface through the drain-timeout diagnostics instead.
  """

  alias Docket.Bench.Scorecard.Db

  @terminal_statuses_sql Docket.Run.terminal_statuses()
                         |> Enum.map_join(", ", &"'#{&1}'")

  def check(ctx, expected) do
    runs = Db.table(ctx.prefix, "docket_runs")
    events = Db.table(ctx.prefix, "docket_events")

    [
      check_zero(
        "dup_claim_tokens",
        "SELECT count(*) FROM (SELECT claim_token FROM #{runs} WHERE claim_token IS NOT NULL GROUP BY claim_token HAVING count(*) > 1) AS duplicates"
      ),
      check_zero(
        "no_active_claims",
        "SELECT count(*) FROM #{runs} WHERE claim_token IS NOT NULL"
      ),
      check_eq(
        "all_done",
        expected,
        "SELECT count(*) FROM #{runs} WHERE status = 'done'"
      ),
      check_zero(
        "no_stranded",
        "SELECT count(*) FROM #{runs} WHERE status NOT IN (#{@terminal_statuses_sql})"
      ),
      check_zero(
        "no_poisoned",
        "SELECT count(*) FROM #{runs} WHERE poisoned_at IS NOT NULL"
      ),
      check_zero(
        "event_seq_unique",
        "SELECT count(*) FROM (SELECT run_id, seq FROM #{events} GROUP BY run_id, seq HAVING count(*) > 1) AS duplicates"
      ),
      check_eq(
        "one_terminal_event",
        expected,
        "SELECT count(*) FROM #{events} WHERE type = 'run_completed'"
      )
    ]
  end

  defp check_zero(name, sql) do
    [[actual]] = Db.repo().query!(sql).rows
    %{name: name, pass: actual == 0, expected: 0, actual: actual}
  end

  defp check_eq(name, expected, sql) do
    [[actual]] = Db.repo().query!(sql).rows
    %{name: name, pass: actual == expected, expected: expected, actual: actual}
  end
end
