defmodule Docket.Coordinator do
  @moduledoc """
  Single-writer coordination seam for claim lifecycle.

  One active run has one current claim holder. An expired holder may still be
  alive briefly after another worker steals its claim, so claims limit current
  commit authority rather than guaranteeing that only one process exists.
  `Docket.Storage.Runs.commit/5` combines the claim token with the checkpoint
  sequence fence so at most one durable commit wins.

  ## Invariant vocabulary

  - **claim** — momentary commit authority for one run; at most one current
    holder; won at dispatch, refreshed by commits or heartbeats, released at
    park or fence loss. A crash can leave one dangling, and expiry makes it
    stealable.
  - **fence** — the optimistic commit guard: a commit succeeds only if
    `checkpoint_seq` still matches and, for advance commits, the claim token
    still matches.
  - **park** — the storage commit ending a drain: final checkpoint, claim
    release, and next wake recorded atomically.
  - **wake** — the moment a parked run becomes due to advance again.

  All callbacks take an opaque backend context as the first argument.
  """

  @type ctx :: term()
  @type claim_token :: String.t()

  @doc """
  Takes the execution claim for a run.

  Wins if the run has no claim or the existing claim is expired per backend
  policy. Winning over an expired claim is safe for durable state because the
  stale token can no longer pass `Docket.Storage.Runs.commit/5`.
  """
  @callback claim_run(ctx(), run_id :: String.t(), claim_token(), opts :: keyword()) ::
              {:ok, Docket.Run.t()} | {:error, :claim_held} | {:error, :not_found}

  @doc """
  Refreshes claim liveness under the caller's token.
  """
  @callback refresh_claim(ctx(), run_id :: String.t(), claim_token(), opts :: keyword()) ::
              :ok | {:error, :claim_lost}

  @doc """
  Releases the claim under the caller's token.

  Guarded by the token and idempotent: a stale release is `:ok` and must not
  disturb a newer holder or its schedule.
  """
  @callback release_claim(ctx(), run_id :: String.t(), claim_token(), opts :: keyword()) :: :ok
end
