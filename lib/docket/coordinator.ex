defmodule Docket.Coordinator do
  @moduledoc """
  Single-writer coordination seam: claim lifecycle plus the fenced commit.

  One active run has one live mutator. The claim guarantees at most one worker
  drives a run at a time; the fence guarantees at most one commit wins.

  ## Invariant vocabulary

  These words are shared by every backend that implements this seam.

  - **claim** — momentary execution ownership of one run; at most one live
    holder; won at dispatch, refreshed by commits or heartbeats, released at
    park or fence loss. Only a crash leaves one dangling, and expiry makes it
    stealable.
  - **fence** — the optimistic commit guard: a commit succeeds only if
    `checkpoint_seq` still matches what the committer read, and, for advance
    commits, only if the claim token still matches. Checked at commit time
    only; nothing is held during execution.
  - **park** — the commit that ends a drain: final checkpoint, claim release,
    and next wake recorded atomically. A parked run lives entirely in storage
    and needs no process.
  - **wake** — the moment a parked run becomes due to advance again; a run is
    always terminal, parked with an explicit wake source, or claimed by a live
    worker.

  All callbacks take an opaque backend context as the first argument; core
  never interprets it.
  """

  @type ctx :: term()
  @type claim_token :: String.t()

  @typedoc """
  What a commit does to the claim and schedule, applied atomically with the
  state change.

  - `:continue` keeps the claim, refreshing its liveness (a mid-drain commit).
  - `{:park, wake_at}` releases the claim and records the next wake — a
    `DateTime` for a scheduled wake, `nil` for an external wake source or a
    terminal run.
  """
  @type disposition :: :continue | {:park, wake_at :: DateTime.t() | nil}

  @doc """
  Takes the execution claim for a run.

  Wins if the run has no claim or the existing claim is expired per backend
  policy — a liveness TTL owned by the backend, not this contract. Winning
  over an expired claim is a steal, safe by construction: a stale holder can
  no longer commit, because commits fence on the token. A live claim returns
  `{:error, :claim_held}`; an unknown run returns `{:error, :not_found}`.
  """
  @callback claim_run(ctx(), run_id :: String.t(), claim_token(), opts :: keyword()) ::
              {:ok, Docket.Run.t()} | {:error, :claim_held} | {:error, :not_found}

  @doc """
  Refreshes claim liveness under the caller's token.

  The token-guarded heartbeat for long single supersteps: it succeeds only
  while the caller still holds the claim. `{:error, :claim_lost}` tells a
  worker to stop driving.
  """
  @callback refresh_claim(ctx(), run_id :: String.t(), claim_token(), opts :: keyword()) ::
              :ok | {:error, :claim_lost}

  @doc """
  Releases the claim under the caller's token.

  Guarded by the token and idempotent: releasing a claim you no longer hold —
  already stolen, already released — is `:ok` and must not disturb the current
  holder or any schedule a winning commit set.
  """
  @callback release_claim(ctx(), run_id :: String.t(), claim_token(), opts :: keyword()) :: :ok

  @doc """
  Commits one runtime moment under the fence.

  The checkpoint carries the post-commit run, whose `checkpoint_seq` equals
  `checkpoint.seq`. The commit succeeds only if the stored run's
  `checkpoint_seq` equals `checkpoint.seq - 1`.

  Advance commits — a worker driving supersteps — pass their `claim_token`;
  the commit additionally fences on it, so a stale worker whose claim was
  stolen cannot commit. Signal commits — cancel, interrupt resolution — pass
  `nil`; they fence on seq alone, so they can win against an in-flight
  advance. The advance's next commit then fails the fence, discards its work,
  releases its claim, and stops.

  `disposition` says what the commit does to claim and schedule atomically
  with the state change. On success the backend persists the checkpoint's run,
  persists its events per the storage policy, and applies the disposition as
  one atomic operation. On `{:error, :stale_fence}` nothing is persisted; the
  caller discards uncommitted work, releases its claim, and stops. An unknown
  run returns `{:error, :not_found}`.
  """
  @callback commit(
              ctx(),
              Docket.Checkpoint.t(),
              claim_token() | nil,
              disposition(),
              opts :: keyword()
            ) :: {:ok, Docket.Run.t()} | {:error, :stale_fence} | {:error, :not_found}
end
