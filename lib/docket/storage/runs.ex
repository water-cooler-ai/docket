defmodule Docket.Storage.Runs do
  @moduledoc """
  Persistence contract for the durable run aggregate.

  This capability owns every operation that enforces the run row's shared
  schedule, claim, fence, and poison invariants: insertion and reads, atomic
  due claims, claim refresh and release, advance commits, serialized signal
  mutations, and poison recovery. Those concerns cannot be split into
  independently configured stores because they mutate and fence the same
  aggregate.

  Lifecycle code composes run and event writes inside
  `Docket.Storage.transaction/2`; graph versions are saved separately before
  a run can reference them. This contract deliberately accepts neutral
  run proposals and schedule effects, never `Docket.Checkpoint` or
  `Docket.Runtime.Moment` values. A successful outer transaction is the point
  at which the transition becomes durable.
  """

  @type ctx :: Docket.Storage.ctx()
  @type scope :: Docket.Storage.scope()
  @type owner_scope :: Docket.Storage.owner_scope()
  @type claim_token :: nonempty_binary()
  @type checkpoint_type :: atom()

  @typedoc """
  Storage effect applied with a committed run transition.

  `:retain_claim` keeps the current token, refreshes its claimed time, and
  leaves the run without a wake. A release clears the token and claimed time.
  `:immediate` records a wake at the backend's current time, `{:at, time}`
  records a future or current wake, and `:external` or `:terminal` records no
  wake. The two nil-wake reasons remain distinct here so implementations can
  validate the proposed run status.
  """
  @type schedule ::
          :retain_claim
          | {:release_claim, :immediate | :external | :terminal | {:at, DateTime.t()}}

  @typedoc "Policy for one atomic due-claim scan."
  @type claim_policy :: %{
          required(:now) => DateTime.t(),
          required(:limit) => pos_integer(),
          required(:orphan_ttl_ms) => non_neg_integer(),
          required(:max_claim_attempts) => pos_integer()
        }

  @typedoc "Lightweight authority returned for a run that should be launched."
  @type claim_lease :: %{
          required(:run_id) => String.t(),
          required(:graph_id) => String.t(),
          required(:graph_hash) => String.t(),
          required(:checkpoint_seq) => non_neg_integer(),
          required(:claim_token) => claim_token(),
          required(:claimed_at) => DateTime.t(),
          required(:claim_attempt) => pos_integer()
        }

  @typedoc "Operational result for an exhausted candidate that was not launched."
  @type poisoned_claim :: %{
          required(:run_id) => String.t(),
          required(:poisoned_at) => DateTime.t(),
          required(:poison_reason) => map()
        }

  @typedoc "Results of one claim scan. Poisoned candidates never appear as leases."
  @type claim_batch :: %{
          required(:leases) => [claim_lease()],
          required(:poisoned) => [poisoned_claim()]
        }

  @typedoc "Neutral proposal for one claim-fenced advance commit."
  @type commit_proposal :: %{
          required(:run) => Docket.Run.t(),
          required(:expected_checkpoint_seq) => non_neg_integer(),
          required(:claim_token) => claim_token(),
          required(:checkpoint_type) => checkpoint_type(),
          required(:schedule) => schedule()
        }

  @typedoc "Decision returned by a pure serialized run mutation."
  @type mutation_decision ::
          {:commit, Docket.Run.t(), checkpoint_type(), schedule(), opaque :: term()}
          | {:no_change, opaque :: term()}
          | {:error, reason :: term()}

  @type mutation :: (Docket.Run.t() -> mutation_decision())

  @type mutation_result ::
          {:ok, {:committed, term()} | {:unchanged, term()}} | {:error, term()}

  @doc """
  Inserts one initialized, already-durable run.

  `owner_scope` determines the stored tenant: `:tenantless` stores `nil` and
  `{:tenant, tenant_id}` stores that identifier. The initialized run must not
  have the transient `:created` status, must already carry its first committed
  sequence and start time, and requires `checkpoint_type == :run_initialized`.
  That type becomes the latest checkpoint metadata, and `wake_at` is the
  run's first explicit schedule.

  This callback writes only the run aggregate. `Docket.Lifecycle` appends
  assigned initialization events in the same outer transaction; it never
  publishes the already-saved graph version.
  """
  @callback insert_run(
              ctx(),
              owner_scope(),
              Docket.Run.t(),
              checkpoint_type(),
              wake_at :: DateTime.t()
            ) :: {:ok, Docket.Run.t()} | {:error, term()}

  @doc """
  Reads the last committed graph-run document under an explicit scope.

  An unknown run or scope mismatch returns `{:error, :not_found}`.
  """
  @callback fetch_run(ctx(), scope(), run_id :: String.t()) ::
              {:ok, Docket.Run.t()} | {:error, :not_found}

  @doc """
  Reads the committed run with substrate-neutral operational information.

  The `Docket.RunInfo` projection exposes the wake, claimed time,
  claim-attempt count, and poison facts, but never the current claim token.
  An unknown run or scope mismatch returns `{:error, :not_found}`.
  """
  @callback inspect_run(ctx(), scope(), run_id :: String.t()) ::
              {:ok, Docket.RunInfo.t()} | {:error, :not_found}

  @doc """
  Atomically claims a batch of ready or expired runs for system execution.

  Ready candidates are non-poisoned running runs with no claim and a wake at
  or before `policy.now`. Expired candidates are non-poisoned running runs
  whose current claim is older than `policy.orphan_ttl_ms` relative to that
  same timestamp.

  For each selected candidate whose current claim-attempt count is below
  `max_claim_attempts`, the store assigns a fresh token, sets `claimed_at` to
  `policy.now`, clears `wake_at`, increments the count, and returns a lease. If
  the count has already reached the maximum, the store clears claim and wake,
  records paired poison facts at `policy.now`, and returns a poison result
  without a lease. A maximum of three therefore launches exactly three
  vehicles; a fourth recovery need poisons the run.

  Selection, attempt accounting, claim steal, and poison mutation occur in
  the same atomic operation. The total number of returned outcomes is at most
  `policy.limit`.
  """
  @callback claim_due(ctx(), :system, claim_policy()) ::
              {:ok, claim_batch()} | {:error, term()}

  @doc """
  Refreshes claim liveness under the exact current token.

  Success sets `claimed_at` to `now`. A stale token, missing run, or lost claim
  returns `{:error, :claim_lost}` and changes nothing.
  """
  @callback refresh_claim(
              ctx(),
              :system,
              run_id :: String.t(),
              claim_token(),
              now :: DateTime.t()
            ) :: :ok | {:error, :claim_lost}

  @doc """
  Releases the claim under the exact current token.

  Release is idempotent. A stale token or missing run returns `:ok` without
  disturbing a newer claim or the run's schedule. A matching release clears
  the claim token and claimed time and records an immediate wake at `now`, so
  it cannot strand a non-poisoned running row without a claim or schedule.
  """
  @callback release_claim(
              ctx(),
              :system,
              run_id :: String.t(),
              claim_token(),
              now :: DateTime.t()
            ) :: :ok

  @doc """
  Commits one neutral runtime proposal under a mandatory token-and-sequence fence.

  The stored checkpoint sequence must equal
  `proposal.expected_checkpoint_seq`, the current claim must equal the
  non-empty `proposal.claim_token`, and the proposed run's sequence must be
  exactly `expected_checkpoint_seq + 1`. A stored sequence or token mismatch
  returns `{:error, :stale_fence}` without changing anything. A nil token,
  wrong proposed sequence, run identity mismatch, or invalid schedule/status
  combination returns `{:error, :invalid_commit}`.

  Success replaces the run, records `checkpoint_type`, resets consecutive
  claim attempts and poison facts, and applies the schedule atomically.
  `:retain_claim` refreshes the current claim for a continuing vehicle; every
  release schedule clears it. This callback never appends events. Lifecycle
  code appends the proposal's already-assigned events through
  `Docket.Storage.Events` in the same outer transaction.

  An unknown run or scope mismatch returns `{:error, :not_found}`. Proposal
  shape and exact-next-sequence validation happen before any run lookup, so
  `:invalid_commit` precedes `:not_found`: a malformed proposal returns
  `:invalid_commit` even when its run id would not be visible under the
  supplied scope, and only valid proposals return `:not_found` for an unknown
  or out-of-scope run.
  """
  @callback commit(ctx(), scope(), commit_proposal()) ::
              {:ok, Docket.Run.t()}
              | {:error, :stale_fence | :invalid_commit | :not_found}

  @doc """
  Serializes one short read, pure decision, and optional unclaimed run update.

  The store checks scope before invoking `mutation`, loads and exclusively
  serializes the current committed run, and invokes the function while that
  serialization is held. The function must perform no external I/O.

  A commit decision must propose the same run id and exactly the current
  checkpoint sequence plus one. Serialized mutation is the only unclaimed
  graph-run write path: it revokes any current claim and applies a release
  schedule atomically. `:retain_claim` is therefore invalid for this callback.
  On success the store resets claim attempts and poison facts and returns the
  opaque value tagged `:committed`; lifecycle code can then append events in
  the same outer transaction.

  A no-change decision returns the opaque value tagged `:unchanged` and must
  not touch the row, claim, schedule, counters, timestamps, or event sequence.
  An error decision is returned unchanged and also changes nothing. An unknown
  run or scope mismatch returns `{:error, :not_found}` without invoking the
  function; malformed proposals return `{:error, :invalid_mutation}`.
  """
  @callback mutate_run(ctx(), scope(), run_id :: String.t(), mutation()) :: mutation_result()

  @doc """
  Recovers a non-terminal poisoned run's backend-owned operational state.

  Terminal status is checked first and returns `{:error, :inactive_run}`.
  Otherwise a poisoned run has its poison facts and claim cleared, its claim
  attempts reset, and an immediate wake recorded at `now`. Calling this for an
  already-unpoisoned non-terminal run is idempotent success and changes
  nothing. The command consumes neither checkpoint nor event sequence.

  An unknown run or scope mismatch returns `{:error, :not_found}`.
  """
  @callback retry_poisoned_run(ctx(), scope(), run_id :: String.t(), now :: DateTime.t()) ::
              {:ok, Docket.Run.t()} | {:error, :not_found | :inactive_run}
end
