defmodule Docket.Storage.Runs do
  @moduledoc """
  Persistence contract for the durable run aggregate.

  This capability owns every operation that enforces the run row's shared
  schedule, claim, fence, and poison invariants: insertion and reads, atomic
  due claims, claim refresh and release, pre-execution claim abandons, advance
  commits, serialized signal mutations, and poison recovery. Those concerns
  cannot be split into independently configured stores because they mutate
  and fence the same aggregate.

  Lifecycle code composes run and event writes inside one backend transaction;
  graph versions are saved separately before
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

  @typedoc "Stable newest-first run-list cursor: `{started_at, run_id}`."
  @type list_cursor :: Docket.RunPage.cursor()

  @typedoc """
  Trusted, normalized filters for one run collection read.

  Public callers are normalized into this shape before reaching storage.
  `before` is exclusive, and `statuses` is either `nil` or a non-empty list
  containing only durable run statuses.
  """
  @type list_query :: %{
          required(:limit) => pos_integer(),
          required(:before) => list_cursor() | nil,
          required(:graph_id) => String.t() | nil,
          required(:graph_hash) => String.t() | nil,
          required(:statuses) => [Docket.Run.durable_status()] | nil
        }

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

  @typedoc """
  Policy for one atomic due-claim scan.

  `:preference` is an optional, advisory demand-1 hint naming the candidate
  class (`:ready` or `:expired`) served first when `limit` is exactly one.
  It is operational and local - dispatchers alternate it across consecutive
  polls and lose the phase on restart - never durable run state. Absent (or
  at any other demand) selection is class-neutral.
  """
  @type claim_policy :: %{
          required(:now) => DateTime.t(),
          required(:limit) => pos_integer(),
          required(:orphan_ttl_ms) => non_neg_integer(),
          required(:max_claim_attempts) => pos_integer(),
          optional(:preference) => :ready | :expired | nil
        }

  @typedoc """
  Lightweight authority returned for a run that should be launched.

  `orphan_ttl_ms` echoes the claiming policy's TTL so the holder knows the
  staleness bound its own claim expires under. It is informational per-claim
  state, not a promise that the TTL cannot differ on a later claim.
  """
  @type claim_lease :: %{
          required(:run_id) => String.t(),
          required(:graph_id) => String.t(),
          required(:graph_hash) => String.t(),
          required(:checkpoint_seq) => non_neg_integer(),
          required(:claim_token) => claim_token(),
          required(:claimed_at) => DateTime.t(),
          required(:claim_attempt) => pos_integer(),
          required(:orphan_ttl_ms) => non_neg_integer()
        }

  @typedoc "Operational result for an exhausted candidate that was not launched."
  @type poisoned_claim :: %{
          required(:run_id) => String.t(),
          required(:poisoned_at) => DateTime.t(),
          required(:poison_reason) => String.t()
        }

  @typedoc "Results of one claim scan. Poisoned candidates never appear as leases."
  @type claim_batch :: %{
          required(:leases) => [claim_lease()],
          required(:poisoned) => [poisoned_claim()]
        }

  @typedoc """
  Policy for one pre-execution claim abandon.

  `expected_checkpoint_seq` is the lease's committed sequence, `now` is the
  caller's clock reading, `retry_at` is the future wake the abandoned run
  receives, and `max_claim_abandons` bounds consecutive abandons before the
  run is poisoned instead of rescheduled.

  A `:non_poisoning` abandon reverses the claim-attempt increment, counts
  one claim abandon, and never poisons regardless of the count. When
  `:backoff` is present the store ignores `retry_at` and computes the wake
  from its durable abandon count as
  `now + min(base_ms * 2^claim_abandons, cap_ms)`.
  """
  @type abandon_policy :: %{
          required(:expected_checkpoint_seq) => non_neg_integer(),
          required(:now) => DateTime.t(),
          required(:retry_at) => DateTime.t(),
          required(:max_claim_abandons) => pos_integer(),
          optional(:non_poisoning) => boolean(),
          optional(:backoff) => %{base_ms: pos_integer(), cap_ms: pos_integer()}
        }

  @typedoc "Disposition applied by one pre-execution claim abandon."
  @type abandon_result :: {:ok, :rescheduled | :poisoned | :stale}

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

  This callback writes only the run aggregate. Lifecycle orchestration appends
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
  Backend/infrastructure failure and corrupt persisted run state are not
  collapsed into absence; implementations raise in those cases.
  """
  @callback fetch_run(ctx(), scope(), run_id :: String.t()) ::
              {:ok, Docket.Run.t()} | {:error, :not_found}

  @doc """
  Lists lightweight run summaries under an explicit scope.

  Results are ordered newest first by immutable `(started_at, run_id)` and
  use an exclusive keyset cursor. Scope and every requested filter must be
  enforced by the backing query; implementations must not load out-of-scope
  rows and filter them in application memory. The full durable run state is
  deliberately absent from `Docket.RunSummary` and must not be decoded for a
  collection read.

  No matching rows is successful and returns an empty `Docket.RunPage` whose
  `next_before` preserves the supplied cursor and whose `has_more?` is false.
  """
  @callback list_runs(ctx(), scope(), list_query()) :: {:ok, Docket.RunPage.t()}

  @doc """
  Reads the committed run with substrate-neutral operational information.

  The `Docket.RunInfo` projection exposes the wake, claimed time,
  claim-attempt count, and poison facts, but never the current claim token.
  An unknown run or scope mismatch returns `{:error, :not_found}`.
  Backend/infrastructure failure and corrupt persisted run state raise rather
  than being reported as `:not_found`.
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
  without a lease. A maximum of three therefore permits exactly three claims
  that proceed into node execution; a fourth recovery need poisons the run.
  Claims handed back through `abandon_claim/5` before execution began do not
  count toward that maximum — they are bounded separately by the abandon
  policy.

  Selection, attempt accounting, claim steal, and poison mutation occur in
  the same atomic operation. The total number of returned outcomes is at most
  `policy.limit`.

  Selection keeps both continuously eligible classes making progress:

    * With `limit >= 2` and both classes non-empty, at least one outcome
      (lease or poison - the reservation is per outcome, not per lease) goes
      to each class; the remaining demand takes the oldest eligible rows
      under the stable `(eligible_at, id)` tie-break.
    * With `limit == 1`, the optional `policy.preference` class is served
      first, falling through to the other class when the preferred one is
      empty so preference never wastes demand. Without a preference the
      oldest eligible row wins regardless of class.

  Eligibility-time ordering provides aging, not fairness. Implementations
  promise no strict FIFO, no bounded queue wait, no tenant or workload-class
  fairness, and no starvation freedom under sustained arrivals or persistent
  row locks; concurrent claimants may skip locked rows, so per-class
  progress holds only up to rows another transaction currently holds.
  """
  @callback claim_due(ctx(), :system, claim_policy()) ::
              {:ok, claim_batch()} | {:error, term()}

  @doc """
  Refreshes claim liveness under the exact current token.

  Success advances `claimed_at`, and never backward: the stored time only
  moves toward the backend's authoritative current time, so a refresh that
  was delayed in flight cannot regress a fresher value written by a
  `:retain_claim` commit and re-expose the run to steal. `now` is the
  caller's clock reading; backends with an authoritative clock of their own
  may prefer that clock over `now`. A stale token, missing run, or lost
  claim returns `{:error, :claim_lost}` and changes nothing.
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
  Hands back a claim whose vehicle could not begin node execution.

  This is the disposition for deterministic pre-execution failure — above all
  graph compilation incompatibility, where the executing node cannot compile
  the stored effective graph against its locally installed node contracts.
  That is a deployment-compatibility condition, not node execution failure,
  so it must consume neither the run's execution-attempt budget nor be
  reported through the run's failure machinery. Transient infrastructure
  errors (for example a failed graph fetch) are not abandons; they use
  ordinary `release_claim/5` or crash into claim expiry.

  The operation is fenced on the exact current token **and** the lease's
  committed checkpoint sequence, so it applies only before the holder's first
  lifecycle commit. A matched abandon atomically:

    * clears the claim token and claimed time;
    * hands the acquisition increment back by decrementing the claim-attempt
      count (floored at zero), keeping poison-by-attempts exclusively about
      claims that reached execution;
    * increments the consecutive claim-abandon count; and
    * either records `policy.retry_at` as the next wake and returns
      `{:ok, :rescheduled}`, or — when the abandon count had already reached
      `policy.max_claim_abandons` — records paired poison facts at
      `policy.now` with reason `"max_claim_abandons_exceeded"` and returns
      `{:ok, :poisoned}` so persistent incompatibility becomes an explicit
      operator concern instead of unbounded retry.

  It never touches the committed run document, `checkpoint_seq`, or the
  run's `updated_at`. A stale token, an advanced sequence, or an unknown run
  changes nothing and returns `{:ok, :stale}`; after a steal or a serialized
  signal commit the abandon can never disturb the winning claim or schedule.
  Every return is success-shaped because the caller's next action is
  identical in all cases — stop without committing; the atom is for
  telemetry. The abandon count resets on any committed run mutation and on
  poison recovery.

  `policy.retry_at` must not precede `policy.now`: the future wake is what
  keeps an incompatible node from immediately re-claiming the same run.
  Callers should add jitter to their retry backoff so runs abandoned together
  do not become due together. A rolling deployment therefore self-heals — a
  compatible node claims the run at or after `retry_at` — while a fleet that
  can never compile the graph poisons it after a bounded number of abandons.
  Both timestamps are normalized like every other operational timestamp.
  """
  @callback abandon_claim(
              ctx(),
              :system,
              run_id :: String.t(),
              claim_token(),
              abandon_policy()
            ) :: abandon_result()

  @doc """
  Commits one neutral runtime proposal under a mandatory token-and-sequence fence.

  The stored checkpoint sequence must equal
  `proposal.expected_checkpoint_seq`, the current claim must equal the
  non-empty `proposal.claim_token`, and the proposed run's sequence must be
  exactly `expected_checkpoint_seq + 1`. The checkpoint type must be a
  supported Docket checkpoint type. A stored sequence or token mismatch
  returns `{:error, :stale_fence}` without changing anything. A nil token,
  wrong proposed sequence, run identity mismatch, or invalid schedule/status
  combination returns `{:error, :invalid_commit}`.

  Success replaces the run, records `checkpoint_type`, resets consecutive
  claim attempt and abandon counts and poison facts, and applies the schedule
  atomically.
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
  On success the store resets claim attempt and abandon counts and poison
  facts and returns the opaque value tagged `:committed`; lifecycle code can
  then append events in the same outer transaction.

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
  attempt and abandon counts reset, and an immediate wake recorded at `now`.
  Calling this for an
  already-unpoisoned non-terminal run is idempotent success and changes
  nothing. The command consumes neither commit nor event sequence.

  An unknown run or scope mismatch returns `{:error, :not_found}`.
  """
  @callback retry_poisoned_run(ctx(), scope(), run_id :: String.t(), now :: DateTime.t()) ::
              {:ok, Docket.Run.t()} | {:error, :not_found | :inactive_run}
end
