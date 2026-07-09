defmodule Docket.Storage.Runs do
  @moduledoc """
  Persistence contract for durable run state and its operational lifecycle.

  The run store owns the canonical `Docket.Run` document, optimistic commit
  fencing, claim disposition, scheduling, serialized run mutations, and
  poisoned-run recovery. Event rows are owned separately by
  `Docket.Storage.Events`.

  Lifecycle code composes run writes with graph and event writes inside
  `Docket.Storage.transaction/2`. A successful outer transaction is the point
  at which a checkpoint becomes durable and may be exposed to observers.
  """

  @type ctx :: Docket.Storage.ctx()
  @type claim_token :: String.t()

  @typedoc "How a run commit leaves execution ownership and scheduling."
  @type disposition :: :continue | {:park, wake_at :: DateTime.t() | nil}

  @typedoc """
  Optimistic run commit guard.

  `expected_seq` is the `Docket.Run.checkpoint_seq` the committer read. When
  `claim_token` is non-nil, the write additionally fences on the backend's
  current claim for the run.
  """
  @type fence :: %{expected_seq: non_neg_integer(), claim_token: claim_token() | nil}

  @typedoc "Result returned by a pure serialized run mutation."
  @type mutation_result ::
          {:commit, Docket.Checkpoint.t(), disposition()} | {:error, term()}

  @type mutation :: (Docket.Run.t() -> mutation_result())

  @doc """
  Inserts an initialized run and records its first durable wake time.

  Inserting an existing `run.id` is an error. This operation only writes the
  run store. Lifecycle orchestration is responsible for saving the graph and
  appending initialization events in the same `Docket.Storage.transaction/2`.
  `opts` may carry `:tenant_id`.
  """
  @callback insert_run(
              ctx(),
              Docket.Run.t(),
              wake_at :: DateTime.t(),
              opts :: keyword()
            ) :: {:ok, Docket.Run.t()} | {:error, term()}

  @doc """
  Reads the last committed run by id.

  A `:tenant_id` in `opts` scopes the lookup, and a tenant mismatch reads as
  `{:error, :not_found}`. Whether an unscoped public read is permitted is an
  instance-level facade policy; storage supports unscoped internal reads for
  dispatch and recovery.
  """
  @callback fetch_run(ctx(), run_id :: String.t(), opts :: keyword()) ::
              {:ok, Docket.Run.t()} | {:error, :not_found}

  @doc """
  Commits one proposed runtime moment under an optimistic fence.

  The stored sequence must equal `fence.expected_seq`; a non-nil claim token
  must also match the current claim. On a mismatch, nothing changes.

  On success the backend replaces the run with `checkpoint.run`, records the
  latest checkpoint metadata on the run row, and applies `disposition`.
  `:continue` keeps and refreshes the claim. `{:park, wake_at}` releases any
  claim and records the next wake; `nil` means an external wake source or a
  terminal run.

  This callback does not append event rows. Its caller must append the
  checkpoint's retained events through `Docket.Storage.Events` within the
  surrounding `Docket.Storage.transaction/2` before reporting success.
  """
  @callback commit(
              ctx(),
              Docket.Checkpoint.t(),
              fence(),
              disposition(),
              opts :: keyword()
            ) :: {:ok, Docket.Run.t()} | {:error, :stale_fence} | {:error, :not_found}

  @doc """
  Serializes a short read, validation, and run-row mutation.

  The backend loads and exclusively serializes mutation of `run_id`, invokes
  the pure `mutation` function with the current committed run, and either
  returns its error unchanged or persists the proposed checkpoint run and
  disposition. The function must perform no external I/O.

  On success this callback returns the committed checkpoint, not only its run,
  so lifecycle orchestration can append `checkpoint.events` through
  `Docket.Storage.Events` before the surrounding `Docket.Storage.transaction/2`
  commits. The run mutation and event append must complete in that same outer
  transaction before success is exposed.

  Postgres implements serialization with a short `SELECT ... FOR UPDATE`.
  Other stores may use an equivalent per-key primitive. The callback must
  enforce tenant scoping from `opts` before invoking `mutation`.
  """
  @callback mutate_run(ctx(), run_id :: String.t(), mutation(), opts :: keyword()) ::
              {:ok, Docket.Checkpoint.t()} | {:error, term()}

  @doc """
  Reactivates a poisoned run's backend-owned operational state.

  This is not a graph signal. It atomically resets the backend attempt counter
  and operational error/status, clears any claim, and schedules an immediate
  wake. Calling it for an already-active run returns the stored run.
  Operational telemetry records the command; it does not consume the graph
  run's checkpoint or event sequence.
  """
  @callback retry_poisoned_run(ctx(), run_id :: String.t(), opts :: keyword()) ::
              {:ok, Docket.Run.t()}
              | {:error, :not_found | :blocked | :inactive_run}
end
