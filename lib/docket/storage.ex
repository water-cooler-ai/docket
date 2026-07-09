defmodule Docket.Storage do
  @moduledoc """
  Atomic persistence seam implemented by a durable backend.

  The run store holds the canonical `Docket.Run` document as the single full
  document for a run. Reads through this behaviour are storage-backed: they
  return the last committed run and may lag live execution by one uncommitted
  superstep.

  The graph store holds canonical graph documents keyed by
  `{graph_id, graph_hash}` so a worker can recover without a host call.

  Run initialization and every later checkpoint commit are atomic backend
  operations. A successful mutation persists the run, checkpoint metadata,
  retained events, claim disposition, and schedule together. There are
  deliberately no public `update_run` or `persist_events` callbacks that a
  driver could compose non-atomically. The stored run is the only full
  document; checkpoint history is metadata-only.

  `Docket.Checkpoint` handlers remain the host-facing integration seam. Under
  a Docket-owned durable driver they are post-commit notifications, not the
  durable committer.

  All callbacks take an opaque backend context as the first argument. Core
  never interprets it.
  """

  @type ctx :: term()
  @type claim_token :: String.t()

  @typedoc "How an atomic commit leaves execution ownership and scheduling."
  @type disposition :: :continue | {:park, wake_at :: DateTime.t() | nil}

  @typedoc """
  Optimistic commit guard.

  `expected_seq` is the `Docket.Run.checkpoint_seq` the committer read. When
  `claim_token` is non-nil the write additionally fences on the backend's
  current claim for the run.
  """
  @type fence :: %{expected_seq: non_neg_integer(), claim_token: claim_token() | nil}

  @typedoc "Result returned by a pure serialized run mutation."
  @type mutation_result ::
          {:commit, Docket.Checkpoint.t(), disposition()} | {:error, term()}

  @type mutation :: (Docket.Run.t() -> mutation_result())

  @doc """
  Atomically publishes a graph version and persists an initialized run.

  `checkpoint` must be the run's `:run_initialized` checkpoint. The operation
  upserts `graph_document`, inserts the checkpoint's run, persists checkpoint
  metadata and retained events, and records `wake_at` as one durable mutation.
  A backend must never make the run visible without its graph version or make
  it dispatchable without its initialized checkpoint.

  Inserting a `run_id` that already exists is an error. Reusing a
  `{graph_id, graph_hash}` with different content is also an error rather than
  silently accepting a hash/document mismatch. `opts` may carry `:tenant_id`.
  """
  @callback initialize_run(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              graph_document :: map(),
              Docket.Checkpoint.t(),
              wake_at :: DateTime.t(),
              opts :: keyword()
            ) :: {:ok, Docket.Run.t()} | {:error, term()}

  @doc """
  Reads the last committed run by id.

  A `:tenant_id` in `opts` scopes the lookup, and a tenant mismatch reads as
  `{:error, :not_found}`. Whether an unscoped public read is permitted is an
  instance-level facade policy; storage itself supports unscoped internal
  reads for dispatch and recovery.
  """
  @callback fetch_run(ctx(), run_id :: String.t(), opts :: keyword()) ::
              {:ok, Docket.Run.t()} | {:error, :not_found}

  @doc """
  Atomically commits one proposed runtime moment under an optimistic fence.

  The stored sequence must equal `fence.expected_seq`; a non-nil claim token
  must also match the current claim. On a mismatch, nothing changes.

  On success the backend replaces the run with `checkpoint.run`, stores
  checkpoint metadata, persists retained events, and applies `disposition` in
  one operation. `:continue` keeps and refreshes the claim. `{:park, wake_at}`
  releases any claim and records the next wake; `nil` means an external wake
  source or a terminal run.
  """
  @callback commit(
              ctx(),
              Docket.Checkpoint.t(),
              fence(),
              disposition(),
              opts :: keyword()
            ) :: {:ok, Docket.Run.t()} | {:error, :stale_fence} | {:error, :not_found}

  @doc """
  Serializes a short read/validate/commit mutation for a public signal.

  The backend loads and exclusively serializes mutation of `run_id`, invokes
  the pure `mutation` function with the current committed run, and either
  returns its error unchanged or atomically persists the returned checkpoint
  and disposition. The function must perform no external I/O.

  Postgres implements this with a short `SELECT ... FOR UPDATE` transaction.
  Other stores may use an equivalent per-key serialization primitive. The
  callback must enforce tenant scoping from `opts` before invoking `mutation`.
  """
  @callback mutate_run(ctx(), run_id :: String.t(), mutation(), opts :: keyword()) ::
              {:ok, Docket.Run.t()} | {:error, term()}

  @doc """
  Reactivates a poisoned run's backend-owned operational state.

  This is deliberately not a `Docket.Run` signal: it atomically resets the
  backend attempt counter and operational error/status, clears any claim,
  and schedules an immediate wake. Calling it for an already-active run returns
  the stored run. Operational telemetry records the command; it does not
  consume the graph run's checkpoint or event sequence.
  """
  @callback retry_poisoned_run(ctx(), run_id :: String.t(), opts :: keyword()) ::
              {:ok, Docket.Run.t()}
              | {:error, :not_found | :blocked | :inactive_run}

  @doc """
  Reads a canonical graph document by `{graph_id, graph_hash}`.
  """
  @callback fetch_graph(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              opts :: keyword()
            ) :: {:ok, map()} | {:error, :not_found}
end
