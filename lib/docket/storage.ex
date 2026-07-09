defmodule Docket.Storage do
  @moduledoc """
  Persistence seam a durable backend implements: a run store, a graph store,
  and event persistence.

  The run store holds the canonical `Docket.Run` document as the single full
  document for a run. Reads through this behaviour are storage-backed: they
  return the last committed run and may lag a live in-memory run by one
  uncommitted superstep. A storage-backed read is distinct from a live process
  read, which reflects work a running worker has not yet committed.

  The graph store is a content-addressed record of compiled graph documents,
  keyed by `{graph_id, graph_hash}`, that a worker reloads with no host call
  in the loop. Event persistence appends run facts under a backend-defined
  policy.

  Checkpoint-commit history persists as metadata-only events: a backend that
  wants a durable per-commit record derives a metadata-only entry from the
  checkpoint it is committing and persists it through `persist_events/4`.
  There is no checkpoint-snapshot callback, and a backend never persists a
  full run document per commit — the stored run is the only full document.

  `Docket.Checkpoint` handlers remain the host-facing notification seam;
  implementing this behaviour is a separate, deeper commitment for backends
  that own persistence directly.

  All callbacks take an opaque backend context as the first argument. `ctx`
  is whatever the backend needs — a repo, an agent pid, a config struct — and
  core never interprets it.
  """

  @type ctx :: term()
  @type claim_token :: String.t()

  @typedoc """
  Optimistic commit guard.

  `expected_seq` is the `Docket.Run.checkpoint_seq` the committer read. When
  `claim_token` is non-nil the write additionally fences on the current claim
  for the run.
  """
  @type fence :: %{expected_seq: non_neg_integer(), claim_token: claim_token() | nil}

  @doc """
  Persists a new run.

  Inserting a `run_id` that already exists is an error. `opts` may carry a
  `:tenant_id`, a scoping value the backend stores alongside the run; core has
  no tenant field on `Docket.Run`.
  """
  @callback insert_run(ctx(), Docket.Run.t(), opts :: keyword()) ::
              {:ok, Docket.Run.t()} | {:error, term()}

  @doc """
  Reads the last committed run by id.

  This is the storage-backed read: it returns the last committed document and
  may lag a live in-memory run by one uncommitted superstep. Runs are keyed by
  `run_id` alone; a `:tenant_id` in `opts` scopes the lookup, and a tenant
  mismatch reads as `{:error, :not_found}`, never a permission error. An
  unscoped fetch by `run_id` succeeds regardless of the stored tenant.
  """
  @callback fetch_run(ctx(), run_id :: String.t(), opts :: keyword()) ::
              {:ok, Docket.Run.t()} | {:error, :not_found}

  @doc """
  Replaces the stored run under an optimistic fence.

  Succeeds only if the stored run's `checkpoint_seq` equals
  `fence.expected_seq` and, when `fence.claim_token` is non-nil, only if it
  matches the backend's current claim for the run. No lock is held between the
  committer's read and this write; the fence is checked at commit time only.
  Any mismatch returns `{:error, :stale_fence}` and leaves the stored run
  untouched.
  """
  @callback update_run(ctx(), Docket.Run.t(), fence(), opts :: keyword()) ::
              {:ok, Docket.Run.t()} | {:error, :stale_fence} | {:error, :not_found}

  @doc """
  Upserts a compiled graph document keyed by `{graph_id, graph_hash}`.

  `document` is the compiled graph wire map from `Docket.Graph.to_map/1`. The
  upsert is content-addressed and idempotent: two callers racing to put the
  same version both succeed, because content addressing makes the document
  byte-identical.
  """
  @callback put_graph(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              document :: map(),
              opts :: keyword()
            ) :: :ok | {:error, term()}

  @doc """
  Reads a compiled graph document by `{graph_id, graph_hash}`.

  This is the recovery read a worker uses to reload the exact graph content
  for a run it claimed, with no host call in the loop. An unknown key returns
  `{:error, :not_found}`.
  """
  @callback fetch_graph(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              opts :: keyword()
            ) :: {:ok, map()} | {:error, :not_found}

  @doc """
  Appends run events under the backend's persistence policy.

  Events are append-only facts. Persistence volume is backend policy — all,
  none, or selected types — so the callback may drop events per that policy
  and still return `:ok`.
  """
  @callback persist_events(ctx(), run_id :: String.t(), [Docket.Event.t()], opts :: keyword()) ::
              :ok | {:error, term()}
end
