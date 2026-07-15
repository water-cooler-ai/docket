defmodule Docket.Backend.GraphStore do
  @moduledoc """
  Persistence contract for tenant-owned effective graph versions.

  Graph documents are immutable and addressed within one explicit owner scope
  by `{graph_id, graph_hash}`. Publication materializes node configuration
  defaults before hashing, so the stored document carries its effective
  configuration. Compiled runtime graphs remain node-local and are not part of
  this storage contract.

  An owner scope is either `:tenantless` or `{:tenant, tenant_id}`. `:system`
  is deliberately not accepted: even trusted runtime work must carry the
  owning run's scope when it loads a graph. A `Docket.GraphRef` is therefore a
  scope-relative content address rather than an authorization credential.

  Graph publication is explicit and precedes run creation. `start_run` accepts
  a saved graph reference and reads through this capability; lifecycle
  transactions never publish graph documents.
  """

  @type ctx :: Docket.Backend.ctx()
  @type owner_scope :: Docket.Backend.owner_scope()

  @typedoc "Stable newest-first graph-version cursor: `{published_at, graph_hash}`."
  @type list_cursor :: Docket.GraphVersionPage.cursor()

  @typedoc "Trusted, normalized options for one graph-version collection read."
  @type list_query :: %{
          required(:limit) => pos_integer(),
          required(:before) => list_cursor() | nil
        }

  @doc """
  Saves an effective graph under `{owner_scope, graph_id, graph_hash}`.

  Saving the same document again is idempotent. Reusing the key for different
  content is an error; a backend must not silently accept a hash/document
  mismatch. An idempotent save preserves the original publication timestamp
  and must not move that version to the front of version ordering. Durable
  backends assign that timestamp from their storage substrate rather than an
  application-node clock. The facade compiles and validates a graph before
  calling this function. Storage receives the durable `Docket.Graph` directly;
  public map serialization is not part of this contract.
  """
  @callback save_graph(
              ctx(),
              owner_scope(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              graph :: Docket.Graph.t()
            ) :: :ok | {:error, :graph_content_conflict | term()}

  @doc """
  Reads an effective graph by `{owner_scope, graph_id, graph_hash}`.

  A wrong owner scope and an absent key both return `:not_found`. Backend or
  infrastructure failure is not collapsed into absence; implementations raise
  when the substrate cannot complete the read.
  """
  @callback fetch_graph(
              ctx(),
              owner_scope(),
              graph_id :: String.t(),
              graph_hash :: String.t()
            ) :: {:ok, Docket.Graph.t()} | {:error, :not_found | :corrupt_graph}

  @doc """
  Reads the latest saved version reference for a graph ID in one owner scope.

  Latest is defined by the same immutable order as `list_graph_versions/4`:
  descending publication time, then descending graph hash. This is a metadata
  read and does not load or decode the effective graph document.

  `:not_found` is reserved for an owner scope with no saved version under that
  graph ID. A wrong owner scope is indistinguishable from absence.
  """
  @callback fetch_latest_graph_ref(
              ctx(),
              owner_scope(),
              graph_id :: String.t()
            ) :: {:ok, Docket.GraphRef.t()} | {:error, :not_found | :corrupt_graph}

  @doc """
  Lists retained version metadata for one graph ID in newest-first order.

  `query.before` is an exclusive `{published_at, graph_hash}` cursor and
  `query.limit` bounds the returned versions. Implementations read at most one
  lookahead row and construct a `Docket.GraphVersionPage`, which centralizes
  trimming and next-cursor derivation without revalidating the backend's order.
  The metadata read never loads or decodes effective graph documents. An
  unknown graph ID returns an empty page.
  """
  @callback list_graph_versions(
              ctx(),
              owner_scope(),
              graph_id :: String.t(),
              list_query()
            ) :: {:ok, Docket.GraphVersionPage.t()} | {:error, :corrupt_graph}
end
