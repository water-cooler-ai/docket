defmodule Docket.Storage.Graphs do
  @moduledoc """
  Persistence contract for effective durable graph versions.

  Graph documents are immutable and addressed by `{graph_id, graph_hash}`.
  Publication materializes node configuration defaults before hashing, so the
  stored document carries its effective configuration. Compiled runtime graphs
  remain node-local and are not part of this storage contract.
  Graph publication is explicit and precedes run creation. `start_run` accepts
  a saved graph reference and reads through this capability; lifecycle
  transactions never publish graph documents.

  Graph versions are content addressed rather than tenant scoped. Backend
  configuration belongs in the opaque context, not per-call options.
  """

  @type ctx :: Docket.Storage.ctx()

  @doc """
  Saves an effective graph under `{graph_id, graph_hash}`.

  Saving the same document again is idempotent. Reusing the key for different
  content is an error; a backend must not silently accept a hash/document
  mismatch. The facade compiles and validates a graph before calling this
  function. Storage receives the durable `Docket.Graph` directly; public map
  serialization is not part of this contract.
  """
  @callback save_graph(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              graph :: Docket.Graph.t()
            ) :: :ok | {:error, :graph_content_conflict | term()}

  @doc """
  Reads an effective graph by `{graph_id, graph_hash}`.

  `:not_found` is reserved for an absent key. Backend/infrastructure failure
  is not collapsed into absence; implementations raise when the substrate
  cannot complete the read.
  """
  @callback fetch_graph(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t()
            ) :: {:ok, Docket.Graph.t()} | {:error, :not_found | :corrupt_graph}

  @doc """
  Reads the latest saved version of a graph ID.

  Latest is defined by durable publication order: descending `inserted_at`,
  with the backend row ID as the stable descending tie-break. The returned
  projection includes both the effective graph and its exact content address.

  `:not_found` is reserved for an absent graph ID. A present latest row whose
  content address or durable document is invalid returns `:corrupt_graph`; an
  implementation must not fall back to an older version.
  """
  @callback fetch_latest_graph(
              ctx(),
              graph_id :: String.t()
            ) :: {:ok, Docket.SavedGraph.t()} | {:error, :not_found | :corrupt_graph}
end
