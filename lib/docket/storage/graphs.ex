defmodule Docket.Storage.Graphs do
  @moduledoc """
  Persistence contract for effective canonical graph versions.

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
  Saves an effective canonical graph document under `{graph_id, graph_hash}`.

  Saving the same document again is idempotent. Reusing the key for different
  content is an error; a backend must not silently accept a hash/document
  mismatch. The facade compiles and validates a graph before calling this
  function, while storage retains the portable canonical document.
  """
  @callback save_graph(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              graph_document :: map()
            ) :: :ok | {:error, :graph_content_conflict | term()}

  @doc """
  Reads a canonical graph document by `{graph_id, graph_hash}`.

  `:not_found` is reserved for an absent key. Backend/infrastructure failure
  is not collapsed into absence; implementations raise when the substrate
  cannot complete the read.
  """
  @callback fetch_graph(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t()
            ) :: {:ok, map()} | {:error, :not_found}
end
