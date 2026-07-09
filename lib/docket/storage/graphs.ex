defmodule Docket.Storage.Graphs do
  @moduledoc """
  Persistence contract for canonical graph versions.

  Graph documents are immutable and addressed by `{graph_id, graph_hash}`.
  Lifecycle orchestration uses `Docket.Storage.transaction/2` when publishing
  a graph version must be atomic with creating a run and its initial events.
  """

  @type ctx :: Docket.Storage.ctx()

  @doc """
  Saves a canonical graph document under `{graph_id, graph_hash}`.

  Saving the same document again is idempotent. Reusing the key for different
  content is an error; a backend must not silently accept a hash/document
  mismatch. `opts` carries backend-specific storage options.
  """
  @callback save_graph(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              graph_document :: map(),
              opts :: keyword()
            ) :: :ok | {:error, term()}

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
