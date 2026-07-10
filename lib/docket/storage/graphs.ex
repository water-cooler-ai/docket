defmodule Docket.Storage.Graphs do
  @moduledoc """
  Persistence contract for canonical graph versions and compiled artifacts.

  Graph documents are immutable and addressed by `{graph_id, graph_hash}`.
  Graph publication is explicit and precedes run creation. One publication
  transaction saves the canonical source and a JSON-safe execution artifact
  selected by compiler ABI. `start_run`, signals, recovery, and vehicles load
  the artifact; lifecycle transactions never publish graph data.

  Graph versions are content addressed rather than tenant scoped. Backend
  configuration belongs in the opaque context, not per-call options.
  """

  @type ctx :: Docket.Storage.ctx()

  @doc """
  Saves a canonical graph document under `{graph_id, graph_hash}`.

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
  """
  @callback fetch_graph(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t()
            ) :: {:ok, map()} | {:error, :not_found}

  @doc """
  Saves one immutable compiled execution artifact for a graph version and ABI.

  Identical replay is idempotent. Different artifact content under the same
  `{graph_id, graph_hash, compiler_abi}` is a conflict.
  """
  @callback save_artifact(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              compiler_abi :: String.t(),
              artifact :: map()
            ) :: :ok | {:error, :artifact_content_conflict | term()}

  @doc "Fetches the exact compiled artifact selected by graph version and ABI."
  @callback fetch_artifact(
              ctx(),
              graph_id :: String.t(),
              graph_hash :: String.t(),
              compiler_abi :: String.t()
            ) :: {:ok, map()} | {:error, :not_found}
end
