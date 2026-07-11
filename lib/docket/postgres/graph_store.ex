if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.GraphStore do
    @moduledoc "Postgres storage for immutable, content-addressed durable graphs."

    @behaviour Docket.Storage.Graphs

    import Ecto.Query

    alias Docket.{DurableCodec, Graph}
    alias Docket.Postgres.Schemas.GraphVersion
    alias Docket.Postgres.Storage

    @impl Docket.Storage.Graphs
    def save_graph(ctx, graph_id, graph_hash, %Graph{id: id, diagnostics: []} = graph)
        when id == graph_id do
      with {:ok, bytes} <- encode(graph),
           :ok <- verify_hash(bytes, graph_hash),
           :ok <- insert(ctx, graph_id, graph_hash, bytes) do
        verify_insert(ctx, graph_id, graph_hash, bytes)
      end
    end

    def save_graph(_ctx, _graph_id, _graph_hash, _graph),
      do: {:error, :invalid_graph_document}

    @impl Docket.Storage.Graphs
    def fetch_graph(ctx, graph_id, graph_hash) do
      with {:ok, bytes} <- fetch_bytes(ctx, graph_id, graph_hash),
           :ok <- verify_stored(bytes, graph_id, graph_hash),
           {:ok, graph} <- decode(bytes),
           true <- valid_decoded?(graph, graph_id, bytes) do
        {:ok, graph}
      else
        {:error, :not_found} -> {:error, :not_found}
        _invalid -> {:error, :corrupt_graph}
      end
    end

    defp insert(ctx, graph_id, graph_hash, bytes) do
      {repo, prefix} = Storage.context!(ctx)

      changeset =
        GraphVersion.changeset(%{graph_id: graph_id, graph_hash: graph_hash, graph: bytes})

      opts =
        maybe_put_prefix(
          [on_conflict: :nothing, conflict_target: [:graph_id, :graph_hash]],
          prefix
        )

      case repo.insert(changeset, opts) do
        {:ok, _version} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end

    defp verify_insert(ctx, graph_id, graph_hash, bytes) do
      case fetch_bytes(ctx, graph_id, graph_hash) do
        {:ok, ^bytes} -> :ok
        {:ok, _other} -> {:error, :graph_content_conflict}
        {:error, reason} -> {:error, reason}
      end
    end

    defp fetch_bytes(ctx, graph_id, graph_hash) do
      {repo, prefix} = Storage.context!(ctx)

      query =
        GraphVersion
        |> where([version], version.graph_id == ^graph_id and version.graph_hash == ^graph_hash)
        |> select([version], version.graph)
        |> with_prefix(prefix)

      case repo.one(query) do
        nil -> {:error, :not_found}
        bytes when is_binary(bytes) -> {:ok, bytes}
        _invalid -> {:error, :corrupt_graph}
      end
    end

    defp encode(graph) do
      {:ok, DurableCodec.encode!(:graph, graph)}
    rescue
      _error in Docket.Error -> {:error, :invalid_graph_document}
    end

    defp decode(bytes) do
      case DurableCodec.decode(bytes, :graph) do
        {:ok, %Graph{} = graph} -> {:ok, graph}
        {:ok, _other} -> {:error, :corrupt_graph}
        {:error, %Docket.Error{}} -> {:error, :corrupt_graph}
      end
    end

    defp verify_stored(bytes, _graph_id, graph_hash), do: verify_hash(bytes, graph_hash)

    defp verify_hash(bytes, graph_hash) when is_binary(graph_hash) do
      if digest(bytes) == graph_hash, do: :ok, else: {:error, :invalid_graph_hash}
    end

    defp verify_hash(_bytes, _graph_hash), do: {:error, :invalid_graph_hash}

    defp valid_decoded?(%Graph{id: id, diagnostics: []} = graph, graph_id, bytes) do
      id == graph_id and DurableCodec.encode!(:graph, graph) == bytes
    rescue
      _error in Docket.Error -> false
    end

    defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    defp with_prefix(query, nil), do: query
    defp with_prefix(query, prefix), do: put_query_prefix(query, prefix)
    defp maybe_put_prefix(opts, nil), do: opts
    defp maybe_put_prefix(opts, prefix), do: Keyword.put(opts, :prefix, prefix)
  end
end
