if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.GraphStore do
    @moduledoc """
    Postgres persistence for immutable, content-addressed graph documents.

    This store accepts only the effective canonical wire map produced before
    publication. It verifies the document ID and SHA-256 content address
    directly from canonical JSON; it deliberately does not deserialize the
    document or load node implementation modules.

    Inserts use Postgres conflict arbitration. After every insert attempt the
    stored JSON document is read back and compared structurally, so an
    existing equal version is idempotent while different content under the
    same address is reported as a conflict.
    """

    @behaviour Docket.Storage.Graphs

    import Ecto.Query

    alias Docket.Graph.Serializer
    alias Docket.Postgres.Schemas.GraphVersion
    alias Docket.Postgres.Storage

    @id_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_-]*$/

    @impl Docket.Storage.Graphs
    def save_graph(ctx, graph_id, graph_hash, document) do
      with :ok <- validate_document(graph_id, document),
           :ok <- validate_hash(graph_hash, document),
           :ok <- insert_version(ctx, graph_id, graph_hash, document) do
        verify_stored_document(ctx, graph_id, graph_hash, document)
      end
    end

    @impl Docket.Storage.Graphs
    def fetch_graph(ctx, graph_id, graph_hash) do
      {repo, prefix} = Storage.context!(ctx)

      query =
        GraphVersion
        |> where([version], version.graph_id == ^graph_id and version.graph_hash == ^graph_hash)
        |> select([version], version.graph)
        |> with_prefix(prefix)

      case repo.one(query) do
        nil -> {:error, :not_found}
        document -> {:ok, document}
      end
    end

    defp insert_version(ctx, graph_id, graph_hash, document) do
      {repo, prefix} = Storage.context!(ctx)

      changeset =
        GraphVersion.changeset(%{
          graph_id: graph_id,
          graph_hash: graph_hash,
          graph: document
        })

      opts =
        [on_conflict: :nothing, conflict_target: [:graph_id, :graph_hash]]
        |> maybe_put_prefix(prefix)

      case repo.insert(changeset, opts) do
        {:ok, _version} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end

    defp verify_stored_document(ctx, graph_id, graph_hash, document) do
      case fetch_graph(ctx, graph_id, graph_hash) do
        {:ok, stored} when stored === document -> :ok
        {:ok, _different} -> {:error, :graph_content_conflict}
        {:error, reason} -> {:error, reason}
      end
    end

    defp validate_document(graph_id, document)
         when is_binary(graph_id) and byte_size(graph_id) > 0 and
                is_map(document) and not is_struct(document) do
      if Regex.match?(@id_pattern, graph_id) and Map.get(document, "id") == graph_id and
           canonical_json?(document) do
        :ok
      else
        {:error, :invalid_graph_document}
      end
    end

    defp validate_document(_graph_id, _document), do: {:error, :invalid_graph_document}

    defp validate_hash(graph_hash, document) when is_binary(graph_hash) do
      actual_hash =
        document
        |> Serializer.canonical_json_encode()
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      if graph_hash == actual_hash, do: :ok, else: {:error, :invalid_graph_hash}
    end

    defp validate_hash(_graph_hash, _document), do: {:error, :invalid_graph_hash}

    defp canonical_json?(value)
         when is_integer(value) or is_float(value) or is_boolean(value) or is_nil(value),
         do: true

    defp canonical_json?(value) when is_binary(value), do: String.valid?(value)
    defp canonical_json?(value) when is_list(value), do: Enum.all?(value, &canonical_json?/1)

    defp canonical_json?(value) when is_map(value) and not is_struct(value) do
      Enum.all?(value, fn
        {key, nested} when is_binary(key) -> String.valid?(key) and canonical_json?(nested)
        _other -> false
      end)
    end

    defp canonical_json?(_value), do: false

    defp with_prefix(query, nil), do: query
    defp with_prefix(query, prefix), do: put_query_prefix(query, prefix)

    defp maybe_put_prefix(opts, nil), do: opts
    defp maybe_put_prefix(opts, prefix), do: Keyword.put(opts, :prefix, prefix)
  end
end
