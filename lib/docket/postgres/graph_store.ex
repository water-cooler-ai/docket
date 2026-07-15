if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.GraphStore do
    @moduledoc "Postgres storage for immutable, content-addressed durable graphs."

    @behaviour Docket.Backend.GraphStore

    import Ecto.Query

    alias Docket.{DurableCodec, Graph, GraphRef, GraphVersion, GraphVersionPage}
    alias Docket.Postgres.Schemas.GraphVersion, as: GraphVersionSchema
    alias Docket.Postgres.Storage

    @impl Docket.Backend.GraphStore
    def save_graph(
          ctx,
          owner_scope,
          graph_id,
          graph_hash,
          %Graph{id: id, diagnostics: []} = graph
        )
        when id == graph_id do
      started = System.monotonic_time()

      result =
        with :ok <- validate_owner_scope!(owner_scope),
             {:ok, bytes} <- encode(graph),
             :ok <- verify_hash(bytes, graph_hash),
             :ok <- insert(ctx, owner_scope, graph_id, graph_hash, bytes) do
          verify_insert(ctx, owner_scope, graph_id, graph_hash, bytes)
        end

      emit_store(:graph_save, started, result, graph_bytes(result, graph))
      result
    end

    def save_graph(_ctx, _owner_scope, _graph_id, _graph_hash, _graph),
      do: {:error, :invalid_graph_document}

    @impl Docket.Backend.GraphStore
    def fetch_graph(ctx, owner_scope, graph_id, graph_hash) do
      started = System.monotonic_time()

      result =
        with :ok <- validate_owner_scope!(owner_scope),
             {:ok, bytes} <- fetch_bytes(ctx, owner_scope, graph_id, graph_hash),
             {:ok, graph} <- load_graph(bytes, graph_id, graph_hash) do
          {:ok, graph}
        else
          {:error, :not_found} -> {:error, :not_found}
          _invalid -> {:error, :corrupt_graph}
        end

      emit_store(:graph_fetch, started, result, 0)
      result
    end

    @impl Docket.Backend.GraphStore
    def fetch_latest_graph_ref(ctx, owner_scope, graph_id) do
      started = System.monotonic_time()

      result =
        with :ok <- validate_owner_scope!(owner_scope) do
          fetch_latest_ref(ctx, owner_scope, graph_id)
        end

      emit_store(:graph_fetch_latest_ref, started, result, 0)
      result
    end

    @impl Docket.Backend.GraphStore
    def list_graph_versions(ctx, owner_scope, graph_id, query) do
      started = System.monotonic_time()

      result =
        with :ok <- validate_owner_scope!(owner_scope),
             %{limit: limit, before: before} <- validate_list_query!(query),
             {:ok, candidates} <-
               list_version_candidates(ctx, owner_scope, graph_id, before, limit) do
          {:ok, GraphVersionPage.new(candidates, before, limit)}
        end

      emit_store(:graph_list_versions, started, result, 0)
      result
    end

    defp emit_store(operation, started, result, bytes) do
      :telemetry.execute(
        [:docket, :postgres, :store],
        %{
          duration: System.monotonic_time() - started,
          encoded_bytes: bytes,
          attempted_rows: if(operation == :graph_save, do: 1, else: 0),
          selected_rows: selected_rows(operation, result)
        },
        Map.merge(Docket.Telemetry.correlation_metadata(), %{
          operation: operation,
          result: Docket.Telemetry.result_kind(result)
        })
      )
    end

    defp graph_bytes(:ok, graph),
      do: graph |> then(&DurableCodec.encode!(:graph, &1)) |> byte_size()

    defp graph_bytes(_, _), do: 0

    defp selected_rows(operation, {:ok, _result})
         when operation in [:graph_fetch, :graph_fetch_latest_ref],
         do: 1

    defp selected_rows(:graph_list_versions, {:ok, %GraphVersionPage{versions: versions}}),
      do: length(versions)

    defp selected_rows(_operation, _result), do: 0

    defp insert(ctx, owner_scope, graph_id, graph_hash, bytes) do
      {repo, prefix} = Storage.context!(ctx)

      changeset =
        GraphVersionSchema.changeset(%{
          tenant_id: owner_tenant_id!(owner_scope),
          graph_id: graph_id,
          graph_hash: graph_hash,
          graph: bytes
        })

      opts =
        maybe_put_prefix(
          [
            on_conflict: :nothing,
            conflict_target: [:scope_key, :graph_id, :graph_hash]
          ],
          prefix
        )

      case repo.insert(changeset, opts) do
        {:ok, _version} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end

    defp verify_insert(ctx, owner_scope, graph_id, graph_hash, bytes) do
      case fetch_bytes(ctx, owner_scope, graph_id, graph_hash) do
        {:ok, ^bytes} -> :ok
        {:ok, _other} -> {:error, :graph_content_conflict}
        {:error, reason} -> {:error, reason}
      end
    end

    defp fetch_bytes(ctx, owner_scope, graph_id, graph_hash) do
      {repo, prefix} = Storage.context!(ctx)

      query =
        GraphVersionSchema
        |> scope_query(owner_scope)
        |> where([version], version.graph_id == ^graph_id and version.graph_hash == ^graph_hash)
        |> select([version], version.graph)
        |> with_prefix(prefix)

      case repo.one(query) do
        nil -> {:error, :not_found}
        bytes when is_binary(bytes) -> {:ok, bytes}
        _invalid -> {:error, :corrupt_graph}
      end
    end

    defp fetch_latest_ref(ctx, owner_scope, graph_id) do
      {repo, prefix} = Storage.context!(ctx)

      query =
        GraphVersionSchema
        |> scope_query(owner_scope)
        |> where([version], version.graph_id == ^graph_id)
        |> order_by([version], desc: version.inserted_at, desc: version.graph_hash)
        |> limit(1)
        |> select([version], version.graph_hash)
        |> with_prefix(prefix)

      case repo.one(query) do
        nil ->
          {:error, :not_found}

        graph_hash when is_binary(graph_hash) and byte_size(graph_hash) > 0 ->
          {:ok, %GraphRef{graph_id: graph_id, graph_hash: graph_hash}}

        _invalid ->
          {:error, :corrupt_graph}
      end
    end

    defp list_version_candidates(ctx, owner_scope, graph_id, before, limit) do
      {repo, prefix} = Storage.context!(ctx)

      GraphVersionSchema
      |> scope_query(owner_scope)
      |> where([version], version.graph_id == ^graph_id)
      |> before_query(before)
      |> order_by([version], desc: version.inserted_at, desc: version.graph_hash)
      |> limit(^(limit + 1))
      |> select([version], {version.graph_hash, version.inserted_at})
      |> with_prefix(prefix)
      |> repo.all()
      |> build_graph_versions(graph_id)
    end

    defp build_graph_versions(rows, graph_id) do
      Enum.reduce_while(rows, {:ok, []}, fn
        {graph_hash, %DateTime{} = published_at}, {:ok, versions}
        when is_binary(graph_hash) and byte_size(graph_hash) > 0 ->
          version =
            %GraphVersion{
              ref: %GraphRef{graph_id: graph_id, graph_hash: graph_hash},
              published_at: published_at
            }

          {:cont, {:ok, [version | versions]}}

        _invalid, _versions ->
          {:halt, {:error, :corrupt_graph}}
      end)
      |> case do
        {:ok, versions} -> {:ok, Enum.reverse(versions)}
        {:error, :corrupt_graph} = error -> error
      end
    end

    defp before_query(query, nil), do: query

    defp before_query(query, {%DateTime{} = published_at, graph_hash}) do
      where(
        query,
        [version],
        version.inserted_at < ^published_at or
          (version.inserted_at == ^published_at and version.graph_hash < ^graph_hash)
      )
    end

    defp validate_list_query!(%{limit: limit, before: before} = query)
         when map_size(query) == 2 and is_integer(limit) and limit > 0 do
      validate_list_cursor!(before)
      query
    end

    defp validate_list_query!(query) do
      raise ArgumentError,
            "graph version list query requires positive limit and normalized before, got: " <>
              inspect(query)
    end

    defp validate_list_cursor!(nil), do: :ok

    defp validate_list_cursor!({%DateTime{}, graph_hash})
         when is_binary(graph_hash) and byte_size(graph_hash) > 0,
         do: :ok

    defp validate_list_cursor!(before) do
      raise ArgumentError,
            "graph version list before cursor must be nil or " <>
              "{DateTime, non-empty graph_hash}, got: #{inspect(before)}"
    end

    defp scope_query(query, :tenantless), do: where(query, [version], version.scope_key == "")

    defp scope_query(query, {:tenant, tenant_id})
         when is_binary(tenant_id) and byte_size(tenant_id) > 0 do
      where(query, [version], version.scope_key == ^tenant_id)
    end

    defp owner_tenant_id!(:tenantless), do: nil

    defp owner_tenant_id!({:tenant, tenant_id})
         when is_binary(tenant_id) and byte_size(tenant_id) > 0,
         do: tenant_id

    defp validate_owner_scope!(:tenantless), do: :ok

    defp validate_owner_scope!({:tenant, tenant_id})
         when is_binary(tenant_id) and byte_size(tenant_id) > 0,
         do: :ok

    defp validate_owner_scope!(scope) do
      raise ArgumentError,
            "graph owner scope must be :tenantless or {:tenant, tenant_id}, got: " <>
              inspect(scope)
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

    defp load_graph(bytes, graph_id, graph_hash) do
      with :ok <- verify_hash(bytes, graph_hash),
           {:ok, graph} <- decode(bytes),
           true <- valid_decoded?(graph, graph_id, bytes) do
        {:ok, graph}
      else
        _invalid -> {:error, :corrupt_graph}
      end
    end

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
