if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.GraphCache do
    @moduledoc """
    Optional node-local cache of compiled runtime graphs and known-incompatible
    graph versions.

    Entries are keyed by the store-provided `{graph_id, graph_hash}` and
    validated on every read against the local generation: a fingerprint of the
    `:docket` application's loaded modules plus the beam MD5 of each node
    implementation module recorded when the entry was written. An entry whose
    generation no longer matches is erased and reported as a miss, so a cached
    graph never crosses an incompatible local generation and cache loss only
    affects latency.

    Incompatible entries record why a version cannot run here so repeated
    claims of the same doomed version skip the fetch and compile. A version
    whose stored document could not even be decoded records no module list;
    those entries additionally expire after `:undecodable_ttl_ms` so a deploy
    that only adds host modules still retries within one backoff window.

    Terms live in `:persistent_term`: reads are copy-free, writes happen once
    per graph version per generation, and stale entries self-evict on read.
    Entries for graph versions that stop being claimed are not swept; `clear/0`
    removes everything.

    All functions accept the same options:

      * `:generation` - zero-arity function replacing the built-in `:docket`
        module fingerprint, for hosts that prefer an explicit release identity
      * `:undecodable_ttl_ms` - lifetime of entries recorded without a module
        list (default `#{30_000}`)
    """

    @behaviour Docket.Postgres.GraphCache.Contract

    defmodule Contract do
      @moduledoc """
      Contract for a vehicle graph cache.

      `fetch/3` returns a runtime graph compiled under the current local
      generation, a recorded incompatibility for the current generation, or
      `:miss`. Writers record the local generation with the entry; stale
      entries must never be returned.
      """

      @callback fetch(String.t(), String.t(), keyword()) ::
                  {:ok, Docket.Runtime.Graph.t()} | {:incompatible, term()} | :miss

      @callback put_compiled(String.t(), String.t(), Docket.Runtime.Graph.t(), keyword()) :: :ok

      @callback put_incompatible(
                  String.t(),
                  String.t(),
                  Docket.Graph.t() | :undecodable,
                  term(),
                  keyword()
                ) :: :ok
    end

    @default_undecodable_ttl_ms 30_000

    @impl Contract
    def fetch(graph_id, graph_hash, opts \\ []) do
      key = key(graph_id, graph_hash)

      case :persistent_term.get(key, :miss) do
        :miss ->
          :miss

        {:compiled, rtg, generation} ->
          if current?(generation, opts), do: {:ok, rtg}, else: evict(key)

        {:incompatible, reason, generation} ->
          if current?(generation, opts) and not expired?(generation),
            do: {:incompatible, reason},
            else: evict(key)
      end
    end

    @impl Contract
    def put_compiled(graph_id, graph_hash, rtg, opts \\ []) do
      modules = rtg.nodes |> Map.values() |> Enum.map(& &1.module)
      entry = {:compiled, rtg, generation(modules, nil, opts)}
      :persistent_term.put(key(graph_id, graph_hash), entry)
    end

    @impl Contract
    def put_incompatible(graph_id, graph_hash, source, reason, opts \\ []) do
      generation =
        case source do
          :undecodable ->
            expires_at = System.monotonic_time(:millisecond) + undecodable_ttl_ms(opts)
            generation(:undecodable, expires_at, opts)

          %Docket.Graph{} = graph ->
            generation(implementation_modules(graph), nil, opts)
        end

      entry = {:incompatible, reason, generation}
      :persistent_term.put(key(graph_id, graph_hash), entry)
    end

    @doc "Erases every cache entry."
    @spec clear() :: :ok
    def clear do
      for {{__MODULE__, _graph_id, _graph_hash} = key, _entry} <- :persistent_term.get() do
        :persistent_term.erase(key)
      end

      :ok
    end

    defp key(graph_id, graph_hash), do: {__MODULE__, graph_id, graph_hash}

    defp evict(key) do
      :persistent_term.erase(key)
      :miss
    end

    defp generation(modules, expires_at, opts) do
      %{
        docket: docket_generation(opts),
        modules: fingerprint(modules),
        expires_at: expires_at
      }
    end

    defp current?(generation, opts) do
      generation.docket == docket_generation(opts) and
        generation.modules == refreshed(generation.modules)
    end

    defp expired?(%{expires_at: nil}), do: false

    defp expired?(%{expires_at: expires_at}),
      do: System.monotonic_time(:millisecond) >= expires_at

    defp fingerprint(:undecodable), do: :undecodable

    defp fingerprint(modules) do
      modules
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map(&{&1, module_md5(&1)})
    end

    defp refreshed(:undecodable), do: :undecodable

    defp refreshed(modules),
      do: Enum.map(modules, fn {module, _md5} -> {module, module_md5(module)} end)

    defp docket_generation(opts) do
      case Keyword.get(opts, :generation) do
        nil ->
          :docket
          |> Application.spec(:modules)
          |> List.wrap()
          |> fingerprint()
          |> :erlang.term_to_binary()
          |> :erlang.md5()

        generation when is_function(generation, 0) ->
          generation.()
      end
    end

    defp module_md5(module) do
      if Code.ensure_loaded?(module), do: module.module_info(:md5), else: :missing
    end

    defp implementation_modules(%Docket.Graph{nodes: nodes}) do
      for {_id, node} <- nodes,
          match?(%{type: :module, module: module} when is_atom(module), node.implementation) do
        node.implementation.module
      end
    end

    defp undecodable_ttl_ms(opts),
      do: Keyword.get(opts, :undecodable_ttl_ms, @default_undecodable_ttl_ms)
  end
end
