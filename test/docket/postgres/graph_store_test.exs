if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.GraphStoreTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres

    alias Docket.{DurableCodec, Graph}
    alias Docket.Postgres.{GraphStore, Storage}
    alias Docket.Postgres.GraphStoreTestRepo, as: TestRepo
    alias Docket.Postgres.Schemas.GraphVersion
    alias Docket.Test.Fixtures.Graphs

    @migration_version 20_260_710_000_021
    @prefixed_migration_version 20_260_710_000_022

    defmodule InstallDocket do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    defmodule InstallDocketPrefixed do
      use Ecto.Migration
      def up, do: Docket.Postgres.Migration.up(prefix: "docket_private")
      def down, do: Docket.Postgres.Migration.down(prefix: "docket_private")
    end

    setup_all do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)

      :ok =
        Ecto.Migrator.up(TestRepo, @prefixed_migration_version, InstallDocketPrefixed, log: false)
    end

    setup do
      TestRepo.delete_all(GraphVersion)
      GraphVersion |> put_query_prefix("docket_private") |> TestRepo.delete_all()
      :ok
    end

    test "stores exact deterministic ETF and returns the effective Graph" do
      {graph, graph_hash} = effective_graph()
      bytes = DurableCodec.encode!(:graph, graph)

      assert digest(bytes) == graph_hash
      assert :ok = GraphStore.save_graph(TestRepo, graph.id, graph_hash, graph)
      assert :ok = GraphStore.save_graph(TestRepo, graph.id, graph_hash, graph)
      assert {:ok, ^graph} = GraphStore.fetch_graph(TestRepo, graph.id, graph_hash)
      assert TestRepo.one(from(version in GraphVersion, select: version.graph)) == bytes
      assert TestRepo.aggregate(GraphVersion, :count) == 1
    end

    test "a fetched graph compiles directly from durable storage" do
      {graph, graph_hash} = effective_graph()
      :ok = GraphStore.save_graph(TestRepo, graph.id, graph_hash, graph)

      assert {:ok, stored} = GraphStore.fetch_graph(TestRepo, graph.id, graph_hash)
      assert {:ok, runtime_graph} = Docket.ensure_compiled_effective(stored, [])
      assert runtime_graph.graph_hash == graph_hash
    end

    test "rejects the wrong id, diagnostics, value type, and content address" do
      {graph, graph_hash} = effective_graph()

      assert {:error, :invalid_graph_document} =
               GraphStore.save_graph(TestRepo, "other", graph_hash, graph)

      assert {:error, :invalid_graph_document} =
               GraphStore.save_graph(TestRepo, graph.id, graph_hash, %{
                 graph
                 | diagnostics: [:bad]
               })

      assert {:error, :invalid_graph_document} =
               GraphStore.save_graph(TestRepo, graph.id, graph_hash, %{})

      assert {:error, :invalid_graph_hash} =
               GraphStore.save_graph(TestRepo, graph.id, String.duplicate("0", 64), graph)
    end

    test "never replaces different bytes already stored under the address" do
      {graph, graph_hash} = effective_graph()
      other = %{graph | metadata: %{"different" => true}}
      insert_raw!(graph.id, graph_hash, DurableCodec.encode!(:graph, other))

      assert {:error, :graph_content_conflict} =
               GraphStore.save_graph(TestRepo, graph.id, graph_hash, graph)
    end

    test "corrupt bytes fail closed instead of looking absent" do
      corrupt = <<131, 0, 1, 2, 3>>
      graph_hash = digest(corrupt)
      insert_raw!("corrupt", graph_hash, corrupt)

      assert {:error, :corrupt_graph} =
               GraphStore.fetch_graph(TestRepo, "corrupt", graph_hash)

      assert {:error, :not_found} = GraphStore.fetch_graph(TestRepo, "missing", graph_hash)
    end

    test "structurally malformed graph ETF fails closed" do
      graph = %{Graph.new!(id: "malformed") | inputs: %{"x" => %{not: :field}}}

      bytes =
        :erlang.term_to_binary(
          {:docket, 1, :graph, graph},
          [:deterministic, minor_version: 2]
        )

      graph_hash = digest(bytes)
      insert_raw!(graph.id, graph_hash, bytes)

      assert {:error, :corrupt_graph} =
               GraphStore.fetch_graph(TestRepo, graph.id, graph_hash)
    end

    test "repo prefixes isolate graph versions" do
      {graph, graph_hash} = effective_graph()
      ctx = %{repo: TestRepo, prefix: "docket_private"}

      assert {:ok, :saved} =
               Storage.transaction(ctx, fn tx ->
                 with :ok <- GraphStore.save_graph(tx, graph.id, graph_hash, graph),
                      do: {:ok, :saved}
               end)

      assert {:ok, ^graph} = GraphStore.fetch_graph(ctx, graph.id, graph_hash)
      assert {:error, :not_found} = GraphStore.fetch_graph(TestRepo, graph.id, graph_hash)
    end

    defp effective_graph do
      {:ok, graph, runtime_graph} =
        Graph.Compiler.compile_for_publication(Graphs.minimal_linear(), profile: :publish)

      {graph, runtime_graph.graph_hash}
    end

    defp insert_raw!(graph_id, graph_hash, bytes) do
      %{graph_id: graph_id, graph_hash: graph_hash, graph: bytes}
      |> GraphVersion.changeset()
      |> TestRepo.insert!()
    end

    defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
  end
end
