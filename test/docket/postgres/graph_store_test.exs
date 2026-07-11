if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.GraphStoreTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres

    alias Docket.Graph.Serializer
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
        Ecto.Migrator.up(
          TestRepo,
          @prefixed_migration_version,
          InstallDocketPrefixed,
          log: false
        )

      :ok
    end

    setup do
      TestRepo.delete_all(GraphVersion)

      GraphVersion
      |> put_query_prefix("docket_private")
      |> TestRepo.delete_all()

      :ok
    end

    test "saves idempotently and fetches the exact canonical map" do
      document = %{
        "schema_version" => 1,
        "id" => "graph",
        "metadata" => %{"nested" => [nil, true, 1, 1.5, %{"value" => "exact"}]}
      }

      graph_hash = hash(document)

      assert :ok = GraphStore.save_graph(TestRepo, "graph", graph_hash, document)
      assert :ok = GraphStore.save_graph(TestRepo, "graph", graph_hash, document)
      assert {:ok, ^document} = GraphStore.fetch_graph(TestRepo, "graph", graph_hash)

      assert TestRepo.aggregate(GraphVersion, :count) == 1
    end

    test "preserves canonical number types and escaped NUL strings through Postgres JSON" do
      document =
        document("wire-fidelity", %{
          "exponent_float" => 1.0e20,
          "negative_zero" => -0.0,
          "escaped_nul" => "before\0after"
        })

      graph_hash = hash(document)

      assert :ok = GraphStore.save_graph(TestRepo, "wire-fidelity", graph_hash, document)
      assert {:ok, fetched} = GraphStore.fetch_graph(TestRepo, "wire-fidelity", graph_hash)
      assert fetched === document
      assert hash(fetched) == graph_hash
      assert is_float(fetched["metadata"]["exponent_float"])
      assert fetched["metadata"]["negative_zero"] === -0.0
      assert fetched["metadata"]["escaped_nul"] == "before\0after"
    end

    test "reloads a real canonical graph for compilation only outside the store" do
      graph = Graphs.minimal_linear()
      document = Docket.Graph.to_map(graph)
      graph_hash = Docket.Graph.hash(graph)

      assert :ok = GraphStore.save_graph(TestRepo, graph.id, graph_hash, document)
      assert {:ok, ^document} = GraphStore.fetch_graph(TestRepo, graph.id, graph_hash)

      reloaded = Docket.Graph.from_map!(document)
      assert Docket.Graph.hash(reloaded) == graph_hash
      assert {:ok, runtime_graph} = Docket.ensure_compiled(reloaded, [])
      assert runtime_graph.graph_hash == graph_hash
    end

    test "validates the document ID and canonical content address without loading node modules" do
      unknown_module = "Elixir.Docket.Test.DefinitelyNotInstalled"
      refute module_loaded?(unknown_module)

      document = %{
        "schema_version" => 1,
        "id" => "portable",
        "nodes" => %{
          "remote" => %{
            "implementation" => %{"type" => "module", "module" => unknown_module}
          }
        }
      }

      graph_hash = hash(document)

      assert :ok = GraphStore.save_graph(TestRepo, "portable", graph_hash, document)
      refute module_loaded?(unknown_module)

      assert {:error, :invalid_graph_document} =
               GraphStore.save_graph(TestRepo, "other-id", graph_hash, document)

      assert {:error, :invalid_graph_document} =
               GraphStore.save_graph(
                 TestRepo,
                 "portable",
                 graph_hash,
                 Map.put(document, :atom_key, true)
               )

      assert {:error, :invalid_graph_hash} =
               GraphStore.save_graph(TestRepo, "portable", String.duplicate("0", 64), document)
    end

    test "reports a structural conflict and never replaces an existing document" do
      requested = document("conflict", %{"winner" => "requested"})
      graph_hash = hash(requested)
      existing = document("conflict", %{"winner" => "existing"})

      insert_raw!("conflict", graph_hash, existing)

      assert {:error, :graph_content_conflict} =
               GraphStore.save_graph(TestRepo, "conflict", graph_hash, requested)

      assert {:ok, ^existing} = GraphStore.fetch_graph(TestRepo, "conflict", graph_hash)
      assert TestRepo.aggregate(GraphVersion, :count) == 1
    end

    test "concurrent equal publications are idempotent" do
      document = document("concurrent-equal", %{"value" => [1, 2, 3]})
      graph_hash = hash(document)

      results =
        concurrently(8, fn ->
          GraphStore.save_graph(TestRepo, "concurrent-equal", graph_hash, document)
        end)

      assert results == List.duplicate(:ok, 8)

      assert {:ok, ^document} =
               GraphStore.fetch_graph(TestRepo, "concurrent-equal", graph_hash)

      assert TestRepo.aggregate(GraphVersion, :count) == 1
    end

    test "concurrent writers all reject an existing conflicting document" do
      requested = document("concurrent-conflict", %{"winner" => "requested"})
      graph_hash = hash(requested)
      existing = document("concurrent-conflict", %{"winner" => "existing"})

      insert_raw!("concurrent-conflict", graph_hash, existing)

      results =
        concurrently(8, fn ->
          GraphStore.save_graph(TestRepo, "concurrent-conflict", graph_hash, requested)
        end)

      assert results == List.duplicate({:error, :graph_content_conflict}, 8)

      assert {:ok, ^existing} =
               GraphStore.fetch_graph(TestRepo, "concurrent-conflict", graph_hash)

      assert TestRepo.aggregate(GraphVersion, :count) == 1
    end

    test "repo/prefix context isolates publication and fetch" do
      ctx = %{repo: TestRepo, prefix: "docket_private"}
      document = document("prefixed", %{"location" => "private"})
      graph_hash = hash(document)

      assert {:ok, :saved} =
               Storage.transaction(ctx, fn transaction_ctx ->
                 assert transaction_ctx == ctx

                 assert :ok =
                          GraphStore.save_graph(transaction_ctx, "prefixed", graph_hash, document)

                 {:ok, :saved}
               end)

      assert {:ok, ^document} = GraphStore.fetch_graph(ctx, "prefixed", graph_hash)
      assert {:error, :not_found} = GraphStore.fetch_graph(TestRepo, "prefixed", graph_hash)
    end

    defp concurrently(count, operation) do
      parent = self()

      tasks =
        for _index <- 1..count do
          Task.async(fn ->
            send(parent, {:ready, self()})

            receive do
              :go -> operation.()
            end
          end)
        end

      pids =
        for _index <- 1..count do
          assert_receive {:ready, pid}, 5_000
          pid
        end

      Enum.each(pids, &send(&1, :go))
      Enum.map(tasks, &Task.await(&1, 10_000))
    end

    defp insert_raw!(graph_id, graph_hash, document) do
      %{graph_id: graph_id, graph_hash: graph_hash, graph: document}
      |> GraphVersion.changeset()
      |> TestRepo.insert!()
    end

    defp document(id, metadata) do
      %{"schema_version" => 1, "id" => id, "metadata" => metadata}
    end

    defp hash(document) do
      document
      |> Serializer.canonical_json_encode()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)
    end

    defp module_loaded?(name) do
      name
      |> String.to_existing_atom()
      |> Code.ensure_loaded?()
    rescue
      ArgumentError -> false
    end
  end
end
