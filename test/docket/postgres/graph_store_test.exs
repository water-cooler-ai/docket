if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.GraphStoreTest do
    use ExUnit.Case, async: false

    import Ecto.Query

    @moduletag :postgres

    alias Docket.{DurableCodec, Graph, GraphRef, SavedGraph}
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

    test "fetches the latest saved graph with its exact content address" do
      {older, older_hash} = effective_graph("minimal-linear", "older")
      {newer, newer_hash} = effective_graph("minimal-linear", "newer")

      assert :ok = GraphStore.save_graph(TestRepo, older.id, older_hash, older)
      assert :ok = GraphStore.save_graph(TestRepo, newer.id, newer_hash, newer)

      assert {:ok,
              %SavedGraph{
                ref: %GraphRef{graph_id: "minimal-linear", graph_hash: ^newer_hash},
                graph: ^newer
              }} = GraphStore.fetch_latest_graph(TestRepo, "minimal-linear")

      assert {:error, :not_found} = GraphStore.fetch_latest_graph(TestRepo, "missing")
    end

    test "latest graph order uses inserted_at and then the row id as a stable tie-break" do
      {first, first_hash} = effective_graph("revision-order", "first")
      {second, second_hash} = effective_graph("revision-order", "second")

      assert :ok = GraphStore.save_graph(TestRepo, first.id, first_hash, first)
      assert :ok = GraphStore.save_graph(TestRepo, second.id, second_hash, second)

      TestRepo.update_all(
        from(version in GraphVersion,
          where: version.graph_id == "revision-order" and version.graph_hash == ^first_hash
        ),
        set: [inserted_at: ~U[2026-07-12 12:00:01.000000Z]]
      )

      TestRepo.update_all(
        from(version in GraphVersion,
          where: version.graph_id == "revision-order" and version.graph_hash == ^second_hash
        ),
        set: [inserted_at: ~U[2026-07-12 12:00:00.000000Z]]
      )

      assert {:ok, %SavedGraph{ref: %GraphRef{graph_hash: ^first_hash}, graph: ^first}} =
               GraphStore.fetch_latest_graph(TestRepo, "revision-order")

      TestRepo.update_all(
        from(version in GraphVersion, where: version.graph_id == "revision-order"),
        set: [inserted_at: ~U[2026-07-12 12:00:00.000000Z]]
      )

      assert {:ok, %SavedGraph{ref: %GraphRef{graph_hash: ^second_hash}, graph: ^second}} =
               GraphStore.fetch_latest_graph(TestRepo, "revision-order")
    end

    test "latest graph verifies the stored hash, decoded id, and durable document" do
      {hash_mismatch, _hash} = effective_graph("hash-mismatch", "document")

      insert_raw!(
        hash_mismatch.id,
        String.duplicate("0", 64),
        DurableCodec.encode!(:graph, hash_mismatch)
      )

      {wrong_id, wrong_id_hash} = effective_graph("actual-id", "document")
      insert_raw!("wrong-id", wrong_id_hash, DurableCodec.encode!(:graph, wrong_id))

      corrupt = <<131, 0, 1, 2, 3>>
      insert_raw!("bad-document", digest(corrupt), corrupt)

      assert {:error, :corrupt_graph} =
               GraphStore.fetch_latest_graph(TestRepo, "hash-mismatch")

      assert {:error, :corrupt_graph} = GraphStore.fetch_latest_graph(TestRepo, "wrong-id")
      assert {:error, :corrupt_graph} = GraphStore.fetch_latest_graph(TestRepo, "bad-document")
    end

    test "a corrupt latest graph fails closed instead of falling back" do
      {valid, valid_hash} = effective_graph("no-fallback", "valid")
      assert :ok = GraphStore.save_graph(TestRepo, valid.id, valid_hash, valid)

      corrupt = <<131, 0, 1, 2, 3>>
      insert_raw!(valid.id, digest(corrupt), corrupt)

      assert {:error, :corrupt_graph} = GraphStore.fetch_latest_graph(TestRepo, valid.id)
    end

    test "latest graph fetch emits bounded store telemetry" do
      {graph, graph_hash} = effective_graph()
      assert :ok = GraphStore.save_graph(TestRepo, graph.id, graph_hash, graph)

      handler_id = "latest-graph-telemetry-#{System.unique_integer([:positive])}"
      parent = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:docket, :postgres, :store],
          &Docket.Test.TelemetryRelay.tagged/4,
          {parent, :latest_graph_telemetry}
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert {:ok, %SavedGraph{}} = GraphStore.fetch_latest_graph(TestRepo, graph.id)

      assert_receive {:latest_graph_telemetry, measurements, metadata}
      assert is_integer(measurements.duration) and measurements.duration >= 0
      assert measurements.selected_rows == 1
      assert measurements.attempted_rows == 0
      assert measurements.encoded_bytes == 0
      assert metadata.operation == :graph_fetch_latest
      assert metadata.result == :ok
      refute Map.has_key?(metadata, :graph_id)
      refute Map.has_key?(metadata, :graph_hash)
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

      assert {:ok,
              %SavedGraph{
                ref: %GraphRef{graph_id: "minimal-linear", graph_hash: ^graph_hash},
                graph: ^graph
              }} = GraphStore.fetch_latest_graph(ctx, graph.id)

      assert {:error, :not_found} = GraphStore.fetch_latest_graph(TestRepo, graph.id)
    end

    defp effective_graph do
      effective_graph("minimal-linear", nil)
    end

    defp effective_graph(graph_id, revision) do
      metadata = if revision, do: %{"revision" => revision}, else: %{}
      authored = %{Graphs.minimal_linear() | id: graph_id, metadata: metadata}

      {:ok, graph, runtime_graph} =
        Graph.Compiler.compile_for_publication(authored, profile: :publish)

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
