if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.StorageTest do
    use ExUnit.Case, async: false

    @moduletag :postgres

    alias Docket.Postgres.{ClaimPolicy, GraphStore, Storage}
    alias Docket.Postgres.StorageTestRepo, as: TestRepo

    @migration_version 20_260_710_000_020

    defmodule InstallDocket do
      use Ecto.Migration

      def up, do: Docket.Postgres.Migration.up()
      def down, do: Docket.Postgres.Migration.down()
    end

    setup_all do
      config = TestRepo.config()
      _ = Ecto.Adapters.Postgres.storage_down(config)
      :ok = Ecto.Adapters.Postgres.storage_up(config)
      start_supervised!(TestRepo)
      :ok = Ecto.Migrator.up(TestRepo, @migration_version, InstallDocket, log: false)
      :ok
    end

    setup do
      TestRepo.delete_all(Docket.Postgres.Schemas.GraphVersion)
      :ok
    end

    test "accepts only prefixes safe for migration and raw claim SQL" do
      assert Storage.context!(%{repo: TestRepo, prefix: "docket_private"}) ==
               {TestRepo, "docket_private"}

      assert Storage.context!(%{repo: TestRepo, prefix: "select"}) == {TestRepo, "select"}

      for prefix <- ["Upper", "1leading", "has-dash", String.duplicate("a", 64), ""] do
        assert_raise ArgumentError, fn -> Storage.context!(%{repo: TestRepo, prefix: prefix}) end
      end
    end

    test "commits {:ok, value} and supplies one normalized store context" do
      document = document("committed")
      graph_hash = hash(document)

      assert {:ok, :committed} =
               Docket.Postgres.transaction(TestRepo, fn ctx ->
                 assert ctx == %{repo: TestRepo, prefix: nil}

                 assert :ok =
                          GraphStore.save_graph(
                            ctx,
                            :tenantless,
                            "committed",
                            graph_hash,
                            document
                          )

                 {:ok, :committed}
               end)

      assert {:ok, ^document} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "committed", graph_hash)
    end

    test "outer and nested transactions preserve the exact resolved ClaimPolicy value" do
      root = %{repo: TestRepo, prefix: nil}

      claim_policy =
        ClaimPolicy.new(
          [
            implementation: Docket.Test.RelayOptionsClaimPolicy,
            relayed_option: 4
          ],
          root
        )

      root = Map.put(root, :claim_policy, claim_policy)

      assert {:ok, :preserved} =
               Docket.Postgres.transaction(root, fn outer_ctx ->
                 assert outer_ctx.claim_policy === claim_policy

                 assert {:ok, :nested} =
                          Docket.Postgres.transaction(outer_ctx, fn inner_ctx ->
                            assert inner_ctx.claim_policy === claim_policy
                            {:ok, :nested}
                          end)

                 {:ok, :preserved}
               end)
    end

    test "error results, exceptions, throws, and invalid returns all roll back" do
      error_doc = document("error")
      error_hash = hash(error_doc)

      assert {:error, :stop} =
               Docket.Postgres.transaction(TestRepo, fn ctx ->
                 assert :ok =
                          GraphStore.save_graph(ctx, :tenantless, "error", error_hash, error_doc)

                 {:error, :stop}
               end)

      assert {:error, :not_found} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "error", error_hash)

      raised_doc = document("raised")
      raised_hash = hash(raised_doc)

      assert_raise RuntimeError, "boom", fn ->
        Docket.Postgres.transaction(TestRepo, fn ctx ->
          assert :ok =
                   GraphStore.save_graph(ctx, :tenantless, "raised", raised_hash, raised_doc)

          raise "boom"
        end)
      end

      assert {:error, :not_found} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "raised", raised_hash)

      thrown_doc = document("thrown")
      thrown_hash = hash(thrown_doc)

      assert catch_throw(
               Docket.Postgres.transaction(TestRepo, fn ctx ->
                 assert :ok =
                          GraphStore.save_graph(
                            ctx,
                            :tenantless,
                            "thrown",
                            thrown_hash,
                            thrown_doc
                          )

                 throw(:halt)
               end)
             ) == :halt

      assert {:error, :not_found} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "thrown", thrown_hash)

      invalid_doc = document("invalid-return")
      invalid_hash = hash(invalid_doc)

      assert_raise ArgumentError, ~r/transaction callback must return/, fn ->
        Docket.Postgres.transaction(TestRepo, fn ctx ->
          assert :ok =
                   GraphStore.save_graph(
                     ctx,
                     :tenantless,
                     "invalid-return",
                     invalid_hash,
                     invalid_doc
                   )

          :not_a_result
        end)
      end

      assert {:error, :not_found} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "invalid-return", invalid_hash)
    end

    test "nested transactions join and a swallowed nested rollback still aborts the outer write" do
      outer_doc = document("outer")
      outer_hash = hash(outer_doc)
      inner_doc = document("inner")
      inner_hash = hash(inner_doc)

      assert {:error, :rollback} =
               Docket.Postgres.transaction(TestRepo, fn outer_ctx ->
                 assert :ok =
                          GraphStore.save_graph(
                            outer_ctx,
                            :tenantless,
                            "outer",
                            outer_hash,
                            outer_doc
                          )

                 assert {:error, :inner_stop} =
                          Docket.Postgres.transaction(outer_ctx, fn inner_ctx ->
                            assert inner_ctx == outer_ctx

                            assert :ok =
                                     GraphStore.save_graph(
                                       inner_ctx,
                                       :tenantless,
                                       "inner",
                                       inner_hash,
                                       inner_doc
                                     )

                            {:error, :inner_stop}
                          end)

                 {:ok, :attempted_swallow}
               end)

      assert {:error, :not_found} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "outer", outer_hash)

      assert {:error, :not_found} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "inner", inner_hash)
    end

    test "nested success commits once and preserves the caller's result shape" do
      document = document("nested-success")
      graph_hash = hash(document)

      assert {:ok, {:outer, :inner}} =
               Docket.Postgres.transaction(%{repo: TestRepo}, fn outer_ctx ->
                 assert {:ok, :inner} =
                          Docket.Postgres.transaction(outer_ctx, fn inner_ctx ->
                            assert inner_ctx == %{repo: TestRepo, prefix: nil}

                            assert :ok =
                                     GraphStore.save_graph(
                                       inner_ctx,
                                       :tenantless,
                                       "nested-success",
                                       graph_hash,
                                       document
                                     )

                            {:ok, :inner}
                          end)

                 {:ok, {:outer, :inner}}
               end)

      assert {:ok, ^document} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "nested-success", graph_hash)
    end

    test "writes stay invisible to another connection until the transaction commits" do
      parent = self()
      document = document("isolated")
      graph_hash = hash(document)

      transaction =
        Task.async(fn ->
          Docket.Postgres.transaction(TestRepo, fn ctx ->
            assert :ok =
                     GraphStore.save_graph(
                       ctx,
                       :tenantless,
                       "isolated",
                       graph_hash,
                       document
                     )

            send(parent, {:inserted, self()})

            receive do
              :commit -> {:ok, :committed}
            end
          end)
        end)

      assert_receive {:inserted, transaction_pid}, 5_000

      assert {:error, :not_found} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "isolated", graph_hash)

      send(transaction_pid, :commit)
      assert Task.await(transaction, 5_000) == {:ok, :committed}

      assert {:ok, ^document} =
               GraphStore.fetch_graph(TestRepo, :tenantless, "isolated", graph_hash)
    end

    defp document(id), do: Docket.Graph.new!(id: id)

    defp hash(graph) do
      graph
      |> then(&Docket.DurableCodec.encode!(:graph, &1))
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)
    end
  end
end
