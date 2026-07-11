if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.GraphCacheTest do
    use ExUnit.Case, async: false

    alias Docket.Graph.Compiler
    alias Docket.Postgres.GraphCache
    alias Docket.Test.Fixtures.Graphs

    @mutable_module Docket.Postgres.GraphCacheTest.MutableNode
    @late_module Docket.Postgres.GraphCacheTest.LateModule

    setup do
      on_exit(&GraphCache.clear/0)
      :ok
    end

    test "put_compiled then fetch returns the cached runtime graph" do
      rtg = compiled_minimal()
      assert :ok = GraphCache.put_compiled("minimal-linear", "hash-1", rtg)
      assert GraphCache.fetch("minimal-linear", "hash-1") == {:ok, rtg}
    end

    test "fetch of an unknown graph version is a miss" do
      assert GraphCache.fetch("unknown", "hash-1") == :miss
    end

    test "an entry written under another generation is erased on read" do
      rtg = compiled_minimal()

      assert :ok =
               GraphCache.put_compiled("minimal-linear", "hash-1", rtg,
                 generation: fn -> :gen_a end
               )

      assert GraphCache.fetch("minimal-linear", "hash-1", generation: fn -> :gen_b end) == :miss
      assert GraphCache.fetch("minimal-linear", "hash-1", generation: fn -> :gen_a end) == :miss
    end

    test "put_incompatible with a graph source records the reason for fetch" do
      graph = Graphs.minimal_linear()

      assert :ok =
               GraphCache.put_incompatible("minimal-linear", "hash-1", graph, :schema_mismatch)

      assert GraphCache.fetch("minimal-linear", "hash-1") == {:incompatible, :schema_mismatch}
    end

    test "an undecodable entry expires after its TTL" do
      assert :ok =
               GraphCache.put_incompatible("garbled", "hash-1", :undecodable, :corrupt_graph,
                 undecodable_ttl_ms: 0
               )

      assert GraphCache.fetch("garbled", "hash-1") == :miss
    end

    test "an undecodable entry within its TTL reports the recorded reason" do
      assert :ok =
               GraphCache.put_incompatible("garbled", "hash-1", :undecodable, :corrupt_graph,
                 undecodable_ttl_ms: 60_000
               )

      assert GraphCache.fetch("garbled", "hash-1") == {:incompatible, :corrupt_graph}
    end

    test "a redefined node implementation module invalidates the cached graph" do
      define_node_module(@mutable_module, "v1")
      {:ok, rtg} = Compiler.compile(graph_with_module(@mutable_module))

      assert :ok = GraphCache.put_compiled("mutable", "hash-1", rtg)
      assert GraphCache.fetch("mutable", "hash-1") == {:ok, rtg}

      define_node_module(@mutable_module, "v2")

      assert GraphCache.fetch("mutable", "hash-1") == :miss
    end

    test "a missing-module incompatibility self-heals when the module appears" do
      graph = graph_with_module(@late_module)

      assert :ok =
               GraphCache.put_incompatible("late", "hash-1", graph, :node_module_not_loaded)

      assert GraphCache.fetch("late", "hash-1") == {:incompatible, :node_module_not_loaded}

      define_node_module(@late_module, "arrived")

      assert GraphCache.fetch("late", "hash-1") == :miss
    end

    defp compiled_minimal do
      {:ok, rtg} = Compiler.compile(Graphs.minimal_linear())
      rtg
    end

    defp graph_with_module(module) do
      graph = Graphs.minimal_linear()
      put_in(graph.nodes["copy"].implementation.module, module)
    end

    defp define_node_module(module, marker) do
      Code.compiler_options(ignore_module_conflict: true)

      Code.compile_string("""
      defmodule #{inspect(module)} do
        @behaviour Docket.Node

        @impl true
        def config_schema do
          Docket.Schema.object(%{
            "from" => Docket.Schema.string(required: true),
            "to" => Docket.Schema.string(required: true)
          })
        end

        @impl true
        def call(_state, config, _context) do
          {:ok, %{config["to"] => #{inspect(marker)}}}
        end
      end
      """)

      :code.purge(module)
      :ok
    after
      Code.compiler_options(ignore_module_conflict: false)
    end
  end
end
