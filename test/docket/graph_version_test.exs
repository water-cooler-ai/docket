defmodule Docket.GraphVersionTest do
  use ExUnit.Case, async: true

  alias Docket.{GraphRef, GraphVersion, GraphVersionPage}

  @published_at ~U[2026-07-12 10:00:00.000000Z]

  test "is lightweight, scope-neutral metadata" do
    ref = %GraphRef{graph_id: "workflow", graph_hash: "bbbb"}
    version = %GraphVersion{ref: ref, published_at: @published_at}

    assert %GraphVersion{ref: ^ref, published_at: @published_at} = version
    refute Map.has_key?(version, :tenant_id)
    refute Map.has_key?(version, :scope_key)
    refute function_exported?(GraphVersion, :new!, 1)
  end

  test "page trims lookahead and keeps backend order" do
    candidates = [version("cccc"), version("bbbb"), version("aaaa")]

    assert %GraphVersionPage{
             versions: [
               %GraphVersion{ref: %GraphRef{graph_hash: "cccc"}},
               %GraphVersion{ref: %GraphRef{graph_hash: "bbbb"}}
             ],
             next_before: {@published_at, "bbbb"},
             has_more?: true
           } = GraphVersionPage.new(candidates, nil, 2)
  end

  test "page cursor uses publication time and graph hash" do
    older_at = DateTime.add(@published_at, -1, :second)
    before = {DateTime.add(@published_at, 1, :second), "aaaa"}

    candidates = [version("aaaa"), version("zzzz", "workflow", older_at)]

    assert %GraphVersionPage{
             versions: [
               %GraphVersion{ref: %GraphRef{graph_hash: "aaaa"}},
               %GraphVersion{ref: %GraphRef{graph_hash: "zzzz"}}
             ],
             next_before: {^older_at, "zzzz"},
             has_more?: false
           } = GraphVersionPage.new(candidates, before, 2)
  end

  test "empty page preserves its exclusive cursor" do
    before = {@published_at, "bbbb"}

    assert %GraphVersionPage{versions: [], next_before: ^before, has_more?: false} =
             GraphVersionPage.new([], before, 2)
  end

  defp version(hash, graph_id \\ "workflow", published_at \\ @published_at) do
    %GraphVersion{
      ref: %GraphRef{graph_id: graph_id, graph_hash: hash},
      published_at: published_at
    }
  end
end
