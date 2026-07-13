defmodule Docket.GraphVersionSummaryTest do
  use ExUnit.Case, async: true

  alias Docket.{GraphRef, GraphVersionPage, GraphVersionSummary}

  @published_at ~U[2026-07-12 10:00:00.000000Z]

  test "builds scoped-neutral metadata and its stable cursor" do
    ref = %GraphRef{graph_id: "workflow", graph_hash: "bbbb"}
    summary = GraphVersionSummary.new!(ref: ref, published_at: @published_at)

    assert %GraphVersionSummary{ref: ^ref, published_at: @published_at} = summary
    assert GraphVersionSummary.cursor(summary) == {@published_at, "bbbb"}
    refute Map.has_key?(summary, :tenant_id)
    refute Map.has_key?(summary, :scope_key)
  end

  test "rejects incomplete and malformed metadata" do
    for fields <- [
          %{published_at: @published_at},
          %{ref: %GraphRef{graph_id: "", graph_hash: "hash"}, published_at: @published_at},
          %{ref: %GraphRef{graph_id: "graph", graph_hash: ""}, published_at: @published_at},
          %{ref: %GraphRef{graph_id: "graph", graph_hash: "hash"}, published_at: "today"}
        ] do
      assert_raise ArgumentError, fn -> GraphVersionSummary.new!(fields) end
    end
  end

  test "page trims lookahead and orders equal timestamps by descending hash" do
    candidates = [summary("cccc"), summary("bbbb"), summary("aaaa")]

    assert %GraphVersionPage{
             versions: [
               %GraphVersionSummary{ref: %GraphRef{graph_hash: "cccc"}},
               %GraphVersionSummary{ref: %GraphRef{graph_hash: "bbbb"}}
             ],
             next_before: {@published_at, "bbbb"},
             has_more?: true
           } = GraphVersionPage.new(candidates, nil, 2)
  end

  test "publication time is the primary order and before is exclusive" do
    older_at = DateTime.add(@published_at, -1, :second)
    before = {DateTime.add(@published_at, 1, :second), "aaaa"}

    candidates = [summary("aaaa"), summary("zzzz", "workflow", older_at)]

    assert %GraphVersionPage{
             versions: [
               %GraphVersionSummary{ref: %GraphRef{graph_hash: "aaaa"}},
               %GraphVersionSummary{ref: %GraphRef{graph_hash: "zzzz"}}
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

  test "page rejects malformed cursors, mixed graphs, invalid order, and cursor overlap" do
    assert_raise ArgumentError, ~r/cursor/, fn ->
      GraphVersionPage.new([], {@published_at, ""}, 2)
    end

    assert_raise ArgumentError, ~r/one graph ID/, fn ->
      GraphVersionPage.new([summary("bbbb"), summary("aaaa", "other")], nil, 2)
    end

    assert_raise ArgumentError, ~r/strictly newest-first/, fn ->
      GraphVersionPage.new([summary("aaaa"), summary("bbbb")], nil, 2)
    end

    assert_raise ArgumentError, ~r/strictly older/, fn ->
      GraphVersionPage.new([summary("bbbb")], {@published_at, "aaaa"}, 2)
    end
  end

  defp summary(hash, graph_id \\ "workflow", published_at \\ @published_at) do
    GraphVersionSummary.new!(
      ref: %GraphRef{graph_id: graph_id, graph_hash: hash},
      published_at: published_at
    )
  end
end
