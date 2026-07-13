defmodule Docket.RunSummaryTest do
  use ExUnit.Case, async: true

  @started_at ~U[2026-07-12 10:00:00.000000Z]
  @updated_at ~U[2026-07-12 10:01:00.000000Z]

  test "builds a lightweight summary and exact graph reference" do
    summary = summary("run-1")

    assert %Docket.RunSummary{
             id: "run-1",
             tenant_id: "tenant",
             graph_id: "graph",
             graph_hash: "hash",
             status: :running,
             checkpoint_seq: 3
           } = summary

    assert Docket.RunSummary.graph_ref(summary) ==
             %Docket.GraphRef{graph_id: "graph", graph_hash: "hash"}

    refute Map.has_key?(summary, :claim_token)
  end

  test "rejects incomplete and malformed summaries" do
    assert_raise ArgumentError, fn ->
      summary_fields("run-1") |> Map.delete(:tenant_id) |> Docket.RunSummary.new!()
    end

    for overrides <- [
          %{id: ""},
          %{tenant_id: ""},
          %{status: :created},
          %{step: -1},
          %{checkpoint_seq: 0},
          %{started_at: "today"},
          %{finished_at: "later"}
        ] do
      assert_raise ArgumentError, fn ->
        "run-1" |> summary_fields() |> Map.merge(overrides) |> Docket.RunSummary.new!()
      end
    end
  end

  test "page trims a lookahead row and preserves the cursor for an empty page" do
    candidates = [summary("c"), summary("b"), summary("a")]

    assert %Docket.RunPage{
             runs: [%Docket.RunSummary{id: "c"}, %Docket.RunSummary{id: "b"}],
             next_before: {@started_at, "b"},
             has_more?: true
           } = Docket.RunPage.new(candidates, nil, 2)

    before = {@started_at, "a"}

    assert %Docket.RunPage{runs: [], next_before: ^before, has_more?: false} =
             Docket.RunPage.new([], before, 2)
  end

  defp summary(id), do: id |> summary_fields() |> Docket.RunSummary.new!()

  defp summary_fields(id) do
    %{
      id: id,
      tenant_id: "tenant",
      graph_id: "graph",
      graph_hash: "hash",
      status: :running,
      step: 2,
      checkpoint_seq: 3,
      started_at: @started_at,
      updated_at: @updated_at,
      finished_at: nil
    }
  end
end
