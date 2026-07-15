defmodule Docket.Backend.GraphStoreTest do
  use ExUnit.Case, async: true

  test "the graph capability exposes scoped publication and version reads" do
    assert Docket.Backend.GraphStore.behaviour_info(:callbacks) |> Enum.sort() ==
             [
               fetch_graph: 4,
               fetch_latest_graph_ref: 3,
               list_graph_versions: 4,
               save_graph: 5
             ]
  end
end
