defmodule DocketTest do
  use ExUnit.Case, async: true

  doctest Docket

  test "loads the root module" do
    assert Code.ensure_loaded?(Docket)
  end
end
