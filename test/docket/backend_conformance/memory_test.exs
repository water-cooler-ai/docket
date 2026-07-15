defmodule Docket.BackendConformance.MemoryTest do
  use ExUnit.Case, async: true

  use Docket.Backend.Conformance,
    harness: Docket.Test.BackendConformance.MemoryHarness
end
