defmodule Docket.Backend.Conformance.Harness do
  @moduledoc """
  Substrate lifecycle contract for `Docket.Backend.Conformance`.

  A harness starts and isolates a backend; it does not construct Docket graphs,
  runs, events, claims, or expected results. The conformance suite owns those
  portable fixtures and resolves every focused store from the returned backend.

  `setup_suite/0` is useful for expensive shared resources such as a database
  and migrations. `setup_case/2` must return a fresh namespace or isolated
  state for each test. Optional teardown callbacks run through ExUnit `on_exit`
  callbacks, including after a failed test.
  """

  alias Docket.Backend.Conformance.Instance

  @callback setup_suite() :: {:ok, suite_state :: term()}
  @callback setup_case(suite_state :: term(), ex_unit_context :: map()) ::
              {:ok, Instance.t()}
  @callback teardown_case(Instance.t()) :: term()
  @callback teardown_suite(suite_state :: term()) :: term()

  @optional_callbacks setup_suite: 0, teardown_case: 1, teardown_suite: 1
end
