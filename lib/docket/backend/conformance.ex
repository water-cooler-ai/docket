defmodule Docket.Backend.Conformance do
  @moduledoc """
  Reusable ExUnit conformance profile for `Docket.Backend` implementations.

  The suite is an executable specification of the portable transaction and
  focused-store contract. It is intentionally black-box: every capability is
  resolved from the backend bundle, every operation uses the root or yielded
  opaque context, and no substrate implementation details are inspected.

  A backend package invokes the same Docket-owned cases used by the built-in
  memory and PostgreSQL backends:

      defmodule MyBackendConformanceTest do
        use ExUnit.Case, async: false

        use Docket.Backend.Conformance,
          harness: MyApp.BackendConformanceHarness
      end

  The harness must implement `Docket.Backend.Conformance.Harness`. The profile
  covers publicly inducible behavior. Physical retention pruning, deliberately
  corrupted storage, query/locking mechanics, and deterministic substrate race
  hooks remain implementation-specific tests.
  """

  alias Docket.Backend.Conformance.Instance

  defmacro __using__(opts) do
    harness = Keyword.fetch!(opts, :harness)

    quote bind_quoted: [harness: harness] do
      @docket_backend_conformance_harness harness
      @moduletag :backend_conformance

      setup_all context do
        suite_state =
          Docket.Backend.Conformance.setup_suite!(@docket_backend_conformance_harness)

        on_exit(fn ->
          Docket.Backend.Conformance.teardown_suite(
            @docket_backend_conformance_harness,
            suite_state
          )
        end)

        {:ok, docket_backend_conformance_suite: suite_state, conformance_module: context.module}
      end

      setup context do
        instance =
          Docket.Backend.Conformance.setup_case!(
            @docket_backend_conformance_harness,
            context.docket_backend_conformance_suite,
            context
          )

        on_exit(fn ->
          Docket.Backend.Conformance.teardown_case(
            @docket_backend_conformance_harness,
            instance
          )
        end)

        instance =
          Docket.Backend.Conformance.validate_instance!(
            @docket_backend_conformance_harness,
            instance
          )

        {:ok, docket_backend_conformance: instance}
      end

      use Docket.Backend.Conformance.Cases
    end
  end

  @doc false
  def setup_suite!(harness) do
    Code.ensure_loaded!(harness)

    result =
      if function_exported?(harness, :setup_suite, 0),
        do: harness.setup_suite(),
        else: {:ok, nil}

    case result do
      {:ok, state} ->
        state

      other ->
        raise "#{inspect(harness)}.setup_suite/0 must return {:ok, state}, got: #{inspect(other)}"
    end
  end

  @doc false
  def setup_case!(harness, suite_state, context) do
    Code.ensure_loaded!(harness)

    case harness.setup_case(suite_state, context) do
      {:ok, %Instance{} = instance} ->
        instance

      other ->
        raise "#{inspect(harness)}.setup_case/2 must return {:ok, %Instance{}}, got: #{inspect(other)}"
    end
  end

  @doc false
  def teardown_case(harness, instance) do
    Code.ensure_loaded!(harness)
    if function_exported?(harness, :teardown_case, 1), do: harness.teardown_case(instance)
  end

  @doc false
  def teardown_suite(harness, suite_state) do
    Code.ensure_loaded!(harness)
    if function_exported?(harness, :teardown_suite, 1), do: harness.teardown_suite(suite_state)
  end

  @doc false
  def await_task(_task, {:ok, result}), do: result

  def await_task(_task, {:exit, reason}), do: exit(reason)

  def await_task(task, nil), do: Task.await(task, 5_000)

  @doc false
  def validate_instance!(harness, %Instance{} = instance) do
    cond do
      not is_atom(instance.backend) ->
        raise "#{inspect(harness)} returned a non-module backend: #{inspect(instance.backend)}"

      not (is_binary(instance.namespace) and byte_size(instance.namespace) > 0) ->
        raise "#{inspect(harness)} returned an empty conformance namespace"

      not is_struct(instance.now, DateTime) ->
        raise "#{inspect(harness)} returned a non-DateTime :now value: #{inspect(instance.now)}"

      instance.now.utc_offset != 0 or instance.now.std_offset != 0 ->
        raise "#{inspect(harness)} returned a non-UTC :now value: #{inspect(instance.now)}"

      elem(instance.now.microsecond, 1) != 6 ->
        raise "#{inspect(harness)} returned :now without microsecond precision: #{inspect(instance.now)}"

      true ->
        instance
    end
  end
end
