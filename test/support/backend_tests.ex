defmodule Docket.BackendTests do
  @moduledoc """
  Shared black-box tests for `Docket.Backend` implementations.

  This module is test support, not shipped library API. A test module supplies
  a normal ExUnit setup callback that places a backend subject in the context:

      setup do
        {:ok,
         backend_test: %{
           backend: MyBackend,
           context: MyBackend.context(name: :test),
           namespace: "case-#{System.unique_integer([:positive])}",
           now: DateTime.utc_now() |> DateTime.truncate(:microsecond)
         }}
      end

      use Docket.BackendTests

  The cases construct all portable fixtures and expected results themselves.
  Backend-specific lifecycle, migrations, locking hooks, and cleanup remain in
  ordinary setup and backend-owned tests.
  """

  @type subject :: %{
          required(:backend) => module(),
          required(:context) => Docket.Backend.ctx(),
          required(:namespace) => nonempty_binary(),
          required(:now) => DateTime.t()
        }

  defmacro __using__(_opts) do
    quote do
      @moduletag :backend_contract

      setup %{backend_test: subject} do
        {:ok, backend_test: Docket.BackendTests.validate_subject!(subject)}
      end

      use Docket.BackendTests.Cases
    end
  end

  @spec validate_subject!(subject()) :: subject()
  def validate_subject!(subject) when is_map(subject) do
    case subject do
      %{backend: backend, context: _context, namespace: namespace, now: %DateTime{} = now}
      when is_atom(backend) and is_binary(namespace) and byte_size(namespace) > 0 ->
        cond do
          now.utc_offset != 0 or now.std_offset != 0 ->
            raise "backend test setup returned a non-UTC :now value: #{inspect(now)}"

          elem(now.microsecond, 1) != 6 ->
            raise "backend test setup returned :now without microsecond precision: #{inspect(now)}"

          true ->
            subject
        end

      _other ->
        raise "backend test setup must provide backend, context, non-empty namespace, and UTC now: " <>
                inspect(subject)
    end
  end

  def validate_subject!(other) do
    raise "backend test setup must return a subject map, got: #{inspect(other)}"
  end

  def await_task(_task, {:ok, result}), do: result
  def await_task(_task, {:exit, reason}), do: exit(reason)
  def await_task(task, nil), do: Task.await(task, 5_000)
end
