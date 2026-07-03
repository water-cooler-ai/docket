defmodule Docket.Test.Case do
  @moduledoc """
  Case template for Docket tests with compiler assertion helpers.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import Docket.Test.Case

      alias Docket.Graph
      alias Docket.Graph.Compiler
      alias Docket.Test.Fixtures.Graphs
      alias Docket.Test.Fixtures.Nodes
    end
  end

  import ExUnit.Assertions

  @doc """
  Compiles a graph that must be valid, returning the runtime graph.
  """
  def compile!(graph, opts \\ []) do
    case Docket.Graph.Compiler.compile(graph, opts) do
      {:ok, runtime_graph} ->
        runtime_graph

      {:error, %Docket.Graph{} = failed} ->
        flunk("""
        expected graph #{inspect(graph.id)} to compile, got diagnostics:

        #{format_diagnostics(failed.diagnostics)}
        """)
    end
  end

  @doc """
  Returns the checkpoint types in emission order.
  """
  def checkpoint_types(checkpoints), do: Enum.map(checkpoints, & &1.type)

  @doc """
  Returns the event types across all checkpoints in emission order.
  """
  def event_types(checkpoints) do
    Enum.flat_map(checkpoints, fn checkpoint -> Enum.map(checkpoint.events, & &1.type) end)
  end

  @doc """
  Returns the committed value of a public state field from a run.
  """
  def field_value(run, field_id) do
    case Map.fetch(run.channels, "state:" <> field_id) do
      {:ok, state} -> state.value
      :error -> :unwritten
    end
  end

  @doc """
  Verifies a graph that must fail, returning its diagnostics.
  """
  def verify_error!(graph, opts \\ []) do
    case Docket.Graph.verify(graph, opts) do
      {:error, %Docket.Graph{} = failed} ->
        failed.diagnostics

      {:ok, %Docket.Graph{} = verified} ->
        flunk("""
        expected graph #{inspect(graph.id)} to fail verification, got :ok with:

        #{format_diagnostics(verified.diagnostics)}
        """)
    end
  end

  @doc """
  Asserts that a diagnostic with `code` exists and returns it.

  Options narrow the match and are asserted on the found diagnostic:

  - `:severity` (defaults to `:error`)
  - `:path` - exact public graph path
  - `:public_id` - public record ID
  - `:runtime_id` - runtime ID for lowering diagnostics
  """
  def assert_diagnostic(diagnostics, code, opts \\ [])

  def assert_diagnostic(%Docket.Graph{diagnostics: diagnostics}, code, opts) do
    assert_diagnostic(diagnostics, code, opts)
  end

  def assert_diagnostic(diagnostics, code, opts) when is_list(diagnostics) do
    severity = Keyword.get(opts, :severity, :error)

    matches =
      Enum.filter(diagnostics, fn diagnostic ->
        diagnostic.code == code and diagnostic.severity == severity and
          matches_opt?(diagnostic.path, Keyword.fetch(opts, :path)) and
          matches_opt?(diagnostic.public_id, Keyword.fetch(opts, :public_id)) and
          matches_opt?(diagnostic.runtime_id, Keyword.fetch(opts, :runtime_id))
      end)

    case matches do
      [diagnostic | _rest] ->
        assert is_binary(diagnostic.message) and diagnostic.message != ""
        diagnostic

      [] ->
        flunk("""
        no #{severity} diagnostic #{inspect(code)} matching #{inspect(opts)} in:

        #{format_diagnostics(diagnostics)}
        """)
    end
  end

  @doc """
  Asserts no diagnostic carries `:error` severity.
  """
  def refute_error_diagnostics(diagnostics) do
    errors = Enum.filter(diagnostics, &(&1.severity == :error))

    if errors != [] do
      flunk("expected no error diagnostics, got:\n\n#{format_diagnostics(errors)}")
    end

    diagnostics
  end

  defp matches_opt?(_value, :error), do: true
  defp matches_opt?(value, {:ok, expected}), do: value == expected

  defp format_diagnostics([]), do: "(no diagnostics)"

  defp format_diagnostics(diagnostics) do
    Enum.map_join(diagnostics, "\n", fn diagnostic ->
      "  [#{diagnostic.severity}] #{inspect(diagnostic.code)} " <>
        "path=#{inspect(diagnostic.path)} public_id=#{inspect(diagnostic.public_id)} " <>
        "- #{diagnostic.message}"
    end)
  end
end
