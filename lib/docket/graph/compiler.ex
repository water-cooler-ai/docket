defmodule Docket.Graph.Compiler do
  @moduledoc """
  Public verification and runtime materialization entry point.

  The compiler API is in place, but runtime verification and lowering are still
  intentionally stubbed in this outline.
  """

  alias Docket.Graph
  alias Docket.Graph.Diagnostic

  @type opts :: keyword()

  @doc """
  Verifies that a graph is publishable/runnable.
  """
  @spec verify(Graph.t(), opts()) :: {:ok, Graph.t()} | {:error, Graph.t()}
  def verify(%Graph{} = graph, opts \\ []) do
    graph = %{graph | diagnostics: compiler_stub_diagnostics(graph, opts)}
    {:error, graph}
  end

  @doc """
  Compiles a public graph into an internal runtime graph.
  """
  @spec compile(Graph.t(), opts()) :: {:ok, term()} | {:error, Graph.t()}
  def compile(%Graph{} = graph, opts \\ []) do
    {:error, %{graph | diagnostics: compiler_stub_diagnostics(graph, opts)}}
  end

  defp compiler_stub_diagnostics(%Graph{} = graph, _opts) do
    [
      %Diagnostic{
        severity: :error,
        code: :compiler_not_implemented,
        message: "graph compiler verification and lowering are not implemented yet",
        path: [:compiler],
        public_id: graph.id
      }
    ]
  end
end
