defmodule Docket.Graph.Compiler do
  @moduledoc """
  Public verification and runtime materialization entry point.

  The compiler is the only path from the public `Docket.Graph` document to the
  internal `Docket.Runtime.Graph`. `verify/2` and `compile/2` share the same
  validation rules; `compile/2` additionally lowers the graph and validates
  the lowered result when no blocking diagnostics were found.

  Representable graph invalidity surfaces as `Docket.Graph.Diagnostic` values
  attached to the returned graph, never as exceptions. Stale diagnostics on
  the input graph are ignored; every call produces a fresh diagnostic list.

  ## Options

  - `:profile` - `:publish` (default) or `:run`; both apply identical rules
    in v1
  - `:max_supersteps` - runtime default cycle bound used when the graph does
    not declare a `"max_supersteps"` policy
  """

  alias Docket.Graph
  alias Docket.Graph.Compiler.{Diagnostics, Lowering, RuntimeValidation, Validation}
  alias Docket.Runtime

  @type opts :: keyword()

  @profiles [:publish, :run]

  @doc """
  Verifies that a graph is publishable/runnable.

  Returns the graph with fresh diagnostics attached. `{:ok, graph}` may still
  carry warning or info diagnostics; `{:error, graph}` carries at least one
  error diagnostic.
  """
  @spec verify(Graph.t(), opts()) :: {:ok, Graph.t()} | {:error, Graph.t()}
  def verify(%Graph{} = graph, opts \\ []) when is_list(opts) do
    case run_pipeline(graph, validate_opts!(opts)) do
      {:ok, _runtime_graph, diagnostics} -> {:ok, %{graph | diagnostics: diagnostics}}
      {:error, diagnostics} -> {:error, %{graph | diagnostics: diagnostics}}
    end
  end

  @doc """
  Compiles a public graph into an internal runtime graph.
  """
  @spec compile(Graph.t(), opts()) :: {:ok, Runtime.Graph.t()} | {:error, Graph.t()}
  def compile(%Graph{} = graph, opts \\ []) when is_list(opts) do
    case run_pipeline(graph, validate_opts!(opts)) do
      {:ok, runtime_graph, _diagnostics} -> {:ok, runtime_graph}
      {:error, diagnostics} -> {:error, %{graph | diagnostics: diagnostics}}
    end
  end

  defp run_pipeline(graph, opts) do
    diagnostics = Validation.run(graph, opts)

    if Diagnostics.blocking?(diagnostics) do
      {:error, diagnostics}
    else
      runtime_graph = Lowering.run(graph, opts)
      diagnostics = diagnostics ++ RuntimeValidation.run(runtime_graph, graph)

      if Diagnostics.blocking?(diagnostics) do
        {:error, diagnostics}
      else
        {:ok, runtime_graph, diagnostics}
      end
    end
  end

  defp validate_opts!(opts) do
    profile = Keyword.get(opts, :profile, :publish)

    unless profile in @profiles do
      raise ArgumentError,
            "unsupported compiler profile #{inspect(profile)}; expected one of #{inspect(@profiles)}"
    end

    opts
  end
end
