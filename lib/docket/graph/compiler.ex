defmodule Docket.Graph.Compiler do
  @moduledoc """
  Public verification and runtime materialization entry point.

  The compiler is the only path from the public `Docket.Graph` document to the
  internal `Docket.Runtime.Graph`. `verify/2` and `compile/2` run the same
  pipeline - including lowering and lowered-result validation when no blocking
  diagnostics were found - and differ only in what they return: `verify/2`
  returns the annotated public graph, `compile/2` returns the runtime graph.

  Representable graph invalidity surfaces as `Docket.Graph.Diagnostic` values
  attached to the returned graph, never as exceptions. Stale diagnostics on
  the input graph are ignored; every call produces a fresh diagnostic list.

  ## Options

  - `:profile` - `:publish` (default) or `:run`; both apply identical rules
    in v1
  - `:max_supersteps` - runtime default cycle bound used when the graph does
    not declare a `"max_supersteps"` policy
  """

  alias Docket.{DurableCodec, Graph}

  alias Docket.Graph.Compiler.{
    Diagnostics,
    Lowering,
    NodeContracts,
    RuntimeValidation,
    Validation
  }

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
    case run_pipeline(graph, validate_opts!(opts), :authored) do
      {:ok, _effective_graph, _runtime_graph, diagnostics} ->
        {:ok, %{graph | diagnostics: diagnostics}}

      {:error, diagnostics} ->
        {:error, %{graph | diagnostics: diagnostics}}
    end
  end

  @doc """
  Compiles a public graph into an internal runtime graph.
  """
  @spec compile(Graph.t(), opts()) :: {:ok, Runtime.Graph.t()} | {:error, Graph.t()}
  def compile(%Graph{} = graph, opts \\ []) when is_list(opts) do
    case run_pipeline(graph, validate_opts!(opts), :authored) do
      {:ok, _effective_graph, runtime_graph, _diagnostics} -> {:ok, runtime_graph}
      {:error, diagnostics} -> {:error, %{graph | diagnostics: diagnostics}}
    end
  end

  @doc false
  @spec compile_for_publication(Graph.t(), opts()) ::
          {:ok, Graph.t(), Runtime.Graph.t()} | {:error, Graph.t()}
  def compile_for_publication(%Graph{} = graph, opts \\ []) when is_list(opts) do
    case run_pipeline(graph, validate_opts!(opts), :publication) do
      {:ok, effective_graph, runtime_graph, _diagnostics} ->
        {:ok, effective_graph, runtime_graph}

      {:error, diagnostics} ->
        {:error, %{graph | diagnostics: diagnostics}}
    end
  end

  @doc false
  @spec compile_effective_document(Graph.t(), opts()) ::
          {:ok, Runtime.Graph.t()} | {:error, Graph.t()}
  def compile_effective_document(%Graph{} = graph, opts \\ []) when is_list(opts) do
    case run_pipeline(graph, validate_opts!(opts), :effective_document) do
      {:ok, _effective_graph, runtime_graph, _diagnostics} -> {:ok, runtime_graph}
      {:error, diagnostics} -> {:error, %{graph | diagnostics: diagnostics}}
    end
  end

  defp run_pipeline(graph, opts, mode) do
    {canonical, ingest_diagnostics} = ingest(graph)

    if Diagnostics.blocking?(ingest_diagnostics) do
      {:error, ingest_diagnostics}
    else
      run_canonical_pipeline(canonical, ingest_diagnostics, opts, mode)
    end
  end

  defp run_canonical_pipeline(canonical, ingest_diagnostics, opts, mode) do
    # Config schemas are fetched exactly once per compile; validation and
    # lowering must see the same result even when config_schema/0 callbacks
    # are stateful, and lowering must never re-enter user code.
    config_schemas = NodeContracts.config_schemas(canonical)

    {effective, materialization_diagnostics} =
      materialize(canonical, config_schemas, mode != :effective_document)

    diagnostics = ingest_diagnostics ++ materialization_diagnostics

    if Diagnostics.blocking?(diagnostics) do
      {:error, diagnostics}
    else
      diagnostics = diagnostics ++ Validation.run(effective, config_schemas, opts)

      if Diagnostics.blocking?(diagnostics) do
        {:error, diagnostics}
      else
        lower(effective, opts, diagnostics)
      end
    end
  end

  defp lower(effective, opts, diagnostics) do
    runtime_graph = Lowering.run(effective, opts)
    diagnostics = diagnostics ++ RuntimeValidation.run(runtime_graph, effective)

    if Diagnostics.blocking?(diagnostics) do
      {:error, diagnostics}
    else
      {:ok, effective, runtime_graph, diagnostics}
    end
  end

  defp materialize(graph, _config_schemas, false), do: {graph, []}

  defp materialize(graph, config_schemas, true) do
    graph
    |> NodeContracts.materialize_defaults(config_schemas)
    |> ingest()
  end

  # Compilation normalizes only open runtime values through Docket.Wire, then
  # validates the direct durable term. Public JSON interchange is deliberately
  # not part of persistence, hashing, or compiler ingest.
  defp ingest(graph) do
    {graph, _bytes} = DurableCodec.encode_graph!(graph)
    {graph, []}
  rescue
    exception in Docket.Error ->
      {graph,
       [
         Diagnostics.error(
           :non_durable_graph_value,
           "graph contains state that cannot be durably encoded",
           metadata: %{error: exception.message, details: exception.details}
         )
       ]}
  end

  defp validate_opts!(opts) do
    profile = Keyword.get(opts, :profile, :publish)

    unless profile in @profiles do
      raise ArgumentError,
            "unsupported compiler profile #{inspect(profile)}; expected one of #{inspect(@profiles)}"
    end

    case Keyword.get(opts, :max_supersteps) do
      nil ->
        opts

      limit when is_integer(limit) and limit > 0 ->
        opts

      other ->
        raise ArgumentError,
              ":max_supersteps must be a positive integer, got #{inspect(other)}"
    end
  end
end
