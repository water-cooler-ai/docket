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

  alias Docket.Graph

  alias Docket.Graph.Compiler.{
    Diagnostics,
    Lowering,
    NodeContracts,
    RuntimeValidation,
    Validation
  }

  alias Docket.Graph.Serializer
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
    {canonical, ingest_diagnostics} = ingest(graph)

    # Config schemas are fetched exactly once per compile; validation and
    # lowering must see the same result even when config_schema/0 callbacks
    # are stateful, and lowering must never re-enter user code.
    config_schemas = NodeContracts.config_schemas(canonical)
    diagnostics = ingest_diagnostics ++ Validation.run(canonical, config_schemas, opts)

    if Diagnostics.blocking?(diagnostics) do
      {:error, diagnostics}
    else
      runtime_graph = Lowering.run(canonical, config_schemas, opts)
      diagnostics = diagnostics ++ RuntimeValidation.run(runtime_graph, canonical)

      if Diagnostics.blocking?(diagnostics) do
        {:error, diagnostics}
      else
        {:ok, runtime_graph, diagnostics}
      end
    end
  end

  # Graphs are free-form in memory; the compiler is a serialization boundary.
  # Ingest canonicalizes the document through the wire format (atom keys and
  # values in open content become strings, exactly as storage would see them)
  # and validates/lowers the canonical form. Graphs that cannot cross the
  # boundary keep their in-memory shape so the validation passes can still
  # produce granular, path-bearing diagnostics next to the ingest error.
  #
  # The v1 wire format only represents schema_version 1 (dump stamps it), so
  # a graph claiming any other version is never canonicalized; validation
  # rejects it against the in-memory document instead.
  defp ingest(%Graph{schema_version: version} = graph) when version != 1 do
    {graph, []}
  end

  defp ingest(graph) do
    {Serializer.load!(Serializer.dump(graph, []), []), []}
  rescue
    exception in Docket.Graph.Error ->
      {graph, [ingest_diagnostic(exception)]}

    exception ->
      {graph,
       [
         Diagnostics.error(
           :non_durable_graph_value,
           "graph cannot be canonically serialized",
           metadata: %{error: inspect(exception)}
         )
       ]}
  end

  defp ingest_diagnostic(%Docket.Graph.Error{} = error) do
    code =
      case error.code do
        :non_durable_value -> :non_durable_graph_value
        code -> code
      end

    Diagnostics.error(code, error.message, metadata: %{details: error.details})
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
