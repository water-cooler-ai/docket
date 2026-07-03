defmodule Docket.Graph do
  @moduledoc """
  Canonical, editable graph document used by host applications.

  A `Docket.Graph` is the public build-time representation. Editors and
  compilers update this value directly; runtime lowering happens later through
  `Docket.Graph.Compiler`.

  Non-bang editing functions return `{:ok, graph}` or
  `{:error, %Docket.Graph.Error{}}`, which is useful for event-by-event graph
  editors. Bang editing functions return the graph or raise, which keeps
  pipe-oriented graph construction ergonomic.

  Functions that accept record attributes generally accept a keyword list, a
  map, or the matching `Docket.Graph.*` struct. Keyword lists are convenient for
  hand-written Elixir construction, maps are convenient for application/UI
  payloads, and structs let compilers or importers pass already-normalized graph
  records back through the same editing API.
  """

  alias Docket.Graph.{Diagnostic, Edge, Error, Field, Node, Output, Serializer}

  @id_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_-]*$/
  @start_id "$start"
  @finish_id "$finish"
  @schema_version 1

  defstruct [
    :id,
    :name,
    :description,
    schema_version: @schema_version,
    fields: %{},
    inputs: %{},
    outputs: %{},
    nodes: %{},
    edges: %{},
    policies: %{},
    metadata: %{},
    diagnostics: []
  ]

  @type id :: String.t()
  @type t :: %__MODULE__{
          id: id(),
          name: String.t() | nil,
          description: String.t() | nil,
          schema_version: pos_integer(),
          fields: %{optional(id()) => Field.t()},
          inputs: %{optional(id()) => Field.t()},
          outputs: %{optional(id()) => Output.t()},
          nodes: %{optional(id()) => Node.t()},
          edges: %{optional(id()) => Edge.t()},
          policies: map(),
          metadata: map(),
          diagnostics: [Diagnostic.t()]
        }
  @type edit_result :: {:ok, t()} | {:error, Error.t()}

  @doc """
  Creates a new editable graph document.

  Options must be a keyword list. Supported options are:

  - `:id` - graph ID; generated when omitted
  - `:name` - optional display name
  - `:description` - optional description
  - `:schema_version` - Docket graph document schema version
  - `:metadata` - application metadata map
  - `:policies` - graph policy map
  """
  @spec new(keyword()) :: edit_result()
  def new(opts \\ []) do
    edit_result(fn -> new!(opts) end)
  end

  @doc """
  Creates a new editable graph document, raising on invalid options.
  """
  @spec new!(keyword()) :: t()
  def new!(opts \\ []) do
    opts = opts_to_keyword!(opts)
    id = Keyword.get(opts, :id, generate_id(:graph, opts))
    assert_public_id!(id, :graph_id)

    graph = %__MODULE__{
      id: id,
      name: Keyword.get(opts, :name),
      description: Keyword.get(opts, :description),
      schema_version: Keyword.get(opts, :schema_version, @schema_version),
      metadata: Keyword.get(opts, :metadata, %{}),
      policies: Keyword.get(opts, :policies, %{})
    }

    finalize_edit(graph, opts)
  end

  @doc """
  Adds or replaces an input field.

  `attrs` may be a keyword list, a map, or a `Docket.Graph.Field` struct. The
  explicit `id` argument is used as the stored field ID, and the stored field
  kind is forced to `:input`.
  """
  @spec put_input(t(), id(), keyword() | map() | Field.t(), keyword()) :: edit_result()
  def put_input(graph, id, attrs, opts \\ []) do
    edit_result(fn -> put_input!(graph, id, attrs, opts) end)
  end

  @doc """
  Adds or replaces an input field, raising on malformed arguments.
  """
  @spec put_input!(t(), id(), keyword() | map() | Field.t(), keyword()) :: t()
  def put_input!(graph, id, attrs, opts \\ [])

  def put_input!(%__MODULE__{} = graph, id, attrs, opts) do
    assert_public_id!(id, :input_id)

    field =
      attrs
      |> attrs_to_map()
      |> Map.put(:id, id)
      |> Map.put(:kind, :input)
      |> field_from_attrs()

    graph
    |> put_in([Access.key!(:inputs), id], field)
    |> update_in([Access.key!(:fields)], &Map.delete(&1, id))
    |> finalize_edit(opts)
  end

  def put_input!(graph, _id, _attrs, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Adds or replaces a state field.

  `attrs` may be a keyword list, a map, or a `Docket.Graph.Field` struct. The
  explicit `id` argument is used as the stored field ID, and the stored field
  kind is forced to `:state`.
  """
  @spec put_field(t(), id(), keyword() | map() | Field.t(), keyword()) :: edit_result()
  def put_field(graph, id, attrs, opts \\ []) do
    edit_result(fn -> put_field!(graph, id, attrs, opts) end)
  end

  @doc """
  Adds or replaces a state field, raising on malformed arguments.
  """
  @spec put_field!(t(), id(), keyword() | map() | Field.t(), keyword()) :: t()
  def put_field!(graph, id, attrs, opts \\ [])

  def put_field!(%__MODULE__{} = graph, id, attrs, opts) do
    assert_public_id!(id, :field_id)

    field =
      attrs
      |> attrs_to_map()
      |> Map.put(:id, id)
      |> Map.put(:kind, :state)
      |> field_from_attrs()

    graph
    |> put_in([Access.key!(:fields), id], field)
    |> update_in([Access.key!(:inputs)], &Map.delete(&1, id))
    |> finalize_edit(opts)
  end

  def put_field!(graph, _id, _attrs, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Adds or replaces an output projection.

  `attrs` may be a keyword list, a map, or a `Docket.Graph.Output` struct. The
  explicit `id` argument is used as the stored output ID. If `:source` is omitted,
  it defaults to the output ID.
  """
  @spec put_output(t(), id(), keyword() | map() | Output.t(), keyword()) :: edit_result()
  def put_output(graph, id, attrs, opts \\ []) do
    edit_result(fn -> put_output!(graph, id, attrs, opts) end)
  end

  @doc """
  Adds or replaces an output projection, raising on malformed arguments.
  """
  @spec put_output!(t(), id(), keyword() | map() | Output.t(), keyword()) :: t()
  def put_output!(graph, id, attrs, opts \\ [])

  def put_output!(%__MODULE__{} = graph, id, attrs, opts) do
    assert_public_id!(id, :output_id)

    attrs =
      attrs
      |> attrs_to_map()
      |> Map.put(:id, id)
      |> Map.put_new(:source, id)

    output = struct(Output, attrs)

    graph
    |> put_in([Access.key!(:outputs), id], output)
    |> finalize_edit(opts)
  end

  def put_output!(graph, _id, _attrs, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Stores a graph-level policy value.
  """
  @spec policy(t(), binary() | atom(), term(), keyword()) :: edit_result()
  def policy(graph, key, value, opts \\ []) do
    edit_result(fn -> policy!(graph, key, value, opts) end)
  end

  @doc """
  Stores a graph-level policy value, raising on malformed arguments.

  Content is stored as given. When the graph crosses the serialization
  boundary (`to_map/2`, `hash/2`), atom keys and values are canonicalized to
  strings and terms with no JSON representation are rejected. Keys starting
  with `"$"` are reserved for the wire format.
  """
  @spec policy!(t(), binary(), term(), keyword()) :: t()
  def policy!(graph, key, value, opts \\ [])

  def policy!(%__MODULE__{} = graph, key, value, opts) do
    graph
    |> Map.put(:policies, Map.put(graph.policies, key, value))
    |> finalize_edit(opts)
  end

  def policy!(graph, _key, _value, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Stores graph-level application metadata.
  """
  @spec metadata(t(), binary() | atom(), term(), keyword()) :: edit_result()
  def metadata(graph, key, value, opts \\ []) do
    edit_result(fn -> metadata!(graph, key, value, opts) end)
  end

  @doc """
  Stores graph-level application metadata, raising on malformed arguments.

  Content is stored as given. When the graph crosses the serialization
  boundary (`to_map/2`, `hash/2`), atom keys and values are canonicalized to
  strings and terms with no JSON representation are rejected. Keys starting
  with `"$"` are reserved for the wire format.
  """
  @spec metadata!(t(), binary(), term(), keyword()) :: t()
  def metadata!(graph, key, value, opts \\ [])

  def metadata!(%__MODULE__{} = graph, key, value, opts) do
    graph
    |> Map.put(:metadata, Map.put(graph.metadata, key, value))
    |> finalize_edit(opts)
  end

  def metadata!(graph, _key, _value, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Reads diagnostics stored on the graph document.
  """
  @spec diagnostics(t(), keyword()) :: [Diagnostic.t()]
  def diagnostics(%__MODULE__{} = graph, _opts \\ []) do
    graph.diagnostics
  end

  @doc """
  Dumps the graph to a plain, JSON-safe map (the v1 wire format).

  Keys are strings and values are durable JSON-safe terms. Compiler diagnostics
  are transient and are never serialized.

  Graphs are free-form in memory; this is the boundary where content is
  canonicalized. Open content (metadata, policies, config, defaults, enum
  values, guard arguments, branch groups) is coerced the way `Jason` would
  encode it: atom keys and atom values become strings, silently. Terms with no
  JSON representation - tuples, keyword lists, pids, refs, functions, structs -
  raise `Docket.Graph.Error` (`:non_durable_value` and friends).

  The graph hash is computed from this document, so it is stable across
  storage round trips: `hash(from_map!(to_map(graph))) == hash(graph)` for any
  dumpable graph. Graphs whose open content is already canonical (string keys
  and values) also round-trip on struct equality:
  `from_map!(to_map(graph)) == graph`.
  """
  @spec to_map(t(), keyword()) :: map()
  def to_map(%__MODULE__{} = graph, opts \\ []) do
    Serializer.dump(graph, opts)
  end

  @doc """
  Loads a graph from a v1 wire map.
  """
  @spec from_map(map(), keyword()) :: edit_result()
  def from_map(map, opts \\ []) do
    edit_result(fn -> Serializer.load!(map, opts) end)
  end

  @doc """
  Loads a graph from a v1 wire map, raising `Docket.Graph.Error` on invalid input.
  """
  @spec from_map!(map(), keyword()) :: t()
  def from_map!(map, opts \\ []) do
    Serializer.load!(map, opts)
  end

  @doc """
  Computes the SHA-256 graph hash used to bind runs to graph content.

  The hash is a SHA-256 digest over the canonical JSON encoding of
  `to_map/1`. It excludes host-owned versioning and compiler diagnostics.
  """
  @spec hash(t(), keyword()) :: String.t()
  def hash(%__MODULE__{} = graph, opts \\ []) do
    Serializer.hash(graph, opts)
  end

  @doc """
  Verifies the graph and returns it with compiler diagnostics attached.
  """
  @spec verify(t(), keyword()) :: {:ok, t()} | {:error, t()}
  def verify(%__MODULE__{} = graph, opts \\ []) do
    Docket.Graph.Compiler.verify(graph, opts)
  end

  @doc """
  Adds or replaces a node by ID.

  `attrs` may be a keyword list, a map, or a `Docket.Graph.Node` struct. The
  explicit `id` argument is used as the stored node ID.

  `:implementation` accepts shorthand module forms:

  - `MyNode` becomes `%{type: :module, module: MyNode, function: :call}`
  - `{MyNode, :run}` becomes `%{type: :module, module: MyNode, function: :run}`
  - maps are preserved for compiler/runtime validation
  """
  @spec put_node(t(), id(), keyword() | map() | Node.t(), keyword()) :: edit_result()
  def put_node(graph, id, attrs, opts \\ []) do
    edit_result(fn -> put_node!(graph, id, attrs, opts) end)
  end

  @doc """
  Adds or replaces a node by ID, raising on malformed arguments.
  """
  @spec put_node!(t(), id(), keyword() | map() | Node.t(), keyword()) :: t()
  def put_node!(graph, id, attrs, opts \\ [])

  def put_node!(%__MODULE__{} = graph, id, attrs, opts) do
    assert_public_id!(id, :node_id)

    node =
      attrs
      |> node_from_attrs(id)
      |> Map.put(:id, id)

    graph
    |> put_in([Access.key!(:nodes), id], node)
    |> finalize_edit(opts)
  end

  def put_node!(graph, _id, _attrs, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Updates a node with a map/keyword patch or a function.

  `attrs_or_fun` may be:

  - a keyword list patch
  - a map patch
  - a `Docket.Graph.Node` struct
  - a function that receives the current `Docket.Graph.Node` and returns a
    keyword list, map, or `Docket.Graph.Node`

  The explicit `id` argument remains the stored node ID after the update.
  """
  @spec update_node(
          t(),
          id(),
          (Node.t() -> Node.t() | map() | keyword()) | Node.t() | map() | keyword(),
          keyword()
        ) ::
          edit_result()
  def update_node(graph, id, attrs_or_fun, opts \\ []) do
    edit_result(fn -> update_node!(graph, id, attrs_or_fun, opts) end)
  end

  @doc """
  Updates a node with a map/keyword patch or a function, raising on malformed
  arguments.
  """
  @spec update_node!(
          t(),
          id(),
          (Node.t() -> Node.t() | map() | keyword()) | Node.t() | map() | keyword(),
          keyword()
        ) ::
          t()
  def update_node!(graph, id, attrs_or_fun, opts \\ [])

  def update_node!(%__MODULE__{} = graph, id, attrs_or_fun, opts) do
    assert_public_id!(id, :node_id)
    node = Map.get(graph.nodes, id, %Node{id: id})
    updated = apply_update(node, attrs_or_fun, &node_from_attrs(&1, id)) |> Map.put(:id, id)

    graph
    |> put_in([Access.key!(:nodes), id], updated)
    |> finalize_edit(opts)
  end

  def update_node!(graph, _id, _attrs_or_fun, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Deletes a node.
  """
  @spec delete_node(t(), id(), keyword()) :: edit_result()
  def delete_node(graph, id, opts \\ []) do
    edit_result(fn -> delete_node!(graph, id, opts) end)
  end

  @doc """
  Deletes a node, raising on malformed arguments.
  """
  @spec delete_node!(t(), id(), keyword()) :: t()
  def delete_node!(graph, id, opts \\ [])

  def delete_node!(%__MODULE__{} = graph, id, opts) do
    assert_public_id!(id, :node_id)

    graph
    |> Map.put(:nodes, Map.delete(graph.nodes, id))
    |> finalize_edit(opts)
  end

  def delete_node!(graph, _id, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Adds or replaces an edge by ID.

  `attrs` may be a keyword list, a map, or a `Docket.Graph.Edge` struct. The
  explicit `id` argument is used as the stored edge ID.
  """
  @spec put_edge(t(), id(), keyword() | map() | Edge.t(), keyword()) :: edit_result()
  def put_edge(graph, id, attrs, opts \\ []) do
    edit_result(fn -> put_edge!(graph, id, attrs, opts) end)
  end

  @doc """
  Adds or replaces an edge by ID, raising on malformed arguments.
  """
  @spec put_edge!(t(), id(), keyword() | map() | Edge.t(), keyword()) :: t()
  def put_edge!(graph, id, attrs, opts \\ [])

  def put_edge!(%__MODULE__{} = graph, id, attrs, opts) do
    assert_public_id!(id, :edge_id)

    edge =
      attrs
      |> edge_from_attrs(id)
      |> Map.put(:id, id)

    graph
    |> put_in([Access.key!(:edges), id], edge)
    |> finalize_edit(opts)
  end

  def put_edge!(graph, _id, _attrs, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Updates an edge with a map/keyword patch or a function.

  `attrs_or_fun` may be:

  - a keyword list patch
  - a map patch
  - a `Docket.Graph.Edge` struct
  - a function that receives the current `Docket.Graph.Edge` and returns a
    keyword list, map, or `Docket.Graph.Edge`

  The explicit `id` argument remains the stored edge ID after the update.
  """
  @spec update_edge(
          t(),
          id(),
          (Edge.t() -> Edge.t() | map() | keyword()) | Edge.t() | map() | keyword(),
          keyword()
        ) ::
          edit_result()
  def update_edge(graph, id, attrs_or_fun, opts \\ []) do
    edit_result(fn -> update_edge!(graph, id, attrs_or_fun, opts) end)
  end

  @doc """
  Updates an edge with a map/keyword patch or a function, raising on malformed
  arguments.
  """
  @spec update_edge!(
          t(),
          id(),
          (Edge.t() -> Edge.t() | map() | keyword()) | Edge.t() | map() | keyword(),
          keyword()
        ) ::
          t()
  def update_edge!(graph, id, attrs_or_fun, opts \\ [])

  def update_edge!(%__MODULE__{} = graph, id, attrs_or_fun, opts) do
    assert_public_id!(id, :edge_id)
    edge = Map.get(graph.edges, id, %Edge{id: id})
    updated = apply_update(edge, attrs_or_fun, &edge_from_attrs(&1, id)) |> Map.put(:id, id)

    graph
    |> put_in([Access.key!(:edges), id], updated)
    |> finalize_edit(opts)
  end

  def update_edge!(graph, _id, _attrs_or_fun, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Deletes an edge.
  """
  @spec delete_edge(t(), id(), keyword()) :: edit_result()
  def delete_edge(graph, id, opts \\ []) do
    edit_result(fn -> delete_edge!(graph, id, opts) end)
  end

  @doc """
  Deletes an edge, raising on malformed arguments.
  """
  @spec delete_edge!(t(), id(), keyword()) :: t()
  def delete_edge!(graph, id, opts \\ [])

  def delete_edge!(%__MODULE__{} = graph, id, opts) do
    assert_public_id!(id, :edge_id)

    graph =
      %{graph | edges: Map.delete(graph.edges, id)}

    finalize_edit(graph, opts)
  end

  def delete_edge!(graph, _id, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Updates a field with a map/keyword patch or a function.

  `attrs_or_fun` may be:

  - a keyword list patch
  - a map patch
  - a `Docket.Graph.Field` struct
  - a function that receives the current `Docket.Graph.Field` and returns a
    keyword list, map, or `Docket.Graph.Field`

  The resulting field kind decides whether the field is stored as an input or a
  state field. The explicit `id` argument remains the stored field ID.
  """
  @spec update_field(
          t(),
          id(),
          (Field.t() -> Field.t() | map() | keyword()) | Field.t() | map() | keyword(),
          keyword()
        ) ::
          edit_result()
  def update_field(graph, id, attrs_or_fun, opts \\ []) do
    edit_result(fn -> update_field!(graph, id, attrs_or_fun, opts) end)
  end

  @doc """
  Updates a field with a map/keyword patch or a function, raising on malformed
  arguments.
  """
  @spec update_field!(
          t(),
          id(),
          (Field.t() -> Field.t() | map() | keyword()) | Field.t() | map() | keyword(),
          keyword()
        ) ::
          t()
  def update_field!(graph, id, attrs_or_fun, opts \\ [])

  def update_field!(%__MODULE__{} = graph, id, attrs_or_fun, opts) do
    assert_public_id!(id, :field_id)

    existing =
      Map.get(graph.inputs, id) ||
        Map.get(graph.fields, id) ||
        %Field{id: id, kind: :state}

    updated = apply_update(existing, attrs_or_fun, &field_from_attrs(&1, id))

    case updated.kind do
      :input -> put_input!(graph, id, updated, opts)
      _kind -> put_field!(graph, id, updated, opts)
    end
  end

  def update_field!(graph, _id, _attrs_or_fun, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  @doc """
  Deletes a field from both input and state field collections.
  """
  @spec delete_field(t(), id(), keyword()) :: edit_result()
  def delete_field(graph, id, opts \\ []) do
    edit_result(fn -> delete_field!(graph, id, opts) end)
  end

  @doc """
  Deletes a field from both input and state field collections, raising on
  malformed arguments.
  """
  @spec delete_field!(t(), id(), keyword()) :: t()
  def delete_field!(graph, id, opts \\ [])

  def delete_field!(%__MODULE__{} = graph, id, opts) do
    assert_public_id!(id, :field_id)

    graph = %{
      graph
      | inputs: Map.delete(graph.inputs, id),
        fields: Map.delete(graph.fields, id)
    }

    finalize_edit(graph, opts)
  end

  def delete_field!(graph, _id, _opts) do
    invalid!(:invalid_graph, "graph must be a Docket.Graph, got #{inspect(graph)}")
  end

  defp valid_id?(id), do: is_binary(id) and Regex.match?(@id_pattern, id)

  defp assert_public_id!(id, label) do
    cond do
      not is_binary(id) ->
        invalid!(:invalid_public_id, "#{label} must be a binary", %{label: label, id: id})

      id in [@start_id, @finish_id] ->
        invalid!(:reserved_id, "#{label} cannot be reserved endpoint #{inspect(id)}", %{
          label: label,
          id: id
        })

      not valid_id?(id) ->
        invalid!(
          :invalid_public_id,
          "#{label} must match #{inspect(@id_pattern)}; got #{inspect(id)}",
          %{label: label, id: id}
        )

      true ->
        :ok
    end
  end

  defp attrs_to_map(%_struct{} = struct), do: Map.from_struct(struct)
  defp attrs_to_map(attrs) when is_map(attrs), do: attrs

  defp attrs_to_map(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs) do
      Map.new(attrs)
    else
      invalid!(:invalid_attrs, "attrs must be a map or keyword list, got #{inspect(attrs)}")
    end
  end

  defp attrs_to_map(attrs) do
    invalid!(:invalid_attrs, "attrs must be a map or keyword list, got #{inspect(attrs)}")
  end

  defp field_from_attrs(attrs, id \\ nil)

  defp field_from_attrs(%Field{} = field, _id), do: field

  defp field_from_attrs(attrs, id) do
    attrs
    |> attrs_to_map()
    |> Map.put_new(:id, id)
    |> Map.put_new(:kind, :state)
    |> then(&struct(Field, &1))
  end

  defp node_from_attrs(%Node{} = node, _id) do
    Map.update!(node, :implementation, &Serializer.normalize_implementation/1)
  end

  defp node_from_attrs(attrs, id) do
    attrs
    |> attrs_to_map()
    |> Map.put_new(:id, id)
    |> then(&struct(Node, &1))
    |> Map.update!(:implementation, &Serializer.normalize_implementation/1)
  end

  defp edge_from_attrs(%Edge{} = edge, _id), do: edge

  defp edge_from_attrs(attrs, id) do
    attrs
    |> attrs_to_map()
    |> Map.put_new(:id, id)
    |> then(&struct(Edge, &1))
  end

  defp apply_update(existing, fun, normalizer) when is_function(fun, 1) do
    existing
    |> fun.()
    |> normalizer.()
  end

  defp apply_update(existing, attrs, normalizer) do
    existing
    |> Map.from_struct()
    |> Map.merge(attrs_to_map(attrs))
    |> normalizer.()
  end

  defp finalize_edit(%__MODULE__{} = graph, opts) do
    _opts = opts_to_keyword!(opts)
    %{graph | diagnostics: []}
  end

  defp edit_result(fun) do
    {:ok, fun.()}
  rescue
    error in Error -> {:error, error}
  end

  defp opts_to_keyword!(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      opts
    else
      invalid!(:invalid_options, "opts must be a keyword list, got #{inspect(opts)}")
    end
  end

  defp opts_to_keyword!(opts) do
    invalid!(:invalid_options, "opts must be a keyword list, got #{inspect(opts)}")
  end

  defp invalid!(code, message, details \\ %{}) do
    raise Error, code: code, message: message, details: details
  end

  defp generate_id(kind, opts) do
    case Keyword.get(opts, :id_generator) do
      generator when is_function(generator, 1) ->
        generator.(kind)

      nil ->
        "#{kind}_#{System.unique_integer([:positive, :monotonic])}"
    end
  end
end
