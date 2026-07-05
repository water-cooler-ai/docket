defmodule Docket.Graph.Compiler.Validation do
  @moduledoc false

  # Public graph validation passes (compiler design phases 9.2 - 9.10).
  #
  # Passes never trust edit-time normalization: hosts may load old, manually
  # edited, or externally generated graph documents. All passes run even when
  # earlier passes found errors, so a single verify yields maximum
  # diagnostics; passes skip individual records they cannot interpret.

  alias Docket.Graph
  alias Docket.Graph.Compiler.{NodeContracts, Policies}
  alias Docket.Graph.{Edge, Field}
  alias Docket.{Guard, Reducer, Schema}

  import Docket.Graph.Compiler.Diagnostics, only: [error: 3, warning: 3]

  @id_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_-]*$/
  @start_id "$start"
  @finish_id "$finish"
  @supported_schema_version 1
  @supported_guard_ops [:all, :any, :changed, :equals, :exists, :not, :path, :version_at_least]
  @schema_types [:boolean, :enum, :float, :integer, :list, :map, :object, :string]

  @spec run(Graph.t(), %{optional(String.t()) => NodeContracts.fetch_result()}, keyword()) ::
          [Docket.Graph.Diagnostic.t()]
  def run(%Graph{} = graph, config_schemas, opts) do
    List.flatten([
      validate_document(graph, opts),
      validate_fields(graph, opts),
      validate_outputs(graph, opts),
      validate_nodes(graph, config_schemas),
      validate_edges(graph, opts),
      validate_branches(graph, opts),
      validate_guards(graph, opts),
      validate_topology(graph, opts),
      analyze_cycles(graph, opts),
      analyze_dead_ends(graph, opts)
    ])
  end

  # ---------------------------------------------------------------------------
  # 9.2 Public document
  # ---------------------------------------------------------------------------

  # Durability is not checked here: compiler ingest canonicalizes the graph
  # through the wire format and reports serialization failures itself.
  defp validate_document(graph, opts) do
    List.flatten([
      check_schema_version(graph),
      check_graph_id(graph),
      check_record_ids(graph),
      check_field_collisions(graph),
      check_policies(graph, opts)
    ])
  end

  # Validated regardless of topology so an invalid limit never ships into the
  # runtime graph (lowering would otherwise carry it verbatim).
  defp check_policies(graph, opts) do
    case Policies.max_supersteps(graph, opts) do
      {:ok, _limit} ->
        []

      {:invalid, value} ->
        [
          error(
            :invalid_policy,
            "graph policy \"max_supersteps\" must be a positive integer, got #{inspect(value)}",
            path: [:policies, Policies.max_supersteps_key()]
          )
        ]
    end
  end

  defp check_schema_version(%Graph{schema_version: @supported_schema_version}), do: []

  defp check_schema_version(%Graph{schema_version: version}) do
    [
      error(
        :unsupported_schema_version,
        "graph schema_version #{inspect(version)} is not supported; expected #{@supported_schema_version}",
        path: [:schema_version]
      )
    ]
  end

  defp check_graph_id(%Graph{id: id}) do
    if is_binary(id) and Regex.match?(@id_pattern, id) do
      []
    else
      [
        error(:invalid_public_id, "graph ID must be a valid public ID, got #{inspect(id)}",
          path: [:id]
        )
      ]
    end
  end

  defp check_record_ids(graph) do
    for collection <- [:inputs, :fields, :outputs, :nodes, :edges],
        {id, _record} <- sorted(Map.fetch!(graph, collection)),
        diagnostic = check_record_id(collection, id),
        diagnostic != nil do
      diagnostic
    end
  end

  defp check_record_id(collection, id) when is_binary(id) do
    cond do
      id in [@start_id, @finish_id] ->
        error(:reserved_id, "#{singular(collection)} ID #{inspect(id)} is a reserved endpoint",
          path: [collection, id],
          public_id: id
        )

      not Regex.match?(@id_pattern, id) ->
        error(
          :invalid_public_id,
          "#{singular(collection)} ID #{inspect(id)} is not a valid public ID",
          path: [collection, id],
          public_id: id
        )

      true ->
        nil
    end
  end

  defp check_record_id(collection, id) do
    error(:invalid_public_id, "#{singular(collection)} ID must be a binary, got #{inspect(id)}",
      path: [collection, id]
    )
  end

  defp check_field_collisions(graph) do
    input_ids = graph.inputs |> Map.keys() |> MapSet.new()

    for id <- Enum.sort(Map.keys(graph.fields)), MapSet.member?(input_ids, id) do
      error(
        :duplicate_state_id,
        "field #{inspect(id)} is declared as both an input and a state field",
        path: [:fields, id],
        public_id: id
      )
    end
  end

  # ---------------------------------------------------------------------------
  # 9.3 Fields, schemas, and reducers
  # ---------------------------------------------------------------------------

  defp validate_fields(graph, _opts) do
    for collection <- [:inputs, :fields],
        {id, field} <- sorted(Map.fetch!(graph, collection)),
        diagnostic <- validate_field(collection, id, field) do
      diagnostic
    end
  end

  defp validate_field(collection, id, %Field{} = field) do
    List.flatten([
      check_field_schema(collection, id, field.schema),
      check_field_reducer(collection, id, field.reducer),
      check_field_default(collection, id, field)
    ])
  end

  defp validate_field(collection, id, other) do
    [
      error(
        :invalid_schema,
        "#{singular(collection)} #{inspect(id)} must be a field record, got #{inspect(other)}",
        path: [collection, id],
        public_id: id
      )
    ]
  end

  defp check_field_schema(collection, id, nil) do
    [
      error(:missing_field_schema, "#{singular(collection)} #{inspect(id)} requires a schema",
        path: [collection, id, :schema],
        public_id: id
      )
    ]
  end

  defp check_field_schema(collection, id, schema) do
    if valid_schema?(schema) do
      []
    else
      [
        error(
          :invalid_schema,
          "#{singular(collection)} #{inspect(id)} schema is not a valid Docket.Schema",
          path: [collection, id, :schema],
          public_id: id
        )
      ]
    end
  end

  defp check_field_reducer(_collection, _id, nil), do: []
  defp check_field_reducer(_collection, _id, %Reducer{type: :last_value}), do: []

  defp check_field_reducer(collection, id, reducer) do
    [
      error(
        :invalid_reducer,
        "#{singular(collection)} #{inspect(id)} reducer is not supported in v1; only last_value reducers are supported, got #{inspect(reducer)}",
        path: [collection, id, :reducer],
        public_id: id
      )
    ]
  end

  defp check_field_default(_collection, _id, %Field{default: nil}), do: []

  defp check_field_default(collection, id, %Field{default: default, schema: schema}) do
    with true <- valid_schema?(schema),
         {:error, reasons} <- Schema.validate(schema, default) do
      [
        error(
          :invalid_field_default,
          "#{singular(collection)} #{inspect(id)} default does not match its schema",
          path: [collection, id, :default],
          public_id: id,
          metadata: %{reasons: reasons}
        )
      ]
    else
      _valid -> []
    end
  end

  defp valid_schema?(%Schema{type: type}) when type in @schema_types, do: true
  defp valid_schema?(_schema), do: false

  # ---------------------------------------------------------------------------
  # 9.4 Outputs
  # ---------------------------------------------------------------------------

  defp validate_outputs(graph, _opts) do
    for {id, output} <- sorted(graph.outputs), diagnostic <- validate_output(graph, id, output) do
      diagnostic
    end
  end

  defp validate_output(graph, id, %Graph.Output{} = output) do
    case resolve_field(graph, output.source) do
      nil ->
        [
          error(
            :unknown_output_source,
            "output #{inspect(id)} source #{inspect(output.source)} is not an input or state field",
            path: [:outputs, id, :source],
            public_id: id
          )
        ]

      source_field ->
        check_output_schema(id, output.schema, source_field.schema)
    end
  end

  defp validate_output(_graph, id, other) do
    [
      error(
        :unknown_output_source,
        "output #{inspect(id)} is not an output record, got #{inspect(other)}",
        path: [:outputs, id],
        public_id: id
      )
    ]
  end

  defp check_output_schema(_id, nil, _source_schema), do: []

  defp check_output_schema(id, schema, source_schema) do
    cond do
      not valid_schema?(schema) ->
        [
          error(:invalid_schema, "output #{inspect(id)} schema is not a valid Docket.Schema",
            path: [:outputs, id, :schema],
            public_id: id
          )
        ]

      valid_schema?(source_schema) and not compatible_schemas?(source_schema, schema) ->
        [
          error(
            :incompatible_output_schema,
            "output #{inspect(id)} schema is not compatible with its source field schema",
            path: [:outputs, id, :schema],
            public_id: id
          )
        ]

      true ->
        []
    end
  end

  # v1 compatibility: same type; enum outputs must accept every source value;
  # list outputs with a declared item must accept the source's items.
  defp compatible_schemas?(%Schema{type: :enum} = source, %Schema{type: :enum} = output) do
    MapSet.subset?(MapSet.new(source.values), MapSet.new(output.values))
  end

  defp compatible_schemas?(%Schema{type: :list} = source, %Schema{type: :list} = output) do
    case {source.item, output.item} do
      {_source_item, nil} -> true
      {nil, %Schema{}} -> false
      {source_item, output_item} -> compatible_schemas?(source_item, output_item)
    end
  end

  defp compatible_schemas?(%Schema{type: type}, %Schema{type: type}), do: true
  defp compatible_schemas?(_source, _output), do: false

  defp resolve_field(graph, source) when is_binary(source) do
    case Map.get(graph.inputs, source) || Map.get(graph.fields, source) do
      %Field{} = field -> field
      _other -> nil
    end
  end

  defp resolve_field(_graph, _source), do: nil

  # ---------------------------------------------------------------------------
  # 9.5 Nodes
  # ---------------------------------------------------------------------------

  defp validate_nodes(graph, config_schemas) do
    for {id, node} <- sorted(graph.nodes),
        diagnostic <- validate_node(id, node, config_schemas) ++ validate_node_policies(id, node) do
      diagnostic
    end
  end

  # 9.5: the v1 node policy surface defined by the runtime ("timeout_ms",
  # "retry", reserved "on_error"). The rules live in Policies so the compiler
  # and plan-time validation cannot drift apart.
  defp validate_node_policies(id, %Graph.Node{policies: policies}) do
    case Policies.node_policies(canonicalize_open(policies)) do
      {:ok, _resolved} ->
        []

      {:error, errors} ->
        for {key, message} <- errors do
          error(:invalid_policy, "node #{inspect(id)}: #{message}",
            path: [:nodes, id, :policies] ++ List.wrap(key),
            public_id: id
          )
        end
    end
  end

  defp validate_node_policies(_id, _other), do: []

  defp validate_node(id, %Graph.Node{} = node, config_schemas) do
    case node.implementation do
      nil ->
        [
          error(:missing_node_implementation, "node #{inspect(id)} has no implementation",
            path: [:nodes, id, :implementation],
            public_id: id
          )
        ]

      %{type: :module, module: module, function: :call} when is_atom(module) ->
        validate_node_module(id, node, module, config_schemas)

      %{type: :module} = implementation ->
        [
          error(
            :unsupported_node_implementation,
            "node #{inspect(id)} implementation is not supported in v1; module implementations must use call/3, got #{inspect(implementation)}",
            path: [:nodes, id, :implementation],
            public_id: id
          )
        ]

      implementation ->
        [
          error(
            :unsupported_node_implementation,
            "node #{inspect(id)} implementation type is not supported in v1, got #{inspect(implementation)}",
            path: [:nodes, id, :implementation],
            public_id: id
          )
        ]
    end
  end

  defp validate_node(id, other, _config_schemas) do
    [
      error(
        :missing_node_implementation,
        "node #{inspect(id)} is not a node record, got #{inspect(other)}",
        path: [:nodes, id],
        public_id: id
      )
    ]
  end

  defp validate_node_module(id, node, module, config_schemas) do
    cond do
      not module_loaded?(module) ->
        [
          error(
            :node_module_not_loaded,
            "node #{inspect(id)} implementation module #{inspect(module)} cannot be loaded",
            path: [:nodes, id, :implementation],
            public_id: id
          )
        ]

      not (function_exported?(module, :config_schema, 0) and function_exported?(module, :call, 3)) ->
        [
          error(
            :invalid_node_implementation,
            "node #{inspect(id)} implementation module #{inspect(module)} must export config_schema/0 and call/3",
            path: [:nodes, id, :implementation],
            public_id: id
          )
        ]

      true ->
        validate_node_config(id, node, module, config_schemas)
    end
  end

  defp validate_node_config(id, node, module, config_schemas) do
    case Map.get(config_schemas, id) do
      {:ok, schema} ->
        # The config is canonicalized here rather than trusting ingest: in
        # fallback mode (a graph that failed serialization elsewhere) the
        # in-memory config may still be atom-keyed, and validating it raw
        # would produce false diagnostics against the string-keyed schema.
        case Schema.validate(schema, canonicalize_open(node.config)) do
          :ok ->
            []

          {:error, reasons} ->
            [
              error(
                :invalid_node_config,
                "node #{inspect(id)} config does not match #{inspect(module)}.config_schema/0",
                path: [:nodes, id, :config],
                public_id: id,
                metadata: %{reasons: reasons}
              )
            ]
        end

      {:error, metadata} ->
        [
          error(
            :invalid_node_config_schema,
            "node #{inspect(id)} implementation module #{inspect(module)} does not provide a valid config schema",
            path: [:nodes, id, :implementation],
            public_id: id,
            metadata: metadata
          )
        ]

      nil ->
        []
    end
  end

  # Canonicalizes open content the way the wire format would (atoms become
  # strings, map keys stringify) without failing on non-durable terms; those
  # are already reported by ingest.
  defp canonicalize_open(value) when is_map(value) and not is_struct(value) do
    Map.new(value, fn {key, child} -> {canonicalize_key(key), canonicalize_open(child)} end)
  end

  defp canonicalize_open(value) when is_list(value), do: Enum.map(value, &canonicalize_open/1)

  defp canonicalize_open(value) when is_atom(value) and value not in [nil, true, false] do
    Atom.to_string(value)
  end

  defp canonicalize_open(value), do: value

  defp canonicalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp canonicalize_key(key), do: key

  defp module_loaded?(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} -> true
      {:error, _reason} -> false
    end
  end

  # ---------------------------------------------------------------------------
  # 9.6 Edges
  # ---------------------------------------------------------------------------

  defp validate_edges(graph, _opts) do
    for {id, edge} <- sorted(graph.edges), diagnostic <- validate_edge(graph, id, edge) do
      diagnostic
    end
  end

  defp validate_edge(graph, id, %Edge{} = edge) do
    List.flatten([
      validate_edge_from(graph, id, edge.from),
      validate_edge_to(graph, id, edge.to),
      validate_edge_guard_shape(id, edge.guard)
    ])
  end

  defp validate_edge(_graph, id, other) do
    [
      error(
        :unknown_edge_source,
        "edge #{inspect(id)} is not an edge record, got #{inspect(other)}",
        path: [:edges, id]
      )
    ]
  end

  defp validate_edge_from(_graph, _id, @start_id), do: []

  defp validate_edge_from(_graph, id, @finish_id) do
    [
      error(:invalid_finish_endpoint, "edge #{inspect(id)} cannot use $finish as a source",
        path: [:edges, id, :from],
        public_id: id
      )
    ]
  end

  defp validate_edge_from(graph, id, from) when is_binary(from) do
    check_edge_node(graph, id, from, :unknown_edge_source, :from, "source")
  end

  defp validate_edge_from(_graph, id, []) do
    [
      error(:empty_edge_sources, "edge #{inspect(id)} multi-source list cannot be empty",
        path: [:edges, id, :from],
        public_id: id
      )
    ]
  end

  defp validate_edge_from(graph, id, from) when is_list(from) do
    reserved =
      Enum.flat_map(Enum.uniq(from), fn
        @start_id ->
          [
            error(
              :invalid_start_endpoint,
              "edge #{inspect(id)} cannot include $start in a multi-source list",
              path: [:edges, id, :from],
              public_id: id
            )
          ]

        @finish_id ->
          [
            error(
              :invalid_finish_endpoint,
              "edge #{inspect(id)} cannot include $finish in a multi-source list",
              path: [:edges, id, :from],
              public_id: id
            )
          ]

        _source ->
          []
      end)

    duplicates =
      if Enum.uniq(from) == from do
        []
      else
        [
          error(
            :duplicate_edge_source,
            "edge #{inspect(id)} multi-source list contains duplicate sources",
            path: [:edges, id, :from],
            public_id: id
          )
        ]
      end

    unknown =
      from
      |> Enum.uniq()
      |> Enum.reject(&(&1 in [@start_id, @finish_id]))
      |> Enum.flat_map(&check_edge_node(graph, id, &1, :unknown_edge_source, :from, "source"))

    reserved ++ duplicates ++ unknown
  end

  defp validate_edge_from(_graph, id, from) do
    [
      error(
        :unknown_edge_source,
        "edge #{inspect(id)} source must be $start, a node ID, or a list of node IDs, got #{inspect(from)}",
        path: [:edges, id, :from],
        public_id: id
      )
    ]
  end

  defp validate_edge_to(_graph, _id, @finish_id), do: []

  defp validate_edge_to(_graph, id, @start_id) do
    [
      error(:invalid_start_endpoint, "edge #{inspect(id)} cannot use $start as a target",
        path: [:edges, id, :to],
        public_id: id
      )
    ]
  end

  defp validate_edge_to(graph, id, to) when is_binary(to) do
    check_edge_node(graph, id, to, :unknown_edge_target, :to, "target")
  end

  defp validate_edge_to(_graph, id, to) do
    [
      error(
        :unknown_edge_target,
        "edge #{inspect(id)} target must be $finish or a node ID, got #{inspect(to)}",
        path: [:edges, id, :to],
        public_id: id
      )
    ]
  end

  defp check_edge_node(graph, id, node_id, code, key, label) do
    if Map.has_key?(graph.nodes, node_id) do
      []
    else
      [
        error(code, "edge #{inspect(id)} #{label}s unknown node #{inspect(node_id)}",
          path: [:edges, id, key],
          public_id: id
        )
      ]
    end
  end

  defp validate_edge_guard_shape(_id, nil), do: []
  defp validate_edge_guard_shape(_id, %Guard{}), do: []

  defp validate_edge_guard_shape(id, guard) do
    [
      error(
        :invalid_guard,
        "edge #{inspect(id)} guard must be a Docket.Guard expression, got #{inspect(guard)}",
        path: [:edges, id, :guard],
        public_id: id
      )
    ]
  end

  # ---------------------------------------------------------------------------
  # 9.7 Branch groups
  # ---------------------------------------------------------------------------

  defp validate_branches(graph, _opts) do
    for {node_id, node} <- sorted(graph.nodes),
        is_struct(node, Graph.Node),
        is_map(node.branches),
        diagnostic <- validate_node_branches(graph, node_id, node.branches) do
      diagnostic
    end
  end

  defp validate_node_branches(graph, node_id, branches) do
    group_diagnostics =
      for {group_name, edge_ids} <- sorted(branches),
          diagnostic <- validate_branch_group(graph, node_id, group_name, edge_ids) do
        diagnostic
      end

    group_diagnostics ++ check_duplicate_branch_edges(node_id, branches)
  end

  defp validate_branch_group(graph, node_id, group_name, edge_ids) when is_list(edge_ids) do
    Enum.flat_map(edge_ids, fn edge_id ->
      case Map.get(graph.edges, edge_id) do
        nil ->
          [
            error(
              :unknown_branch_edge,
              "node #{inspect(node_id)} branch #{inspect(group_name)} references unknown edge #{inspect(edge_id)}",
              path: [:nodes, node_id, :branches, group_name],
              public_id: node_id
            )
          ]

        %Edge{from: ^node_id} = edge ->
          if edge.guard == nil do
            [
              warning(
                :unguarded_branch_edge,
                "node #{inspect(node_id)} branch #{inspect(group_name)} edge #{inspect(edge_id)} has no guard",
                path: [:nodes, node_id, :branches, group_name],
                public_id: edge_id
              )
            ]
          else
            []
          end

        _edge ->
          [
            error(
              :branch_edge_source_mismatch,
              "node #{inspect(node_id)} branch #{inspect(group_name)} edge #{inspect(edge_id)} does not start at #{inspect(node_id)}",
              path: [:nodes, node_id, :branches, group_name],
              public_id: node_id
            )
          ]
      end
    end)
  end

  defp validate_branch_group(_graph, node_id, group_name, other) do
    [
      error(
        :unknown_branch_edge,
        "node #{inspect(node_id)} branch #{inspect(group_name)} must be a list of edge IDs, got #{inspect(other)}",
        path: [:nodes, node_id, :branches, group_name],
        public_id: node_id
      )
    ]
  end

  # An edge may sit in at most one branch position. Occurrences are counted
  # across all groups without a per-group uniq, so an edge repeated inside a
  # single group is caught as well as one spanning two groups.
  defp check_duplicate_branch_edges(node_id, branches) do
    duplicated =
      branches
      |> Enum.flat_map(fn
        {_group_name, edge_ids} when is_list(edge_ids) -> edge_ids
        _other -> []
      end)
      |> Enum.frequencies()
      |> Enum.filter(fn {_edge_id, count} -> count > 1 end)
      |> Enum.map(fn {edge_id, _count} -> edge_id end)
      |> Enum.sort()

    for edge_id <- duplicated do
      error(
        :duplicate_branch_edge,
        "node #{inspect(node_id)} references edge #{inspect(edge_id)} more than once in its branch groups",
        path: [:nodes, node_id, :branches],
        public_id: node_id,
        metadata: %{edge_id: edge_id}
      )
    end
  end

  # ---------------------------------------------------------------------------
  # 9.8 Guards
  # ---------------------------------------------------------------------------

  defp validate_guards(graph, _opts) do
    for {edge_id, %Edge{guard: %Guard{} = guard}} <- sorted(graph.edges),
        diagnostic <- validate_guard_expression(graph, edge_id, guard) do
      diagnostic
    end
  end

  defp validate_guard_expression(graph, edge_id, %Guard{op: op, args: args} = guard) do
    cond do
      op not in @supported_guard_ops ->
        [guard_error(:invalid_guard, edge_id, "guard op #{inspect(op)} is not supported")]

      not is_list(args) ->
        [
          guard_error(
            :invalid_guard,
            edge_id,
            "guard #{inspect(op)} args must be a list, got #{inspect(args)}"
          )
        ]

      true ->
        validate_guard_args(graph, edge_id, guard)
    end
  end

  defp validate_guard_args(graph, edge_id, %Guard{op: :changed, args: [channel]}) do
    validate_guard_channel(graph, edge_id, channel)
  end

  defp validate_guard_args(graph, edge_id, %Guard{op: :version_at_least, args: [channel, version]}) do
    version_diagnostics =
      if is_integer(version) and version >= 0 do
        []
      else
        [
          guard_error(
            :invalid_guard,
            edge_id,
            "version_at_least requires a non-negative integer version, got #{inspect(version)}"
          )
        ]
      end

    validate_guard_channel(graph, edge_id, channel) ++ version_diagnostics
  end

  defp validate_guard_args(graph, edge_id, %Guard{op: :path, args: [channel, segments]}) do
    segment_diagnostics =
      if is_list(segments) do
        for segment <- segments, not valid_path_segment?(segment) do
          guard_error(
            :invalid_guard_path,
            edge_id,
            "path segment #{inspect(segment)} must be a string, atom, or integer"
          )
        end
      else
        [
          guard_error(
            :invalid_guard_path,
            edge_id,
            "path segments must be a list, got #{inspect(segments)}"
          )
        ]
      end

    validate_guard_channel(graph, edge_id, channel) ++ segment_diagnostics
  end

  defp validate_guard_args(graph, edge_id, %Guard{op: op, args: [ref]}) when op in [:exists] do
    validate_guard_ref(graph, edge_id, ref)
  end

  defp validate_guard_args(graph, edge_id, %Guard{op: :equals, args: [ref, _value]}) do
    validate_guard_ref(graph, edge_id, ref)
  end

  defp validate_guard_args(graph, edge_id, %Guard{op: op, args: args}) when op in [:all, :any] do
    Enum.flat_map(args, fn
      %Guard{} = nested ->
        validate_guard_expression(graph, edge_id, nested)

      other ->
        [
          guard_error(
            :invalid_guard,
            edge_id,
            "#{op} arguments must be guard expressions, got #{inspect(other)}"
          )
        ]
    end)
  end

  defp validate_guard_args(graph, edge_id, %Guard{op: :not, args: [%Guard{} = nested]}) do
    validate_guard_expression(graph, edge_id, nested)
  end

  defp validate_guard_args(_graph, edge_id, %Guard{op: op, args: args}) do
    [
      guard_error(
        :invalid_guard,
        edge_id,
        "guard #{inspect(op)} has malformed arguments #{inspect(args)}"
      )
    ]
  end

  defp validate_guard_ref(graph, edge_id, %Guard{} = nested) do
    validate_guard_expression(graph, edge_id, nested)
  end

  defp validate_guard_ref(graph, edge_id, ref) when is_binary(ref) do
    validate_guard_channel(graph, edge_id, ref)
  end

  defp validate_guard_ref(_graph, edge_id, ref) do
    [
      guard_error(
        :invalid_guard,
        edge_id,
        "guard reference must be a field ID or guard expression, got #{inspect(ref)}"
      )
    ]
  end

  defp validate_guard_channel(graph, edge_id, channel) when is_binary(channel) do
    if Map.has_key?(graph.inputs, channel) or Map.has_key?(graph.fields, channel) do
      []
    else
      [
        guard_error(
          :unknown_guard_field,
          edge_id,
          "guard references unknown field #{inspect(channel)}"
        )
      ]
    end
  end

  defp validate_guard_channel(_graph, edge_id, channel) do
    [
      guard_error(
        :unknown_guard_field,
        edge_id,
        "guard channel reference must be a field ID, got #{inspect(channel)}"
      )
    ]
  end

  defp valid_path_segment?(segment) do
    is_binary(segment) or is_atom(segment) or is_integer(segment)
  end

  defp guard_error(code, edge_id, message) do
    error(code, "edge #{inspect(edge_id)}: #{message}",
      path: [:edges, edge_id, :guard],
      public_id: edge_id
    )
  end

  # ---------------------------------------------------------------------------
  # 9.9 Topology
  # ---------------------------------------------------------------------------

  defp validate_topology(graph, _opts) do
    start_edges =
      Enum.filter(graph.edges, fn {_id, edge} -> match?(%Edge{from: @start_id}, edge) end)

    cond do
      map_size(graph.nodes) == 0 and map_size(graph.edges) == 0 ->
        []

      start_edges == [] ->
        [error(:no_entrypoint, "graph has no edge from $start to a node", path: [:edges])]

      true ->
        reachable = reachable_nodes(graph)

        for node_id <- Enum.sort(Map.keys(graph.nodes)),
            not MapSet.member?(reachable, node_id) do
          error(:unreachable_node, "node #{inspect(node_id)} is not reachable from $start",
            path: [:nodes, node_id],
            public_id: node_id
          )
        end
    end
  end

  # A multi-source edge only makes its target reachable when all of its
  # sources are reachable: a barrier that can never fully fire is not a path.
  defp reachable_nodes(graph) do
    expand_reachable(MapSet.new(), Map.values(graph.edges))
  end

  defp expand_reachable(reachable, edges) do
    expanded =
      Enum.reduce(edges, reachable, fn
        %Edge{from: from, to: to}, acc when is_binary(to) and to != @finish_id ->
          if edge_traversable?(from, acc), do: MapSet.put(acc, to), else: acc

        _edge, acc ->
          acc
      end)

    if MapSet.equal?(expanded, reachable) do
      reachable
    else
      expand_reachable(expanded, edges)
    end
  end

  defp edge_traversable?(@start_id, _reachable), do: true

  defp edge_traversable?(from, reachable) when is_binary(from) do
    MapSet.member?(reachable, from)
  end

  defp edge_traversable?(from, reachable) when is_list(from) and from != [] do
    Enum.all?(from, &(is_binary(&1) and MapSet.member?(reachable, &1)))
  end

  defp edge_traversable?(_from, _reachable), do: false

  # ---------------------------------------------------------------------------
  # 9.10 Cycles
  # ---------------------------------------------------------------------------

  defp analyze_cycles(graph, opts) do
    case cyclic_components(graph) do
      [] -> []
      cycles -> cycle_diagnostics(graph, opts, cycles)
    end
  end

  defp cycle_diagnostics(graph, opts, cycles) do
    case Policies.max_supersteps(graph, opts) do
      # An invalid policy already carries its own :invalid_policy error;
      # piling :unbounded_cycle on top would steer the user two ways at once.
      {:invalid, _value} ->
        []

      {:ok, nil} ->
        for component <- cycles do
          error(
            :unbounded_cycle,
            "graph contains a cycle through #{inspect(component)} with no max_supersteps limit; set the \"max_supersteps\" graph policy or a runtime default",
            path: [:policies, Policies.max_supersteps_key()],
            metadata: %{nodes: component}
          )
        end

      {:ok, _limit} ->
        for component <- cycles, not component_guarded?(graph, component) do
          warning(
            :unguarded_cycle,
            "cycle through #{inspect(component)} has no guarded edge; it will only halt at the max_supersteps limit",
            path: [:nodes, hd(component)],
            metadata: %{nodes: component}
          )
        end
    end
  end

  defp component_guarded?(graph, component) do
    members = MapSet.new(component)

    Enum.any?(graph.edges, fn {_id, edge} ->
      match?(%Edge{guard: %Guard{}}, edge) and
        edge_in_component?(edge, members)
    end)
  end

  defp edge_in_component?(%Edge{from: from, to: to}, members) do
    sources = if is_list(from), do: from, else: [from]

    is_binary(to) and MapSet.member?(members, to) and
      Enum.any?(sources, &(is_binary(&1) and MapSet.member?(members, &1)))
  end

  # Tarjan-style SCC detection over the node adjacency induced by edges.
  # Returns sorted lists of node IDs, one per cyclic component (including
  # self-loops), sorted by first member for determinism.
  defp cyclic_components(graph) do
    adjacency = adjacency(graph)

    {components, _state} =
      Enum.reduce(
        Enum.sort(Map.keys(adjacency)),
        {[], %{index: 0, indexes: %{}, lowlinks: %{}, stack: [], on_stack: MapSet.new()}},
        fn node, {components, state} ->
          if Map.has_key?(state.indexes, node) do
            {components, state}
          else
            {found, state} = strong_connect(node, adjacency, state)
            {components ++ found, state}
          end
        end
      )

    components
    |> Enum.filter(fn component ->
      length(component) > 1 or self_loop?(adjacency, component)
    end)
    |> Enum.map(&Enum.sort/1)
    |> Enum.sort()
  end

  defp self_loop?(adjacency, [node]) do
    node in Map.get(adjacency, node, [])
  end

  defp self_loop?(_adjacency, _component), do: false

  defp adjacency(graph) do
    base = Map.new(graph.nodes, fn {id, _node} -> {id, []} end)

    Enum.reduce(graph.edges, base, fn {_id, edge}, acc ->
      with %Edge{from: from, to: to} <- edge,
           true <- is_binary(to) and Map.has_key?(acc, to) do
        sources =
          if(is_list(from), do: from, else: [from])
          |> Enum.filter(&(is_binary(&1) and Map.has_key?(acc, &1)))

        Enum.reduce(sources, acc, fn source, inner ->
          Map.update!(inner, source, &Enum.uniq([to | &1]))
        end)
      else
        _other -> acc
      end
    end)
    |> Map.new(fn {node, targets} -> {node, Enum.sort(targets)} end)
  end

  defp strong_connect(node, adjacency, state) do
    state = %{
      state
      | indexes: Map.put(state.indexes, node, state.index),
        lowlinks: Map.put(state.lowlinks, node, state.index),
        index: state.index + 1,
        stack: [node | state.stack],
        on_stack: MapSet.put(state.on_stack, node)
    }

    {components, state} =
      Enum.reduce(Map.get(adjacency, node, []), {[], state}, fn target, {components, state} ->
        cond do
          not Map.has_key?(state.indexes, target) ->
            {found, state} = strong_connect(target, adjacency, state)

            state = %{
              state
              | lowlinks:
                  Map.put(
                    state.lowlinks,
                    node,
                    min(state.lowlinks[node], state.lowlinks[target])
                  )
            }

            {components ++ found, state}

          MapSet.member?(state.on_stack, target) ->
            state = %{
              state
              | lowlinks:
                  Map.put(state.lowlinks, node, min(state.lowlinks[node], state.indexes[target]))
            }

            {components, state}

          true ->
            {components, state}
        end
      end)

    if state.lowlinks[node] == state.indexes[node] do
      {component, rest} = pop_component(state.stack, node, [])

      state = %{
        state
        | stack: rest,
          on_stack: Enum.reduce(component, state.on_stack, &MapSet.delete(&2, &1))
      }

      {components ++ [component], state}
    else
      {components, state}
    end
  end

  defp pop_component([node | rest], node, acc), do: {[node | acc], rest}

  defp pop_component([head | rest], node, acc), do: pop_component(rest, node, [head | acc])

  # ---------------------------------------------------------------------------
  # 9.11 Termination (dead ends)
  # ---------------------------------------------------------------------------

  # The mirror of the topology pass. A graph with no edge to $finish can never
  # terminate normally: one graph-level warning says so, rather than flagging
  # every node as its own dead end. When a $finish edge exists, each node
  # reachable from $start that still cannot reach $finish traps any run that
  # enters it until the max_supersteps limit. Unreachable nodes are excluded —
  # they already carry :unreachable_node, and steering the user two ways at
  # once helps no one. A fully empty graph is left alone, matching topology.
  defp analyze_dead_ends(graph, _opts) do
    cond do
      map_size(graph.nodes) == 0 ->
        []

      not declares_finish_edge?(graph) ->
        [
          warning(
            :no_terminal_edge,
            "graph has no edge to $finish; no run can terminate normally and every run halts at the max_supersteps limit",
            path: [:edges]
          )
        ]

      true ->
        start_reachable = reachable_nodes(graph)
        finish_reaching = nodes_reaching_finish(graph)

        for node_id <- Enum.sort(Map.keys(graph.nodes)),
            MapSet.member?(start_reachable, node_id),
            not MapSet.member?(finish_reaching, node_id) do
          warning(
            :dead_end_node,
            "node #{inspect(node_id)} is reachable from $start but cannot reach $finish; runs entering it can only halt at the max_supersteps limit",
            path: [:nodes, node_id],
            public_id: node_id
          )
        end
    end
  end

  defp declares_finish_edge?(graph) do
    Enum.any?(graph.edges, fn {_id, edge} -> match?(%Edge{to: @finish_id}, edge) end)
  end

  # Backward reachability from $finish over the forward node adjacency. A
  # multi-source edge contributes each of its sources as able to reach the
  # target, mirroring adjacency/1: an over-approximation that keeps dead-end
  # warnings conservative (a barrier that still needs a sibling is not flagged).
  defp nodes_reaching_finish(graph) do
    expand_finish_reaching(direct_finishers(graph), finish_successors(graph))
  end

  defp expand_finish_reaching(reaching, successors) do
    expanded =
      Enum.reduce(successors, reaching, fn {node, targets}, acc ->
        if Enum.any?(targets, &MapSet.member?(acc, &1)), do: MapSet.put(acc, node), else: acc
      end)

    if MapSet.equal?(expanded, reaching) do
      reaching
    else
      expand_finish_reaching(expanded, successors)
    end
  end

  defp finish_successors(graph) do
    base = Map.new(graph.nodes, fn {id, _node} -> {id, []} end)

    Enum.reduce(graph.edges, base, fn {_id, edge}, acc ->
      with %Edge{from: from, to: to} <- edge,
           true <- is_binary(to) and to != @finish_id and Map.has_key?(base, to) do
        from
        |> source_list()
        |> Enum.filter(&(is_binary(&1) and Map.has_key?(base, &1)))
        |> Enum.reduce(acc, fn source, inner -> Map.update!(inner, source, &[to | &1]) end)
      else
        _other -> acc
      end
    end)
  end

  defp direct_finishers(graph) do
    Enum.reduce(graph.edges, MapSet.new(), fn
      {_id, %Edge{from: from, to: @finish_id}}, acc ->
        from
        |> source_list()
        |> Enum.filter(&is_binary/1)
        |> Enum.reduce(acc, &MapSet.put(&2, &1))

      {_id, _edge}, acc ->
        acc
    end)
  end

  defp source_list(from) when is_list(from), do: from
  defp source_list(from), do: [from]

  # ---------------------------------------------------------------------------
  # Shared helpers
  # ---------------------------------------------------------------------------

  defp sorted(map) when is_map(map), do: Enum.sort_by(map, fn {id, _record} -> id end)

  defp singular(:inputs), do: "input"
  defp singular(:fields), do: "field"
  defp singular(:outputs), do: "output"
  defp singular(:nodes), do: "node"
  defp singular(:edges), do: "edge"
end
