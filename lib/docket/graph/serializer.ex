defmodule Docket.Graph.Serializer do
  @moduledoc false

  # Internal implementation of the JSON-safe map interchange for `Docket.Graph`
  # documents. The only public entry/exit points are `Docket.Graph.to_map/2`,
  # `Docket.Graph.from_map/2`, and `Docket.Graph.from_map!/2`, which delegate
  # here. Do not call this module from host applications.
  #
  # This module owns two related concerns:
  #
  # - `dump/2` produces a plain, JSON-safe map (the v1 wire format) from an
  #   in-memory `Docket.Graph`. All keys are binaries and all values are
  #   durable JSON-safe terms (binaries, numbers, booleans, nil, lists,
  #   string-keyed maps).
  # - `load!/2` reconstructs a `Docket.Graph` from a wire map. It validates the
  #   document shape strictly and never creates new atoms from untrusted
  #   strings.
  #
  # Executable node implementations resolve through an explicit host registry
  # passed as the `:implementations` option (a map of stable string identifiers
  # to module implementations). `dump/2` looks up a node's normalized
  # `%{type: :module, module: M, function: F}` implementation in the reverse
  # registry and emits only the identifier; `load!/2` maps an identifier back to
  # the registered implementation. No module, function, or type name is ever
  # converted to an atom on load.
  #
  # dump/2 canonicalizes open content with Jason-style coercion: atoms become
  # strings (both map keys and values), silently. Terms with no JSON
  # representation - tuples (including keyword lists), pids, refs, functions,
  # `MapSet`s, and structs - are rejected with `:non_durable_value`. The wire
  # document therefore always contains only binaries, numbers, booleans,
  # `nil`, lists, and string-keyed maps.
  #
  # `to_map(from_map!(map)) == map` holds for any document dump/2 produces.
  # Map keys starting with "$" are reserved for wire-format tags (for example
  # the "$guard" wrapper for guard expressions nested in plain argument
  # positions).

  alias Docket.Graph
  alias Docket.Graph.{Edge, Error, Field, Node, Output}
  alias Docket.{Guard, Reducer, Schema}

  @schema_version 1

  @start_id "$start"
  @finish_id "$finish"
  @id_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_-]*$/

  @field_kinds %{"input" => :input, "state" => :state}
  @field_kinds_out Map.new(@field_kinds, fn {k, v} -> {v, k} end)

  @schema_types %{
    "string" => :string,
    "float" => :float,
    "integer" => :integer,
    "boolean" => :boolean,
    "map" => :map,
    "list" => :list,
    "object" => :object,
    "enum" => :enum
  }
  @schema_types_out Map.new(@schema_types, fn {k, v} -> {v, k} end)

  @reducer_types %{
    "append" => :append,
    "first_value" => :first_value,
    "last_value" => :last_value,
    "merge" => :merge,
    "sum" => :sum,
    "union" => :union
  }
  @reducer_types_out Map.new(@reducer_types, fn {k, v} -> {v, k} end)

  @guard_ops %{
    "all" => :all,
    "any" => :any,
    "changed" => :changed,
    "equals" => :equals,
    "exists" => :exists,
    "not" => :not,
    "path" => :path,
    "version_at_least" => :version_at_least
  }
  @guard_ops_out Map.new(@guard_ops, fn {k, v} -> {v, k} end)
  @guard_recursive_ops [:all, :any, :not]

  @graph_keys ~w(schema_version id name description inputs fields outputs nodes edges policies metadata)
  @field_keys ~w(kind label description schema reducer required default metadata)
  @node_keys ~w(label description implementation branches config policies metadata)
  @edge_keys ~w(from to label description source_handle target_handle guard metadata)
  @output_keys ~w(source label description schema metadata)
  @schema_keys ~w(type fields item values required default constraints metadata)
  @reducer_keys ~w(type opts)
  @guard_keys ~w(op args)

  @module_impl_keys ~w(type implementation)

  # ---------------------------------------------------------------------------
  # Wire format (dump / load) - exposed as Docket.Graph.to_map/from_map
  # ---------------------------------------------------------------------------

  @doc false
  @spec dump(Graph.t(), keyword()) :: map()
  def dump(%Graph{} = graph, opts \\ []) do
    registry = build_registry!(opts)

    %{"schema_version" => @schema_version, "id" => graph.id}
    |> put_present_string("name", graph.name)
    |> put_present_string("description", graph.description)
    |> put_collection("inputs", graph.inputs, &dump_field/1)
    |> put_collection("fields", graph.fields, &dump_field/1)
    |> put_collection("outputs", graph.outputs, &dump_output/1)
    |> put_collection("nodes", graph.nodes, &dump_node(&1, registry))
    |> put_collection("edges", graph.edges, &dump_edge/1)
    |> put_open_map("policies", graph.policies)
    |> put_open_map("metadata", graph.metadata)
  end

  @doc false
  @spec load!(map(), keyword()) :: Graph.t()
  def load!(map, opts \\ []) do
    registry = build_registry!(opts)

    unless is_map(map) and not is_struct(map) do
      invalid!(:invalid_document, "graph document must be a plain map, got #{inspect(map)}")
    end

    assert_string_keys!(map, "graph document")
    assert_known_keys!(map, @graph_keys, "graph")

    version = load_schema_version!(map)
    id = fetch_required!(map, "id", "graph")
    assert_public_id!(id, :graph_id)

    %Graph{
      id: id,
      schema_version: version,
      name: load_optional_string!(map, "name", "graph"),
      description: load_optional_string!(map, "description", "graph"),
      inputs: load_collection!(map, "inputs", :input_id, &load_field(&1, &2, :input)),
      fields: load_collection!(map, "fields", :field_id, &load_field(&1, &2, :state)),
      outputs: load_collection!(map, "outputs", :output_id, &load_output/2),
      nodes: load_collection!(map, "nodes", :node_id, &load_node(&1, &2, registry)),
      edges: load_collection!(map, "edges", :edge_id, &load_edge/2),
      policies: load_open_map!(map, "policies", "graph"),
      metadata: load_open_map!(map, "metadata", "graph"),
      diagnostics: []
    }
  end

  # ---------------------------------------------------------------------------
  # Implementation registry
  # ---------------------------------------------------------------------------

  defp build_registry!(opts) do
    raw =
      case Keyword.get(opts, :implementations, %{}) do
        map when is_map(map) and not is_struct(map) ->
          map

        other ->
          invalid!(
            :invalid_registry,
            "implementations must be a map of identifiers to implementations, got #{inspect(other)}"
          )
      end

    {forward, reverse} =
      Enum.reduce(raw, {%{}, %{}}, fn {id, value}, {forward, reverse} ->
        assert_registry_id!(id)
        impl = normalize_registry_value!(id, value)
        key = {impl.module, impl.function}

        case Map.fetch(reverse, key) do
          {:ok, existing_id} ->
            invalid!(
              :invalid_registry,
              "implementations #{inspect(id)} and #{inspect(existing_id)} both resolve to " <>
                "#{inspect(key)}; identifiers must map to distinct implementations",
              %{identifiers: [existing_id, id], implementation: inspect(key)}
            )

          :error ->
            {Map.put(forward, id, impl), Map.put(reverse, key, id)}
        end
      end)

    %{forward: forward, reverse: reverse}
  end

  defp assert_registry_id!(id) when is_binary(id) do
    cond do
      id == "" ->
        invalid!(:invalid_registry, "implementation identifiers must be non-empty strings")

      String.starts_with?(id, "$") ->
        invalid!(
          :invalid_registry,
          "implementation identifier #{inspect(id)} must not start with \"$\""
        )

      true ->
        :ok
    end
  end

  defp assert_registry_id!(other) do
    invalid!(
      :invalid_registry,
      "implementation identifiers must be strings, got #{inspect(other)}"
    )
  end

  defp normalize_registry_value!(_id, module) when is_atom(module) and not is_nil(module) do
    %{type: :module, module: module, function: :call}
  end

  defp normalize_registry_value!(_id, {module, function})
       when is_atom(module) and not is_nil(module) and is_atom(function) do
    %{type: :module, module: module, function: function}
  end

  defp normalize_registry_value!(id, %{type: :module} = impl) do
    module = Map.get(impl, :module)
    function = Map.get(impl, :function, :call)

    cond do
      not (is_atom(module) and not is_nil(module)) ->
        invalid!(
          :invalid_registry,
          "implementation #{inspect(id)} module must be a module atom, got #{inspect(module)}"
        )

      not is_atom(function) ->
        invalid!(
          :invalid_registry,
          "implementation #{inspect(id)} function must be an atom, got #{inspect(function)}"
        )

      true ->
        %{type: :module, module: module, function: function}
    end
  end

  defp normalize_registry_value!(id, other) do
    invalid!(
      :invalid_registry,
      "implementation #{inspect(id)} must be a module, {module, function}, or module " <>
        "implementation map, got #{inspect(other)}"
    )
  end

  # ---------------------------------------------------------------------------
  # Dump helpers
  # ---------------------------------------------------------------------------

  defp dump_field(%Field{} = field) do
    kind = lookup!(@field_kinds_out, field.kind, :invalid_field, "field kind")

    %{"kind" => kind}
    |> put_present_string("label", field.label)
    |> put_present_string("description", field.description)
    |> put_present("schema", dump_schema(field.schema))
    |> put_present("reducer", dump_reducer(field.reducer))
    |> put_true("required", field.required)
    |> put_present("default", durable!(field.default))
    |> put_open_map("metadata", field.metadata)
  end

  defp dump_output(%Output{} = output) do
    %{}
    |> put_present_string("source", output.source)
    |> put_present_string("label", output.label)
    |> put_present_string("description", output.description)
    |> put_present("schema", dump_schema(output.schema))
    |> put_open_map("metadata", output.metadata)
  end

  defp dump_node(%Node{} = node, registry) do
    %{}
    |> put_present_string("label", node.label)
    |> put_present_string("description", node.description)
    |> put_present("implementation", dump_implementation(node.implementation, registry))
    |> put_open_map("branches", dump_branches(node.branches))
    |> put_open_map("config", node.config)
    |> put_open_map("policies", node.policies)
    |> put_open_map("metadata", node.metadata)
  end

  defp dump_edge(%Edge{} = edge) do
    %{}
    |> put_present_endpoint("from", edge.from)
    |> put_present_endpoint("to", edge.to)
    |> put_present_string("label", edge.label)
    |> put_present_string("description", edge.description)
    |> put_present_string("source_handle", edge.source_handle)
    |> put_present_string("target_handle", edge.target_handle)
    |> put_present("guard", dump_guard(edge.guard))
    |> put_open_map("metadata", edge.metadata)
  end

  defp dump_schema(nil), do: nil

  defp dump_schema(%Schema{} = schema) do
    type = lookup!(@schema_types_out, schema.type, :invalid_schema, "schema type")

    %{"type" => type}
    |> put_present("fields", dump_schema_fields(schema.fields))
    |> put_present("item", dump_schema(schema.item))
    |> put_present("values", dump_schema_values(schema.values))
    |> put_true("required", schema.required)
    |> put_schema_default(schema.default)
    |> put_open_map("constraints", schema.constraints)
    |> put_open_map("metadata", schema.metadata)
  end

  defp dump_schema(other) do
    invalid!(:invalid_schema, "schema must be a Docket.Schema or nil, got #{inspect(other)}")
  end

  defp dump_schema_fields(fields) when map_size(fields) == 0, do: nil

  defp dump_schema_fields(fields) when is_map(fields) do
    Map.new(fields, fn {name, schema} -> {durable_key!(name), dump_schema(schema)} end)
  end

  defp dump_schema_fields(other) do
    invalid!(:invalid_schema, "schema fields must be a map, got #{inspect(other)}")
  end

  defp dump_schema_values([]), do: nil
  defp dump_schema_values(values) when is_list(values), do: Enum.map(values, &durable!/1)

  defp dump_schema_values(other) do
    invalid!(:invalid_schema, "schema values must be a list, got #{inspect(other)}")
  end

  defp dump_reducer(nil), do: nil

  defp dump_reducer(%Reducer{} = reducer) do
    type = lookup!(@reducer_types_out, reducer.type, :invalid_reducer, "reducer type")

    %{"type" => type}
    |> put_open_map("opts", reducer.opts)
  end

  defp dump_reducer(other) do
    invalid!(:invalid_reducer, "reducer must be a Docket.Reducer or nil, got #{inspect(other)}")
  end

  defp dump_guard(nil), do: nil

  defp dump_guard(%Guard{op: op, args: args}) when is_list(args) do
    op_string = lookup!(@guard_ops_out, op, :invalid_guard, "guard op")
    %{"op" => op_string, "args" => dump_guard_args(op, args)}
  end

  defp dump_guard(other) do
    invalid!(:invalid_guard, "guard must be a Docket.Guard or nil, got #{inspect(other)}")
  end

  defp dump_guard_args(op, args) when op in @guard_recursive_ops do
    Enum.map(args, fn
      %Guard{} = arg ->
        dump_guard(arg)

      other ->
        non_durable!(other, "guard #{op} arguments must be Docket.Guard structs")
    end)
  end

  # Plain argument positions (changed/equals/exists/path/version_at_least) may
  # reference nested guard expressions, e.g. equals(path(...), value). Nested
  # guards are wrapped in a reserved "$guard" tag; "$"-prefixed keys are
  # rejected in durable values.
  defp dump_guard_args(_op, args) do
    Enum.map(args, fn
      %Guard{} = arg -> %{"$guard" => dump_guard(arg)}
      other -> durable!(other)
    end)
  end

  # An executable module implementation is emitted as only its registered
  # identifier; module and function names never reach the wire. Any other map
  # is a passthrough implementation and round-trips as a plain durable value.
  defp dump_implementation(nil, _registry), do: nil

  defp dump_implementation(%{type: :module} = impl, registry) do
    case Map.keys(impl) -- [:type, :module, :function] do
      [] ->
        :ok

      extra ->
        invalid!(
          :invalid_implementation,
          "module implementations support only :type, :module, and :function, " <>
            "got extra keys #{inspect(extra)}"
        )
    end

    module = fetch_module!(impl)
    function = Map.get(impl, :function) || :call

    case Map.fetch(registry.reverse, {module, function}) do
      {:ok, identifier} ->
        %{"type" => "module", "implementation" => identifier}

      :error ->
        invalid!(
          :unregistered_implementation,
          "node implementation #{inspect({module, function})} is not registered; add it to " <>
            ":implementations to serialize this graph",
          %{module: inspect(module), function: function}
        )
    end
  end

  defp dump_implementation(%{"type" => "module"} = impl, _registry) when not is_struct(impl) do
    invalid!(
      :invalid_implementation,
      "passthrough implementation #{inspect(impl)} uses the reserved \"type\" => \"module\" " <>
        "tag; the module tag is reserved for registered module implementations"
    )
  end

  defp dump_implementation(%{} = impl, _registry) when not is_struct(impl), do: durable!(impl)

  defp dump_implementation(other, _registry) do
    invalid!(
      :invalid_implementation,
      "implementation must be a map or nil, got #{inspect(other)}"
    )
  end

  defp dump_branches(branches) when is_map(branches) and map_size(branches) == 0, do: %{}

  defp dump_branches(branches) when is_map(branches) and not is_struct(branches) do
    Map.new(branches, fn {name, group} -> {durable_key!(name), dump_branch_group(group)} end)
  end

  defp dump_branches(other) do
    non_durable!(other, "branches must be a map")
  end

  defp dump_branch_group(group) when is_list(group), do: Enum.map(group, &durable!/1)
  defp dump_branch_group(group) when is_map(group) and not is_struct(group), do: durable!(group)

  defp dump_branch_group(group) do
    non_durable!(group, "branch group must be a list of edge IDs or a string-keyed map")
  end

  # ---------------------------------------------------------------------------
  # Load helpers
  # ---------------------------------------------------------------------------

  defp load_schema_version!(map) do
    case Map.fetch(map, "schema_version") do
      {:ok, version} when is_integer(version) and version > 0 ->
        if version > @schema_version do
          invalid!(
            :unsupported_schema_version,
            "graph document schema_version #{version} is newer than supported version #{@schema_version}",
            %{schema_version: version, supported: @schema_version}
          )
        end

        version

      {:ok, other} ->
        invalid!(
          :invalid_document,
          "graph schema_version must be a positive integer, got #{inspect(other)}"
        )

      :error ->
        invalid!(:invalid_document, "graph document is missing required key \"schema_version\"")
    end
  end

  defp load_field(id, map, default_kind) do
    assert_string_keys!(map, "field #{inspect(id)}")
    assert_known_keys!(map, @field_keys, "field #{inspect(id)}")

    kind =
      case Map.fetch(map, "kind") do
        {:ok, value} -> lookup!(@field_kinds, value, :invalid_document, "field kind")
        :error -> default_kind
      end

    %Field{
      id: id,
      kind: kind,
      label: load_optional_string!(map, "label", "field #{inspect(id)}"),
      description: load_optional_string!(map, "description", "field #{inspect(id)}"),
      schema: load_schema!(Map.get(map, "schema"), "field #{inspect(id)}"),
      reducer: load_reducer!(Map.get(map, "reducer"), "field #{inspect(id)}"),
      required: load_bool!(map, "required", "field #{inspect(id)}"),
      default: load_durable_value!(Map.get(map, "default"), "field #{inspect(id)} default"),
      metadata: load_open_map!(map, "metadata", "field #{inspect(id)}")
    }
  end

  defp load_output(id, map) do
    assert_string_keys!(map, "output #{inspect(id)}")
    assert_known_keys!(map, @output_keys, "output #{inspect(id)}")

    %Output{
      id: id,
      source: load_optional_string!(map, "source", "output #{inspect(id)}"),
      label: load_optional_string!(map, "label", "output #{inspect(id)}"),
      description: load_optional_string!(map, "description", "output #{inspect(id)}"),
      schema: load_schema!(Map.get(map, "schema"), "output #{inspect(id)}"),
      metadata: load_open_map!(map, "metadata", "output #{inspect(id)}")
    }
  end

  defp load_node(id, map, registry) do
    assert_string_keys!(map, "node #{inspect(id)}")
    assert_known_keys!(map, @node_keys, "node #{inspect(id)}")

    %Node{
      id: id,
      label: load_optional_string!(map, "label", "node #{inspect(id)}"),
      description: load_optional_string!(map, "description", "node #{inspect(id)}"),
      implementation: load_implementation!(Map.get(map, "implementation"), id, registry),
      branches: load_branches!(Map.get(map, "branches"), id),
      config: load_open_map!(map, "config", "node #{inspect(id)}"),
      policies: load_open_map!(map, "policies", "node #{inspect(id)}"),
      metadata: load_open_map!(map, "metadata", "node #{inspect(id)}")
    }
  end

  defp load_edge(id, map) do
    assert_string_keys!(map, "edge #{inspect(id)}")
    assert_known_keys!(map, @edge_keys, "edge #{inspect(id)}")

    %Edge{
      id: id,
      from: load_endpoint!(Map.get(map, "from"), "edge #{inspect(id)} from"),
      to: load_endpoint!(Map.get(map, "to"), "edge #{inspect(id)} to"),
      label: load_optional_string!(map, "label", "edge #{inspect(id)}"),
      description: load_optional_string!(map, "description", "edge #{inspect(id)}"),
      source_handle: load_optional_string!(map, "source_handle", "edge #{inspect(id)}"),
      target_handle: load_optional_string!(map, "target_handle", "edge #{inspect(id)}"),
      guard: load_guard!(Map.get(map, "guard"), "edge #{inspect(id)}"),
      metadata: load_open_map!(map, "metadata", "edge #{inspect(id)}")
    }
  end

  defp load_schema!(nil, _location), do: nil

  defp load_schema!(map, location) when is_map(map) and not is_struct(map) do
    assert_string_keys!(map, "schema in #{location}")
    assert_known_keys!(map, @schema_keys, "schema in #{location}")

    type =
      lookup!(
        @schema_types,
        fetch_required!(map, "type", "schema"),
        :invalid_document,
        "schema type"
      )

    %Schema{
      type: type,
      fields: load_schema_fields!(Map.get(map, "fields"), location),
      item: load_schema!(Map.get(map, "item"), location),
      values: load_list!(Map.get(map, "values"), "schema values in #{location}"),
      required: load_bool_value!(Map.get(map, "required"), "schema required in #{location}"),
      default: load_schema_default(map, location),
      constraints:
        load_open_map_value!(Map.get(map, "constraints"), "schema constraints in #{location}"),
      metadata: load_open_map_value!(Map.get(map, "metadata"), "schema metadata in #{location}")
    }
  end

  defp load_schema!(other, location) do
    invalid!(:invalid_document, "schema in #{location} must be a map, got #{inspect(other)}")
  end

  defp load_schema_fields!(nil, _location), do: %{}

  defp load_schema_fields!(fields, location) when is_map(fields) and not is_struct(fields) do
    assert_string_keys!(fields, "schema fields in #{location}")

    Map.new(fields, fn {name, schema} ->
      key = load_durable_key!(name, "schema fields in #{location}")
      {key, load_schema!(schema, "#{location}.#{name}")}
    end)
  end

  defp load_schema_fields!(other, location) do
    invalid!(
      :invalid_document,
      "schema fields in #{location} must be a map, got #{inspect(other)}"
    )
  end

  defp load_schema_default(map, location) do
    case Map.fetch(map, "default") do
      {:ok, value} -> load_durable_value!(value, "schema default in #{location}")
      :error -> Schema.no_default()
    end
  end

  defp load_reducer!(nil, _location), do: nil

  defp load_reducer!(map, location) when is_map(map) and not is_struct(map) do
    assert_string_keys!(map, "reducer in #{location}")
    assert_known_keys!(map, @reducer_keys, "reducer in #{location}")

    type =
      lookup!(
        @reducer_types,
        fetch_required!(map, "type", "reducer"),
        :invalid_document,
        "reducer type"
      )

    %Reducer{
      type: type,
      opts: load_open_map_value!(Map.get(map, "opts"), "reducer opts in #{location}")
    }
  end

  defp load_reducer!(other, location) do
    invalid!(:invalid_document, "reducer in #{location} must be a map, got #{inspect(other)}")
  end

  defp load_guard!(nil, _location), do: nil

  defp load_guard!(map, location) when is_map(map) and not is_struct(map) do
    assert_string_keys!(map, "guard in #{location}")
    assert_known_keys!(map, @guard_keys, "guard in #{location}")
    op = lookup!(@guard_ops, fetch_required!(map, "op", "guard"), :invalid_document, "guard op")
    args = Map.get(map, "args", [])

    unless is_list(args) do
      invalid!(
        :invalid_document,
        "guard args in #{location} must be a list, got #{inspect(args)}"
      )
    end

    %Guard{op: op, args: load_guard_args!(op, args, location)}
  end

  defp load_guard!(other, location) do
    invalid!(:invalid_document, "guard in #{location} must be a map, got #{inspect(other)}")
  end

  defp load_guard_args!(op, args, location) when op in @guard_recursive_ops do
    Enum.map(args, &load_guard!(&1, location))
  end

  defp load_guard_args!(_op, args, location) do
    Enum.map(args, fn
      %{"$guard" => inner} = wrapper when map_size(wrapper) == 1 ->
        load_guard!(inner, location)

      other ->
        load_durable_value!(other, "guard args in #{location}")
    end)
  end

  defp load_implementation!(nil, _id, _registry), do: nil

  defp load_implementation!(map, id, registry) when is_map(map) and not is_struct(map) do
    assert_string_keys!(map, "node #{inspect(id)} implementation")

    case Map.get(map, "type") do
      "module" -> load_module_implementation!(map, id, registry)
      _other -> load_durable_value!(map, "node #{inspect(id)} implementation")
    end
  end

  defp load_implementation!(other, id, _registry) do
    invalid!(
      :invalid_document,
      "node #{inspect(id)} implementation must be a map, got #{inspect(other)}"
    )
  end

  defp load_module_implementation!(map, id, registry) do
    assert_known_keys!(map, @module_impl_keys, "node #{inspect(id)} implementation")
    identifier = fetch_required!(map, "implementation", "node #{inspect(id)} implementation")

    unless is_binary(identifier) do
      invalid!(
        :invalid_document,
        "node #{inspect(id)} implementation identifier must be a string, got #{inspect(identifier)}"
      )
    end

    case Map.fetch(registry.forward, identifier) do
      {:ok, impl} ->
        impl

      :error ->
        invalid!(
          :unknown_implementation,
          "node #{inspect(id)} references unregistered implementation #{inspect(identifier)}; " <>
            "add it to :implementations to load this document",
          %{node_id: id, identifier: identifier}
        )
    end
  end

  defp load_branches!(nil, _id), do: %{}

  defp load_branches!(branches, id) when is_map(branches) and not is_struct(branches) do
    assert_string_keys!(branches, "node #{inspect(id)} branches")

    Map.new(branches, fn {name, group} ->
      key = load_durable_key!(name, "node #{inspect(id)} branches")
      {key, load_branch_group!(group, id, name)}
    end)
  end

  defp load_branches!(other, id) do
    invalid!(
      :invalid_document,
      "node #{inspect(id)} branches must be a map, got #{inspect(other)}"
    )
  end

  defp load_branch_group!(group, id, name) when is_list(group) do
    load_durable_value!(group, "node #{inspect(id)} branch #{inspect(name)}")
  end

  defp load_branch_group!(group, id, name) when is_map(group) and not is_struct(group) do
    load_durable_value!(group, "node #{inspect(id)} branch #{inspect(name)}")
  end

  defp load_branch_group!(other, id, name) do
    invalid!(
      :invalid_document,
      "node #{inspect(id)} branch #{inspect(name)} must be a list or map, got #{inspect(other)}"
    )
  end

  defp load_endpoint!(nil, _location), do: nil
  defp load_endpoint!(value, _location) when is_binary(value), do: value

  defp load_endpoint!(list, location) when is_list(list) do
    Enum.each(list, fn
      value when is_binary(value) ->
        :ok

      other ->
        invalid!(:invalid_document, "#{location} entries must be strings, got #{inspect(other)}")
    end)

    list
  end

  defp load_endpoint!(other, location) do
    invalid!(
      :invalid_document,
      "#{location} must be a string or list of strings, got #{inspect(other)}"
    )
  end

  # ---------------------------------------------------------------------------
  # Collection helpers (dump)
  # ---------------------------------------------------------------------------

  defp put_collection(map, _key, collection, _fun) when map_size(collection) == 0, do: map

  defp put_collection(map, key, collection, fun) do
    Map.put(map, key, Map.new(collection, fn {id, record} -> {id, fun.(record)} end))
  end

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)

  defp put_present_string(map, _key, nil), do: map
  defp put_present_string(map, key, value) when is_binary(value), do: Map.put(map, key, value)

  defp put_present_string(_map, key, other) do
    invalid!(:invalid_attrs, "#{key} must be a string or nil, got #{inspect(other)}")
  end

  defp put_present_endpoint(map, _key, nil), do: map

  defp put_present_endpoint(map, key, value) when is_binary(value), do: Map.put(map, key, value)

  defp put_present_endpoint(map, key, list) when is_list(list) do
    Enum.each(list, fn
      value when is_binary(value) -> :ok
      other -> invalid!(:invalid_attrs, "#{key} entries must be strings, got #{inspect(other)}")
    end)

    Map.put(map, key, list)
  end

  defp put_present_endpoint(_map, key, other) do
    invalid!(:invalid_attrs, "#{key} must be a string or list of strings, got #{inspect(other)}")
  end

  defp put_true(map, _key, false), do: map
  defp put_true(map, key, true), do: Map.put(map, key, true)

  defp put_true(_map, key, other) do
    invalid!(:invalid_attrs, "#{key} must be a boolean, got #{inspect(other)}")
  end

  defp put_open_map(map, _key, value) when value == %{}, do: map

  defp put_open_map(map, key, value) when is_map(value) and not is_struct(value),
    do: Map.put(map, key, durable!(value))

  defp put_open_map(_map, key, other) do
    non_durable!(other, "#{key} must be a string-keyed map")
  end

  defp put_schema_default(map, default) do
    if default == Schema.no_default() do
      map
    else
      Map.put(map, "default", durable!(default))
    end
  end

  # ---------------------------------------------------------------------------
  # Collection helpers (load)
  # ---------------------------------------------------------------------------

  defp load_collection!(map, key, label, fun) do
    case Map.get(map, key) do
      nil ->
        %{}

      collection when is_map(collection) and not is_struct(collection) ->
        assert_string_keys!(collection, key)

        Map.new(collection, fn {id, record} ->
          assert_public_id!(id, label)

          unless is_map(record) and not is_struct(record) do
            invalid!(
              :invalid_document,
              "#{key} entry #{inspect(id)} must be a map, got #{inspect(record)}"
            )
          end

          {id, fun.(id, record)}
        end)

      other ->
        invalid!(:invalid_document, "#{key} must be a map, got #{inspect(other)}")
    end
  end

  defp load_open_map!(map, key, location) do
    load_open_map_value!(Map.get(map, key), "#{location} #{key}")
  end

  defp load_open_map_value!(nil, _location), do: %{}

  defp load_open_map_value!(value, location) when is_map(value) and not is_struct(value) do
    load_durable_value!(value, location)
  end

  defp load_open_map_value!(other, location) do
    invalid!(:invalid_document, "#{location} must be a map, got #{inspect(other)}")
  end

  defp load_durable_value!(value, _location)
       when is_binary(value) or is_integer(value) or is_float(value) or is_boolean(value) or
              is_nil(value),
       do: value

  defp load_durable_value!(list, location) when is_list(list) do
    load_durable_list!(list, location)
  end

  defp load_durable_value!(map, location) when is_map(map) and not is_struct(map) do
    Map.new(map, fn {key, value} ->
      {load_durable_key!(key, location), load_durable_value!(value, location)}
    end)
  end

  defp load_durable_value!(other, location) do
    invalid!(
      :invalid_document,
      "#{location} contains a non-durable value #{inspect(other)}",
      %{location: location, value: inspect(other)}
    )
  end

  defp load_durable_list!([], _location), do: []

  defp load_durable_list!([head | tail], location) when is_list(tail) do
    [load_durable_value!(head, location) | load_durable_list!(tail, location)]
  end

  defp load_durable_list!([_head | tail], location) do
    invalid!(
      :non_durable_value,
      "#{location} contains an improper list with non-list tail #{inspect(tail)}",
      %{location: location, value: inspect(tail)}
    )
  end

  defp load_durable_key!(key, location) when is_binary(key) do
    if String.starts_with?(key, "$") do
      invalid!(
        :invalid_document,
        "#{location} map keys starting with \"$\" are reserved, got #{inspect(key)}",
        %{location: location, key: key}
      )
    else
      key
    end
  end

  defp load_durable_key!(key, location) do
    invalid!(
      :invalid_document,
      "#{location} map keys must be strings, got #{inspect(key)}",
      %{location: location, key: key}
    )
  end

  defp load_list!(nil, _location), do: []

  defp load_list!(list, location) when is_list(list) do
    Enum.map(list, &load_durable_value!(&1, location))
  end

  defp load_list!(other, location) do
    invalid!(:invalid_document, "#{location} must be a list, got #{inspect(other)}")
  end

  defp load_optional_string!(map, key, location) do
    case Map.get(map, key) do
      nil ->
        nil

      value when is_binary(value) ->
        value

      other ->
        invalid!(:invalid_document, "#{location} #{key} must be a string, got #{inspect(other)}")
    end
  end

  defp load_bool!(map, key, location) do
    load_bool_value!(Map.get(map, key), "#{location} #{key}")
  end

  defp load_bool_value!(nil, _location), do: false
  defp load_bool_value!(value, _location) when is_boolean(value), do: value

  defp load_bool_value!(other, location) do
    invalid!(:invalid_document, "#{location} must be a boolean, got #{inspect(other)}")
  end

  defp fetch_required!(map, key, location) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        invalid!(
          :invalid_document,
          "#{location} document is missing required key #{inspect(key)}"
        )
    end
  end

  defp lookup!(table, value, code, label) do
    case Map.fetch(table, value) do
      {:ok, mapped} ->
        mapped

      :error ->
        invalid!(code, "unknown #{label} #{inspect(value)}", %{label: label, value: value})
    end
  end

  # ---------------------------------------------------------------------------
  # Document validation helpers
  # ---------------------------------------------------------------------------

  defp assert_string_keys!(map, location) do
    Enum.each(Map.keys(map), fn
      key when is_binary(key) ->
        :ok

      other ->
        invalid!(
          :invalid_document,
          "#{location} keys must be strings, got #{inspect(other)}",
          %{location: location, key: other}
        )
    end)
  end

  defp assert_known_keys!(map, allowed, location) do
    Enum.each(Map.keys(map), fn key ->
      unless key in allowed do
        invalid!(:invalid_document, "unknown #{location} key #{inspect(key)}", %{
          location: location,
          key: key
        })
      end
    end)
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

  defp fetch_module!(%{module: module}) when is_atom(module), do: module

  defp fetch_module!(impl) do
    non_durable!(impl, "module implementation must have an atom :module")
  end

  # ---------------------------------------------------------------------------
  # Durable value core
  # ---------------------------------------------------------------------------

  defp durable!(value)
       when is_binary(value) or is_integer(value) or is_float(value) or is_boolean(value) or
              is_nil(value),
       do: value

  defp durable!(atom) when is_atom(atom), do: Atom.to_string(atom)

  defp durable!(list) when is_list(list), do: durable_list!(list)

  defp durable!(%_struct{} = struct) do
    non_durable!(struct, "structs are not durable values")
  end

  defp durable!(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {durable_key!(key), durable!(value)} end)
  end

  defp durable!(value) do
    non_durable!(value, durable_hint(value))
  end

  defp durable_list!([]), do: []

  defp durable_list!([head | tail]) when is_list(tail) do
    [durable!(head) | durable_list!(tail)]
  end

  defp durable_list!([_head | tail]) do
    non_durable!(tail, "improper lists are not durable")
  end

  defp durable_key!(key) when is_binary(key) do
    if String.starts_with?(key, "$") do
      non_durable!(key, "map keys starting with \"$\" are reserved for the wire format")
    else
      key
    end
  end

  defp durable_key!(key) when is_atom(key), do: durable_key!(Atom.to_string(key))

  defp durable_key!(key) do
    non_durable!(key, "map keys must be strings or atoms")
  end

  defp durable_hint(value) when is_list(value), do: "lists must contain only durable values"

  defp durable_hint(value) when is_tuple(value) do
    "tuples and keyword lists are not durable; keyword lists should become string-keyed maps"
  end

  defp durable_hint(_value), do: "value is not durable"

  defp non_durable!(value, hint) do
    invalid!(
      :non_durable_value,
      "graph contains a non-durable value #{inspect(value)}: #{hint}",
      %{value: inspect(value), hint: hint}
    )
  end

  defp invalid!(code, message, details \\ %{}) do
    raise Error, code: code, message: message, details: details
  end
end
