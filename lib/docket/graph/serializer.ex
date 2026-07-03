defmodule Docket.Graph.Serializer do
  @moduledoc false

  # Internal implementation of the canonical wire serialization for
  # `Docket.Graph` documents. The only public entry/exit points for graph
  # documents are `Docket.Graph.to_map/2` and `Docket.Graph.from_map/2` (and
  # `from_map!/2`), which delegate here. Do not call this module from host
  # applications.
  #
  # This module owns three related concerns:
  #
  # - `dump/2` produces a plain, JSON-safe map (the v1 wire format) from an
  #   in-memory `Docket.Graph`. All keys are binaries and all values are
  #   durable JSON-safe terms (binaries, numbers, booleans, nil, lists,
  #   string-keyed maps).
  # - `load!/2` reconstructs a `Docket.Graph` from a wire map. It validates the
  #   document shape strictly and never creates new atoms from untrusted
  #   strings.
  # - `canonical_json_encode/1` renders the wire map to a deterministic,
  #   compact JSON binary used as the input to the graph hash.
  #
  # The module also owns the durable value normalization used at edit time. The
  # editing API normalizes "open" content (metadata, policies, config,
  # constraints, defaults, enum values, guard args, branch groups, and
  # non-reserved implementation entries) so the in-memory struct is always in
  # canonical durable form. This makes the round-trip law
  # `from_map!(to_map(graph)) == graph` hold on struct equality.
  #
  # Durable values are: binaries, integers, floats, booleans, `nil`, lists of
  # durable values, and string-keyed maps of durable values. Atoms are
  # converted to strings via `Atom.to_string/1` for both keys and values. Map
  # keys starting with "$" are reserved for wire-format tags (for example the
  # "$guard" wrapper for guard expressions nested in plain argument positions).
  # Anything else - tuples (including keyword lists), pids, refs, functions,
  # `MapSet`s, and structs of any kind - is rejected as non-durable.

  alias Docket.Graph
  alias Docket.Graph.{Edge, Error, Field, Node, Output}
  alias Docket.{Guard, Reducer, Schema}

  @schema_version 1
  @hash_algorithm :sha256

  @start_id "$start"
  @finish_id "$finish"
  @id_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_-]*$/

  @field_kinds %{"input" => :input, "state" => :state}
  @field_kinds_out %{input: "input", state: "state"}

  @schema_types %{
    "string" => :string,
    "float" => :float,
    "map" => :map,
    "object" => :object,
    "enum" => :enum
  }
  @schema_types_out %{
    string: "string",
    float: "float",
    map: "map",
    object: "object",
    enum: "enum"
  }

  @reducer_types %{"last_value" => :last_value}
  @reducer_types_out %{last_value: "last_value"}

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
  @guard_ops_out %{
    all: "all",
    any: "any",
    changed: "changed",
    equals: "equals",
    exists: "exists",
    not: "not",
    path: "path",
    version_at_least: "version_at_least"
  }
  @guard_recursive_ops [:all, :any, :not]

  @graph_keys ~w(schema_version id name description inputs fields outputs nodes edges policies metadata)
  @field_keys ~w(kind label description schema reducer required default metadata)
  @node_keys ~w(label description implementation branches config policies metadata)
  @edge_keys ~w(from to label description source_handle target_handle guard metadata)
  @output_keys ~w(source label description schema metadata)
  @schema_keys ~w(type fields item values required default constraints metadata)
  @reducer_keys ~w(type opts)
  @guard_keys ~w(op args)

  # ---------------------------------------------------------------------------
  # Wire format (dump / load) - exposed as Docket.Graph.to_map/from_map
  # ---------------------------------------------------------------------------

  # Dumps a graph to the plain, JSON-safe v1 wire map. Raises
  # `Docket.Graph.Error` when the graph contains non-durable content. Graphs
  # built through the `Docket.Graph` editing API are always dumpable because
  # edits normalize durable values.
  @doc false
  @spec dump(Graph.t(), keyword()) :: map()
  def dump(%Graph{} = graph, _opts \\ []) do
    %{"schema_version" => @schema_version, "id" => graph.id}
    |> put_present("name", graph.name)
    |> put_present("description", graph.description)
    |> put_collection("inputs", graph.inputs, &dump_field/1)
    |> put_collection("fields", graph.fields, &dump_field/1)
    |> put_collection("outputs", graph.outputs, &dump_output/1)
    |> put_collection("nodes", graph.nodes, &dump_node/1)
    |> put_collection("edges", graph.edges, &dump_edge/1)
    |> put_open_map("policies", graph.policies)
    |> put_open_map("metadata", graph.metadata)
  end

  # Loads a graph from a wire map, raising `Docket.Graph.Error` on invalid
  # input.
  @doc false
  @spec load!(map(), keyword()) :: Graph.t()
  def load!(map, _opts \\ []) do
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
      nodes: load_collection!(map, "nodes", :node_id, &load_node/2),
      edges: load_collection!(map, "edges", :edge_id, &load_edge/2),
      policies: load_open_map!(map, "policies", "graph"),
      metadata: load_open_map!(map, "metadata", "graph"),
      diagnostics: []
    }
  end

  # ---------------------------------------------------------------------------
  # Hash
  # ---------------------------------------------------------------------------

  # Computes the lowercase 64-char hex SHA-256 hash of a graph: a SHA-256
  # digest over the canonical JSON encoding of `dump/2`.
  @doc false
  @spec hash(Graph.t(), keyword()) :: String.t()
  def hash(%Graph{} = graph, opts \\ []) do
    graph
    |> dump(opts)
    |> canonical_json_encode()
    |> then(&:crypto.hash(@hash_algorithm, &1))
    |> Base.encode16(case: :lower)
  end

  # ---------------------------------------------------------------------------
  # Dump helpers
  # ---------------------------------------------------------------------------

  defp dump_field(%Field{} = field) do
    kind = Map.fetch!(@field_kinds_out, field.kind)

    %{"kind" => kind}
    |> put_present("label", field.label)
    |> put_present("description", field.description)
    |> put_present("schema", dump_schema(field.schema))
    |> put_present("reducer", dump_reducer(field.reducer))
    |> put_true("required", field.required)
    |> put_present("default", field.default)
    |> put_open_map("metadata", field.metadata)
  end

  defp dump_output(%Output{} = output) do
    %{}
    |> put_present("source", output.source)
    |> put_present("label", output.label)
    |> put_present("description", output.description)
    |> put_present("schema", dump_schema(output.schema))
    |> put_open_map("metadata", output.metadata)
  end

  defp dump_node(%Node{} = node) do
    %{}
    |> put_present("label", node.label)
    |> put_present("description", node.description)
    |> put_present("implementation", dump_implementation(node.implementation))
    |> put_open_map("branches", dump_branches(node.branches))
    |> put_open_map("config", node.config)
    |> put_open_map("policies", node.policies)
    |> put_open_map("metadata", node.metadata)
  end

  defp dump_edge(%Edge{} = edge) do
    %{}
    |> put_present("from", edge.from)
    |> put_present("to", edge.to)
    |> put_present("label", edge.label)
    |> put_present("description", edge.description)
    |> put_present("source_handle", edge.source_handle)
    |> put_present("target_handle", edge.target_handle)
    |> put_present("guard", dump_guard(edge.guard))
    |> put_open_map("metadata", edge.metadata)
  end

  defp dump_schema(nil), do: nil

  defp dump_schema(%Schema{} = schema) do
    type = Map.fetch!(@schema_types_out, schema.type)

    %{"type" => type}
    |> put_present("fields", dump_schema_fields(schema.fields))
    |> put_present("item", dump_schema(schema.item))
    |> put_present("values", schema.values)
    |> put_true("required", schema.required)
    |> put_schema_default(schema.default)
    |> put_open_map("constraints", schema.constraints)
    |> put_open_map("metadata", schema.metadata)
  end

  defp dump_schema_fields(fields) when map_size(fields) == 0, do: nil

  defp dump_schema_fields(fields) do
    Map.new(fields, fn {name, schema} -> {to_string(name), dump_schema(schema)} end)
  end

  defp dump_reducer(nil), do: nil

  defp dump_reducer(%Reducer{} = reducer) do
    type = Map.fetch!(@reducer_types_out, reducer.type)

    %{"type" => type}
    |> put_open_map("opts", reducer.opts)
  end

  defp dump_guard(nil), do: nil

  defp dump_guard(%Guard{op: op, args: args}) do
    op_string = Map.fetch!(@guard_ops_out, op)
    %{"op" => op_string, "args" => dump_guard_args(op, args)}
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
  # guards are wrapped in a reserved "$guard" tag so loading can distinguish
  # them from plain map values; "$"-prefixed keys are rejected in durable
  # values, which keeps the tag unambiguous.
  defp dump_guard_args(_op, args) do
    Enum.map(args, fn
      %Guard{} = arg -> %{"$guard" => dump_guard(arg)}
      other -> durable!(other)
    end)
  end

  defp dump_implementation(nil), do: nil

  defp dump_implementation(%{type: :module} = impl) do
    %{"type" => "module", "module" => Atom.to_string(impl.module)}
    |> then(fn map ->
      case Map.get(impl, :function) do
        nil -> map
        function -> Map.put(map, "function", Atom.to_string(function))
      end
    end)
  end

  defp dump_implementation(%{type: type} = impl) when is_atom(type) do
    base = %{"type" => Atom.to_string(type)}

    impl
    |> Map.delete(:type)
    |> Enum.reduce(base, fn {key, value}, acc ->
      Map.put(acc, durable_key!(key), durable!(value))
    end)
  end

  defp dump_branches(branches) when map_size(branches) == 0, do: %{}

  defp dump_branches(branches) do
    Map.new(branches, fn {name, group} -> {to_string(name), dump_branch_group(group)} end)
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

  defp load_node(id, map) do
    assert_string_keys!(map, "node #{inspect(id)}")
    assert_known_keys!(map, @node_keys, "node #{inspect(id)}")

    %Node{
      id: id,
      label: load_optional_string!(map, "label", "node #{inspect(id)}"),
      description: load_optional_string!(map, "description", "node #{inspect(id)}"),
      implementation: load_implementation!(Map.get(map, "implementation"), id),
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
    Map.new(fields, fn {name, schema} -> {name, load_schema!(schema, "#{location}.#{name}")} end)
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

  defp load_implementation!(nil, _id), do: nil

  defp load_implementation!(map, id) when is_map(map) and not is_struct(map) do
    assert_string_keys!(map, "node #{inspect(id)} implementation")
    type_string = fetch_required!(map, "type", "implementation")

    case type_string do
      "module" -> load_module_implementation!(map, id)
      _other -> load_custom_implementation!(map, type_string, id)
    end
  end

  defp load_implementation!(other, id) do
    invalid!(
      :invalid_document,
      "node #{inspect(id)} implementation must be a map, got #{inspect(other)}"
    )
  end

  defp load_module_implementation!(map, id) do
    assert_known_keys!(map, ~w(type module function), "node #{inspect(id)} implementation")
    module_string = fetch_required!(map, "module", "implementation")
    module = to_existing_atom!(module_string, :unknown_module, "implementation module")

    base = %{type: :module, module: module}

    case Map.fetch(map, "function") do
      {:ok, function_string} ->
        function =
          to_existing_atom!(function_string, :unknown_function, "implementation function")

        Map.put(base, :function, function)

      :error ->
        Map.put(base, :function, :call)
    end
  end

  defp load_custom_implementation!(map, type_string, _id) do
    type = to_existing_atom!(type_string, :unknown_implementation_type, "implementation type")

    map
    |> Map.delete("type")
    |> Map.new(fn {key, value} -> {durable_key!(key), durable!(value)} end)
    |> Map.put(:type, type)
  end

  defp load_branches!(nil, _id), do: %{}

  defp load_branches!(branches, id) when is_map(branches) and not is_struct(branches) do
    assert_string_keys!(branches, "node #{inspect(id)} branches")

    Map.new(branches, fn {name, group} ->
      {name, load_branch_group!(group, id, name)}
    end)
  end

  defp load_branches!(other, id) do
    invalid!(
      :invalid_document,
      "node #{inspect(id)} branches must be a map, got #{inspect(other)}"
    )
  end

  defp load_branch_group!(group, _id, _name) when is_list(group), do: group

  defp load_branch_group!(group, id, name) when is_map(group) and not is_struct(group) do
    assert_string_keys!(group, "node #{inspect(id)} branch #{inspect(name)}")
    group
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

  defp put_true(map, _key, false), do: map
  defp put_true(map, key, true), do: Map.put(map, key, true)

  defp put_open_map(map, _key, value) when value == %{}, do: map
  defp put_open_map(map, key, value) when is_map(value), do: Map.put(map, key, durable!(value))

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
    Enum.map(list, &load_durable_value!(&1, location))
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

  defp to_existing_atom!(value, code, label) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError ->
      invalid!(
        code,
        "#{label} #{inspect(value)} is not a known/loaded atom",
        %{label: label, value: value}
      )
  end

  defp to_existing_atom!(other, code, label) do
    invalid!(code, "#{label} must be a string, got #{inspect(other)}", %{
      label: label,
      value: other
    })
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
    allowed_set = MapSet.new(allowed)

    Enum.each(Map.keys(map), fn key ->
      unless MapSet.member?(allowed_set, key) do
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

  # ---------------------------------------------------------------------------
  # Durable value normalization (edit time)
  # ---------------------------------------------------------------------------

  @doc """
  Normalizes an open durable value into canonical form.

  Atoms become strings, map keys (binaries or atoms) become strings, and lists
  and maps recurse. Non-durable terms raise `Docket.Graph.Error`.
  """
  @spec normalize_value(term()) :: term()
  def normalize_value(value), do: durable!(value)

  @doc """
  Normalizes an open map key. Binaries pass through; atoms become strings.
  """
  @spec normalize_key(term()) :: String.t()
  def normalize_key(key), do: durable_key!(key)

  @doc false
  @spec normalize_field(Field.t()) :: Field.t()
  def normalize_field(%Field{} = field) do
    %Field{
      field
      | schema: normalize_schema(field.schema),
        reducer: normalize_reducer(field.reducer),
        default: normalize_default(field.default),
        metadata: normalize_open_map(field.metadata)
    }
  end

  @doc false
  @spec normalize_node(Node.t()) :: Node.t()
  def normalize_node(%Node{} = node) do
    %Node{
      node
      | implementation: normalize_implementation(node.implementation),
        branches: normalize_branches(node.branches),
        config: normalize_open_map(node.config),
        policies: normalize_open_map(node.policies),
        metadata: normalize_open_map(node.metadata)
    }
  end

  @doc false
  @spec normalize_edge(Edge.t()) :: Edge.t()
  def normalize_edge(%Edge{} = edge) do
    %Edge{
      edge
      | guard: normalize_guard(edge.guard),
        metadata: normalize_open_map(edge.metadata)
    }
  end

  @doc false
  @spec normalize_output(Output.t()) :: Output.t()
  def normalize_output(%Output{} = output) do
    %Output{
      output
      | schema: normalize_schema(output.schema),
        metadata: normalize_open_map(output.metadata)
    }
  end

  @doc false
  @spec normalize_open_map(term()) :: map()
  def normalize_open_map(map) when is_map(map) and not is_struct(map), do: durable!(map)

  def normalize_open_map(other) do
    non_durable!(other, "expected a map")
  end

  @doc false
  @spec normalize_implementation(term()) :: map() | nil
  def normalize_implementation(nil), do: nil

  def normalize_implementation(module) when is_atom(module) do
    %{type: :module, module: module, function: :call}
  end

  def normalize_implementation({module, function}) when is_atom(module) and is_atom(function) do
    %{type: :module, module: module, function: function}
  end

  def normalize_implementation(%{type: :module} = impl) do
    normalized = %{type: :module, module: fetch_module!(impl)}

    case Map.get(impl, :function) do
      nil -> Map.put(normalized, :function, :call)
      function when is_atom(function) -> Map.put(normalized, :function, function)
      other -> non_durable!(other, "implementation function must be an atom")
    end
  end

  def normalize_implementation(%{type: type} = impl) when is_atom(type) do
    base = %{type: type}

    impl
    |> Map.delete(:type)
    |> Enum.reduce(base, fn {key, value}, acc ->
      Map.put(acc, durable_key!(key), durable!(value))
    end)
  end

  def normalize_implementation(%{} = impl) do
    non_durable!(impl, "implementation map must have an atom :type")
  end

  def normalize_implementation(other) do
    invalid!(
      :invalid_implementation,
      "implementation must be a module atom, {module, function}, map, or nil, got #{inspect(other)}"
    )
  end

  defp fetch_module!(%{module: module}) when is_atom(module), do: module

  defp fetch_module!(impl) do
    non_durable!(impl, "module implementation must have an atom :module")
  end

  @doc false
  @spec normalize_schema(term()) :: Schema.t() | nil
  def normalize_schema(nil), do: nil

  def normalize_schema(%Schema{} = schema) do
    unless Map.has_key?(@schema_types_out, schema.type) do
      invalid!(:invalid_schema, "unknown schema type #{inspect(schema.type)}", %{
        type: schema.type
      })
    end

    %Schema{
      schema
      | fields: normalize_schema_fields(schema.fields),
        item: normalize_schema(schema.item),
        values: normalize_list(schema.values),
        default: normalize_default(schema.default),
        constraints: normalize_open_map(schema.constraints),
        metadata: normalize_open_map(schema.metadata)
    }
  end

  def normalize_schema(other) do
    invalid!(:invalid_schema, "schema must be a Docket.Schema or nil, got #{inspect(other)}")
  end

  defp normalize_schema_fields(fields) when is_map(fields) do
    Map.new(fields, fn {name, schema} -> {durable_key!(name), normalize_schema(schema)} end)
  end

  @doc false
  @spec normalize_reducer(term()) :: Reducer.t() | nil
  def normalize_reducer(nil), do: nil

  def normalize_reducer(%Reducer{} = reducer) do
    unless Map.has_key?(@reducer_types_out, reducer.type) do
      invalid!(:invalid_reducer, "unknown reducer type #{inspect(reducer.type)}", %{
        type: reducer.type
      })
    end

    %Reducer{reducer | opts: normalize_open_map(reducer.opts)}
  end

  def normalize_reducer(other) do
    invalid!(:invalid_reducer, "reducer must be a Docket.Reducer or nil, got #{inspect(other)}")
  end

  @doc false
  @spec normalize_guard(term()) :: Guard.t() | nil
  def normalize_guard(nil), do: nil

  def normalize_guard(%Guard{op: op, args: args}) do
    unless Map.has_key?(@guard_ops_out, op) do
      invalid!(:invalid_guard, "unknown guard op #{inspect(op)}", %{op: op})
    end

    %Guard{op: op, args: normalize_guard_args(op, args)}
  end

  def normalize_guard(other) do
    invalid!(:invalid_guard, "guard must be a Docket.Guard or nil, got #{inspect(other)}")
  end

  defp normalize_guard_args(op, args) when op in @guard_recursive_ops do
    Enum.map(args, fn
      %Guard{} = arg -> normalize_guard(arg)
      other -> non_durable!(other, "guard #{op} arguments must be Docket.Guard structs")
    end)
  end

  # Plain argument positions may reference nested guard expressions, e.g.
  # equals(path(...), value).
  defp normalize_guard_args(_op, args) when is_list(args) do
    Enum.map(args, fn
      %Guard{} = arg -> normalize_guard(arg)
      other -> durable!(other)
    end)
  end

  defp normalize_guard_args(_op, args) do
    invalid!(:invalid_guard, "guard args must be a list, got #{inspect(args)}")
  end

  defp normalize_branches(branches) when is_map(branches) and not is_struct(branches) do
    Map.new(branches, fn {name, group} -> {durable_key!(name), normalize_branch_group(group)} end)
  end

  defp normalize_branches(other) do
    non_durable!(other, "branches must be a map")
  end

  defp normalize_branch_group(group) when is_list(group), do: normalize_list(group)

  defp normalize_branch_group(group) when is_map(group) and not is_struct(group),
    do: durable!(group)

  defp normalize_branch_group(group) do
    non_durable!(group, "branch group must be a list of edge IDs or a string-keyed map")
  end

  defp normalize_list(list) when is_list(list), do: Enum.map(list, &durable!/1)

  defp normalize_default(default) do
    if default == Schema.no_default() do
      default
    else
      durable!(default)
    end
  end

  # ---------------------------------------------------------------------------
  # Durable value core
  # ---------------------------------------------------------------------------

  defp durable!(value)
       when is_binary(value) or is_integer(value) or is_float(value) or is_boolean(value) or
              is_nil(value),
       do: value

  defp durable!(atom) when is_atom(atom), do: Atom.to_string(atom)

  defp durable!(list) when is_list(list), do: Enum.map(list, &durable!/1)

  defp durable!(%_struct{} = struct) do
    non_durable!(struct, "structs are not durable values")
  end

  defp durable!(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {durable_key!(key), durable!(value)} end)
  end

  defp durable!(value) do
    non_durable!(value, durable_hint(value))
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
    non_durable!(key, "map keys must be binaries or atoms")
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

  # ---------------------------------------------------------------------------
  # Canonical JSON encoder
  # ---------------------------------------------------------------------------

  @doc """
  Encodes the wire-map domain into deterministic, compact JSON.

  Object keys are sorted by binary byte order, there is no insignificant
  whitespace, and floats use the shortest round-tripping representation. Only
  the `dump/2` output domain is supported: any other term raises.
  """
  @spec canonical_json_encode(term()) :: binary()
  def canonical_json_encode(term) do
    IO.iodata_to_binary(encode_json(term))
  end

  defp encode_json(nil), do: "null"
  defp encode_json(true), do: "true"
  defp encode_json(false), do: "false"
  defp encode_json(value) when is_integer(value), do: Integer.to_string(value)
  defp encode_json(value) when is_float(value), do: :erlang.float_to_binary(value, [:short])
  defp encode_json(value) when is_binary(value), do: encode_json_string(value)

  defp encode_json(list) when is_list(list) do
    ["[", encode_json_elements(list), "]"]
  end

  defp encode_json(map) when is_map(map) and not is_struct(map) do
    entries =
      map
      |> Enum.map(fn {key, value} -> {encode_json_key!(key), value} end)
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Enum.map(fn {key, value} -> [encode_json_string(key), ":", encode_json(value)] end)
      |> Enum.intersperse(",")

    ["{", entries, "}"]
  end

  defp encode_json(other) do
    raise ArgumentError, "canonical JSON encoder received a non-JSON term: #{inspect(other)}"
  end

  defp encode_json_elements([]), do: []

  defp encode_json_elements(list) do
    list
    |> Enum.map(&encode_json/1)
    |> Enum.intersperse(",")
  end

  defp encode_json_key!(key) when is_binary(key), do: key

  defp encode_json_key!(other) do
    raise ArgumentError, "canonical JSON object keys must be binaries, got #{inspect(other)}"
  end

  defp encode_json_string(string) do
    [?", escape_json(string, []), ?"]
  end

  defp escape_json(<<>>, acc), do: Enum.reverse(acc)

  defp escape_json(<<?", rest::binary>>, acc), do: escape_json(rest, ["\\\"" | acc])
  defp escape_json(<<?\\, rest::binary>>, acc), do: escape_json(rest, ["\\\\" | acc])

  defp escape_json(<<char::utf8, rest::binary>>, acc) when char < 0x20 do
    escape_json(rest, [unicode_escape(char) | acc])
  end

  defp escape_json(<<char::utf8, rest::binary>>, acc) do
    escape_json(rest, [<<char::utf8>> | acc])
  end

  defp unicode_escape(char) do
    hex =
      char
      |> Integer.to_string(16)
      |> String.downcase()
      |> String.pad_leading(4, "0")

    "\\u" <> hex
  end

  defp invalid!(code, message, details \\ %{}) do
    raise Error, code: code, message: message, details: details
  end
end
