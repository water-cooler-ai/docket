defmodule Docket.Run.Serializer do
  @moduledoc false

  # Internal implementation of the canonical wire serialization for
  # `Docket.Run` documents. The only public entry/exit points are
  # `Docket.Run.to_map/2` and `Docket.Run.from_map/2` (and `from_map!/2`).
  #
  # The wire format mirrors the graph document conventions: string keys only,
  # empty collections and nil optionals omitted, open content restricted to
  # durable JSON-safe terms, "$"-prefixed map keys reserved, strict load
  # validation that never creates atoms.
  #
  # `active_tasks`, `pending_writes`, and `timers` are always empty on
  # committed runs and have no wire representation yet; the keys are
  # reserved for async execution.
  #
  # Version 2 adds the terminal `failure` payload and admits only the five
  # durable statuses: the private `:created` sentinel is rejected on dump
  # and load. Version-1 documents are not loadable; there is no released
  # userbase to migrate.

  alias Docket.Run
  alias Docket.Run.{ChannelState, Failure, InterruptState}
  alias Docket.Wire

  @version 2

  @statuses %{
    "running" => :running,
    "waiting" => :waiting,
    "done" => :done,
    "failed" => :failed,
    "cancelled" => :cancelled
  }
  @statuses_out Map.new(@statuses, fn {string, atom} -> {atom, string} end)

  @interrupt_statuses %{"open" => :open, "resolved" => :resolved}
  @interrupt_statuses_out Map.new(@interrupt_statuses, fn {string, atom} -> {atom, string} end)

  @run_keys ~w(version id graph_id graph_hash status step input output failure
               started_at updated_at finished_at channels changed_channels
               pending_nodes interrupts checkpoint_seq event_seq metadata)
  @channel_keys ~w(value version barrier_seen)
  @interrupt_keys ~w(node_id status resume_channel prompt schema created_at
                     resolved_at metadata)
  @failure_keys ~w(code message node_id details)

  # ---------------------------------------------------------------------------
  # Dump
  # ---------------------------------------------------------------------------

  @spec dump(Run.t(), keyword()) :: map()
  def dump(%Run{} = run, _opts \\ []) do
    validate_failure!(run)

    %{
      "version" => @version,
      "id" => required_string!(run.id, "run id"),
      "graph_id" => required_string!(run.graph_id, "run graph_id"),
      "status" => dump_status(run.status),
      "step" => run.step,
      "checkpoint_seq" => run.checkpoint_seq,
      "event_seq" => run.event_seq
    }
    |> put_present("graph_hash", run.graph_hash)
    |> put_open_map("input", run.input || %{}, "run input")
    |> put_present("output", dump_output(run.output))
    |> put_present("failure", dump_failure(run.failure))
    |> put_present("started_at", dump_timestamp(run.started_at))
    |> put_present("updated_at", dump_timestamp(run.updated_at))
    |> put_present("finished_at", dump_timestamp(run.finished_at))
    |> put_collection("channels", run.channels, &dump_channel/1)
    |> put_id_set("changed_channels", run.changed_channels)
    |> put_id_set("pending_nodes", run.pending_nodes)
    |> put_collection("interrupts", run.interrupts, &dump_interrupt/1)
    |> put_open_map("metadata", run.metadata, "run metadata")
  end

  defp dump_channel(%ChannelState{} = channel) do
    %{"version" => channel.version}
    |> put_present("value", Wire.dump_value!(channel.value, "channel value"))
    |> then(fn map ->
      case channel.barrier_seen do
        [] -> map
        seen -> Map.put(map, "barrier_seen", Enum.sort(seen))
      end
    end)
  end

  defp dump_interrupt(%InterruptState{} = interrupt) do
    %{
      "node_id" => required_string!(interrupt.node_id, "interrupt node_id"),
      "status" => dump_interrupt_status(interrupt.status),
      "resume_channel" => required_string!(interrupt.resume_channel, "interrupt resume_channel")
    }
    |> put_present("prompt", interrupt.prompt)
    |> put_present("schema", dump_schema(interrupt.schema))
    |> put_present("created_at", dump_timestamp(interrupt.created_at))
    |> put_present("resolved_at", dump_timestamp(interrupt.resolved_at))
    |> put_open_map("metadata", interrupt.metadata, "interrupt metadata")
  end

  defp dump_status(:created) do
    invalid!(
      :invalid_run,
      "run status :created is a private initialization sentinel and cannot be " <>
        "written durably; initialize the run first"
    )
  end

  defp dump_status(status) do
    Map.get(@statuses_out, status) ||
      invalid!(
        :invalid_run,
        "run status must be one of #{inspect(Map.keys(@statuses_out))}, got #{inspect(status)}"
      )
  end

  defp validate_failure!(%Run{} = run) do
    case Run.validate_failure(run) do
      :ok -> :ok
      {:error, %Docket.Error{} = error} -> raise error
    end
  end

  defp dump_failure(nil), do: nil

  defp dump_failure(%Failure{} = failure) do
    %{
      "code" => required_string!(failure.code, "failure code"),
      "message" => required_string!(failure.message, "failure message")
    }
    |> put_present("node_id", dump_failure_node_id(failure.node_id))
    |> put_open_map("details", failure.details, "failure details")
  end

  defp dump_failure_node_id(nil), do: nil
  defp dump_failure_node_id(node_id), do: required_string!(node_id, "failure node_id")

  defp dump_interrupt_status(status) do
    Map.get(@interrupt_statuses_out, status) ||
      invalid!(
        :invalid_run,
        "interrupt status must be :open or :resolved, got #{inspect(status)}"
      )
  end

  defp dump_output(nil), do: nil

  defp dump_output(output) when is_map(output) and not is_struct(output) do
    Wire.dump_value!(output, "run output")
  end

  defp dump_output(other) do
    invalid!(:invalid_run, "run output must be a map or nil, got #{inspect(other)}")
  end

  defp dump_timestamp(nil), do: nil
  defp dump_timestamp(%DateTime{} = timestamp), do: DateTime.to_iso8601(timestamp)

  defp dump_timestamp(other) do
    invalid!(:invalid_run, "run timestamps must be DateTime or nil, got #{inspect(other)}")
  end

  defp dump_schema(schema) do
    Docket.Graph.Serializer.dump_schema(schema)
  rescue
    error in Docket.Graph.Error ->
      invalid!(:invalid_run, "interrupt schema is not durable: #{error.message}")
  end

  defp required_string!(value, _label) when is_binary(value), do: value

  defp required_string!(value, label) do
    invalid!(:invalid_run, "#{label} must be a string, got #{inspect(value)}")
  end

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)

  defp put_open_map(map, _key, value, _location) when value == %{}, do: map

  defp put_open_map(map, key, value, location) when is_map(value) and not is_struct(value) do
    Map.put(map, key, Wire.dump_value!(value, location))
  end

  defp put_open_map(_map, key, other, _location) do
    invalid!(:invalid_run, "#{key} must be a string-keyed map, got #{inspect(other)}")
  end

  defp put_id_set(map, key, %MapSet{} = set) do
    case MapSet.to_list(set) do
      [] -> map
      ids -> Map.put(map, key, Enum.sort(ids))
    end
  end

  defp put_collection(map, _key, collection, _fun) when map_size(collection) == 0, do: map

  defp put_collection(map, key, collection, fun) do
    Map.put(map, key, Map.new(collection, fn {id, record} -> {id, fun.(record)} end))
  end

  # ---------------------------------------------------------------------------
  # Load
  # ---------------------------------------------------------------------------

  @spec load!(map(), keyword()) :: Run.t()
  def load!(map, _opts \\ []) do
    unless is_map(map) and not is_struct(map) do
      invalid!(:invalid_document, "run document must be a plain map, got #{inspect(map)}")
    end

    assert_string_keys!(map, "run document")
    load_version!(map)
    assert_known_keys!(map, @run_keys, "run")

    run = %Run{
      id: load_required_string!(map, "id", "run"),
      graph_id: load_required_string!(map, "graph_id", "run"),
      graph_hash: load_optional_string!(map, "graph_hash", "run"),
      status: load_enum!(map, "status", @statuses, "run status"),
      step: load_non_neg_integer!(map, "step", "run"),
      input: load_open_map!(map, "input", "run input"),
      output: load_output!(map),
      started_at: load_timestamp!(map, "started_at"),
      updated_at: load_timestamp!(map, "updated_at"),
      finished_at: load_timestamp!(map, "finished_at"),
      channels: load_collection!(map, "channels", &load_channel!/2),
      changed_channels: load_id_set!(map, "changed_channels"),
      pending_nodes: load_id_set!(map, "pending_nodes"),
      interrupts: load_collection!(map, "interrupts", &load_interrupt!/2),
      checkpoint_seq: load_non_neg_integer!(map, "checkpoint_seq", "run"),
      event_seq: load_non_neg_integer!(map, "event_seq", "run"),
      metadata: load_open_map!(map, "metadata", "run metadata")
    }

    run = %{run | failure: load_failure!(map)}

    case Run.validate_failure(run) do
      :ok -> run
      {:error, %Docket.Error{message: message}} -> invalid!(:invalid_document, message)
    end
  end

  defp load_version!(map) do
    case Map.fetch(map, "version") do
      {:ok, @version} ->
        :ok

      {:ok, version} when is_integer(version) and version >= 1 ->
        invalid!(
          :unsupported_schema_version,
          "run document version #{version} is not the supported version #{@version}",
          %{version: version, supported: @version}
        )

      {:ok, other} ->
        invalid!(
          :invalid_document,
          "run version must be a positive integer, got #{inspect(other)}"
        )

      :error ->
        invalid!(:invalid_document, "run document is missing required key \"version\"")
    end
  end

  defp load_failure!(map) do
    case Map.get(map, "failure") do
      nil ->
        nil

      value when is_map(value) and not is_struct(value) ->
        assert_string_keys!(value, "run failure")
        assert_known_keys!(value, @failure_keys, "run failure")

        %Failure{
          code: load_required_string!(value, "code", "run failure"),
          message: load_required_string!(value, "message", "run failure"),
          node_id: load_optional_string!(value, "node_id", "run failure"),
          details: load_open_map!(value, "details", "run failure details")
        }

      other ->
        invalid!(:invalid_document, "run failure must be a map, got #{inspect(other)}")
    end
  end

  defp load_channel!(id, map) do
    assert_string_keys!(map, "channel #{inspect(id)}")
    assert_known_keys!(map, @channel_keys, "channel #{inspect(id)}")

    version = load_non_neg_integer!(map, "version", "channel #{inspect(id)}")

    unless version >= 1 do
      invalid!(
        :invalid_document,
        "channel #{inspect(id)} version must be at least 1, got #{version}"
      )
    end

    %ChannelState{
      channel_id: id,
      value: Wire.load_value!(Map.get(map, "value"), "channel #{inspect(id)} value"),
      version: version,
      barrier_seen:
        load_string_list!(Map.get(map, "barrier_seen"), "channel #{inspect(id)} barrier_seen")
    }
  end

  defp load_interrupt!(id, map) do
    assert_string_keys!(map, "interrupt #{inspect(id)}")
    assert_known_keys!(map, @interrupt_keys, "interrupt #{inspect(id)}")

    %InterruptState{
      id: id,
      node_id: load_required_string!(map, "node_id", "interrupt #{inspect(id)}"),
      status: load_enum!(map, "status", @interrupt_statuses, "interrupt #{inspect(id)} status"),
      resume_channel: load_required_string!(map, "resume_channel", "interrupt #{inspect(id)}"),
      prompt: load_optional_string!(map, "prompt", "interrupt #{inspect(id)}"),
      schema: load_schema!(Map.get(map, "schema"), "interrupt #{inspect(id)}"),
      created_at: load_timestamp!(map, "created_at"),
      resolved_at: load_timestamp!(map, "resolved_at"),
      metadata: load_open_map!(map, "metadata", "interrupt #{inspect(id)} metadata")
    }
  end

  defp load_schema!(nil, _location), do: nil

  defp load_schema!(value, location) do
    Docket.Graph.Serializer.load_schema!(value, location)
  rescue
    error in Docket.Graph.Error ->
      invalid!(:invalid_document, error.message, error.details)
  end

  defp load_output!(map) do
    case Map.get(map, "output") do
      nil ->
        nil

      value when is_map(value) and not is_struct(value) ->
        Wire.load_value!(value, "run output")

      other ->
        invalid!(:invalid_document, "run output must be a map, got #{inspect(other)}")
    end
  end

  defp load_timestamp!(map, key) do
    case Map.get(map, key) do
      nil ->
        nil

      value when is_binary(value) ->
        case DateTime.from_iso8601(value) do
          {:ok, timestamp, _offset} ->
            timestamp

          {:error, reason} ->
            invalid!(
              :invalid_document,
              "run #{key} is not a valid ISO8601 timestamp: #{inspect(reason)}",
              %{key: key, value: value}
            )
        end

      other ->
        invalid!(:invalid_document, "run #{key} must be a string, got #{inspect(other)}")
    end
  end

  defp load_enum!(map, key, table, label) do
    value = load_required_string!(map, key, label)

    Map.get(table, value) ||
      invalid!(
        :invalid_document,
        "unknown #{label} #{inspect(value)}; expected one of #{inspect(Map.keys(table))}"
      )
  end

  defp load_collection!(map, key, fun) do
    case Map.get(map, key) do
      nil ->
        %{}

      collection when is_map(collection) and not is_struct(collection) ->
        assert_string_keys!(collection, key)

        Map.new(collection, fn {id, record} ->
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

  defp load_id_set!(map, key) do
    map
    |> Map.get(key)
    |> load_string_list!("run #{key}")
    |> MapSet.new()
  end

  defp load_string_list!(nil, _location), do: []

  defp load_string_list!(list, location) when is_list(list) do
    Enum.each(list, fn
      value when is_binary(value) ->
        :ok

      other ->
        invalid!(:invalid_document, "#{location} entries must be strings, got #{inspect(other)}")
    end)

    list
  end

  defp load_string_list!(other, location) do
    invalid!(:invalid_document, "#{location} must be a list, got #{inspect(other)}")
  end

  defp load_open_map!(map, key, location) do
    case Map.get(map, key) do
      nil ->
        %{}

      value when is_map(value) and not is_struct(value) ->
        Wire.load_value!(value, location)

      other ->
        invalid!(:invalid_document, "#{location} must be a map, got #{inspect(other)}")
    end
  end

  defp load_required_string!(map, key, location) do
    case Map.fetch(map, key) do
      {:ok, value} when is_binary(value) ->
        value

      {:ok, other} ->
        invalid!(:invalid_document, "#{location} #{key} must be a string, got #{inspect(other)}")

      :error ->
        invalid!(
          :invalid_document,
          "#{location} document is missing required key #{inspect(key)}"
        )
    end
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

  defp load_non_neg_integer!(map, key, location) do
    case Map.fetch(map, key) do
      {:ok, value} when is_integer(value) and value >= 0 ->
        value

      {:ok, other} ->
        invalid!(
          :invalid_document,
          "#{location} #{key} must be a non-negative integer, got #{inspect(other)}"
        )

      :error ->
        invalid!(
          :invalid_document,
          "#{location} document is missing required key #{inspect(key)}"
        )
    end
  end

  defp assert_string_keys!(map, location) do
    Enum.each(Map.keys(map), fn
      key when is_binary(key) ->
        :ok

      other ->
        invalid!(:invalid_document, "#{location} keys must be strings, got #{inspect(other)}", %{
          location: location
        })
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

  defp invalid!(type, message, details \\ %{}) do
    raise Docket.Error, type: type, message: message, details: details
  end
end
