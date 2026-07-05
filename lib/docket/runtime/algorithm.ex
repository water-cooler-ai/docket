defmodule Docket.Runtime.Algorithm do
  @moduledoc false

  # Deterministic graph execution helpers shared by `Docket.Runtime.Loop`.
  #
  # Every function here is a pure transformation over the runtime graph and
  # committed run data: no mailbox, no clock reads, no random values, no
  # executor calls, no checkpoint side effects. All iteration is sorted by
  # public ID so identical inputs produce identical outputs.

  alias Docket.Graph.Compiler.Policies
  alias Docket.Graph.Serializer
  alias Docket.Guard
  alias Docket.Run.ChannelState
  alias Docket.Runtime.Activation
  alias Docket.{Schema, Wire}

  @start_id "$start"
  @finish_id "$finish"

  # ---------------------------------------------------------------------------
  # Plan
  # ---------------------------------------------------------------------------

  @doc """
  Selects the next superstep decision from committed run state.

  Returns:

  - `{:execute, [node_public_id]}` - nodes to activate, sorted
  - `{:wait, [interrupt_id]}` - nothing can proceed until an open interrupt
    resolves
  - `:done` - no activations and no pending external work
  - `{:failed, :max_supersteps_exceeded}` - activations exist but the step
    limit is reached
  """
  def plan(rtg, run, config) do
    interrupted = interrupted_node_ids(run)
    candidates = candidate_node_ids(rtg, run, interrupted)

    cond do
      candidates == [] and map_size(open_interrupts(run)) == 0 ->
        :done

      candidates == [] ->
        {:wait, run |> open_interrupts() |> Map.keys() |> Enum.sort()}

      exceeds_max_supersteps?(rtg, run, config) ->
        {:failed, :max_supersteps_exceeded}

      true ->
        {:execute, candidates}
    end
  end

  defp candidate_node_ids(rtg, run, interrupted) do
    resumable = MapSet.difference(run.pending_nodes, interrupted)

    rtg.nodes
    |> Enum.filter(fn {_runtime_id, node} ->
      not MapSet.member?(interrupted, node.public_id) and
        (MapSet.member?(resumable, node.public_id) or
           Enum.any?(node.subscribe, &MapSet.member?(run.changed_channels, &1)))
    end)
    |> Enum.map(fn {_runtime_id, node} -> node.public_id end)
    |> Enum.sort()
  end

  defp interrupted_node_ids(run) do
    run
    |> open_interrupts()
    |> Enum.map(fn {_id, interrupt} -> interrupt.node_id end)
    |> MapSet.new()
  end

  @doc false
  def open_interrupts(run) do
    Map.filter(run.interrupts, fn {_id, interrupt} -> interrupt.status == :open end)
  end

  defp exceeds_max_supersteps?(rtg, run, config) do
    case max_supersteps(rtg, config) do
      nil -> false
      limit -> run.step >= limit
    end
  end

  @doc false
  def max_supersteps(rtg, config) do
    Map.get(rtg.policies, Policies.max_supersteps_key()) || Map.get(config, :max_supersteps)
  end

  # ---------------------------------------------------------------------------
  # Activations
  # ---------------------------------------------------------------------------

  @doc """
  Builds one activation per selected node from committed run state only.

  Returns `{:ok, [activation]}` or `{:error, Docket.Error.t()}` when a node
  declares an invalid v1 policy.
  """
  def prepare_activations(rtg, run, node_ids, _config) do
    snapshot = state_snapshot(rtg, run)
    versions = state_versions(rtg, run)
    input_hash = hash_snapshot(snapshot)

    node_ids
    |> Enum.sort()
    |> Enum.reduce_while({:ok, []}, fn node_id, {:ok, acc} ->
      node = Map.fetch!(rtg.nodes, "node:" <> node_id)

      case Policies.node_policies(node.policies) do
        {:ok, %{timeout_ms: timeout_ms, retry: retry}} ->
          task_id = "#{run.id}:#{run.step}:#{node_id}"

          activation = %Activation{
            task_id: task_id,
            node_id: node_id,
            runtime_node_id: node.id,
            step: run.step,
            attempt: 1,
            input_hash: input_hash,
            idempotency_key: "#{task_id}:1",
            snapshot: snapshot,
            source_versions: versions,
            config: node.config,
            timeout_ms: timeout_ms,
            retry: retry
          }

          {:cont, {:ok, [activation | acc]}}

        {:error, errors} ->
          message = Enum.map_join(errors, "; ", fn {_key, message} -> message end)

          {:halt,
           {:error, Docket.Error.new(:invalid_policy, message, node_id: node_id, phase: :plan)}}
      end
    end)
    |> case do
      {:ok, activations} -> {:ok, Enum.reverse(activations)}
      error -> error
    end
  end

  @doc """
  Builds the committed state snapshot: one flat map keyed by public input and
  field ID.

  Never-written channels are absent unless the graph declares a non-nil
  default, so nodes and guards see missing state as missing rather than nil.
  """
  def state_snapshot(rtg, run) do
    for {channel_id, {kind, public_id}} <- rtg.lowering.runtime_to_public,
        kind in [:input, :field],
        {:ok, value} <- [snapshot_value(rtg, run, channel_id)],
        into: %{} do
      {public_id, value}
    end
  end

  defp snapshot_value(rtg, run, channel_id) do
    case Map.fetch(run.channels, channel_id) do
      {:ok, %ChannelState{value: value}} ->
        {:ok, value}

      :error ->
        case Map.fetch!(rtg.channels, channel_id).default do
          nil -> :missing
          default -> {:ok, default}
        end
    end
  end

  @doc false
  def state_versions(rtg, run) do
    for {channel_id, {kind, public_id}} <- rtg.lowering.runtime_to_public,
        kind in [:input, :field],
        into: %{} do
      case Map.fetch(run.channels, channel_id) do
        {:ok, %ChannelState{version: version}} -> {public_id, version}
        :error -> {public_id, 0}
      end
    end
  end

  @doc false
  def hash_snapshot(snapshot) do
    snapshot
    |> Serializer.canonical_json_encode()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  # ---------------------------------------------------------------------------
  # Write validation and reducers
  # ---------------------------------------------------------------------------

  @doc """
  Validates and canonicalizes one node's returned update map.

  Returns `{:ok, coerced_update}` (atoms coerced to strings, exactly as the
  checkpointed run would persist them) or `{:error, [reason]}`. Validation
  failures are deterministic and never retried.
  """
  def validate_state_update(rtg, node_id, update) when is_map(update) and not is_struct(update) do
    case Wire.dump_value(update) do
      {:ok, coerced} ->
        reasons =
          coerced
          |> Enum.sort_by(fn {field_id, _value} -> field_id end)
          |> Enum.flat_map(fn {field_id, value} ->
            validate_write(rtg, node_id, field_id, value)
          end)

        if reasons == [], do: {:ok, coerced}, else: {:error, reasons}

      {:error, reason} ->
        {:error, ["node #{inspect(node_id)} returned a non-durable update: #{reason}"]}
    end
  end

  def validate_state_update(_rtg, node_id, other) do
    {:error, ["node #{inspect(node_id)} must return a state update map, got #{inspect(other)}"]}
  end

  defp validate_write(rtg, node_id, field_id, value) do
    cond do
      Map.has_key?(rtg.lowering.public_to_runtime.fields, field_id) ->
        channel = Map.fetch!(rtg.channels, "state:" <> field_id)
        validate_write_schema(node_id, field_id, value, channel.value_schema)

      Map.has_key?(rtg.lowering.public_to_runtime.inputs, field_id) ->
        ["node #{inspect(node_id)} wrote #{inspect(field_id)}, which is an input and read-only"]

      true ->
        ["node #{inspect(node_id)} wrote unknown field #{inspect(field_id)}"]
    end
  end

  defp validate_write_schema(_node_id, _field_id, _value, nil), do: []

  defp validate_write_schema(node_id, field_id, value, %Schema{} = schema) do
    case Schema.validate(schema, value) do
      :ok ->
        []

      {:error, reasons} ->
        Enum.map(reasons, fn reason ->
          "node #{inspect(node_id)} wrote invalid value to #{inspect(field_id)}: #{reason}"
        end)
    end
  end

  @doc """
  Applies validated writes through channel reducers.

  `writes` is a list of `{node_id, update_map}` pairs. Returns
  `{channels, changed_field_ids, writers_by_field}` where `changed_field_ids`
  are public field IDs (write-based: any committed write marks the field
  changed, even when the value is equal).
  """
  def apply_state_writes(rtg, channels, writes) do
    writes
    |> Enum.sort_by(fn {node_id, _update} -> node_id end)
    |> Enum.flat_map(fn {node_id, update} ->
      update
      |> Enum.sort_by(fn {field_id, _value} -> field_id end)
      |> Enum.map(fn {field_id, value} -> {field_id, node_id, value} end)
    end)
    |> Enum.group_by(fn {field_id, _node, _value} -> field_id end)
    |> Enum.sort_by(fn {field_id, _writes} -> field_id end)
    |> Enum.reduce({channels, MapSet.new(), %{}}, fn {field_id, field_writes},
                                                     {channels, changed, writers} ->
      channel_id = "state:" <> field_id
      channel = Map.fetch!(rtg.channels, channel_id)
      values = Enum.map(field_writes, fn {_field, _node, value} -> value end)
      value = reduce_values(channel.reducer, values)
      node_ids = Enum.map(field_writes, fn {_field, node_id, _value} -> node_id end)

      {bump_channel(channels, channel_id, value), MapSet.put(changed, field_id),
       Map.put(writers, field_id, node_ids)}
    end)
  end

  # v1 supports only the :last_value reducer: the last write in deterministic
  # order (sorted by writer node ID) wins. The compiler guarantees the
  # reducer type.
  defp reduce_values(%Docket.Reducer{type: :last_value}, values), do: List.last(values)
  defp reduce_values(nil, values), do: List.last(values)

  defp bump_channel(channels, channel_id, value) do
    Map.update(
      channels,
      channel_id,
      %ChannelState{channel_id: channel_id, value: value, version: 1},
      fn %ChannelState{} = state -> %{state | value: value, version: state.version + 1} end
    )
  end

  # ---------------------------------------------------------------------------
  # Edge triggers
  # ---------------------------------------------------------------------------

  @doc """
  Evaluates outgoing edges of successfully completed nodes against the newly
  committed state.

  Handles barrier seen-set accumulation for multi-source edges (LangGraph
  `NamedBarrierValue` semantics: fire when every source has completed since
  the last firing, then reset) and guard filtering.

  Returns `{:ok, result}` with:

  - `channels` - updated channel map (barrier seen-sets and fired edge
    activation channels)
  - `triggered` - sorted public edge IDs that fired an activation channel
  - `finish` - sorted public edge IDs targeting `$finish` that triggered

  or `{:error, {edge_id, reasons}}` on guard evaluation failure.
  """
  def evaluate_edge_triggers(rtg, channels, ok_node_ids, changed_fields) do
    guard_context = guard_context(rtg, channels, changed_fields)
    ok_set = MapSet.new(ok_node_ids)

    edges =
      ok_node_ids
      |> Enum.sort()
      |> Enum.flat_map(fn node_id ->
        Map.fetch!(rtg.nodes, "node:" <> node_id).outgoing_edges
      end)
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map(&Map.fetch!(rtg.edges, &1))

    {channels, candidates} = collect_candidates(edges, channels, ok_set)

    candidates
    |> Enum.reduce_while({:ok, {channels, [], []}}, fn edge,
                                                       {:ok, {channels, triggered, finish}} ->
      case edge_triggers?(edge, guard_context) do
        {:ok, false} ->
          {:cont, {:ok, {channels, triggered, finish}}}

        {:ok, true} ->
          if edge.to == @finish_id do
            {:cont, {:ok, {channels, triggered, [edge.id | finish]}}}
          else
            {:cont,
             {:ok, {bump_channel(channels, edge.channel_id, true), [edge.id | triggered], finish}}}
          end

        {:error, reasons} ->
          {:halt, {:error, {edge.id, reasons}}}
      end
    end)
    |> case do
      {:ok, {channels, triggered, finish}} ->
        {:ok,
         %{channels: channels, triggered: Enum.reverse(triggered), finish: Enum.reverse(finish)}}

      error ->
        error
    end
  end

  # Records barrier completions and selects candidate edges. Barrier seen-set
  # mutations persist even when the barrier does not fire; a fired barrier
  # resets its seen set so it can fire again in cycles. Guards filter after
  # firing and do not restore the seen set.
  defp collect_candidates(edges, channels, ok_set) do
    Enum.reduce(edges, {channels, []}, fn edge, {channels, candidates} ->
      if edge.barrier do
        completed = Enum.filter(edge.from, &MapSet.member?(ok_set, &1))
        state = Map.get(channels, edge.channel_id, %ChannelState{channel_id: edge.channel_id})

        seen =
          state.barrier_seen
          |> MapSet.new()
          |> then(&Enum.reduce(completed, &1, fn id, acc -> MapSet.put(acc, id) end))

        if MapSet.subset?(MapSet.new(edge.from), seen) do
          channels = Map.put(channels, edge.channel_id, %{state | barrier_seen: []})
          {channels, [edge | candidates]}
        else
          channels = Map.put(channels, edge.channel_id, %{state | barrier_seen: Enum.sort(seen)})
          {channels, candidates}
        end
      else
        {channels, [edge | candidates]}
      end
    end)
    |> then(fn {channels, candidates} ->
      {channels, Enum.sort_by(Enum.reverse(candidates), & &1.id)}
    end)
  end

  defp edge_triggers?(%{guard: nil}, _context), do: {:ok, true}
  defp edge_triggers?(%{guard: guard}, context), do: evaluate_guard(guard, context)

  @doc """
  Evaluates `$start` edges at run initialization.

  `$start` behaves as a virtual completed source node; guards run against
  the initial committed state (the input channels just written).
  """
  def evaluate_start_edges(rtg, channels, changed_input_ids) do
    guard_context = guard_context(rtg, channels, MapSet.new(changed_input_ids))

    start_edges =
      rtg.edges
      |> Enum.filter(fn {_id, edge} -> edge.from == [@start_id] end)
      |> Enum.map(fn {_id, edge} -> edge end)
      |> Enum.sort_by(& &1.id)

    Enum.reduce_while(start_edges, {:ok, {channels, []}}, fn edge, {:ok, {channels, triggered}} ->
      case edge_triggers?(edge, guard_context) do
        {:ok, true} ->
          {:cont, {:ok, {bump_channel(channels, edge.channel_id, true), [edge.id | triggered]}}}

        {:ok, false} ->
          {:cont, {:ok, {channels, triggered}}}

        {:error, reasons} ->
          {:halt, {:error, {edge.id, reasons}}}
      end
    end)
    |> case do
      {:ok, {channels, triggered}} ->
        {:ok, %{channels: channels, triggered: Enum.reverse(triggered)}}

      error ->
        error
    end
  end

  # ---------------------------------------------------------------------------
  # Guards
  # ---------------------------------------------------------------------------

  @doc false
  def guard_context(rtg, channels, changed_fields) do
    run_view = %Docket.Run{channels: channels}

    %{
      values: state_snapshot(rtg, run_view),
      versions: state_versions(rtg, run_view),
      changed: changed_fields
    }
  end

  @doc """
  Evaluates a guard expression against committed state.

  Guards are lax: never-written channels and missing path segments make
  `exists/1`, `equals/2`, and `changed/1` false rather than raising.
  Returns `{:ok, boolean}` or `{:error, [reason]}`.
  """
  def evaluate_guard(%Guard{op: :all, args: args}, context) do
    evaluate_boolean_args(args, context, &Enum.all?/1)
  end

  def evaluate_guard(%Guard{op: :any, args: args}, context) do
    evaluate_boolean_args(args, context, &Enum.any?/1)
  end

  def evaluate_guard(%Guard{op: :not, args: [arg]}, context) do
    with {:ok, value} <- evaluate_guard(arg, context), do: {:ok, not value}
  end

  def evaluate_guard(%Guard{op: :changed, args: [channel]}, context) when is_binary(channel) do
    {:ok, MapSet.member?(context.changed, channel)}
  end

  def evaluate_guard(%Guard{op: :version_at_least, args: [channel, version]}, context)
      when is_binary(channel) and is_integer(version) do
    {:ok, Map.get(context.versions, channel, 0) >= version}
  end

  def evaluate_guard(%Guard{op: :exists, args: [ref]}, context) do
    case resolve_ref(ref, context) do
      {:ok, _value} -> {:ok, true}
      :missing -> {:ok, false}
      {:error, reasons} -> {:error, reasons}
    end
  end

  def evaluate_guard(%Guard{op: :equals, args: [ref, expected]}, context) do
    with {:ok, resolved_expected} <- resolve_literal(expected, context) do
      case resolve_ref(ref, context) do
        {:ok, value} -> {:ok, value == resolved_expected}
        :missing -> {:ok, false}
        {:error, reasons} -> {:error, reasons}
      end
    end
  end

  def evaluate_guard(%Guard{} = guard, _context) do
    {:error, ["guard #{inspect(guard.op)} is not a boolean expression or has invalid arguments"]}
  end

  def evaluate_guard(other, _context) do
    {:error, ["guard expression must be a Docket.Guard, got #{inspect(other)}"]}
  end

  defp evaluate_boolean_args(args, context, combine) do
    args
    |> Enum.reduce_while({:ok, []}, fn arg, {:ok, acc} ->
      case evaluate_guard(arg, context) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, reasons} -> {:halt, {:error, reasons}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, combine.(Enum.reverse(values))}
      error -> error
    end
  end

  defp resolve_ref(channel, context) when is_binary(channel) do
    case Map.fetch(context.values, channel) do
      {:ok, value} -> {:ok, value}
      :error -> :missing
    end
  end

  defp resolve_ref(%Guard{op: :path, args: [channel, segments]}, context)
       when is_binary(channel) and is_list(segments) do
    case Map.fetch(context.values, channel) do
      {:ok, value} -> walk_path(value, segments)
      :error -> :missing
    end
  end

  defp resolve_ref(other, _context) do
    {:error, ["guard reference must be a channel ID or path expression, got #{inspect(other)}"]}
  end

  # A literal comparison value may itself be a reference expression, e.g.
  # equals(path("a", ["x"]), path("b", ["y"])) - missing references make the
  # comparison false via a sentinel that cannot equal any durable value.
  defp resolve_literal(%Guard{op: :path} = ref, context) do
    case resolve_ref(ref, context) do
      {:ok, value} -> {:ok, value}
      :missing -> {:ok, :__docket_missing__}
      {:error, reasons} -> {:error, reasons}
    end
  end

  defp resolve_literal(%Guard{} = guard, _context) do
    {:error, ["guard comparison value cannot be #{inspect(guard.op)} expression"]}
  end

  defp resolve_literal(value, _context), do: {:ok, value}

  defp walk_path(value, []), do: {:ok, value}

  defp walk_path(value, [segment | rest]) when is_map(value) and not is_struct(value) do
    case Map.fetch(value, segment) do
      {:ok, next} -> walk_path(next, rest)
      :error -> :missing
    end
  end

  defp walk_path(value, [segment | rest]) when is_list(value) and is_integer(segment) do
    case Enum.fetch(value, segment) do
      {:ok, next} -> walk_path(next, rest)
      :error -> :missing
    end
  end

  defp walk_path(_value, _segments), do: :missing

  # ---------------------------------------------------------------------------
  # Output projection
  # ---------------------------------------------------------------------------

  @doc """
  Projects committed channel values into the public output map.

  Never-written sources project as explicit `nil` entries so the output map
  always carries every declared output key.
  """
  def project_output(rtg, channels) do
    for {output_id, projection} <- Enum.sort_by(rtg.outputs, fn {id, _} -> id end), into: %{} do
      value =
        case Map.fetch(channels, projection.source_channel) do
          {:ok, %ChannelState{value: value}} -> value
          :error -> nil
        end

      {output_id, value}
    end
  end

  @doc false
  def start_id, do: @start_id

  @doc false
  def finish_id, do: @finish_id
end
