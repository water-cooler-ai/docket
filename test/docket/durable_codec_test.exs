defmodule Docket.DurableCodecTest do
  use Docket.Test.Case, async: true

  alias Docket.{DurableCodec, Graph}

  defp etf(term, opts \\ []) do
    :erlang.term_to_binary(term, [:deterministic, {:minor_version, 2} | opts])
  end

  test "round trips deterministic graph and run roots" do
    graph =
      Graph.new!(id: "g", metadata: Map.new([{"b", 2}, {"a", 1}]))
      |> Graph.put_node!("n", implementation: __MODULE__)

    reordered = %{graph | metadata: Map.new([{"a", 1}, {"b", 2}])}
    graph_bytes = DurableCodec.encode!(:graph, graph)

    assert graph_bytes == DurableCodec.encode!(:graph, reordered)
    assert {:ok, ^graph} = DurableCodec.decode(graph_bytes, :graph)

    run_state = %{
      channels: %{"x" => %Docket.Run.ChannelState{channel_id: "x", value: 1}},
      changed_channels: MapSet.new(["x"]),
      updated_at: ~U[2026-07-03 10:00:00Z]
    }

    run_bytes = DurableCodec.encode!(:run, run_state)
    assert {:ok, ^run_state} = DurableCodec.decode(run_bytes, :run)
  end

  test "drops transient graph diagnostics before encoding" do
    diagnostic = %Docket.Graph.Diagnostic{severity: :warning, code: :ignored, message: "ignored"}
    graph = %{Graph.new!(id: "g") | diagnostics: [diagnostic]}

    assert DurableCodec.encode!(:graph, graph) ==
             DurableCodec.encode!(:graph, %{graph | diagnostics: []})

    assert {:ok, %Graph{diagnostics: []}} =
             DurableCodec.encode!(:graph, graph) |> DurableCodec.decode(:graph)
  end

  test "normalizes graph open values without a map serialization round trip" do
    graph =
      Graph.new!(id: "g", metadata: %{owner: :operations})
      |> Graph.put_input!("priority", schema: Docket.Schema.enum([:low, :high]))
      |> Graph.put_edge!("start",
        from: "$start",
        to: "$finish",
        guard: Docket.Guard.equals("priority", :high)
      )

    {normalized, _bytes} = DurableCodec.encode_graph!(graph)

    assert normalized.metadata == %{"owner" => "operations"}
    assert normalized.inputs["priority"].schema.values == ["low", "high"]
    assert %Docket.Guard{args: ["priority", "high"]} = normalized.edges["start"].guard
    assert graph.metadata == %{owner: :operations}
  end

  test "rejects mismatched envelopes and malformed ETF" do
    graph_bytes = DurableCodec.encode!(:graph, Graph.new!(id: "g"))

    invalid = [
      {graph_bytes, :run},
      {etf({:docket, 2, :run, %{}}), :run},
      {graph_bytes <> <<0>>, :graph},
      {:erlang.term_to_binary(
         {:docket, 1, :run, %{payload: String.duplicate("a", 1_000)}},
         compressed: 9
       ), :run},
      {<<131, 255>>, :run}
    ]

    for {bytes, kind} <- invalid do
      assert {:error, %Docket.Error{type: :invalid_durable_state}} =
               DurableCodec.decode(bytes, kind)
    end
  end

  test "rejects runtime resources and foreign or spoofed structs" do
    graph = Graph.new!(id: "g")

    write_values = [self(), make_ref(), fn -> :nope end, %Docket.Event{}, [1 | 2], <<1::1>>]

    for value <- write_values do
      assert_raise Docket.Error, fn ->
        DurableCodec.encode!(:graph, %{graph | metadata: %{"bad" => value}})
      end
    end

    unsafe = etf({:docket, 1, :run, %{pid: self()}})
    assert {:error, %Docket.Error{}} = DurableCodec.decode(unsafe, :run)

    spoofed = Map.put(graph, :unexpected, true)
    bytes = etf({:docket, 1, :graph, spoofed})
    assert {:error, %Docket.Error{}} = DurableCodec.decode(bytes, :graph)
  end

  test "rejects malformed collection structs, DateTimes, and improper lists as typed errors" do
    cold = String.to_atom("docket_bad_marker_#{System.unique_integer([:positive])}")
    malformed_set = %MapSet{map: %{"x" => cold}}
    malformed_datetime = %{~U[2026-07-03 10:00:00Z] | zone_abbr: cold}
    malformed_channel = Map.delete(%Docket.Run.ChannelState{}, :version)

    for term <- [
          %{set: malformed_set},
          %{at: malformed_datetime},
          %{channel: malformed_channel},
          %{list: [1 | 2]}
        ] do
      bytes = etf({:docket, 1, :run, term})

      assert {:error, %Docket.Error{type: :invalid_durable_state}} =
               DurableCodec.decode(bytes, :run)
    end
  end

  test "safe-decodes parked task atoms in a fresh VM" do
    state = %{
      active_tasks: %{"task" => %Docket.Run.TaskState{status: :retry_scheduled}},
      interrupts: %{
        "approval" => %Docket.Run.InterruptState{
          id: "approval",
          node_id: "review",
          status: :open,
          resume_channel: "decision",
          schema: Docket.Schema.enum(["yes", "no"]),
          created_at: ~U[2026-07-03 10:00:00Z]
        }
      },
      timers: %{
        "task" => %Docket.Run.TimerState{kind: :retry, fires_at: ~U[2026-07-03 10:00:00Z]}
      }
    }

    encoded = DurableCodec.encode!(:run, state) |> Base.encode64()
    ebin = Path.expand("_build/test/lib/docket/ebin")

    script = """
    bytes = System.fetch_env!("DOCKET_ETF") |> Base.decode64!()
    Docket.DurableCodec.decode!(bytes, :run)
    IO.write("ok")
    """

    assert {"ok", 0} =
             System.cmd(System.find_executable("elixir"), ["-pa", ebin, "-e", script],
               env: [{"DOCKET_ETF", encoded}],
               stderr_to_stdout: true
             )
  end

  test "safe-decodes normalized host atom values in a fresh VM" do
    cold_atom = String.to_atom("docket_cold_atom_#{System.unique_integer([:positive])}")

    graph =
      Graph.new!(
        id: "g",
        metadata: %{
          owner: cold_atom,
          tuple: {:tag, cold_atom},
          set: MapSet.new([cold_atom]),
          at: ~U[2026-07-03 10:00:00Z]
        }
      )

    encoded = DurableCodec.encode!(:graph, graph) |> Base.encode64()
    ebin = Path.expand("_build/test/lib/docket/ebin")

    script = """
    bytes = System.fetch_env!("DOCKET_ETF") |> Base.decode64!()
    graph = Docket.DurableCodec.decode!(bytes, :graph)
    expected = graph.metadata["owner"]
    {"tag", ^expected} = graph.metadata["tuple"]
    true = MapSet.member?(graph.metadata["set"], expected)
    %DateTime{} = graph.metadata["at"]
    IO.write(expected)
    """

    expected = Atom.to_string(cold_atom)

    assert {^expected, 0} =
             System.cmd(System.find_executable("elixir"), ["-pa", ebin, "-e", script],
               env: [{"DOCKET_ETF", encoded}],
               stderr_to_stdout: true
             )
  end
end
