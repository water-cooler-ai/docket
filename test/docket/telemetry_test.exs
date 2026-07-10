defmodule Docket.TelemetryTest do
  use Docket.Test.Case, async: true

  # One telemetry event per run event, emitted only for committed
  # transitions, carrying run/graph identity but never channel values.

  @events [
    [:docket, :run, :initialized],
    [:docket, :run, :completed],
    [:docket, :run, :failed],
    [:docket, :checkpoint, :committed],
    [:docket, :node, :completed],
    [:docket, :node, :failed],
    [:docket, :channel, :updated],
    [:docket, :edge, :triggered],
    [:docket, :interrupt, :requested],
    [:docket, :interrupt, :resolved]
  ]

  setup context do
    parent = self()
    handler_id = {context.module, context.test}

    :telemetry.attach_many(
      handler_id,
      @events,
      fn name, measurements, metadata, _config ->
        send(parent, {:telemetry, name, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  # Telemetry handlers are VM-global, so concurrent tests' runs also fire
  # this test's handler: always filter received events to one run.
  defp received_events(run_id) do
    receive_all([], run_id)
  end

  defp receive_all(acc, run_id) do
    receive do
      {:telemetry, name, measurements, %{run_id: ^run_id} = metadata} ->
        receive_all([{name, measurements, metadata} | acc], run_id)

      {:telemetry, _name, _measurements, _metadata} ->
        receive_all(acc, run_id)
    after
      0 -> Enum.reverse(acc)
    end
  end

  test "a completed run emits lifecycle, checkpoint, node, channel, and edge events" do
    assert {:ok, run, _checkpoints} =
             Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"})

    assert run.status == :done
    events = received_events(run.id)
    names = Enum.map(events, fn {name, _measurements, _metadata} -> name end)

    assert [:docket, :run, :initialized] == hd(names)
    assert List.last(names) == [:docket, :checkpoint, :committed]
    assert [:docket, :node, :completed] in names
    assert [:docket, :channel, :updated] in names
    assert [:docket, :edge, :triggered] in names
    assert Enum.count(names, &(&1 == [:docket, :checkpoint, :committed])) == 3

    {_name, measurements, metadata} =
      Enum.find(events, fn {name, _m, _md} -> name == [:docket, :node, :completed] end)

    assert measurements.step == 0
    assert is_integer(measurements.seq)
    assert metadata.run_id == run.id
    assert metadata.graph_id == run.graph_id
    assert metadata.graph_hash == run.graph_hash
    assert metadata.node_id == "copy"
    assert %Docket.Event{type: :node_completed} = metadata.event

    {_name, checkpoint_measurements, checkpoint_metadata} =
      Enum.find(events, fn {name, _m, _md} ->
        name == [:docket, :checkpoint, :committed]
      end)

    assert checkpoint_measurements.seq == checkpoint_metadata.event.seq
    assert checkpoint_metadata.event.metadata["checkpoint_seq"] == 1
    assert checkpoint_metadata.event.metadata["checkpoint_type"] == "run_initialized"
  end

  test "failed runs and interrupts emit their events" do
    assert {:ok, run, _} = Docket.Test.run_inline(Graphs.parallel_failure(), %{})
    assert run.status == :failed

    names = Enum.map(received_events(run.id), fn {name, _m, _md} -> name end)
    assert [:docket, :node, :failed] in names
    assert [:docket, :run, :failed] in names
    assert List.last(names) == [:docket, :checkpoint, :committed]

    assert {:ok, run, _} = Docket.Test.run_inline(Graphs.interrupt_review(), %{})
    assert run.status == :waiting
    [interrupt_id] = Map.keys(run.interrupts)

    assert [:docket, :interrupt, :requested] in Enum.map(
             received_events(run.id),
             fn {name, _m, _md} -> name end
           )

    assert {:ok, run, _} =
             Docket.Test.resolve_interrupt_inline(run, interrupt_id, "approved",
               graph: Graphs.interrupt_review()
             )

    assert run.status == :done

    names = Enum.map(received_events(run.id), fn {name, _m, _md} -> name end)
    assert [:docket, :interrupt, :resolved] in names
    assert [:docket, :run, :completed] in names
    assert List.last(names) == [:docket, :checkpoint, :committed]
  end

  test "channel events never carry channel values" do
    assert {:ok, run, _} = Docket.Test.run_inline(Graphs.minimal_linear(), %{"value" => "x"})

    for {name, _measurements, metadata} <- received_events(run.id),
        name == [:docket, :channel, :updated] do
      refute Map.has_key?(metadata.payload, "value")
      assert is_binary(metadata.channel_id)
    end
  end
end
