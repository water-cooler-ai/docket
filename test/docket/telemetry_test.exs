defmodule Docket.TelemetryTest do
  use Docket.Test.Case, async: true

  test "metric metadata drops identities and correlation references" do
    metadata = %{
      operation: :moment,
      result: :ok,
      lifecycle_ref: make_ref(),
      run_id: "run-1",
      claim_token: "secret"
    }

    assert Docket.Telemetry.metric_metadata(
             [:docket, :lifecycle, :transaction, :stop],
             metadata
           ) == %{operation: :moment, result: :ok}

    assert Docket.Telemetry.metric_metadata([:docket, :run, :completed], metadata) == %{}
  end

  test "lifecycle and nested store spans share correlation without leaking it to labels" do
    parent = self()
    id = "lifecycle-correlation-#{System.unique_integer([:positive])}"
    tag = make_ref()

    :telemetry.attach_many(
      id,
      [
        [:docket, :lifecycle, :transaction, :start],
        [:docket, :lifecycle, :transaction, :stop],
        [:docket, :store, :operation, :start],
        [:docket, :store, :operation, :stop]
      ],
      &Docket.Test.TelemetryRelay.tagged_event/4,
      {parent, tag}
    )

    on_exit(fn -> :telemetry.detach(id) end)

    assert :ok =
             Docket.Telemetry.lifecycle_span(:moment, fn ->
               metadata = Map.put(Docket.Telemetry.correlation_metadata(), :operation, :test)

               Docket.Telemetry.span([:docket, :store, :operation], metadata, fn ->
                 {:ok, %{result: :ok}}
               end)
             end)

    events =
      Enum.map(1..4, fn _ ->
        receive do
          {^tag, name, measurements, metadata} -> {name, measurements, metadata}
        after
          100 -> flunk("missing span")
        end
      end)

    refs = Enum.map(events, fn {_name, _measurements, metadata} -> metadata.lifecycle_ref end)
    assert length(Enum.uniq(refs)) == 1

    assert Enum.map(events, &elem(&1, 0)) == [
             [:docket, :lifecycle, :transaction, :start],
             [:docket, :store, :operation, :start],
             [:docket, :store, :operation, :stop],
             [:docket, :lifecycle, :transaction, :stop]
           ]
  end

  test "span exception telemetry is bounded" do
    parent = self()
    id = "bounded-exception-#{System.unique_integer([:positive])}"
    name = [:docket, :store, :operation, :exception]
    :telemetry.attach(id, name, &Docket.Test.TelemetryRelay.raw/4, parent)
    on_exit(fn -> :telemetry.detach(id) end)

    assert_raise RuntimeError, "secret token 123", fn ->
      Docket.Telemetry.span([:docket, :store, :operation], %{operation: :test}, fn ->
        raise "secret token 123"
      end)
    end

    assert_receive {^name, %{duration: duration}, %{operation: :test, result: :exception}}
    assert is_integer(duration) and duration >= 0
  end

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
      &Docket.Test.TelemetryRelay.event/4,
      parent
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
