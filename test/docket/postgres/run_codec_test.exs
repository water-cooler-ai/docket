if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunCodecTest do
    use ExUnit.Case, async: true

    alias Docket.Postgres.RunCodec
    alias Docket.Postgres.Schemas.Run, as: RunRow
    alias Docket.Run
    alias Docket.Run.{ChannelState, Failure, InterruptState, PendingWrite, TaskState, TimerState}
    alias Docket.Schema

    @started_at ~U[2026-07-10 12:00:00.123456Z]
    @updated_at ~U[2026-07-10 12:00:01.234567Z]
    @finished_at ~U[2026-07-10 12:00:02.345678Z]

    @promoted_wire_keys ~w(
      id
      graph_id
      graph_hash
      status
      step
      input
      output
      failure
      metadata
      checkpoint_seq
      started_at
      updated_at
      finished_at
    )

    test "round trips each durable status through its valid row projection" do
      for status <- Run.durable_statuses() do
        run = run_for_status(status)

        assert {:ok, attrs} = RunCodec.dump(run)
        assert attrs.status == status
        assert {:ok, loaded} = RunCodec.load(attrs)
        assert loaded == run
      end
    end

    test "promotes failure exactly once and reconstructs its typed value" do
      run = run_for_status(:failed)

      assert {:ok, attrs} = RunCodec.dump(run)

      assert attrs.failure == %{
               "code" => "node_failed",
               "message" => "reviewer failed permanently",
               "node_id" => "reviewer",
               "details" => %{"attempts" => 3}
             }

      refute Map.has_key?(attrs.state, "failure")
      assert RunCodec.load!(attrs) == run
    end

    test "loads an Ecto run schema struct returned by the store" do
      run = base_run()
      assert {:ok, attrs} = RunCodec.dump(run)

      assert attrs |> then(&struct!(RunRow, &1)) |> RunCodec.load!() == run
    end

    test "keeps interrupts in opaque state and round trips them" do
      interrupt = %InterruptState{
        id: "interrupt-1",
        node_id: "approval",
        status: :open,
        resume_channel: "decision",
        prompt: "Approve?",
        schema: Schema.string(),
        created_at: @updated_at,
        metadata: %{"audience" => "operator"}
      }

      run = %{
        base_run()
        | status: :waiting,
          pending_nodes: MapSet.new(["approval"]),
          interrupts: %{interrupt.id => interrupt}
      }

      assert {:ok, attrs} = RunCodec.dump(run)
      assert attrs.state["interrupts"]["interrupt-1"]["status"] == "open"
      assert RunCodec.load!(attrs) == run
    end

    test "keeps a parked active superstep wholly in opaque state" do
      run = parked_run()

      assert {:ok, attrs} = RunCodec.dump(run)
      task_id = "run-1:3:flaky"

      assert attrs.state["active_tasks"][task_id]["attempt"] == 2
      assert [%{"kind" => "update", "node_id" => "steady"}] = attrs.state["pending_writes"]
      assert attrs.state["timers"][task_id]["kind"] == "retry"
      assert RunCodec.load!(attrs) == run
    end

    test "stores timestamps in columns while version and event sequence stay opaque" do
      run = %{base_run() | event_seq: 47, checkpoint_seq: 11}

      assert {:ok, attrs} = RunCodec.dump(run)
      assert attrs.started_at == @started_at
      assert attrs.updated_at == @updated_at
      assert attrs.finished_at == nil
      assert attrs.checkpoint_seq == 11
      assert attrs.state["version"] == 2
      assert attrs.state["event_seq"] == 47
      refute Map.has_key?(attrs, :event_seq)

      assert RunCodec.load!(attrs) == run
    end

    test "rejects created status and version-1 state without compatibility paths" do
      assert {:error, %Docket.Error{type: :invalid_run}} =
               RunCodec.dump(%{base_run() | status: :created})

      assert {:ok, attrs} = RunCodec.dump(base_run())

      assert {:error,
              %Docket.Error{
                type: :corrupt_run_row,
                details: %{cause_type: :invalid_document}
              }} = RunCodec.load(%{attrs | status: :created})

      version_one = put_in(attrs.state["version"], 1)

      assert {:error,
              %Docket.Error{
                type: :corrupt_run_row,
                details: %{cause_type: :unsupported_schema_version}
              }} = RunCodec.load(version_one)

      assert_raise Docket.Error, fn -> RunCodec.load!(version_one) end
    end

    test "dump rejects in-memory values that canonical serialization would normalize" do
      for run <- [
            %{base_run() | input: nil},
            %{base_run() | input: %{prompt: :review}},
            %{base_run() | started_at: ~U[2026-07-10 12:00:00Z]},
            %{base_run() | updated_at: ~U[2026-07-10 12:00:01.234Z]}
          ] do
        assert {:error, %Docket.Error{type: :invalid_run}} = RunCodec.dump(run)
      end
    end

    test "rejects every promoted key in state even when values agree" do
      assert {:ok, attrs} = RunCodec.dump(base_run())

      for key <- @promoted_wire_keys do
        collision = put_in(attrs.state[key], promoted_value(attrs, key))

        assert {:error,
                %Docket.Error{
                  type: :corrupt_run_row,
                  details: %{keys: [^key]}
                }} = RunCodec.load(collision)
      end
    end

    test "fails closed on missing or malformed opaque state" do
      assert {:ok, attrs} = RunCodec.dump(base_run())

      for corrupt <- [
            %{attrs | state: nil},
            %{attrs | state: Map.delete(attrs.state, "version")},
            put_in(attrs.state["event_seq"], "forty-seven"),
            put_in(attrs.state["unknown_internal"], true)
          ] do
        assert {:error, %Docket.Error{type: :corrupt_run_row}} = RunCodec.load(corrupt)
      end
    end

    test "preserves empty maps separately from nil optionals" do
      empty = %{base_run() | input: %{}, metadata: %{}, output: nil}
      present_empty = %{run_for_status(:done) | output: %{}}

      assert {:ok, empty_attrs} = RunCodec.dump(empty)
      assert empty_attrs.input == %{}
      assert empty_attrs.metadata == %{}
      assert empty_attrs.output == nil
      assert empty_attrs.failure == nil
      assert empty_attrs.finished_at == nil
      assert empty_attrs.state == %{"event_seq" => 0, "version" => 2}
      assert RunCodec.load!(empty_attrs) == empty

      assert {:ok, present_empty_attrs} = RunCodec.dump(present_empty)
      assert present_empty_attrs.output == %{}
      assert RunCodec.load!(present_empty_attrs).output == %{}
    end

    test "measures small and large logical input/state duplication deterministically" do
      measurements =
        Map.new([small: 1_024, large: 1_048_576], fn {label, payload_bytes} ->
          payload = String.duplicate("x", payload_bytes)

          run = %{
            base_run()
            | input: %{"prompt" => payload},
              channels: %{
                "input:prompt" => %ChannelState{
                  channel_id: "input:prompt",
                  value: payload,
                  version: 1
                }
              }
          }

          assert {:ok, attrs} = RunCodec.dump(run)
          {label, duplication_measurement(attrs)}
        end)

      assert measurements == %{
               small: %{
                 payload_value_bytes: 1_024,
                 promoted_input_json_bytes: 1_037,
                 opaque_state_json_bytes: 1_104,
                 logical_total_json_bytes: 2_141,
                 logical_duplicated_value_bytes: 1_024,
                 logical_row_to_state_amplification_ppm: 1_939_311
               },
               large: %{
                 payload_value_bytes: 1_048_576,
                 promoted_input_json_bytes: 1_048_589,
                 opaque_state_json_bytes: 1_048_656,
                 logical_total_json_bytes: 2_097_245,
                 logical_duplicated_value_bytes: 1_048_576,
                 logical_row_to_state_amplification_ppm: 1_999_936
               }
             }
    end

    defp base_run do
      %Run{
        id: "run-1",
        graph_id: "review-graph",
        graph_hash: String.duplicate("ab", 32),
        status: :running,
        input: %{},
        started_at: @started_at,
        updated_at: @updated_at
      }
    end

    defp run_for_status(:running), do: base_run()
    defp run_for_status(:waiting), do: %{base_run() | status: :waiting}

    defp run_for_status(:done) do
      %{base_run() | status: :done, output: %{"answer" => 42}, finished_at: @finished_at}
    end

    defp run_for_status(:failed) do
      %{
        base_run()
        | status: :failed,
          failure:
            Failure.new("node_failed", "reviewer failed permanently",
              node_id: "reviewer",
              details: %{"attempts" => 3}
            ),
          finished_at: @finished_at
      }
    end

    defp run_for_status(:cancelled) do
      %{base_run() | status: :cancelled, finished_at: @finished_at}
    end

    defp parked_run do
      run = %{base_run() | step: 3}
      snapshot = %{"prompt" => "review", "draft" => "v1"}
      task_id = TaskState.task_id(run.id, run.step, "flaky")

      %{
        run
        | active_tasks: %{
            task_id => %TaskState{
              task_id: task_id,
              node_id: "flaky",
              step: run.step,
              attempt: 2,
              status: :retry_scheduled,
              input_hash: TaskState.snapshot_hash(snapshot),
              idempotency_key: TaskState.idempotency_key(task_id, 2),
              snapshot: snapshot,
              source_versions: %{"prompt" => 1, "draft" => 2},
              failures: [%{attempt: 1, reason: ":transient"}]
            }
          },
          pending_writes: [
            %PendingWrite{
              task_id: TaskState.task_id(run.id, run.step, "steady"),
              node_id: "steady",
              attempt: 1,
              kind: :update,
              value: %{"draft" => "v2"}
            }
          ],
          timers: %{
            task_id => %TimerState{
              kind: :retry,
              fires_at: ~U[2026-07-10 12:00:05.000000Z]
            }
          }
      }
    end

    defp promoted_value(attrs, "id"), do: attrs.run_id
    defp promoted_value(attrs, "graph_id"), do: attrs.graph_id
    defp promoted_value(attrs, "graph_hash"), do: attrs.graph_hash
    defp promoted_value(attrs, "status"), do: Atom.to_string(attrs.status)
    defp promoted_value(attrs, "step"), do: attrs.step
    defp promoted_value(attrs, "input"), do: attrs.input
    defp promoted_value(attrs, "output"), do: attrs.output
    defp promoted_value(attrs, "failure"), do: attrs.failure
    defp promoted_value(attrs, "metadata"), do: attrs.metadata
    defp promoted_value(attrs, "checkpoint_seq"), do: attrs.checkpoint_seq

    defp promoted_value(attrs, "started_at"),
      do: DateTime.to_iso8601(attrs.started_at)

    defp promoted_value(attrs, "updated_at"),
      do: DateTime.to_iso8601(attrs.updated_at)

    defp promoted_value(attrs, "finished_at"), do: attrs.finished_at

    defp duplication_measurement(attrs) do
      promoted_value = attrs.input["prompt"]
      opaque_value = attrs.state["channels"]["input:prompt"]["value"]
      promoted_input_json_bytes = attrs.input |> Jason.encode!() |> byte_size()
      opaque_state_json_bytes = attrs.state |> Jason.encode!() |> byte_size()
      logical_total_json_bytes = promoted_input_json_bytes + opaque_state_json_bytes

      %{
        payload_value_bytes: byte_size(promoted_value),
        promoted_input_json_bytes: promoted_input_json_bytes,
        opaque_state_json_bytes: opaque_state_json_bytes,
        logical_total_json_bytes: logical_total_json_bytes,
        logical_duplicated_value_bytes:
          if(promoted_value == opaque_value, do: byte_size(promoted_value), else: 0),
        # Integer parts-per-million keeps the fixture exact. This is logical
        # encoded JSON size relative to the state-only recovery payload, not a
        # claim about physical PostgreSQL json, TOAST, or WAL write amplification.
        logical_row_to_state_amplification_ppm:
          div(logical_total_json_bytes * 1_000_000, opaque_state_json_bytes)
      }
    end
  end
end
