if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.RunCodecTest do
    use ExUnit.Case, async: true

    alias Docket.{DurableCodec, Run}
    alias Docket.Postgres.RunCodec
    alias Docket.Postgres.Schemas.Run, as: RunRow
    alias Docket.Run.{ChannelState, Failure, InterruptState, PendingWrite, TaskState, TimerState}

    @started_at ~U[2026-07-10 12:00:00.123456Z]
    @updated_at ~U[2026-07-10 12:00:01.234567Z]
    @finished_at ~U[2026-07-10 12:00:02.345678Z]

    test "round trips every durable status through one bytea state" do
      for status <- Run.durable_statuses() do
        run = run_for_status(status)

        assert {:ok, attrs} = RunCodec.dump(run)
        assert is_binary(attrs.state)
        assert RunCodec.load!(attrs) === run
        assert attrs |> then(&struct!(RunRow, &1)) |> RunCodec.load!() === run
      end
    end

    test "state is a direct term projection with no relational duplicates" do
      channel = %ChannelState{channel_id: "input:prompt", value: "review", version: 1}

      run = %{
        base_run()
        | input: %{"prompt" => "review"},
          channels: %{channel.channel_id => channel},
          changed_channels: MapSet.new([channel.channel_id]),
          metadata: %{"source" => "test"}
      }

      assert {:ok, attrs} = RunCodec.dump(run)
      state = DurableCodec.decode!(attrs.state, :run)

      assert state.input == %{"prompt" => "review"}
      assert state.channels[channel.channel_id] === channel
      refute Map.has_key?(state, :id)
      refute Map.has_key?(attrs, :input)
      refute Map.has_key?(attrs, :metadata)
      assert RunCodec.load!(attrs) === run
    end

    test "round trips waiting interrupts and a parked active superstep" do
      interrupt = %InterruptState{
        id: "approval",
        node_id: "review",
        status: :open,
        resume_channel: "decision",
        prompt: "Approve?",
        schema: Docket.Schema.string(),
        created_at: @updated_at,
        metadata: %{}
      }

      waiting = %{
        base_run()
        | status: :waiting,
          pending_nodes: MapSet.new(["review"]),
          interrupts: %{interrupt.id => interrupt}
      }

      assert {:ok, waiting_attrs} = RunCodec.dump(waiting)
      assert RunCodec.load!(waiting_attrs) === waiting

      snapshot = %{"state:input" => "value"}
      task_id = TaskState.task_id(base_run().id, 0, "retrying")

      task = %TaskState{
        task_id: task_id,
        node_id: "retrying",
        step: 0,
        attempt: 2,
        status: :retry_scheduled,
        input_hash: TaskState.snapshot_hash(snapshot),
        idempotency_key: TaskState.idempotency_key(task_id, 2),
        snapshot: snapshot,
        source_versions: %{"state:input" => 1},
        failures: [%{attempt: 1, reason: "retry"}]
      }

      pending = %PendingWrite{
        task_id: TaskState.task_id(base_run().id, 0, "finished"),
        node_id: "finished",
        attempt: 1,
        kind: :update,
        value: %{"result" => "done"}
      }

      parked = %{
        base_run()
        | active_tasks: %{task_id => task},
          pending_writes: [pending],
          timers: %{task_id => %TimerState{kind: :retry, fires_at: @finished_at}}
      }

      assert {:ok, parked_attrs} = RunCodec.dump(parked)
      assert RunCodec.load!(parked_attrs) === parked
    end

    test "rejects invalid runs and non-durable state" do
      atom_schema_interrupt = %InterruptState{
        id: "approval",
        node_id: "review",
        status: :open,
        resume_channel: "decision",
        schema: Docket.Schema.enum([:cold_host_atom]),
        created_at: @updated_at
      }

      invalid = [
        %{base_run() | status: :created},
        %{base_run() | input: %{owner: self()}},
        %{base_run() | started_at: ~U[2026-07-10 12:00:00Z]},
        %{
          base_run()
          | status: :waiting,
            interrupts: %{atom_schema_interrupt.id => atom_schema_interrupt}
        }
      ]

      for run <- invalid do
        assert {:error, %Docket.Error{}} = RunCodec.dump(run)
      end
    end

    test "fails closed on corrupt ETF, wrong state shape, and invalid reassembly" do
      assert {:ok, attrs} = RunCodec.dump(base_run())

      failed_without_failure = %{
        attrs
        | status: :failed,
          finished_at: @finished_at
      }

      corrupt_rows = [
        %{attrs | state: attrs.state <> <<0>>},
        %{attrs | state: DurableCodec.encode!(:run, %{input: %{}})},
        %{attrs | status: :done},
        failed_without_failure
      ]

      for row <- corrupt_rows do
        assert {:error, %Docket.Error{type: :corrupt_run_row}} = RunCodec.load(row)
        assert_raise Docket.Error, fn -> RunCodec.load!(row) end
      end
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

    defp run_for_status(:done),
      do: %{base_run() | status: :done, output: %{"answer" => 42}, finished_at: @finished_at}

    defp run_for_status(:failed) do
      %{
        base_run()
        | status: :failed,
          failure: Failure.new("node_failed", "reviewer failed permanently"),
          finished_at: @finished_at
      }
    end

    defp run_for_status(:cancelled),
      do: %{base_run() | status: :cancelled, finished_at: @finished_at}
  end
end
