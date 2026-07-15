defmodule Docket.Runtime.MomentTest do
  use Docket.Test.Case, async: true

  alias Docket.Runtime.{Loop, Moment}
  alias Docket.Test.MemoryBackend

  @now DateTime.from_naive!(~N[2026-07-10 12:00:00], "Etc/UTC")

  defp opts(overrides \\ []) do
    Keyword.merge(
      [
        clock: fn -> @now end,
        id_generator: fn kind -> "#{kind}_fixed" end,
        run_id: "run_fixed"
      ],
      overrides
    )
  end

  defp watch_telemetry(run_id) do
    handler_id = {__MODULE__, run_id, self()}

    names = [
      [:docket, :run, :initialized],
      [:docket, :run, :completed],
      [:docket, :run, :failed],
      [:docket, :node, :completed],
      [:docket, :node, :failed],
      [:docket, :channel, :updated],
      [:docket, :edge, :triggered],
      [:docket, :interrupt, :requested],
      [:docket, :interrupt, :resolved]
    ]

    :telemetry.attach_many(
      handler_id,
      names,
      &Docket.Test.TelemetryRelay.filtered_name/4,
      %{pid: self(), run_id: run_id}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp propose_init!(rtg, input, opts) do
    run = Loop.build_initial_run(rtg, input, opts)
    {:ok, %Moment{} = moment} = Loop.propose_init(rtg, run, opts)
    moment
  end

  # Drives propose -> continue until a terminal moment, collecting every
  # commit-boundary moment, without ever committing anywhere.
  defp drain(rtg, run, opts, moments \\ []) do
    case Loop.propose_advance(rtg, run, opts) do
      {:ok, %Moment{disposition: {:park, :terminal, _reason}} = moment} ->
        Enum.reverse([moment | moments])

      {:ok, %Moment{} = moment} ->
        drain(rtg, moment.run, opts, [moment | moments])

      {:wait, _run, _interrupt_ids} ->
        Enum.reverse(moments)
    end
  end

  describe "propose_init/3" do
    test "calculates one moment without handler delivery or telemetry" do
      rtg = compile!(Graphs.minimal_linear())
      opts = opts()
      watch_telemetry("run_fixed")

      moment = propose_init!(rtg, %{"value" => "hello"}, opts)

      assert %Moment{checkpoint_type: :run_initialized, disposition: :continue} = moment
      assert moment.run.status == :running
      assert moment.run.checkpoint_seq == 1
      assert moment.proposed_at == @now

      assert [first | _rest] = moment.events
      assert first.type == :run_initialized
      assert Enum.map(moment.events, & &1.seq) == Enum.to_list(1..length(moment.events))
      assert moment.run.event_seq == length(moment.events)

      assert %Docket.Event{type: :checkpoint_committed, metadata: metadata} =
               List.last(moment.events)

      assert metadata == moment.checkpoint_metadata
      assert metadata["checkpoint_seq"] == 1
      assert metadata["checkpoint_type"] == "run_initialized"
      assert metadata["graph_step"] == 0
      assert metadata["park_reason"] == nil
      assert metadata["wake_disposition"] == "continue"
      assert metadata["active_superstep"] == nil
      assert metadata["node_attempts"] == []

      refute_received {:telemetry, _name}
    end

    test "an already-terminal run proposes nothing" do
      rtg = compile!(Graphs.minimal_linear())
      {:ok, done, _checkpoints} = Docket.Test.run_inline(rtg, %{"value" => "x"})

      assert {:terminal, ^done} = Loop.propose_init(rtg, done, opts())
    end

    test "the proposed run matches host-owned initialization exactly" do
      rtg = compile!(Graphs.minimal_linear())
      moment = propose_init!(rtg, %{"value" => "hello"}, opts())

      committed = moment.run
      checkpoint = Moment.checkpoint(moment)

      assert moment.run == committed
      assert moment.events == checkpoint.events
      assert moment.checkpoint_type == checkpoint.type
    end
  end

  describe "yield/2" do
    defp continue_moment!(rtg, input \\ %{}) do
      init = propose_init!(rtg, input, opts())

      {:ok, %Moment{disposition: :continue} = moment} =
        Loop.propose_advance(rtg, init.run, opts())

      moment
    end

    test "narrows :continue to an immediate park with a consistent envelope" do
      moment = continue_moment!(compile!(Graphs.cycle_counter()))
      assert moment.checkpoint_type == :step_committed

      assert {:ok, yielded} = Moment.yield(moment, :drain_budget)

      assert yielded.disposition == {:park, :immediate, :drain_budget}
      assert yielded.checkpoint_type == :step_committed

      expected_metadata =
        Map.merge(moment.checkpoint_metadata, %{
          "wake_disposition" => "immediate",
          "park_reason" => "drain_budget"
        })

      assert yielded.checkpoint_metadata == expected_metadata
      assert %Docket.Event{type: :checkpoint_committed} = final = List.last(yielded.events)
      assert final.metadata == expected_metadata
    end

    test "preserves run, sequences, timestamps, payloads, and identity" do
      moment = continue_moment!(compile!(Graphs.cycle_counter()))
      {:ok, yielded} = Moment.yield(moment, :drain_budget)

      assert yielded.run == moment.run
      assert yielded.proposed_at == moment.proposed_at
      assert yielded.pending_attempts == moment.pending_attempts
      assert length(yielded.events) == length(moment.events)

      strip = &Enum.map(&1, fn event -> %{event | metadata: nil} end)
      assert strip.(yielded.events) == strip.(moment.events)
      assert Enum.map(yielded.events, & &1.seq) == Enum.map(moment.events, & &1.seq)
      assert Enum.map(yielded.events, & &1.timestamp) == Enum.map(moment.events, & &1.timestamp)
      assert Enum.map(yielded.events, & &1.payload) == Enum.map(moment.events, & &1.payload)
    end

    test "preserves :interrupt_requested on a barrier that stays runnable" do
      moment = continue_moment!(compile!(Graphs.interrupt_with_parallel_work()))
      assert moment.checkpoint_type == :interrupt_requested
      assert moment.run.status == :running

      assert {:ok, yielded} = Moment.yield(moment, :drain_budget)

      assert yielded.checkpoint_type == :interrupt_requested
      assert yielded.disposition == {:park, :immediate, :drain_budget}
      assert yielded.checkpoint_metadata["checkpoint_type"] == "interrupt_requested"
      assert List.last(yielded.events).metadata == yielded.checkpoint_metadata
    end

    test "never overrides an existing park or terminal disposition" do
      rtg = compile!(Graphs.minimal_linear())
      init = propose_init!(rtg, %{"value" => "x"}, opts())
      terminal = List.last(drain(rtg, init.run, opts()))

      assert terminal.disposition == {:park, :terminal, :run_completed}

      assert {:error, {:not_continue, {:park, :terminal, :run_completed}}} =
               Moment.yield(terminal, :drain_budget)

      retry_rtg = compile!(Graphs.retry_then_continue())
      retry_init = propose_init!(retry_rtg, %{}, opts())
      {:ok, retry_moment} = Loop.propose_advance(retry_rtg, retry_init.run, opts())

      assert {:park, {:at, %DateTime{}}, :retry_backoff} = retry_moment.disposition
      assert {:error, {:not_continue, _park}} = Moment.yield(retry_moment, :drain_budget)
    end

    test "rejects malformed moments" do
      moment = continue_moment!(compile!(Graphs.cycle_counter()))
      {runtime_events, [checkpoint_event]} = Enum.split(moment.events, -1)

      assert {:error, :malformed_moment} =
               Moment.yield(%{moment | events: runtime_events}, :drain_budget)

      assert {:error, :malformed_moment} =
               Moment.yield(%{moment | events: []}, :drain_budget)

      assert {:error, :malformed_moment} =
               Moment.yield(%{moment | checkpoint_metadata: %{}}, :drain_budget)

      duplicated = %{moment | events: moment.events ++ [checkpoint_event]}
      assert {:error, :malformed_moment} = Moment.yield(duplicated, :drain_budget)
    end
  end

  describe "propose_advance/3" do
    test "drives a multi-step graph one commit boundary at a time to inline parity" do
      graph = Graphs.cycle_counter()
      rtg = compile!(graph)

      {:ok, inline_run, inline_checkpoints} =
        Docket.Test.run_inline(rtg, %{}, opts())

      init_moment = propose_init!(rtg, %{}, opts())
      moments = [init_moment | drain(rtg, init_moment.run, opts())]

      assert Enum.map(moments, & &1.checkpoint_type) == checkpoint_types(inline_checkpoints)
      assert List.last(moments).run == inline_run
      assert List.last(moments).disposition == {:park, :terminal, :run_completed}

      assert Enum.map(moments, & &1.run.checkpoint_seq) ==
               Enum.to_list(1..length(moments))
    end

    test "never delivers a checkpoint or telemetry during calculation" do
      rtg = compile!(Graphs.minimal_linear())
      opts = opts()
      watch_telemetry("run_fixed")

      init_moment = propose_init!(rtg, %{"value" => "hi"}, opts)
      moments = drain(rtg, init_moment.run, opts)

      assert Enum.map(moments, & &1.checkpoint_type) == [:step_committed, :run_completed]
      refute_received {:telemetry, _name}
    end

    test "a retryable failure proposes a retry moment that stays running" do
      rtg = compile!(Graphs.retry_then_continue())
      opts = opts()

      init_moment = propose_init!(rtg, %{}, opts)
      assert {:ok, retry_moment} = Loop.propose_advance(rtg, init_moment.run, opts)

      assert retry_moment.checkpoint_type == :retry_scheduled
      assert retry_moment.run.status == :running
      assert retry_moment.run.step == 0
      assert {:park, {:at, %DateTime{}}, :retry_backoff} = retry_moment.disposition

      assert [{task_id, task}] = Map.to_list(retry_moment.run.active_tasks)
      assert task.status == :retry_scheduled
      assert task.attempt == 2
      assert %Docket.Run.TimerState{kind: :retry} = retry_moment.run.timers[task_id]

      context = Moment.context(retry_moment)
      assert context.checkpoint_seq == retry_moment.run.checkpoint_seq
      assert context.graph_step == 0

      assert context.active_superstep == %{
               step: 0,
               tasks: [
                 %{
                   task_id: task_id,
                   node_id: "flaky",
                   scheduled_attempt: 2,
                   idempotency_key: "#{task_id}:2"
                 }
               ],
               pending_attempts: []
             }

      assert context.node_attempts == [
               %{
                 task_id: task_id,
                 node_id: "flaky",
                 attempted: 1,
                 outcome: :failed,
                 next_scheduled_attempt: 2
               }
             ]

      assert retry_moment.checkpoint_metadata["active_superstep"] == %{
               "graph_step" => 0,
               "tasks" => [
                 %{
                   "task_id" => task_id,
                   "node_id" => "flaky",
                   "scheduled_attempt" => 2,
                   "idempotency_key" => "#{task_id}:2"
                 }
               ],
               "pending_attempts" => []
             }

      assert retry_moment.checkpoint_metadata["node_attempts"] == [
               %{
                 "task_id" => task_id,
                 "node_id" => "flaky",
                 "attempted" => 1,
                 "outcome" => "failed",
                 "next_scheduled_attempt" => 2
               }
             ]

      assert retry_moment.checkpoint_metadata["checkpoint_type"] == "retry_scheduled"
      assert retry_moment.checkpoint_metadata["graph_step"] == 0
      assert retry_moment.checkpoint_metadata["park_reason"] == "retry_backoff"
      assert retry_moment.checkpoint_metadata["wake_disposition"] == "at"

      moments = drain(rtg, retry_moment.run, opts)

      assert Enum.map(moments, & &1.checkpoint_type) ==
               [:retry_scheduled, :step_committed, :run_completed]

      assert List.last(moments).run.status == :done
    end

    test "every moment appends exactly one checkpoint fact after its runtime facts" do
      rtg = compile!(Graphs.minimal_linear())
      init_moment = propose_init!(rtg, %{"value" => "hello"}, opts())
      moments = [init_moment | drain(rtg, init_moment.run, opts())]

      for moment <- moments do
        checkpoint_facts = Enum.filter(moment.events, &(&1.type == :checkpoint_committed))
        assert [checkpoint_fact] = checkpoint_facts
        assert checkpoint_fact == List.last(moment.events)
        assert checkpoint_fact.timestamp == moment.proposed_at
        assert checkpoint_fact.seq == moment.run.event_seq
        assert checkpoint_fact.metadata["checkpoint_seq"] == moment.run.checkpoint_seq
        assert checkpoint_fact.metadata["graph_step"] == moment.run.step
      end

      all_events = Enum.flat_map(moments, & &1.events)
      assert Enum.map(all_events, & &1.seq) == Enum.to_list(1..length(all_events))
      assert Enum.map(moments, & &1.run.checkpoint_seq) == [1, 2, 3]

      expected_event_seqs =
        Enum.scan(moments, 0, fn moment, prior_seq -> prior_seq + length(moment.events) end)

      assert Enum.map(moments, & &1.run.event_seq) == expected_event_seqs
    end

    test "an active superstep with no attempt due parks uncommitted" do
      graph =
        Graph.new!(id: "slow-retry")
        |> Graph.put_field!("out", schema: Docket.Schema.string())
        |> Graph.put_node!("flaky",
          implementation: Nodes.FlakyThenSucceeds,
          config: %{failures: 1.0, field: "out", value: "done"},
          policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 60_000}}
        )
        |> Graph.put_edge!("edge_start_flaky", from: "$start", to: "flaky")
        |> Graph.put_edge!("edge_flaky_finish", from: "flaky", to: "$finish")

      rtg = compile!(graph)
      opts = opts()

      init_moment = propose_init!(rtg, %{}, opts)
      assert {:ok, retry_moment} = Loop.propose_advance(rtg, init_moment.run, opts)

      deadline = DateTime.add(@now, 60_000, :millisecond)
      assert retry_moment.disposition == {:park, {:at, deadline}, :retry_backoff}

      # The frozen clock has not reached the deadline: nothing to commit,
      # and the committed run stays exactly as proposed by the retry moment.
      assert {:park, run, park} = Loop.propose_advance(rtg, retry_moment.run, opts)
      assert run == retry_moment.run
      assert park == %{resume_at: deadline, wait_ms: 60_000}

      # A driver that served the park's wait passes the deadline as the
      # floor, making exactly the due attempt eligible.
      floored = Keyword.put(opts, :resume_floor, deadline)
      assert {:ok, step_moment} = Loop.propose_advance(rtg, retry_moment.run, floored)
      assert step_moment.checkpoint_type == :step_committed
      assert step_moment.disposition == :continue
      assert step_moment.run.active_tasks == %{}
    end

    test "a waiting barrier parks externally; further advancement reports the interrupts" do
      rtg = compile!(Graphs.interrupt_review())
      opts = opts()

      init_moment = propose_init!(rtg, %{}, opts)
      assert {:ok, waiting_moment} = Loop.propose_advance(rtg, init_moment.run, opts)

      assert waiting_moment.checkpoint_type == :interrupt_requested
      assert waiting_moment.run.status == :waiting
      assert waiting_moment.disposition == {:park, :external, :awaiting_interrupts}

      interrupt_event = Enum.find(waiting_moment.events, &(&1.type == :interrupt_requested))
      assert is_binary(interrupt_event.task_id)
      assert interrupt_event.payload["attempt"] == 1

      assert Moment.context(waiting_moment).node_attempts == [
               %{
                 task_id: interrupt_event.task_id,
                 node_id: interrupt_event.node_id,
                 attempted: 1,
                 outcome: :interrupted,
                 next_scheduled_attempt: nil
               }
             ]

      assert waiting_moment.checkpoint_metadata["node_attempts"] == [
               %{
                 "task_id" => interrupt_event.task_id,
                 "node_id" => interrupt_event.node_id,
                 "attempted" => 1,
                 "outcome" => "interrupted",
                 "next_scheduled_attempt" => nil
               }
             ]

      assert {:wait, run, [interrupt_id]} = Loop.propose_advance(rtg, waiting_moment.run, opts)
      assert run.interrupts[interrupt_id].status == :open
    end

    test "an already-terminal run proposes nothing" do
      rtg = compile!(Graphs.minimal_linear())
      {:ok, done, _checkpoints} = Docket.Test.run_inline(rtg, %{"value" => "x"})

      assert {:terminal, ^done} = Loop.propose_advance(rtg, done, opts())
    end
  end

  describe "durable moment commitment" do
    # The moment disposition is decided by the runtime core; a lifecycle
    # driver derives the storage schedule effect from it. This mapping is
    # the test driver's, standing in for the composer that lands later.
    defp schedule_for(:continue), do: :retain_claim
    defp schedule_for({:park, :immediate, _reason}), do: {:release_claim, :immediate}
    defp schedule_for({:park, :external, _reason}), do: {:release_claim, :external}
    defp schedule_for({:park, {:at, at}, _reason}), do: {:release_claim, {:at, at}}
    defp schedule_for({:park, :terminal, _reason}), do: {:release_claim, :terminal}

    defp start_backend do
      opts = [clock: fn -> @now end]
      backend = start_supervised!(MemoryBackend.child_spec(opts, nil))
      backend
    end

    defp insert!(backend, moment) do
      {:ok, effective, publication_rtg} =
        Compiler.compile_for_publication(Graphs.minimal_linear())

      assert publication_rtg.graph_hash == moment.run.graph_hash

      {:ok, _run} =
        MemoryBackend.transaction(backend, fn tx ->
          with :ok <-
                 MemoryBackend.save_graph(
                   tx,
                   :tenantless,
                   moment.run.graph_id,
                   moment.run.graph_hash,
                   effective
                 ),
               {:ok, run} <-
                 MemoryBackend.insert_run(tx, :tenantless, moment.run, :run_initialized, @now) do
            case MemoryBackend.append_events(tx, :tenantless, run.id, moment.events) do
              :ok -> {:ok, run}
              {:error, reason} -> {:error, reason}
            end
          end
        end)

      :ok
    end

    defp claim!(backend) do
      policy = %{now: @now, limit: 10, orphan_ttl_ms: 60_000, max_claim_attempts: 5}
      {:ok, %{leases: [lease], poisoned: []}} = MemoryBackend.claim_due(backend, :system, policy)
      lease.claim_token
    end

    defp commit_moment(backend, moment, expected_seq, token) do
      MemoryBackend.transaction(backend, fn tx ->
        proposal = %{
          run: moment.run,
          expected_checkpoint_seq: expected_seq,
          claim_token: token,
          checkpoint_type: moment.checkpoint_type,
          schedule: schedule_for(moment.disposition)
        }

        with {:ok, run} <- MemoryBackend.commit(tx, :tenantless, proposal) do
          case MemoryBackend.append_events(tx, :tenantless, run.id, moment.events) do
            :ok -> {:ok, run}
            {:error, reason} -> {:error, reason}
          end
        end
      end)
    end

    defp published_runtime! do
      {:ok, _effective, rtg} = Compiler.compile_for_publication(Graphs.minimal_linear())
      rtg
    end

    test "a vehicle drives propose -> commit -> continue until the terminal park" do
      rtg = published_runtime!()
      opts = opts()
      backend = start_backend()

      init_moment = propose_init!(rtg, %{"value" => "hello"}, opts)
      insert!(backend, init_moment)
      token = claim!(backend)

      final =
        Enum.reduce_while(Stream.cycle([nil]), init_moment.run, fn nil, run ->
          {:ok, moment} = Loop.propose_advance(rtg, run, opts)
          {:ok, committed} = commit_moment(backend, moment, run.checkpoint_seq, token)

          # Only after transaction success does a committed checkpoint exist.
          checkpoint =
            Moment.checkpoint(moment)

          assert checkpoint.run == committed
          assert checkpoint.seq == committed.checkpoint_seq

          case moment.disposition do
            {:park, :terminal, _reason} -> {:halt, committed}
            :continue -> {:cont, committed}
          end
        end)

      assert final.status == :done
      assert {:ok, ^final} = MemoryBackend.fetch_run(backend, :tenantless, final.id)
      assert MemoryBackend.claim(backend, final.id) == nil
      assert MemoryBackend.wake_at(backend, final.id) == nil

      stored_types = Enum.map(MemoryBackend.events(backend, final.id), & &1.type)
      assert :run_initialized in stored_types
      assert :run_completed in stored_types
    end

    test "a lost fence discards the moment and exposes no committed checkpoint" do
      rtg = published_runtime!()
      opts = opts()
      backend = start_backend()

      init_moment = propose_init!(rtg, %{"value" => "hello"}, opts)
      insert!(backend, init_moment)
      _token = claim!(backend)

      {:ok, moment} = Loop.propose_advance(rtg, init_moment.run, opts)

      assert {:error, :stale_fence} =
               commit_moment(backend, moment, init_moment.run.checkpoint_seq, "stale-token")

      # The prior committed run remains the durable truth and none of the
      # discarded moment's events were appended.
      assert {:ok, run} = MemoryBackend.fetch_run(backend, :tenantless, "run_fixed")
      assert run == init_moment.run

      stored_seqs = Enum.map(MemoryBackend.events(backend, "run_fixed"), & &1.seq)
      assert stored_seqs == Enum.map(init_moment.events, & &1.seq)
    end

    test "an event append failure discards the whole moment" do
      rtg = published_runtime!()
      opts = opts()
      backend = start_backend()

      init_moment = propose_init!(rtg, %{"value" => "hello"}, opts)
      insert!(backend, init_moment)
      token = claim!(backend)

      {:ok, moment} = Loop.propose_advance(rtg, init_moment.run, opts)

      conflicting =
        Enum.map(moment.events, fn event ->
          %{event | seq: 1, payload: %{"conflict" => true}}
        end)

      result =
        MemoryBackend.transaction(backend, fn tx ->
          proposal = %{
            run: moment.run,
            expected_checkpoint_seq: init_moment.run.checkpoint_seq,
            claim_token: token,
            checkpoint_type: moment.checkpoint_type,
            schedule: schedule_for(moment.disposition)
          }

          with {:ok, run} <- MemoryBackend.commit(tx, :tenantless, proposal) do
            case MemoryBackend.append_events(tx, :tenantless, run.id, conflicting) do
              :ok -> {:ok, run}
              {:error, reason} -> {:error, reason}
            end
          end
        end)

      assert {:error, :event_conflict} = result

      # The run commit inside the failed transaction never became durable.
      assert {:ok, run} = MemoryBackend.fetch_run(backend, :tenantless, "run_fixed")
      assert run == init_moment.run
    end
  end
end
