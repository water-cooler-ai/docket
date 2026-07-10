defmodule Docket.Runtime.MomentTest do
  use Docket.Test.Case, async: true

  alias Docket.Checkpoint
  alias Docket.Runtime.{Loop, Moment}
  alias Docket.Test.MemoryBackend

  defmodule NeverCalled do
    @behaviour Docket.Checkpoint

    def handle(_checkpoint, _context) do
      raise "checkpoint handler invoked during moment calculation"
    end
  end

  defmodule RaisingObserver do
    @behaviour Docket.Checkpoint

    def handle(_checkpoint, _context), do: raise("observer exploded")
  end

  @now DateTime.from_naive!(~N[2026-07-10 12:00:00], "Etc/UTC")

  defp opts(overrides \\ []) do
    Keyword.merge(
      [
        checkpoint: NeverCalled,
        clock: fn -> @now end,
        id_generator: fn kind -> "#{kind}_fixed" end,
        run_id: "run_fixed"
      ],
      overrides
    )
  end

  defp watch_telemetry(run_id) do
    handler_id = {__MODULE__, run_id, self()}
    parent = self()

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
      fn name, _measurements, metadata, _config ->
        if metadata.run_id == run_id, do: send(parent, {:telemetry, name})
      end,
      nil
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

      accepted_opts = opts(checkpoint: Docket.Test.Checkpoint.Accept)
      run = Loop.build_initial_run(rtg, %{"value" => "hello"}, accepted_opts)

      {:ok, committed, [{:checkpoint, checkpoint, _context, :accepted}]} =
        Loop.init(rtg, run, accepted_opts)

      assert moment.run == committed
      assert moment.events == checkpoint.events
      assert moment.checkpoint_type == checkpoint.type
    end
  end

  describe "propose_advance/3" do
    test "drives a multi-step graph one commit boundary at a time to inline parity" do
      graph = Graphs.cycle_counter()
      rtg = compile!(graph)

      {:ok, inline_run, inline_checkpoints} =
        Docket.Test.run_inline(rtg, %{}, opts(checkpoint: Docket.Test.Checkpoint.Accept))

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

      moments = drain(rtg, retry_moment.run, opts)

      assert Enum.map(moments, & &1.checkpoint_type) ==
               [:retry_scheduled, :step_committed, :run_completed]

      assert List.last(moments).run.status == :done
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
      backend = start_supervised!({MemoryBackend, clock: fn -> @now end})
      backend
    end

    defp insert!(backend, moment) do
      {:ok, _run} =
        MemoryBackend.transaction(backend, fn tx ->
          with {:ok, run} <-
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

    test "a vehicle drives propose -> commit -> continue until the terminal park" do
      rtg = compile!(Graphs.minimal_linear())
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
            Moment.checkpoint(moment, Checkpoint.delivery(moment.checkpoint_type))

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
      rtg = compile!(Graphs.minimal_linear())
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
      rtg = compile!(Graphs.minimal_linear())
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

    test "post-commit observer failure cannot change durable state" do
      rtg = compile!(Graphs.minimal_linear())
      opts = opts()
      backend = start_backend()

      init_moment = propose_init!(rtg, %{"value" => "hello"}, opts)
      insert!(backend, init_moment)
      token = claim!(backend)

      {:ok, moment} = Loop.propose_advance(rtg, init_moment.run, opts)
      {:ok, committed} = commit_moment(backend, moment, init_moment.run.checkpoint_seq, token)

      checkpoint = Moment.checkpoint(moment, Checkpoint.delivery(moment.checkpoint_type))
      context = Moment.context(moment, %{})

      assert {:error, {:raised, _exception}} =
               Loop.deliver_checkpoint(RaisingObserver, checkpoint, context)

      assert {:ok, ^committed} = MemoryBackend.fetch_run(backend, :tenantless, "run_fixed")
    end
  end
end
