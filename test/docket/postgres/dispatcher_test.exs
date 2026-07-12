if Code.ensure_loaded?(Ecto.Adapters.SQL) and Code.ensure_loaded?(Postgrex) do
  defmodule Docket.Postgres.DispatcherTest do
    use ExUnit.Case, async: false

    alias Docket.Postgres.Dispatcher

    @now ~U[2026-07-11 12:00:00.000000Z]

    defmodule RunStore do
      def claim_due(agent, :system, policy) do
        action =
          Agent.get_and_update(agent, fn state ->
            {action, rest} =
              case state.claims do
                [action | rest] -> {action, rest}
                [] -> {{:ok, %{leases: [], poisoned: []}}, []}
              end

            {action, %{state | claims: rest, policies: state.policies ++ [policy]}}
          end)

        case action do
          fun when is_function(fun, 1) -> fun.(policy)
          result -> result
        end
      end

      def release_claim(agent, :system, run_id, token, now) do
        Agent.update(agent, fn state ->
          %{state | releases: state.releases ++ [{run_id, token, now}]}
        end)

        :ok
      end
    end

    setup do
      {:ok, agent} =
        start_supervised(
          {Agent,
           fn ->
             %{claims: [], policies: [], releases: []}
           end}
        )

      %{agent: agent}
    end

    test "demand is concurrency minus monitored vehicles", %{agent: agent} do
      set_claims(agent, [batch([lease("one"), lease("two")]), batch([lease("three")])])
      parent = self()

      dispatcher =
        start_dispatcher!(agent,
          concurrency: 2,
          launch: fn lease ->
            pid = spawn(fn -> receive(do: (:stop -> :ok)) end)
            send(parent, {:launched, lease.run_id, pid})
            {:ok, pid}
          end
        )

      assert_receive {:launched, "one", one}
      assert_receive {:launched, "two", _two}
      assert [%{limit: 2}] = policies(agent)

      Dispatcher.request_poll(dispatcher)
      refute_receive {:launched, "three", _}, 50
      assert [%{limit: 2}] = policies(agent)

      send(one, :stop)
      assert_receive {:launched, "three", _three}
      assert Enum.map(policies(agent), & &1.limit) == [2, 1]
    end

    test "an immediate burst becomes one in-flight poll plus one pending poll", %{agent: agent} do
      parent = self()

      blocker = fn policy ->
        send(parent, {:poll_started, self(), policy})
        receive do: (:continue -> batch())
      end

      set_claims(agent, [blocker, blocker, batch()])
      dispatcher = start_dispatcher!(agent)

      assert_receive {:poll_started, first, %{limit: 1}}
      for _ <- 1..200, do: Dispatcher.request_poll(dispatcher)
      send(first, :continue)

      assert_receive {:poll_started, second, %{limit: 1}}
      refute_receive {:poll_started, _, _}, 50
      send(second, :continue)

      Process.sleep(20)
      assert length(policies(agent)) == 2
    end

    test "telemetry exposes active, pending, completed, and worker-failure poll transitions", %{
      agent: agent
    } do
      parent = self()
      handler = "dispatcher-state-#{System.unique_integer([:positive])}"

      events = [
        [:docket, :postgres, :dispatcher, :state],
        [:docket, :postgres, :dispatcher, :poll]
      ]

      :telemetry.attach_many(
        handler,
        events,
        fn name, measurements, metadata, _ -> send(parent, {name, measurements, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      blocker = fn _policy ->
        send(parent, {:blocked_poll, self()})
        receive do: (:continue -> batch())
      end

      set_claims(agent, [blocker, fn _ -> raise "database worker failed" end, batch()])
      dispatcher = start_dispatcher!(agent)

      assert_receive {:blocked_poll, poll}

      assert_receive {[:docket, :postgres, :dispatcher, :state],
                      %{poll_active: 1, poll_pending: 0, demand: 1, in_flight: 0}, %{}}

      Dispatcher.request_poll(dispatcher)

      assert_receive {[:docket, :postgres, :dispatcher, :state],
                      %{poll_active: 1, poll_pending: 1, demand: 1, in_flight: 0}, %{}}

      send(poll, :continue)

      assert_receive {[:docket, :postgres, :dispatcher, :poll], %{demand: 1},
                      %{result: :ok, source: :initial}}

      assert_receive {[:docket, :postgres, :dispatcher, :poll], %{demand: 1},
                      %{result: :error, source: :notification}},
                     1_000

      assert Process.alive?(dispatcher)
      assert eventually(fn -> length(policies(agent)) >= 2 end)
    end

    test "launches leases, observes poison without mutation, and releases failed launches", %{
      agent: agent
    } do
      parent = self()
      poisoned = [%{run_id: "poisoned", poisoned_at: @now, poison_reason: "exhausted"}]

      set_claims(agent, [batch([lease("failed")], poisoned), batch()])

      _dispatcher =
        start_dispatcher!(agent,
          launch: fn lease ->
            send(parent, {:launch_attempt, lease})
            {:error, :supervisor_down}
          end,
          on_poisoned: &send(parent, {:poisoned, &1})
        )

      assert_receive {:launch_attempt, %{run_id: "failed"}}
      assert_receive {:poisoned, ^poisoned}

      assert eventually(fn ->
               Agent.get(agent, & &1.releases) == [{"failed", "token-failed", @now}]
             end)

      assert eventually(fn -> length(policies(agent)) == 2 end)
    end

    test "shutdown stops demand and bounds draining without releasing an active vehicle", %{
      agent: agent
    } do
      set_claims(agent, [batch([lease("long")])])
      parent = self()

      dispatcher =
        start_dispatcher!(agent,
          drain_timeout_ms: 25,
          launch: fn _lease ->
            pid = spawn(fn -> receive(do: (:stop -> :ok)) end)
            send(parent, {:vehicle, pid})
            {:ok, pid}
          end
        )

      assert_receive {:vehicle, vehicle}
      started = System.monotonic_time(:millisecond)
      :ok = GenServer.stop(dispatcher, :normal, 500)
      elapsed = System.monotonic_time(:millisecond) - started

      assert elapsed >= 20
      assert elapsed < 250
      assert Process.alive?(vehicle)
      assert Agent.get(agent, & &1.releases) == []
      send(vehicle, :stop)
    end

    test "shutdown releases leases returned by an in-flight claim instead of launching them", %{
      agent: agent
    } do
      parent = self()

      set_claims(agent, [
        fn _policy ->
          send(parent, {:claim_committed, self()})

          receive do
            {:return_after_stop_queued, dispatcher} ->
              wait_for_message(dispatcher)
              batch([lease("shutdown")])
          end
        end
      ])

      dispatcher =
        start_dispatcher!(agent,
          drain_timeout_ms: 200,
          launch: fn lease ->
            send(parent, {:unexpected_launch, lease})
            {:error, :unexpected}
          end
        )

      assert_receive {:claim_committed, poll}
      stopper = Task.async(fn -> GenServer.stop(dispatcher, :normal, 500) end)
      send(poll, {:return_after_stop_queued, dispatcher})

      assert :ok = Task.await(stopper)
      refute_receive {:unexpected_launch, _}
      assert Agent.get(agent, & &1.releases) == [{"shutdown", "token-shutdown", @now}]
    end

    test "supervisor shutdown runs the bounded vehicle drain", %{agent: agent} do
      set_claims(agent, [batch([lease("supervised")])])
      parent = self()

      opts =
        dispatcher_opts(agent,
          drain_timeout_ms: 25,
          launch: fn _lease ->
            pid = spawn(fn -> receive(do: (:stop -> :ok)) end)
            send(parent, {:supervised_vehicle, pid})
            {:ok, pid}
          end
        )

      {:ok, supervisor} = Supervisor.start_link([{Dispatcher, opts}], strategy: :one_for_one)
      Process.unlink(supervisor)
      assert_receive {:supervised_vehicle, vehicle}

      started = System.monotonic_time(:millisecond)
      assert :ok = Supervisor.stop(supervisor, :shutdown, 500)
      elapsed = System.monotonic_time(:millisecond) - started

      assert elapsed >= 20
      assert elapsed < 250
      assert Process.alive?(vehicle)
      send(vehicle, :stop)
    end

    test "linked vehicle exits are accounted through their monitor without crashing", %{
      agent: agent
    } do
      set_claims(agent, [batch([lease("linked")]), batch()])
      parent = self()

      dispatcher =
        start_dispatcher!(agent,
          launch: fn _lease ->
            pid = spawn_link(fn -> receive(do: (:stop -> :ok)) end)
            send(parent, {:linked_vehicle, pid})
            {:ok, pid}
          end
        )

      assert_receive {:linked_vehicle, vehicle}
      send(vehicle, :stop)

      assert eventually(fn -> length(policies(agent)) == 2 end)
      assert Process.alive?(dispatcher)
    end

    test "consecutive successful demand-1 polls alternate the preferred class", %{agent: agent} do
      set_claims(agent, [batch(), batch(), batch()])
      dispatcher = start_dispatcher!(agent)

      assert eventually(fn -> length(policies(agent)) == 1 end)
      Dispatcher.request_poll(dispatcher)
      assert eventually(fn -> length(policies(agent)) == 2 end)
      Dispatcher.request_poll(dispatcher)
      assert eventually(fn -> length(policies(agent)) == 3 end)

      assert Enum.map(policies(agent), & &1.preference) == [:ready, :expired, :ready]
      assert Enum.map(policies(agent), & &1.limit) == [1, 1, 1]
    end

    test "an errored poll does not flip the demand-1 preference", %{agent: agent} do
      set_claims(agent, [{:error, :db_down}, batch()])
      dispatcher = start_dispatcher!(agent)

      assert eventually(fn -> length(policies(agent)) == 1 end)
      Dispatcher.request_poll(dispatcher)
      assert eventually(fn -> length(policies(agent)) == 2 end)

      assert Enum.map(policies(agent), & &1.preference) == [:ready, :ready]
    end

    test "polls above demand 1 carry the preference but never flip it", %{agent: agent} do
      set_claims(agent, [batch(), batch()])
      dispatcher = start_dispatcher!(agent, concurrency: 2)

      assert eventually(fn -> length(policies(agent)) == 1 end)
      Dispatcher.request_poll(dispatcher)
      assert eventually(fn -> length(policies(agent)) == 2 end)

      assert Enum.map(policies(agent), & &1.preference) == [:ready, :ready]
      assert Enum.map(policies(agent), & &1.limit) == [2, 2]
    end

    defp start_dispatcher!(agent, overrides \\ []) do
      start_supervised!({Dispatcher, dispatcher_opts(agent, overrides)})
    end

    defp dispatcher_opts(agent, overrides) do
      Keyword.merge(
        [
          context: agent,
          run_store: RunStore,
          concurrency: 1,
          poll_interval_ms: 60_000,
          orphan_ttl_ms: 1_000,
          max_claim_attempts: 3,
          drain_timeout_ms: 0,
          launch: fn _lease -> {:error, :not_configured} end,
          clock: fn -> @now end,
          jitter: fn interval -> interval end
        ],
        overrides
      )
    end

    defp set_claims(agent, claims), do: Agent.update(agent, &%{&1 | claims: claims})
    defp policies(agent), do: Agent.get(agent, & &1.policies)

    defp batch(leases \\ [], poisoned \\ []),
      do: {:ok, %{leases: leases, poisoned: poisoned}}

    defp lease(id) do
      %{
        run_id: id,
        graph_id: "graph",
        graph_hash: "hash",
        checkpoint_seq: 1,
        claim_token: "token-#{id}",
        claimed_at: @now,
        claim_attempt: 1
      }
    end

    defp eventually(fun, attempts \\ 50)

    defp eventually(fun, attempts) when attempts > 0 do
      if fun.() do
        true
      else
        Process.sleep(5)
        eventually(fun, attempts - 1)
      end
    end

    defp eventually(_fun, 0), do: false

    defp wait_for_message(pid) do
      queued? =
        match?(
          {:message_queue_len, length} when length > 0,
          Process.info(pid, :message_queue_len)
        )

      stopping? =
        case Process.info(pid, :current_stacktrace) do
          {:current_stacktrace, stacktrace} ->
            Enum.any?(stacktrace, fn
              {Docket.Postgres.Dispatcher, :settle_poll_for_shutdown, _, _} -> true
              _ -> false
            end)

          _ ->
            false
        end

      if queued? or stopping? do
        :ok
      else
        Process.sleep(1)
        wait_for_message(pid)
      end
    end
  end
end
