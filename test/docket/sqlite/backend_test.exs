if Code.ensure_loaded?(Exqlite.Sqlite3) do
  defmodule Docket.SQLite.BackendTest do
    # Hosts are named modules, so tests that start the same host run serially.
    use ExUnit.Case, async: false

    alias Docket.{Graph, Reducer, Schema}
    alias Docket.SQLite.Store
    alias Docket.Test.Fixtures.{Graphs, Nodes}

    defmodule Host do
      use Docket,
        backend: {Docket.SQLite, database: ":memory:", runner: [poll_interval_ms: 60_000]}
    end

    defmodule InlineHost do
      use Docket, testing: :inline, backend: {Docket.SQLite, database: ":memory:"}
    end

    defmodule ManualHost do
      use Docket, testing: :manual, backend: {Docket.SQLite, database: ":memory:"}
    end

    setup do
      dir = Path.join(System.tmp_dir!(), "docket-sqlite-#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)
      {:ok, database: Path.join(dir, "docket.db")}
    end

    defp start_host(host, database, extra \\ []) do
      defaults = if host == Host, do: [runner: [poll_interval_ms: 60_000]], else: []

      backend =
        {Docket.SQLite, defaults |> Keyword.merge(extra) |> Keyword.put(:database, database)}

      start_supervised!(Supervisor.child_spec({host, backend: backend}, id: {host, database}))
    end

    test "the runner advances a run to completion in the background", %{database: db} do
      start_host(Host, db)
      {:ok, ref} = Host.save_graph(Graphs.minimal_linear())
      {:ok, run} = Host.start_run(ref, %{"value" => "hello"})

      assert {:ok, %Docket.Run{status: :done, output: %{"result" => "hello"}}} =
               Host.await_run(run.id, timeout: 2_000)
    end

    test "an interrupt parks the run until resolved", %{database: db} do
      start_host(Host, db)
      {:ok, ref} = Host.save_graph(Graphs.interrupt_review())
      {:ok, run} = Host.start_run(ref, %{})

      assert {:ok, %Docket.Run{status: :waiting} = waiting} =
               Host.await_run(run.id, timeout: 2_000)

      [{interrupt_id, _}] = Map.to_list(waiting.interrupts)
      assert {:ok, _} = Host.resolve_interrupt(run.id, interrupt_id, "approved")
      assert {:ok, %Docket.Run{status: :done}} = Host.await_run(run.id, timeout: 2_000)
      assert {:ok, %Docket.EventPage{events: events}} = Host.list_events(run.id)
      assert :interrupt_resolved in Enum.map(events, & &1.type)
    end

    test "many concurrent runs all complete", %{database: db} do
      start_host(Host, db, runner: [concurrency: 8, poll_interval_ms: 60_000])
      {:ok, ref} = Host.save_graph(Graphs.minimal_linear())

      ids =
        for i <- 1..200 do
          {:ok, run} = Host.start_run(ref, %{"value" => "v#{i}"})
          run.id
        end

      for id <- ids do
        assert {:ok, %Docket.Run{status: :done}} = Host.await_run(id, timeout: 5_000)
      end

      assert {:ok, %Docket.RunPage{runs: runs}} = Host.list_runs(limit: 500, status: :done)
      assert length(runs) == 200
    end

    test "a retry backoff wakes the run from the runner's timer, not the poll", %{database: db} do
      # The poll interval is a minute, so only the next-wake timer can resume
      # the run after its 150ms retry backoff.
      start_host(Host, db)

      graph =
        Graph.new!(id: "backoff")
        |> Graph.put_field!("out", schema: Schema.string(), reducer: Reducer.last_value())
        |> Graph.put_node!("flaky",
          implementation: Nodes.FlakyThenSucceeds,
          config: %{failures: 1.0, field: "out", value: "done"},
          policies: %{"retry" => %{"max_attempts" => 2, "backoff_ms" => 150}}
        )
        |> Graph.put_edge!("start_flaky", from: "$start", to: "flaky")
        |> Graph.put_edge!("flaky_finish", from: "flaky", to: "$finish")
        |> Graph.put_output!("out", [])

      {:ok, ref} = Host.save_graph(graph)
      started = System.monotonic_time(:millisecond)
      {:ok, run} = Host.start_run(ref, %{})
      assert {:ok, %Docket.Run{status: :done}} = Host.await_run(run.id, timeout: 3_000)
      assert System.monotonic_time(:millisecond) - started >= 150
    end

    test "inline testing mode drains in the caller", %{database: db} do
      start_host(InlineHost, db)
      backend_name = Module.concat(InlineHost, Backend)
      refute Process.whereis(Docket.SQLite.runner_name(backend_name))

      {:ok, ref} = InlineHost.save_graph(Graphs.interrupt_review())
      assert {:ok, %Docket.Run{status: :waiting} = waiting} = InlineHost.start_run(ref, %{})
      [{interrupt_id, _}] = Map.to_list(waiting.interrupts)

      assert {:ok, %Docket.Run{status: :done}} =
               InlineHost.resolve_interrupt(waiting.id, interrupt_id, "approved")
    end

    test "a claim left by a stopped process is released and finished on restart",
         %{database: db} do
      # Claim the run in manual mode and stop without committing, as if the
      # node crashed mid-vehicle.
      start_host(ManualHost, db)
      {:ok, ref} = ManualHost.save_graph(Graphs.minimal_linear())
      {:ok, run} = ManualHost.start_run(ref, %{"value" => "survived"})

      context = Docket.SQLite.context(name: Module.concat(ManualHost, Backend))

      {:ok, %{leases: [%{run_id: run_id}]}} =
        Store.claim_due(context, :system, %{
          now: DateTime.utc_now(),
          limit: 1,
          orphan_ttl_ms: 60_000,
          max_claim_attempts: 5
        })

      assert run_id == run.id
      stop_supervised!({ManualHost, db})

      # The orphan TTL is a minute; the restart must not wait for it.
      start_host(Host, db)

      assert {:ok, %Docket.Run{status: :done, output: %{"result" => "survived"}}} =
               Host.await_run(run.id, timeout: 2_000)

      assert {:ok, %Docket.RunInfo{claim_attempts: 0}} = Host.inspect_run(run.id)
    end

    test "state survives a restart on the same file", %{database: db} do
      start_host(Host, db)
      {:ok, ref} = Host.save_graph(Graphs.interrupt_review())
      {:ok, run} = Host.start_run(ref, %{})

      assert {:ok, %Docket.Run{status: :waiting} = waiting} =
               Host.await_run(run.id, timeout: 2_000)

      stop_supervised!({Host, db})

      start_host(Host, db)
      [{interrupt_id, _}] = Map.to_list(waiting.interrupts)
      assert {:ok, _} = Host.resolve_interrupt(run.id, interrupt_id, "later")
      assert {:ok, %Docket.Run{status: :done}} = Host.await_run(run.id, timeout: 2_000)
      assert {:ok, %Docket.GraphRef{}} = Host.fetch_latest_graph_ref("interrupt-review")
    end

    test "rejects a missing database and a production clock" do
      assert_raise ArgumentError, ~r/:database/, fn ->
        Docket.SQLite.init({[name: :x], %{conn: :c, runner: :r, clock: &DateTime.utc_now/0}})
      end

      assert_raise ArgumentError, ~r/testing-only/, fn ->
        Docket.SQLite.init(
          {[name: :x, database: ":memory:", clock: &DateTime.utc_now/0],
           %{conn: :c, runner: :r, clock: &DateTime.utc_now/0}}
        )
      end
    end
  end
end
