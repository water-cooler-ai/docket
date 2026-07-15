defmodule Docket.BackendTests.Cases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote location: :keep do
      alias Docket.BackendTests.{Contract, Fixture}

      @tag docket_invariant: "CONTRACT-CALLBACK-COMPLETENESS"
      test "[CONTRACT-CALLBACK-COMPLETENESS] bundle exports every required callback",
           %{backend_test: instance} do
        assert Contract.violations(instance.backend) == [],
               "backend contract violations:\n" <>
                 Enum.map_join(Contract.violations(instance.backend), "\n", &"  * #{&1}")
      end

      @tag docket_invariant: "TX-COMMIT-ROLLBACK-PROPAGATION"
      test "[TX-COMMIT-ROLLBACK-PROPAGATION] commit, error, exception, throw, and invalid return",
           %{backend_test: instance} do
        backend = instance.backend
        graphs = backend.graphs()

        {committed, committed_hash} = Fixture.graph(instance, "tx-committed")

        assert {:ok, {:value, 42}} =
                 backend.transaction(instance.context, fn tx ->
                   assert :ok =
                            graphs.save_graph(
                              tx,
                              :tenantless,
                              committed.id,
                              committed_hash,
                              committed
                            )

                   {:ok, {:value, 42}}
                 end)

        assert {:ok, ^committed} =
                 graphs.fetch_graph(
                   instance.context,
                   :tenantless,
                   committed.id,
                   committed_hash
                 )

        for {kind, suffix} <- [
              error: "error",
              exception: "exception",
              throw: "throw",
              invalid: "invalid"
            ] do
          {marker, marker_hash} = Fixture.graph(instance, "tx-#{suffix}")

          callback = fn tx ->
            assert :ok =
                     graphs.save_graph(
                       tx,
                       :tenantless,
                       marker.id,
                       marker_hash,
                       marker
                     )

            case kind do
              :error -> {:error, {:stop, suffix}}
              :exception -> raise RuntimeError, "tx-#{suffix}"
              :throw -> throw({:thrown, suffix})
              :invalid -> {:not, :a_transaction_result}
            end
          end

          case kind do
            :error ->
              assert {:error, {:stop, ^suffix}} = backend.transaction(instance.context, callback)

            :exception ->
              assert_raise RuntimeError, "tx-#{suffix}", fn ->
                backend.transaction(instance.context, callback)
              end

            :throw ->
              assert catch_throw(backend.transaction(instance.context, callback)) ==
                       {:thrown, suffix}

            :invalid ->
              assert_raise ArgumentError, ~r/must return/, fn ->
                backend.transaction(instance.context, callback)
              end
          end

          assert {:error, :not_found} =
                   graphs.fetch_graph(
                     instance.context,
                     :tenantless,
                     marker.id,
                     marker_hash
                   )
        end
      end

      @tag docket_invariant: "TX-NESTING-ROLLBACK-ONLY"
      test "[TX-NESTING-ROLLBACK-ONLY] nested work joins and swallowed failures abort outer publication",
           %{backend_test: instance} do
        backend = instance.backend
        graphs = backend.graphs()
        {outer, outer_hash} = Fixture.graph(instance, "nested-success-outer")
        {inner, inner_hash} = Fixture.graph(instance, "nested-success-inner")

        assert {:ok, :outer} =
                 backend.transaction(instance.context, fn outer_tx ->
                   assert :ok =
                            graphs.save_graph(
                              outer_tx,
                              :tenantless,
                              outer.id,
                              outer_hash,
                              outer
                            )

                   assert {:ok, :inner} =
                            backend.transaction(outer_tx, fn inner_tx ->
                              assert :ok =
                                       graphs.save_graph(
                                         inner_tx,
                                         :tenantless,
                                         inner.id,
                                         inner_hash,
                                         inner
                                       )

                              {:ok, :inner}
                            end)

                   {:ok, :outer}
                 end)

        assert {:ok, ^outer} =
                 graphs.fetch_graph(instance.context, :tenantless, outer.id, outer_hash)

        assert {:ok, ^inner} =
                 graphs.fetch_graph(instance.context, :tenantless, inner.id, inner_hash)

        {outer_failure, outer_failure_hash} =
          Fixture.graph(instance, "nested-outer-failure-outer")

        {inner_success, inner_success_hash} =
          Fixture.graph(instance, "nested-outer-failure-inner")

        assert {:error, :outer_stop} =
                 backend.transaction(instance.context, fn outer_tx ->
                   assert :ok =
                            graphs.save_graph(
                              outer_tx,
                              :tenantless,
                              outer_failure.id,
                              outer_failure_hash,
                              outer_failure
                            )

                   assert {:ok, :inner_committed} =
                            backend.transaction(outer_tx, fn inner_tx ->
                              assert :ok =
                                       graphs.save_graph(
                                         inner_tx,
                                         :tenantless,
                                         inner_success.id,
                                         inner_success_hash,
                                         inner_success
                                       )

                              {:ok, :inner_committed}
                            end)

                   {:error, :outer_stop}
                 end)

        assert {:error, :not_found} =
                 graphs.fetch_graph(
                   instance.context,
                   :tenantless,
                   outer_failure.id,
                   outer_failure_hash
                 )

        assert {:error, :not_found} =
                 graphs.fetch_graph(
                   instance.context,
                   :tenantless,
                   inner_success.id,
                   inner_success_hash
                 )

        for kind <- [:error, :invalid, :exception, :throw] do
          {outer_marker, outer_marker_hash} = Fixture.graph(instance, "nested-#{kind}-outer")
          {inner_marker, inner_marker_hash} = Fixture.graph(instance, "nested-#{kind}-inner")

          assert {:error, :rollback} =
                   backend.transaction(instance.context, fn outer_tx ->
                     assert :ok =
                              graphs.save_graph(
                                outer_tx,
                                :tenantless,
                                outer_marker.id,
                                outer_marker_hash,
                                outer_marker
                              )

                     nested = fn ->
                       backend.transaction(outer_tx, fn inner_tx ->
                         assert :ok =
                                  graphs.save_graph(
                                    inner_tx,
                                    :tenantless,
                                    inner_marker.id,
                                    inner_marker_hash,
                                    inner_marker
                                  )

                         case kind do
                           :error -> {:error, :inner_stop}
                           :invalid -> :invalid
                           :exception -> raise "inner boom"
                           :throw -> throw(:inner_boom)
                         end
                       end)
                     end

                     case kind do
                       :error -> assert {:error, :inner_stop} = nested.()
                       :invalid -> assert_raise ArgumentError, nested
                       :exception -> assert_raise RuntimeError, "inner boom", nested
                       :throw -> assert catch_throw(nested.()) == :inner_boom
                     end

                     {:ok, :attempted_swallow}
                   end)

          assert {:error, :not_found} =
                   graphs.fetch_graph(
                     instance.context,
                     :tenantless,
                     outer_marker.id,
                     outer_marker_hash
                   )

          assert {:error, :not_found} =
                   graphs.fetch_graph(
                     instance.context,
                     :tenantless,
                     inner_marker.id,
                     inner_marker_hash
                   )
        end
      end

      @tag docket_invariant: "TX-CONCURRENT-PUBLICATION"
      test "[TX-CONCURRENT-PUBLICATION] overlapping commits and rollback publication cannot erase winners",
           %{backend_test: instance} do
        backend = instance.backend
        graphs = backend.graphs()
        parent = self()

        {first_graph, first_hash} = Fixture.graph(instance, "overlap-first")
        {second_graph, second_hash} = Fixture.graph(instance, "overlap-second")

        first =
          Task.async(fn ->
            backend.transaction(instance.context, fn tx ->
              send(parent, :first_transaction_entered)
              receive do: (:release_first -> :ok)
              :ok = graphs.save_graph(tx, :tenantless, first_graph.id, first_hash, first_graph)
              {:ok, :first}
            end)
          end)

        assert_receive :first_transaction_entered, 5_000

        second =
          Task.async(fn ->
            send(parent, :second_transaction_attempting)

            backend.transaction(instance.context, fn tx ->
              send(parent, :second_transaction_entered)
              :ok = graphs.save_graph(tx, :tenantless, second_graph.id, second_hash, second_graph)
              {:ok, :second}
            end)
          end)

        assert_receive :second_transaction_attempting, 5_000
        second_result = Task.yield(second, 100)
        send(first.pid, :release_first)
        assert {:ok, :first} = Task.await(first, 5_000)

        assert {:ok, :second} =
                 Docket.BackendTests.await_task(second, second_result)

        assert {:ok, ^first_graph} =
                 graphs.fetch_graph(instance.context, :tenantless, first_graph.id, first_hash)

        assert {:ok, ^second_graph} =
                 graphs.fetch_graph(instance.context, :tenantless, second_graph.id, second_hash)

        {rolled_back, rolled_back_hash} = Fixture.graph(instance, "overlap-rollback")
        {winner, winner_hash} = Fixture.graph(instance, "overlap-winner")

        rollback =
          Task.async(fn ->
            backend.transaction(instance.context, fn tx ->
              send(parent, :rollback_transaction_entered)
              receive do: (:release_rollback -> :ok)

              :ok =
                graphs.save_graph(tx, :tenantless, rolled_back.id, rolled_back_hash, rolled_back)

              {:error, :rolled_back}
            end)
          end)

        assert_receive :rollback_transaction_entered, 5_000

        commit =
          Task.async(fn ->
            send(parent, :winner_transaction_attempting)

            backend.transaction(instance.context, fn tx ->
              send(parent, :winner_transaction_entered)
              :ok = graphs.save_graph(tx, :tenantless, winner.id, winner_hash, winner)
              {:ok, :winner}
            end)
          end)

        assert_receive :winner_transaction_attempting, 5_000
        commit_result = Task.yield(commit, 100)
        send(rollback.pid, :release_rollback)
        assert {:error, :rolled_back} = Task.await(rollback, 5_000)

        assert {:ok, :winner} =
                 Docket.BackendTests.await_task(commit, commit_result)

        assert {:error, :not_found} =
                 graphs.fetch_graph(
                   instance.context,
                   :tenantless,
                   rolled_back.id,
                   rolled_back_hash
                 )

        assert {:ok, ^winner} =
                 graphs.fetch_graph(instance.context, :tenantless, winner.id, winner_hash)
      end

      @tag docket_invariant: "BUNDLE-CROSS-STORE-ATOMICITY"
      test "[BUNDLE-CROSS-STORE-ATOMICITY] one yielded context commits and rolls back graph, run, and events",
           %{backend_test: instance} do
        backend = instance.backend
        graphs = backend.graphs()
        runs = backend.runs()
        events = backend.events()
        {graph, graph_hash} = Fixture.graph(instance, "bundle-commit")
        run = Fixture.run(instance, "bundle-commit-run", graph, graph_hash, event_seq: 1)
        event = Fixture.event(run, 1, instance.now)

        assert {:ok, ^run} =
                 backend.transaction(instance.context, fn tx ->
                   with :ok <- graphs.save_graph(tx, :tenantless, graph.id, graph_hash, graph),
                        {:ok, initialized} <-
                          runs.insert_run(
                            tx,
                            :tenantless,
                            run,
                            :run_initialized,
                            instance.now
                          ),
                        :ok <- events.append_events(tx, :tenantless, run.id, [event]) do
                     assert {:ok, ^graph} =
                              graphs.fetch_graph(tx, :tenantless, graph.id, graph_hash)

                     assert {:ok, ^run} = runs.fetch_run(tx, :tenantless, run.id)
                     assert {:ok, ^event} = events.fetch_event(tx, :tenantless, run.id, 1)
                     {:ok, initialized}
                   end
                 end)

        assert {:ok, ^graph} =
                 graphs.fetch_graph(
                   instance.context,
                   :tenantless,
                   graph.id,
                   graph_hash
                 )

        assert {:ok, ^run} = runs.fetch_run(instance.context, :tenantless, run.id)
        assert {:ok, ^event} = events.fetch_event(instance.context, :tenantless, run.id, 1)

        {rollback_graph, rollback_hash} = Fixture.graph(instance, "bundle-rollback")

        rollback_run =
          Fixture.run(
            instance,
            "bundle-rollback-run",
            rollback_graph,
            rollback_hash,
            event_seq: 1
          )

        partial = Fixture.event(rollback_run, 1, instance.now)
        mismatched = Fixture.event(Fixture.id(instance, "other-run"), 2, instance.now)

        assert {:error, :event_run_mismatch} =
                 backend.transaction(instance.context, fn tx ->
                   with :ok <-
                          graphs.save_graph(
                            tx,
                            :tenantless,
                            rollback_graph.id,
                            rollback_hash,
                            rollback_graph
                          ),
                        {:ok, _} <-
                          runs.insert_run(
                            tx,
                            :tenantless,
                            rollback_run,
                            :run_initialized,
                            instance.now
                          ),
                        :ok <- events.append_events(tx, :tenantless, rollback_run.id, [partial]),
                        :ok <-
                          events.append_events(tx, :tenantless, rollback_run.id, [mismatched]) do
                     {:ok, :impossible}
                   end
                 end)

        assert {:error, :not_found} =
                 graphs.fetch_graph(
                   instance.context,
                   :tenantless,
                   rollback_graph.id,
                   rollback_hash
                 )

        assert {:error, :not_found} =
                 runs.fetch_run(instance.context, :tenantless, rollback_run.id)

        assert {:error, :not_found} =
                 events.fetch_event(instance.context, :tenantless, rollback_run.id, 1)
      end

      @tag docket_invariant: "TX-UNCOMMITTED-VISIBILITY"
      test "[TX-UNCOMMITTED-VISIBILITY] completed root reads cannot expose a partial uncommitted transition",
           %{backend_test: instance} do
        backend = instance.backend
        runs = backend.runs()
        events = backend.events()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "visibility")
        run = Fixture.run(instance, "visibility-run", graph, graph_hash, event_seq: 1)
        event = Fixture.event(run, 1, instance.now)
        parent = self()

        transaction =
          Task.async(fn ->
            backend.transaction(instance.context, fn tx ->
              {:ok, _} =
                runs.insert_run(tx, :tenantless, run, :run_initialized, instance.now)

              :ok = events.append_events(tx, :tenantless, run.id, [event])
              send(parent, :transition_written)
              receive do: (:commit_transition -> {:ok, :committed})
            end)
          end)

        assert_receive :transition_written, 5_000

        reader =
          Task.async(fn ->
            send(parent, :visibility_reader_started)

            {
              runs.fetch_run(instance.context, :tenantless, run.id),
              events.fetch_event(instance.context, :tenantless, run.id, 1)
            }
          end)

        assert_receive :visibility_reader_started, 5_000
        early_read = Task.yield(reader, 100)

        if early_read do
          assert {:ok, {{:error, :not_found}, {:error, :not_found}}} = early_read
        end

        send(transaction.pid, :commit_transition)
        assert {:ok, :committed} = Task.await(transaction, 5_000)

        read_result = Docket.BackendTests.await_task(reader, early_read)

        assert read_result in [
                 {{:error, :not_found}, {:error, :not_found}},
                 {{:ok, run}, {:ok, event}}
               ]

        assert {:ok, ^run} = runs.fetch_run(instance.context, :tenantless, run.id)
        assert {:ok, ^event} = events.fetch_event(instance.context, :tenantless, run.id, 1)
      end

      @tag docket_invariant: "SCOPE-OWNER-ISOLATION"
      test "[SCOPE-OWNER-ISOLATION] graph, run, and event access preserves explicit ownership",
           %{backend_test: instance} do
        backend = instance.backend
        graphs = backend.graphs()
        runs = backend.runs()
        events = backend.events()
        tenant_a = {:tenant, Fixture.id(instance, "tenant-a")}
        tenant_b = {:tenant, Fixture.id(instance, "tenant-b")}
        {graph, graph_hash} = Fixture.graph(instance, "owned-graph")

        assert :ok =
                 graphs.save_graph(
                   instance.context,
                   :tenantless,
                   graph.id,
                   graph_hash,
                   graph
                 )

        assert :ok =
                 graphs.save_graph(instance.context, tenant_a, graph.id, graph_hash, graph)

        assert {:ok, ^graph} =
                 graphs.fetch_graph(instance.context, :tenantless, graph.id, graph_hash)

        assert {:ok, ^graph} =
                 graphs.fetch_graph(instance.context, tenant_a, graph.id, graph_hash)

        assert {:error, :not_found} =
                 graphs.fetch_graph(instance.context, tenant_b, graph.id, graph_hash)

        assert_raise ArgumentError, fn ->
          graphs.fetch_graph(instance.context, :system, graph.id, graph_hash)
        end

        tenantless_run =
          Fixture.run(instance, "tenantless-run", graph, graph_hash, event_seq: 1)

        tenant_run = Fixture.run(instance, "tenant-run", graph, graph_hash, event_seq: 1)
        tenantless_event = Fixture.event(tenantless_run, 1, instance.now)
        tenant_event = Fixture.event(tenant_run, 1, instance.now)

        assert {:ok, ^tenantless_run} =
                 Fixture.initialize(instance, :tenantless, tenantless_run, [tenantless_event])

        assert {:ok, ^tenant_run} =
                 Fixture.initialize(instance, tenant_a, tenant_run, [tenant_event])

        assert {:ok, ^tenantless_run} =
                 runs.fetch_run(instance.context, :system, tenantless_run.id)

        assert {:ok, ^tenant_run} = runs.fetch_run(instance.context, :system, tenant_run.id)

        assert {:ok, ^tenant_run} = runs.fetch_run(instance.context, tenant_a, tenant_run.id)

        assert {:ok, %Docket.RunInfo{run: ^tenant_run}} =
                 runs.inspect_run(instance.context, tenant_a, tenant_run.id)

        assert {:ok, ^tenant_event} =
                 events.fetch_event(instance.context, tenant_a, tenant_run.id, 1)

        assert {:error, :not_found} =
                 runs.fetch_run(instance.context, tenant_a, tenantless_run.id)

        assert {:error, :not_found} =
                 runs.fetch_run(instance.context, tenant_b, tenant_run.id)

        assert {:error, :not_found} =
                 runs.inspect_run(instance.context, tenant_b, tenant_run.id)

        assert {:error, :not_found} =
                 events.fetch_event(instance.context, tenant_b, tenant_run.id, 1)

        assert {:ok, ^tenant_event} =
                 events.fetch_event(instance.context, :system, tenant_run.id, 1)

        assert_raise ArgumentError, fn ->
          runs.fetch_run(instance.context, nil, tenantless_run.id)
        end

        assert_raise ArgumentError, fn ->
          runs.inspect_run(instance.context, nil, tenantless_run.id)
        end

        assert_raise ArgumentError, fn ->
          events.fetch_event(instance.context, nil, tenantless_run.id, 1)
        end
      end

      @tag docket_invariant: "GRAPH-CONTENT-ADDRESS-AND-VERSIONS"
      test "[GRAPH-CONTENT-ADDRESS-AND-VERSIONS] publication is idempotent, addressed, and owner-scoped",
           %{backend_test: instance} do
        graphs = instance.backend.graphs()
        tenant = {:tenant, Fixture.id(instance, "graph-tenant")}
        {graph, graph_hash} = Fixture.graph(instance, "graph-version", %{"revision" => 1})

        {second_graph, second_hash} =
          Fixture.graph(instance, "graph-version", %{"revision" => 2})

        assert :ok =
                 graphs.save_graph(
                   instance.context,
                   :tenantless,
                   graph.id,
                   graph_hash,
                   graph
                 )

        assert :ok =
                 graphs.save_graph(
                   instance.context,
                   :tenantless,
                   second_graph.id,
                   second_hash,
                   second_graph
                 )

        assert :ok =
                 graphs.save_graph(
                   instance.context,
                   :tenantless,
                   graph.id,
                   graph_hash,
                   graph
                 )

        assert {:error, _reason} =
                 graphs.save_graph(
                   instance.context,
                   :tenantless,
                   graph.id,
                   String.duplicate("0", 64),
                   graph
                 )

        assert {:ok, %Docket.GraphVersionPage{versions: versions, has_more?: false}} =
                 graphs.list_graph_versions(instance.context, :tenantless, graph.id, %{
                   limit: 10,
                   before: nil
                 })

        assert MapSet.new(Enum.map(versions, & &1.ref.graph_hash)) ==
                 MapSet.new([graph_hash, second_hash])

        assert versions ==
                 Enum.sort_by(
                   versions,
                   fn version ->
                     {DateTime.to_unix(version.published_at, :microsecond),
                      version.ref.graph_hash}
                   end,
                   :desc
                 )

        assert {:ok, latest_ref} =
                 graphs.fetch_latest_graph_ref(instance.context, :tenantless, graph.id)

        assert latest_ref == hd(versions).ref

        assert {:ok, %Docket.GraphVersionPage{versions: [first_page], has_more?: true} = page} =
                 graphs.list_graph_versions(instance.context, :tenantless, graph.id, %{
                   limit: 1,
                   before: nil
                 })

        assert {:ok, %Docket.GraphVersionPage{versions: [second_page], has_more?: false}} =
                 graphs.list_graph_versions(instance.context, :tenantless, graph.id, %{
                   limit: 1,
                   before: page.next_before
                 })

        refute first_page.ref == second_page.ref

        assert :ok =
                 graphs.save_graph(
                   instance.context,
                   :tenantless,
                   graph.id,
                   graph_hash,
                   graph
                 )

        assert {:ok, %Docket.GraphVersionPage{versions: ^versions}} =
                 graphs.list_graph_versions(instance.context, :tenantless, graph.id, %{
                   limit: 10,
                   before: nil
                 })

        assert {:error, :not_found} =
                 graphs.fetch_latest_graph_ref(instance.context, tenant, graph.id)

        assert :ok =
                 graphs.save_graph(instance.context, tenant, graph.id, graph_hash, graph)

        assert {:ok, %Docket.GraphVersionPage{versions: [_]}} =
                 graphs.list_graph_versions(instance.context, tenant, graph.id, %{
                   limit: 10,
                   before: nil
                 })
      end

      @tag docket_invariant: "EVENT-IDEMPOTENCY-CURSOR-GAPS"
      test "[EVENT-IDEMPOTENCY-CURSOR-GAPS] assigned events preserve conflict, scope, ordering, and sparse bounds",
           %{backend_test: instance} do
        events = instance.backend.events()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "event-graph")
        run = Fixture.run(instance, "event-run", graph, graph_hash, event_seq: 4)
        first = Fixture.event(run, 1, instance.now, payload: %{"winner" => true})
        third = Fixture.event(run, 3, instance.now)
        fourth = Fixture.event(run, 4, instance.now)

        assert {:ok, ^run} =
                 Fixture.initialize(instance, :tenantless, run, [first, third, fourth])

        assert :ok = events.append_events(instance.context, :tenantless, run.id, [first])

        conflicting = %{first | payload: %{"winner" => false}}

        assert {:error, :event_conflict} =
                 events.append_events(instance.context, :tenantless, run.id, [conflicting])

        second = Fixture.event(run, 2, instance.now)

        assert {:error, :event_conflict} =
                 events.append_events(
                   instance.context,
                   :tenantless,
                   run.id,
                   [second, conflicting]
                 )

        assert {:error, :not_found} =
                 events.fetch_event(instance.context, :tenantless, run.id, 2)

        mismatch = Fixture.event(Fixture.id(instance, "wrong-run"), 5, instance.now)

        assert {:error, :event_run_mismatch} =
                 events.append_events(instance.context, :tenantless, run.id, [mismatch])

        assert {:ok, ^fourth} =
                 events.fetch_latest_event(instance.context, :tenantless, run.id)

        assert {:ok,
                %Docket.EventPage{
                  events: [^first],
                  next_after_seq: 1,
                  has_more?: true,
                  oldest_available_seq: 1,
                  latest_available_seq: 4,
                  latest_seq: 4
                }} =
                 events.list_events(instance.context, :tenantless, run.id, %{
                   after_seq: 0,
                   limit: 1
                 })

        assert {:ok, %Docket.EventPage{events: [^third, ^fourth], next_after_seq: 4}} =
                 events.list_events(instance.context, :tenantless, run.id, %{
                   after_seq: 1,
                   limit: 10
                 })

        assert {:error, :not_found} =
                 events.list_events(
                   instance.context,
                   {:tenant, Fixture.id(instance, "wrong-tenant")},
                   run.id,
                   %{after_seq: 0, limit: 10}
                 )

        empty_run = Fixture.run(instance, "sparse-run", graph, graph_hash, event_seq: 4)
        assert {:ok, ^empty_run} = Fixture.initialize(instance, :tenantless, empty_run)

        assert {:ok, nil} =
                 events.fetch_latest_event(instance.context, :tenantless, empty_run.id)

        assert {:ok,
                %Docket.EventPage{
                  events: [],
                  oldest_available_seq: nil,
                  latest_available_seq: nil,
                  latest_seq: 4
                }} =
                 events.list_events(instance.context, :tenantless, empty_run.id, %{
                   after_seq: 0,
                   limit: 10
                 })
      end

      @tag docket_invariant: "RUN-CLAIM-FENCE-SEQUENCE"
      test "[RUN-CLAIM-FENCE-SEQUENCE] stale authority cannot commit and exact next sequence wins once",
           %{backend_test: instance} do
        backend = instance.backend
        runs = backend.runs()
        events = backend.events()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "fence-graph")
        stored = Fixture.run(instance, "fence-run", graph, graph_hash)
        assert {:ok, ^stored} = Fixture.initialize(instance, :tenantless, stored)
        lease = Fixture.claim(instance)

        active_lease =
          Fixture.claim(
            instance,
            now: DateTime.add(instance.now, 1, :millisecond),
            orphan_ttl_ms: 0
          )

        next = %{
          stored
          | checkpoint_seq: 2,
            event_seq: 1,
            updated_at: DateTime.add(instance.now, 1, :second)
        }

        retained = Fixture.event(next, 1, next.updated_at)

        stale = Fixture.proposal(next, lease.claim_token)

        assert {:error, :stale_fence} =
                 backend.transaction(instance.context, fn tx ->
                   with :ok <- events.append_events(tx, :system, next.id, [retained]),
                        {:ok, _} <- runs.commit(tx, :system, stale) do
                     {:ok, :impossible}
                   end
                 end)

        assert {:ok, ^stored} = runs.fetch_run(instance.context, :tenantless, stored.id)

        assert {:error, :not_found} =
                 events.fetch_event(instance.context, :system, stored.id, retained.seq)

        skipped = %{next | checkpoint_seq: 3}

        assert {:error, :invalid_commit} =
                 runs.commit(
                   instance.context,
                   :system,
                   Fixture.proposal(skipped, active_lease.claim_token, expected_checkpoint_seq: 1)
                 )

        proposal = Fixture.proposal(next, active_lease.claim_token)

        assert {:ok, ^next} =
                 backend.transaction(instance.context, fn tx ->
                   with {:ok, committed} <- runs.commit(tx, :system, proposal),
                        :ok <- events.append_events(tx, :system, next.id, [retained]) do
                     {:ok, committed}
                   end
                 end)

        assert {:error, :stale_fence} = runs.commit(instance.context, :system, proposal)
        assert {:ok, ^next} = runs.fetch_run(instance.context, :tenantless, next.id)
        assert {:ok, ^retained} = events.fetch_event(instance.context, :system, next.id, 1)

        concurrent = Fixture.run(instance, "same-fence-run", graph, graph_hash)
        assert {:ok, ^concurrent} = Fixture.initialize(instance, :tenantless, concurrent)
        concurrent_lease = Fixture.claim(instance)

        concurrent_next = %{
          concurrent
          | checkpoint_seq: 2,
            event_seq: 1,
            updated_at: DateTime.add(instance.now, 2, :second)
        }

        concurrent_event = Fixture.event(concurrent_next, 1, concurrent_next.updated_at)
        concurrent_proposal = Fixture.proposal(concurrent_next, concurrent_lease.claim_token)
        parent = self()

        tasks =
          for _ <- 1..2 do
            Task.async(fn ->
              send(parent, {:same_fence_ready, self()})
              receive do: (:same_fence_go -> :ok)

              backend.transaction(instance.context, fn tx ->
                with {:ok, committed} <- runs.commit(tx, :system, concurrent_proposal),
                     :ok <-
                       events.append_events(tx, :system, concurrent_next.id, [concurrent_event]) do
                  {:ok, committed}
                end
              end)
            end)
          end

        ready =
          for _ <- 1..2 do
            assert_receive {:same_fence_ready, pid}, 5_000
            pid
          end

        Enum.each(ready, &send(&1, :same_fence_go))
        results = Enum.map(tasks, &Task.await(&1, 5_000))

        assert Enum.count(results, &match?({:ok, %Docket.Run{}}, &1)) == 1
        assert Enum.count(results, &(&1 == {:error, :stale_fence})) == 1

        assert {:ok, ^concurrent_event} =
                 events.fetch_event(instance.context, :system, concurrent_next.id, 1)
      end

      @tag docket_invariant: "RUN-CLAIM-LIVENESS-RECOVERY"
      test "[RUN-CLAIM-LIVENESS-RECOVERY] refresh, release, abandon, poison, and recovery preserve authority",
           %{backend_test: instance} do
        runs = instance.backend.runs()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "claim-graph")
        run = Fixture.run(instance, "claim-run", graph, graph_hash)
        assert {:ok, ^run} = Fixture.initialize(instance, :tenantless, run)
        first = Fixture.claim(instance)
        refreshed_at = DateTime.add(instance.now, 1, :second)

        assert {:error, :claim_lost} =
                 runs.refresh_claim(instance.context, :system, run.id, "wrong", refreshed_at)

        assert :ok =
                 runs.refresh_claim(
                   instance.context,
                   :system,
                   run.id,
                   first.claim_token,
                   refreshed_at
                 )

        assert {:ok, after_newer_refresh} =
                 runs.inspect_run(instance.context, :system, run.id)

        assert DateTime.compare(after_newer_refresh.claimed_at, first.claimed_at) != :lt

        assert :ok =
                 runs.refresh_claim(
                   instance.context,
                   :system,
                   run.id,
                   first.claim_token,
                   instance.now
                 )

        assert {:ok, refreshed_info} = runs.inspect_run(instance.context, :system, run.id)

        assert DateTime.compare(refreshed_info.claimed_at, after_newer_refresh.claimed_at) != :lt

        stolen_at = DateTime.add(refreshed_at, 1, :millisecond)
        second = Fixture.claim(instance, now: stolen_at, orphan_ttl_ms: 0)
        assert second.claim_token != first.claim_token

        assert {:ok, before_stale_release} =
                 runs.inspect_run(instance.context, :system, run.id)

        assert :ok =
                 runs.release_claim(
                   instance.context,
                   :system,
                   run.id,
                   first.claim_token,
                   stolen_at
                 )

        assert {:ok, after_stale_release} =
                 runs.inspect_run(instance.context, :system, run.id)

        assert after_stale_release == before_stale_release

        assert :ok =
                 runs.refresh_claim(
                   instance.context,
                   :system,
                   run.id,
                   second.claim_token,
                   stolen_at
                 )

        released_at = DateTime.add(stolen_at, 1, :second)

        assert :ok =
                 runs.release_claim(
                   instance.context,
                   :system,
                   run.id,
                   second.claim_token,
                   released_at
                 )

        assert {:ok, %{claimed_at: nil, wake_at: wake_at}} =
                 runs.inspect_run(instance.context, :system, run.id)

        assert wake_at == released_at

        parked_lease = Fixture.claim(instance, now: released_at)
        parked = %{run | checkpoint_seq: 2, updated_at: released_at}

        assert {:ok, ^parked} =
                 runs.commit(
                   instance.context,
                   :system,
                   Fixture.proposal(parked, parked_lease.claim_token)
                 )

        abandon_run = Fixture.run(instance, "abandon-run", graph, graph_hash)
        assert {:ok, ^abandon_run} = Fixture.initialize(instance, :tenantless, abandon_run)
        abandon_lease = Fixture.claim(instance)
        abandoned_at = DateTime.add(instance.now, 2, :second)
        retry_at = DateTime.add(abandoned_at, 10, :second)

        policy = %{
          expected_checkpoint_seq: 1,
          now: abandoned_at,
          retry_at: retry_at,
          max_claim_abandons: 1
        }

        assert {:ok, before_stale_abandon} =
                 runs.inspect_run(instance.context, :system, abandon_run.id)

        assert {:ok, :stale} =
                 runs.abandon_claim(instance.context, :system, abandon_run.id, "wrong", policy)

        assert {:ok, after_stale_abandon} =
                 runs.inspect_run(instance.context, :system, abandon_run.id)

        assert after_stale_abandon == before_stale_abandon

        assert {:ok, :rescheduled} =
                 runs.abandon_claim(
                   instance.context,
                   :system,
                   abandon_run.id,
                   abandon_lease.claim_token,
                   policy
                 )

        assert {:ok,
                %Docket.RunInfo{
                  run: ^abandon_run,
                  wake_at: ^retry_at,
                  claimed_at: nil,
                  claim_attempts: 0,
                  claim_abandons: 1,
                  poisoned_at: nil,
                  poison_reason: nil
                }} = runs.inspect_run(instance.context, :system, abandon_run.id)

        next_lease = Fixture.claim(instance, now: retry_at)
        poisoned_at = DateTime.add(retry_at, 1, :second)

        assert {:ok, :poisoned} =
                 runs.abandon_claim(
                   instance.context,
                   :system,
                   abandon_run.id,
                   next_lease.claim_token,
                   %{policy | now: poisoned_at, retry_at: DateTime.add(poisoned_at, 10, :second)}
                 )

        assert {:ok, poisoned_info} =
                 runs.inspect_run(instance.context, :system, abandon_run.id)

        assert poisoned_info.run == abandon_run
        assert poisoned_info.claimed_at == nil
        assert poisoned_info.claim_attempts == 0
        assert poisoned_info.claim_abandons == 1
        assert poisoned_info.poisoned_at == poisoned_at
        assert poisoned_info.poison_reason == "max_claim_abandons_exceeded"
        assert poisoned_info.wake_at == nil
        recovered_at = DateTime.add(poisoned_at, 1, :second)

        assert {:ok, ^abandon_run} =
                 runs.retry_poisoned_run(
                   instance.context,
                   :tenantless,
                   abandon_run.id,
                   recovered_at
                 )

        assert {:ok, recovered_info} =
                 runs.inspect_run(instance.context, :tenantless, abandon_run.id)

        assert recovered_info.run == abandon_run
        assert recovered_info.wake_at == recovered_at
        assert recovered_info.claimed_at == nil
        assert recovered_info.poisoned_at == nil
        assert recovered_info.poison_reason == nil
        assert recovered_info.claim_attempts == 0
        assert recovered_info.claim_abandons == 0
      end

      @tag docket_invariant: "RUN-READS-MUTATION-RESULTS"
      test "[RUN-READS-MUTATION-RESULTS] list cursors, scopes, and mutation result shapes are preserved",
           %{backend_test: instance} do
        runs = instance.backend.runs()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "run-read-graph")
        first = Fixture.run(instance, "run-read-first", graph, graph_hash)
        later = DateTime.add(instance.now, 1, :second)

        second =
          Fixture.run(instance, "run-read-second", graph, graph_hash,
            started_at: later,
            updated_at: later
          )

        tied =
          Fixture.run(instance, "run-read-z-tie", graph, graph_hash,
            started_at: later,
            updated_at: later
          )

        assert {:ok, ^first} = Fixture.initialize(instance, :tenantless, first)
        assert {:ok, ^second} = Fixture.initialize(instance, :tenantless, second)
        assert {:ok, ^tied} = Fixture.initialize(instance, :tenantless, tied)

        base_query = %{
          limit: 1,
          before: nil,
          graph_id: graph.id,
          graph_hash: graph_hash,
          statuses: [:running]
        }

        assert {:ok, %Docket.RunPage{runs: [first_page], has_more?: true} = page} =
                 runs.list_runs(instance.context, :tenantless, base_query)

        assert first_page.id == tied.id

        assert {:ok, %Docket.RunPage{runs: [second_page], has_more?: true} = second_page_result} =
                 runs.list_runs(
                   instance.context,
                   :tenantless,
                   %{base_query | before: page.next_before}
                 )

        assert second_page.id == second.id

        assert {:ok, %Docket.RunPage{runs: [third_page], has_more?: false} = final_page} =
                 runs.list_runs(
                   instance.context,
                   :tenantless,
                   %{base_query | before: second_page_result.next_before}
                 )

        assert third_page.id == first.id

        assert {:ok, %Docket.RunPage{runs: [], has_more?: false} = empty_page} =
                 runs.list_runs(
                   instance.context,
                   :tenantless,
                   %{base_query | before: final_page.next_before}
                 )

        assert empty_page.next_before == final_page.next_before

        assert {:ok, %Docket.RunPage{runs: []}} =
                 runs.list_runs(
                   instance.context,
                   {:tenant, Fixture.id(instance, "absent-tenant")},
                   %{base_query | limit: 10}
                 )

        assert {:ok, %Docket.RunPage{runs: system_runs}} =
                 runs.list_runs(instance.context, :system, %{base_query | limit: 10})

        assert MapSet.new(Enum.map(system_runs, & &1.id)) ==
                 MapSet.new([first.id, second.id, tied.id])

        assert {:ok, %Docket.RunInfo{run: ^second}} =
                 runs.inspect_run(instance.context, :tenantless, second.id)

        assert {:ok, {:unchanged, :already_applied}} =
                 runs.mutate_run(instance.context, :tenantless, first.id, fn _current ->
                   {:no_change, :already_applied}
                 end)

        assert {:error, :rejected} =
                 runs.mutate_run(instance.context, :tenantless, first.id, fn _current ->
                   {:error, :rejected}
                 end)

        assert {:error, :invalid_mutation} =
                 runs.mutate_run(instance.context, :tenantless, first.id, fn _current ->
                   :invalid_decision
                 end)

        assert {:ok, ^first} = runs.fetch_run(instance.context, :tenantless, first.id)
      end

      @tag docket_invariant: "RUN-SERIALIZED-MUTATION"
      test "[RUN-SERIALIZED-MUTATION] concurrent updates are not lost and failed event publication rolls back",
           %{backend_test: instance} do
        backend = instance.backend
        runs = backend.runs()
        events = backend.events()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "mutation-graph")
        stored = Fixture.run(instance, "mutation-run", graph, graph_hash, input: %{"count" => 0})
        assert {:ok, ^stored} = Fixture.initialize(instance, :tenantless, stored)
        parent = self()

        mutate = fn current ->
          count = Map.fetch!(current.input, "count") + 1

          next = %{
            current
            | input: %{"count" => count},
              checkpoint_seq: current.checkpoint_seq + 1,
              updated_at: DateTime.add(current.updated_at, 1, :microsecond)
          }

          {:commit, next, :step_committed, {:release_claim, :immediate}, count}
        end

        first =
          Task.async(fn ->
            runs.mutate_run(instance.context, :tenantless, stored.id, fn current ->
              send(parent, {:first_mutation_entered, self()})
              receive do: (:release_first_mutation -> :ok)
              mutate.(current)
            end)
          end)

        assert_receive {:first_mutation_entered, mutation_holder}, 5_000

        second =
          Task.async(fn ->
            send(parent, :second_mutation_attempting)

            runs.mutate_run(instance.context, :tenantless, stored.id, fn current ->
              send(parent, :second_mutation_entered)
              mutate.(current)
            end)
          end)

        assert_receive :second_mutation_attempting, 5_000
        second_result = Task.yield(second, 100)
        refute_received :second_mutation_entered
        send(mutation_holder, :release_first_mutation)

        results = [
          Task.await(first, 5_000),
          Docket.BackendTests.await_task(second, second_result)
        ]

        assert Enum.sort(results) == [ok: {:committed, 1}, ok: {:committed, 2}]

        assert {:ok, %{input: %{"count" => 2}, checkpoint_seq: 3} = twice_mutated} =
                 runs.fetch_run(instance.context, :tenantless, stored.id)

        assert {:error, :not_found} =
                 runs.mutate_run(
                   instance.context,
                   {:tenant, Fixture.id(instance, "wrong-owner")},
                   stored.id,
                   fn _ ->
                     send(parent, :out_of_scope_mutation_called)
                     {:no_change, :impossible}
                   end
                 )

        refute_receive :out_of_scope_mutation_called

        claimed = Fixture.claim(instance, now: DateTime.add(instance.now, 60, :second))
        mismatched = Fixture.event(Fixture.id(instance, "wrong-mutation-run"), 1, instance.now)

        assert {:error, :event_run_mismatch} =
                 backend.transaction(instance.context, fn tx ->
                   mutation = fn current ->
                     next = %{
                       current
                       | checkpoint_seq: current.checkpoint_seq + 1,
                         updated_at: DateTime.add(current.updated_at, 1, :microsecond)
                     }

                     {:commit, next, :step_committed, {:release_claim, :immediate}, [mismatched]}
                   end

                   with {:ok, {:committed, retained}} <-
                          runs.mutate_run(tx, :tenantless, stored.id, mutation),
                        :ok <- events.append_events(tx, :tenantless, stored.id, retained) do
                     {:ok, :impossible}
                   end
                 end)

        assert {:ok, ^twice_mutated} =
                 runs.fetch_run(instance.context, :tenantless, stored.id)

        assert :ok =
                 runs.refresh_claim(
                   instance.context,
                   :system,
                   stored.id,
                   claimed.claim_token,
                   DateTime.add(instance.now, 61, :second)
                 )
      end
    end
  end
end
