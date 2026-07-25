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

      @tag docket_invariant: "EVENT-CURSOR-GAPS"
      test "[EVENT-CURSOR-GAPS] assigned events preserve scope, ordering, and sparse bounds",
           %{backend_test: instance} do
        events = instance.backend.events()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "event-graph")
        run = Fixture.run(instance, "event-run", graph, graph_hash, event_seq: 4)
        first = Fixture.event(run, 1, instance.now, payload: %{"winner" => true})
        third = Fixture.event(run, 3, instance.now)
        fourth = Fixture.event(run, 4, instance.now)

        assert {:ok, ^run} =
                 Fixture.initialize(instance, :tenantless, run, [first, third, fourth])

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

      @tag docket_invariant: "RUN-CLAIM-LIVENESS-RECOVERY"
      test "[RUN-CLAIM-LIVENESS-RECOVERY] refresh, release, abandon, poison, and recovery preserve authority",
           %{backend_test: instance} do
        runs = instance.backend.runs()
        transitions = instance.backend.transitions()
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
                 transitions.commit_claimed(
                   instance.context,
                   :system,
                   Fixture.proposal(parked, parked_lease.claim_token),
                   []
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

      @tag docket_invariant: "RUN-READS-CURSORS"
      test "[RUN-READS-CURSORS] list cursors and scoped reads are preserved",
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

        assert {:ok, ^first} = runs.fetch_run(instance.context, :tenantless, first.id)
      end
    end
  end
end
