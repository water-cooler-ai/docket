defmodule Docket.BackendTests.TransitionCases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote location: :keep do
      alias Docket.Backend.TransitionStore
      alias Docket.BackendTests.Fixture

      @tag docket_invariant: "TRANSITION-INITIALIZE-REPLAY-TENANCY"
      test "[TRANSITION-INITIALIZE-REPLAY-TENANCY] initialization is atomic, replayable, and concealed",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "transition-init")

        run =
          Fixture.run(instance, "transition-init-run", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 1
          )

        event = Fixture.event(run, 1, instance.now, type: :run_initialized)

        proposal = %{
          transition_id: Fixture.id(instance, "transition-init-id"),
          run: run,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^run} =
                 transitions.initialize(instance.context, :tenantless, proposal, [event])

        assert {:ok, ^run} =
                 transitions.initialize(instance.context, :tenantless, proposal, [event])

        conflicting = %{proposal | run: %{run | metadata: %{"different" => true}}}

        assert {:error, :conflict} =
                 transitions.initialize(instance.context, :tenantless, conflicting, [event])

        missing =
          Fixture.run(instance, "transition-missing-run", graph, "missing-hash",
            checkpoint_seq: 1
          )

        missing_proposal = %{
          transition_id: Fixture.id(instance, "transition-missing-id"),
          run: missing,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:error, :not_found} =
                 transitions.initialize(instance.context, :tenantless, missing_proposal, [])

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   :tenantless,
                   missing.id
                 )

        {tenant_graph, tenant_hash} =
          Fixture.graph(instance, "transition-cross-tenant-collision", %{})

        for owner <- [{:tenant, "tenant-a"}, {:tenant, "tenant-b"}] do
          :ok =
            instance.backend.graphs().save_graph(
              instance.context,
              owner,
              tenant_graph.id,
              tenant_hash,
              tenant_graph
            )
        end

        tenant_run =
          Fixture.run(instance, "transition-shared-run-id", tenant_graph, tenant_hash,
            checkpoint_seq: 1
          )

        tenant_a = %{
          transition_id: Fixture.id(instance, "transition-tenant-a"),
          run: tenant_run,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^tenant_run} =
                 transitions.initialize(instance.context, {:tenant, "tenant-a"}, tenant_a, [])

        tenant_b = %{tenant_a | transition_id: Fixture.id(instance, "transition-tenant-b")}

        assert {:error, :not_found} =
                 transitions.initialize(instance.context, {:tenant, "tenant-b"}, tenant_b, [])
      end

      @tag docket_invariant: "TRANSITION-CLAIMED-FENCES-REPLAY"
      test "[TRANSITION-CLAIMED-FENCES-REPLAY] claimed writes fence state and replay canonical events",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "transition-claimed")
        initial = Fixture.run(instance, "transition-claimed-run", graph, graph_hash)

        init = %{
          transition_id: Fixture.id(instance, "transition-claimed-init"),
          run: initial,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^initial} = transitions.initialize(instance.context, :tenantless, init, [])
        lease = Fixture.claim(instance)

        advanced = %{
          initial
          | checkpoint_seq: 2,
            event_seq: 1,
            updated_at: DateTime.add(instance.now, 1, :microsecond),
            metadata: %{"winner" => 1}
        }

        event = Fixture.event(advanced, 1, instance.now, payload: %{"canonical" => true})

        proposal = %{
          transition_id: Fixture.id(instance, "transition-claimed-id"),
          run: advanced,
          expected_checkpoint_seq: 1,
          expected_event_seq: 0,
          claim_token: lease.claim_token,
          checkpoint_type: :step_committed,
          schedule: :retain_claim
        }

        assert :ok =
                 instance.backend.events().append_events(
                   instance.context,
                   :tenantless,
                   initial.id,
                   [event]
                 )

        assert {:ok, ^advanced} =
                 transitions.commit_claimed(instance.context, :tenantless, proposal, [event])

        assert {:ok, ^advanced} =
                 transitions.commit_claimed(instance.context, :tenantless, proposal, [event])

        assert {:error, :conflict} =
                 transitions.commit_claimed(
                   instance.context,
                   :tenantless,
                   %{proposal | run: %{advanced | metadata: %{"winner" => 2}}},
                   [event]
                 )

        next = %{
          advanced
          | checkpoint_seq: 3,
            updated_at: DateTime.add(instance.now, 2, :microsecond)
        }

        stale = %{
          proposal
          | transition_id: Fixture.id(instance, "transition-stale-token"),
            run: next,
            expected_checkpoint_seq: 2,
            expected_event_seq: 1,
            claim_token: "00000000-0000-0000-0000-000000000000"
        }

        assert {:error, :stale_checkpoint} =
                 transitions.commit_claimed(instance.context, :tenantless, stale, [])

        malformed = %{stale | run: %{next | graph_hash: "immutable-mismatch"}}

        assert {:error, :invalid_transition} =
                 transitions.commit_claimed(instance.context, :tenantless, malformed, [])
      end

      @tag docket_invariant: "TRANSITION-CONCURRENT-SAME-FENCE"
      test "[TRANSITION-CONCURRENT-SAME-FENCE] exactly one claimed proposal wins",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "transition-race")
        initial = Fixture.run(instance, "transition-race-run", graph, graph_hash)

        init = %{
          transition_id: Fixture.id(instance, "transition-race-init"),
          run: initial,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^initial} = transitions.initialize(instance.context, :tenantless, init, [])
        lease = Fixture.claim(instance)

        results =
          1..2
          |> Task.async_stream(
            fn winner ->
              advanced = %{
                initial
                | checkpoint_seq: 2,
                  updated_at: DateTime.add(instance.now, winner, :microsecond),
                  metadata: %{"winner" => winner}
              }

              proposal = %{
                transition_id: Fixture.id(instance, "transition-race-#{winner}"),
                run: advanced,
                expected_checkpoint_seq: 1,
                expected_event_seq: 0,
                claim_token: lease.claim_token,
                checkpoint_type: :step_committed,
                schedule: :retain_claim
              }

              transitions.commit_claimed(instance.context, :tenantless, proposal, [])
            end,
            max_concurrency: 2,
            ordered: false
          )
          |> Enum.map(fn {:ok, result} -> result end)

        assert Enum.count(results, &match?({:ok, %Docket.Run{}}, &1)) == 1
        assert Enum.count(results, &(&1 == {:error, :stale_checkpoint})) == 1
      end

      @tag docket_invariant: "TRANSITION-UNCLAIMED-CAS"
      test "[TRANSITION-UNCLAIMED-CAS] optimistic writes distinguish replay, ID conflict, and stale state",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "transition-unclaimed")
        initial = Fixture.run(instance, "transition-unclaimed-run", graph, graph_hash)

        init = %{
          transition_id: Fixture.id(instance, "transition-unclaimed-init"),
          run: initial,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^initial} = transitions.initialize(instance.context, :tenantless, init, [])
        lease = Fixture.claim(instance)

        for offset <- 1..3 do
          assert :ok =
                   instance.backend.runs().refresh_claim(
                     instance.context,
                     :system,
                     initial.id,
                     lease.claim_token,
                     DateTime.add(instance.now, offset, :microsecond)
                   )
        end

        advanced = %{
          initial
          | checkpoint_seq: 2,
            updated_at: DateTime.add(instance.now, 1, :microsecond),
            metadata: %{"signal" => 1}
        }

        proposal = %{
          transition_id: Fixture.id(instance, "transition-unclaimed-id"),
          run: advanced,
          expected_event_seq: 0,
          checkpoint_type: :step_committed,
          schedule: {:release_claim, :immediate}
        }

        assert {:ok, ^advanced} =
                 transitions.commit_unclaimed(
                   instance.context,
                   :tenantless,
                   1,
                   proposal,
                   []
                 )

        assert {:ok, ^advanced} =
                 transitions.commit_unclaimed(
                   instance.context,
                   :tenantless,
                   1,
                   proposal,
                   []
                 )

        assert {:error, :conflict} =
                 transitions.commit_unclaimed(
                   instance.context,
                   :tenantless,
                   1,
                   %{proposal | run: %{advanced | metadata: %{"signal" => 2}}},
                   []
                 )

        stale_run = %{
          advanced
          | checkpoint_seq: 2,
            updated_at: DateTime.add(instance.now, 2, :microsecond)
        }

        stale = %{
          proposal
          | transition_id: Fixture.id(instance, "transition-unclaimed-stale"),
            run: stale_run
        }

        assert {:error, :stale_checkpoint} =
                 transitions.commit_unclaimed(
                   instance.context,
                   :tenantless,
                   1,
                   stale,
                   []
                 )
      end

      @tag docket_invariant: "TRANSITION-EVENT-VALIDATION-LIMITS"
      test "[TRANSITION-EVENT-VALIDATION-LIMITS] malformed sequences and oversized batches write nothing",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "transition-invalid")

        run =
          Fixture.run(instance, "transition-invalid-run", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 2
          )

        proposal = %{
          transition_id: Fixture.id(instance, "transition-invalid-id"),
          run: run,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        sparse = [
          Fixture.event(run, 1, instance.now),
          Fixture.event(run, 3, instance.now)
        ]

        assert {:error, :invalid_transition} =
                 transitions.initialize(instance.context, :tenantless, proposal, sparse)

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(instance.context, :tenantless, run.id)

        oversized_run = %{
          run
          | id: Fixture.id(instance, "transition-oversized-run"),
            event_seq: 1
        }

        oversized_event =
          Fixture.event(oversized_run, 1, instance.now,
            payload: %{"bytes" => :binary.copy(<<0>>, 64_001)}
          )

        oversized = %{
          proposal
          | transition_id: Fixture.id(instance, "transition-oversized-id"),
            run: oversized_run
        }

        assert {:error, :too_large} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   oversized,
                   [oversized_event]
                 )

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   :tenantless,
                   oversized_run.id
                 )

        malformed = %{proposal | transition_id: Fixture.id(instance, "transition-malformed")}

        assert {:error, :invalid_transition} =
                 transitions.initialize(instance.context, :tenantless, malformed, [:not_an_event])

        exact_run =
          Fixture.run(instance, "transition-exact-event-count", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 100
          )

        exact_events = Enum.map(1..100, &Fixture.event(exact_run, &1, instance.now))

        exact = %{
          proposal
          | transition_id: Fixture.id(instance, "transition-exact-event-count-id"),
            run: exact_run
        }

        assert {:ok, ^exact_run} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   exact,
                   exact_events
                 )

        too_many_run =
          Fixture.run(instance, "transition-too-many-events", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 101
          )

        too_many_events = Enum.map(1..101, &Fixture.event(too_many_run, &1, instance.now))

        too_many = %{
          proposal
          | transition_id: Fixture.id(instance, "transition-too-many-events-id"),
            run: too_many_run
        }

        assert {:error, :too_large} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   too_many,
                   too_many_events
                 )

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   :tenantless,
                   too_many_run.id
                 )
      end
    end
  end
end
