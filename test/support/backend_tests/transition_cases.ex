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

      @tag docket_invariant: "TRANSITION-CLAIMED-TENANCY-IDENTITY"
      test "[TRANSITION-CLAIMED-TENANCY-IDENTITY] claimed commits validate before lookup and conceal tenancy",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()
        token = "00000000-0000-0000-0000-000000000001"

        {ghost_graph, ghost_hash} =
          Fixture.graph(instance, "transition-claimed-ghost-graph", %{})

        ghost =
          Fixture.run(instance, "transition-claimed-ghost", ghost_graph, ghost_hash,
            checkpoint_seq: 2
          )

        well_formed = %{
          transition_id: Fixture.id(instance, "transition-claimed-ghost-id"),
          run: ghost,
          expected_checkpoint_seq: 1,
          expected_event_seq: 0,
          claim_token: token,
          checkpoint_type: :step_committed,
          schedule: :retain_claim
        }

        assert {:error, :not_found} =
                 transitions.commit_claimed(instance.context, :tenantless, well_formed, [])

        malformed = Map.delete(well_formed, :claim_token)

        assert {:error, :invalid_transition} =
                 transitions.commit_claimed(instance.context, :tenantless, malformed, [])

        {graph, graph_hash} =
          Fixture.publish_graph(
            instance,
            {:tenant, "tenant-a"},
            "transition-claimed-tenant-graph"
          )

        tenant_run =
          Fixture.run(instance, "transition-claimed-tenant-run", graph, graph_hash)

        init = %{
          transition_id: Fixture.id(instance, "transition-claimed-tenant-init"),
          run: tenant_run,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^tenant_run} =
                 transitions.initialize(instance.context, {:tenant, "tenant-a"}, init, [])

        advanced = %{
          tenant_run
          | checkpoint_seq: 2,
            updated_at: DateTime.add(instance.now, 1, :microsecond)
        }

        cross_tenant = %{
          transition_id: Fixture.id(instance, "transition-claimed-cross-tenant"),
          run: advanced,
          expected_checkpoint_seq: 1,
          expected_event_seq: 0,
          claim_token: token,
          checkpoint_type: :step_committed,
          schedule: :retain_claim
        }

        assert {:error, :not_found} =
                 transitions.commit_claimed(
                   instance.context,
                   {:tenant, "tenant-b"},
                   cross_tenant,
                   []
                 )

        for {suffix, mutate} <- [
              {"graph-id", &%{&1 | graph_id: "immutable-other-graph"}},
              {"started-at", &%{&1 | started_at: DateTime.add(instance.now, 7, :second)}}
            ] do
          mismatch = %{
            cross_tenant
            | transition_id: Fixture.id(instance, "transition-immutable-#{suffix}"),
              run: mutate.(advanced)
          }

          assert {:error, :invalid_transition} =
                   transitions.commit_claimed(
                     instance.context,
                     {:tenant, "tenant-a"},
                     mismatch,
                     []
                   )
        end

        assert {:ok, %Docket.Run{checkpoint_seq: 1}} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   {:tenant, "tenant-a"},
                   tenant_run.id
                 )
      end

      @tag docket_invariant: "TRANSITION-CLAIMED-SCHEDULES"
      test "[TRANSITION-CLAIMED-SCHEDULES] claimed commits apply every release schedule variant",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()

        {graph, graph_hash} =
          Fixture.publish_graph(instance, :tenantless, "transition-schedules")

        commit_with = fn suffix, mutate_run, schedule ->
          initial = Fixture.run(instance, "transition-schedule-#{suffix}", graph, graph_hash)

          init = %{
            transition_id: Fixture.id(instance, "transition-schedule-#{suffix}-init"),
            run: initial,
            checkpoint_type: :run_initialized,
            wake_at: instance.now
          }

          assert {:ok, ^initial} =
                   transitions.initialize(instance.context, :tenantless, init, [])

          lease = Fixture.claim(instance)
          assert lease.run_id == initial.id

          advanced =
            mutate_run.(%{
              initial
              | checkpoint_seq: 2,
                updated_at: DateTime.add(instance.now, 1, :microsecond)
            })

          proposal = %{
            transition_id: Fixture.id(instance, "transition-schedule-#{suffix}-id"),
            run: advanced,
            expected_checkpoint_seq: 1,
            expected_event_seq: 0,
            claim_token: lease.claim_token,
            checkpoint_type: :step_committed,
            schedule: schedule
          }

          assert {:ok, ^advanced} =
                   transitions.commit_claimed(instance.context, :tenantless, proposal, [])

          assert {:ok, stored} =
                   instance.backend.runs().fetch_run(
                     instance.context,
                     :tenantless,
                     advanced.id
                   )

          assert stored.status == advanced.status
          assert stored.checkpoint_seq == 2
        end

        commit_with.(
          "at",
          & &1,
          {:release_claim, {:at, DateTime.add(instance.now, 3600, :second)}}
        )

        commit_with.(
          "external",
          &%{&1 | status: :waiting},
          {:release_claim, :external}
        )

        commit_with.(
          "terminal",
          &%{&1 | status: :done, finished_at: DateTime.add(instance.now, 1, :microsecond)},
          {:release_claim, :terminal}
        )
      end

      @tag docket_invariant: "TRANSITION-EVENT-CANONICAL-REPLAY"
      test "[TRANSITION-EVENT-CANONICAL-REPLAY] replay and duplicates compare canonical event content",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()

        {graph, graph_hash} =
          Fixture.publish_graph(instance, :tenantless, "transition-canonical")

        stored_conflict =
          Fixture.run(instance, "transition-stored-conflict-run", graph, graph_hash)

        stored_init = %{
          transition_id: Fixture.id(instance, "transition-stored-conflict-init"),
          run: stored_conflict,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^stored_conflict} =
                 transitions.initialize(instance.context, :tenantless, stored_init, [])

        lease = Fixture.claim(instance)
        assert lease.run_id == stored_conflict.id

        assert :ok =
                 instance.backend.events().append_events(
                   instance.context,
                   :tenantless,
                   stored_conflict.id,
                   [Fixture.event(stored_conflict, 1, instance.now, payload: %{"stored" => 1})]
                 )

        conflicted_advance = %{
          stored_conflict
          | checkpoint_seq: 2,
            event_seq: 1,
            updated_at: DateTime.add(instance.now, 1, :microsecond)
        }

        conflicting = %{
          transition_id: Fixture.id(instance, "transition-stored-conflict-id"),
          run: conflicted_advance,
          expected_checkpoint_seq: 1,
          expected_event_seq: 0,
          claim_token: lease.claim_token,
          checkpoint_type: :step_committed,
          schedule: :retain_claim
        }

        assert {:error, :event_conflict} =
                 transitions.commit_claimed(
                   instance.context,
                   :tenantless,
                   conflicting,
                   [
                     Fixture.event(stored_conflict, 1, instance.now, payload: %{"proposed" => 2})
                   ]
                 )

        assert {:ok, %Docket.Run{checkpoint_seq: 1}} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   :tenantless,
                   stored_conflict.id
                 )

        run =
          Fixture.run(instance, "transition-canonical-run", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 2
          )

        first = Fixture.event(run, 1, instance.now, type: :run_initialized)
        second = Fixture.event(run, 2, instance.now)

        proposal = %{
          transition_id: Fixture.id(instance, "transition-canonical-id"),
          run: run,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^run} =
                 transitions.initialize(instance.context, :tenantless, proposal, [
                   first,
                   second
                 ])

        assert {:ok, ^run} =
                 transitions.initialize(instance.context, :tenantless, proposal, [
                   second,
                   first
                 ])

        assert {:ok, ^run} =
                 transitions.initialize(instance.context, :tenantless, proposal, [
                   first,
                   first,
                   second
                 ])

        collapsed =
          Fixture.run(instance, "transition-collapsed-run", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 1
          )

        duplicate = Fixture.event(collapsed, 1, instance.now, type: :run_initialized)

        assert {:ok, ^collapsed} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   %{
                     proposal
                     | transition_id: Fixture.id(instance, "transition-collapsed-id"),
                       run: collapsed
                   },
                   [duplicate, duplicate]
                 )

        assert {:ok, %Docket.Event{seq: 1}} =
                 instance.backend.events().fetch_latest_event(
                   instance.context,
                   :tenantless,
                   collapsed.id
                 )

        contradictory =
          Fixture.run(instance, "transition-contradictory-run", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 1
          )

        assert {:error, :event_conflict} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   %{
                     proposal
                     | transition_id: Fixture.id(instance, "transition-contradictory-id"),
                       run: contradictory
                   },
                   [
                     Fixture.event(contradictory, 1, instance.now, payload: %{"pick" => 1}),
                     Fixture.event(contradictory, 1, instance.now, payload: %{"pick" => 2})
                   ]
                 )

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   :tenantless,
                   contradictory.id
                 )

        mismatched =
          Fixture.run(instance, "transition-mismatched-run", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 1
          )

        assert {:error, :invalid_transition} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   %{
                     proposal
                     | transition_id: Fixture.id(instance, "transition-mismatched-id"),
                       run: mismatched
                   },
                   [Fixture.event("some-other-run", 1, instance.now)]
                 )
      end

      @tag docket_invariant: "TRANSITION-ID-PORTABILITY"
      test "[TRANSITION-ID-PORTABILITY] transition IDs are bounded UTF-8 on every backend",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()

        {graph, graph_hash} =
          Fixture.publish_graph(instance, :tenantless, "transition-id-portability")

        run = Fixture.run(instance, "transition-id-run", graph, graph_hash)

        proposal = %{
          transition_id: <<0xFF, 0xFE, 0x00>>,
          run: run,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:error, :invalid_transition} =
                 transitions.initialize(instance.context, :tenantless, proposal, [])

        max_bytes = TransitionStore.max_transition_id_bytes()

        oversized = %{
          proposal
          | transition_id: String.pad_trailing(Fixture.id(instance, "id"), max_bytes + 1, "a")
        }

        assert {:error, :invalid_transition} =
                 transitions.initialize(instance.context, :tenantless, oversized, [])

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(instance.context, :tenantless, run.id)

        exact = %{
          proposal
          | transition_id: String.pad_trailing(Fixture.id(instance, "id"), max_bytes, "a")
        }

        assert {:ok, ^run} =
                 transitions.initialize(instance.context, :tenantless, exact, [])
      end

      @tag docket_invariant: "TRANSITION-EXACT-BYTE-LIMITS"
      test "[TRANSITION-EXACT-BYTE-LIMITS] every portable byte bound passes exactly and rejects one over",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()
        limits = TransitionStore.portable_limits()

        {graph, graph_hash} =
          Fixture.publish_graph(instance, :tenantless, "transition-exact-bytes")

        base_proposal = fn run ->
          %{
            transition_id: Fixture.id(instance, "#{run.id}-id"),
            run: run,
            checkpoint_type: :run_initialized,
            wake_at: instance.now
          }
        end

        sized_run = fn suffix, event_seq, target ->
          Fixture.pad_to_encoded_size(target, fn pad ->
            Fixture.run(instance, suffix, graph, graph_hash,
              checkpoint_seq: 1,
              event_seq: event_seq,
              input: %{"pad" => pad}
            )
          end)
        end

        exact_run = sized_run.("transition-exact-run-bytes", 0, limits.max_run_bytes)

        assert {:ok, ^exact_run} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   base_proposal.(exact_run),
                   []
                 )

        oversized_run =
          sized_run.("transition-over-run-bytes", 0, limits.max_run_bytes + 1)

        assert {:error, :too_large} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   base_proposal.(oversized_run),
                   []
                 )

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   :tenantless,
                   oversized_run.id
                 )

        event_run =
          Fixture.run(instance, "transition-exact-event-bytes", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 1
          )

        exact_event = Fixture.sized_event(event_run, 1, instance.now, limits.max_event_bytes)

        assert {:ok, ^event_run} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   base_proposal.(event_run),
                   [exact_event]
                 )

        over_event_run =
          Fixture.run(instance, "transition-over-event-bytes", graph, graph_hash,
            checkpoint_seq: 1,
            event_seq: 1
          )

        over_event =
          Fixture.sized_event(over_event_run, 1, instance.now, limits.max_event_bytes + 1)

        assert {:error, :too_large} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   base_proposal.(over_event_run),
                   [over_event]
                 )

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   :tenantless,
                   over_event_run.id
                 )

        event_count = 54
        event_bytes = event_count * limits.max_event_bytes
        proposal_budget = limits.max_transition_bytes - event_bytes

        sized_total_proposal = fn suffix, target ->
          Fixture.pad_to_encoded_size(target, fn pad ->
            run =
              Fixture.run(instance, suffix, graph, graph_hash,
                checkpoint_seq: 1,
                event_seq: event_count,
                input: %{"pad" => pad}
              )

            %{
              transition_id: Fixture.id(instance, "#{suffix}-id"),
              run: run,
              checkpoint_type: :run_initialized,
              wake_at: instance.now
            }
          end)
        end

        exact_total = sized_total_proposal.("transition-exact-total", proposal_budget)

        exact_total_events =
          Enum.map(
            1..event_count,
            &Fixture.sized_event(exact_total.run, &1, instance.now, limits.max_event_bytes)
          )

        assert {:ok, _run} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   exact_total,
                   exact_total_events
                 )

        over_total = sized_total_proposal.("transition-over-total", proposal_budget + 1)

        over_total_events =
          Enum.map(
            1..event_count,
            &Fixture.sized_event(over_total.run, &1, instance.now, limits.max_event_bytes)
          )

        assert {:error, :too_large} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   over_total,
                   over_total_events
                 )

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   :tenantless,
                   over_total.run.id
                 )
      end
    end
  end
end
