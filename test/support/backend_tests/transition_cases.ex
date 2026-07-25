defmodule Docket.BackendTests.TransitionCases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote location: :keep do
      alias Docket.BackendTests.Fixture

      @tag docket_invariant: "TRANSITION-INITIALIZE-TENANCY"
      test "[TRANSITION-INITIALIZE-TENANCY] initialization is atomic, collision-safe, and concealed",
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
          run: run,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^run} =
                 transitions.initialize(instance.context, :tenantless, proposal, [event])

        assert {:error, :conflict} =
                 transitions.initialize(instance.context, :tenantless, proposal, [event])

        conflicting = %{proposal | run: %{run | metadata: %{"different" => true}}}

        assert {:error, :conflict} =
                 transitions.initialize(instance.context, :tenantless, conflicting, [event])

        missing =
          Fixture.run(instance, "transition-missing-run", graph, "missing-hash",
            checkpoint_seq: 1
          )

        missing_proposal = %{
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

        tenant_proposal = %{
          run: tenant_run,
          checkpoint_type: :run_initialized,
          wake_at: instance.now
        }

        assert {:ok, ^tenant_run} =
                 transitions.initialize(
                   instance.context,
                   {:tenant, "tenant-a"},
                   tenant_proposal,
                   []
                 )

        assert {:error, :conflict} =
                 transitions.initialize(
                   instance.context,
                   {:tenant, "tenant-b"},
                   tenant_proposal,
                   []
                 )
      end

      @tag docket_invariant: "TRANSITION-CLAIMED-FENCES"
      test "[TRANSITION-CLAIMED-FENCES] claimed writes fence state and accept identical stored events",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "transition-claimed")
        initial = Fixture.run(instance, "transition-claimed-run", graph, graph_hash)

        init = %{
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
          run: advanced,
          expected_checkpoint_seq: 1,
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

        assert {:error, :stale_checkpoint} =
                 transitions.commit_claimed(instance.context, :tenantless, proposal, [event])

        next = %{
          advanced
          | checkpoint_seq: 3,
            updated_at: DateTime.add(instance.now, 2, :microsecond)
        }

        stale = %{
          proposal
          | run: next,
            expected_checkpoint_seq: 2,
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
                run: advanced,
                expected_checkpoint_seq: 1,
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
      test "[TRANSITION-UNCLAIMED-CAS] optimistic writes apply once and reject stale fences",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()
        {graph, graph_hash} = Fixture.publish_graph(instance, :tenantless, "transition-unclaimed")
        initial = Fixture.run(instance, "transition-unclaimed-run", graph, graph_hash)

        init = %{
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
          run: advanced,
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

        assert {:error, :stale_checkpoint} =
                 transitions.commit_unclaimed(
                   instance.context,
                   :tenantless,
                   1,
                   proposal,
                   []
                 )

        stale_run = %{
          advanced
          | checkpoint_seq: 2,
            updated_at: DateTime.add(instance.now, 2, :microsecond),
            metadata: %{"signal" => 2}
        }

        assert {:error, :stale_checkpoint} =
                 transitions.commit_unclaimed(
                   instance.context,
                   :tenantless,
                   1,
                   %{proposal | run: stale_run},
                   []
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
          run: ghost,
          expected_checkpoint_seq: 1,
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
          run: advanced,
          expected_checkpoint_seq: 1,
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
          _ = suffix

          mismatch = %{cross_tenant | run: mutate.(advanced)}

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
            run: advanced,
            expected_checkpoint_seq: 1,
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

      @tag docket_invariant: "TRANSITION-EVENT-CANONICAL"
      test "[TRANSITION-EVENT-CANONICAL] events compare canonical content and reject conflicts atomically",
           %{backend_test: instance} do
        transitions = instance.backend.transitions()

        {graph, graph_hash} =
          Fixture.publish_graph(instance, :tenantless, "transition-canonical")

        stored_conflict =
          Fixture.run(instance, "transition-stored-conflict-run", graph, graph_hash)

        stored_init = %{
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
          run: conflicted_advance,
          expected_checkpoint_seq: 1,
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
                     run: collapsed,
                     checkpoint_type: :run_initialized,
                     wake_at: instance.now
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
                     run: contradictory,
                     checkpoint_type: :run_initialized,
                     wake_at: instance.now
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
                     run: mismatched,
                     checkpoint_type: :run_initialized,
                     wake_at: instance.now
                   },
                   [Fixture.event("some-other-run", 1, instance.now)]
                 )

        assert {:error, :invalid_transition} =
                 transitions.initialize(
                   instance.context,
                   :tenantless,
                   %{
                     run: mismatched,
                     checkpoint_type: :run_initialized,
                     wake_at: instance.now
                   },
                   [:not_an_event]
                 )

        assert {:error, :not_found} =
                 instance.backend.runs().fetch_run(
                   instance.context,
                   :tenantless,
                   mismatched.id
                 )
      end
    end
  end
end
