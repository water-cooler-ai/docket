defmodule Docket.PublicFacadeTest do
  use ExUnit.Case, async: false

  defmodule Host do
    use Docket, backend: Docket.Test.MemoryBackend
  end

  defmodule TenantHost do
    use Docket, backend: Docket.Test.MemoryBackend, tenant_mode: :required
  end

  defmodule InvalidClaimPolicyAdmin do
    def get_default(_context), do: {:ok, :backend_private_value}
    def put_default(_context, _maximum, _opts), do: :backend_private_value

    def put_override(_context, _owner_scope, _maximum, _opts),
      do: {:ok, :backend_private_value}

    def reset_override(_context, _owner_scope, _opts), do: {:ok, :backend_private_value}
    def get_effective(_context, _owner_scope), do: {:ok, :backend_private_value}
  end

  defmodule InvalidCapabilityBackend do
    @behaviour Docket.Backend

    defdelegate transaction(context, fun), to: Docket.Test.MemoryBackend
    defdelegate context(opts), to: Docket.Test.MemoryBackend
    defdelegate child_spec(opts, context), to: Docket.Test.MemoryBackend
    defdelegate drain_runs(context, opts), to: Docket.Test.MemoryBackend

    def graphs, do: Docket.Test.MemoryBackend
    def runs, do: Docket.Test.MemoryBackend
    def events, do: Docket.Test.MemoryBackend
    def claim_policy_admin, do: InvalidClaimPolicyAdmin
  end

  defmodule InvalidCapabilityHost do
    use Docket,
      backend: InvalidCapabilityBackend,
      claim_policy: [
        implementation: Docket.Postgres.ClaimPolicy.TenantFair,
        default_max_active_runs: 1
      ]
  end

  defmodule LegacyCapabilityHost do
    use Docket, backend: InvalidCapabilityBackend
  end

  test "the 0.0.1 production facade is not exported" do
    assert Code.ensure_loaded?(Docket)
    assert Code.ensure_loaded?(Host)

    for {name, arity} <- [run: 3, run: 4, resume: 3, resume: 4, get_run: 2, get_run: 3] do
      refute function_exported?(Docket, name, arity)
    end

    for {name, arity} <- [run: 2, run: 3, resume: 2, resume: 3, get_run: 1, get_run: 2] do
      refute function_exported?(Host, name, arity)
    end
  end

  test "the durable facade and processless helpers remain exported" do
    assert Code.ensure_loaded?(Docket)
    assert Code.ensure_loaded?(Docket.Test)

    for {name, arity} <- [
          save_graph: 3,
          fetch_graph: 3,
          fetch_latest_graph_ref: 3,
          list_graph_versions: 3,
          start_run: 4,
          fetch_run: 3,
          inspect_run: 3,
          list_runs: 2,
          fetch_latest_run: 2,
          fetch_event: 4,
          fetch_latest_event: 3,
          list_events: 3,
          await_run: 3,
          resolve_interrupt: 5,
          cancel_run: 3,
          retry_poisoned_run: 3
        ] do
      assert function_exported?(Docket, name, arity)
    end

    assert function_exported?(Host, :list_events, 1)
    assert function_exported?(Host, :list_events, 2)

    for {name, arities} <- [
          fetch_graph: [1, 2],
          fetch_latest_graph_ref: [1, 2],
          list_graph_versions: [1, 2],
          list_runs: [0, 1],
          fetch_latest_run: [0, 1],
          fetch_event: [2, 3],
          fetch_latest_event: [1, 2]
        ],
        arity <- arities do
      assert function_exported?(Host, name, arity)
    end

    for {name, arity} <- [run_inline: 3, resume_inline: 3, step_inline: 2] do
      assert function_exported?(Docket.Test, name, arity)
    end
  end

  test "claim-policy administration freezes the top-level surface and omits unsupported wrappers" do
    for {name, arities} <- [
          fetch_claim_policy_default: [1],
          put_claim_policy_default: [2, 3],
          put_claim_policy_override: [3, 4],
          reset_claim_policy_override: [2, 3],
          inspect_claim_policy: [2]
        ],
        arity <- arities do
      assert function_exported?(Docket, name, arity)
    end

    for {name, arities} <- [
          fetch_claim_policy_default: [0],
          put_claim_policy_default: [1, 2],
          put_claim_policy_override: [2, 3],
          reset_claim_policy_override: [1, 2],
          inspect_claim_policy: [1]
        ],
        arity <- arities do
      refute function_exported?(Host, name, arity)
      refute function_exported?(LegacyCapabilityHost, name, arity)
    end
  end

  test "top-level claim-policy calls return one stable error for an unsupported backend" do
    start_supervised!(Host)

    for result <- [
          Docket.fetch_claim_policy_default(Host),
          Docket.put_claim_policy_default(Host, 2),
          Docket.put_claim_policy_override(Host, {:tenant, "acme"}, 2),
          Docket.reset_claim_policy_override(Host, {:tenant, "acme"}),
          Docket.inspect_claim_policy(Host, {:tenant, "acme"})
        ] do
      assert {:error,
              %Docket.Error{
                type: :unsupported_capability,
                details: %{capability: :claim_policy_administration}
              }} = result
    end
  end

  test "claim-policy validation is backend-neutral and runs before delegation" do
    assert {:error, :invalid_max_active_runs} =
             Docket.put_claim_policy_default(Host, 0)

    assert {:error, :invalid_owner_scope} =
             Docket.put_claim_policy_override(Host, :system, 2)

    assert {:error, :invalid_expected_version} =
             Docket.reset_claim_policy_override(Host, :tenantless, expected_version: -1)

    assert {:error, :invalid_options} =
             Docket.put_claim_policy_default(Host, 2, actor: "not-supported")
  end

  test "a malformed capability result cannot leak through the public facade" do
    start_supervised!(InvalidCapabilityHost)

    for result <- [
          InvalidCapabilityHost.fetch_claim_policy_default(),
          InvalidCapabilityHost.put_claim_policy_default(2),
          InvalidCapabilityHost.put_claim_policy_override(:tenantless, 2),
          InvalidCapabilityHost.reset_claim_policy_override(:tenantless),
          InvalidCapabilityHost.inspect_claim_policy(:tenantless)
        ] do
      assert {:error,
              %Docket.Error{
                type: :invalid_backend_capability,
                details: %{capability: :claim_policy_administration}
              }} = result
    end
  end

  describe "read APIs through a configured memory-backend runtime" do
    setup do
      start_supervised!(Host)
      {:ok, defaults} = Docket.Runtime.Instance.defaults(Host)

      %{
        backend: Keyword.fetch!(defaults, :backend),
        context: Keyword.fetch!(defaults, :backend_context)
      }
    end

    test "fetches exact graphs and lists their scoped version references" do
      graph = Docket.Test.Fixtures.Graphs.minimal_linear()

      assert {:ok, ref} = Host.save_graph(graph)
      assert {:ok, saved} = Host.fetch_graph(ref)
      assert saved.id == graph.id

      assert {:ok, ^ref} = Host.fetch_latest_graph_ref(graph.id)

      assert {:ok, %Docket.GraphVersionPage{versions: [%Docket.GraphVersion{} = version]}} =
               Host.list_graph_versions(graph.id)

      assert version.ref == ref

      assert {:error, :not_found} = Host.fetch_latest_graph_ref("missing")

      assert {:error, %Docket.Error{type: :invalid_graph_reference}} =
               Host.fetch_graph("")

      assert {:error, %Docket.Error{type: :invalid_graph_id}} =
               Host.list_graph_versions("")

      for result <- [
            Host.fetch_graph(ref, %{}),
            Host.fetch_latest_graph_ref(graph.id, %{}),
            Host.list_graph_versions(graph.id, %{})
          ] do
        assert {:error, %Docket.Error{type: :invalid_options}} = result
      end

      malformed_ref = %Docket.GraphRef{graph_id: nil, graph_hash: nil}

      assert {:error, %Docket.Error{type: :invalid_graph_reference}} =
               Host.start_run(malformed_ref, %{})
    end

    test "lists and fetches latest run summaries with stable pagination", %{
      backend: backend,
      context: context
    } do
      insert_run!(backend, context, :tenantless, run("older", ~U[2026-07-12 00:00:00Z]))
      insert_run!(backend, context, :tenantless, run("newer-a", ~U[2026-07-12 01:00:00Z]))

      insert_run!(
        backend,
        context,
        :tenantless,
        run("newer-b", ~U[2026-07-12 01:00:00Z], graph_id: "other")
      )

      assert {:ok, %Docket.RunPage{} = first} = Host.list_runs(limit: 2)
      assert Enum.map(first.runs, & &1.id) == ["newer-b", "newer-a"]
      assert first.has_more?

      assert {:ok, second} = Host.list_runs(limit: 2, before: first.next_before)
      assert Enum.map(second.runs, & &1.id) == ["older"]
      refute second.has_more?

      assert {:ok, %Docket.RunSummary{id: "newer-b"}} = Host.fetch_latest_run()

      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.fetch_latest_run(limit: 1)

      for result <- [
            Host.list_runs(%{}),
            Host.fetch_latest_run(%{}),
            Host.list_runs([:not_a_keyword]),
            Host.fetch_latest_run([:not_a_keyword])
          ] do
        assert {:error, %Docket.Error{type: :invalid_options}} = result
      end

      assert {:ok, filtered} = Host.list_runs(graph_id: "g", status: :running)
      assert Enum.map(filtered.runs, & &1.id) == ["newer-a", "older"]

      for opts <- [
            [limit: 0],
            [before: "bad"],
            [status: :created],
            [status: []],
            [graph_id: ""],
            [graph_hash: ""]
          ] do
        assert {:error, %Docket.Error{type: :invalid_options}} = Host.list_runs(opts)
      end
    end

    test "fetches exact and latest retained events and distinguishes an empty history", %{
      backend: backend,
      context: context
    } do
      populated = run("populated", ~U[2026-07-12 00:00:00Z], event_seq: 2)
      empty = run("empty", ~U[2026-07-12 00:01:00Z])
      insert_run!(backend, context, :tenantless, populated)
      insert_run!(backend, context, :tenantless, empty)

      events = [event("populated", 1), event("populated", 2)]
      assert :ok = backend.append_events(context, :tenantless, populated.id, events)

      assert {:ok, %Docket.Event{seq: 1}} = Host.fetch_event(populated.id, 1)
      assert {:ok, %Docket.Event{seq: 2}} = Host.fetch_latest_event(populated.id)
      assert {:ok, nil} = Host.fetch_latest_event(empty.id)
      assert {:error, :not_found} = Host.fetch_event(populated.id, 3)
      assert {:error, :not_found} = Host.fetch_latest_event("missing")

      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.fetch_event(populated.id, 0)
    end
  end

  test "run collections enforce required tenant scope" do
    start_supervised!(TenantHost)
    {:ok, defaults} = Docket.Runtime.Instance.defaults(TenantHost)
    backend = Keyword.fetch!(defaults, :backend)
    context = Keyword.fetch!(defaults, :backend_context)

    insert_run!(backend, context, {:tenant, "a"}, run("a-run", ~U[2026-07-12 00:00:00Z]))
    insert_run!(backend, context, {:tenant, "b"}, run("b-run", ~U[2026-07-12 00:01:00Z]))
    assert :ok = backend.append_events(context, {:tenant, "a"}, "a-run", [event("a-run", 1)])

    assert {:error, %Docket.Error{type: :invalid_tenant}} = TenantHost.list_runs()
    assert {:ok, page} = TenantHost.list_runs(tenant_id: "a")
    assert Enum.map(page.runs, & &1.id) == ["a-run"]
    assert {:ok, %Docket.RunSummary{id: "b-run"}} = TenantHost.fetch_latest_run(tenant_id: "b")

    assert {:ok, %Docket.Event{seq: 1}} = TenantHost.fetch_latest_event("a-run", tenant_id: "a")
    assert {:error, :not_found} = TenantHost.fetch_event("a-run", 1, tenant_id: "b")

    graph = Docket.Test.Fixtures.Graphs.minimal_linear()
    assert {:error, %Docket.Error{type: :invalid_tenant}} = TenantHost.save_graph(graph)
    assert {:ok, graph_ref} = TenantHost.save_graph(graph, tenant_id: "a")
    assert {:ok, %Docket.Graph{}} = TenantHost.fetch_graph(graph_ref, tenant_id: "a")
    assert {:error, :not_found} = TenantHost.fetch_graph(graph_ref, tenant_id: "b")
    assert {:ok, ^graph_ref} = TenantHost.fetch_latest_graph_ref(graph.id, tenant_id: "a")
    assert {:error, :not_found} = TenantHost.fetch_latest_graph_ref(graph.id, tenant_id: "b")

    assert {:ok, %{versions: [%{ref: ^graph_ref}]}} =
             TenantHost.list_graph_versions(graph.id, tenant_id: "a")

    assert {:ok, %{versions: []}} =
             TenantHost.list_graph_versions(graph.id, tenant_id: "b")

    assert {:ok, ^graph_ref} = TenantHost.save_graph(graph, tenant_id: "b")
    assert {:ok, %Docket.Graph{}} = TenantHost.fetch_graph(graph_ref, tenant_id: "b")
  end

  describe "list_events through a configured memory-backend runtime" do
    setup do
      start_supervised!(Host)
      {:ok, defaults} = Docket.Runtime.Instance.defaults(Host)

      %{
        backend: Keyword.fetch!(defaults, :backend),
        context: Keyword.fetch!(defaults, :backend_context)
      }
    end

    test "rejects invalid options before reaching storage" do
      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.list_events("run", after_seq: -1)

      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.list_events("run", limit: 0)

      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.list_events("run", limit: 1001)

      assert {:error, %Docket.Error{type: :invalid_options}} =
               Host.list_events("run", after_seq: "0")
    end

    test "reads a page of retained events", %{backend: backend, context: context} do
      run = %Docket.Run{
        id: "run",
        graph_id: "g",
        graph_hash: String.duplicate("ab", 32),
        status: :running,
        input: %{},
        checkpoint_seq: 1,
        event_seq: 2,
        started_at: ~U[2026-07-12 00:00:00Z],
        updated_at: ~U[2026-07-12 00:00:00Z]
      }

      run = publish_for_run!(backend, context, :tenantless, run)

      events =
        for seq <- 1..2 do
          %Docket.Event{
            run_id: "run",
            seq: seq,
            type: :node_completed,
            step: seq,
            timestamp: ~U[2026-07-12 00:00:00Z],
            payload: %{},
            metadata: %{}
          }
        end

      assert {:ok, :done} =
               backend.transaction(context, fn tx ->
                 with {:ok, _} <-
                        backend.insert_run(
                          tx,
                          :tenantless,
                          run,
                          :run_initialized,
                          ~U[2026-07-12 00:00:00Z]
                        ),
                      :ok <- backend.append_events(tx, :tenantless, "run", events) do
                   {:ok, :done}
                 end
               end)

      assert {:ok, %Docket.EventPage{} = page} = Host.list_events("run", after_seq: 0, limit: 1)
      assert [%Docket.Event{seq: 1}] = page.events
      assert page.next_after_seq == 1
      assert page.has_more?
      assert page.oldest_available_seq == 1
      assert page.latest_available_seq == 2
      assert page.latest_seq == 2

      assert {:error, :not_found} = Host.list_events("missing")
    end
  end

  defp insert_run!(backend, context, scope, run) do
    run = publish_for_run!(backend, context, scope, run)

    assert {:ok, ^run} =
             backend.insert_run(context, scope, run, :run_initialized, run.started_at)

    run
  end

  defp publish_for_run!(backend, context, scope, run) do
    graph = %{Docket.Test.Fixtures.Graphs.minimal_linear() | id: run.graph_id}
    {:ok, effective, rtg} = Docket.Graph.Compiler.compile_for_publication(graph)
    :ok = backend.save_graph(context, scope, rtg.graph_id, rtg.graph_hash, effective)
    %{run | graph_hash: rtg.graph_hash}
  end

  defp run(id, started_at, opts \\ []) do
    %Docket.Run{
      id: id,
      graph_id: Keyword.get(opts, :graph_id, "g"),
      graph_hash: Keyword.get(opts, :graph_hash, String.duplicate("ab", 32)),
      status: :running,
      input: %{},
      checkpoint_seq: 1,
      event_seq: Keyword.get(opts, :event_seq, 0),
      started_at: started_at,
      updated_at: started_at
    }
  end

  defp event(run_id, seq) do
    %Docket.Event{
      run_id: run_id,
      seq: seq,
      type: :checkpoint_committed,
      step: seq,
      timestamp: ~U[2026-07-12 00:00:00Z],
      payload: %{},
      metadata: %{}
    }
  end
end
