defmodule Docket.LifecycleTest do
  use Docket.Test.Case, async: false

  @moduletag capture_log: true

  alias Docket.Runtime.{Loop, Moment}
  alias Docket.Test.MemoryBackend

  defmodule RecordingObserver do
    @behaviour Docket.Checkpoint.Observer

    @impl true
    def observe(checkpoint, %{application: %{notify: pid}}) do
      send(pid, {:observed, checkpoint})
      :ok
    end

    def observe(_checkpoint, _context), do: :ok
  end

  defmodule FailingObserver do
    @behaviour Docket.Checkpoint.Observer

    @impl true
    def observe(_checkpoint, %{application: %{notify: pid}}) do
      send(pid, :failing_observer_called)
      {:error, :observer_down}
    end

    def observe(_checkpoint, _context), do: {:error, :observer_down}
  end

  defmodule BlockingObserver do
    @behaviour Docket.Checkpoint.Observer

    @impl true
    def observe(_checkpoint, %{application: %{notify: pid}}) do
      send(pid, {:blocking_observer_started, self()})

      receive do
        :release -> :ok
      end
    end
  end

  defmodule MutableDefaultsNode do
    @behaviour Docket.Node

    @impl true
    def config_schema do
      calls = Process.get({__MODULE__, :calls}, 0)
      Process.put({__MODULE__, :calls}, calls + 1)

      fields = %{
        "from" => Docket.Schema.string(required: true),
        "to" => Docket.Schema.string(required: true),
        "tone" => Docket.Schema.string(default: Process.get({__MODULE__, :tone}, "calm"))
      }

      fields =
        if Process.get({__MODULE__, :add_future_default}, false) do
          Map.put(fields, "future", Docket.Schema.string(default: "new-release"))
        else
          fields
        end

      Docket.Schema.object(fields)
    end

    @impl true
    def call(state, config, _context) do
      {:ok, %{config["to"] => Map.get(state, config["from"])}}
    end
  end

  defmodule Host do
    use Docket,
      backend: Docket.Test.MemoryBackend,
      checkpoint_observers: [FailingObserver, RecordingObserver]
  end

  defmodule TenantHost do
    use Docket,
      backend: Docket.Test.MemoryBackend,
      tenant_mode: :required
  end

  defmodule BlockingHost do
    use Docket,
      backend: Docket.Test.MemoryBackend,
      checkpoint_observers: [BlockingObserver]
  end

  setup do
    start_supervised!(Host)
    start_supervised!(TenantHost)
    start_supervised!(BlockingHost)
    :ok
  end

  test "durable facade atomically starts, fetches, inspects, cancels, and observes" do
    assert {:ok, reference} = Host.save_graph(Graphs.minimal_linear())

    assert {:ok, started} =
             Host.start_run(reference, %{"value" => "hello"}, context: %{notify: self()})

    assert started.status == :running
    assert {:ok, ^started} = Host.fetch_run(started.id)

    assert {:ok, %Docket.RunInfo{run: ^started, wake_at: %DateTime{}}} =
             Host.inspect_run(started.id)

    assert_receive {:observed, %Docket.Checkpoint{type: :run_initialized}}
    assert_receive :failing_observer_called

    assert {:ok, cancelled} = Host.cancel_run(started.id, context: %{notify: self()})

    assert cancelled.status == :cancelled
    assert {:ok, ^cancelled} = Host.await_run(started.id, timeout: 0)
    assert_receive {:observed, %Docket.Checkpoint{type: :run_cancelled}}
    assert_receive :failing_observer_called
  end

  test "publication materializes schema defaults before hashing and local compilation" do
    graph = graph_with_mutable_default()
    {backend, context} = backend_ref(Host)

    Process.put({MutableDefaultsNode, :tone}, "calm")
    Process.put({MutableDefaultsNode, :calls}, 0)
    assert {:ok, calm_ref} = Host.save_graph(graph)
    assert Process.get({MutableDefaultsNode, :calls}) == 1

    assert {:ok, calm_graph} =
             backend.graphs().fetch_graph(
               context,
               :tenantless,
               calm_ref.graph_id,
               calm_ref.graph_hash
             )

    assert calm_graph.nodes["copy"].config["tone"] == "calm"
    assert {:ok, calm_runtime} = Docket.ensure_compiled_effective(calm_graph, [])
    assert calm_runtime.graph_hash == calm_ref.graph_hash

    Process.put({MutableDefaultsNode, :tone}, "bright")
    assert {:ok, bright_ref} = Host.save_graph(graph)
    refute bright_ref.graph_hash == calm_ref.graph_hash

    assert calm_runtime.nodes["node:copy"].config["tone"] == "calm"

    Process.put({MutableDefaultsNode, :calls}, 0)
    assert {:ok, _started} = Host.start_run(calm_ref, %{"value" => "hello"})
    assert Process.get({MutableDefaultsNode, :calls}) == 1
  end

  test "local compilation never injects defaults introduced after publication" do
    graph = graph_with_mutable_default()
    {backend, context} = backend_ref(Host)

    Process.put({MutableDefaultsNode, :add_future_default}, false)
    assert {:ok, reference} = Host.save_graph(graph)

    assert {:ok, effective} =
             backend.graphs().fetch_graph(
               context,
               :tenantless,
               reference.graph_id,
               reference.graph_hash
             )

    refute Map.has_key?(effective.nodes["copy"].config, "future")

    Process.put({MutableDefaultsNode, :add_future_default}, true)

    assert {:ok, ordinary} = Docket.ensure_compiled(effective, [])
    assert ordinary.nodes["node:copy"].config["future"] == "new-release"

    assert {:ok, pinned} = Docket.ensure_compiled_effective(effective, [])
    refute Map.has_key?(pinned.nodes["node:copy"].config, "future")
    assert pinned.graph_hash == reference.graph_hash

    assert {:ok, started} = Host.start_run(reference, %{"value" => "hello"})
    assert started.graph_hash == reference.graph_hash
  end

  test "explicit config wins over changing schema defaults" do
    graph =
      graph_with_mutable_default()
      |> update_in(
        [Access.key!(:nodes), "copy", Access.key!(:config)],
        &Map.put(&1, :tone, "fixed")
      )

    Process.put({MutableDefaultsNode, :tone}, "calm")
    assert {:ok, first} = Host.save_graph(graph)
    Process.put({MutableDefaultsNode, :tone}, "bright")
    assert {:ok, second} = Host.save_graph(graph)

    assert first == second
  end

  test "invalid materialized defaults fail before graph storage" do
    {_backend, context} = backend_ref(Host)

    for invalid_default <- [123, {:not, :json}] do
      Process.put({MutableDefaultsNode, :tone}, invalid_default)

      assert {:error, %Docket.Error{type: :invalid_graph}} =
               Host.save_graph(graph_with_mutable_default())
    end

    assert Agent.get(context, & &1.graphs) == %{}
  end

  test "durable resolve loads the stored graph and commits mutation plus events" do
    graph = Graphs.interrupt_review()
    assert {:ok, reference} = Host.save_graph(graph)
    assert {:ok, started} = Host.start_run(reference, %{})
    {backend, context} = backend_ref(Host)
    {:ok, rtg} = Docket.ensure_compiled(graph, [])

    claim_now = DateTime.add(started.updated_at, 1, :second)

    assert {:ok, %{leases: [lease], poisoned: []}} =
             backend.runs().claim_due(context, :system, %{
               now: claim_now,
               limit: 1,
               orphan_ttl_ms: 60_000,
               max_claim_attempts: 3
             })

    assert {:ok, %Moment{} = waiting_moment} =
             Loop.propose_advance(rtg, started, clock: fn -> claim_now end)

    assert waiting_moment.run.status == :waiting

    [event | events] = waiting_moment.events
    invalid_moment = %{waiting_moment | events: [%{event | run_id: "other"} | events]}

    assert {:error, :event_run_mismatch} =
             Docket.Lifecycle.commit_moment(
               {backend, context},
               :tenantless,
               invalid_moment,
               started.checkpoint_seq,
               lease.claim_token
             )

    assert {:ok, ^started} = backend.runs().fetch_run(context, :tenantless, started.id)

    assert {:ok, ^waiting_moment} =
             Docket.Lifecycle.commit_moment(
               {backend, context},
               :tenantless,
               waiting_moment,
               started.checkpoint_seq,
               lease.claim_token
             )

    [interrupt_id] = Map.keys(waiting_moment.run.interrupts)

    assert {:ok, resolved} = Host.resolve_interrupt(waiting_moment.run.id, interrupt_id, "yes")
    assert resolved.status == :running
    assert resolved.interrupts[interrupt_id].status == :resolved
    assert {:ok, ^resolved} = Host.fetch_run(resolved.id)

    types = Enum.map(MemoryBackend.events(context, :tenantless, resolved.id), & &1.type)
    assert :interrupt_resolved in types
    assert Enum.count(types, &(&1 == :checkpoint_committed)) == 3
  end

  test "tenant mode is resolved before storage and scopes never cross" do
    assert {:error, %Docket.Error{type: :invalid_tenant}} =
             TenantHost.save_graph(Graphs.minimal_linear())

    assert {:ok, reference} =
             TenantHost.save_graph(Graphs.minimal_linear(), tenant_id: "a")

    assert {:error, %Docket.Error{type: :invalid_tenant}} =
             TenantHost.start_run(reference, %{"value" => "x"})

    assert {:ok, tenant_run} =
             TenantHost.start_run(reference, %{"value" => "x"}, tenant_id: "a")

    assert {:ok, ^tenant_run} = TenantHost.fetch_run(tenant_run.id, tenant_id: "a")
    assert {:error, :not_found} = TenantHost.fetch_run(tenant_run.id, tenant_id: "b")
    assert {:error, :not_found} = TenantHost.inspect_run(tenant_run.id, tenant_id: "b")
    assert {:error, :not_found} = TenantHost.cancel_run(tenant_run.id, tenant_id: "b")

    assert {:error, :not_found} =
             TenantHost.retry_poisoned_run(tenant_run.id, tenant_id: "b")

    assert {:error, :not_found} =
             TenantHost.await_run(tenant_run.id, tenant_id: "b", timeout: 0)

    assert {:error, %Docket.Error{type: :invalid_tenant}} =
             Host.fetch_run(tenant_run.id, tenant_id: "a")
  end

  test "graph save is explicit and idempotent, and forged references do not start" do
    graph = Graphs.minimal_linear()
    assert {:ok, first} = Host.save_graph(graph)

    assert {:ok, ^first} =
             Host.save_graph(graph,
               backend: __MODULE__.NotABackend,
               backend_context: :not_the_instance_context
             )

    assert {:ok, ^first} = Host.save_graph(graph)

    forged = %{first | graph_hash: String.duplicate("0", 64)}
    assert {:error, :not_found} = Host.start_run(forged, %{"value" => "x"})

    refute function_exported?(Host, :run, 2)
  end

  test "a blocking observer cannot delay a committed API result" do
    assert {:ok, reference} = BlockingHost.save_graph(Graphs.minimal_linear())

    assert {:ok, run} =
             BlockingHost.start_run(reference, %{"value" => "x"}, context: %{notify: self()})

    assert run.status == :running
    assert_receive {:blocking_observer_started, observer}
    send(observer, :release)
  end

  test "poisoned await halts immediately and retry clears operational poison" do
    assert {:ok, reference} = Host.save_graph(Graphs.minimal_linear())
    assert {:ok, run} = Host.start_run(reference, %{"value" => "x"})
    {_backend, context} = backend_ref(Host)
    :ok = MemoryBackend.poison(context, run.id)

    assert {:error, {:poisoned, %Docket.RunInfo{run: ^run}}} =
             Host.await_run(run.id, timeout: 1_000)

    assert {:ok, ^run} = Host.retry_poisoned_run(run.id)

    assert {:ok, %Docket.RunInfo{poisoned_at: nil, poison_reason: nil}} =
             Host.inspect_run(run.id)
  end

  test "lifecycle owns the complete disposition to schedule mapping" do
    at = ~U[2026-07-10 21:00:00Z]

    assert Docket.Lifecycle.schedule(:continue, :claimed) == :retain_claim
    assert Docket.Lifecycle.schedule(:continue, :unclaimed) == {:release_claim, :immediate}

    assert Docket.Lifecycle.schedule({:park, :immediate, :yield}, :claimed) ==
             {:release_claim, :immediate}

    assert Docket.Lifecycle.schedule({:park, {:at, at}, :retry}, :claimed) ==
             {:release_claim, {:at, at}}

    assert Docket.Lifecycle.schedule({:park, :external, :waiting}, :claimed) ==
             {:release_claim, :external}

    assert Docket.Lifecycle.schedule({:park, :terminal, :done}, :claimed) ==
             {:release_claim, :terminal}
  end

  test "lifecycle rolls back run/event start and signal failures without publishing graphs" do
    graph = Graphs.minimal_linear()
    {:ok, rtg} = Docket.ensure_compiled(graph, [])
    {backend, context} = backend_ref(Host)

    fresh = Loop.build_initial_run(rtg, %{"value" => "x"}, run_id: "bad-start")
    {:ok, start_moment} = Loop.propose_init(rtg, fresh, [])
    [first | rest] = start_moment.events
    invalid_start = %{start_moment | events: [%{first | run_id: "other"} | rest]}

    assert {:ok, reference} = Host.save_graph(graph)

    assert {:error, :event_run_mismatch} =
             Docket.Lifecycle.start({backend, context}, :tenantless, invalid_start)

    assert {:error, :not_found} = backend.runs().fetch_run(context, :system, "bad-start")

    assert {:ok, %Docket.Graph{}} =
             backend.graphs().fetch_graph(context, :tenantless, graph.id, rtg.graph_hash)

    assert {:ok, started} = Host.start_run(reference, %{"value" => "x"})

    assert {:error, :event_run_mismatch} =
             Docket.Lifecycle.signal({backend, context}, :tenantless, started.id, fn run ->
               {:ok, moment} =
                 Docket.Runtime.RunMutation.cancel_run(run, ~U[2026-07-10 22:00:00Z])

               [event | events] = moment.events
               {:ok, %{moment | events: [%{event | run_id: "other"} | events]}}
             end)

    assert {:ok, ^started} = backend.runs().fetch_run(context, :tenantless, started.id)
  end

  defp backend_ref(host) do
    {:ok, defaults} = Docket.Runtime.Instance.defaults(host)
    {Keyword.fetch!(defaults, :backend), Keyword.fetch!(defaults, :backend_context)}
  end

  defp graph_with_mutable_default do
    graph = Graphs.minimal_linear()
    put_in(graph.nodes["copy"].implementation.module, MutableDefaultsNode)
  end
end
