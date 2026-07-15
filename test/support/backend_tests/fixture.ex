defmodule Docket.BackendTests.Fixture do
  @moduledoc false

  alias Docket.{DurableCodec, Event, Graph, Run}

  @spec id(Docket.BackendTests.subject(), String.t()) :: String.t()
  def id(%{namespace: namespace}, suffix), do: "#{namespace}-#{suffix}"

  @spec graph(Docket.BackendTests.subject(), String.t(), map()) :: {Graph.t(), String.t()}
  def graph(instance, suffix, metadata \\ %{}) do
    graph = Graph.new!(id: id(instance, suffix), metadata: metadata)
    {graph, hash(graph)}
  end

  @spec hash(Graph.t()) :: String.t()
  def hash(graph) do
    :graph
    |> DurableCodec.encode!(graph)
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @spec run(Docket.BackendTests.subject(), String.t(), Graph.t(), String.t(), keyword()) ::
          Run.t()
  def run(instance, suffix, graph, graph_hash, opts \\ []) do
    now = Keyword.get(opts, :now, instance.now)

    %Run{
      id: id(instance, suffix),
      graph_id: graph.id,
      graph_hash: graph_hash,
      status: Keyword.get(opts, :status, :running),
      input: Keyword.get(opts, :input, %{}),
      started_at: Keyword.get(opts, :started_at, now),
      updated_at: Keyword.get(opts, :updated_at, now),
      checkpoint_seq: Keyword.get(opts, :checkpoint_seq, 1),
      event_seq: Keyword.get(opts, :event_seq, 0)
    }
  end

  @spec event(Run.t() | String.t(), pos_integer(), DateTime.t(), keyword()) :: Event.t()
  def event(run_or_id, seq, now, opts \\ []) do
    run_id = if is_struct(run_or_id, Run), do: run_or_id.id, else: run_or_id

    %Event{
      run_id: run_id,
      seq: seq,
      type: Keyword.get(opts, :type, :node_completed),
      step: Keyword.get(opts, :step, seq),
      timestamp: now,
      payload: Keyword.get(opts, :payload, %{}),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  @spec publish_graph(
          Docket.BackendTests.subject(),
          Docket.Backend.owner_scope(),
          String.t(),
          map()
        ) ::
          {Graph.t(), String.t()}
  def publish_graph(instance, owner_scope, suffix, metadata \\ %{}) do
    {graph, graph_hash} = graph(instance, suffix, metadata)

    :ok =
      instance.backend.graphs().save_graph(
        instance.context,
        owner_scope,
        graph.id,
        graph_hash,
        graph
      )

    {graph, graph_hash}
  end

  @spec insert_run(Docket.BackendTests.subject(), Docket.Backend.owner_scope(), Run.t()) ::
          {:ok, Run.t()}
  def insert_run(instance, owner_scope, run) do
    instance.backend.runs().insert_run(
      instance.context,
      owner_scope,
      run,
      :run_initialized,
      instance.now
    )
  end

  @spec initialize(Docket.BackendTests.subject(), Docket.Backend.owner_scope(), Run.t(), [
          Event.t()
        ]) ::
          Docket.Backend.transaction_result()
  def initialize(instance, owner_scope, run, events \\ []) do
    backend = instance.backend
    runs = backend.runs()
    event_store = backend.events()

    backend.transaction(instance.context, fn tx ->
      with {:ok, initialized} <-
             runs.insert_run(tx, owner_scope, run, :run_initialized, instance.now),
           :ok <- event_store.append_events(tx, owner_scope, run.id, events) do
        {:ok, initialized}
      end
    end)
  end

  @spec claim(Docket.BackendTests.subject(), keyword()) ::
          Docket.Backend.RunStore.claim_lease()
  def claim(instance, opts \\ []) do
    policy = %{
      now: Keyword.get(opts, :now, instance.now),
      limit: 1,
      orphan_ttl_ms: Keyword.get(opts, :orphan_ttl_ms, 60_000),
      max_claim_attempts: Keyword.get(opts, :max_claim_attempts, 3),
      preference: Keyword.get(opts, :preference)
    }

    {:ok, %{leases: [lease], poisoned: []}} =
      instance.backend.runs().claim_due(instance.context, :system, policy)

    lease
  end

  @spec proposal(Run.t(), Docket.Backend.RunStore.claim_token(), keyword()) ::
          Docket.Backend.RunStore.commit_proposal()
  def proposal(run, token, opts \\ []) do
    %{
      run: run,
      expected_checkpoint_seq:
        Keyword.get(opts, :expected_checkpoint_seq, run.checkpoint_seq - 1),
      claim_token: token,
      checkpoint_type: Keyword.get(opts, :checkpoint_type, :step_committed),
      schedule: Keyword.get(opts, :schedule, :retain_claim)
    }
  end
end
