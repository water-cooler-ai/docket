# Parent App Integration Example

Durable Docket runs can reference app-owned users, accounts, and workflow rows
without making the parent application a second persistence driver.

## Configure one backend

```elixir
defmodule MyApp.Docket do
  use Docket,
    repo: MyApp.Repo,
    backend: Docket.Postgres,
    tenant_mode: :required,
    claim_policy: [
      implementation: Docket.Postgres.ClaimPolicy.WindowedInterleave
    ],
    checkpoint_observers: [MyApp.DocketProjection],
    pruner: [
      interval_ms: :timer.hours(1),
      event_retention_ms: :timer.hours(24 * 30),
      run_retention_ms: :timer.hours(24 * 90),
      batch_size: 1_000
    ]
end
```

Start the Repo before the facade in the application's supervision tree:

```elixir
children = [MyApp.Repo, MyApp.Docket]
Supervisor.start_link(children, strategy: :one_for_one)
```

`Docket.Postgres` owns graph/run persistence, claiming,
scheduling, and cold recovery. The private ETF state stored by the existing
PostgreSQL stores is not an application wire format.

## Publish, then start

Publish an effective graph once and retain its stable reference on the
application's workflow record. Given an application-built `graph` and workflow
record:

```elixir
tenant_id = to_string(workflow.account_id)
{:ok, graph_ref} = MyApp.Docket.save_graph(graph, tenant_id: tenant_id)

workflow =
  MyApp.Workflows.update_graph_ref!(workflow, %{
    graph_id: graph_ref.graph_id,
    graph_hash: graph_ref.graph_hash
  })
```

Keep graph input separate from app-owned metadata when starting a run:

```elixir
def run_workflow(user, workflow, attrs) do
  # Authorization must establish that this workflow belongs to the user's
  # account before its ID becomes the durable storage scope.
  tenant_id = to_string(user.account_id)

  graph_ref = %Docket.GraphRef{
    graph_id: workflow.graph_id,
    graph_hash: workflow.graph_hash
  }

  metadata = %{
    "user_id" => user.id,
    "account_id" => user.account_id,
    "workflow_id" => workflow.id
  }

  with {:ok, run} <-
         MyApp.Docket.start_run(graph_ref, Map.new(attrs),
           tenant_id: tenant_id,
           metadata: metadata
         ) do
    MyApp.Workflows.link_docket_run!(workflow, run.id)
  end
end
```

`input` is workflow data. `metadata` is durable app context that Docket
preserves but does not interpret. Applications should keep metadata terms
portable and free of processes, references, ports, and functions.
The linked workflow must remain in the same authorized account; reads and
signals must derive their tenant ID from that trusted ownership relationship,
not from caller-controlled input.

## Read and inspect

Application rows store the Docket run ID and business projections, not a copy
of Docket's durable run state:

```elixir
tenant_id = to_string(workflow.account_id)

{:ok, run} =
  MyApp.Docket.fetch_run(workflow.docket_run_id,
    tenant_id: tenant_id
  )

{:ok, info} =
  MyApp.Docket.inspect_run(workflow.docket_run_id,
    tenant_id: tenant_id
  )
```

`fetch_run` returns the last committed `%Docket.Run{}`. `inspect_run` adds
token-free scheduling and operational health facts.

## Project after commit

Checkpoint observers are suitable for best-effort UI projections and
notifications. They run after the durable transaction and cannot veto it:

```elixir
defmodule MyApp.DocketProjection do
  @behaviour Docket.Checkpoint.Observer

  @impl true
  def observe(%Docket.Checkpoint{run: run}, _context) do
    MyApp.Workflows.project_status(run.id, run.status, run.updated_at)
    :ok
  end
end
```

Observers may be lost or duplicated around a crash. Consumers that require a
durable delivery guarantee should consume retained events rather than treating
the observer as an outbox.

## Important boundaries

- Use a stable, non-empty binary `tenant_id` for storage authorization and
  scope; convert integer application IDs explicitly.
- Store business identity in Run metadata, not graph input.
- Store `run.id` and app projections in parent tables; do not duplicate the
  private Run state blob.
- Publish graphs explicitly and start runs only from the returned `GraphRef`.
- Use `fetch_run`/`inspect_run`; v0.1.0 has no host-owned Run map codec or
  resume orchestration.
- Authorize claim-policy changes in the parent application before calling the
  five public cap operations. Docket does not provide actor persistence, audit
  history, hold/drain states, bulk changes, weights, borrowing, or preemption.
