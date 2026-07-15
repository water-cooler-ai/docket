# Docket

Docket is an Elixir library for durable, graph-based workflow execution —
built for long-running, interruptible work like agentic LLM sessions, where a
run may pause for an external resolution, survive a deploy, and resume exactly
where it left off.

This branch is `0.1.0-dev`. Graph semantics are usable processlessly through
`Docket.Test`; supervised production requires the assembled `Docket.Postgres`
durable backend. The PostgreSQL bundle owns its stores,
transaction recipes, claim fencing, dispatcher, claimed-run vehicles,
notification fast path, and retention pruner. See the
[PostgreSQL backend guide](docs/architecture/docket-operational-transition-spec.md)
for configuration and operational boundaries.

You describe a workflow as a graph document: nodes that do work, shared state
fields they read and write, and edges (optionally guarded) that decide what
runs next. Docket executes the graph in deterministic supersteps, emitting a
checkpoint at every durable transition boundary.

## Delivery and execution guarantees

Docket guarantees one atomic durable winner for each committed run transition:
the claim-fenced run update, schedule change, and retained events commit
together in PostgreSQL. That guarantee ends at the database transaction
boundary. A node attempt that proposes the transition may execute more than
once after a crash, timeout, or claim steal, even though only one proposal can
commit. External effects therefore require a cooperating idempotency scheme
when duplicates are unacceptable.

Checkpoint observers, notifications, and telemetry are best effort and are
not business-delivery mechanisms. Retained events are durable facts during
their configured retention period, but exporting or consuming them is a
separate delivery boundary. See the
[delivery and execution guarantees](docs/delivery-guarantees.md) for the full
matrix, partition behavior, and integration rules.

## Docket.Postgres quickstart

This path starts a durable, tenantless runtime in an existing Ecto application.
It uses polling only, which keeps the first setup independent of a dedicated
LISTEN connection; notifications can be enabled later without changing
correctness.

### 1. Install Docket and its optional PostgreSQL dependencies

Until 0.1.0 is published to Hex, pin the release branch:

```elixir
def deps do
  [
    {:docket, github: "water-cooler-ai/docket", branch: "v0.1.0"},
    {:ecto_sql, "~> 3.10"},
    {:postgrex, "~> 0.17"}
  ]
end
```

```sh
mix deps.get
mix docket.gen.migration -r MyApp.Repo
mix ecto.migrate -r MyApp.Repo
```

The generated host migration delegates to Docket's pinned schema version.
Commit that migration with the application; do not call the migration module
directly from application startup.

### 2. Configure and supervise one durable runtime

Retention is required and explicit. These example values retain events for 30
days and terminal runs for 90 days:

```elixir
defmodule MyApp.DurableDocket do
  use Docket,
    repo: MyApp.Repo,
    backend: Docket.Postgres,
    tenant_mode: :none,
    notifier: :none,
    pruner: [
      interval_ms: :timer.hours(1),
      event_retention_ms: :timer.hours(24 * 30),
      run_retention_ms: :timer.hours(24 * 90),
      batch_size: 1_000
    ]
end
```

Start the Repo before Docket in the application's supervision tree:

```elixir
children = [
  MyApp.Repo,
  MyApp.DurableDocket
]

Supervisor.start_link(children, strategy: :one_for_one)
```

### 3. Define a node and graph

Define a node — a module that declares its config schema and does one unit of
work against the shared state:

```elixir
defmodule MyApp.Nodes.Shout do
  @behaviour Docket.Node

  @impl true
  def config_schema do
    Docket.Schema.object(%{
      "from" => Docket.Schema.string(required: true),
      "to" => Docket.Schema.string(required: true)
    })
  end

  @impl true
  def call(state, config, _context) do
    {:ok, %{config["to"] => String.upcase(state[config["from"]])}}
  end
end
```

Build a graph document:

```elixir
graph =
  Docket.Graph.new!(id: "shout")
  |> Docket.Graph.put_input!("message", schema: Docket.Schema.string(), required: true)
  |> Docket.Graph.put_field!("result", schema: Docket.Schema.string())
  |> Docket.Graph.put_node!("shout",
    implementation: MyApp.Nodes.Shout,
    config: %{from: "message", to: "result"}
  )
  |> Docket.Graph.put_edge!("edge_start_shout", from: "$start", to: "shout")
  |> Docket.Graph.put_edge!("edge_shout_finish", from: "shout", to: "$finish")
  |> Docket.Graph.put_output!("result", [])
```

An authored graph is a plain document, so it round-trips through any JSON codec
for storage in an editor or transmission over the wire. Executable node
implementations map through an explicit host registry of stable string
identifiers, so decoding never creates atoms from the document:

```elixir
registry = %{"myapp.shout" => MyApp.Nodes.Shout}

json =
  graph
  |> Docket.Graph.to_map(implementations: registry)
  |> Jason.encode!()

{:ok, graph} =
  json
  |> Jason.decode!()
  |> Docket.Graph.from_map(implementations: registry)
```

This is the editable *authored* graph; it carries no `GraphRef` hash. Only
`save_graph` materializes node defaults and hashes the *effective* graph into a
content-addressed reference, so re-saving after a node's defaults change may
produce a different effective reference even when the authored document is
unchanged. Docket takes no JSON dependency — the host owns encode/decode.

Before publishing, the same graph can run processlessly in a unit test:

```elixir
{:ok, run, checkpoints} = Docket.Test.run_inline(graph, %{"message" => "hello world"})

run.status  #=> :done
run.output  #=> %{"result" => "HELLO WORLD"}
Enum.map(checkpoints, & &1.type)
#=> [:run_initialized, :step_committed, :run_completed]
```

### 4. Publish and start a durable run

`Docket.Postgres` assembles the durable stores, dispatcher, vehicles,
LISTEN/NOTIFY fast path, and retention pruner behind one backend boundary.
The application owns and supervises its Ecto Repo. Publish an immutable graph
version, then start work from the returned reference:

```elixir
{:ok, graph_ref} = MyApp.DurableDocket.save_graph(graph)

{:ok, run} =
  MyApp.DurableDocket.start_run(graph_ref, %{"message" => "hello world"})

{:ok, finished} = MyApp.DurableDocket.await_run(run.id, timeout: 5_000)
finished.status #=> :done
finished.output #=> %{"result" => "HELLO WORLD"}

{:ok, committed} = MyApp.DurableDocket.fetch_run(run.id)
{:ok, operational} = MyApp.DurableDocket.inspect_run(run.id)

{:ok, latest_ref} = MyApp.DurableDocket.fetch_latest_graph_ref("shout")
{:ok, effective_graph} = MyApp.DurableDocket.fetch_graph(latest_ref)

{:ok, versions} = MyApp.DurableDocket.list_graph_versions("shout", limit: 100)
versions.versions
versions.next_before
versions.has_more?
```

`start_run` returns after the initialized run is durably committed; production
advancement is asynchronous. `await_run` is a bounded convenience for callers
that need to wait until a run pauses or terminates. Use `fetch_run` for the last
committed graph state and `inspect_run` for scheduling and poison health.

Use the tenant-scoped collection reader to discover runs without maintaining a
second run-ID index in the host application. It returns lightweight summaries
newest first, using the immutable `{started_at, run_id}` pair as its cursor:

```elixir
{:ok, page} =
  MyApp.DurableDocket.list_runs(
    graph_id: "shout",
    status: [:running, :waiting],
    limit: 100
  )

page.runs
page.next_before
page.has_more?

{:ok, latest} = MyApp.DurableDocket.fetch_latest_run(graph_id: "shout")
```

Pass `before: page.next_before` to continue. Graph reads are tenant-owned just
like run reads: `fetch_latest_graph_ref/1` resolves the newest distinct version
for an ID, `list_graph_versions/2` pages retained version metadata newest first,
and `fetch_graph/1` reads only the exact `GraphRef` supplied by the caller.
A `GraphRef` is relative to the resolved tenant scope and never acts as an
authorization credential.

To read a run's history, page its retained events in ascending sequence order:

```elixir
{:ok, page} = MyApp.DurableDocket.list_events(run.id, after_seq: 0, limit: 250)

page.events            # this page, ascending by sequence
page.next_after_seq    # cursor for the next page
page.has_more?         # whether more retained events follow
```

For point reads, use `fetch_event(run.id, seq)` or
`fetch_latest_event(run.id)`. The latter means the latest *retained* event and
returns `{:ok, nil}` when the run is visible but retention has removed its
entire event history; a missing or wrong-tenant run still returns
`{:error, :not_found}`.

Pass `tenant_id:` under `tenant_mode: :required`; a wrong tenant and an unknown
run both return `{:error, :not_found}`. This is the durable repair source for
observer gaps: `checkpoint_observers:` run best-effort after commit and may drop
or duplicate, but the reader exposes what durably committed within the retained
window. Sequence
gaps are normal — persistence filtering and retention pruning both leave holes,
so pages are never contiguous. `oldest_available_seq`/`latest_available_seq`
report the retained window, while `latest_seq` is the run's latest committed
event sequence regardless of retention, so a fully pruned history is detectable
as `latest_seq > 0` with `latest_available_seq == nil`.

For multi-tenant applications, configure `tenant_mode: :required` and pass a
non-empty `tenant_id` to every run, read, and signal call. See the
[parent-application example](examples/parent-app-integration.md). To enable the
LISTEN/NOTIFY latency fast path, remove `notifier: :none`; deployments behind
PgBouncer transaction or statement pooling must give the notifier a direct or
session-pooled connection. Polling always remains the correctness path.

`save_graph` snapshots node configuration schemas, materializes their defaults,
and validates and compiles the effective graph before storing its canonical,
content-addressed document. `start_run` accepts only the returned stable
reference, fetches the saved document, and compiles it on the executing node;
later schema defaults are never injected into that retained graph version, and
starting a run never republishes the graph. Compiled runtime graphs are
node-local and ephemeral. The production vehicle compiles once per
claim and reuses that value while draining supersteps. Applications must keep node code and
retained checkpoints compatible across deploys, drain old vehicles, or use
versioned node modules when behavior must remain fixed. Cyclic graphs may run
without a superstep limit; hosts may optionally configure `max_supersteps`, or
publish a graph policy when the limit should be part of graph identity. The
backend-neutral durable facade also provides
`resolve_interrupt`, `cancel_run`,
`retry_poisoned_run`, and bounded `await_run`. `tenant_mode: :none` permits
only tenantless rows; `tenant_mode: :required` requires a non-empty
`tenant_id` before storage access. Durable `checkpoint_observers:` run after
commit, are best-effort, and cannot veto state. Durable consumers that cannot
tolerate lost or duplicate delivery should export retained events with a
durable cursor and idempotent downstream handling instead of using observer
callbacks. Production `checkpoint:` configuration is rejected;
`Docket.Test` returns read-only checkpoint values for processless semantic
assertions; those values cannot affect execution.

### Migration from 0.0.1

The production-boundary break does not replace the graph programming model.
Node modules and graph definitions carry over unchanged, including
`Docket.Node`, `Docket.Graph`, `Docket.Schema`, reducers, interrupts, and
executors. `Docket.Test.run_inline` and its related processless helpers remain
the PostgreSQL-free graph-semantics testing surface. Run persistence is
backend-private. The v0.0.1 host Run map codec is not part
of v0.1.0 and there is no compatibility decoder or dual-write path.

Durable integration tests use the production lifecycle and PostgreSQL stores.
Configure `testing: :inline` to commit `start_run` and named signals in the
caller and synchronously drain due work to its next park, without starting a
dispatcher, notifier, vehicle task, or pruner. Configure `testing: :manual` to
disable automatic advancement and call
`MyApp.DurableDocket.drain_runs(max_runs: 100)`
after starts, signals, or manual clock changes. The bound prevents cyclic
graphs that remain immediately due from hanging a test. Both modes keep one
production `Docket.Lifecycle`/storage transaction per logical moment; neither
wraps node execution or a whole drain in a Docket transaction. An SQL Sandbox
owner transaction may still surround the test itself.

The intended cutover is:

1. Drain or terminate active `0.0.1` runs and stop old writers.
2. Delete the host checkpoint committer and Docket-specific host tables.
3. Install Docket's migration and configure `repo:` plus
   `backend: Docket.Postgres`.
4. Publish graphs with `save_graph` and retain the returned `GraphRef`.
5. Replace `run` with `start_run`, `get_run` with `fetch_run` or
   `inspect_run`, and remove host-owned `resume` orchestration.
6. Use observers only for best-effort after-commit notifications; export
   retained events with a durable cursor and idempotent downstream handling
   when delivery must survive crashes.

Because `0.0.1` storage is application-defined, Docket cannot provide one
universal database migration. The supported path is an explicit
drain-and-cut-over rather than a transparent dual-driver period.

## External interrupts

A node pauses the run by returning an interrupt naming the state field where
the external resolution should be written:

```elixir
def call(state, _config, _context) do
  case state["decision"] do
    nil -> {:interrupt, %Docket.Interrupt{resume_channel: "decision"}}
    decision -> {:ok, %{"applied" => decision}}
  end
end
```

The run commits as `:waiting` without retaining a per-run process. When the
external system resolves it:

```elixir
{:ok, run} =
  MyApp.DurableDocket.resolve_interrupt(run_id, interrupt_id, "approved")
```

The value is validated against the interrupt's schema (if any), written to
the resume field, and the interrupted node re-executes in the next superstep
with the resolved value visible in its state.

## Execution model

A run advances in Pregel-style supersteps:

1. **Plan** — from committed state only, select the activated nodes and build
   their task descriptors (deterministic IDs and idempotency keys).
2. **Execute** — dispatch each activation through the configured executor.
   Nodes see the same committed snapshot; nothing observes a same-step write.
3. **Commit** — validate writes against field schemas, apply reducers,
   resolve same-step conflicts in sorted node order, evaluate edge guards and
   fan-in barriers, and commit the step atomically with a checkpoint.

Edges carry the control flow: fan-out (multiple edges from one node), fan-in
joins (multi-source barrier edges that fire when every source has completed),
guarded branches (durable, serializable guard expressions over state), and
cycles (optionally bounded by a graph or host `max_supersteps` policy). Node failures retry per node
policy; a permanently failed superstep commits none of its writes.

Durable graphs and run state use a private versioned deterministic ETF codec;
the compiler canonicalizes and validates the effective graph before its exact
ETF bytes are hashed once and stored. Graph hashing is private, and every run
records the published graph ID and hash it was started from. Recovery validates
strict collection key/value shapes and fails closed on malformed stored terms.

## Production architecture

Each instance supervises one backend bundle and shared execution resources:

```text
MyApp.DurableDocket (Supervisor, :one_for_all)
├── Docket.Postgres     — stores, dispatcher, vehicles, notifier, pruner
├── Runtime.Instance    — immutable facade configuration
└── Task.Supervisor     — isolated node execution and observer delivery
```

Backend vehicles claim persisted work and execute nodes in isolated supervised
tasks. Claim fencing protects commits, and recovery reclaims persisted runs
after process or node failure without a host-owned resume path.

## Current boundaries

Docket deliberately leaves these to the host application:

- Authorization and ownership checks before calling a Docket facade.
- Graph publish workflows and application-level version selection.
- UI projections for editors and live run views.
- External effects — nodes call your code; Docket never talks to the network.

The configured backend exclusively owns graph/run persistence, scheduling,
recovery, signals, and production supervision.

## Package status

Docket 0.1.0 is not yet published to Hex. The quickstart pins the active
release branch; after publication, prefer the Hex requirement documented on
the package page.

## Learn more

- [Backend conformance guide](docs/backend-conformance.md) — the reusable,
  core-only ExUnit contract for third-party backend implementations.
- [examples/parent-app-integration.md](examples/parent-app-integration.md) —
  the durable parent-application integration boundary.
- [0.0.1 to 0.1.0 migration guide](docs/architecture/migration-0.0.1-to-0.1.0.md).
- [PostgreSQL operations and correctness guide](docs/postgres-operations.md) —
  statuses, claims, poison recovery, configuration, and inspection.
- [examples/llm-node.md](examples/llm-node.md) — a generic, configurable LLM
  node implementation.
- [docs/architecture/](docs/architecture/) — design rationale: the graph
  document contract, compiler, execution contract, and runtime background.
- Module docs — `Docket`, `Docket.Graph`, `Docket.Run`, `Docket.Node`,
  `Docket.Checkpoint`, and `Docket.Test` are the authoritative API reference.
