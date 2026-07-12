# Docket

Docket is an Elixir library for durable, graph-based workflow execution —
built for long-running, interruptible work like agentic LLM sessions, where a
run may pause for a human decision, survive a deploy, and resume exactly where
it left off.

This branch is `0.1.0-dev`. The graph runtime is usable inline, through the
transitional supervised checkpoint driver, and through the assembled
`Docket.Postgres` durable backend. The PostgreSQL bundle owns its stores,
transaction recipes, claim fencing, dispatcher, claimed-run vehicles,
notification fast path, and retention pruner. See the
[PostgreSQL backend guide](docs/architecture/docket-operational-transition-spec.md)
for configuration and operational boundaries.

You describe a workflow as a graph document: nodes that do work, shared state
fields they read and write, and edges (optionally guarded) that decide what
runs next. Docket executes the graph in deterministic supersteps, emitting a
checkpoint at every durable transition boundary.

## Goals

- **Durable by contract.** State changes are proposed at explicit checkpoint
  boundaries. The legacy driver hands them to host storage; the developing
  PostgreSQL backend commits them with a token-and-sequence fence.
- **Deterministic semantics.** Superstep planning, write conflict resolution,
  and guard evaluation are pure and ordered; replanning after a crash produces
  byte-identical task identities, so external effects can be deduplicated.
- **Explicit durable boundaries.** Docket owns execution semantics and the
  private backend storage codec. Applications own graph versioning,
  authorization and UI; the PostgreSQL stores encode opaque state as
  versioned ETF while preserving operational facts in relational columns.
- **One interpreter.** The supervised runtime and the inline test runtime run
  the same loop code — tests exercise real semantics, synchronously, in the
  calling process.

## Inspirations

- **[Pregel](https://research.google/pubs/pregel-a-system-for-large-scale-graph-processing/)** —
  execution proceeds in supersteps with barrier semantics: every node
  activated in a step sees the same committed state snapshot, all writes
  commit together at the step boundary, and same-step conflicts resolve
  deterministically.
- **[LangGraph](https://github.com/langchain-ai/langgraph)** — the
  programming model: a graph of nodes over shared state channels with
  reducers, checkpointing as the durability primitive, and first-class
  human-in-the-loop interrupts.
- **OTP** — where Python-based graph runtimes have to bolt on persistence,
  queues, and schedulers, the BEAM already is the scheduler. Each run is one
  supervised, addressable process; nodes selected in the same superstep execute
  concurrently against one committed snapshot, and node code can execute in an
  additional isolated task process with real timeouts; a million concurrent
  waiting sessions is a normal Tuesday for the runtime.

## Quick start

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

Run it inline (no processes — great for tests and exploration):

```elixir
{:ok, run, checkpoints} = Docket.Test.run_inline(graph, %{"message" => "hello world"})

run.status  #=> :done
run.output  #=> %{"result" => "HELLO WORLD"}
Enum.map(checkpoints, & &1.type)
#=> [:run_initialized, :step_committed, :run_completed]
```

### PostgreSQL facade

`Docket.Postgres` assembles the durable stores, dispatcher, vehicles,
LISTEN/NOTIFY fast path, and retention pruner behind one backend boundary.
The application owns and supervises its Ecto Repo. Retention is explicit so
the library never silently chooses when durable records are deleted:

```elixir
defmodule MyApp.DurableDocket do
  use Docket,
    repo: MyApp.Repo,
    backend: Docket.Postgres,
    tenant_mode: :required,
    checkpoint_observers: [MyApp.DocketObserver],
    pruner: [
      interval_ms: :timer.hours(1),
      event_retention_ms: :timer.hours(24 * 30),
      run_retention_ms: :timer.hours(24 * 90),
      batch_size: 1_000
    ]
end

{:ok, graph_ref} = MyApp.DurableDocket.save_graph(graph)

{:ok, run} =
  MyApp.DurableDocket.start_run(graph_ref, input,
    tenant_id: account.id,
    metadata: %{"workflow_id" => workflow.id}
  )

{:ok, run} = MyApp.DurableDocket.fetch_run(run.id, tenant_id: account.id)
{:ok, info} = MyApp.DurableDocket.inspect_run(run.id, tenant_id: account.id)
```

`save_graph` snapshots node configuration schemas, materializes their defaults,
and validates and compiles the effective graph before storing its canonical,
content-addressed document. `start_run` accepts only the returned stable
reference, fetches the saved document, and compiles it on the executing node;
later schema defaults are never injected into that retained graph version, and
starting a run never republishes the graph. Compiled runtime graphs are
node-local and ephemeral. The planned production vehicle compiles once per
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
tolerate lost or duplicate delivery should consume retained events instead of
observer callbacks. The host-owned `checkpoint:` committer remains on this
branch and is planned for removal from the final v0.1.0 production facade.

### Planned migration from 0.0.1

The production-boundary break does not replace the graph programming model.
Node modules and graph definitions carry over unchanged, including
`Docket.Node`, `Docket.Graph`, `Docket.Schema`, reducers, interrupts, and
executors. `Docket.Test.run_inline` and its related processless helpers remain
the PostgreSQL-free graph-semantics testing surface. Run persistence is
backend-private. The v0.0.1 host Run map codec is not part
of v0.1.0 and there is no compatibility decoder or dual-write path.

The intended cutover is:

1. Drain or terminate active `0.0.1` runs and stop old writers.
2. Delete the host checkpoint committer and Docket-specific host tables.
3. Install Docket's migration and configure `repo:` plus
   `backend: Docket.Postgres`.
4. Publish graphs with `save_graph` and retain the returned `GraphRef`.
5. Replace `run` with `start_run`, `get_run` with `fetch_run` or
   `inspect_run`, and remove host-owned `resume` orchestration.
6. Use observers only for best-effort after-commit notifications; consume
   retained events when delivery must survive crashes.

Because `0.0.1` storage is application-defined, Docket cannot provide one
universal database migration. The supported path is an explicit
drain-and-cut-over rather than a transparent dual-driver period.

## Human-in-the-loop interrupts

A node pauses the run by returning an interrupt naming the state field the
answer should land in:

```elixir
def call(state, _config, _context) do
  case state["decision"] do
    nil -> {:interrupt, %Docket.Interrupt{prompt: "Approve this draft?", resume_channel: "decision"}}
    decision -> {:ok, %{"applied" => decision}}
  end
end
```

The run checkpoints as `:waiting` and its process sits idle — a paused
agentic session is just a cheap BEAM process (or no process at all: you can
let it finish and resume later). When the human answers:

```elixir
{:ok, run} = MyApp.Docket.resolve_interrupt(run_id, interrupt_id, "approved")
```

The value is validated against the interrupt's schema (if any), written to
the resume field, and the interrupted node re-executes in the next superstep
with the answer visible in its state.

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

## Current supervised architecture

Each runtime instance is one supervision tree:

```text
MyApp.Docket (Supervisor, :one_for_all)
├── Registry            — run_id → runtime process; pids never leave the library
├── Task.Supervisor     — isolated node execution, async checkpoint delivery
└── DynamicSupervisor   — one Docket.Runtime process per active run
    ├── Docket.Runtime (run "a3f…")
    └── Docket.Runtime (run "9c1…")
```

This shape is what makes durable agentic sessions natural on the BEAM:

- **A session is a process.** Each run is owned by exactly one lightweight
  GenServer that drives the loop on self-scheduled ticks, staying responsive
  to `get_run` and `resolve_interrupt` between supersteps. Thousands of
  concurrent sessions are just thousands of processes.
- **Isolation where it matters.** With `Docket.Executor.Task`, node code runs
  in a separate supervised task process — a hung LLM call gets a real
  timeout, and a crashing node fails one activation, not the runtime.
- **Crash recovery is resume.** Because every committed transition was
  checkpointed to host storage first, the failure story has one answer at
  every level: node crash → retry policy; runtime crash → resume from the
  last checkpoint; whole-tree restart → resume from the last checkpoint.
- **No external orchestrator.** No queue, no scheduler service, no polling
  loop. The BEAM's preemptive scheduler runs the sessions; your database
  holds the truth.

## Current boundaries

Docket deliberately leaves these to the host application:

- Authorization and ownership checks before calling a Docket facade.
- Graph publish workflows and application-level version selection.
- UI projections for editors and live run views.
- External effects — nodes call your code; Docket never talks to the network.

On `0.1.0-dev`, the host also owns persistence when using the legacy supervised
driver. The developing PostgreSQL backend owns its tables and tenant scoping,
but it is not operational until its bundle and vehicle are assembled.

## Installation

Docket is not yet published to Hex. Add it as a git dependency:

```elixir
def deps do
  [
    {:docket, github: "water-cooler-ai/docket"}
  ]
end
```

## Learn more

- [examples/parent-app-integration.md](examples/parent-app-integration.md) —
  the target durable integration boundary, marked where PostgreSQL assembly is
  still pending.
- [examples/llm-node.md](examples/llm-node.md) — a generic, configurable LLM
  node implementation.
- [docs/architecture/](docs/architecture/) — design rationale: the graph
  document contract, compiler, execution contract, and runtime background.
- Module docs — `Docket`, `Docket.Graph`, `Docket.Run`, `Docket.Node`,
  `Docket.Checkpoint`, and `Docket.Test` are the authoritative API reference.
