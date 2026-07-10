# Docket

Docket is an Elixir library for durable, graph-based workflow execution —
built for long-running, interruptible work like agentic LLM sessions, where a
run may pause for a human decision, survive a deploy, and resume exactly where
it left off.

This README describes the `0.0.x` core runtime line. The transition toward the
`0.1.0` operational runtime with the `Docket.Postgres` backend is documented in
[docs/architecture/docket-operational-transition-spec.md](docs/architecture/docket-operational-transition-spec.md).

You describe a workflow as a graph document: nodes that do work, shared state
fields they read and write, and edges (optionally guarded) that decide what
runs next. Docket executes the graph in deterministic supersteps, emitting a
checkpoint after every committed transition. Your application persists those
checkpoints; Docket can rebuild a live run from the last one at any time.

## Goals

- **Durable by contract.** The source of truth for a run is the last accepted
  checkpoint in *your* storage, not memory in a process. Kill the node,
  redeploy, resume.
- **Deterministic semantics.** Superstep planning, write conflict resolution,
  and guard evaluation are pure and ordered; replanning after a crash produces
  byte-identical task identities, so external effects can be deduplicated.
- **Host-owned boundaries.** Docket owns execution semantics. Your application
  owns persistence, graph versioning, authorization, tenancy, and UI. Graphs
  and runs are plain documents with a JSON-safe wire format, so they store
  anywhere.
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
  supervised, addressable process; node code can execute in an isolated task
  process with real timeouts; a million concurrent waiting sessions is a
  normal Tuesday for the runtime.

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

Or run it supervised. Implement a checkpoint handler (your persistence
boundary) and a runtime module, and add the runtime to your supervision tree:

```elixir
defmodule MyApp.DocketCheckpoint do
  @behaviour Docket.Checkpoint

  @impl true
  def handle(%Docket.Checkpoint{run: run}, _context) do
    MyApp.Workflows.upsert_run!(run.id, Docket.Run.to_map(run))
    :ok
  end
end

defmodule MyApp.Docket do
  use Docket, checkpoint: MyApp.DocketCheckpoint
end

# in your application supervision tree
children = [MyApp.Docket]
```

```elixir
{:ok, run} = MyApp.Docket.run(graph, %{"message" => "hello world"})
{:ok, live} = MyApp.Docket.get_run(run.id)
```

`run/3` returns once the run is durably initialized — the synchronous
`:run_initialized` checkpoint has been accepted by your handler — and
execution continues in a supervised process. Progress arrives through
checkpoints (the durable truth); `get_run/2` reads the live in-memory
snapshot while the run is active.

To resume after a crash, restart, or deploy, load what you persisted and hand
it back:

```elixir
run = Docket.Run.from_map!(stored_run_map)
{:ok, run} = MyApp.Docket.resume(graph, run)
```

### Durable backend facade

A durable host configures one compatible backend bundle rather than mixing
transaction and store modules:

```elixir
defmodule MyApp.DurableDocket do
  use Docket,
    storage: MyApp.DocketBackend,
    tenant_mode: :required,
    checkpoint_observers: [MyApp.DocketObserver]
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

`save_graph` validates and compiles the graph before storing its canonical,
content-addressed document. `start_run` accepts only the returned stable
reference, fetches the saved document, and compiles it for execution; starting
a run never republishes the graph. The operational facade also provides
`resolve_interrupt`, `cancel_run`,
`retry_poisoned_run`, and bounded `await_run`. `tenant_mode: :none` permits
only tenantless rows; `tenant_mode: :required` requires a non-empty
`tenant_id` before storage access. Durable `checkpoint_observers:` run after
commit, are best-effort, and cannot veto state. The legacy `checkpoint:`
callback remains the veto-capable committer for the storage-free supervised
driver. Durable consumers that cannot tolerate lost or duplicate delivery
should consume retained events instead of observer callbacks.

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
cycles (bounded by a `max_supersteps` policy). Node failures retry per node
policy; a permanently failed superstep commits none of its writes.

Everything that crosses a boundary is a document. `Docket.Graph` and
`Docket.Run` both serialize to a canonical JSON-safe wire format; graphs are
content-hashed, and every run records the graph ID and hash it was started
from, so a resume against the wrong graph version is rejected.

## Built on OTP

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

## What Docket does not do

Docket deliberately leaves to the host application:

- Persistence — checkpoints hand you the run document; you store it.
- Graph versioning and publish workflows.
- Authorization, tenancy, and ownership (attach identity via run `metadata`).
- UI projections for editors and live run views.
- External effects — nodes call your code; Docket never talks to the network.

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
  wiring Docket runs to your users, accounts, and database rows.
- [examples/llm-node.md](examples/llm-node.md) — a generic, configurable LLM
  node implementation.
- [docs/architecture/](docs/architecture/) — design rationale: the graph
  document contract, compiler, execution contract, and runtime background.
- Module docs — `Docket`, `Docket.Graph`, `Docket.Run`, `Docket.Node`,
  `Docket.Checkpoint`, and `Docket.Test` are the authoritative API reference.
