# Docket

[![Hex Version](https://img.shields.io/hexpm/v/docket.svg)](https://hex.pm/packages/docket)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/docket)
[![CI](https://github.com/water-cooler-ai/docket/actions/workflows/ci.yml/badge.svg)](https://github.com/water-cooler-ai/docket/actions/workflows/ci.yml)
[![License](https://img.shields.io/hexpm/l/docket.svg)](https://github.com/water-cooler-ai/docket/blob/main/LICENSE)

Durable, graph-based workflow execution for Elixir — built for long-running,
interruptible work like agentic LLM sessions, where a run may pause for an
external resolution, survive a deploy, and resume exactly where it left off.

You describe a workflow as a graph document: nodes that do work, shared state
fields they read and write, and edges (optionally guarded) that decide what
runs next. Docket executes the graph in deterministic supersteps and commits
a checkpoint at every durable transition.

## Features

- **Durable supersteps** — runs advance in Pregel-style plan/execute/commit
  steps; the claim-fenced run update, schedule change, and retained events
  commit together in one PostgreSQL transaction.
- **External interrupts** — a node parks its run as `:waiting` with no live
  process; when the outside world answers, `resolve_interrupt` writes the
  value and the run continues in the next superstep.
- **Crash-safe recovery** — claim fencing admits exactly one durable winner
  per transition, and recovery reclaims persisted runs after a process or
  node failure without host-owned resume code.
- **Graphs as data** — an authored graph is a plain document that round-trips
  through any JSON codec; publishing stores an immutable, content-addressed
  version, and every run records the graph ID and hash it started from.
- **Rich control flow** — fan-out, fan-in barriers, guarded branches, and
  cycles with optional superstep bounds; node failures retry per node policy.
- **Processless testing** — `Docket.Test.run_inline/2` exercises full graph
  semantics in a unit test with no processes and no PostgreSQL.
- **Multi-tenant fairness** — optional tenant scoping with claim policies
  that admit due work breadth-first across tenants, so one tenant cannot
  starve another.
- **Small core** — the core depends only on `telemetry`; the PostgreSQL
  backend compiles when the host already uses `ecto_sql` and `postgrex`, and
  Docket takes no JSON dependency.

Docket guarantees one atomic durable winner for each committed run
transition — and is explicit about where that guarantee ends. A node attempt
that proposes a transition may execute more than once after a crash, timeout,
or claim steal, even though only one proposal commits, so external effects
need a cooperating idempotency scheme. Checkpoint observers, notifications,
and telemetry are best effort. See the
[delivery and execution guarantees](docs/delivery-guarantees.md) for the full
matrix.

## Requirements

- Elixir 1.18+.
- The graph core and `Docket.Test` have no database requirement.
- The durable backend requires PostgreSQL 13 or newer through the optional
  `ecto_sql ~> 3.10` and `postgrex ~> 0.17` dependencies.

## Installation

Add Docket and, for the durable backend, its optional PostgreSQL
dependencies:

```elixir
def deps do
  [
    {:docket, "~> 0.1.0"},
    {:ecto_sql, "~> 3.10"},
    {:postgrex, "~> 0.17"}
  ]
end
```

Then install the tables through a host-owned migration:

```sh
mix deps.get
mix docket.gen.migration -r MyApp.Repo
mix ecto.migrate -r MyApp.Repo
```

The generated migration delegates to Docket's pinned schema version. Commit
it with the application; do not call the migration module directly from
application startup.

## Quickstart

This starts a durable, tenantless runtime in an existing Ecto application,
using polling only; the LISTEN/NOTIFY fast path can be enabled later without
changing correctness. Retention is required and explicit:

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

Define a node — a module that declares its config schema and does one unit
of work against the shared state:

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

Build a graph document wiring the node between `$start` and `$finish`:

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

Before publishing, the same graph can run processlessly in a unit test:

```elixir
{:ok, run, checkpoints} = Docket.Test.run_inline(graph, %{"message" => "hello world"})

run.status  #=> :done
run.output  #=> %{"result" => "HELLO WORLD"}
Enum.map(checkpoints, & &1.type)
#=> [:run_initialized, :step_committed, :run_completed]
```

Publish an immutable graph version, then start durable work from the
returned reference:

```elixir
{:ok, graph_ref} = MyApp.DurableDocket.save_graph(graph)

{:ok, run} =
  MyApp.DurableDocket.start_run(graph_ref, %{"message" => "hello world"})

{:ok, finished} = MyApp.DurableDocket.await_run(run.id, timeout: 5_000)
finished.status #=> :done
finished.output #=> %{"result" => "HELLO WORLD"}
```

`start_run` returns after the initialized run is durably committed;
production advancement is asynchronous, and `await_run` is a bounded
convenience for callers that need to wait until a run pauses or terminates.
The facade also provides paged run, event, and graph-version readers plus
`fetch_run`, `inspect_run`, `cancel_run`, and `retry_poisoned_run`; the
`Docket` module docs are the authoritative API reference.

Multi-tenant applications configure `tenant_mode: :required` with a fair
claim policy such as `Docket.Postgres.ClaimPolicy.WindowedInterleave`, and
durable integration tests use the production lifecycle through
`testing: :inline` or `testing: :manual`. See the
[parent-application example](examples/parent-app-integration.md) and the
[PostgreSQL operations guide](docs/postgres-operations.md).

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

## Current boundaries

Docket deliberately leaves these to the host application:

- Authorization and ownership checks before calling a Docket facade.
- Graph publish workflows and application-level version selection.
- UI projections for editors and live run views.
- External effects — nodes call your code; Docket never talks to the network.

The configured backend exclusively owns graph/run persistence, scheduling,
recovery, signals, and production supervision.

## Learn more

- [PostgreSQL operations and correctness guide](docs/postgres-operations.md) —
  statuses, claims, poison recovery, configuration, testing modes, and
  inspection.
- [Delivery and execution guarantees](docs/delivery-guarantees.md) — the full
  guarantee matrix, partition behavior, and integration rules.
- [examples/parent-app-integration.md](examples/parent-app-integration.md) —
  the durable parent-application integration boundary.
- [examples/llm-node.md](examples/llm-node.md) — a generic, configurable LLM
  node implementation.
- [0.0.1 to 0.1.0 migration guide](docs/architecture/migration-0.0.1-to-0.1.0.md).
- [Backend test guide](docs/backend-conformance.md) — the shared source test
  suite and backend-specific coverage boundary.
- [Telemetry](docs/telemetry.md) and [benchmarks](docs/benchmarks.md) —
  operational signals and non-oracle regression measurements.
- [Future roadmap](docs/future-roadmap.md) — future features, improvements,
  investigations, and research.
- [Architecture guide index](docs/architecture/README.md) — design rationale:
  the graph document contract, compiler, execution contract, and runtime
  background.
- Module docs — `Docket`, `Docket.Graph`, `Docket.Run`, `Docket.Node`,
  `Docket.Checkpoint`, and `Docket.Test` are the authoritative API reference.
