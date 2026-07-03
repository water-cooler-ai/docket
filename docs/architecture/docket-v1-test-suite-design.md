# Docket: v1 Test Suite Design

Status: reference draft
Date: 2026-06-26

Related documents:

- `docs/architecture/docket-v1-implementation-path.md`
- `docs/architecture/docket-runtime-design.md`
- `docs/architecture/docket-graph-construction-design.md`
- `docs/architecture/docket-graph-execution-contract-design.md`

Implementation note: use `docket-v1-implementation-path.md` as the active v1
build sequence. This document owns the detailed test layers, fixtures, and
coverage matrix.

## 1. Purpose

This document defines the v1 test-suite shape for Docket.

The goal is to test the full path from graph authoring to runtime execution
without depending on host application infrastructure. Docket tests should prove
that:

- users can build and edit canonical `Docket.Graph` documents
- the compiler verifies and lowers those documents into executable runtime
  graphs
- the inline runtime can execute graph semantics in the calling test process
- the supervised Runtime uses the same execution loop as the inline runtime
- checkpoints and execution state can be tested with in-memory and ETS-backed
  test adapters

The suite should start small, deterministic, and contract-focused. By the end of
v1 it should act as Docket's executable specification.

## 2. Testing Principles

v1 tests follow these rules:

- No test requires Ecto, a Repo, Postgres, Redis, Docker, network access, API
  credentials, object storage, or a host app database.
- Execution-state and checkpoint persistence tests use process-local memory or
  ETS tables created by the test.
- Ordinary graph execution tests run through `Docket.Test` in the calling test
  process.
- Supervised tests are reserved for lifecycle behavior that genuinely requires
  processes, registries, task execution, monitors, or crash recovery.
- Tests do not use `Process.sleep/1` as a synchronization mechanism. They use
  messages, monitors, barriers, controlled executors, or inline execution.
- Construction helpers allow incomplete drafts. Compiler tests decide what is
  runnable.
- Public tests assert public structs, graph-attached diagnostics, checkpoints,
  runs, and lowering maps. They do not assert private Runtime process state.
- Runtime algorithm tests may assert internal data only in internal test files.
  Public contract tests should stay stable across implementation refactors.
- Fixtures are ordinary Elixir modules and graph values, not external files,
  services, or seeded database rows.

LangGraph's test suite is useful as a pattern: it compiles small graphs, asserts
graph projections, then runs the compiled graph to prove the builder/runtime
link works. Docket should make that bridge more explicit because
`Docket.Graph` and `Docket.Runtime.Graph` are intentionally separate.

## 3. Test Layers

The v1 suite should be organized around six layers.

### 3.1 Construction Tests

Construction tests cover the public editable graph document.

They verify:

- `Docket.Graph.new/1` creates a canonical draft graph with stable IDs
- `put_input/4`, `put_field/4`, `put_output/4`, `put_node/4`, `put_edge/4`,
  `policy/4`, and metadata helpers update the graph document
- multi-source edges represent fan-in joins
- node-local branch groups preserve guarded outgoing edge groupings
- `update_*` and `delete_*` helpers preserve stable IDs where appropriate
- incomplete drafts are representable without diagnostics until verification
- published graph documents are not mutated in ordinary editing flows
- UI layout and editor projection state are host-owned and not part of Docket
  graph documents
- malformed arguments that cannot be represented as graph data return hard
  errors or raise according to the public API contract

Construction tests should not prove runtime behavior. They prove the public
document can be built, edited, inspected, and saved by a host application.

### 3.2 Compiler Tests

Compiler tests cover `Docket.Graph -> Docket.Runtime.Graph`.

They verify:

- `Docket.Graph.verify/2`, `Docket.Graph.Compiler.verify/2`, and
  `Docket.Graph.Compiler.compile/2` share the same validation rules
- inputs lower to input channels
- state fields lower to state channels with the intended reducer
- outputs lower to output projections
- public nodes lower to `Docket.Runtime.Graph.Node` values
- node config validates against the node behaviour's config schema
- configured state field references validate against graph input/state fields
- simple edges lower to generated ephemeral activation channels
- source runtime graph nodes reference outgoing edges for trigger evaluation
- target nodes subscribe to generated activation channels
- fan-out creates one generated activation channel per target
- multi-source edges lower to the required barrier/all representation
- node branch groups lower through grouped guarded edges and generated activation
  channels
- guarded edges compile durable `Docket.Guard` expressions
- compiler lowering metadata exposes public-to-runtime and runtime-to-public maps
- diagnostics use public IDs and public graph paths whenever possible
- generated channel IDs cannot collide with user-declared IDs
- compile rejects unknown fields, unknown nodes, invalid guards, impossible
  multi-source edge barriers, invalid reducers, unsafe node implementations,
  invalid node config, and cycles without an explicit v1 limit or halt condition

Compiler tests are the bridge suite. They should assert exact lowering shape
where Docket needs a stable internal contract between builder and runtime.

### 3.3 Compiler Integration Tests

Compiler integration tests prove that compiled runtime graphs actually execute.

They follow this shape:

```text
Docket.Graph fixture
  -> Docket.Graph.Compiler.compile/2
  -> Docket.Test.run_inline/3
  -> assertions on Docket.Run, checkpoints, events, and outputs
```

The integration suite should also include direct compiled-runtime entry tests:

```text
Docket.Runtime.Graph fixture
  -> Docket.Test.run_inline/3
  -> assertions on Docket.Run, checkpoints, events, and outputs
```

These tests are where Docket verifies that construction, compiler lowering, and
runtime execution link up correctly.

Examples:

- a simple edge activates the target node exactly once
- fan-out activates both targets from one source
- multi-source edge waits until all upstream nodes have committed
- a guarded edge activates only when source completion produces a candidate and
  the guard sees committed state plus changed fields
- generated edge channels are not writable by user node output
- output projections return the public output shape
- public IDs in events map back through compiler lowering metadata

### 3.4 Inline Execution Tests

Inline execution tests cover graph semantics through `Docket.Test`.

They verify:

- superstep Plan -> Execution -> Update behavior
- barrier visibility: writes from one node are invisible to other nodes in the
  same superstep
- reducer application and write ordering
- checkpoint ordering and checkpoint failure behavior
- idempotency key stability across checkpoint failure: re-planning a superstep
  after a failed checkpoint produces byte-identical idempotency keys, proving
  attempt counters only advance inside committed barriers
- retry policy and max-attempt behavior
- interrupts, interrupt checkpoints, and resume-channel writes
- terminal detection
- max-superstep failures
- resume from a saved `Docket.Run`
- node output validation failures
- guard evaluation failures

Inline tests should be the default for ordinary execution behavior. They are
fast, deterministic, and do not depend on BEAM scheduling.

### 3.5 Supervised Runtime Tests

Supervised tests cover process behavior that inline tests cannot.

They verify:

- `Docket.run/4` starts or locates a Runtime through the registry/supervisor
- only one active Runtime owns a run ID
- `get_run/3` reads only active Runtime state
- finished or evicted runs return `{:error, :not_found}` when no Runtime owns
  them
- Runtime crashes can resume from the latest ETS-backed checkpoint
- task executor completions are correlated by task ID, attempt, and input hash
- stale task completions are ignored
- timeouts become node attempt failures
- checkpoint callback failures block sync checkpoint commits
- async checkpoint delivery failures are observable without blocking the active
  Runtime

Supervised tests may use real processes, monitors, unique registries, and
supervisors. They still must not use external services.

### 3.6 Test Adapter Contract Tests

Adapter contract tests cover the in-test persistence helpers.

They verify:

- memory checkpoint sinks store accepted checkpoints in order
- ETS checkpoint sinks isolate data by test table and run ID
- checkpoint handlers are idempotent by checkpoint sequence or checkpoint ID
- latest-run lookup returns the most recently accepted checkpoint run
- checkpoint history can be listed newest-first and oldest-first if helpers
  expose both
- deleting a test run removes only that run's ETS rows
- ETS helpers work with async ExUnit tests by using unique table names or test
  owner keys

These are test-support contracts, not host-storage contracts. They keep runtime
tests honest without introducing Ecto.

## 4. Baseline Suite To Start v1

The baseline suite is the first set of tests to add as implementation begins.
It should be small enough to keep green while the internals are still forming,
but broad enough to lock the important seams.

### 4.1 Baseline Test Support

Add test support modules first:

```text
test/support/docket_case.ex
test/support/fixtures/graph_fixtures.ex
test/support/fixtures/test_nodes.ex
test/support/checkpoint/memory_sink.ex
test/support/checkpoint/ets_sink.ex
test/support/deterministic_ids.ex
test/support/deterministic_clock.ex
```

Baseline helpers:

- `Docket.Test.Fixtures.Graphs.minimal_linear/0`
- `Docket.Test.Fixtures.Graphs.unknown_config_field/0`
- `Docket.Test.Fixtures.Graphs.unknown_update_field/0`
- `Docket.Test.Fixtures.Graphs.simple_edge/0`
- `Docket.Test.Fixtures.Graphs.fanout/0`
- `Docket.Test.Fixtures.Graphs.multi_source_edge/0`
- `Docket.Test.Fixtures.Graphs.guarded_edge/0`
- `Docket.Test.Checkpoint.MemorySink`
- `Docket.Test.Checkpoint.EtsSink`
- deterministic ID and clock options for compiler/runtime tests

### 4.2 Baseline Construction Tests

Create:

```text
test/docket/graph/graph_test.exs
test/docket/graph/editing_test.exs
test/docket/graph/diagnostics_test.exs
```

Initial assertions:

- a fresh graph has an ID, schema version, empty collections, and empty diagnostics
- adding input, field, node, edge, output updates the graph document
- incomplete graphs are valid draft data but not runnable
- graph construction tests do not require or inspect UI layout metadata
- deleting a node updates only the graph document according to the chosen public
  contract

### 4.3 Baseline Compiler Tests

Create:

```text
test/docket/graph/compiler/validation_test.exs
test/docket/graph/compiler/lowering_test.exs
test/docket/graph/compiler/lowering_metadata_test.exs
```

Initial assertions:

- `minimal_linear/0` compiles
- `unknown_config_field/0` fails with a diagnostic path to the public node config
- `unknown_update_field/0` fails with a diagnostic path to the returned update
- simple edges produce generated activation channels
- source runtime graph nodes expose outgoing edge references
- target runtime graph nodes subscribe to incoming generated edge channels
- compiler lowering metadata maps public node, edge, input, field, and output
  IDs to runtime IDs

### 4.4 Baseline Inline Execution Tests

Create:

```text
test/docket/test/inline_runtime_test.exs
test/docket/runtime/inline_execution_test.exs
test/docket/runtime/checkpoint_order_test.exs
```

Initial assertions:

- `Docket.Test.run_inline/3` runs `minimal_linear/0` in the calling test process
- accepted checkpoints are returned to the test
- inline execution initializes through `Docket.Runtime.Loop.init/3`
- `Loop.init/3` infers fresh versus saved execution from the supplied run
  without an extra caller-supplied flag
- `:run_initialized` is emitted before any node execution
- `:run_completed` is emitted only after terminal detection
- the checkpoint sink receives the same checkpoints returned by the inline
  helper
- `step_inline/2` advances exactly one committed superstep

### 4.5 Baseline ETS Tests

Create:

```text
test/docket/test/ets_checkpoint_sink_test.exs
test/docket/test/ets_run_state_test.exs
```

Initial assertions:

- each test creates a private ETS table or a private owner key
- accepted checkpoints can be inserted, listed, and fetched by run ID
- latest run document is read from the latest checkpoint
- no test starts Ecto or a Repo
- ETS data is cleaned up by `on_exit/1`

## 5. End-of-v1 Suite

By the end of v1, the suite should include these files or equivalent coverage.

```text
test/docket/graph/
  graph_test.exs
  editing_test.exs
  diagnostics_test.exs
  schema_test.exs
  guard_test.exs
  versioning_test.exs

test/docket/graph/compiler/
  validation_test.exs
  lowering_test.exs
  lowering_metadata_test.exs
  generated_id_test.exs
  compile_and_run_test.exs

test/docket/channel/
  last_value_test.exs
  ephemeral_test.exs
  barrier_test.exs
  reducer_validation_test.exs

test/docket/runtime/
  inline_execution_test.exs
  superstep_test.exs
  barrier_visibility_test.exs
  checkpoint_order_test.exs
  checkpoint_failure_test.exs
  interrupt_test.exs
  resume_test.exs
  retry_test.exs
  failure_test.exs
  max_supersteps_test.exs

test/docket/supervised/
  runtime_start_test.exs
  runtime_registry_test.exs
  runtime_lifecycle_test.exs
  task_executor_test.exs
  crash_recovery_test.exs

test/docket/test/
  inline_runtime_test.exs
  fixtures_test.exs
  memory_checkpoint_sink_test.exs
  ets_checkpoint_sink_test.exs
```

End-of-v1 acceptance means `mix test` runs all default tests without external
services. Optional slow or stress tests may be tagged, but they still should not
require external infrastructure.

## 6. Fixture Catalog

Fixtures should be small and named for the behavior they prove.

### 6.1 Graph Fixtures

`minimal_linear/0`

```text
input: value
field: result
start -> copy -> finish
```

Proves the smallest runnable graph and output projection.

`simple_edge/0`

```text
start -> writer -> reviewer -> finish
```

Proves generated edge channel lowering and sequential activation.

`fanout/0`

```text
start -> source
source -> left
source -> right
```

Proves fan-out lowering and same-superstep parallel activations.

`multi_source_edge/0`

```text
start -> source
source -> left
source -> right
edge [left, right] -> combine
```

Proves barrier lowering and multi-source edge execution.

`guarded_edge/0`

```text
start -> fetch
fetch -> premium_step when user.premium_user == true
fetch -> standard_step when user.premium_user == false
```

Proves guard expression compilation and committed-state reads.

`interrupt_review/0`

```text
start -> draft -> review_interrupt -> apply_decision -> finish
```

Proves interrupt creation, resume-channel writes, and resume execution.

`parallel_failure/0`

```text
start -> ok_node
start -> failing_node
edge [ok_node, failing_node] -> should_not_run
```

Proves v1 permanent failure commits no writes from the failed superstep.

`retry_then_continue/0`

```text
start -> flaky -> after_flaky -> finish
```

Proves retry attempts and continuation after success.

`cycle_counter/0`

```text
start -> increment -> decide
decide -> increment while count < limit
decide -> finish when count >= limit
```

Proves cycles, guards, and max-superstep protection.

### 6.2 Node Fixtures

Test nodes should be ordinary modules under `test/support/fixtures`.

They should include:

- `CopyInput`
- `AppendValue`
- `WriteStatic`
- `WriteMultiple`
- `ReadCommittedOnly`
- `InterruptOnce`
- `FlakyThenSucceeds`
- `AlwaysFails`
- `Raises`
- `Exits`
- `Throws`
- `SleepsUntilReleased`

`SleepsUntilReleased` should not use wall-clock sleep. It should wait on a
message or test-controlled barrier so tests can release it deterministically.

### 6.3 Checkpoint Fixtures

Checkpoint fixtures should include:

- memory sink for simple inline tests
- ETS sink for recovery and state lookup tests
- failing sink that returns `{:error, reason}` on a configured checkpoint type
- recording sink that sends `{:checkpoint, checkpoint}` to the test process
- duplicate-tolerant sink for idempotency tests

All sinks should be configurable with a test owner or table name. No sink should
write to Repo, disk, or external services.

## 7. Test Helpers

### 7.1 Public Inline Helpers

`Docket.Test` is the public test-facing execution helper.

```elixir
Docket.Test.run_inline(graph_or_runtime_graph, input, opts \\ [])
Docket.Test.step_inline(run, opts \\ [])
```

Return shape:

```elixir
{:ok, Docket.Run.t(), [Docket.Checkpoint.t()]}
| {:error, Docket.Error.t(), [Docket.Checkpoint.t()]}
```

`run_inline/3` should:

- accept either a canonical `Docket.Graph` or a precompiled
  `Docket.Runtime.Graph`
- compile `Docket.Graph` inputs through the same compiler path used by the
  supervised Runtime
- run precompiled `Docket.Runtime.Graph` inputs directly while preserving the
  same loop execution semantics
- create the initial run document
- initialize execution through `Docket.Runtime.Loop.init/3`, matching the
  supervised Runtime launch path
- rely on `Loop.init/3` to infer fresh execution and emit the required
  `:run_initialized` checkpoint
- execute in the calling test process until terminal, failed, waiting, or step
  limit
- call the configured checkpoint sink
- return only accepted checkpoints

`step_inline/2` should:

- drive one committed superstep
- return after the checkpoint for that step is accepted
- return the updated public run and accepted checkpoint list

The inline helper must call the same compiler, loop, algorithm, reducer,
validation, and checkpoint-building code as the supervised Runtime.

### 7.2 Test-Only Convenience Helpers

Test support may add convenience wrappers around public helpers:

```elixir
compile!(graph, opts \\ [])
run_inline!(graph, input, opts \\ [])
compile_and_run!(graph, input, opts \\ [])
checkpoint_types(checkpoints)
latest_checkpoint(checkpoints)
assert_diagnostic(diagnostics, code, path)
assert_lowered_edge(lowering, public_edge_id, opts)
```

These helpers belong under `test/support`; they are not public Docket API.

### 7.3 Determinism Helpers

Tests should be able to inject:

- ID generator
- monotonic logical clock
- wall-clock timestamp provider
- retry backoff scheduler
- executor module
- checkpoint sink
- max supersteps

Default tests should avoid asserting opaque generated IDs. Where IDs matter,
use deterministic ID options or assert through compiler lowering metadata and
public IDs.

## 8. ETS Guidance

ETS is the default persistence tool for execution-state tests.

Use ETS for:

- checkpoint sink storage
- latest run documents derived from checkpoints
- supervised recovery tests
- idempotency and duplicate checkpoint tests
- small host-like lookup helpers used only in tests

Do not use ETS to smuggle in runtime internals that public code should not see.
The ETS rows should contain public `Docket.Checkpoint`, `Docket.Run`, and event
documents, or test-support metadata around those documents.

Recommended ETS sink behavior:

```text
table key:
  {run_id, checkpoint_seq}

table value:
  %{
    checkpoint: Docket.Checkpoint.t(),
    inserted_at: logical_time,
    owner: test_owner
  }
```

Recommended helper API:

```elixir
Docket.Test.Checkpoint.EtsSink.start_link(opts)
Docket.Test.Checkpoint.EtsSink.handle(checkpoint, context)
Docket.Test.Checkpoint.EtsSink.latest_run(table, run_id)
Docket.Test.Checkpoint.EtsSink.list_checkpoints(table, run_id)
Docket.Test.Checkpoint.EtsSink.delete_run(table, run_id)
```

Each test should create its own anonymous table or owner key:

```elixir
setup do
  table = :ets.new(:docket_checkpoint, [:public, :ordered_set])

  on_exit(fn ->
    if :ets.info(table) != :undefined do
      :ets.delete(table)
    end
  end)

  {:ok, checkpoint_table: table}
end
```

If a test must use named tables, table names must include a unique integer or
reference. Tests should not create atoms from test names or user-controlled
strings. Anonymous ETS tables are preferred when the helper API can pass the
table reference directly.

## 9. No External Dependency Policy

Default v1 tests must not start or require:

- Ecto
- a Repo
- SQL databases
- Redis
- Docker Compose
- network services
- cloud queues
- object storage
- LLM providers
- browser automation

If future adapter packages need database contract tests, those tests should live
outside the loop default suite or be explicitly tagged. The loop Docket v1 suite
should remain runnable on a clean machine with only Elixir dependencies fetched
by the project.

## 10. Baseline To End-of-v1 Progression

The sequence should be:

1. Add support helpers, deterministic IDs, deterministic clock, memory sink, and
   ETS sink.
2. Add construction tests for canonical `Docket.Graph` updates and explicit
   verification diagnostics.
3. Add compiler validation and lowering tests before runtime execution grows.
4. Add `Docket.Test.run_inline/3` and a minimal compile-and-run test.
5. Expand compiler integration fixtures: simple edge, fan-out, multi-source
   edge, guarded edge branch group, output projection.
6. Expand inline runtime semantics: barriers, reducers, checkpoint ordering,
   interrupts, retry, failure, resume.
7. Add supervised Runtime tests only after the inline semantics are stable.
8. Add crash recovery with ETS-backed checkpoint state.
9. Add regression fixtures for bugs found during v1 implementation.
10. Keep `mix test` dependency-free throughout.

By the end of v1, every major public promise should have at least one test at
the layer where it belongs and one integration test proving the layers connect.

## 11. Coverage Matrix

| Capability | Construction | Compiler | Inline execution | Supervised |
| --- | --- | --- | --- | --- |
| Inputs and fields | graph document | channels and schemas | initial values | start API |
| Outputs | graph document | output projection | returned run/output | public wrappers |
| Nodes | public node records | runtime graph nodes | node input/output | executor dispatch |
| Simple edges | edge records | activation channels | sequential activation | runtime tick |
| Fan-out | edge records | generated channels | parallel step | task executor |
| Multi-source edge | edge record | barrier/all lowering | waits for all sources | task completion order |
| Guards | guard expression | guard validation | committed-state reads | runtime scheduling |
| Diagnostics | empty until verify | blocking errors | typed failures | public errors |
| Checkpoints | n/a | checkpoint metadata map | order and failure | callback path |
| Interrupts | node capability | resume channel wiring | wait/resume | public resolution |
| Resume | computed graph hash | graph/run match | `Loop.init/3` infers saved state | crash recovery |

## 12. Open Decisions

The v1 implementation still needs to settle these testing details:

- whether checkpoint IDs are deterministic under injected ID generation
- whether published graph immutability is enforced by Docket structs or remains
  host-owned convention with Docket helper support

Resolved testing detail:

- async checkpoint delivery remains in v1; default tests should cover accepted
  async step checkpoints and observable async delivery failures without requiring
  sleeps or external services

Those decisions should be made before the end-of-v1 suite is considered
complete.
