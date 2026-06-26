# Docket: v1 Test Suite Design

Status: draft
Date: 2026-06-26

Related documents:

- `docs/architecture/docket-runtime-design.md`
- `docs/architecture/docket-graph-construction-design.md`
- `docs/architecture/docket-graph-execution-contract-design.md`

## 1. Purpose

This document defines the v1 test-suite shape for Docket.

The goal is to test the full path from graph authoring to runtime execution
without depending on host application infrastructure. Docket tests should prove
that:

- users can build and edit canonical `Docket.Graph` documents
- the compiler verifies and lowers those documents into executable runtime
  graphs
- the inline runner can execute graph semantics in the calling test process
- the supervised Runner uses the same execution core as the inline runner
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
- Public tests assert public structs, diagnostics, checkpoints, runs, compiler
  reports, and lowering maps. They do not assert private Runner process state.
- Runtime algorithm tests may assert internal data only in internal test files.
  Public contract tests should stay stable across implementation refactors.
- Fixtures are ordinary Elixir modules and graph values, not external files,
  services, or seeded database rows.

LangGraph's test suite is useful as a pattern: it compiles small graphs, asserts
graph projections, then runs the compiled graph to prove the builder/runtime
link works. Docket should make that bridge more explicit because
`Docket.Graph` and `Docket.Graph.Runtime` are intentionally separate.

## 3. Test Layers

The v1 suite should be organized around six layers.

### 3.1 Construction Tests

Construction tests cover the public editable graph document.

They verify:

- `Docket.Graph.new/1` creates a canonical draft graph with stable IDs
- `input/3`, `field/3`, `output/3`, `node/4`, `edge/4`, `join/4`, `branch/3`,
  `policy/3`, and metadata/layout helpers update the graph document
- `put_*`, `update_*`, and `delete_*` helpers preserve stable IDs where
  appropriate
- incomplete drafts are representable and carry advisory diagnostics
- published graph documents are not mutated in ordinary editing flows
- layout and UI metadata are preserved but do not affect runtime semantics
- malformed arguments that cannot be represented as graph data return hard
  errors or raise according to the public API contract

Construction tests should not prove runtime behavior. They prove the public
document can be built, edited, inspected, and saved by a host application.

### 3.2 Compiler Tests

Compiler tests cover `Docket.Graph -> Docket.Graph.Runtime`.

They verify:

- `verify/2`, `explain/2`, and `compile/2` share the same validation rules
- inputs lower to input channels
- state fields lower to state channels with the intended reducer
- outputs lower to output projections
- public nodes lower to `Docket.Graph.Runtime.CompiledNode` values
- node `reads` lower to readable runtime channels
- node `writes` lower to runtime write permissions
- simple edges lower to generated ephemeral activation channels
- source nodes receive generated system writes for outgoing edge channels
- target nodes subscribe to generated activation channels
- fan-out creates one generated activation channel per target
- joins lower to the required barrier representation
- guarded edges compile durable `Docket.Guard` expressions
- compiler reports expose public-to-runtime and runtime-to-public lowering maps
- diagnostics use public IDs and public graph paths whenever possible
- generated channel IDs cannot collide with user-declared IDs
- compile rejects unknown fields, unknown nodes, invalid guards, impossible
  joins, invalid reducers, unsafe node implementations, unauthorized writes,
  and cycles without an explicit v1 limit or halt condition

Compiler tests are the bridge suite. They should assert exact lowering shape
where Docket needs a stable internal contract between builder and runner.

### 3.3 Compiler Integration Tests

Compiler integration tests prove that compiled runtime graphs actually execute.

They follow this shape:

```text
Docket.Graph fixture
  -> Docket.Graph.Compiler.compile/2
  -> Docket.Test.run_inline/3
  -> assertions on Docket.Run, checkpoints, events, and outputs
```

These tests are where Docket verifies that construction, compiler lowering, and
runtime execution link up correctly.

Examples:

- a simple edge activates the target node exactly once
- fan-out activates both targets from one source
- join waits until all upstream nodes have committed
- a guarded edge activates only when the guard sees committed state
- generated edge channels are not writable by user node output
- output projections return the public output shape
- public IDs in events map back through the compiler lowering report

### 3.4 Inline Execution Tests

Inline execution tests cover graph semantics through `Docket.Test`.

They verify:

- superstep Plan -> Execution -> Update behavior
- barrier visibility: writes from one node are invisible to other nodes in the
  same superstep
- reducer application and write ordering
- checkpoint ordering and checkpoint failure behavior
- retry policy and max-attempt behavior
- interrupts, interrupt checkpoints, and resume-channel writes
- terminal detection
- max-superstep failures
- resume from a saved `Docket.Run`
- node output validation failures
- guard evaluation failures

Inline tests should be the default for ordinary execution behavior. They are
fast, deterministic, and do not depend on BEAM scheduling.

### 3.5 Supervised Runner Tests

Supervised tests cover process behavior that inline tests cannot.

They verify:

- `Docket.run/4` starts or locates a Runner through the registry/supervisor
- only one active Runner owns a run ID
- `get_run/3` reads only active Runner state
- finished or evicted runs return `{:error, :not_found}` when no Runner owns
  them
- Runner crashes can resume from the latest ETS-backed checkpoint
- task executor completions are correlated by task ID, attempt, and input hash
- stale task completions are ignored
- timeouts become node attempt failures
- checkpoint callback failures block sync checkpoint commits
- async checkpoint delivery failures are observable if async checkpoints remain
  in v1

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
- `Docket.Test.Fixtures.Graphs.unknown_read/0`
- `Docket.Test.Fixtures.Graphs.unknown_write/0`
- `Docket.Test.Fixtures.Graphs.simple_edge/0`
- `Docket.Test.Fixtures.Graphs.fanout/0`
- `Docket.Test.Fixtures.Graphs.join/0`
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

- a fresh graph has an ID, schema version, empty collections, and diagnostics
- adding input, field, node, edge, output updates the graph document
- incomplete graphs are valid draft data but not runnable
- layout metadata can be changed without changing semantic graph fields
- deleting a node removes or diagnoses affected edges according to the chosen
  public contract

### 4.3 Baseline Compiler Tests

Create:

```text
test/docket/graph/compiler/validation_test.exs
test/docket/graph/compiler/lowering_test.exs
test/docket/graph/compiler/report_test.exs
```

Initial assertions:

- `minimal_linear/0` compiles
- `unknown_read/0` fails with a diagnostic path to the public node read
- `unknown_write/0` fails with a diagnostic path to the public node write
- simple edges produce generated activation channels
- source compiled nodes have system writes for outgoing edges
- target compiled nodes subscribe to incoming generated edge channels
- compiler reports map public node, edge, input, field, and output IDs to
  runtime IDs

### 4.4 Baseline Inline Execution Tests

Create:

```text
test/docket/test/inline_runner_test.exs
test/docket/runner/inline_execution_test.exs
test/docket/runner/checkpoint_order_test.exs
```

Initial assertions:

- `Docket.Test.run_inline/3` runs `minimal_linear/0` in the calling test process
- accepted checkpoints are returned to the test
- `:run_started` is emitted before any node execution
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
- latest run snapshot is read from the latest checkpoint
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
  report_test.exs
  explain_test.exs
  generated_id_test.exs
  compile_and_run_test.exs

test/docket/channel/
  last_value_test.exs
  aggregate_test.exs
  ephemeral_test.exs
  reducer_validation_test.exs

test/docket/runner/
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
  runner_registry_test.exs
  runner_lifecycle_test.exs
  task_executor_test.exs
  crash_recovery_test.exs

test/docket/test/
  inline_runner_test.exs
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

`join/0`

```text
start -> source
source -> left
source -> right
join [left, right] -> combine
```

Proves barrier lowering and join execution.

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
join [ok_node, failing_node] -> should_not_run
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

`layout_only_change/0`

Same semantic graph as `minimal_linear/0`, but with changed layout metadata.
Proves layout does not affect compiler lowering.

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
Docket.Test.run_inline(graph, input, opts \\ [])
Docket.Test.step_inline(run_or_state, opts \\ [])
```

Return shape:

```elixir
{:ok, Docket.Run.t(), [Docket.Checkpoint.t()]}
| {:error, Docket.Error.t(), [Docket.Checkpoint.t()]}
```

`run_inline/3` should:

- compile or accept a compiled runtime graph according to the final API
- create the initial run snapshot
- emit the required `:run_started` checkpoint
- execute in the calling test process until terminal, failed, waiting, or step
  limit
- call the configured checkpoint sink
- return only accepted checkpoints

`step_inline/2` should:

- drive one committed superstep
- return after the checkpoint for that step is accepted
- return the updated public run and accepted checkpoint list

The inline helper must call the same compiler, core, algorithm, reducer,
validation, and checkpoint-building code as the supervised Runner.

### 7.2 Test-Only Convenience Helpers

Test support may add convenience wrappers around public helpers:

```elixir
compile!(graph, opts \\ [])
run_inline!(graph, input, opts \\ [])
compile_and_run!(graph, input, opts \\ [])
checkpoint_types(checkpoints)
latest_checkpoint(checkpoints)
assert_diagnostic(diagnostics, code, path)
assert_lowered_edge(report, public_edge_id, opts)
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
use deterministic ID options or assert through compiler reports and public IDs.

## 8. ETS Guidance

ETS is the default persistence tool for execution-state tests.

Use ETS for:

- checkpoint sink storage
- latest run snapshots derived from checkpoints
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
outside the core default suite or be explicitly tagged. The core Docket v1 suite
should remain runnable on a clean machine with only Elixir dependencies fetched
by the project.

## 10. Baseline To End-of-v1 Progression

The sequence should be:

1. Add support helpers, deterministic IDs, deterministic clock, memory sink, and
   ETS sink.
2. Add construction tests for canonical `Docket.Graph` updates and advisory
   diagnostics.
3. Add compiler validation and lowering tests before runtime execution grows.
4. Add `Docket.Test.run_inline/3` and a minimal compile-and-run test.
5. Expand compiler integration fixtures: simple edge, fan-out, join, guarded
   edge, output projection.
6. Expand inline runtime semantics: barriers, reducers, checkpoint ordering,
   interrupts, retry, failure, resume.
7. Add supervised Runner tests only after the inline semantics are stable.
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
| Nodes | public node records | compiled nodes | node input/output | executor dispatch |
| Simple edges | edge records | activation channels | sequential activation | runner tick |
| Fan-out | edge records | generated channels | parallel step | task executor |
| Join | join record | barrier lowering | waits for all sources | task completion order |
| Guards | guard expression | guard validation | committed-state reads | runner scheduling |
| Diagnostics | advisory warnings | blocking errors | typed failures | public errors |
| Checkpoints | n/a | checkpoint metadata map | order and failure | callback path |
| Interrupts | node capability | resume channel wiring | wait/resume | public resolution |
| Resume | graph version metadata | graph/run match | hydrate run | crash recovery |
| Layout | graph metadata | ignored by lowering | no effect | no effect |

## 12. Open Decisions

The v1 implementation still needs to settle these testing details:

- whether `Docket.Test.run_inline/3` accepts only `Docket.Graph` or also accepts
  a precompiled `Docket.Graph.Runtime`
- whether compiler reports are public structs or opaque maps with documented
  accessors
- whether checkpoint IDs are deterministic under injected ID generation
- whether async checkpoint delivery remains in v1 and, if so, which tests are
  default versus tagged
- whether published graph immutability is enforced by Docket structs or remains
  host-owned convention with Docket helper support

Those decisions should be made before the end-of-v1 suite is considered
complete.
