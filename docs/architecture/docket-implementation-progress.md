# Docket Implementation Progress

Status: active progress note
Date: 2026-07-03

This document tracks what has actually landed in code for the v1 MVP. It is not
the design source of truth; use `docket-v1-implementation-path.md` for the
planned build sequence.

## Implemented

### Graph Document Skeleton

The public graph document has been introduced as `Docket.Graph`.

Implemented fields:

- graph identity and descriptive fields
- `schema_version`
- `inputs`
- `fields`
- `outputs`
- `nodes`
- `edges`
- `policies`
- `metadata`
- `diagnostics`

Implemented public graph record structs:

- `Docket.Graph.Node`
- `Docket.Graph.Edge`
- `Docket.Graph.Field`
- `Docket.Graph.Output`
- `Docket.Graph.Diagnostic`
- `Docket.Graph.Error`

### Graph Editing API

The current public editing API uses explicit `put_*`, `update_*`, and
`delete_*` functions. The earlier shorthand `node/edge/field/input/output`
style is not implemented.

Implemented functions:

- `Docket.Graph.new/1`
- `Docket.Graph.new!/1`
- `Docket.Graph.put_input/4`
- `Docket.Graph.put_input!/4`
- `Docket.Graph.put_field/4`
- `Docket.Graph.put_field!/4`
- `Docket.Graph.put_output/4`
- `Docket.Graph.put_output!/4`
- `Docket.Graph.put_node/4`
- `Docket.Graph.put_node!/4`
- `Docket.Graph.put_edge/4`
- `Docket.Graph.put_edge!/4`
- `Docket.Graph.update_node/4`
- `Docket.Graph.update_node!/4`
- `Docket.Graph.update_edge/4`
- `Docket.Graph.update_edge!/4`
- `Docket.Graph.update_field/4`
- `Docket.Graph.update_field!/4`
- `Docket.Graph.delete_node/3`
- `Docket.Graph.delete_node!/3`
- `Docket.Graph.delete_edge/3`
- `Docket.Graph.delete_edge!/3`
- `Docket.Graph.delete_field/3`
- `Docket.Graph.delete_field!/3`
- `Docket.Graph.policy/4`
- `Docket.Graph.policy!/4`
- `Docket.Graph.metadata/4`
- `Docket.Graph.metadata!/4`
- `Docket.Graph.diagnostics/2`
- `Docket.Graph.to_map/2`
- `Docket.Graph.from_map/2`
- `Docket.Graph.from_map!/2`
- `Docket.Graph.hash/2`
- `Docket.Graph.verify/2`

Non-bang graph edit functions return `{:ok, graph}` or
`{:error, %Docket.Graph.Error{}}`. Bang edit functions return the graph or raise
`Docket.Graph.Error`. Edits clear stale diagnostics, but they do not compile or
diagnose the graph. Verification is explicit through `Docket.Graph.verify/2`.

### Graph Serialization And Hashing

`Docket.Graph.to_map/2` and `Docket.Graph.from_map/2` are the only public
entry/exit points for serialized graph documents (the v1 JSON-safe wire
format, implemented internally by `Docket.Graph.Serializer`).
`Docket.Graph.hash/2` is a SHA-256 digest over the canonical JSON encoding of
`to_map/1`, so hashes survive host storage round trips and library upgrades.

Graphs are free-form in memory, Ecto-style: editing helpers store content
exactly as given and perform no durability validation (the one edit-time
rewrite is the module / `{module, function}` implementation construction
shorthand). Canonicalization happens at the serialization boundary:
`to_map/2` coerces open content the way a JSON encoder would (atom keys and
values become strings, silently) and rejects terms with no JSON
representation (tuples, keyword lists, pids, refs, functions, structs) with
`:non_durable_value`. Hashes are computed from the canonical document, so
`hash(from_map!(to_map(graph))) == hash(graph)` holds for every dumpable
graph; graphs whose open content is already canonical also satisfy
`from_map!(to_map(graph)) == graph` on struct equality. Guards nested in
plain guard-argument positions are wrapped in a reserved `"$guard"` wire tag;
`$`-prefixed map keys are reserved in durable content. `from_map/2` validates
documents strictly (schema version, unknown keys, enum-like values,
implementation atoms via `String.to_existing_atom/1`) and never creates
atoms.

### Compiler Boundary

The compiler boundary is implemented; see "Compiler (Attempt 1)" below.

Implemented functions:

- `Docket.Graph.Compiler.verify/2`
- `Docket.Graph.Compiler.compile/2`

There is no compiler report struct and no `explain/2` function.

### Node Contracts

The executable node behavior has been introduced.

Implemented modules:

- `Docket.Node`

Implemented callbacks:

- `config_schema/0`
- `call/3`

### Public Value Constructors

The initial public value constructors used by graph records and node contracts
exist.

Implemented modules:

- `Docket.Schema`
- `Docket.Reducer`
- `Docket.Guard`

Implemented schema constructors:

- `Docket.Schema.string/1`
- `Docket.Schema.float/1`
- `Docket.Schema.map/1`
- `Docket.Schema.object/2`
- `Docket.Schema.enum/2`

Implemented reducer constructors:

- `Docket.Reducer.last_value/1`

Implemented guard constructors:

- `Docket.Guard.changed/1`
- `Docket.Guard.version_at_least/2`
- `Docket.Guard.path/2`
- `Docket.Guard.exists/1`
- `Docket.Guard.equals/2`
- `Docket.Guard.all/1`
- `Docket.Guard.any/1`
- `Docket.Guard.not/1`

### Tests

Initial graph construction tests exist in `test/docket/graph_test.exs`.

Current coverage checks:

- graph construction with public structs
- multi-source edge joins and node-local branch metadata
- kind-scoped public IDs for fields/outputs and nodes/edges
- realtime-style put/update/delete editing
- guard/schema/reducer value construction
- public ID argument errors
- to_map/from_map round-trip law on a rich graph
- free-form in-memory content with atom-to-string coercion at to_map and
  rejection of non-representable terms
- nested guard `$guard` wire tagging and reserved `$` key rejection
- schema no-default sentinel vs explicit-nil default round trip
- strict from_map document validation (versions, unknown keys, unknown
  enum-like values, unknown implementation modules)
- SHA-256 graph hashing over the canonical JSON encoding of the wire
  document, stable across to_map/from_map round trips
- graph verification attaching compiler diagnostics
- edit helpers clearing stale diagnostics

### Compiler (Attempt 1)

The compiler slice from `docket-compiler-design.md` is implemented; decisions
specific to this attempt are recorded in `docket-compiler-v1-attempt-1.md`.

- `Docket.Graph.Compiler.verify/2` and `compile/2` run the real pipeline:
  document, field/schema/reducer, output, node, edge, branch, guard,
  topology, and cycle validation, then lowering and runtime graph
  self-validation. Both share the same rules; diagnostics are always fresh.
- `Docket.Runtime.Graph`, `Docket.Runtime.Graph.Node`,
  `Docket.Runtime.Graph.Channel`, and `Docket.Runtime.Graph.Lowering` exist
  as derived internal structs with no execution behavior.
- Lowering generates `input:`/`state:`/`edge:` channels, `node:` runtime
  nodes with subscriptions and outgoing edges, `output:` projections,
  barrier channels for multi-source edges, guard descriptors on runtime
  edges, and required lowering metadata in both directions. Branch groups
  lower to metadata only.
- `Docket.Schema.validate/2` provides the minimal v1 validation engine used
  for field defaults and node config.
- Compilation is deterministic; compiling the same graph twice yields
  identical runtime graphs regardless of map insertion order.
- Test support landed under `test/support`: node fixtures, graph fixtures
  (`minimal_linear/0` through `cycle_counter/0` plus invalid variants), and
  a `Docket.Test.Case` with `assert_diagnostic/3` helpers.

## Not Yet Implemented

- `Docket.Event` codecs (events are in-memory structs delivered with
  checkpoints; hosts that persist them own the encoding for now)

## Runtime (Attempt 1) — Inline Slice

The shared execution loop from `docket-runtime-v1-attempt-1.md` is
implemented through the inline shell (run-path slices 1-7 of the
implementation path). All section 15 clarifications in the attempt doc were
confirmed; implementation deviations are recorded in its section 16.

Implemented public modules:

- `Docket.Run` with `Docket.Run.ChannelState`, `Docket.Run.TaskState`,
  `Docket.Run.InterruptState`, and strict `to_map/2` / `from_map/2` /
  `from_map!/2` codecs (`Docket.Run.Serializer` internally)
- `Docket.Checkpoint` behaviour/struct with `Docket.Checkpoint.Context`,
  sync/async delivery defaults, and `:sync`-forcing overrides
- `Docket.Event`, `Docket.Error`, `Docket.Interrupt`
- `Docket.Executor` behaviour and `Docket.Executor.Local`
- `Docket.Test.run_inline/3`, `step_inline/2`, `resume_inline/3`,
  `resolve_interrupt_inline/4`, and `Docket.Test.Checkpoint.Accept`

Implemented internal modules:

- `Docket.Runtime.Loop` — stateless transitions (`init/3`, `plan/3`,
  `apply_results/4`, `resolve_interrupt/5`) owning checkpoint emission and
  barrier semantics
- `Docket.Runtime.Algorithm` — pure planning, activation preparation, guard
  evaluation, write validation, reducer application, barrier seen-set
  tracking, edge triggering, and output projection
- `Docket.Runtime.Dispatcher` — executor dispatch, outcome normalization,
  and the mechanical retry loop
- `Docket.Runtime.Config` — option resolution with injectable clock, ID
  generator, and retry sleeper
- `Docket.Runtime.Activation`, `Docket.Runtime.TaskResult`, `Docket.Wire`

Semantics proven by tests:

- `:run_initialized` before any node execution; `Docket.run`-style start
  barrier through `Loop.init/3` inferring fresh (`:created`) versus saved
  runs
- Plan -> Execution -> Update supersteps with barrier visibility; write-based
  change tracking; deterministic same-step `last_value` conflict resolution
- fan-out, sticky multi-source barriers with reset-on-fire, guarded edges
  with lax reads, bounded cycles, and `max_supersteps` limit failures
- default-strict failure policy (no writes commit), retry policy with
  injectable backoff, deterministic non-retryable failures, and runtime
  node-policy validation (`timeout_ms`, `retry`, reserved `on_error`)
- interrupts: `:waiting` committed eagerly at the interrupt barrier, sibling
  writes committed, resolution re-executing the interrupted node through
  committed `pending_nodes`
- checkpoint ordering, sync failure keeping the previous committed run,
  async `:step_committed` failures not blocking the run, and byte-identical
  idempotency keys when re-planning an uncommitted superstep
- resume from persisted `Docket.Run` documents (including
  `to_map`/`from_map` round trips) with graph hash matching and terminal
  runs never restarting

Test support added: `Docket.Test.Checkpoint.MemorySink` and `FailOn` sinks;
executable node fixtures (`Increment`, `InterruptOnce`, `FlakyThenSucceeds`,
`AlwaysFails`, `Raises`, `Throws`, `Awaits`, `AtomWriter`, `BadReturn`);
graph fixtures `interrupt_review/0`, `retry_then_continue/0`,
`parallel_failure/0`, `same_step_isolation/0`, `write_conflict/0`; and
`cycle_counter/0` upgraded to an executable increment cycle.

`Docket.Runtime.Graph.Channel` gained a `required` flag lowered from input
`Field.required` so run initialization can enforce required inputs.

## Runtime (Attempt 1) — Supervised Slice

Run-path slices 8-11 of the implementation path are implemented on top of
the shared loop: the supervised GenServer shell, registry/supervisor,
public run APIs, task executor, ETS sink test support, and crash-recovery
coverage.

Implemented public modules:

- `Docket` public facade: `run/4`, `resume/4`, `get_run/3`,
  `resolve_interrupt/5`, `child_spec/1`, and the `use Docket` host module
  generator (`run/3`, `resume/3`, `get_run/2`, `resolve_interrupt/4`
  wrappers plus `child_spec/1`/`start_link/1`)
- `Docket.Runtime.Supervisor` — one named runtime instance: unique
  registry, `Task.Supervisor`, and a `DynamicSupervisor` for per-run
  Runtime processes (`:one_for_all`); startup options become the instance's
  default run options (stored as registry metadata), merged under per-call
  options
- `Docket.Runtime` — `restart: :temporary` GenServer shell for one active
  run driving the same `Loop` transitions on self-scheduled ticks. Emits
  the sync `:run_initialized` checkpoint inside `init/1` and reports the
  initialized run to the caller before start returns, so `Docket.run/4` is
  a true start barrier. Async checkpoint effects are submitted to the
  instance task supervisor and tracked; failures are logged without
  blocking the run. The process exits normally once the run is terminal and
  deliveries have settled, after which `get_run/3` returns `:not_found`.
- `Docket.Executor.Task` — process-isolated node execution with
  `timeout_ms` enforcement (supervised `async_nolink` under the instance
  task supervisor; unsupervised monitored process without one). Timeouts
  and task crashes normalize to retryable attempt failures.

Semantics proven by supervised tests (`test/docket/supervised/`):

- `run/4` start barrier ordering, `use Docket` wrappers, invalid input and
  failed `:run_initialized` checkpoints leaving no runtime registered
- one active Runtime per run ID (`:already_active` on duplicate run/resume),
  `get_run/3` reading only active Runtime memory, `:not_found` for unknown
  and finished runs, normal exit + deregistration after terminal
- interrupts parking the run as `:waiting` and resolving through the public
  API; stale/unknown messages ignored; async delivery failures observable
  (logged + rejected checkpoint) without blocking the run
- task executor: timeout -> non-permanent node attempt failure -> retry
  dispatches a fresh attempt; exhausted timeouts fail the run; node crashes
  isolated from the runtime tree; inline (unsupervised) Executor.Task path
- crash recovery: killed waiting and mid-superstep runtimes resume from the
  latest ETS-backed checkpoint (including a `to_map`/`from_map` round trip),
  re-executing the uncommitted superstep with the same attempt counter;
  terminal runs never restart; mismatched graph hashes rejected

Test support added: `Docket.Test.Checkpoint.EtsSink` (anonymous
`:ordered_set` tables keyed `{run_id, seq}`, `latest_run/2`,
`list_checkpoints/2`, `delete_run/2`, idempotent by seq, optional test-pid
notify), `Docket.Test.Checkpoint.Recording` (sends every checkpoint to the
test process, optional forced failures per type), the
`SleepsUntilReleased` node fixture (message-released, no wall-clock
sleeps), and the `blocking/1` graph fixture with attachable node policies.

### Compiler Node Policy Validation (Phase 9.5)

The deferred compiler validation of the v1 node policy surface is closed:
`Docket.Graph.Compiler.Policies.node_policies/1` owns the rules
(`"timeout_ms"`, `"retry" => %{"max_attempts", "backoff_ms"}`, reserved
`"on_error"`; other keys stay open content) and is shared by compiler
validation (one `:invalid_policy` diagnostic per offending key at
`[:nodes, id, :policies, key]`) and `Docket.Runtime.Algorithm` plan-time
validation, which remains as defense for hand-built runtime graphs.

## Current Test Status

Latest local check:

```text
mix test
256 tests, 0 failures
```
