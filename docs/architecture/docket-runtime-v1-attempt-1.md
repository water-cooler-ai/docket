# Docket Runtime v1 — Attempt 1 Implementation Design

Status: active implementation attempt (inline slice implemented; clarifications
in section 15 all confirmed 2026-07-03, implementation deviations in section 16)
Date: 2026-07-03
Parent designs: `docket-graph-execution-contract-design.md`,
`docket-runtime-design.md`, `docket-v1-implementation-path.md`,
`docket-v1-test-suite-design.md`

This note records the concrete implementation plan for the first runtime
slice: the shared execution loop, its structs, the local executor, and the
inline test runtime. It covers run-path slices 1–7 from the implementation
path. The supervised GenServer shell, registry/supervisor, task executor, and
public `Docket.run/4` APIs (slices 8–11) reuse everything here but are a
follow-up attempt.

Where the reference documents disagree or leave decisions open, section 14
records the tiebreak and section 15 lists the questions that should be
answered before code is written.

## 1. Scope Of This Attempt

In scope:

1. Public structs: `Docket.Run` (+ `ChannelState`, `TaskState`,
   `InterruptState`), `Docket.Checkpoint` (+ behaviour + `Context`),
   `Docket.Event`, `Docket.Error`, `Docket.Interrupt`.
2. `Docket.Runtime.Algorithm`: pure planning, guard evaluation, write
   validation, reducer application, edge triggering, terminal detection,
   checkpoint data construction.
3. `Docket.Runtime.Loop`: processless transitions (`init/3`, `plan/3`,
   `apply_results/4`, `resolve_interrupt/5`) including checkpoint emission.
4. `Docket.Runtime.Dispatcher` and `Docket.Executor` + `Docket.Executor.Local`.
5. `Docket.Test.run_inline/3`, `Docket.Test.step_inline/2`, and the memory
   checkpoint sink.
6. Run codecs `Docket.Run.to_map/1` / `from_map/1` / `from_map!/1`.

Out of scope (next attempt): `Docket.Runtime` GenServer, registry, supervisor,
`Docket.Executor.Task`, ETS sink, crash-recovery tests, `use Docket` wrappers.

## 2. Module Layout

```text
lib/docket/run.ex                       Docket.Run + to_map/from_map
lib/docket/run/channel_state.ex         Docket.Run.ChannelState
lib/docket/run/task_state.ex            Docket.Run.TaskState
lib/docket/run/interrupt_state.ex       Docket.Run.InterruptState
lib/docket/error.ex                     Docket.Error
lib/docket/event.ex                     Docket.Event
lib/docket/checkpoint.ex                Docket.Checkpoint behaviour + struct
lib/docket/checkpoint/context.ex        Docket.Checkpoint.Context
lib/docket/interrupt.ex                 Docket.Interrupt

lib/docket/runtime/loop.ex              transitions + checkpoint emission
lib/docket/runtime/algorithm.ex         pure helpers (no side effects)
lib/docket/runtime/dispatcher.ex        executor dispatch + result normalization
lib/docket/runtime/activation.ex        internal Activation struct
lib/docket/runtime/task_result.ex       internal TaskResult struct

lib/docket/executor.ex                  behaviour
lib/docket/executor/local.ex            synchronous local executor

lib/docket/test.ex                      inline runtime facade
test/support/checkpoint/memory_sink.ex  Docket.Test.Checkpoint.MemorySink
```

Public modules: `Docket.Run` (and substructs), `Docket.Checkpoint`,
`Docket.Event`, `Docket.Error`, `Docket.Interrupt`, `Docket.Executor`,
`Docket.Executor.Local`, `Docket.Test`. `Loop`, `Algorithm`, `Dispatcher`,
`Activation`, and `TaskResult` are `@moduledoc false` internals.

## 3. Public Structs

### 3.1 Docket.Run

```elixir
defstruct [
  :id,
  :graph_id,
  :graph_hash,
  :status,            # :created | :running | :waiting | :done | :failed | :cancelled
  :input,             # map keyed by public input ID
  :output,            # nil until :done; map keyed by public output ID
  :started_at,
  :updated_at,
  :finished_at,
  step: 0,            # count of committed supersteps
  channels: %{},      # runtime channel ID => Docket.Run.ChannelState
  changed_channels: MapSet.new(),   # runtime channel IDs changed by the last barrier
  pending_nodes: MapSet.new(),      # public node IDs paused by an open interrupt
  active_tasks: %{},                # always empty in committed v1 runs; kept for post-v1 async
  pending_writes: [],               # always empty in committed v1 runs; kept for post-v1
  interrupts: %{},    # interrupt ID => Docket.Run.InterruptState
  timers: %{},        # unused in v1; kept for post-v1
  checkpoint_seq: 0,
  event_seq: 0,
  version: 1,         # run document schema version
  metadata: %{}
]
```

Deviations from the reference sketch (runtime design §9), recorded in
section 14:

- `superstep`/`step` naming is unified as `step`.
- The top-level `channel_versions` map is dropped; the version lives only on
  `ChannelState.version` (one source of truth, no sync bugs).
- `pending_nodes` is added as committed run state so interrupted nodes can be
  re-activated after resolution (section 11).

`status: :created` is the fresh-run sentinel: `Docket.run/4` and
`Docket.Test.run_inline/3` build the initial run with `:created`, and
`Loop.init/3` uses it to infer fresh-versus-saved execution. A `:created`
status never appears in a checkpoint.

`to_map/1` mirrors the graph serializer's rules: JSON-safe map, `MapSet`
encoded as a sorted list, atoms as strings, `$`-prefixed keys reserved,
non-durable values rejected. `from_map/1` validates strictly and never
creates atoms.

### 3.2 Docket.Run.ChannelState

```elixir
defstruct [:channel_id, :value, version: 0, barrier_seen: []]
```

- `version` advances once per committed barrier that wrote the channel
  (write-based change tracking; equal values still bump).
- `barrier_seen` is only used by `:barrier` channels: the sorted list of
  source node public IDs seen since the barrier last fired. It is committed
  run state, so it survives resume.
- Channels that have never been written are absent from `run.channels`
  (guards treat them as not existing; `version` 0 is never observable).

### 3.3 Docket.Run.TaskState, Docket.Run.InterruptState

`TaskState` matches the reference sketch (`task_id`, `node_id`, `step`,
`attempt`, `status`, `input_hash`, `started_at`, `deadline_at`,
`idempotency_key`, `metadata`). In v1 it appears only inside events and
checkpoint metadata, since committed runs never carry in-flight tasks.

`InterruptState`: `id`, `node_id`, `status` (`:open` | `:resolved`),
`resume_channel` (public field ID), `schema`, `prompt`, `created_at`,
`resolved_at`, `metadata`.

### 3.4 Docket.Checkpoint

```elixir
defstruct [:type, :delivery, :seq, :run, :events, :created_at, metadata: %{}]

@callback handle(Docket.Checkpoint.t(), Docket.Checkpoint.Context.t()) ::
            :ok | {:error, term()}
```

v1 types and default delivery:

| type | delivery |
| --- | --- |
| `:run_initialized` | `:sync` |
| `:step_committed` | `:async` |
| `:interrupt_requested` | `:sync` |
| `:interrupt_resolved` | `:sync` |
| `:run_completed` | `:sync` |
| `:run_failed` | `:sync` |

`Docket.Checkpoint.Context` carries `run_id`, `graph_id`, `graph_hash`, and
the caller-supplied application context. Delivery overrides (forcing
`:step_committed` to `:sync`) come from loop config.

### 3.5 Docket.Event

As sketched in runtime design §15.4 (`run_id`, `seq`, `type`, `step`,
`node_id`, `channel_id`, `task_id`, `timestamp`, `payload`, `metadata`).

v1 event catalog (proposal, see clarification C8):

- `:run_initialized`, `:run_completed`, `:run_failed`
- `:node_completed`, `:node_failed` (payload: attempt, duration, reason;
  one `:node_failed` per failed attempt so retries are visible)
- `:channel_updated` (one per channel per barrier; payload: version, writer
  node IDs — not the value, to keep events small)
- `:edge_triggered` (payload: edge ID, guard result)
- `:interrupt_requested`, `:interrupt_resolved`

### 3.6 Docket.Error

```elixir
defstruct [:type, :phase, :node_id, :reason, :message, details: %{}]
```

Known `type` values from the contracts: `:checkpoint_failed`,
`:graph_mismatch`, `:graph_unavailable`, `:invalid_input`,
`:max_supersteps_exceeded`, `:node_failed`, `:guard_evaluation_failed`,
`:invalid_state_update`, `:unsupported_command`, `:unsupported_await`,
`:interrupt_not_found`, `:inactive_run`, `:not_found`.

### 3.7 Docket.Interrupt

```elixir
defstruct [:id, :node_id, :prompt, :schema, :resume_channel, metadata: %{}]
```

Nodes may return `{:interrupt, %Docket.Interrupt{}}` with `id: nil`; the
runtime assigns the ID (injectable generator). `resume_channel` must name a
declared state field; the update barrier validates this and treats a bad
reference as a node attempt failure.

## 4. Internal Structs

### 4.1 Docket.Runtime.Activation

Matches execution contract §9:

```elixir
defstruct [
  :task_id,          # "#{run_id}:#{step}:#{node_public_id}:#{attempt}"
  :node_id,          # public node ID
  :runtime_node_id,  # "node:<public_id>"
  :step,
  :attempt,          # starts at 1, derived from the committed run only
  :input_hash,       # SHA-256 over canonical encoding of the state snapshot
  :idempotency_key,  # task_id (v1 has no command_index)
  :snapshot,         # committed state map, public input/field ID => value
  :source_versions,  # public input/field ID => version
  :config,           # runtime node config (defaults already applied by compiler)
  :timeout_ms,       # resolved node policy; nil = no timeout (Local ignores)
  :retry             # resolved retry policy %{max_attempts: n, backoff_ms: ms}
]
```

The idempotency invariant from the execution contract §14 is structural here:
`Algorithm.prepare_activations/4` derives `attempt` and `task_id` from the
previous committed `Docket.Run` only. Nothing in the plan/execute path
mutates run state, so a superstep that never commits re-plans with
byte-identical keys.

### 4.2 Docket.Runtime.TaskResult

```elixir
defstruct [:task_id, :node_id, :attempt, :status, :value, :meta]
# status: :ok | :interrupt | :error
# value:  update map | %Docket.Interrupt{} | normalized error reason
```

`Dispatcher` normalizes every node outcome into one final `TaskResult` per
activation: `{:ok, map}`, `{:interrupt, interrupt}`, `{:error, reason}`,
raises, exits, throws, timeouts, and the v1-rejected shapes
(`{:command, _}` → `:unsupported_command`, `{:await, _}` →
`:unsupported_await`, any other term → `:invalid_node_return`). Retryable
attempt failures are retried inside the dispatcher (section 10); only the
final outcome reaches the loop.

### 4.3 Loop Config

`Loop` functions receive `opts` and resolve them once into an internal config
map (not a public struct):

```elixir
%{
  checkpoint: module,             # required Docket.Checkpoint implementation
  checkpoint_overrides: %{},      # type => :sync (force stronger delivery)
  executor: Docket.Executor.Local,
  clock: fun,                     # () -> DateTime; injectable
  id_generator: fun,              # (kind) -> String.t(); injectable
  sleeper: fun,                   # (ms) -> :ok; injectable for retry backoff
  max_supersteps: pos_integer | nil,  # runtime override; graph policy wins if set
  context: %{}                    # application context passed to nodes
}
```

## 5. Driver Shape

The loop is processless; a shell drives it. Inline shell (`Docket.Test`) and
the future GenServer shell share this exact sequence:

```text
init:
  {:ok, run, effects} = Loop.init(rtg, run, opts)     # emits :run_initialized
  deliver(effects)                                    # async checkpoints

tick (repeat):
  case Loop.plan(rtg, run, opts) do
    {:execute, run, activations} ->
      results = Dispatcher.dispatch(activations, rtg, run, opts)
      case Loop.apply_results(rtg, run, results, opts) do
        {:ok, run, effects} -> deliver(effects); tick
        {:error, error}     -> halt with previous committed run
      end
    {:wait, run, waiting_on}   -> stop ticking; wait for resolve_interrupt
    {:terminal, run, effects}  -> deliver(effects); done
    {:error, error}            -> halt
  end

resolve_interrupt:
  {:ok, run, effects} = Loop.resolve_interrupt(rtg, run, id, value, opts)
  deliver(effects); resume ticking
```

`deliver/1` performs async checkpoint effects. The inline shell delivers them
synchronously in order (drain-by-default); the GenServer shell will submit
them to a task and track completion. This is the one place Attempt 1 deviates
from the contract's wording that "the loop invokes the configured checkpoint
callback asynchronously": a processless module cannot own async execution, so
the loop *builds* async checkpoints and returns them as effects; the shell
owns delivery (decision D6).

## 6. Loop Transitions

All transitions take the runtime graph and the current committed
`Docket.Run`, and return a new committed run plus effects, or a typed error
with the previous run untouched.

```elixir
init(rtg, run, opts) ::
  {:ok, run, effects} | {:error, Docket.Error.t()}

plan(rtg, run, opts) ::
  {:execute, run, [Activation.t()]}
  | {:wait, run, waiting_on :: [interrupt_id]}
  | {:terminal, run, effects}
  | {:error, Docket.Error.t()}

apply_results(rtg, run, [TaskResult.t()], opts) ::
  {:ok, run, effects} | {:error, Docket.Error.t()}

resolve_interrupt(rtg, run, interrupt_id, value, opts) ::
  {:ok, run, effects} | {:error, Docket.Error.t()}

# effects :: [{:checkpoint, Docket.Checkpoint.t()}]   (async, to be delivered)
```

### 6.1 init/3

1. Verify `run.graph_hash == rtg.graph_hash` (and `run.graph_id ==
   rtg.graph_id`); mismatch → `{:error, :graph_mismatch}`.
2. `status: :created` → fresh initialization:
   - Validate `run.input` against declared inputs: unknown input IDs, missing
     `required` inputs, and schema violations return
     `{:error, %Docket.Error{type: :invalid_input}}` *before any checkpoint*
     — execution never started, so nothing durable is written.
   - Write each supplied input into its `input:<id>` channel (version 1).
   - Evaluate `$start` edges exactly like an update barrier for a virtual
     completed source node `"$start"`: guards run against the initial
     committed state with the changed set = the input channels just written;
     passing edges bump their `edge:<id>` channels.
   - Set `changed_channels` to the written input channels plus fired start
     edges, `status: :running`, `started_at`, `step: 0`.
   - Emit the sync `:run_initialized` checkpoint. Failure →
     `{:error, :checkpoint_failed}` and the run is not installed.
3. Terminal saved run (`:done`/`:failed`/`:cancelled`) → return
   `{:ok, run, []}` unchanged: no checkpoint, no execution restart. The shell
   observes the terminal status and stops.
4. Non-terminal saved run (`:running`/`:waiting`) → continue as-is: emit the
   sync `:run_initialized` checkpoint (host upserts by run ID) and return.
   The next `plan/3` naturally re-executes the uncommitted superstep because
   planning derives everything from committed state.

### 6.2 plan/3

Pure delegation to `Algorithm.plan/3` plus terminal checkpoint emission:

1. Candidates = nodes whose `subscribe` list intersects
   `run.changed_channels`, plus `run.pending_nodes` whose blocking interrupt
   is resolved, sorted by public node ID. Nodes with an *open* interrupt are
   excluded.
2. No candidates, no open interrupts → terminal `:done`: compute the output
   projection (section 12), set `status: :done` / `finished_at`, emit sync
   `:run_completed`, return `{:terminal, run, effects}`.
3. No candidates, open interrupts → `{:wait, run, open_interrupt_ids}`
   (status is already `:waiting`; see section 11).
4. Candidates present but `run.step >= max_supersteps` → fail the run with
   `:max_supersteps_exceeded`: set `status: :failed`, emit sync
   `:run_failed`, return `{:terminal, run, effects}`. The limit is
   `rtg.policies["max_supersteps"]`, else the runtime config default.
5. Otherwise `Algorithm.prepare_activations/4` builds one activation per
   candidate node (snapshot, versions, hashes, keys, resolved policies) and
   plan returns `{:execute, run, activations}` with the run unchanged —
   planning commits nothing.

The state snapshot is one flat map keyed by public input/field ID over
committed values. Never-written channels are absent from the snapshot;
written-with-default semantics come from the channel default at
initialization only if the compiler declared one.

### 6.3 apply_results/4

The update barrier. All steps run in memory on a working copy; nothing is
installed until the checkpoint gate passes.

1. Partition results: `oks`, `interrupts`, `errors` (already
   retry-exhausted, i.e. permanent).
2. Any permanent error → the superstep fails: discard *all* writes (including
   `oks`), set `status: :failed` / `finished_at`, build `:node_failed` +
   `:run_failed` events, emit sync `:run_failed` checkpoint. Return
   `{:ok, failed_run, effects}` — the run is committed as failed. (Sync
   checkpoint failure instead returns `{:error, :checkpoint_failed}` with the
   previous run.)
3. Validate each ok update map (`Algorithm.validate_state_update/4`):
   - every key must be a declared state field (writes to inputs, outputs,
     edge channels, or unknown IDs → `:invalid_state_update`, a node attempt
     failure that is *not* retried — it is deterministic)
   - values must pass the field schema (`Docket.Schema.validate/2`)
   A validation failure converts that node's result to a permanent error and
   re-enters step 2.
4. Collect writes grouped by state channel, ordered by writer public node ID,
   and apply reducers (`last_value` in v1: the last write in that order
   wins). Bump each written channel version once. (See clarification C7 on
   same-step write conflicts.)
5. Record source completions on barrier channels for every ok node's
   multi-source outgoing edges; a barrier whose `barrier_seen` covers all
   sources becomes a fire candidate and resets its seen set.
6. Evaluate outgoing edges of ok nodes (single-source edges plus fired
   barriers), sorted by edge ID. Unguarded → trigger. Guarded → evaluate
   against the *newly committed* state and this barrier's changed field set;
   guard errors fail the run with `:guard_evaluation_failed`. Triggered
   edges with `to != "$finish"` bump their edge channel; `$finish` edges
   produce an `:edge_triggered` event only (termination stays quiescence-
   based, decision D4).
7. Register interrupts: assign IDs, add `InterruptState` (`:open`) to
   `run.interrupts`, add the interrupting node to `run.pending_nodes`. The
   interrupting node's own writes are not applied (it did not complete) and
   its outgoing edges do not trigger.
8. Assemble the next run: new `channels`, `changed_channels` (state channels
   with committed writes + fired edge channels), `step + 1`, cleared
   ephemeral values, `updated_at`.
9. Decide status eagerly: if interrupts were opened and the new
   `changed_channels` activate no node, set `status: :waiting` in this same
   commit so the durable run reflects reality (decision D8).
10. Build events, then checkpoints:
    - interrupts opened → one sync `:interrupt_requested` checkpoint carrying
      the full committed run (sibling writes included) and all step events —
      no separate `:step_committed` for that barrier (decision D9).
    - otherwise → `:step_committed` (async by default) returned as an effect.
    Sync failure → `{:error, :checkpoint_failed}`, previous run kept.

### 6.4 resolve_interrupt/5

1. Look up the interrupt; missing or already resolved →
   `{:error, :not_found}`.
2. Validate `value` against the interrupt schema → `:invalid_input` on
   mismatch.
3. Commit a mini-barrier: write `value` to the resume field's state channel
   (reducer + version bump), mark the interrupt `:resolved`, keep the node in
   `pending_nodes` (it re-activates on the next plan), set
   `changed_channels` to the resume channel, `status: :running`.
4. Emit sync `:interrupt_resolved`; on success return `{:ok, run, []}` and
   the shell resumes ticking.

The interrupted node then re-executes from the next plan with a fresh
snapshot that includes the resume value (the `InterruptOnce` fixture
pattern: interrupt on first call, succeed on re-execution). See
clarification C2.

## 7. Docket.Runtime.Algorithm

Pure functions, no clock reads (timestamps injected), no side effects:

```elixir
plan(rtg, run, config)                      # candidate selection + terminal/wait decision
prepare_activations(rtg, run, nodes, config)
evaluate_guard(expr, guard_context)         # {:ok, boolean} | {:error, reason}
validate_state_update(rtg, activation, update)
collect_state_writes(rtg, activations, results)
apply_state_writes(rtg, run, writes)        # reducers + versions
evaluate_edge_triggers(rtg, run, ok_nodes, changed_fields)
detect_terminal(rtg, run)
project_output(rtg, run)
build_events(run, ...)
build_checkpoint(run, type, events, config)
```

`Loop` composes these and owns checkpoint emission; `Algorithm` never calls
the checkpoint callback or the executor.

## 8. Guard Evaluation

`guard_context`:

```elixir
%{
  values: %{public_id => committed_value},   # inputs + fields
  versions: %{public_id => version},
  changed: MapSet.t(public_id)               # fields written by this barrier
}
```

Public IDs, not runtime channel IDs — guards are authored against the public
graph. Semantics per the execution contract §12: lax reads (never-written
channels and missing path segments make `exists/1`, `equals/2`, `changed/1`
false rather than raising); `changed/1` is membership in this barrier's
changed set; `version_at_least/2` compares committed version counters.
Structurally invalid expressions were rejected at compile time; anything that
still fails at runtime fails the run with `:guard_evaluation_failed`.

## 9. Dispatcher And Executor

```elixir
Docket.Runtime.Dispatcher.dispatch(activations, rtg, run, config) :: [TaskResult.t()]
```

For each activation (serial and sorted in the inline shell; semantic
parallelism is guaranteed by barrier visibility, not scheduling):

1. Build node `context`: `run_id`, `node_id` (public), `step`, `attempt`,
   `source_versions`, `application` (from config context),
   `idempotency_key`.
2. Call `config.executor.execute(task_state, runtime_node, snapshot, config,
   context, opts)`.
3. Normalize the outcome (section 4.2). Raises/exits/throws are caught and
   carried as `{kind, reason, stacktrace-summary}` error values.
4. On a retryable error with attempts remaining: sleep `backoff_ms` via the
   injectable sleeper, increment the in-flight attempt, and re-execute. Only
   the final result is returned. Attempt numbering restarts from the
   committed baseline if the superstep is ever re-executed, preserving the
   idempotency invariant.

`Docket.Executor.Local.execute/6` calls
`apply(node.module, node.function, [state, config, context])` directly in the
calling process. It cannot enforce `timeout_ms` (no process boundary) and
documents that; timeouts become real in `Docket.Executor.Task`.

## 10. Retry Policy (proposed v1 node policy surface)

The compiler currently passes node policies through unvalidated because the
runtime had not defined its surface (compiler attempt 1, decision 17). This
attempt defines it:

```elixir
# durable node policy keys (string keys in the graph document)
"timeout_ms" => pos_integer          # Task executor only in v1
"retry" => %{
  "max_attempts" => pos_integer,     # default 1 (no retry)
  "backoff_ms" => non_neg_integer    # fixed backoff, default 0
}
```

- Default when absent: one attempt, no retry.
- Retryable error classes in v1: `{:error, reason}` returns, raises, exits,
  throws, timeouts. Not retryable: output validation failures and
  `:unsupported_*` returns (deterministic — retrying cannot help).
- `on_error` routing is post-v1; the key is reserved and rejected if present.

Once confirmed, compiler validation for these keys is a small follow-up in
`Validation` (phase 9.5), closing the deferred decision. (Landed with the
supervised slice: `Policies.node_policies/1` is shared by compiler
validation and plan-time validation.)

## 11. Interrupts

- Interrupt IDs are runtime-generated (injectable generator) when the node
  leaves `id` nil; a node-supplied ID is kept (host-correlatable).
- `run.pending_nodes` (committed state) records the interrupted node so its
  activation survives the wait: edge activations are consumed by the barrier
  that observed them, so without this set the node could never re-activate.
- Resolution re-executes the interrupted node (section 6.4). It does not
  synthesize a node completion — outgoing edges trigger only after the node
  actually returns `{:ok, update}` on re-execution.
- Multiple simultaneous interrupts are supported; the run leaves `:waiting`
  when any resolution creates activations, and other interrupts stay open.
- v1 interrupts wait indefinitely (no deadlines).

## 12. Termination And Output

Termination is quiescence: a plan with no candidates and no open interrupts.
`$finish` edges are advisory (compiler warns when absent); firing one does
not force completion (decision D4).

At `:done`, `Algorithm.project_output/2` builds `run.output` as a map keyed
by public output ID, reading each projection's `source_channel` committed
value (never-written source → the output key is present with `nil`; see
clarification C9).

## 13. Determinism Injection

All nondeterminism enters through loop config: `clock`, `id_generator`,
`sleeper`. Defaults are `DateTime.utc_now/0`, UUIDv4-style generation, and
`Process.sleep/1`; tests inject deterministic versions. `Algorithm` receives
timestamps and IDs as arguments — it never generates them. Sorted iteration
everywhere (nodes, edges, channels, writes by public ID) keeps every
transition reproducible, matching the compiler's determinism discipline.

## 14. Decisions Resolving Doc Conflicts And Open Questions

1. **Loop API shape**: the execution contract's signatures win —
   `plan/3`, `apply_results/4`, `resolve_interrupt/5` over the implementation
   path's `plan/2`, `apply_results/3`, `resolve_interrupt/4`. There is no
   `Loop.State` value and therefore no `to_run/1`; shells hold
   `(runtime_graph, run, opts)` directly. (Flagged as clarification C1 since
   the implementation path explicitly lists `to_run/1`.)
2. **Fresh-run inference** uses the `status: :created` sentinel rather than
   heuristics over empty channels (C3).
3. **`step` naming** replaces the `step`/`superstep` mix in the reference
   struct sketch; `channel_versions` top-level duplication is dropped.
4. **`$finish` semantics**: quiescence-based termination; finish edges emit
   an event only (compiler design open decision 2).
5. **`{:await, _}` and `{:command, _}`** node returns are rejected in v1 as
   `:unsupported_await` / `:unsupported_command` permanent failures.
6. **Async checkpoint delivery** is a returned effect performed by the shell,
   not performed inside the processless loop (contract wording adjusted).
7. **Retry mechanics** live in the Dispatcher (attempt loop, backoff sleep);
   retry *policy* is resolved into the Activation by `Algorithm` during
   planning. Only final results reach the barrier.
8. **Waiting status is committed eagerly** in the barrier that opens an
   interrupt when no other activations remain, so the durable run never says
   `:running` while nothing can proceed.
9. **An interrupt barrier emits one checkpoint**: sync
   `:interrupt_requested` carrying the committed sibling writes and step
   events; no additional `:step_committed` for that barrier.
10. **Input validation failures** happen before any checkpoint: the caller
    gets `{:error, %Docket.Error{type: :invalid_input}}` and no host row is
    ever created.
11. **`step_inline/2` requires the graph in opts**
    (`opts[:graph]` or `opts[:runtime_graph]`) since `Docket.Run` does not
    carry the graph document.

## 15. Clarifications (All Confirmed 2026-07-03)

Every proposal below was confirmed as proposed; the implementation follows
them exactly.

- **C1 — Loop state value**: resolved as stateless. No `Loop.State`, no
  `to_run/1`; every loop function takes `(runtime_graph, run, ..., opts)`.
  `docket-v1-implementation-path.md` §5.2 slice 4 is superseded on this
  point by the execution contract §4.1 signatures.
- **C2 — Interrupt resume model**: resolution re-executes the interrupted
  node (`InterruptOnce` pattern). Resolution never synthesizes a node
  completion; outgoing edges trigger only when the node actually returns
  `{:ok, update}` on re-execution.
- **C3 — Fresh-run sentinel**: `:created` is a run status. It never appears
  in a checkpoint.
- **C4 — Node policy surface**: as in section 10 (`"timeout_ms"`,
  `"retry" => %{"max_attempts", "backoff_ms"}`, `"on_error"` reserved and
  rejected). Compiler validation of these keys landed with the supervised
  slice (`Validation` phase 9.5); plan-time validation remains as defense
  for hand-built runtime graphs and fails the run with `:invalid_policy`.
- **C5 — Sibling writes at an interrupt barrier**: they commit; the barrier
  emits one sync `:interrupt_requested` checkpoint carrying the commit.
- **C6 — pending_nodes**: added as committed `Docket.Run` state.
- **C7 — Same-step write conflicts**: reducer semantics, no conflict error;
  `last_value` takes the last write in sorted writer-node-ID order.
- **C8 — Event catalog**: as in section 3.5.
- **C9 — Output projection of never-written sources**: explicit `nil`
  entries; the output map always carries every declared output key.
- **C10 — Cancellation**: post-v1; the loop treats `:cancelled` as terminal
  defensively but nothing produces it yet.

## 16. Implementation Deviations And Additions (Attempt 1)

Discovered while implementing; none change the confirmed contracts:

1. **Checkpoint effects carry sync checkpoints too.** Effects are
   `{:checkpoint, checkpoint, context, :accepted}` (sync, delivered inside
   the transition) and `{:checkpoint, checkpoint, context, :pending}`
   (async, shell-delivered), so shells collect the full accepted sequence
   uniformly.
2. **`Docket.Runtime.Graph.Channel` gained `required`** (lowered from
   `Field.required` on inputs) so `Loop.init/3` can enforce required inputs;
   `Field.required` previously did not survive lowering.
3. **`Docket.Wire`** is a new internal module owning durable-value coercion
   for runtime-side open content (run input, state writes, interrupt
   values, run codec). The graph serializer keeps its own self-contained
   durability core; `Docket.Graph.Serializer.dump_schema/1` and
   `load_schema!/2` are exposed (`@doc false`) for interrupt schemas in the
   run codec.
4. **Event and checkpoint construction live in `Loop`**, not
   `Algorithm.build_checkpoint/4`: they need the injected clock and seq
   threading, and `Algorithm` stays timestamp-free.
5. **`Docket.Test` gained `resume_inline/3` and
   `resolve_interrupt_inline/4`** so resume and interrupt flows are testable
   inline before the public `Docket.resume/4` / `Docket.resolve_interrupt/5`
   APIs exist.
6. **`resolve_interrupt/5` unions the resume channel into
   `changed_channels`** rather than replacing the set, so a resolution
   arriving between supersteps (future GenServer shell) cannot drop
   unconsumed activations.
7. **The dispatcher owns the retry attempt loop** (mechanics only), with the
   policy resolved into the activation during planning — as designed in
   section 9; noted here because the execution contract's module index reads
   as if the loop re-dispatches.
8. **Deterministic failures are never retried**: reserved return shapes
   (`{:await, _}`, `{:command, _}`, invalid returns, invalid interrupts) and
   barrier-time validation failures are permanent regardless of retry
   budget. Only `{:error, _}`, raises, exits, throws, and timeouts are
   retryable.
